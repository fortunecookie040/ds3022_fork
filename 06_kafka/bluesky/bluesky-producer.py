import websocket
import json
import os
import sys
import time
import signal
import logging
import itertools
import threading
from confluent_kafka.admin import AdminClient
from quixstreams import Application
from quixstreams.models import TopicConfig

"""
Robust Kafka producer: streams real-time Bluesky Jetstream events into the `bluesky-events` topic,
keyed by DID. Throughput (msgs/sec) is logged every 5 seconds to stdout and `bluesky-producer.log`.

Delivery guarantees (at-least-once):
- Two cursors: `cursor` = last event handed to the producer; `delivered_cursor` = last event Kafka
  CONFIRMED (via `on_delivery`). Only the delivered cursor is persisted and used for replay.
- The delivered cursor is saved to `jetstream_cursor.txt` every 5s and on exit, and loaded on startup,
  so a crash/restart resumes where it left off (a cursor older than topic retention is ignored).
- Idempotent producer (acks=all) + 1 partition => in-order, no retry duplicates, so the delivered
  cursor is a safe replay point.
- If a delivery fails or the local buffer fills (BufferError), we stop reading, flush, and reconnect
  from the earliest unconfirmed event. Jetstream acts as the replay log.
- Kafka is health-checked before every (re)connect; exit(1) after repeated failures.
- SIGINT (Ctrl-C) and SIGTERM (`docker stop`) both stop cleanly: flush + save cursor.
- A watchdog reconnects if the stream goes silent, and reconnects rotate across Jetstream servers.

Duplicates: replays and the reconnect rewind resend events. Each message carries an `event-id` header
(`did:rev:collection:rkey`, unique per record operation); a consumer can drop duplicates by keeping
a bounded set/LRU of recently seen event-ids (or by upserting on that id downstream).

TEST: run for a couple of minutes, Ctrl+C, wait 5-10s, restart; the next msgs/sec reading should spike.
"""

# --- Configuration ---------------------------------------------------------------------------------
JETSTREAM_SERVERS = [                      # Rotated on each reconnect
    "jetstream2.us-west.bsky.network",
    "jetstream1.us-west.bsky.network",
    "jetstream1.us-east.bsky.network",
    "jetstream2.us-east.bsky.network",
]
JETSTREAM_PATH = "/subscribe?wantedCollections=app.bsky.*"
CURSOR_REWIND_US = 2_000_000               # On reconnect, rewind 2s so no events are missed (may produce a few duplicates)
MAX_BACKOFF = 60                           # Max seconds to wait between reconnect attempts
TOPIC_RETENTION_MS = 3 * 60 * 60 * 1000    # 3 hours
RATE_INTERVAL = 5                          # Seconds between msgs/sec reports and cursor saves
SILENCE_TIMEOUT = 30                       # Reconnect if no message arrives for this many seconds
KAFKA_HEALTH_TRIES = 5                     # Failed Kafka health checks before exiting
FLUSH_TIMEOUT = 30                         # Max seconds to wait for a flush

HERE = os.path.dirname(os.path.abspath(__file__))
CURSOR_FILE = os.path.join(HERE, "jetstream_cursor.txt")
LOG_FILE = os.path.join(HERE, "bluesky-producer.log")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")

# --- Logging: stdout + local file ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE)],
)
log = logging.getLogger("bluesky-producer")

# --- State -----------------------------------------------------------------------------------------
cursor = None                              # time_us of the last event handed to the producer
delivered_cursor = None                    # time_us of the last event Kafka confirmed (safe resume point)
replay_from = None                         # time_us of the earliest failed event, if any
needs_replay = False                       # Set when a delivery fails or the buffer fills
backoff = 1                                # Current reconnect delay in seconds
stop = False                               # Set by SIGINT/SIGTERM to break out of the reconnect loop
current_ws = None                          # Active WebSocketApp (so signals/watchdog can close it)
last_msg_time = time.time()                # When the last websocket message arrived (silence detection)
msg_count = 0                              # Messages produced since the last report
last_report = time.time()                  # When the last msgs/sec report was logged

# --- Kafka setup -----------------------------------------------------------------------------------
app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={
        "broker.address.family": "v4",
        "enable.idempotence": True,        # implies acks=all; retries can't duplicate or reorder
        "compression.type": "zstd",        # JSON compresses well
        "linger.ms": 20,                   # Wait up to 20ms to fill batches
        "batch.size": 262144,              # Batch up to 256KB
    }
)
topic = app.topic(
    name="bluesky-events",
    key_serializer="str",                  # DID keys are plain strings (default is bytes)
    value_serializer="json",
    config=TopicConfig(
        num_partitions=1,
        replication_factor=3,
        extra_config={"retention.ms": str(TOPIC_RETENTION_MS)}
    )
)
producer = app.get_producer()              # Reused for every message


# --- Cursor persistence ----------------------------------------------------------------------------
def load_cursor():
    """Load the saved delivered cursor; ignore it if missing, corrupt, or older than topic retention."""
    try:
        with open(CURSOR_FILE) as f:
            saved = int(f.read().strip())
    except FileNotFoundError:
        return None
    except (ValueError, OSError) as e:
        log.warning(f"Ignoring unreadable cursor file: {e}")
        return None
    age_ms = (time.time_ns() // 1000 - saved) / 1000
    if age_ms > TOPIC_RETENTION_MS:
        log.warning(f"Saved cursor is {age_ms / 3_600_000:.1f}h old (older than retention); starting live")
        return None
    log.info(f"Loaded cursor {saved} ({age_ms / 1000:.0f}s ago)")
    return saved

def save_cursor():
    """Atomically write the replay-safe cursor (temp file + rename, so a crash can't corrupt it)."""
    safe = resume_cursor()
    if safe is None:
        return
    try:
        tmp = CURSOR_FILE + ".tmp"
        with open(tmp, "w") as f:
            f.write(str(safe))
        os.replace(tmp, CURSOR_FILE)
    except OSError as e:
        log.error(f"Failed to save cursor: {e}")

def resume_cursor():
    """Earliest point we must resume from: the last confirmed event, or just before the first failure."""
    if replay_from is not None:
        return min(delivered_cursor, replay_from - 1) if delivered_cursor else replay_from - 1
    return delivered_cursor


# --- Kafka helpers ---------------------------------------------------------------------------------
def kafka_healthy():
    """Return True if the Kafka cluster answers a metadata request within 5s."""
    try:
        AdminClient({"bootstrap.servers": KAFKA_BROKER, "broker.address.family": "v4"}).list_topics(timeout=5)
        return True
    except Exception as e:
        log.warning(f"Kafka health check failed: {e}")
        return False

def wait_for_kafka():
    """Health-check Kafka with backoff; exit(1) after KAFKA_HEALTH_TRIES failures."""
    delay = 1
    for attempt in range(1, KAFKA_HEALTH_TRIES + 1):
        if kafka_healthy():
            return
        if stop:
            return
        log.warning(f"Kafka unreachable (attempt {attempt}/{KAFKA_HEALTH_TRIES}); retrying in {delay}s")
        time.sleep(delay)
        delay = min(delay * 2, MAX_BACKOFF)
    log.error("Kafka is unreachable; giving up")
    sys.exit(1)

def make_on_delivery(time_us):
    """Build a delivery callback bound to this event's time_us (runs inside producer.poll/flush)."""
    def on_delivery(err, msg):
        global delivered_cursor, replay_from, needs_replay
        if err is not None:
            log.error(f"Delivery failed for event {time_us}: {err}")
            replay_from = time_us if replay_from is None else min(replay_from, time_us)
            needs_replay = True
        elif not needs_replay:
            # Only advance while no failure is pending, so we never skip past a failed event
            delivered_cursor = time_us
    return on_delivery

def trigger_replay(ws, reason):
    """Stop reading the stream; the main loop will flush and reconnect from the safe cursor."""
    global needs_replay
    needs_replay = True
    log.warning(f"{reason}: stopping stream to flush and replay")
    ws.close()


# --- WebSocket callbacks ---------------------------------------------------------------------------
def on_open(ws):
    """Connection established: reset the backoff delay."""
    global backoff
    backoff = 1
    log.info("Connected")

def on_message(ws, message):
    """Produce each commit event to Kafka (keyed by DID, timestamped by event time) and track progress."""
    global cursor, msg_count, last_report, last_msg_time, replay_from
    last_msg_time = time.time()
    if needs_replay:                       # A failure is pending: stop handing events to the producer
        ws.close()
        return
    try:
        data = json.loads(message)

        # Jetstream also sends 'identity' and 'account' events, which have no 'commit' field
        if data.get("kind") != "commit":
            return

        did, time_us, commit = data["did"], data["time_us"], data["commit"]

        # Unique per record operation; lets consumers drop duplicates from replays/rewinds
        event_id = f"{did}:{commit.get('rev')}:{commit.get('collection')}:{commit.get('rkey')}"

        # Serialize (DID key, full event value) and hand it to the producer (which also polls callbacks)
        msg = topic.serialize(key=did, value=data)
        producer.produce(
            topic=topic.name,
            key=msg.key,
            value=msg.value,
            headers={"event-id": event_id},
            timestamp=time_us // 1000,     # Kafka timestamps are in ms
            poll_timeout=1.0,              # On BufferError, wait 1s for the buffer to drain...
            buffer_error_max_tries=3,      # ...up to 3 times, then raise
            on_delivery=make_on_delivery(time_us),
        )
        cursor = time_us                   # Handed off safely (not yet confirmed)

        # Count produced messages and report throughput every RATE_INTERVAL seconds
        msg_count += 1
        now = time.time()
        if now - last_report >= RATE_INTERVAL:
            log.info(f"{msg_count / (now - last_report):,.0f} msgs/sec")
            msg_count, last_report = 0, now

    except BufferError:
        # Kafka is slow or down and the local queue is full; this event was NOT produced
        replay_from = time_us if replay_from is None else min(replay_from, time_us)
        trigger_replay(ws, "Producer buffer full")
    except (json.JSONDecodeError, KeyError) as e:
        log.warning(f"Bad message ({type(e).__name__}: {e})")
    except Exception as e:
        log.exception(f"Unexpected error handling message: {e}")

def on_error(ws, error):
    """Report WebSocket-level errors. Ctrl-C can still surface here (websocket-client swallows it)."""
    global stop
    if isinstance(error, KeyboardInterrupt):
        stop = True
    else:
        log.error(f"WebSocket error: {error}")

def on_close(ws, close_status_code, close_msg):
    log.info(f"WebSocket closed ({close_status_code}: {close_msg})")


# --- Signals & watchdog ----------------------------------------------------------------------------
def handle_signal(signum, frame):
    """SIGINT/SIGTERM: flag a stop and close the stream; the finally block flushes and saves."""
    global stop
    log.info(f"Received {signal.Signals(signum).name}; shutting down")
    stop = True
    if current_ws:
        current_ws.close()

def watchdog():
    """Background thread: save the cursor periodically and reconnect if the stream goes silent."""
    while not stop:
        time.sleep(RATE_INTERVAL)
        save_cursor()
        if current_ws and time.time() - last_msg_time > SILENCE_TIMEOUT:
            log.warning(f"No messages for {SILENCE_TIMEOUT}s; forcing reconnect")
            current_ws.close()

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


# --- Main reconnect loop ---------------------------------------------------------------------------
# Each pass: check Kafka, open a connection (resuming from the safe cursor), run until it drops,
# flush, then back off and try the next server.
delivered_cursor = load_cursor()
servers = itertools.cycle(JETSTREAM_SERVERS)
threading.Thread(target=watchdog, daemon=True).start()
try:
    while not stop:
        wait_for_kafka()
        if stop:
            break
        start = resume_cursor()
        replay_from, needs_replay = None, False
        if start:
            delivered_cursor = start       # Replay re-confirms from here
        url = f"wss://{next(servers)}{JETSTREAM_PATH}" + (f"&cursor={start - CURSOR_REWIND_US}" if start else "")
        log.info(f"Connecting to {url}")
        last_msg_time = time.time()
        current_ws = websocket.WebSocketApp(
            url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        # Pings detect silently dead connections; reconnect=0 hands reconnection to this loop
        current_ws.run_forever(ping_interval=30, ping_timeout=10, reconnect=0)

        # Flush so delivery callbacks settle the cursor before we decide where to resume
        remaining = producer.flush(FLUSH_TIMEOUT)
        if remaining:
            log.warning(f"{remaining} messages still unconfirmed after flush")
        save_cursor()
        if stop:
            break
        log.info(f"Disconnected. Reconnecting in {backoff}s...")
        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)
except KeyboardInterrupt:
    pass
finally:
    stop = True
    # Always flush buffered messages, then persist the confirmed cursor
    remaining = producer.flush(FLUSH_TIMEOUT)
    if remaining:
        log.warning(f"{remaining} messages unconfirmed at exit; they will be replayed next run")
    save_cursor()
    log.info(f"Stopped. Saved cursor {resume_cursor()}")
