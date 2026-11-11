import os
import sys
import json
import time
import signal
import logging
from collections import Counter, OrderedDict
import redis
from confluent_kafka import TopicPartition, OFFSET_BEGINNING
from quixstreams import Application

"""
Counts Bluesky commit `operation`s (create/update/delete) from the `bluesky-events` topic into Redis.

- Counts accumulate in memory and are flushed to Redis in batches (every BATCH_SIZE msgs or
  BATCH_SECONDS), as one MULTI/EXEC transaction of INCRBYs.
- Exactly-once counting: each flush writes the counts AND the next Kafka offset to Redis in the same
  transaction. On startup/rebalance, the consumer seeks to the offset stored in Redis, so a crash can
  never double-count or skip a batch. (Offsets are also stored in Kafka, for lag visibility only.)
- Duplicates from the producer (reconnect rewinds/replays) are dropped using the `event-id` header and
  an in-memory LRU of recently seen ids (not persisted across consumer restarts).
- On exit (Ctrl-C / SIGTERM), pending counts are flushed and the project's counts are printed,
  filtered to `bluesky:operation:*` keys only.
"""

# --- Configuration ---------------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
TOPIC = "bluesky-events"
GROUP = "bluesky-operation-counter"
REDIS_HOST, REDIS_PORT = os.getenv("REDIS_HOST", "localhost"), int(os.getenv("REDIS_PORT", "6379"))
OP_PREFIX = "bluesky:operation:"           # Count keys, e.g. bluesky:operation:create
OFFSET_PREFIX = f"bluesky:offset:{GROUP}:" # Next offset to read, e.g. bluesky:offset:<group>:<topic>:<partition>
BATCH_SIZE = 500                           # Flush after this many messages...
BATCH_SECONDS = 1.0                        # ...or this many seconds, whichever comes first
DEDUP_SIZE = 100_000                       # Recent event-ids remembered for duplicate detection
REDIS_RETRIES = 5                          # Failed flush attempts before exiting

LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bluesky-consumer.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE)],
)
log = logging.getLogger("bluesky-consumer")

# --- State -----------------------------------------------------------------------------------------
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
pending = Counter()                        # Operation counts not yet written to Redis
pending_offsets = {}                       # (topic, partition) -> next offset, not yet written to Redis
pending_msgs = 0                           # Messages consumed since the last flush
last_flush = time.time()
seen = OrderedDict()                       # LRU of recent event-ids (dedup)
dupes = 0                                  # Duplicates dropped (for logging)
stop = False                               # Set by SIGINT/SIGTERM


def offset_key(topic, partition):
    return f"{OFFSET_PREFIX}{topic}:{partition}"

def flush(consumer):
    """Write pending counts + next offsets to Redis atomically; retry with backoff, exit if Redis stays down."""
    global pending_msgs, last_flush
    if not pending_offsets:
        return
    for attempt in range(1, REDIS_RETRIES + 1):
        try:
            pipe = r.pipeline(transaction=True)                     # MULTI ... EXEC
            for op, n in pending.items():
                pipe.incrby(OP_PREFIX + op, n)
            for (t, p), off in pending_offsets.items():
                pipe.set(offset_key(t, p), off)
            pipe.execute()
            break
        except redis.RedisError as e:
            log.warning(f"Redis flush failed (attempt {attempt}/{REDIS_RETRIES}): {e}")
            time.sleep(min(2 ** attempt, 30))
    else:
        # Counts/offsets were never written together, so the next run re-reads this batch: nothing lost
        log.error("Redis unreachable; exiting without saving this batch")
        sys.exit(1)

    # Also store offsets in Kafka so lag shows in Redpanda Console (Redis remains the source of truth)
    try:
        consumer.store_offsets(offsets=[TopicPartition(t, p, o) for (t, p), o in pending_offsets.items()])
    except Exception as e:
        log.warning(f"Could not store offsets in Kafka: {e}")

    log.info(f"Flushed {pending_msgs} msgs {dict(pending)} (dupes dropped so far: {dupes})")
    pending.clear()
    pending_offsets.clear()
    pending_msgs, last_flush = 0, time.time()

def on_assign(consumer, partitions):
    """Seek each assigned partition to the offset saved in Redis (or the beginning if none)."""
    for tp in partitions:
        saved = r.get(offset_key(tp.topic, tp.partition))
        tp.offset = int(saved) if saved is not None else OFFSET_BEGINNING
        log.info(f"Assigned {tp.topic}[{tp.partition}] starting at offset {saved or 'earliest'}")
    consumer.assign(partitions)

def is_duplicate(msg):
    """True if this message's event-id was seen recently; otherwise remember it (bounded LRU)."""
    event_id = dict(msg.headers() or []).get("event-id")
    if event_id is None:
        return False
    if event_id in seen:
        seen.move_to_end(event_id)
        return True
    seen[event_id] = None
    if len(seen) > DEDUP_SIZE:
        seen.popitem(last=False)
    return False

def handle_message(msg):
    """Count one message's operation (unless it's a duplicate) and record its next offset."""
    global pending_msgs, dupes
    pending_offsets[(msg.topic(), msg.partition())] = msg.offset() + 1
    pending_msgs += 1
    if is_duplicate(msg):
        dupes += 1
        return
    try:
        pending[json.loads(msg.value())["commit"]["operation"]] += 1
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        log.warning(f"Skipping bad message at offset {msg.offset()} ({type(e).__name__}: {e})")

def print_counts():
    """Print this project's operation counts from Redis (only bluesky:operation:* keys)."""
    try:
        keys = sorted(r.scan_iter(match=OP_PREFIX + "*"))
        counts = {k.removeprefix(OP_PREFIX): int(v) for k, v in zip(keys, r.mget(keys))} if keys else {}
    except redis.RedisError as e:
        log.error(f"Could not read counts from Redis: {e}")
        return
    print(f"\n{'Operation':<12}{'Count':>12}")
    for op, n in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"{op:<12}{n:>12,}")
    print(f"{'TOTAL':<12}{sum(counts.values()):>12,}")

def handle_signal(signum, frame):
    """SIGINT/SIGTERM: finish the current loop iteration, then flush and print."""
    global stop
    log.info(f"Received {signal.Signals(signum).name}; shutting down")
    stop = True


def main():
    r.ping()                               # Fail fast if Redis isn't running
    app = Application(
        broker_address=KAFKA_BROKER,
        loglevel="INFO",
        consumer_group=GROUP,
        auto_offset_reset="earliest",
        consumer_extra_config={"broker.address.family": "v4"},  # avoid IPv6 localhost issues
    )
    with app.get_consumer() as consumer:
        # Flush before partitions are taken away so their offsets/counts are saved
        consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=lambda c, p: flush(consumer))
        while not stop:
            msg = consumer.poll(0.5)
            if msg is not None:
                if msg.error() is not None:
                    log.error(f"Kafka error: {msg.error()}")
                else:
                    handle_message(msg)
            # Batch flush: by size or by time
            if pending_msgs >= BATCH_SIZE or time.time() - last_flush >= BATCH_SECONDS:
                flush(consumer)
        flush(consumer)                    # Final flush on exit


if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    try:
        main()
    except redis.RedisError as e:
        log.error(f"Redis error: {e}")
        sys.exit(1)
    except Exception:
        log.exception("Consumer crashed")
        sys.exit(1)
    finally:
        print_counts()
