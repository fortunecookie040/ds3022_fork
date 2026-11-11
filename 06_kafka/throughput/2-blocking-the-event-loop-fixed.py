"""
FIX for MISSTEP 2: Blocking inside the event loop.

What changed (compare with 2-blocking-the-event-loop.py):
  1. The slow 5 ms lookup runs on a pool of worker THREADS, so the event loop never
     freezes and keeps reading the socket and answering keepalive pings.
  2. Many lookups run AT ONCE (WORKERS threads). Little's law says we need at least
     1000 msg/s * 0.005 s = 5 in flight; 16 gives ~3200 msg/s, enough headroom to
     catch up after a backlog.
  3. The line of pending lookups is BOUNDED (MAX_IN_FLIGHT). When it is full the reader
     waits, which slows reading from the socket: real backpressure, not a memory leak
     (avoids missteps 3 and 4).
  4. Messages are produced to Kafka in the SAME ORDER they arrived (an ordered line of
     futures), even though lookups finish in any order.
  5. `seq` gap bookkeeping happens in the reader, in true arrival order.
  6. One bad message is logged and skipped instead of killing the whole stream; logs go
     to a local file next to this script.
  7. A loop monitor reports how long the event loop was stuck (should stay near 0 ms).

Watch:  `lag` stays near 0 and `max_stall` stays tiny; `missed` stays 0.
"""
import asyncio
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import websockets
from quixstreams import Application

URL = "ws://127.0.0.1:8765/"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
WORKERS = 16                                            # lookups running at the same time
MAX_IN_FLIGHT = 64                                      # max messages waiting; full -> reader pauses

# local file log next to this script (*.log is gitignored)
log = logging.getLogger("blocking-fixed")
log.setLevel(logging.INFO)
_fh = logging.FileHandler(Path(__file__).with_suffix(".log"))
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
log.addHandler(_fh)

app = Application(broker_address=KAFKA_BROKER, producer_extra_config={"broker.address.family": "v4"})
topic = app.topic(name="trades", value_serializer="json")
executor = ThreadPoolExecutor(max_workers=WORKERS)      # the "helpers" that do the slow lookups
stats = {"count": 0, "last_seq": None, "missed": 0, "bad": 0, "stall": 0.0}


def enrich(data):
    """Same 5 ms synchronous lookup as the original, unchanged on purpose. Only WHERE it runs changed."""
    time.sleep(0.005)
    data["enriched"] = True
    return data


async def reader(ws, pending):
    """Read the socket, check seq in arrival order, and hand each lookup to a thread without waiting for it."""
    loop = asyncio.get_running_loop()
    async for message in ws:
        try:
            data = json.loads(message)
            # gap check here (arrival order), not after the lookups, which finish out of order
            if stats["last_seq"] is not None and data["seq"] != stats["last_seq"] + 1:
                stats["missed"] += data["seq"] - stats["last_seq"] - 1
            stats["last_seq"] = data["seq"]
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            stats["bad"] += 1
            log.warning(f"skipping malformed message: {e!r}")
            continue
        fut = loop.run_in_executor(executor, enrich, data)   # start the lookup on a thread
        await pending.put(fut)                               # waits ONLY when MAX_IN_FLIGHT are pending


async def writer(producer, pending):
    """Take lookups in arrival order, wait for each to finish, and produce it to Kafka."""
    while (fut := await pending.get()) is not None:          # None = reader is done
        try:
            data = await fut                                 # wait for THIS message's lookup (keeps order)
            msg = topic.serialize(key=data["symbol"], value=data)
            producer.produce(topic=topic.name, key=msg.key, value=msg.value)
            stats["count"] += 1
            if stats["count"] % 1000 == 0:
                line = (f"produced={stats['count']} missed={stats['missed']} "
                        f"lag={time.time() - data['ts']:.2f}s in_flight={pending.qsize()} "
                        f"max_stall={stats['stall'] * 1000:.0f}ms")
                print(line)
                log.info(line)
                stats["stall"] = 0.0                         # reset for the next 1000 messages
        except Exception as e:
            stats["bad"] += 1
            log.error(f"skipping message after failed lookup/produce: {e!r}")


async def loop_monitor():
    """Sleep 100 ms over and over; any extra time means something blocked the event loop."""
    while True:
        t = time.perf_counter()
        await asyncio.sleep(0.1)
        stats["stall"] = max(stats["stall"], time.perf_counter() - t - 0.1)


async def main():
    """Run reader, writer, and monitor together; drain what's left when the connection ends."""
    pending = asyncio.Queue(maxsize=MAX_IN_FLIGHT)           # bounded line of futures, in arrival order
    monitor = asyncio.create_task(loop_monitor())
    with app.get_producer() as producer:
        w = asyncio.create_task(writer(producer, pending))
        try:
            async with websockets.connect(URL) as ws:
                await reader(ws, pending)
        except Exception as e:
            print(f"\nConnection lost: {e!r}")
            log.error(f"connection lost: {e!r}")
        finally:
            await pending.put(None)                          # tell the writer no more messages are coming
            await w                                          # let it finish what's already in line
            monitor.cancel()
    executor.shutdown()
    summary = f"Total produced: {stats['count']}, missed: {stats['missed']}, bad: {stats['bad']}"
    print(summary)
    log.info(summary)


asyncio.run(main())
