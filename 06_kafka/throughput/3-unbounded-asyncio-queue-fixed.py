"""
FIX for MISSTEP 3: An unbounded asyncio.Queue between the socket and the producer.

What changed (compare with 3-unbounded-asyncio-queue.py):
  1. The queue has a SIZE LIMIT (MAX_IN_FLIGHT). When it is full, `await queue.put()`
     really waits, so the receiver stops reading the socket. The slowness shows up at
     the server (real backpressure) instead of hiding in our memory.
  2. The writer is FASTER: the ~2 ms downstream step now runs for many messages AT ONCE
     (one small task per queued message, never more than MAX_IN_FLIGHT + 1 of them).
     One-at-a-time = ~500 msg/s; overlapping = thousands of msg/s.
  3. Messages are still produced to Kafka in ARRIVAL ORDER: the queue holds the tasks
     in line, and the writer always waits for the front one first.
  4. JSON parsing and the `seq` gap check happen in the receiver, in arrival order.
  5. One bad message is logged and skipped; the writer can't die silently and leave the
     queue unattended. Logs go to a local file next to this script.
  6. On shutdown the writer drains what's left instead of dropping it.

Watch:  `queued` stays small (never above MAX_IN_FLIGHT), `lag` stays near 0, `missed` stays 0.
"""
import asyncio
import json
import logging
import os
import time
from pathlib import Path
import websockets
from quixstreams import Application

URL = "ws://127.0.0.1:8765/"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
MAX_IN_FLIGHT = 64                                      # the size limit: full -> receiver waits

# local file log next to this script (*.log is gitignored)
log = logging.getLogger("unbounded-fixed")
log.setLevel(logging.INFO)
_fh = logging.FileHandler(Path(__file__).with_suffix(".log"))
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
log.addHandler(_fh)

app = Application(broker_address=KAFKA_BROKER, producer_extra_config={"broker.address.family": "v4"})
topic = app.topic(name="trades", value_serializer="json")
queue = asyncio.Queue(maxsize=MAX_IN_FLIGHT)            # <-- THE FIX (bounded)
stats = {"count": 0, "last_seq": None, "missed": 0, "bad": 0, "max_queued": 0}


async def slow_work(data):
    """Same ~2 ms async downstream step as the original, unchanged on purpose."""
    await asyncio.sleep(0.002)
    return data


async def receiver():
    """Read the socket, check seq in arrival order, and start each message's slow step."""
    async with websockets.connect(URL) as ws:
        async for message in ws:
            try:
                data = json.loads(message)
                # gap check here, where messages are still in perfect arrival order
                if stats["last_seq"] is not None and data["seq"] != stats["last_seq"] + 1:
                    stats["missed"] += data["seq"] - stats["last_seq"] - 1
                stats["last_seq"] = data["seq"]
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                stats["bad"] += 1
                log.warning(f"skipping malformed message: {e!r}")
                continue
            # start the slow step now (it runs alongside others), then get in line.
            # put() WAITS when MAX_IN_FLIGHT are already in line -> we stop reading the socket.
            await queue.put(asyncio.create_task(slow_work(data)))
            stats["max_queued"] = max(stats["max_queued"], queue.qsize())


async def writer(producer):
    """Take tasks from the front of the line, wait for each, and produce it to Kafka in order."""
    while (task := await queue.get()) is not None:            # None = receiver is done
        try:
            data = await task                                  # front of the line first (keeps order)
            msg = topic.serialize(key=data["symbol"], value=data)
            producer.produce(topic=topic.name, key=msg.key, value=msg.value)
            stats["count"] += 1
            if stats["count"] % 1000 == 0:
                line = (f"produced={stats['count']} queued={queue.qsize()} "
                        f"max_queued={stats['max_queued']} missed={stats['missed']} "
                        f"lag={time.time() - data['ts']:.2f}s")
                print(line)
                log.info(line)
        except Exception as e:
            stats["bad"] += 1
            log.error(f"skipping message after failed work/produce: {e!r}")


async def main():
    """Run receiver and writer together; drain what's left when the connection ends."""
    with app.get_producer() as producer:
        w = asyncio.create_task(writer(producer))
        try:
            await receiver()
        except Exception as e:
            print(f"\nConnection lost: {e!r}")
            log.error(f"connection lost: {e!r}")
        finally:
            await queue.put(None)                              # tell the writer nothing more is coming
            await w                                            # let it finish what's already in line
    summary = (f"Total produced: {stats['count']}, missed: {stats['missed']}, "
               f"bad: {stats['bad']}, max_queued: {stats['max_queued']}")
    print(summary)
    log.info(summary)


asyncio.run(main())
