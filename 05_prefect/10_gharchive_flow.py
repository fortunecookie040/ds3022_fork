import gzip
import json
import logging
from collections import Counter
from pathlib import Path
import requests
from prefect import flow, task, get_run_logger


URL = "https://data.gharchive.org/2025-01-01-{hour}.json.gz"

# local file log next to this script (*.log is gitignored)
# (handler attached directly: Prefect already configures the root logger,
# so logging.basicConfig would be a silent no-op)
log = logging.getLogger("gharchive")
log.setLevel(logging.INFO)
_fh = logging.FileHandler(Path(__file__).with_name("gharchive_flow.log"))
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
log.addHandler(_fh)


@task(retries=3, retry_delay_seconds=5)
def count_events_for_hour(hour):
    # stream one hourly gzipped file and return {event_type: count}
    logger = get_run_logger()
    counts, bad = Counter(), 0
    try:
        with requests.get(URL.format(hour=hour), stream=True, timeout=30) as r:
            r.raise_for_status()  # raise on HTTP errors so Prefect retries
            # decompress on the fly; each line is one JSON event
            with gzip.open(r.raw, mode="rt", encoding="utf-8") as lines:
                for line in lines:
                    try:
                        counts[json.loads(line).get("type", "Unknown")] += 1
                    except json.JSONDecodeError:
                        bad += 1  # skip corrupt lines rather than failing the hour
    except Exception as e:
        # log locally, then re-raise so Prefect's retries kick in
        log.error(f"hour {hour}: failed with {e!r}")
        raise
    if bad:
        logger.warning(f"hour {hour}: skipped {bad} malformed lines")
        log.warning(f"hour {hour}: skipped {bad} malformed lines")
    log.info(f"hour {hour}: {counts.total()} events")
    print(f"hour {hour}: {counts.total()} events")
    return dict(counts)


@task
def combine_counts(hourly_counts):
    # Total counts by type and order them
    # print those to screen and return them
    pass


@flow(log_prints=True)
def gharchive_flow(hours: list[int] = list(range(1, 4))):
    # take the range and create a prefect .map over the hours
    hourly = count_events_for_hour.map(hours).result()
    totals = combine_counts(hourly)
    for k, v in totals.items():
        print(k, v)


if __name__ == "__main__":
    gharchive_flow()
