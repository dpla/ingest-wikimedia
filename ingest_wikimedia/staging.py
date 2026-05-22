"""Shared helpers for bounded async S3 staging in the get-ids-* tools.

Both get-ids-es and get-ids-nara follow the same pattern when writing ES
source documents to S3: a BoundedSemaphore caps in-flight writes so the
executor queue never holds more than n_workers * 4 documents in memory,
and a done callback releases the slot and counts failures under a Lock.
"""

import json
import logging
import threading
from collections.abc import Callable
from concurrent.futures import Future

from .s3 import S3Client

_QUEUE_DEPTH_MULTIPLIER = 4


def stage_item_to_s3(
    s3_client: S3Client, partner: str, dpla_id: str, source: dict
) -> None:
    """Write item metadata JSON to S3 as dpla-map.json.

    Raises on failure so the caller's ThreadPoolExecutor can observe it via
    future.exception() in the done callback.
    """
    s3_client.write_item_metadata(partner, dpla_id, json.dumps(source))


def make_s3_stage_context(
    n_workers: int,
) -> tuple[threading.BoundedSemaphore, list[int], Callable[[str], Callable]]:
    """Return (sem, failed, on_done_factory) for bounded async S3 staging.

    The semaphore limits in-flight S3 writes to n_workers * 4, preventing
    the executor queue from growing unboundedly and holding thousands of ES
    source documents in memory.  The done callback releases the slot, counts
    failures under a Lock, and logs a warning for each one.

    Typical usage::

        s3_sem, failed, _on_s3_done = make_s3_stage_context(S3_WRITE_WORKERS)

        with ThreadPoolExecutor(max_workers=S3_WRITE_WORKERS) as executor:
            for dpla_id, source in items:
                s3_sem.acquire()
                future = executor.submit(
                    stage_item_to_s3, s3_client, partner, dpla_id, source
                )
                future.add_done_callback(_on_s3_done(dpla_id))

        if failed[0]:
            print(f"Error: {failed[0]} S3 writes failed", file=sys.stderr)
            raise SystemExit(1)
    """
    sem = threading.BoundedSemaphore(n_workers * _QUEUE_DEPTH_MULTIPLIER)
    failed = [0]
    lock = threading.Lock()

    def on_done(dpla_id: str) -> Callable:
        def callback(future: Future) -> None:
            sem.release()
            exc = future.exception()
            if exc:
                with lock:
                    failed[0] += 1
                logging.warning(f"S3 write failed for {dpla_id}: {exc}")

        return callback

    return sem, failed, on_done
