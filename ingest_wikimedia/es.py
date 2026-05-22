"""Shared Elasticsearch helpers for the get-ids-* tools.

Both tools paginate ES with `search_after` and need the same two protections
against silent partial results:

  * a hard wall-clock timeout via SIGALRM (catches ES connections that stall
    mid-response — `requests`' timeout only fires when no bytes arrive)
  * a response validator that raises on `timed_out` or shard failures, so an
    HTTP-200 partial response cannot be mistaken for an empty page and silently
    end pagination (see lessons.md: "Elasticsearch queries: validate `timed_out`
    and `_shards.failed` before consuming results")

Must be imported on the main thread: `signal.signal()` raises ValueError when
called from any other thread.  The get-ids-* CLI tools import this at module
load before spinning up their ThreadPoolExecutor, which satisfies the
requirement.
"""

import signal
import threading
from typing import Any

import requests

ES_URL = "http://search-prod1.internal.dp.la:9200/dpla_alias/_search"
ES_HARD_TIMEOUT = 120


def _alarm_handler(signum: int, frame: object) -> None:
    raise TimeoutError(f"ES query exceeded {ES_HARD_TIMEOUT}s")


# Registered once at import time; only signal.alarm() is toggled per request.
signal.signal(signal.SIGALRM, _alarm_handler)


def post_es(query: dict) -> requests.Response:
    """POST to ES_URL with a hard wall-clock timeout via SIGALRM.

    `requests` timeout=30 fires only when no bytes arrive for 30s — it cannot
    catch ES stalling mid-response (drip-feeding bytes or holding an open
    connection indefinitely). SIGALRM interrupts the blocked socket read in the
    main thread, providing a true ceiling on total request time.

    Raises TimeoutError if the request exceeds ES_HARD_TIMEOUT seconds.
    Raises RuntimeError if called from a non-main thread — `signal.alarm()` is
    a no-op from worker threads but the alarm still fires on the main thread,
    silently corrupting whatever the main thread is doing.
    """
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError(
            "post_es() must be called from the main thread "
            "(SIGALRM-based timeout only works on the main thread)"
        )
    signal.alarm(ES_HARD_TIMEOUT)
    try:
        return requests.post(ES_URL, json=query, timeout=30)
    finally:
        signal.alarm(0)


def check_es_response(data: dict[str, Any]) -> None:
    """Raise if the response was partial — timed-out or had shard failures.

    An ES response can return HTTP 200 while containing partial data — one or
    more shards may have timed out or failed.  In paginated search_after loops
    that exit on empty `hits`, a partial response silently truncates the result
    set.  Always call this after parsing the JSON, before reading `hits`.
    """
    if data.get("timed_out"):
        raise RuntimeError("Elasticsearch query timed out — results may be incomplete")
    shards = data.get("_shards", {})
    if shards.get("failed", 0) > 0:
        raise RuntimeError(
            f"Elasticsearch query had {shards['failed']} shard failure(s)"
        )
