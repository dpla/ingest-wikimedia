from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import time
from pathlib import Path

import requests

from ingest_wikimedia.web import USER_AGENT

BANLIST_FILE_NAME = "dpla-id-banlist.txt"

# The committed ``dpla-id-banlist.txt`` is the *floor* of the banlist. On top of
# it we optionally union the live output of the "DPLA files deleted at
# Commons:Deletion requests" Quarry query (query 90099). Re-running the query on
# Quarry makes the new IDs live on the next pipeline invocation — no PR/merge.
#
# We do NOT blindly follow the ``result/latest`` redirect. A Quarry run takes
# several minutes (~9 min for this query), and while it executes the query's
# ``latest_run`` can already point at the in-progress run — whose output is
# absent or partial. So we read the ``meta`` endpoint first and only consume the
# run output when ``latest_run.status == "complete"``.
#
# Batch launches pass ``wait_for_run=True``: if a run is in progress they BLOCK,
# polling until it completes (bounded by ``RUN_WAIT_TIMEOUT_SECONDS``), so the
# launch uses exactly the data the operator just generated. Runs are only ever
# triggered manually, so an in-progress run at launch time is a rare, deliberate
# choice and the ~10-minute wait is acceptable. Non-launch constructions (e.g.
# the single-ID re-staging fan-out) don't wait — they hold the last good list.
#
# INVARIANT: the remote source can only ADD IDs, never remove them. The banlist
# never shrinks below the committed file, so a Quarry outage, error page, empty
# or still-running run, or garbage response can at worst degrade to today's
# file-only behavior — never to a smaller list that would resume re-uploading
# deleted files (the exact behavior that gets the bot blocked on Commons).
QUARRY_QUERY_ID = 90099
QUARRY_META_URL = f"https://quarry.wmcloud.org/query/{QUARRY_QUERY_ID}/meta"
QUARRY_RUN_OUTPUT_URL = "https://quarry.wmcloud.org/run/{run_id}/output/0/json-lines"

# A DPLA ID is a 32-char lowercase-hex MD5. The Quarry feed scrapes the ID out
# of Commons file titles server-side, so anything that doesn't match is treated
# as noise and dropped rather than trusted into the banlist.
#
# Deliberately stricter than ``partners.is_dpla_id`` (which is case-insensitive):
# committed banlist IDs and Quarry output are always lowercase, and anything
# else in the feed is noise we'd rather drop than normalize.
_DPLA_ID_RE = re.compile(r"^[0-9a-f]{32}$")

REMOTE_FETCH_TIMEOUT = 30  # seconds per HTTP request
CACHE_TTL_SECONDS = 6 * 60 * 60  # 6 hours

# When a launch encounters an in-progress run, how long to block waiting for it
# to complete before giving up and falling back to the last good list. Sized
# above the observed ~9-minute run time with margin for replag.
RUN_WAIT_TIMEOUT_SECONDS = 15 * 60
RUN_WAIT_POLL_SECONDS = 15

# Cache the fetched IDs to a stable local path so the single-ID re-staging
# fan-out (``resolve_dpla_ids`` -> many ``get-ids-es --single-id`` processes)
# reads the cache instead of hitting Quarry once per ID. One fetch per TTL
# window across every invocation on the box.
CACHE_PATH = Path(tempfile.gettempdir()) / "ingest_wikimedia_banlist_remote.txt"


def _remote_enabled() -> bool:
    """Whether to union the Quarry feed into the banlist.

    On by default; set ``INGEST_WIKIMEDIA_BANLIST_REMOTE`` to a falsey value
    (``0``/``false``/``no``/``off``) to pin the banlist to the committed file
    only (e.g. if Quarry is misbehaving).
    """
    val = os.environ.get("INGEST_WIKIMEDIA_BANLIST_REMOTE", "1").strip().lower()
    return val not in ("0", "false", "no", "off")


def _parse_ids(text: str) -> set[str]:
    """Extract valid DPLA IDs from Quarry JSON-lines output.

    Each line is a JSON object with a ``DPLA_id`` key. Lines that aren't valid
    JSON, lack the key, or carry an ID that isn't a 32-char hex hash are
    skipped — an HTML error page or partial response yields an empty set rather
    than junk IDs.
    """
    ids: set[str] = set()
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        dpla_id = record.get("DPLA_id", "") if isinstance(record, dict) else ""
        if isinstance(dpla_id, str) and _DPLA_ID_RE.match(dpla_id):
            ids.add(dpla_id)
    return ids


def _read_cache() -> set[str] | None:
    """Return cached remote IDs, or ``None`` if the cache is absent/unreadable.

    IDs are re-validated on read (same gate as :func:`_parse_ids`) so a stray or
    corrupt temp file at the shared ``CACHE_PATH`` can't inject non-ID junk into
    the banlist.
    """
    try:
        return {
            line
            for raw in CACHE_PATH.read_text().splitlines()
            if _DPLA_ID_RE.match(line := raw.strip())
        }
    except OSError:
        return None


def _cache_is_fresh() -> bool:
    try:
        age = time.time() - CACHE_PATH.stat().st_mtime
    except OSError:
        return False
    return age < CACHE_TTL_SECONDS


def _write_cache(ids: set[str]) -> None:
    # Write to a per-process temp file in the same directory, then atomically
    # rename into place. CACHE_PATH is shared across the concurrent single-ID
    # fan-out, so a plain truncate-then-write could let a reader catch a
    # half-written file; os.replace makes the swap atomic on the same fs.
    tmp = CACHE_PATH.with_name(f"{CACHE_PATH.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text("\n".join(sorted(ids)) + "\n")
        tmp.replace(CACHE_PATH)
    except OSError as e:
        logging.warning("Banlist: could not write remote cache %s: %s", CACHE_PATH, e)
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def _await_complete_run(headers: dict[str, str], wait_for_run: bool) -> int:
    """Return the id of the latest *complete* run of the Quarry query.

    Only a completed run has a full, trustworthy output; a queued/running run's
    output is absent or partial. If ``wait_for_run`` and the latest run is still
    in progress, poll the ``meta`` endpoint until it completes or
    ``RUN_WAIT_TIMEOUT_SECONDS`` elapses. Raises ``ValueError`` if no usable
    complete run is available (not waiting, or the wait timed out).
    """
    deadline = time.monotonic() + RUN_WAIT_TIMEOUT_SECONDS
    waited = False
    while True:
        meta = requests.get(
            QUARRY_META_URL, timeout=REMOTE_FETCH_TIMEOUT, headers=headers
        )
        meta.raise_for_status()
        latest_run = meta.json().get("latest_run") or {}
        status = latest_run.get("status")
        run_id = latest_run.get("id")

        if status == "complete" and isinstance(run_id, int):
            if waited:
                logging.info("Banlist: Quarry run %s complete; proceeding.", run_id)
            return run_id

        # Not complete. Wait only if asked to and there's time left; an
        # in-progress run is the case worth waiting for (someone just kicked
        # off a refresh), but any non-complete status is handled the same way.
        if not wait_for_run or time.monotonic() >= deadline:
            raise ValueError(f"latest Quarry run not usable (status={status!r})")

        if not waited:
            logging.warning(
                "Banlist: Quarry query %s has an in-progress run (status=%s); "
                "blocking up to %ds for it to complete before launch.",
                QUARRY_QUERY_ID,
                status,
                RUN_WAIT_TIMEOUT_SECONDS,
            )
            waited = True
        time.sleep(RUN_WAIT_POLL_SECONDS)


def _fetch_remote_ids(wait_for_run: bool = False) -> set[str]:
    """Return the current set of banned DPLA IDs from Quarry.

    Fail-safe by construction: returns ``set()`` (or a stale cache) on any
    failure. The caller unions the result with the committed file, so an empty
    return is harmless — it just means "add nothing beyond the file".

    ``wait_for_run`` (set by launch entrypoints) blocks on an in-progress run;
    see :func:`_await_complete_run`. A fresh cache is only trusted when NOT
    waiting — a launch wants to observe the current run state, not a stale
    snapshot from before the operator's manual refresh.
    """
    if not wait_for_run and _cache_is_fresh():
        cached = _read_cache()
        if cached is not None:
            return cached
    try:
        headers = {"User-Agent": USER_AGENT}
        run_id = _await_complete_run(headers, wait_for_run)
        response = requests.get(
            QUARRY_RUN_OUTPUT_URL.format(run_id=run_id),
            timeout=REMOTE_FETCH_TIMEOUT,
            headers=headers,
        )
        response.raise_for_status()
        ids = _parse_ids(response.text)
        if not ids:
            # Zero valid IDs from a "complete" run almost always means Quarry
            # served an error/HTML page rather than that nothing is banned.
            # Treat as failure and fall back rather than trusting the emptiness.
            raise ValueError("Quarry run produced no valid DPLA IDs")
        _write_cache(ids)
        return ids
    except (requests.RequestException, ValueError) as e:
        # ValueError also covers JSON decode errors (requests.JSONDecodeError
        # subclasses it), so a non-JSON meta response falls back cleanly too.
        stale = _read_cache()
        logging.warning(
            "Banlist: remote fetch from Quarry failed (%s); falling back to %s.",
            e,
            "stale cache" if stale is not None else "committed file only",
        )
        return stale if stale is not None else set()


class Banlist:
    def __init__(
        self, fetch_remote: bool | None = None, wait_for_run: bool = False
    ) -> None:
        """Load the banlist.

        ``fetch_remote`` defaults to :func:`_remote_enabled` (env-controlled);
        pass ``False`` to load only the committed file (used by tests to avoid
        network access).

        ``wait_for_run`` should be ``True`` for batch-launch entrypoints: if a
        Quarry run is in progress, block until it completes so the launch uses
        the freshly-generated list. Leave ``False`` everywhere else (e.g. the
        single-ID re-staging fan-out), which then holds the last good list.
        """
        banlist_path = Path(__file__).parent.parent / BANLIST_FILE_NAME
        with open(banlist_path, "r") as file:
            ids = {line.rstrip() for line in file if line.strip()}

        if fetch_remote is None:
            fetch_remote = _remote_enabled()
        if fetch_remote:
            ids |= _fetch_remote_ids(wait_for_run=wait_for_run)

        self.dpla_id_banlist = ids

    def is_banned(self, dpla_id: str) -> bool:
        """
        Checks if the given DPLA ID is in the banlist.
        """
        return dpla_id in self.dpla_id_banlist
