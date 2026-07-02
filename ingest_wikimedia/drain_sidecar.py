"""Persistent sidecar for the deferred-drain queue.

The uploader defers a Case-2 hash-drift upload+tag as a unit when
Category:Duplicate is at capacity. Rather than blocking the whole
session (and downstream sdc-sync) waiting for volunteers to clear the
category on human-admin timescales, the uploader writes the deferred
DPLA IDs to this sidecar and exits normally. A subsequent
``drain-deferred`` phase reads the sidecar, waits patiently for
Category:Duplicate to drop below its resume threshold, re-invokes the
uploader + sdc-sync on just those IDs, and loops until the sidecar is
empty.

Per-partner scope: the tmux session name is ``wikimedia-<partner>``, so
only one session per partner runs at a time; a single per-partner
sidecar is sufficient and unambiguous. Location:
``<partner>/deferred-drain.json``.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path

SIDECAR_FILENAME = "deferred-drain.json"


def sidecar_path(partner: str) -> Path:
    """Absolute path to the deferred-drain sidecar for ``partner``.

    Uses the same per-partner working directory the uploader / sdc-sync
    CLI commands run from (their CWD when launched by the tmux
    pipeline). Callers may pass either the canonical partner slug
    (``nara``) or a directory-name variant (``smithsonian`` for the ``si``
    slug); resolution belongs on the caller side.
    """
    return Path(partner) / SIDECAR_FILENAME


def read_sidecar(partner: str) -> list[str]:
    """Return the list of DPLA IDs currently in the sidecar, or an
    empty list if the file is missing or unreadable.

    Missing sidecar is the normal empty state — a fresh partner run with
    no deferrals, or a drain that completed and cleaned up.  A file
    present but unparseable is treated the same as missing to keep the
    drain loop resilient against a mid-write crash; the operator would
    see the corrupt file and can inspect it manually if needed.
    """
    path = sidecar_path(partner)
    if not path.exists():
        return []
    try:
        with open(path) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as ex:
        logging.warning(
            "deferred-drain sidecar at %s is unreadable (%s); treating as empty",
            path,
            ex,
        )
        return []
    ids = data.get("deferred_dpla_ids") if isinstance(data, dict) else None
    if not isinstance(ids, list):
        return []
    return [x for x in ids if isinstance(x, str)]


def write_sidecar(partner: str, dpla_ids: list[str]) -> None:
    """Overwrite the sidecar with ``dpla_ids``.  Removes the file when
    the list is empty so an empty state is unambiguous (missing =
    nothing to drain).

    Atomic write via ``tempfile.NamedTemporaryFile`` + ``os.replace`` so a
    crash mid-write can't leave a truncated file. A drain phase reading
    the sidecar between our tempfile creation and the rename sees the
    previous contents (or nothing) — never a partial write.
    """
    path = sidecar_path(partner)
    if not dpla_ids:
        try:
            path.unlink(missing_ok=True)
        except OSError as ex:
            logging.warning(
                "failed to remove empty deferred-drain sidecar at %s: %s", path, ex
            )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    # Dedup while preserving order — a partner re-run should not grow
    # the queue with repeat entries.
    seen: set[str] = set()
    ordered: list[str] = []
    for dpla_id in dpla_ids:
        if dpla_id not in seen:
            seen.add(dpla_id)
            ordered.append(dpla_id)
    payload = {"partner": partner, "deferred_dpla_ids": ordered}
    with tempfile.NamedTemporaryFile(
        "w",
        dir=str(path.parent),
        prefix=".deferred-drain-",
        suffix=".tmp",
        delete=False,
    ) as tf:
        json.dump(payload, tf, indent=2)
        tf.write("\n")
        tempname = tf.name
    os.replace(tempname, path)


def merge_sidecar(partner: str, new_dpla_ids: list[str]) -> list[str]:
    """Union ``new_dpla_ids`` into the sidecar and return the resulting
    combined list. If the sidecar didn't exist, this is a plain create.

    Merge (rather than overwrite) so a session that ran while a prior
    drain still had items queued doesn't lose the prior queue.  Only
    the drain phase removes items; the uploader only ever appends.
    """
    existing = read_sidecar(partner)
    seen: set[str] = set(existing)
    combined = list(existing)
    for dpla_id in new_dpla_ids:
        if dpla_id not in seen:
            seen.add(dpla_id)
            combined.append(dpla_id)
    write_sidecar(partner, combined)
    return combined
