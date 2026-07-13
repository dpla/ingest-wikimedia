"""Persistent "await community-target free" set.

Case-2 hash drift: our S3 source's SHA1 already lives at a
community-authored Commons title. Rather than tag the community's file
for deletion, the uploader uploads our bytes to the DPLA-canonical
title, tags OUR file ``{{Duplicate|<community title>}}``, and records
the ``(dpla_id, ordinal)`` here. A Commons admin then deletes or
redirects our tagged file; once they do, the DPLA-canonical title is
free and a plain **re-run of the uploader** resolves it through the
existing title-drift machinery (empty canonical → Case-3 move of the
community file into the freed title, preserving its history). This set
exists only so the ``drain-deferred`` phase knows which items to keep
re-running while we wait on the admin, and so the wait doesn't block the
batch.

Design note — why this is just a set of keys, not a rich record:
everything the resolution needs (the community title, the source SHA1,
whether the tag is still pending) is re-derived from live Commons / S3
state on each uploader re-run. The uploader is idempotent, so the drain
is nothing more than "re-run the uploader on these IDs until they stop
needing it" — the same pattern as :mod:`ingest_wikimedia.drain_sidecar`.
There is no per-item state machine to keep crash-consistent. A lost set
degrades gracefully: the tagged files still exist on Commons, and any
future full partner run re-detects and resolves them (an admin deletion
becomes an ordinary empty-canonical Case-3 move); the set only makes the
polling prompt.

Entries are ``"<dpla_id>\\t<ordinal>"`` strings. Per-partner scope,
stored at ``<partner>/await-target-free.json`` under
:data:`~ingest_wikimedia.drain_sidecar.INGEST_WIKI_ROOT`, alongside the
deferred-drain sidecar.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import os
import tempfile
from pathlib import Path

from ingest_wikimedia.drain_sidecar import partner_dir_path

SIDECAR_FILENAME = "await-target-free.json"
_LOCK_SUFFIX = ".lock"
_KEY_SEP = "\t"


def sidecar_path(partner: str) -> Path:
    """Absolute path to the await-target-free set for ``partner``.

    Same anchor + partner-dir resolution as :mod:`drain_sidecar`, so the
    two sidecars sit alongside each other under the partner directory.
    """
    return partner_dir_path(partner) / SIDECAR_FILENAME


def _key(dpla_id: str, ordinal: int) -> str:
    return f"{dpla_id}{_KEY_SEP}{ordinal}"


@contextlib.contextmanager
def _locked_for_write(partner: str):
    """Hold an exclusive ``fcntl.flock`` on a companion lockfile for the
    duration of the block.

    The uploader's ``multiprocessing.Pool`` fans work out to worker
    processes that each call :func:`add_key` / :func:`remove_key`
    concurrently; without this lock the read-modify-write races and a
    worker's update can clobber a sibling's. The lockfile is a separate
    path (``<sidecar>.lock``) because :func:`_write_keys` unlinks the
    sidecar on empty, which would drop a lock held on the sidecar itself.
    ``flock`` releases on fd close, so a killed worker never strands it.
    """
    path = sidecar_path(partner)
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + _LOCK_SUFFIX)
    fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def read_keys(partner: str) -> list[str]:
    """Return the queued ``"<dpla_id>\\t<ordinal>"`` keys, or an empty
    list if the file is missing or unreadable.

    Missing is the normal empty state. A present-but-unparseable file is
    treated as empty (like :func:`drain_sidecar.read_sidecar`): the set
    is reconstructable from Commons, so a mid-write crash mustn't wedge
    the drain — the operator can inspect the file if it lingers.
    """
    path = sidecar_path(partner)
    if not path.exists():
        return []
    try:
        with open(path) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as ex:
        logging.warning(
            "await-target-free set at %s is unreadable (%s); treating as empty",
            path,
            ex,
        )
        return []
    keys = data.get("awaiting") if isinstance(data, dict) else None
    if not isinstance(keys, list):
        return []
    return [k for k in keys if isinstance(k, str) and _KEY_SEP in k]


def _write_keys(partner: str, keys: list[str]) -> None:
    """Overwrite the set with ``keys`` (deduped, order-preserving);
    remove the file when empty so the empty state is unambiguous. Atomic
    via tempfile + ``os.replace``. Caller holds :func:`_locked_for_write`.
    """
    path = sidecar_path(partner)
    seen: set[str] = set()
    ordered: list[str] = []
    for k in keys:
        if isinstance(k, str) and _KEY_SEP in k and k not in seen:
            seen.add(k)
            ordered.append(k)
    if not ordered:
        try:
            path.unlink(missing_ok=True)
        except OSError as ex:
            logging.warning(
                "failed to remove empty await-target-free set at %s: %s", path, ex
            )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"partner": partner, "awaiting": ordered}
    tempname: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            dir=str(path.parent),
            prefix=".await-target-free-",
            suffix=".tmp",
            delete=False,
        ) as tf:
            tempname = tf.name
            json.dump(payload, tf, indent=2)
            tf.write("\n")
        os.replace(tempname, path)
    except Exception:
        if tempname is not None:
            with contextlib.suppress(OSError):
                os.unlink(tempname)
        raise


def has_key(partner: str, dpla_id: str, ordinal: int) -> bool:
    """True iff ``(dpla_id, ordinal)`` is currently awaiting admin action."""
    return _key(dpla_id, ordinal) in read_keys(partner)


def add_key(partner: str, dpla_id: str, ordinal: int) -> None:
    """Record that ``(dpla_id, ordinal)`` is awaiting admin action.
    Idempotent; serialized across processes via :func:`_locked_for_write`.
    """
    with _locked_for_write(partner):
        keys = read_keys(partner)
        k = _key(dpla_id, ordinal)
        if k not in keys:
            keys.append(k)
            _write_keys(partner, keys)


def remove_key(partner: str, dpla_id: str, ordinal: int) -> None:
    """Drop ``(dpla_id, ordinal)`` from the set (no-op if absent).
    Serialized across processes via :func:`_locked_for_write`.
    """
    with _locked_for_write(partner):
        k = _key(dpla_id, ordinal)
        keys = read_keys(partner)
        if k in keys:
            _write_keys(partner, [x for x in keys if x != k])


def awaiting_dpla_ids(partner: str) -> list[str]:
    """Return the unique DPLA IDs with at least one awaiting ordinal, in
    first-seen order. The drain re-runs the uploader per DPLA ID (its
    unit of work is the item), so it dedupes the per-ordinal keys here.
    """
    seen: set[str] = set()
    ordered: list[str] = []
    for k in read_keys(partner):
        dpla_id = k.split(_KEY_SEP, 1)[0]
        if dpla_id not in seen:
            seen.add(dpla_id)
            ordered.append(dpla_id)
    return ordered
