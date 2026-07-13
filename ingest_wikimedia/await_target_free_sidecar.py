"""Persistent sidecar for the "await community-target free" deferral stage.

The uploader defers a Case-2 hash-drift resolution when the file whose title
we intend to overwrite is a community upload (i.e. our S3 SHA1 already lives at
a community-authored Commons title). Rather than tag the community's file for
speedy deletion — the previous behaviour, which requested a Commons admin
destroy an older community contribution — the uploader:

  1. uploads the S3 bytes to the DPLA-canonical title (invariant satisfied),
  2. tags OUR just-uploaded file as ``{{Duplicate|<community title>}}`` so
     Commons admins can delete or redirect it, and
  3. writes an entry to this sidecar naming the tagged (DPLA-canonical) file
     and the community file we want to promote once the tagged file is gone.

The follow-on ``drain-deferred`` phase reads this sidecar, polls each tagged
title, and — once the tag has been actioned (page deleted, or turned into a
redirect) — executes a title-drift move of the community file into the freed
DPLA-canonical title. The community file's revision history and curation
survive the move; the invariant is satisfied again once the move completes.

Distinct from :mod:`ingest_wikimedia.drain_sidecar` (the ``Category:Duplicate``
capacity gate), because the wait conditions are per-item (each tagged title
polled independently), not global (single boolean checked once per round).
Keeping the two sidecars separate is additive — no schema evolution on the
existing sidecar — and lets each stage evolve without disturbing the other.

Per-partner scope: mirrors :mod:`drain_sidecar` — one sidecar file per partner
under ``<partner>/await-target-free.json``, anchored at
:data:`~ingest_wikimedia.drain_sidecar.INGEST_WIKI_ROOT`.

Entry schema::

    {
        "dpla_id":         "22412cd0994d36f03c4fcf549db2b8e5",
        "ordinal":         1,
        "tagged_title":    "File:… - DPLA - 22412cd0….jpg",
        "community_title": "File:….jpg",
        "expected_sha1":   "9719e05a…",
    }

``expected_sha1`` is the S3 source SHA1 that was true at tag time; the drain
phase re-validates it against the community file before executing the move so
that partner-side content churn during the wait window is caught (and doesn't
silently promote stale bytes into the canonical title).
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import tempfile
from pathlib import Path

from ingest_wikimedia.drain_sidecar import partner_dir_path

SIDECAR_FILENAME = "await-target-free.json"


def sidecar_path(partner: str) -> Path:
    """Absolute path to the await-target-free sidecar for ``partner``.

    Same anchor + partner-dir resolution as :mod:`drain_sidecar` so the
    two sidecars sit alongside each other under the partner working
    directory.
    """
    return partner_dir_path(partner) / SIDECAR_FILENAME


def _normalize_entry(raw: object) -> dict | None:
    """Return ``raw`` as a validated entry dict, or ``None`` if it's
    malformed. Guards against corrupt sidecar contents (a stray non-dict
    element, missing required keys) rather than crashing the drain loop.
    """
    if not isinstance(raw, dict):
        return None
    dpla_id = raw.get("dpla_id")
    tagged_title = raw.get("tagged_title")
    community_title = raw.get("community_title")
    if not (
        isinstance(dpla_id, str)
        and isinstance(tagged_title, str)
        and isinstance(community_title, str)
    ):
        return None
    ordinal = raw.get("ordinal")
    if not isinstance(ordinal, int):
        return None
    expected_sha1 = raw.get("expected_sha1")
    if not isinstance(expected_sha1, str):
        return None
    return {
        "dpla_id": dpla_id,
        "ordinal": ordinal,
        "tagged_title": tagged_title,
        "community_title": community_title,
        "expected_sha1": expected_sha1,
    }


def read_sidecar(partner: str) -> list[dict]:
    """Return the list of pending entries in the sidecar, or an empty
    list if the file is missing / unreadable / malformed. Missing sidecar
    is the normal empty state.
    """
    path = sidecar_path(partner)
    if not path.exists():
        return []
    try:
        with open(path) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as ex:
        logging.warning(
            "await-target-free sidecar at %s is unreadable (%s); treating as empty",
            path,
            ex,
        )
        return []
    entries = data.get("entries") if isinstance(data, dict) else None
    if not isinstance(entries, list):
        return []
    out: list[dict] = []
    for raw in entries:
        normalized = _normalize_entry(raw)
        if normalized is not None:
            out.append(normalized)
    return out


def write_sidecar(partner: str, entries: list[dict]) -> None:
    """Overwrite the sidecar with ``entries``. Removes the file when the
    list is empty so an empty state is unambiguous (missing = nothing
    pending). Deduplicates by ``(dpla_id, ordinal)`` preserving order.

    Atomic write via tempfile + ``os.replace`` — matches
    :func:`drain_sidecar.write_sidecar`'s guarantees.
    """
    path = sidecar_path(partner)
    if not entries:
        try:
            path.unlink(missing_ok=True)
        except OSError as ex:
            logging.warning(
                "failed to remove empty await-target-free sidecar at %s: %s",
                path,
                ex,
            )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    seen: set[tuple[str, int]] = set()
    ordered: list[dict] = []
    for entry in entries:
        normalized = _normalize_entry(entry)
        if normalized is None:
            continue
        key = (normalized["dpla_id"], normalized["ordinal"])
        if key not in seen:
            seen.add(key)
            ordered.append(normalized)
    payload = {"partner": partner, "entries": ordered}
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


def has_entry(partner: str, dpla_id: str, ordinal: int) -> bool:
    """True iff an entry for ``(dpla_id, ordinal)`` is currently queued.

    Idempotency guard: the uploader consults this before enqueuing a new
    entry so a re-run of the same partner (before the tagged file is
    actioned by a Commons admin) doesn't re-emit the tag or duplicate the
    sidecar entry.
    """
    for entry in read_sidecar(partner):
        if entry["dpla_id"] == dpla_id and entry["ordinal"] == ordinal:
            return True
    return False


def add_entry(partner: str, entry: dict) -> list[dict]:
    """Add ``entry`` to the sidecar, deduping by ``(dpla_id, ordinal)``,
    and return the resulting combined list. If an entry for that key
    already exists, the sidecar is left unchanged.
    """
    normalized = _normalize_entry(entry)
    if normalized is None:
        raise ValueError(f"malformed entry rejected: {entry!r}")
    existing = read_sidecar(partner)
    key = (normalized["dpla_id"], normalized["ordinal"])
    if any((e["dpla_id"], e["ordinal"]) == key for e in existing):
        return existing
    combined = [*existing, normalized]
    write_sidecar(partner, combined)
    return combined


def remove_entry(partner: str, dpla_id: str, ordinal: int) -> list[dict]:
    """Drop the entry keyed by ``(dpla_id, ordinal)`` from the sidecar
    and return the remaining list. Absent entries are a no-op.
    """
    remaining = [
        e
        for e in read_sidecar(partner)
        if not (e["dpla_id"] == dpla_id and e["ordinal"] == ordinal)
    ]
    write_sidecar(partner, remaining)
    return remaining
