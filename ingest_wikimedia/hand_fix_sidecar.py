"""Per-partner ``hand-fix.jsonl`` sidecar for the SHA1-uniqueness redesign.

When the uploader finds our S3 source's SHA1 already on Commons at a WRONG
title and the canonical title we need is occupied by a DIFFERENT file (a
different SHA1), the rename that would restore the upload invariant is
blocked. Under the one-SHA1-one-file constraint the uploader must NOT upload
a second byte-identical copy, and it cannot pick a winner between two
distinct files, so it hands the case off to a human: it appends a descriptive
record here and moves on (counted as :class:`Result.UPLOAD_HAND_FIX`).

This mirrors :class:`Result.MAINTAIN_RENAME_BLOCKED` (maintain mode's
equivalent "the bot can't safely make this rename" outcome), but persists the
full context needed to resolve it rather than only counting it.

Format: newline-delimited JSON (JSONL), one object per blocked ordinal, so
the file can be appended to across many items in a run and read back one
record at a time. Location: ``<partner>/hand-fix.jsonl`` under
:data:`ingest_wikimedia.partners.INGEST_WIKI_ROOT`, alongside the other
partner working files.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from ingest_wikimedia.partners import partner_dir_path

SIDECAR_FILENAME = "hand-fix.jsonl"

# The descriptive fields every record carries. Kept explicit so the schema is
# self-documenting and a reader (human or a future resolver tool) knows what
# to expect.
_FIELDS = (
    "dpla_id",
    "ordinal",
    "our_sha1",
    "intended_title",
    "occupying_title",
    "occupying_sha1",
    "partner",
)


def sidecar_path(partner: str) -> Path:
    """Absolute path to the hand-fix sidecar for ``partner``."""
    return partner_dir_path(partner) / SIDECAR_FILENAME


def record_hand_fix(
    partner: str,
    *,
    dpla_id: str,
    ordinal: int,
    our_sha1: str,
    intended_title: str,
    occupying_title: str | None,
    occupying_sha1: str | None,
    **extra,
) -> None:
    """Append one hand-fix record to ``<partner>/hand-fix.jsonl``.

    Best-effort: a filesystem error here must never abort the upload run —
    the ordinal is already accounted for by the caller's tracker increment,
    and the same case re-detects and re-records on a future run. Any extra
    keyword fields (e.g. the current wrong-title location of our SHA1) are
    included verbatim so the record can carry more than the minimum schema.
    """
    record = {
        "dpla_id": dpla_id,
        "ordinal": ordinal,
        "our_sha1": our_sha1,
        "intended_title": intended_title,
        "occupying_title": occupying_title,
        "occupying_sha1": occupying_sha1,
        "partner": partner,
        **extra,
    }
    path = sidecar_path(partner)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a") as f:
            f.write(json.dumps(record) + "\n")
    except OSError as ex:
        logging.warning(
            "failed to append hand-fix record for %s ordinal %s to %s: %s",
            dpla_id,
            ordinal,
            path,
            ex,
        )


def count(partner: str) -> int:
    """Number of records currently in the partner's hand-fix sidecar.

    Used for the Slack run-summary tally. Missing file → 0; an unreadable
    file is treated as 0 rather than raised (the tally is informational)."""
    path = sidecar_path(partner)
    if not path.exists():
        return 0
    try:
        with open(path) as f:
            return sum(1 for line in f if line.strip())
    except OSError as ex:
        logging.warning("failed to read hand-fix sidecar %s: %s", path, ex)
        return 0
