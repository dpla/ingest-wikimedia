"""Outcome-level tests pinning the DPLA → Wikimedia upload invariant under
the SHA1-uniqueness constraint (PR C+D).

**Goal** (see ``docs/upload-invariant.md`` for the full statement): every
DPLA item's content is represented and discoverable at its expected Commons
title. **New constraint**: no two Commons files may share a SHA1. We satisfy
the goal by CENTRALIZING each SHA1 to one canonical file (the earliest
existing upload), which carries every contributing item's SDC, and
REDIRECTING the other expected titles to it — never by uploading a
byte-identical second file.

The tests below pin OUTCOMES against that model:

- Our SHA1 already at another live DPLA ID's title → MERGE_AND_REDIRECT
  (centralize onto the canonical file; redirect our title). NOT a second
  upload.
- Our SHA1 at a wrong title and the intended title occupied by a DIFFERENT
  file → HAND_FIX (recorded for a human; no upload, no clobber).
- Our SHA1 already at the intended title (modulo normalization) →
  ALREADY_CORRECT / skip.

If a test here fails, the change is almost certainly wrong. Read the
invariant document before "fixing" the test.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from ingest_wikimedia.tracker import Result
from tools.uploader import (
    ORDINAL_HAND_FIX,
    ORDINAL_MERGED,
    DriftResolution,
    Uploader,
)


# --------------------------------------------------------------------------
# Test helpers — build an Uploader with mockable Commons / S3 / DPLA sides
# so tests can drive it to specific pre-state and inspect outcomes.
# --------------------------------------------------------------------------


def _build_uploader(dpla_get_item_metadata=None) -> Uploader:
    """Uploader with mocked collaborators. ``dpla_get_item_metadata`` is
    the return value the mock DPLA client will yield for
    ``get_item_metadata`` calls — set to a truthy dict for "valid
    live DPLA item", to raise for "item no longer exists"."""
    dpla = MagicMock()
    if dpla_get_item_metadata is not None:
        dpla.get_item_metadata.return_value = dpla_get_item_metadata
    return Uploader(
        tracker=MagicMock(),
        local_fs=MagicMock(),
        s3_client=MagicMock(),
        dpla=dpla,
        site=MagicMock(),
        category_ensurer=None,
    )


def _make_existing_file(title: str, pageid: int = 12345) -> MagicMock:
    """Mock a Commons FilePage returned by ``find_file_by_hash``."""
    ef = MagicMock()
    ef.title.return_value = title
    ef.pageid = pageid
    return ef


# --------------------------------------------------------------------------
# Cross-item / cross-institution source duplication → MERGE_AND_REDIRECT
# --------------------------------------------------------------------------


def test_cross_item_collision_merges_and_redirects():
    """When our SHA1 already lives at ANOTHER live DPLA ID's canonical title,
    ``_resolve_hash_drift`` returns ``MERGE_AND_REDIRECT`` — the caller merges
    our item's SDC onto that (earliest/canonical) file and leaves a redirect
    at our intended title. Under the uniqueness constraint we must NOT upload
    a second byte-identical file (the old ``leave_others_alone`` upload)."""
    uploader = _build_uploader(dpla_get_item_metadata={"id": "other-item-live"})
    our_intended = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 1).jpg"
    other_dpla_id_title = "Item - DPLA - bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb (page 1).jpg"
    existing_file = _make_existing_file(other_dpla_id_title)

    with patch("tools.uploader.get_page"):
        action = uploader._resolve_hash_drift(
            existing_file=existing_file,
            page_title=our_intended,
            dpla_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ordinal=1,
        )

    assert action is DriftResolution.MERGE_AND_REDIRECT, (
        f"expected MERGE_AND_REDIRECT (centralize our SHA1 onto the other live "
        f"DPLA ID's canonical file, redirect our title); got {action!r}. A "
        f"second byte-identical upload would violate the one-SHA1-one-file "
        f"constraint. See docs/upload-invariant.md."
    )


def test_within_item_sibling_slot_merges_and_redirects():
    """When our SHA1 sits at one of THIS item's own current asset positions
    (a within-item duplicate) and the intended title is occupied by a real
    file, the resolver returns ``MERGE_AND_REDIRECT`` rather than renaming
    (which would strand the sibling ordinal) or uploading a duplicate."""
    uploader = _build_uploader()
    dpla_id = "cccccccccccccccccccccccccccccccc"
    our_intended = f"Item - DPLA - {dpla_id} (page 2).jpg"
    sibling_title = f"Item - DPLA - {dpla_id} (page 1).jpg"
    existing_file = _make_existing_file(sibling_title)

    occupant = MagicMock()
    occupant.exists.return_value = True
    occupant.isRedirectPage.return_value = False
    occupant.title.return_value = our_intended

    with patch("tools.uploader.get_page", return_value=occupant):
        action = uploader._resolve_hash_drift(
            existing_file=existing_file,
            page_title=our_intended,
            dpla_id=dpla_id,
            ordinal=2,
            expected_item_titles={sibling_title, our_intended},
        )

    assert action is DriftResolution.MERGE_AND_REDIRECT


# --------------------------------------------------------------------------
# Rename blocked → HAND_FIX
# --------------------------------------------------------------------------


def test_redirect_to_elsewhere_routes_to_hand_fix():
    """When our intended title is a ``#REDIRECT`` whose target is NOT the file
    holding our SHA1, the bot cannot cleanly rename our file into the intended
    title (a third file's redirect occupies it). ``_resolve_hash_drift``
    returns ``HAND_FIX`` — no upload, no clobber, recorded for a human."""
    uploader = _build_uploader()
    dpla_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    our_intended = f"Item - DPLA - {dpla_id} (page 1).jpg"
    our_current = f"Item - DPLA - {dpla_id} (page 9).jpg"  # same item, wrong title
    third_title = "Something - DPLA - dddddddddddddddddddddddddddddddd (page 1).jpg"

    existing_file = _make_existing_file(our_current)
    redirect_target = MagicMock()
    redirect_target.title.return_value = third_title
    intended_page = MagicMock()
    intended_page.exists.return_value = True
    intended_page.isRedirectPage.return_value = True
    intended_page.getRedirectTarget.return_value = redirect_target
    intended_page.title.return_value = our_intended

    with patch("tools.uploader.get_page", return_value=intended_page):
        action = uploader._resolve_hash_drift(
            existing_file=existing_file,
            page_title=our_intended,
            dpla_id=dpla_id,
            ordinal=1,
        )
    assert action is DriftResolution.HAND_FIX


def test_occupied_intended_title_routes_to_hand_fix():
    """Our SHA1 at a wrong title, intended title occupied by a DIFFERENT real
    file that is NOT one of this item's asset positions → HAND_FIX."""
    uploader = _build_uploader()
    dpla_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    our_intended = f"Item - DPLA - {dpla_id} (page 1).jpg"
    our_current = f"Legacy title - DPLA - {dpla_id}.jpg"

    existing_file = _make_existing_file(our_current)
    occupant = MagicMock()
    occupant.exists.return_value = True
    occupant.isRedirectPage.return_value = False
    occupant.title.return_value = our_intended

    with patch("tools.uploader.get_page", return_value=occupant):
        action = uploader._resolve_hash_drift(
            existing_file=existing_file,
            page_title=our_intended,
            dpla_id=dpla_id,
            ordinal=1,
            expected_item_titles={our_intended},  # only the intended title
        )
    assert action is DriftResolution.HAND_FIX


def test_unverifiable_cross_item_collision_routes_to_hand_fix():
    """A non-404 error verifying the colliding DPLA ID leaves the collision
    unverified — the resolver must NOT guess. It returns HAND_FIX rather than
    acting on a transient API blip."""
    uploader = _build_uploader()
    err = Exception("boom")
    err.response = MagicMock()
    err.response.status_code = 503
    uploader.dpla.get_item_metadata.side_effect = err

    other_title = "Other - DPLA - bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb (page 1).jpg"
    with patch("tools.uploader.get_page"):
        action = uploader._resolve_hash_drift(
            existing_file=_make_existing_file(other_title),
            page_title="Ours - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 1).jpg",
            dpla_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ordinal=1,
        )
    assert action is DriftResolution.HAND_FIX


# --------------------------------------------------------------------------
# Invariant already satisfied — normalized identity → ALREADY_CORRECT
# --------------------------------------------------------------------------


def test_normalized_identity_returns_already_correct():
    """When the SHA1 lookup returns the file at the intended title under
    whitespace normalisation (a post-``get_page_title`` truncation artifact),
    the goal is already met — resolver returns ``ALREADY_CORRECT``; caller
    records SKIPPED. No move, no merge, no upload."""
    uploader = _build_uploader()
    same_title = "Item - DPLA - cccccccccccccccccccccccccccccccc.gif"
    raw_page_title = "Item  - DPLA - cccccccccccccccccccccccccccccccc.gif"
    existing_file = _make_existing_file(same_title)

    with patch("tools.uploader.get_page"):
        action = uploader._resolve_hash_drift(
            existing_file=existing_file,
            page_title=raw_page_title,
            dpla_id="cccccccccccccccccccccccccccccccc",
            ordinal=1,
        )
    assert action is DriftResolution.ALREADY_CORRECT


# --------------------------------------------------------------------------
# The merge + redirect mechanics (rule #3 next step the resolver hands off)
# --------------------------------------------------------------------------


def test_merge_and_redirect_merges_sdc_and_creates_redirect():
    """``_merge_and_redirect`` (cross-item) resolves the canonical MediaInfo
    id from the canonical file's pageid, merges this item's staged SDC onto it
    via ``sdc_sync.merge_item_onto_canonical`` (page_numbers=None for
    cross-item), and leaves a ``#REDIRECT`` at the intended title. It returns
    an ``ORDINAL_MERGED`` result and counts UPLOAD_MERGED_TO_CANONICAL."""
    uploader = _build_uploader()
    uploader.s3_client.get_sdc_json.return_value = json.dumps(
        {"claims": [], "ingest_date": "2026-06-23"}
    )
    canonical = _make_existing_file(
        "Canon - DPLA - bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb (page 1).jpg", pageid=987
    )
    intended = "Ours - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 1).jpg"

    redirect_page = MagicMock()
    redirect_page.exists.return_value = False  # intended title is free
    redirect_page.title.return_value = f"File:{intended}"

    with (
        patch("tools.sdc_sync.merge_item_onto_canonical") as merge,
        patch("tools.uploader.get_page", return_value=redirect_page),
        patch("tools.uploader.with_csrf_recovery", side_effect=lambda s, d, fn: fn()),
    ):
        result = uploader._merge_and_redirect(
            canonical_file=canonical,
            intended_title=intended,
            dpla_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ordinal=1,
            partner="georgia",
            page_label="",
            within_item=False,
            sha1="a" * 40,
        )

    merge.assert_called_once()
    args, kwargs = merge.call_args
    assert args[0] == "M987"  # canonical mediaid from pageid
    assert args[1] == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert kwargs["page_numbers"] is None  # cross-item: no page number
    # Redirect wikitext written + saved at the intended title.
    assert redirect_page.text == (
        "#REDIRECT [[File:Canon - DPLA - "
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb (page 1).jpg]]"
    )
    redirect_page.save.assert_called_once()
    assert result["status"] == ORDINAL_MERGED
    assert result["title"] == canonical.title(with_ns=False)
    uploader.tracker.increment.assert_any_call(Result.UPLOAD_MERGED_TO_CANONICAL)


def test_merge_and_redirect_within_item_stamps_page_number():
    """Within-item duplication passes the ordinal's page number to the merge
    so it is stamped as a P304 qualifier on the canonical file."""
    uploader = _build_uploader()
    uploader.s3_client.get_sdc_json.return_value = json.dumps(
        {"claims": [], "ingest_date": "2026-06-23"}
    )
    canonical = _make_existing_file("Canon (page 1).jpg", pageid=55)
    redirect_page = MagicMock()
    redirect_page.exists.return_value = False

    with (
        patch("tools.sdc_sync.merge_item_onto_canonical") as merge,
        patch("tools.uploader.get_page", return_value=redirect_page),
        patch("tools.uploader.with_csrf_recovery", side_effect=lambda s, d, fn: fn()),
    ):
        uploader._merge_and_redirect(
            canonical_file=canonical,
            intended_title="Canon (page 2).jpg",
            dpla_id="dddddddddddddddddddddddddddddddd",
            ordinal=2,
            partner="georgia",
            page_label="2",
            within_item=True,
            sha1="d" * 40,
        )

    _, kwargs = merge.call_args
    assert kwargs["page_numbers"] == {"2"}


def test_create_redirect_refuses_to_clobber_real_file():
    """A real (non-redirect) file at the intended title is never overwritten
    with a redirect — that state is left for manual review."""
    uploader = _build_uploader()
    real_file = MagicMock()
    real_file.exists.return_value = True
    real_file.isRedirectPage.return_value = False

    with (
        patch("tools.uploader.get_page", return_value=real_file),
        patch("tools.uploader.with_csrf_recovery") as csrf,
    ):
        uploader._create_redirect_to_canonical(
            intended_title="Occupied.jpg",
            canonical_title="Canon.jpg",
            dpla_id="dddddddddddddddddddddddddddddddd",
            ordinal=1,
        )
    real_file.save.assert_not_called()
    csrf.assert_not_called()


# --------------------------------------------------------------------------
# Hand-fix recording
# --------------------------------------------------------------------------


def test_record_hand_fix_and_skip_writes_sidecar_and_returns_hand_fix():
    """``_record_hand_fix_and_skip`` writes a descriptive record to the
    per-partner hand-fix sidecar (never uploading) and returns an
    ``ORDINAL_HAND_FIX`` result with no title/pageid."""
    uploader = _build_uploader()
    our_current = _make_existing_file("Wrong - DPLA - id0 (page 9).jpg")
    occupant = MagicMock()
    occupant.exists.return_value = True
    occupant.isRedirectPage.return_value = False
    occupant.title.return_value = "Item - DPLA - id0 (page 1).jpg"
    occupant.latest_file_info.sha1 = "deadbeef"

    with (
        patch("tools.uploader.get_page", return_value=occupant),
        patch("tools.uploader.hand_fix_sidecar.record_hand_fix") as record,
    ):
        result = uploader._record_hand_fix_and_skip(
            partner="georgia",
            dpla_id="id0",
            ordinal=1,
            our_sha1="cafef00d",
            intended_title="Item - DPLA - id0 (page 1).jpg",
            our_current_file=our_current,
        )

    record.assert_called_once()
    _, kwargs = record.call_args
    assert kwargs["dpla_id"] == "id0"
    assert kwargs["our_sha1"] == "cafef00d"
    assert kwargs["occupying_sha1"] == "deadbeef"
    assert result == {"status": ORDINAL_HAND_FIX, "title": None, "pageid": None}
    uploader.tracker.increment.assert_any_call(Result.UPLOAD_HAND_FIX)


# --------------------------------------------------------------------------
# Sentinel contract pin
# --------------------------------------------------------------------------


def test_drift_resolution_sentinel_contract_pin():
    """Pin the ``DriftResolution`` enum's members + string values so a rename
    is a hard error. The retired ``UPLOAD_AND_TAG`` /
    ``UPLOAD_AND_SELF_TAG_DEFER`` / ``LEAVE_OTHERS_ALONE`` members must be
    gone; the SHA1-uniqueness members present."""
    members = {m.name: m.value for m in DriftResolution}
    assert members == {
        "MOVED": "moved",
        "MERGE_AND_REDIRECT": "merge_and_redirect",
        "HAND_FIX": "hand_fix",
        "ALREADY_CORRECT": "already_correct",
    }
    # ``str, Enum`` subclass preserves string-ness for legacy comparisons.
    assert isinstance(DriftResolution.MERGE_AND_REDIRECT, str)
    assert DriftResolution.HAND_FIX == "hand_fix"
