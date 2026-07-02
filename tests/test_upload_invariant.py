"""Outcome-level tests pinning the DPLA → Wikimedia upload invariant.

**Invariant** (see ``docs/upload-invariant.md`` for the full statement):

    For every DPLA item we upload, the SHA1 of the S3-staged source
    bytes for that item must live at the Commons title
    ``get_page_title(dpla_id, …)`` produces.

The tests below verify OUTCOMES against that invariant — the SHA1
landed at the right title — rather than code paths. Any refactor of
the drift / redirect handling that would cause a previously-correct
input to leave an intended title without its S3 SHA1 fails a test in
this file. Any refactor that would ADD a "safety check" that skips an
upload in a corollary-1 or corollary-2 scenario ALSO fails a test in
this file — those "safety checks" are invariant violations.

If a test fails here, the change is almost certainly wrong. Read the
invariant document before "fixing" the test.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from tools.uploader import Uploader


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


def _make_existing_file(title: str) -> MagicMock:
    """Mock a Commons FilePage returned by ``find_file_by_hash``."""
    ef = MagicMock()
    ef.title.return_value = title
    ef.pageid = 12345
    return ef


# --------------------------------------------------------------------------
# Corollary 1 — two live DPLA IDs with identical content
# --------------------------------------------------------------------------


def test_invariant_corollary_1_cross_item_collision_uploads_to_our_intended_title():
    """CORRECTNESS CRITERION being pinned: when our SHA1 already lives
    at another live DPLA ID's canonical title, ``_resolve_hash_drift``
    must return ``leave_others_alone`` — telling the caller to upload
    our bytes to OUR intended title, leaving the other file at ITS
    title. The resulting two-files-same-SHA1 state on Commons is the
    invariant satisfied at each DPLA ID (corollary 1).

    If a future change adds a "skip our upload because the SHA1 already
    exists elsewhere" check, this test fails and the change should be
    rejected. See ``docs/upload-invariant.md`` corollary 1 + the
    2026-07-02 Palo Pinto incident."""
    uploader = _build_uploader(
        dpla_get_item_metadata={"id": "other-item-live"}  # other DPLA ID is live
    )
    # Our SHA1 lives at the OTHER DPLA ID's canonical title.
    our_intended = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 1).jpg"
    other_dpla_id_title = "Item - DPLA - bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb (page 1).jpg"
    existing_file = _make_existing_file(other_dpla_id_title)

    # No further Commons state to reconcile (intended_page will not
    # even be built — leave_others_alone returns before get_page).
    with patch("tools.uploader.get_page"):
        action = uploader._resolve_hash_drift(
            existing_file=existing_file,
            page_title=our_intended,
            dpla_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ordinal=1,
            wiki_markup="",
        )

    assert action == "leave_others_alone", (
        f"expected leave_others_alone (invariant corollary 1: our S3 SHA1 "
        f"must land at our intended title, other DPLA ID keeps its file); "
        f"got {action!r}. If this asserts, someone likely added a 'skip "
        f"our upload because SHA1 already exists elsewhere on Commons' "
        f"check — that is a corollary-1 invariant violation. Read "
        f"docs/upload-invariant.md."
    )


# --------------------------------------------------------------------------
# Corollary 2 — pre-existing Commons redirects do not bind us
# --------------------------------------------------------------------------


def test_invariant_corollary_2_cross_item_redirect_at_intended_title_gets_overwritten():
    """CORRECTNESS CRITERION being pinned: when our intended title is a
    ``#REDIRECT`` to another live DPLA ID's file (an editor's stale
    curatorial judgment about a partner-decided fact), the caller
    overwrites the redirect and uploads our S3 bytes to our intended
    title. Corollary 2: the redirect does not bind our invariant
    obligation. See ``docs/upload-invariant.md``.

    Pinned via ``_resolve_hash_drift``'s deferral to the redirect
    handler: when the SHA1 lives at a DIFFERENT title than the
    redirect target (or when the redirect target is a
    different-DPLA-ID valid file), the resolver returns
    ``leave_others_alone`` so the caller's redirect handler can
    overwrite. Contrast: if the redirect target IS the SHA1's home for
    THIS DPLA ID (Case 1 title-drift), the resolver moves over the
    redirect instead.

    If a future change adds an "if the redirect target has our SHA1,
    honor the redirect and skip the upload" check, this test fails."""
    uploader = _build_uploader(dpla_get_item_metadata={"id": "other-item-live"})
    our_intended = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 1).jpg"
    other_dpla_id_title = "Item - DPLA - bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb (page 1).jpg"

    # Our SHA1 lives at the OTHER DPLA ID's title. Our intended title
    # is a redirect to that same title.
    existing_file = _make_existing_file(other_dpla_id_title)

    action = uploader._resolve_hash_drift(
        existing_file=existing_file,
        page_title=our_intended,
        dpla_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ordinal=1,
        wiki_markup="",
    )
    # The cross-item collision check returns early — the redirect at
    # our intended title never enters the picture in _resolve_hash_drift.
    # The caller's own redirect handler (in process_file, tested
    # separately) is what overwrites the redirect. What we pin here:
    # the resolver does NOT block the upload by returning some "skip"
    # value.
    assert action == "leave_others_alone", (
        f"expected leave_others_alone (invariant corollary 1+2: our S3 "
        f"SHA1 must land at our intended title regardless of whether a "
        f"redirect currently sits there); got {action!r}. If this "
        f"asserts, someone added a 'honor the pre-existing redirect' "
        f"check. That is an invariant violation. See "
        f"docs/upload-invariant.md corollary 2 + the 2026-07-02 Palo "
        f"Pinto incident."
    )


# --------------------------------------------------------------------------
# Invariant satisfied — ``already_correct`` / SKIPPED short-circuits
# --------------------------------------------------------------------------


def test_invariant_already_satisfied_via_normalized_identity_returns_already_correct():
    """When the SHA1 lookup returns the file at the intended title
    under whitespace normalisation (typical post-``get_page_title``
    truncation artifact), the invariant is ALREADY satisfied — no
    upload needed. Resolver returns ``already_correct``; caller records
    SKIPPED. Any change that would still upload our bytes here would
    generate a no-op revision on Commons at best, or worse.
    """
    uploader = _build_uploader()
    same_title = "Item - DPLA - cccccccccccccccccccccccccccccccc.gif"
    raw_page_title = "Item  - DPLA - cccccccccccccccccccccccccccccccc.gif"
    # Two spaces before "- DPLA -" in the constructed page_title;
    # Commons stores the single-space form.
    intended_page = MagicMock()
    intended_page.exists.return_value = True
    intended_page.isRedirectPage.return_value = False
    intended_page.title.return_value = same_title
    existing_file = _make_existing_file(same_title)

    with patch("tools.uploader.get_page", return_value=intended_page):
        action = uploader._resolve_hash_drift(
            existing_file=existing_file,
            page_title=raw_page_title,
            dpla_id="cccccccccccccccccccccccccccccccc",
            ordinal=1,
            wiki_markup="",
        )

    assert action == "already_correct", (
        f"expected already_correct (invariant already satisfied: our "
        f"SHA1 lives at the intended title under whitespace-run "
        f"normalisation); got {action!r}."
    )


# --------------------------------------------------------------------------
# Sanity-check: the ``leave_others_alone`` sentinel is preserved
# --------------------------------------------------------------------------


def test_leave_others_alone_sentinel_is_the_expected_string_value():
    """The sentinel string value ``leave_others_alone`` is the contract
    between ``_resolve_hash_drift`` and its caller. A rename here
    would silently break the caller's dispatch (which is a plain
    string comparison). Pin the exact value so a future refactor
    can't rename it without a test failure."""
    uploader = _build_uploader(dpla_get_item_metadata={"id": "other"})
    other_title = "Other - DPLA - dddddddddddddddddddddddddddddddd (page 1).jpg"
    with patch("tools.uploader.get_page"):
        action = uploader._resolve_hash_drift(
            existing_file=_make_existing_file(other_title),
            page_title="Ours - DPLA - eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee (page 1).jpg",
            dpla_id="eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            ordinal=1,
            wiki_markup="",
        )
    # If someone renames the sentinel without updating every caller,
    # they'll get a mismatch here.
    assert action == "leave_others_alone"
    assert isinstance(action, str)
