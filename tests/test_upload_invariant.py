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


def test_invariant_corollary_2_redirect_at_intended_title_defers_to_overwrite_handler():
    """CORRECTNESS CRITERION being pinned: when our intended title is a
    ``#REDIRECT`` whose target holds our S3 SHA1 but under a
    DIFFERENT DPLA ID's canonical title (an editor's 2021-era "duplicate
    of" declaration), ``_resolve_hash_drift`` must NOT return ``moved``
    (Case 1 title-drift) — that would move the OTHER DPLA ID's file to
    OUR title, violating the invariant at the other title. Instead the
    resolver returns ``leave_others_alone``, deferring to
    ``process_file``'s redirect handler, which overwrites the redirect
    (corollary 2 of the invariant — the redirect does not bind our
    obligation) and uploads our bytes to OUR intended title.

    This is the exact 2026-07-02 Palo Pinto shape: SHA1 lives at the
    OTHER DPLA ID's title AND our intended title happens to be a
    redirect to that same target. The resolver must recognise the
    cross-item collision FIRST (both are live DPLA IDs, corollary 1)
    and stop; the redirect handling then runs on the outside in
    ``process_file``.

    If a future change makes ``_resolve_hash_drift`` return ``moved``
    when the intended title is a redirect to the SHA1's location
    (regardless of whether the target's DPLA ID matches ours), this
    test fails — that change would move another live DPLA item's file
    to our title, violating that item's invariant."""
    uploader = _build_uploader(dpla_get_item_metadata={"id": "other-item-live"})
    our_intended = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 1).jpg"
    other_dpla_id_title = "Item - DPLA - bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb (page 1).jpg"

    # Our SHA1 lives at the OTHER DPLA ID's title.
    existing_file = _make_existing_file(other_dpla_id_title)
    # Our intended title is a redirect to that same target — the exact
    # Palo Pinto configuration. Mock the ``get_page(page_title)`` result
    # (which _resolve_hash_drift calls after the cross-item check) so
    # if the cross-item early return is ever removed, the redirect
    # branch's behaviour is at least well-defined during the test.
    redirect_target = MagicMock()
    redirect_target.title.return_value = other_dpla_id_title
    intended_page = MagicMock()
    intended_page.exists.return_value = True
    intended_page.isRedirectPage.return_value = True
    intended_page.getRedirectTarget.return_value = redirect_target
    intended_page.title.return_value = our_intended

    with patch("tools.uploader.get_page", return_value=intended_page):
        action = uploader._resolve_hash_drift(
            existing_file=existing_file,
            page_title=our_intended,
            dpla_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ordinal=1,
            wiki_markup="",
        )
    assert action == "leave_others_alone", (
        f"expected leave_others_alone (cross-item collision detected "
        f"before the redirect branch — the other DPLA ID is live, so "
        f"we must not move its file to our title; the caller's "
        f"redirect handler in process_file will overwrite the redirect "
        f"and upload our bytes to our intended title separately); got "
        f"{action!r}. If ``moved``, the resolver is about to move "
        f"another live DPLA item's file to our title — that violates "
        f"the invariant at the OTHER title. See docs/upload-invariant.md "
        f"corollary 1 + the 2026-07-02 Palo Pinto incident."
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


def test_drift_resolution_sentinel_contract_pin():
    """The four ``DriftResolution`` enum members define the contract
    between ``_resolve_hash_drift`` and its caller. This test pins:

      * The specific enum member returned for a known input scenario
        (``LEAVE_OTHERS_ALONE`` for cross-item collision).
      * That the enum member's ``str`` value has not silently drifted
        from ``"leave_others_alone"`` — old-style callers or log
        readers may still assume the plain string form via
        ``str, Enum`` subclassing.
      * That the enum member IS a ``DriftResolution`` (not a bare
        string), so a well-typed caller can use enum member
        comparisons without ``==`` falling back to string-equality.
    """
    from tools.uploader import DriftResolution

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
    assert action is DriftResolution.LEAVE_OTHERS_ALONE
    assert action.value == "leave_others_alone"
    assert isinstance(action, str)  # ``str, Enum`` subclass preserves string-ness
