"""Tests for tools/uploader.py helpers.

Currently focused on the end-of-run touch flow added to close the Wikidata
replication-lag race that lands first-batch files in the unknown-institution
category.
"""

from unittest.mock import MagicMock, patch

from ingest_wikimedia.tracker import Result, Tracker
from ingest_wikimedia.wikimedia import WMC_UPLOAD_CHUNK_SIZE
from ingest_wikimedia.worker_slots import WorkerSlotBudget
import pytest

from ingest_wikimedia.csrf import (
    CSRF_TOKEN_ERROR_MARKER,
    CsrfRecoveryFailed,
    MAX_CSRF_RECOVERIES,
    is_csrf_token_error,
)
from tools.uploader import (
    DriftResolution,
    LARGE_FILE_DIRECT_UPLOAD_LIMIT_BYTES,
    NewFilePageBlocked,
    Uploader,
    _post_item_orphan_check,
    _post_upload_touch_new_institutions,
    is_dup_sha1_sibling_at_expected_title,
    select_upload_chunk_size,
)


# A disabled budget (budget <= 0) whose acquire() is a no-op context
# manager — the touch helper now acquires a slot internally, so its unit
# tests just need a budget object that never blocks.
_DISABLED_BUDGET = WorkerSlotBudget(0)


def _ensurer_with(newly_created: set[str]) -> MagicMock:
    """Build a mock category_ensurer whose `newly_created` returns the given set."""
    m = MagicMock()
    # `newly_created` is a property on the real class; mirror that on the mock
    # so attribute access (not call) returns the set.
    type(m).newly_created = property(lambda self: set(newly_created))
    return m


def test_skips_when_category_ensurer_is_none():
    with (
        patch("tools.uploader.time.sleep") as sleep_mock,
        patch("tools.uploader.touch_institution_files") as touch_mock,
    ):
        _post_upload_touch_new_institutions(
            MagicMock(), None, dry_run=False, slot_budget=_DISABLED_BUDGET
        )
    sleep_mock.assert_not_called()
    touch_mock.assert_not_called()


def test_skips_when_dry_run():
    ensurer = _ensurer_with({"Q1", "Q2"})
    with (
        patch("tools.uploader.time.sleep") as sleep_mock,
        patch("tools.uploader.touch_institution_files") as touch_mock,
    ):
        _post_upload_touch_new_institutions(
            MagicMock(), ensurer, dry_run=True, slot_budget=_DISABLED_BUDGET
        )
    sleep_mock.assert_not_called()
    touch_mock.assert_not_called()


def test_skips_when_newly_created_is_empty():
    ensurer = _ensurer_with(set())
    with (
        patch("tools.uploader.time.sleep") as sleep_mock,
        patch("tools.uploader.touch_institution_files") as touch_mock,
    ):
        _post_upload_touch_new_institutions(
            MagicMock(), ensurer, dry_run=False, slot_budget=_DISABLED_BUDGET
        )
    sleep_mock.assert_not_called()
    touch_mock.assert_not_called()


def test_sleeps_then_touches_each_newly_created_qid():
    ensurer = _ensurer_with({"Q1", "Q2", "Q3"})
    site = MagicMock()
    with (
        patch("tools.uploader.time.sleep") as sleep_mock,
        patch("tools.uploader.touch_institution_files", return_value=4) as touch_mock,
        patch("tools.uploader._REPLICATION_SETTLE_SECS", 10),
    ):
        _post_upload_touch_new_institutions(
            site, ensurer, dry_run=False, slot_budget=_DISABLED_BUDGET
        )

    sleep_mock.assert_called_once_with(10)
    assert touch_mock.call_count == 3
    qids_touched = {call.args[1] for call in touch_mock.call_args_list}
    assert qids_touched == {"Q1", "Q2", "Q3"}
    for call in touch_mock.call_args_list:
        assert call.args[0] is site


def test_per_qid_exception_does_not_stop_remaining_qids(caplog):
    """If touch_institution_files raises for one institution, log it and
    keep going for the others — losing one is far better than losing all."""
    import logging as _logging

    ensurer = _ensurer_with({"Q_good_1", "Q_bad", "Q_good_2"})

    def side_effect(site, qid, **kwargs):
        if qid == "Q_bad":
            raise RuntimeError("simulated commons search failure")
        return 7

    with (
        patch("tools.uploader.time.sleep"),
        patch(
            "tools.uploader.touch_institution_files", side_effect=side_effect
        ) as touch_mock,
        caplog.at_level(_logging.INFO),
    ):
        _post_upload_touch_new_institutions(
            MagicMock(), ensurer, dry_run=False, slot_budget=_DISABLED_BUDGET
        )

    qids_attempted = {call.args[1] for call in touch_mock.call_args_list}
    assert qids_attempted == {"Q_good_1", "Q_bad", "Q_good_2"}

    messages = " | ".join(r.message for r in caplog.records)
    assert "Q_bad" in messages
    assert "simulated commons search failure" in messages


# --------------------------------------------------------------------------
# _post_item_orphan_check — log-only audit under the SHA1-uniqueness redesign
#
# The orphan check no longer tags: it never writes to Commons. Every
# trailing-page orphan it finds (SHA1 match or not, keep-title present or
# not) is logged and counted under ``Result.ORPHANS_FLAGGED``.
# ``Result.ORPHANS_TAGGED`` is never incremented anymore. These tests pin
# that no write occurs and that matched orphans are flagged, while keeping
# the probe / gap-tolerance / redirect-skip mechanics covered.
# --------------------------------------------------------------------------


def _stub_s3_client_for_assets(assets_by_ordinal: dict[int, str]):
    """Mock S3Client whose objects return CHECKSUM-keyed sha1 metadata."""

    def _get_media_path(dpla_id, ordinal, partner):
        return f"{partner}/images/x/x/x/x/{dpla_id}/{ordinal}_{dpla_id}"

    def _object(_bucket, key):
        ordinal = int(key.rsplit("/", 1)[1].split("_", 1)[0])
        sha1 = assets_by_ordinal[ordinal]
        obj = MagicMock()
        obj.metadata = {"sha1": sha1}
        return obj

    s3_client = MagicMock()
    s3_client.get_media_s3_path.side_effect = _get_media_path
    inner = MagicMock()
    inner.Object.side_effect = _object
    s3_client.get_s3.return_value = inner
    return s3_client


def _make_file_page_factory(
    existing: dict[str, str],
    redirects: set[str] | None = None,
    created: list | None = None,
):
    """Return a stub for pywikibot.FilePage(site, title).

    `existing` maps Commons title → sha1 of the file at that title. For
    redirect pages, the value is the redirect *target's* sha1, mirroring
    pywikibot's `latest_file_info.sha1` behavior of following redirects.

    `redirects` is the set of titles that should be reported as redirect
    pages (`isRedirectPage()` → True). Titles in `existing` but not in
    `redirects` are real file pages; titles in both are redirects whose
    `latest_file_info` would resolve through to a target.

    Titles not in `existing` are nonexistent (`exists()` → False).

    When `created` is a list, every FilePage mock this factory hands out is
    appended to it, so a test can assert no write method (``save`` /
    ``editpage`` / ``touch``) was ever called on any probed page — the
    log-only audit must never write to Commons.
    """
    redirects = redirects or set()

    def _factory(site, title):
        page = MagicMock()
        page.title.return_value = title
        if title in existing:
            page.exists.return_value = True
            page.isRedirectPage.return_value = title in redirects
            page.latest_file_info.sha1 = existing[title]
        else:
            page.exists.return_value = False
            page.isRedirectPage.return_value = False
            # Accessing latest_file_info on a nonexistent page would raise,
            # but the code-under-test breaks the loop before reading it.
        if created is not None:
            created.append(page)
        return page

    return _factory


def _assert_no_commons_writes(pages: list) -> None:
    """The log-only orphan audit must never write to Commons. Assert that
    none of the probed FilePage mocks had a write method invoked."""
    for page in pages:
        page.save.assert_not_called()
        page.editpage.assert_not_called()
        page.touch.assert_not_called()


def test_orphan_check_no_orphan_when_first_probe_is_missing():
    """Item has 3 jpgs; (page 4).jpg doesn't exist → nothing to do."""
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({1: "aaa", 2: "bbb", 3: "ccc"})
    ordinal_exts = {1: ".jpg", 2: ".jpg", 3: ".jpg"}
    page_labels = {1: "1", 2: "2", 3: "3"}

    with patch("tools.uploader.pywikibot.FilePage") as fp:
        fp.side_effect = _make_file_page_factory(existing={})
        _post_item_orphan_check(
            site=MagicMock(),
            s3_client=s3_client,
            tracker=tracker,
            dpla_id="abcd1234abcd1234abcd1234abcd1234",
            item_title="Some Item",
            partner="nara",
            ordinal_exts=ordinal_exts,
            page_labels=page_labels,
            dry_run=False,
        )
    assert tracker.count(Result.ORPHANS_TAGGED) == 0
    assert tracker.count(Result.ORPHANS_FLAGGED) == 0


def test_orphan_check_flags_trailing_orphan_with_matching_sha1_without_writing():
    """Item has 3 jpgs, (page 4).jpg exists with SHA1 of (page 3). Under the
    log-only redesign this is FLAGGED (not TAGGED) and no write occurs."""
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({1: "aaa", 2: "bbb", 3: "ccc"})
    ordinal_exts = {1: ".jpg", 2: ".jpg", 3: ".jpg"}
    page_labels = {1: "1", 2: "2", 3: "3"}
    base = "Some Item - DPLA - abcd1234abcd1234abcd1234abcd1234"
    orphan_title = f"{base} (page 4).jpg"
    expected_keep = f"{base} (page 3).jpg"

    created: list = []
    with patch("tools.uploader.pywikibot.FilePage") as fp:
        # keep_title also exists on Commons (the "matched a live kept asset"
        # branch), but the audit still only flags — it never tags.
        fp.side_effect = _make_file_page_factory(
            existing={orphan_title: "ccc", expected_keep: "ccc"}, created=created
        )
        _post_item_orphan_check(
            site=MagicMock(),
            s3_client=s3_client,
            tracker=tracker,
            dpla_id="abcd1234abcd1234abcd1234abcd1234",
            item_title="Some Item",
            partner="nara",
            ordinal_exts=ordinal_exts,
            page_labels=page_labels,
            dry_run=False,
        )

    assert tracker.count(Result.ORPHANS_FLAGGED) == 1
    assert tracker.count(Result.ORPHANS_TAGGED) == 0
    _assert_no_commons_writes(created)


def test_orphan_check_flags_orphan_with_unknown_sha1():
    """Orphan exists but SHA1 isn't one of this item's S3 assets → flag, don't tag."""
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({1: "aaa", 2: "bbb"})
    ordinal_exts = {1: ".jpg", 2: ".jpg"}
    page_labels = {1: "1", 2: "2"}
    orphan_title = "T - DPLA - abcd1234abcd1234abcd1234abcd1234 (page 3).jpg"

    created: list = []
    with patch("tools.uploader.pywikibot.FilePage") as fp:
        fp.side_effect = _make_file_page_factory(
            existing={orphan_title: "zzz_unknown"}, created=created
        )
        _post_item_orphan_check(
            site=MagicMock(),
            s3_client=s3_client,
            tracker=tracker,
            dpla_id="abcd1234abcd1234abcd1234abcd1234",
            item_title="T",
            partner="nara",
            ordinal_exts=ordinal_exts,
            page_labels=page_labels,
            dry_run=False,
        )

    assert tracker.count(Result.ORPHANS_FLAGGED) == 1
    assert tracker.count(Result.ORPHANS_TAGGED) == 0
    _assert_no_commons_writes(created)


def test_orphan_check_handles_multiple_trailing_orphans():
    """Item now has 2 jpgs; (page 3) and (page 4) are both orphans."""
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({1: "aaa", 2: "bbb"})
    ordinal_exts = {1: ".jpg", 2: ".jpg"}
    page_labels = {1: "1", 2: "2"}
    base = "Item - DPLA - abcd1234abcd1234abcd1234abcd1234"
    existing = {
        f"{base} (page 1).jpg": "aaa",  # keep target for (page 4) orphan
        f"{base} (page 2).jpg": "bbb",  # keep target for (page 3) orphan
        f"{base} (page 3).jpg": "bbb",  # matches kept (page 2)
        f"{base} (page 4).jpg": "aaa",  # matches kept (page 1)
    }

    created: list = []
    with patch("tools.uploader.pywikibot.FilePage") as fp:
        fp.side_effect = _make_file_page_factory(existing=existing, created=created)
        _post_item_orphan_check(
            site=MagicMock(),
            s3_client=s3_client,
            tracker=tracker,
            dpla_id="abcd1234abcd1234abcd1234abcd1234",
            item_title="Item",
            partner="nara",
            ordinal_exts=ordinal_exts,
            page_labels=page_labels,
            dry_run=False,
        )

    # Both trailing orphans are flagged; neither is tagged, and nothing is
    # written to Commons.
    assert tracker.count(Result.ORPHANS_FLAGGED) == 2
    assert tracker.count(Result.ORPHANS_TAGGED) == 0
    _assert_no_commons_writes(created)


def test_orphan_check_single_asset_probes_from_page_1():
    """Single-asset items use no-suffix titles, so (page 1) and up are orphans.

    Common signature: item used to be multi-page and got reduced to one asset,
    so its (page 1).jpg / (page 2).jpg / ... linger beyond the no-suffix file.
    """
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({1: "aaa"})
    ordinal_exts = {1: ".jpg"}
    page_labels = {1: ""}
    base = "Item - DPLA - abcd1234abcd1234abcd1234abcd1234"
    existing = {
        f"{base}.jpg": "aaa",  # the kept no-suffix asset must exist on Commons
        f"{base} (page 1).jpg": "aaa",  # orphan
    }

    created: list = []
    with patch("tools.uploader.pywikibot.FilePage") as fp:
        fp.side_effect = _make_file_page_factory(existing=existing, created=created)
        _post_item_orphan_check(
            site=MagicMock(),
            s3_client=s3_client,
            tracker=tracker,
            dpla_id="abcd1234abcd1234abcd1234abcd1234",
            item_title="Item",
            partner="nara",
            ordinal_exts=ordinal_exts,
            page_labels=page_labels,
            dry_run=False,
        )

    assert tracker.count(Result.ORPHANS_FLAGGED) == 1
    assert tracker.count(Result.ORPHANS_TAGGED) == 0
    _assert_no_commons_writes(created)


def test_orphan_check_dry_run_flag_is_inert_and_never_writes():
    """``dry_run`` is accepted for call-site symmetry but unused — the audit
    is log-only regardless, so the orphan is still flagged and still no write
    occurs whether dry_run is True or False."""
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({1: "aaa", 2: "bbb"})
    ordinal_exts = {1: ".jpg", 2: ".jpg"}
    page_labels = {1: "1", 2: "2"}
    base = "Item - DPLA - abcd1234abcd1234abcd1234abcd1234"
    existing = {
        f"{base} (page 2).jpg": "bbb",  # kept target must exist on Commons
        f"{base} (page 3).jpg": "bbb",  # orphan
    }

    created: list = []
    with patch("tools.uploader.pywikibot.FilePage") as fp:
        fp.side_effect = _make_file_page_factory(existing=existing, created=created)
        _post_item_orphan_check(
            site=MagicMock(),
            s3_client=s3_client,
            tracker=tracker,
            dpla_id="abcd1234abcd1234abcd1234abcd1234",
            item_title="Item",
            partner="nara",
            ordinal_exts=ordinal_exts,
            page_labels=page_labels,
            dry_run=True,
        )

    assert tracker.count(Result.ORPHANS_FLAGGED) == 1
    assert tracker.count(Result.ORPHANS_TAGGED) == 0
    _assert_no_commons_writes(created)


def test_orphan_check_skips_gap_and_finds_orphan_past_it():
    """Single-asset item with no (page 1) but (page 2) stranded — gap tolerance
    must let the probe reach it.

    Real case observed: LBJ 1956 Desk Diary I (DPLA 72e2342079...) — the item
    is now single-page (no-suffix .jpg authoritative) but (page 2).jpg lingers
    with no (page 1).jpg adjacent to it (a previous session handled the
    (page 1) slot via move/tag, leaving (page 2) stranded).
    """
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({1: "aaa"})
    ordinal_exts = {1: ".jpg"}
    page_labels = {1: ""}
    base = "Item - DPLA - abcd1234abcd1234abcd1234abcd1234"
    # No (page 1).jpg; (page 2).jpg exists with the same SHA1 as the no-suffix asset.
    existing = {
        f"{base}.jpg": "aaa",  # kept no-suffix target on Commons
        f"{base} (page 2).jpg": "aaa",  # orphan past the gap
    }

    created: list = []
    with patch("tools.uploader.pywikibot.FilePage") as fp:
        fp.side_effect = _make_file_page_factory(existing=existing, created=created)
        _post_item_orphan_check(
            site=MagicMock(),
            s3_client=s3_client,
            tracker=tracker,
            dpla_id="abcd1234abcd1234abcd1234abcd1234",
            item_title="Item",
            partner="nara",
            ordinal_exts=ordinal_exts,
            page_labels=page_labels,
            dry_run=False,
        )

    # Gap tolerance let the probe reach the stranded (page 2) orphan; it is
    # flagged, not tagged, and nothing is written.
    assert tracker.count(Result.ORPHANS_FLAGGED) == 1
    assert tracker.count(Result.ORPHANS_TAGGED) == 0
    _assert_no_commons_writes(created)


def test_orphan_check_flags_when_keep_title_does_not_exist():
    """If the S3 asset whose SHA1 matches the orphan was never actually
    uploaded (e.g. process_file SKIPPED it or aborted on timeout), the
    keep_title we'd point at doesn't exist on Commons. The match is still
    flagged for audit; still no write.
    """
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({1: "aaa", 2: "bbb"})
    ordinal_exts = {1: ".jpg", 2: ".jpg"}
    page_labels = {1: "1", 2: "2"}
    base = "Item - DPLA - abcd1234abcd1234abcd1234abcd1234"
    # Orphan exists, but the kept target (page 2).jpg does NOT exist on Commons
    # (e.g. process_file skipped that ordinal).
    existing = {f"{base} (page 3).jpg": "bbb"}

    created: list = []
    with patch("tools.uploader.pywikibot.FilePage") as fp:
        fp.side_effect = _make_file_page_factory(existing=existing, created=created)
        _post_item_orphan_check(
            site=MagicMock(),
            s3_client=s3_client,
            tracker=tracker,
            dpla_id="abcd1234abcd1234abcd1234abcd1234",
            item_title="Item",
            partner="nara",
            ordinal_exts=ordinal_exts,
            page_labels=page_labels,
            dry_run=False,
        )

    assert tracker.count(Result.ORPHANS_TAGGED) == 0
    assert tracker.count(Result.ORPHANS_FLAGGED) == 1
    _assert_no_commons_writes(created)


def test_orphan_check_uses_declared_count_when_sha1_metadata_missing():
    """An in-range ordinal that's missing CHECKSUM metadata on S3 must still
    count toward expected_count for probe-boundary purposes — otherwise the
    probe would start inside the legitimate page range and could tag a real
    page as a duplicate of itself.

    Scenario: 3 jpgs declared in ordinal_exts; ordinal 2's S3 metadata is
    missing the sha1 key.  Probe must still start at (page 4), not (page 3).
    """

    def _stub_s3_with_one_missing_checksum():
        # ordinal 1 and 3 have SHA1; ordinal 2 returns metadata without 'sha1'.
        sha1s = {1: "aaa", 3: "ccc"}

        def _get_media_path(dpla_id, ordinal, partner):
            return f"{partner}/images/x/x/x/x/{dpla_id}/{ordinal}_{dpla_id}"

        def _object(_bucket, key):
            ordinal = int(key.rsplit("/", 1)[1].split("_", 1)[0])
            obj = MagicMock()
            obj.metadata = {"sha1": sha1s[ordinal]} if ordinal in sha1s else {}
            return obj

        s3_client = MagicMock()
        s3_client.get_media_s3_path.side_effect = _get_media_path
        inner = MagicMock()
        inner.Object.side_effect = _object
        s3_client.get_s3.return_value = inner
        return s3_client

    tracker = Tracker()
    s3_client = _stub_s3_with_one_missing_checksum()
    ordinal_exts = {1: ".jpg", 2: ".jpg", 3: ".jpg"}
    page_labels = {1: "1", 2: "2", 3: "3"}
    base = "Item - DPLA - abcd1234abcd1234abcd1234abcd1234"
    # (page 3) is a legitimate page — its SHA1 happens to match ordinal 3's.
    # If the probe started at (page 3), it would tag a real page as a dup.
    # No (page 4) → nothing to tag overall.
    existing = {f"{base} (page 3).jpg": "ccc"}

    created: list = []
    with patch("tools.uploader.pywikibot.FilePage") as fp:
        fp.side_effect = _make_file_page_factory(existing=existing, created=created)
        _post_item_orphan_check(
            site=MagicMock(),
            s3_client=s3_client,
            tracker=tracker,
            dpla_id="abcd1234abcd1234abcd1234abcd1234",
            item_title="Item",
            partner="nara",
            ordinal_exts=ordinal_exts,
            page_labels=page_labels,
            dry_run=False,
        )

    assert tracker.count(Result.ORPHANS_TAGGED) == 0
    assert tracker.count(Result.ORPHANS_FLAGGED) == 0
    _assert_no_commons_writes(created)
    # And the FilePage probe must not have hit (page 3) — it would have
    # if we used the SHA1-filtered count of 2 instead of the declared 3.
    probed_titles = [call.args[1] for call in fp.call_args_list]
    assert f"{base} (page 3).jpg" not in probed_titles


def test_orphan_check_skips_extensions_with_no_assets():
    """Empty ordinal_exts (e.g. stubs-only) → no probing, no errors."""
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({})
    with patch("tools.uploader.pywikibot.FilePage") as fp:
        _post_item_orphan_check(
            site=MagicMock(),
            s3_client=s3_client,
            tracker=tracker,
            dpla_id="abcd1234abcd1234abcd1234abcd1234",
            item_title="Item",
            partner="nara",
            ordinal_exts={},
            page_labels={},
            dry_run=False,
        )
    fp.assert_not_called()
    assert tracker.count(Result.ORPHANS_TAGGED) == 0
    assert tracker.count(Result.ORPHANS_FLAGGED) == 0


def test_orphan_check_skips_redirect_pages():
    """A (page N+1) title that is already a #REDIRECT to a kept asset must
    NOT be flagged.

    pywikibot's `latest_file_info.sha1` on a redirect *follows* the redirect
    and returns the target file's sha1, so a naive `orphan_sha1 in
    sha1_to_kept` lookup would always match. The audit skips redirects
    entirely — they already point at the correct target — so nothing is
    flagged and nothing is written.

    Caught from commit 1219698039 on Commons:
    File:Drift Sight, Italian, Crocco - DPLA - 023de366f0c65bec19d5b61b7e3b42d6 (page 4).jpg
    """
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({1: "aaa", 2: "bbb", 3: "ccc"})
    ordinal_exts = {1: ".jpg", 2: ".jpg", 3: ".jpg"}
    page_labels = {1: "1", 2: "2", 3: "3"}
    base = "Some Item - DPLA - abcd1234abcd1234abcd1234abcd1234"
    redirect_title = f"{base} (page 4).jpg"
    keep_title = f"{base} (page 3).jpg"

    created: list = []
    with patch("tools.uploader.pywikibot.FilePage") as fp:
        # (page 4) exists, is a redirect, and pywikibot's latest_file_info
        # would (mis)report the target's sha1 ("ccc"). (page 3) exists as a
        # real file with the same sha1.
        fp.side_effect = _make_file_page_factory(
            existing={redirect_title: "ccc", keep_title: "ccc"},
            redirects={redirect_title},
            created=created,
        )
        _post_item_orphan_check(
            site=MagicMock(),
            s3_client=s3_client,
            tracker=tracker,
            dpla_id="abcd1234abcd1234abcd1234abcd1234",
            item_title="Some Item",
            partner="nara",
            ordinal_exts=ordinal_exts,
            page_labels=page_labels,
            dry_run=False,
        )

    # A redirect is already doing what we'd flag it for; it is neither
    # flagged nor written.
    assert tracker.count(Result.ORPHANS_TAGGED) == 0
    assert tracker.count(Result.ORPHANS_FLAGGED) == 0
    _assert_no_commons_writes(created)


def test_orphan_check_skips_redirect_but_continues_probing():
    """A redirect at (page N+1) doesn't stop the probe — if (page N+2) is
    a real orphan file, it should still be flagged (not the redirect)."""
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({1: "aaa", 2: "bbb", 3: "ccc"})
    ordinal_exts = {1: ".jpg", 2: ".jpg", 3: ".jpg"}
    page_labels = {1: "1", 2: "2", 3: "3"}
    base = "Some Item - DPLA - abcd1234abcd1234abcd1234abcd1234"
    redirect_title = f"{base} (page 4).jpg"
    real_orphan_title = f"{base} (page 5).jpg"
    keep_title = f"{base} (page 3).jpg"

    created: list = []
    with patch("tools.uploader.pywikibot.FilePage") as fp:
        # (page 4) is a redirect (skipped, not flagged).
        # (page 5) is a real file with the matching sha1 — should be flagged.
        # (page 3) exists as the kept target.
        fp.side_effect = _make_file_page_factory(
            existing={
                redirect_title: "ccc",
                real_orphan_title: "ccc",
                keep_title: "ccc",
            },
            redirects={redirect_title},
            created=created,
        )
        _post_item_orphan_check(
            site=MagicMock(),
            s3_client=s3_client,
            tracker=tracker,
            dpla_id="abcd1234abcd1234abcd1234abcd1234",
            item_title="Some Item",
            partner="nara",
            ordinal_exts=ordinal_exts,
            page_labels=page_labels,
            dry_run=False,
        )

    # Exactly one orphan flagged: the real file at (page 5). The redirect at
    # (page 4) was skipped (proving the probe continued past it), and nothing
    # was written.
    assert tracker.count(Result.ORPHANS_FLAGGED) == 1
    assert tracker.count(Result.ORPHANS_TAGGED) == 0
    _assert_no_commons_writes(created)
    probed_titles = [call.args[1] for call in fp.call_args_list]
    assert redirect_title in probed_titles
    assert real_orphan_title in probed_titles


# --------------------------------------------------------------------------
# Uploader._resolve_hash_drift — SHA1-uniqueness resolution outcomes.
#
# Our SHA1 is already on Commons, so no branch uploads a second copy. The
# method returns exactly one of MOVED / MERGE_AND_REDIRECT / HAND_FIX /
# ALREADY_CORRECT. ``DriftResolution`` subclasses ``str, Enum`` so the enum
# members compare equal to their string values (``DriftResolution.MOVED ==
# "moved"``); the assertions below use the string values for brevity.
# --------------------------------------------------------------------------


def _build_uploader_with_dpla(other_item_exists: bool = True) -> "object":
    from tools.uploader import Uploader

    dpla = MagicMock()
    dpla.get_item_metadata.return_value = (
        {"sourceResource": {}} if other_item_exists else None
    )
    return Uploader(
        tracker=Tracker(),
        local_fs=MagicMock(),
        s3_client=MagicMock(),
        dpla=dpla,
        site=MagicMock(),
        category_ensurer=None,
    )


def _drift_existing_file(title: str) -> MagicMock:
    f = MagicMock()
    f.title.return_value = title
    return f


def test_resolve_hash_drift_within_item_sibling_slot_merges_and_redirects():
    """Our SHA1 lives at one of THIS item's own current asset positions and
    the intended title holds a real (different) file → within-item source
    duplication. No second upload: centralize on the sibling and redirect."""
    uploader = _build_uploader_with_dpla()
    # Existing file: page 5 of the SAME item, one of this run's asset slots.
    existing_title = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 5).jpg"
    intended_title = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 4).jpg"
    expected_titles = {
        f"Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page {n}).jpg"
        for n in range(1, 10)
    }
    # intended_page: exists as a real (non-redirect) file with other content.
    intended_page = MagicMock()
    intended_page.exists.return_value = True
    intended_page.isRedirectPage.return_value = False
    intended_page.title.return_value = intended_title
    with patch("tools.uploader.get_page", return_value=intended_page):
        action = uploader._resolve_hash_drift(
            existing_file=_drift_existing_file(existing_title),
            page_title=intended_title,
            dpla_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ordinal=4,
            expected_item_titles=expected_titles,
        )
    assert action == "merge_and_redirect"


def test_resolve_hash_drift_intended_title_occupied_by_different_file_hand_fix():
    """Our SHA1 is at a wrong title and the intended title is occupied by a
    DIFFERENT real file that is NOT one of this item's asset slots — the bot
    cannot safely rename or overwrite, so hand off to a human."""
    uploader = _build_uploader_with_dpla()
    # Same-item drift (same DPLA id), so the cross-item branch is skipped and
    # we reach the occupied-intended-title logic.
    dpla_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    existing_title = f"Item - DPLA - {dpla_id} (page 99).jpg"
    intended_title = f"Item - DPLA - {dpla_id} (page 4).jpg"
    expected_titles = {f"Item - DPLA - {dpla_id} (page {n}).jpg" for n in range(1, 10)}
    intended_page = MagicMock()
    intended_page.exists.return_value = True
    intended_page.isRedirectPage.return_value = False
    intended_page.title.return_value = intended_title
    with patch("tools.uploader.get_page", return_value=intended_page):
        action = uploader._resolve_hash_drift(
            existing_file=_drift_existing_file(existing_title),
            page_title=intended_title,
            dpla_id=dpla_id,
            ordinal=4,
            expected_item_titles=expected_titles,
        )
    assert action == "hand_fix"


def test_resolve_hash_drift_intended_redirect_elsewhere_hand_fix():
    """Intended title is a redirect to a THIRD file (not the location of our
    SHA1) — a rename would collide with that redirect; route to hand-fix."""
    uploader = _build_uploader_with_dpla()
    dpla_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    existing_title = f"Item - DPLA - {dpla_id} (page 5).jpg"
    intended_title = f"Item - DPLA - {dpla_id} (page 4).jpg"
    intended_page = _make_intended_page(
        intended_title,
        exists=True,
        is_redirect=True,
        redirect_target="Some Other File.jpg",
    )
    with patch("tools.uploader.get_page", return_value=intended_page):
        action = uploader._resolve_hash_drift(
            existing_file=_drift_existing_file(existing_title),
            page_title=intended_title,
            dpla_id=dpla_id,
            ordinal=4,
        )
    assert action == "hand_fix"


def _build_uploader_with_dpla_raising(exc: Exception) -> "object":
    """Like ``_build_uploader_with_dpla`` but DPLA API raises ``exc`` on
    every ``get_item_metadata`` call. Used to exercise the "colliding
    DPLA item couldn't be verified" branches of ``_resolve_hash_drift``.
    """
    from tools.uploader import Uploader

    dpla = MagicMock()
    dpla.get_item_metadata.side_effect = exc
    return Uploader(
        tracker=Tracker(),
        local_fs=MagicMock(),
        s3_client=MagicMock(),
        dpla=dpla,
        site=MagicMock(),
        category_ensurer=None,
    )


def _make_http_404_error() -> Exception:
    """Synthesise a ``requests``-shaped HTTPError with response.status_code
    == 404. Mirrors the shape ``DPLA.get_item_metadata``'s
    ``response.raise_for_status()`` produces when the DPLA API has dropped
    the item. Not constructing a real ``requests.HTTPError`` so the test
    doesn't pull in ``requests`` for a duck-typed check the production
    code reads via ``getattr``.
    """
    err = Exception("404 Client Error: Not Found")
    err.response = MagicMock()  # type: ignore[attr-defined]
    err.response.status_code = 404
    return err


def test_resolve_hash_drift_404_on_colliding_id_falls_through_to_migration():
    """When the colliding file's DPLA ID returns 404, the file is an orphan
    from a removed DPLA item. Fall through to the intended-title logic; with
    an empty intended title that means a move → MOVED. (The 404 must NOT be
    treated as a live cross-item collision — that would MERGE_AND_REDIRECT
    onto a dead item's file.)"""
    uploader = _build_uploader_with_dpla_raising(_make_http_404_error())
    existing_title = "Item - DPLA - bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb (page 2).jpg"
    intended_title = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 2).jpg"
    # Nothing at the intended title → simple move. Patch _move_to_correct_title
    # so the test doesn't drive a real pywikibot move.
    intended_page = MagicMock()
    intended_page.exists.return_value = False
    with (
        patch("tools.uploader.get_page", return_value=intended_page),
        patch.object(uploader, "_move_to_correct_title"),
    ):
        action = uploader._resolve_hash_drift(
            existing_file=_drift_existing_file(existing_title),
            page_title=intended_title,
            dpla_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ordinal=2,
        )
    assert action == "moved", (
        "404 on the colliding DPLA ID is the orphan signal — the file must "
        "be migrated to our title, not merged onto a dead item's file."
    )


def test_resolve_hash_drift_non_404_exception_routes_to_hand_fix():
    """Conservative fallback: a non-404 exception verifying the colliding
    DPLA item (network timeout, JSON parse error, etc.) leaves the collision
    unverified — route to HAND_FIX rather than guessing. Never act on a
    transient DPLA API blip."""

    class FlakyConnError(Exception):
        pass

    uploader = _build_uploader_with_dpla_raising(FlakyConnError("connection reset"))
    existing_title = "Item - DPLA - bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb (page 2).jpg"
    intended_title = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 2).jpg"
    # HAND_FIX returns before get_page is consulted.
    action = uploader._resolve_hash_drift(
        existing_file=_drift_existing_file(existing_title),
        page_title=intended_title,
        dpla_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ordinal=2,
    )
    assert action == "hand_fix"


def test_resolve_hash_drift_5xx_response_routes_to_hand_fix():
    """Sister case to the 404 test: a 5xx-shaped HTTPError must NOT take the
    404 orphan branch. Pin the exact status check so a refactor that broadens
    the gate (e.g. to ``status >= 400``) is caught — a 5xx is unverified, so
    HAND_FIX."""
    err = Exception("503 Service Unavailable")
    err.response = MagicMock()  # type: ignore[attr-defined]
    err.response.status_code = 503

    uploader = _build_uploader_with_dpla_raising(err)
    existing_title = "Item - DPLA - bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb (page 2).jpg"
    intended_title = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 2).jpg"
    action = uploader._resolve_hash_drift(
        existing_file=_drift_existing_file(existing_title),
        page_title=intended_title,
        dpla_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ordinal=2,
    )
    assert action == "hand_fix"


def test_resolve_hash_drift_valid_cross_item_collision_merges_and_redirects():
    """Cross-item source duplication: our SHA1 is a DIFFERENT, still-live DPLA
    item's canonical content. Under the SHA1-uniqueness constraint we don't
    upload a second copy — merge our SDC onto their file and redirect our
    intended title. (live other DPLA id)."""
    uploader = _build_uploader_with_dpla(other_item_exists=True)
    existing_title = "Item - DPLA - bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb (page 2).jpg"
    intended_title = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 2).jpg"
    action = uploader._resolve_hash_drift(
        existing_file=_drift_existing_file(existing_title),
        page_title=intended_title,
        dpla_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ordinal=2,
    )
    assert action == "merge_and_redirect"


# --------------------------------------------------------------------------
# select_upload_chunk_size — pin the OOM-prevention contract: files above
# LARGE_FILE_DIRECT_UPLOAD_LIMIT_BYTES MUST force chunked upload even when
# caller flags prefer direct, because direct can't physically succeed past
# Wikimedia's gateway limit (the 211 MB NARA incident at 6.7 GB RSS).
# --------------------------------------------------------------------------


def test_chunk_size_neither_pref_returns_chunked():
    """Default path (neither file_exists nor force_ignore_warnings): chunked."""
    assert select_upload_chunk_size(
        file_exists=False,
        force_ignore_warnings=False,
        file_size_bytes=10 * 1024 * 1024,
    ) == (WMC_UPLOAD_CHUNK_SIZE, False)


def test_chunk_size_file_exists_small_returns_direct():
    """Overwrite of an existing small page → direct upload to bypass warnings."""
    assert select_upload_chunk_size(
        file_exists=True,
        force_ignore_warnings=False,
        file_size_bytes=10 * 1024 * 1024,
    ) == (0, True)


def test_chunk_size_force_ignore_warnings_small_returns_direct():
    """force_ignore_warnings on a small file → direct upload (warning bypass)."""
    assert select_upload_chunk_size(
        file_exists=False,
        force_ignore_warnings=True,
        file_size_bytes=10 * 1024 * 1024,
    ) == (0, True)


def test_chunk_size_large_file_exists_forces_chunked():
    """Overwrite of a large file: direct can't succeed at this size, so chunked
    even though the warning-bypass benefit is lost. Bounded FAILED > OOM."""
    one_byte_over = LARGE_FILE_DIRECT_UPLOAD_LIMIT_BYTES + 1
    assert select_upload_chunk_size(
        file_exists=True,
        force_ignore_warnings=False,
        file_size_bytes=one_byte_over,
    ) == (WMC_UPLOAD_CHUNK_SIZE, True)


def test_chunk_size_large_force_ignore_warnings_forces_chunked():
    """force_ignore_warnings on a 211 MB file (the NARA incident): chunked,
    and prefers_direct still True so the caller logs the size override."""
    nara_incident_size = 221_583_206  # exact bytes of 2_7d3114...
    assert select_upload_chunk_size(
        file_exists=False,
        force_ignore_warnings=True,
        file_size_bytes=nara_incident_size,
    ) == (WMC_UPLOAD_CHUNK_SIZE, True)


def test_chunk_size_exactly_at_threshold_stays_direct():
    """At-threshold is fine for direct upload; only strictly above forces chunked."""
    assert select_upload_chunk_size(
        file_exists=True,
        force_ignore_warnings=False,
        file_size_bytes=LARGE_FILE_DIRECT_UPLOAD_LIMIT_BYTES,
    ) == (0, True)


def test_chunk_size_threshold_under_wikimedia_gateway_limit():
    """The threshold itself must stay safely under Wikimedia's ~100 MB
    practical direct-upload limit — otherwise we re-introduce the OOM."""
    assert LARGE_FILE_DIRECT_UPLOAD_LIMIT_BYTES < 100 * 1024 * 1024


# --------------------------------------------------------------------------
# is_dup_sha1_sibling_at_expected_title — the within-item MERGE_AND_REDIRECT gate
#
# Pin the contract that prevents the orphan-duplicate bug observed on item
# fe6f59a29fddf8e3483e91ad805bf039 (Zuni in costume). NARA's mediaMaster
# listed the same TIF URL at two positions, so duplicate_source_sha1s
# contained that SHA1. The helper must return True only when the existing
# file lives at one of THIS item's own expected current titles (routing to
# the within-item MERGE_AND_REDIRECT short-circuit in process_file); a legacy
# 2011 NARA-bot title with the same SHA1 must return False so the caller
# falls through to normal drift handling instead.
# --------------------------------------------------------------------------

_SHA1 = "76f6fe0a766e4a664b7d99b9e6c0fb8594bac083"
_DPLA_PAGE_1 = "Zuni in costume - DPLA - fe6f59a29fddf8e3483e91ad805bf039 (page 1).tiff"
_DPLA_PAGE_2 = "Zuni in costume - DPLA - fe6f59a29fddf8e3483e91ad805bf039 (page 2).tiff"
_LEGACY_NARA = "Zuni in costume - NARA - 523667.tif"


def test_dup_sha1_sibling_true_when_existing_is_at_expected_title():
    """SHA1 appears at multiple source positions AND the Commons file lives
    at one of our (page N).ext titles → true sibling, upload-only is safe."""
    assert (
        is_dup_sha1_sibling_at_expected_title(
            sha1=_SHA1,
            existing_file_title=_DPLA_PAGE_1,
            duplicate_source_sha1s={_SHA1},
            expected_item_titles={_DPLA_PAGE_1, _DPLA_PAGE_2},
        )
        is True
    )


def test_dup_sha1_sibling_false_when_existing_is_legacy_title():
    """THE BUG FIX: SHA1 is in duplicate_source_sha1s, but the existing file
    is at a legacy title (here the 2011 NARA-bot upload). Returning True
    would short-circuit drift handling and leave the legacy title in place
    forever. Must return False so the caller falls through to drift
    handling, which Case-3-moves the legacy title to (page 1).tiff."""
    assert (
        is_dup_sha1_sibling_at_expected_title(
            sha1=_SHA1,
            existing_file_title=_LEGACY_NARA,
            duplicate_source_sha1s={_SHA1},
            expected_item_titles={_DPLA_PAGE_1, _DPLA_PAGE_2},
        )
        is False
    )


def test_dup_sha1_sibling_false_when_sha1_not_in_dup_set():
    """No multi-position SHA1 case at all → False even if title coincidentally
    matches an expected title; the caller's normal drift path handles this."""
    assert (
        is_dup_sha1_sibling_at_expected_title(
            sha1="ffffffffffffffffffffffffffffffffffffffff",
            existing_file_title=_DPLA_PAGE_1,
            duplicate_source_sha1s={_SHA1},
            expected_item_titles={_DPLA_PAGE_1, _DPLA_PAGE_2},
        )
        is False
    )


def test_dup_sha1_sibling_false_when_duplicate_source_sha1s_is_none():
    """Defensive: process_file's signature allows None; the gate must
    not raise and must return False, routing through normal drift."""
    assert (
        is_dup_sha1_sibling_at_expected_title(
            sha1=_SHA1,
            existing_file_title=_DPLA_PAGE_1,
            duplicate_source_sha1s=None,
            expected_item_titles={_DPLA_PAGE_1},
        )
        is False
    )


def test_dup_sha1_sibling_false_when_expected_item_titles_is_none():
    """Defensive: same as above for the other optional parameter."""
    assert (
        is_dup_sha1_sibling_at_expected_title(
            sha1=_SHA1,
            existing_file_title=_DPLA_PAGE_1,
            duplicate_source_sha1s={_SHA1},
            expected_item_titles=None,
        )
        is False
    )


def test_dup_sha1_sibling_false_when_duplicate_source_sha1s_is_empty_set():
    """Empty set is falsy in the early-return; explicit test for clarity."""
    assert (
        is_dup_sha1_sibling_at_expected_title(
            sha1=_SHA1,
            existing_file_title=_DPLA_PAGE_1,
            duplicate_source_sha1s=set(),
            expected_item_titles={_DPLA_PAGE_1},
        )
        is False
    )


# --------------------------------------------------------------------------
# CommonsDelinker suppression when actual_filename will be overwritten by
# a later ordinal in the same session. See process_file's per-ordinal
# iteration and _resolve_hash_drift's Case 1 / Case 3 paths.
#
# Background: when Case 3 moves existing_file → intended_title, the source
# title becomes a redirect. If THAT title also happens to be one of this
# item's other current asset positions (i.e. a later ordinal will write
# different content to it), the CommonsDelinker request to rewrite
# external references away from the source title becomes invalid the
# moment the redirect is overwritten — external uses pointing at the new
# content get silently rewritten to the wrong file. Suppress the
# request in that case. The move itself is still useful (places the file
# at its new canonical title cheaply); only the link-rewrite is wrong.
# --------------------------------------------------------------------------


_ITEM_PAGE_8 = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 8).jpg"
_ITEM_PAGE_11 = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 11).jpg"


def _make_intended_page(title, exists=False, is_redirect=False, redirect_target=None):
    p = MagicMock()
    p.exists.return_value = exists
    p.isRedirectPage.return_value = is_redirect
    p.title.return_value = title
    if redirect_target:
        rt = MagicMock()
        rt.title.return_value = redirect_target
        p.getRedirectTarget.return_value = rt
    return p


def test_case3_move_suppresses_commonsdelinker_when_actual_is_sibling():
    """Reproduces the production bug: ord 11 of a multi-page item finds its
    SHA1 at (page 11).jpg but its renumbered intended title is (page 8).jpg.
    Case 3 fires — but (page 11).jpg is in this item's expected_item_titles
    because a LATER ordinal will write different content to it. The
    CommonsDelinker request must be suppressed."""
    uploader = _build_uploader_with_dpla()
    intended = _make_intended_page(_ITEM_PAGE_8, exists=False)
    expected_titles = {
        _ITEM_PAGE_8,
        _ITEM_PAGE_11,  # ← sibling: another ordinal will overwrite this slot
    }
    with (
        patch("tools.uploader.get_page", return_value=intended),
        patch.object(uploader, "_move_to_correct_title") as mock_move,
    ):
        action = uploader._resolve_hash_drift(
            existing_file=_drift_existing_file(_ITEM_PAGE_11),
            page_title=_ITEM_PAGE_8,
            dpla_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ordinal=11,
            expected_item_titles=expected_titles,
        )
    assert action == "moved"
    mock_move.assert_called_once()
    kwargs = mock_move.call_args.kwargs
    assert kwargs.get("post_commonsdelinker") is False, (
        "Case 3 must suppress CommonsDelinker when actual_filename is a"
        " sibling slot that will be overwritten by a later ordinal."
    )


def test_case3_move_posts_commonsdelinker_when_actual_is_not_sibling():
    """The common case: existing file at a legacy NARA-bot title that's
    NOT in the current asset positions. CommonsDelinker request stays."""
    uploader = _build_uploader_with_dpla()
    intended = _make_intended_page(_ITEM_PAGE_8, exists=False)
    # Old NARA-bot title from an earlier scheme — not part of current asset list.
    legacy_title = "Foo - NAID 99999.jpg"
    expected_titles = {_ITEM_PAGE_8, _ITEM_PAGE_11}
    with (
        patch("tools.uploader.get_page", return_value=intended),
        patch.object(uploader, "_move_to_correct_title") as mock_move,
    ):
        action = uploader._resolve_hash_drift(
            existing_file=_drift_existing_file(legacy_title),
            page_title=_ITEM_PAGE_8,
            dpla_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ordinal=11,
            expected_item_titles=expected_titles,
        )
    assert action == "moved"
    kwargs = mock_move.call_args.kwargs
    assert kwargs.get("post_commonsdelinker") is True, (
        "Case 3 must post the CommonsDelinker request when actual_filename"
        " is a legitimate orphan (not one of this item's current slots)."
    )


def test_case1_move_suppresses_commonsdelinker_when_actual_is_sibling():
    """Case 1: intended title is a redirect pointing at actual_filename.
    The Case-1 move overwrites the redirect with the file. Same
    sibling-slot logic: if actual_filename is in expected_item_titles,
    suppress the CommonsDelinker request."""
    uploader = _build_uploader_with_dpla()
    intended = _make_intended_page(
        _ITEM_PAGE_8, exists=True, is_redirect=True, redirect_target=_ITEM_PAGE_11
    )
    expected_titles = {_ITEM_PAGE_8, _ITEM_PAGE_11}
    with (
        patch("tools.uploader.get_page", return_value=intended),
        patch.object(uploader, "_move_to_correct_title") as mock_move,
    ):
        action = uploader._resolve_hash_drift(
            existing_file=_drift_existing_file(_ITEM_PAGE_11),
            page_title=_ITEM_PAGE_8,
            dpla_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ordinal=11,
            expected_item_titles=expected_titles,
        )
    assert action == "moved"
    kwargs = mock_move.call_args.kwargs
    assert kwargs.get("post_commonsdelinker") is False


def test_move_to_correct_title_checks_usage_before_move_and_skips_when_unused():
    """The inbound-usage gate must run BEFORE the move: afterward the old
    title is a redirect and the usage query is unreliable. When the live
    file has no inbound usage, no CommonsDelinker request is posted."""
    uploader = _build_uploader_with_dpla()
    existing = _drift_existing_file("Old Title - DPLA - a (page 1).jpg")
    intended = _make_intended_page("New Title - DPLA - b (page 1).jpg")
    order = []
    existing.move.side_effect = lambda *a, **k: order.append("move")

    def _usage(_site, _name):
        order.append("usage_check")
        return False

    with (
        patch("tools.uploader.file_has_inbound_usage", side_effect=_usage),
        patch("tools.uploader.post_commonsdelinker_request") as mock_post,
    ):
        uploader._move_to_correct_title(existing, intended, "a", "Case 3")

    assert order == ["usage_check", "move"], (
        "usage must be checked before the move (old title becomes a redirect)"
    )
    mock_post.assert_not_called()


def test_move_to_correct_title_posts_with_check_usage_false_when_used():
    """When the live file IS used, the request is posted after the move with
    check_usage=False — the pre-move decision is authoritative, so the
    post-move (redirect) re-check is bypassed."""
    uploader = _build_uploader_with_dpla()
    existing = _drift_existing_file("Old Title - DPLA - a (page 1).jpg")
    intended = _make_intended_page("New Title - DPLA - b (page 1).jpg")
    order = []
    existing.move.side_effect = lambda *a, **k: order.append("move")

    def _usage(_site, _name):
        order.append("usage_check")
        return True

    with (
        patch("tools.uploader.file_has_inbound_usage", side_effect=_usage),
        patch("tools.uploader.post_commonsdelinker_request") as mock_post,
    ):
        uploader._move_to_correct_title(existing, intended, "a", "Case 3")

    assert order == ["usage_check", "move"]
    mock_post.assert_called_once()
    assert mock_post.call_args.kwargs.get("check_usage") is False


def test_move_to_correct_title_does_not_rewrite_description_defers_to_cleanup():
    """Deferral fix: after the title-drift move, the method must NOT rewrite the
    moved page's description. It previously blind-overwrote it (dropping
    community metadata like {{Creator:...}}); the community-preserving migration
    is now left to the post-SDC sdc-sync cleanup. Assert the move still happens
    but there is no post-move page fetch/save (the only get_page use in this
    method was the removed description-rewrite block)."""
    uploader = _build_uploader_with_dpla()
    existing = _drift_existing_file("Old Title - DPLA - a (page 1).jpg")
    intended = _make_intended_page("New Title - DPLA - b (page 1).jpg")
    with (
        patch("tools.uploader.file_has_inbound_usage", return_value=False),
        patch("tools.uploader.post_commonsdelinker_request"),
        patch("tools.uploader.get_page") as mock_get_page,
    ):
        uploader._move_to_correct_title(existing, intended, "b", "Case 3")
    existing.move.assert_called_once()
    mock_get_page.assert_not_called()


# ---------------------------------------------------------------------------
# Granular skip-class counters: NOT_PRESENT vs INELIGIBLE both bump the
# legacy ``SKIPPED`` aggregate AND a granular counter.
# ---------------------------------------------------------------------------


def test_track_ordinal_skip_bumps_both_legacy_and_granular_counters():
    """``_track_ordinal_skip(kind)`` increments ``Result.SKIPPED``
    (so legacy dashboards keep working) AND the granular ``kind``
    counter (so the Slack summary's breakdown is non-zero). Pinned
    so future refactors of the helper don't silently regress one of
    the two increments."""
    from tools.uploader import Uploader

    tracker = Tracker()
    uploader = Uploader.__new__(Uploader)  # bypass full __init__
    uploader.tracker = tracker

    uploader._track_ordinal_skip(Result.UPLOAD_SKIPPED_NOT_PRESENT)
    assert tracker.count(Result.SKIPPED) == 1
    assert tracker.count(Result.UPLOAD_SKIPPED_NOT_PRESENT) == 1
    assert tracker.count(Result.UPLOAD_SKIPPED_INELIGIBLE) == 0

    uploader._track_ordinal_skip(Result.UPLOAD_SKIPPED_INELIGIBLE)
    assert tracker.count(Result.SKIPPED) == 2
    assert tracker.count(Result.UPLOAD_SKIPPED_NOT_PRESENT) == 1
    assert tracker.count(Result.UPLOAD_SKIPPED_INELIGIBLE) == 1


# ---------------------------------------------------------------------------
# Post-upload pageid refresh retry: live bug repro on a 327 MB TIFF where
# Commons indexing lag returned pageid=0 on the first attempt.
# ---------------------------------------------------------------------------


def test_is_community_file_true_when_non_shaped_and_non_bot():
    """Non-DPLA/NARA-shaped title AND a non-bot uploader → community (both
    signals non-ours), so hands-off."""
    from tools import uploader as um

    up = _uploader_for_helper_tests()
    fp = _drift_existing_file("Grandma's quilt, summer 1932.jpg")
    with patch.object(um, "first_uploader", return_value="SomeVolunteer"):
        assert up._is_community_file(fp) is True


def test_is_community_file_false_for_dpla_or_nara_shaped_title():
    """A DPLA/NARA-shaped title is ours regardless of uploader — covers the
    'manual DPLA upload from a personal account' edge case."""
    from tools import uploader as um

    up = _uploader_for_helper_tests()
    with patch.object(um, "first_uploader", return_value="APersonalAccount"):
        assert (
            up._is_community_file(
                _drift_existing_file(
                    "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.jpg"
                )
            )
            is False
        )
        assert (
            up._is_community_file(_drift_existing_file('"Photo." - NARA - 12345.tif'))
            is False
        )


def test_is_community_file_false_for_non_shaped_but_bot_uploaded():
    """A bot upload with a malformed (non-shaped) title is still ours to
    fix — covers the 'bot uploaded a malformed title' edge case."""
    from tools import uploader as um

    up = _uploader_for_helper_tests()
    fp = _drift_existing_file("malformed title, no dpla marker.jpg")
    # Full canonical DPLA_BOT_ACCOUNTS set (incl. Flickr upload bot) plus an
    # underscore variant to exercise _normalize_account (DPLA_bot == DPLA bot).
    for bot in (
        "DPLA bot",
        "US National Archives bot",
        "Flickr upload bot",
        "DPLA_bot",
    ):
        with patch.object(um, "first_uploader", return_value=bot):
            assert up._is_community_file(fp) is False, bot


def test_is_community_file_true_when_uploader_unreadable_and_non_shaped():
    """Unreadable file history + non-shaped title → err toward community
    (hands-off) rather than risk touching a non-ours file."""
    from tools import uploader as um

    up = _uploader_for_helper_tests()
    fp = _drift_existing_file("mystery-scan.jpg")
    with patch.object(um, "first_uploader", side_effect=RuntimeError("no history")):
        assert up._is_community_file(fp) is True


def test_record_community_hand_fix_and_skip_uses_distinct_reason():
    """The community hand-fix records the distinct ``community_file`` reason,
    counts one HAND_FIX, returns ORDINAL_HAND_FIX, and never uploads."""
    from ingest_wikimedia.tracker import Result, Tracker
    from tools import uploader as um

    up = _uploader_for_helper_tests()
    up.tracker = Tracker()
    community = _drift_existing_file("Grandma's quilt.jpg")
    community.latest_file_info.sha1 = "deadbeef"
    recorded = {}
    with (
        patch.object(um, "first_uploader", return_value="Volunteer"),
        patch.object(
            um.hand_fix_sidecar,
            "record_hand_fix",
            side_effect=lambda *a, **kw: recorded.update(kw),
        ),
    ):
        result = up._record_community_hand_fix_and_skip(
            partner="ohio",
            dpla_id="abc",
            ordinal=1,
            our_sha1="deadbeef",
            intended_title="Foo - DPLA - abc.jpg",
            community_file=community,
        )
    assert result["status"] == um.ORDINAL_HAND_FIX
    assert result["title"] is None
    assert recorded["reason"] == "community_file"
    assert recorded["community_uploader"] == "Volunteer"
    assert up.tracker.count(Result.UPLOAD_HAND_FIX) == 1


def _uploader_for_helper_tests():
    """Build an ``Uploader`` instance bypassing ``__init__`` for direct
    invocation of internal helpers — sufficient when the helper only
    touches ``self.site`` and doesn't need a real tracker / S3 / DPLA
    wiring."""
    from tools.uploader import Uploader

    uploader = Uploader.__new__(Uploader)
    uploader.site = MagicMock(name="site")
    return uploader


def test_pageid_refresh_retries_until_indexed(monkeypatch):
    """Live-bug regression: the post-upload pageid refresh races
    Commons indexing on large (chunked) uploads. First attempts
    return 0; a subsequent attempt (after backoff) returns the real
    pageid. Calls the actual ``_refresh_pageid_with_retries`` helper
    on the ``Uploader`` class — per CR #302 review, an inline copy of
    the retry loop would silently pass while production diverges."""
    from tools import uploader as uploader_mod

    # Three FilePage instances returned in sequence by get_page —
    # first two have .pageid=0 (indexing lag), third has the real id.
    fresh_pages = [
        MagicMock(pageid=0),
        MagicMock(pageid=0),
        MagicMock(pageid=193644002),
    ]
    for p in fresh_pages:
        p.exists.return_value = True

    monkeypatch.setattr(
        uploader_mod, "get_page", lambda site, title: fresh_pages.pop(0)
    )
    monkeypatch.setattr(uploader_mod.time, "sleep", lambda s: None)
    monkeypatch.setattr(uploader_mod, "PAGEID_REFRESH_MAX_ATTEMPTS", 3)
    monkeypatch.setattr(uploader_mod, "PAGEID_REFRESH_BACKOFF_SECS", 0)

    uploader = _uploader_for_helper_tests()
    assert uploader._refresh_pageid_with_retries("File:X.tiff") == 193644002


def test_pageid_refresh_gives_up_after_max_attempts(monkeypatch):
    """Indexing lag that exceeds the retry budget (or a genuinely
    deleted page) returns ``None``. The upload itself still
    succeeded; the sidecar carries ``pageid: null`` and sdc-sync's
    title→pageid fallback recovers on the next run."""
    from tools import uploader as uploader_mod

    perpetually_lagging = MagicMock(pageid=0)
    perpetually_lagging.exists.return_value = True

    monkeypatch.setattr(
        uploader_mod, "get_page", lambda site, title: perpetually_lagging
    )
    monkeypatch.setattr(uploader_mod.time, "sleep", lambda s: None)
    monkeypatch.setattr(uploader_mod, "PAGEID_REFRESH_MAX_ATTEMPTS", 3)
    monkeypatch.setattr(uploader_mod, "PAGEID_REFRESH_BACKOFF_SECS", 0)

    uploader = _uploader_for_helper_tests()
    assert uploader._refresh_pageid_with_retries("File:X.tiff") is None


def test_pageid_refresh_returns_none_on_persistent_api_error(monkeypatch):
    """API failure on every attempt (network down, persistent rate
    limit) returns ``None`` rather than raising — the upload itself
    succeeded so the call must not bubble the refresh failure up
    through ``process_file``."""
    from tools import uploader as uploader_mod

    def raise_always(site, title):
        raise RuntimeError("boom")

    monkeypatch.setattr(uploader_mod, "get_page", raise_always)
    monkeypatch.setattr(uploader_mod.time, "sleep", lambda s: None)
    monkeypatch.setattr(uploader_mod, "PAGEID_REFRESH_MAX_ATTEMPTS", 3)
    monkeypatch.setattr(uploader_mod, "PAGEID_REFRESH_BACKOFF_SECS", 0)

    uploader = _uploader_for_helper_tests()
    assert uploader._refresh_pageid_with_retries("File:X.tiff") is None


def test_pageid_refresh_returns_immediately_on_first_success(monkeypatch):
    """Small-file fast path: typical pageid lookup succeeds on
    attempt 1 with zero added latency. Pin no-sleep + single
    ``get_page`` call so a future regression that always sleeps
    (or always retries) is caught."""
    from tools import uploader as uploader_mod

    fresh_page = MagicMock(pageid=42)
    fresh_page.exists.return_value = True

    get_page_calls = []

    def stub_get_page(site, title):
        get_page_calls.append(title)
        return fresh_page

    sleep_calls = []
    monkeypatch.setattr(uploader_mod, "get_page", stub_get_page)
    monkeypatch.setattr(uploader_mod.time, "sleep", lambda s: sleep_calls.append(s))
    monkeypatch.setattr(uploader_mod, "PAGEID_REFRESH_MAX_ATTEMPTS", 3)
    monkeypatch.setattr(uploader_mod, "PAGEID_REFRESH_BACKOFF_SECS", 4)

    uploader = _uploader_for_helper_tests()
    assert uploader._refresh_pageid_with_retries("File:X.tiff") == 42
    assert len(get_page_calls) == 1
    assert sleep_calls == [], "no sleep on first-attempt-success path"


# --------------------------------------------------------------------------
# Box-wide Commons-write budget wiring (shared with the SDC-sync phase).
# The uploader is single-process but must check out one slot per item so
# it counts as one writer against the shared cap. Verifies main() builds
# the budget from --workers-budget and wraps each process_item in acquire().
# --------------------------------------------------------------------------


def test_main_acquires_one_budget_slot_per_item():
    """main() must construct a WorkerSlotBudget from --workers-budget and
    enter its acquire() context exactly once per DPLA item, around the
    process_item call. A spy budget records every constructed instance
    (the uploader now builds two: a priority pool with the shared pool
    wired as its fallback) and counts context entries on the outer
    instance the per-item loop actually uses."""
    from click.testing import CliRunner

    import tools.uploader as up

    acquire_calls = []
    instances: list = []

    class _SpyBudget:
        def __init__(self, budget, slot_dir=None, fallback=None):
            self.budget = budget
            self.slot_dir = slot_dir
            self.fallback = fallback
            instances.append(self)

        def acquire(self):
            from contextlib import contextmanager

            outer = self

            @contextmanager
            def _cm():
                acquire_calls.append(outer)
                yield

            return _cm()

    processed = []

    # Mock the heavy setup seams so main() runs its loop without touching
    # AWS / Commons / disk. ToolsContext.init returns a context whose
    # getters yield mocks; the Uploader's process_item just records the id.
    fake_ctx = MagicMock()
    fake_ctx.get_tracker.return_value = Tracker()
    fake_ctx.get_local_fs.return_value = MagicMock()
    fake_ctx.get_s3_client.return_value = MagicMock()
    fake_dpla = MagicMock()
    fake_dpla.get_providers_data.return_value = {}
    fake_ctx.get_dpla.return_value = fake_dpla

    def fake_process_item(dpla_id, *a, **kw):
        processed.append(dpla_id)

    with (
        patch.object(up.ToolsContext, "init", return_value=fake_ctx),
        patch.object(up, "get_site", return_value=MagicMock()),
        patch.object(up, "CategoryEnsurer", return_value=MagicMock()),
        patch.object(up, "WorkerSlotBudget", _SpyBudget),
        patch.object(up, "setup_logging"),
        patch.object(up, "notify_phase_start"),
        patch.object(up, "notify_upload_complete"),
        patch.object(up, "_post_upload_touch_new_institutions"),
        patch.object(up, "load_ids", return_value=["id_a", "id_b", "id_c"]),
        patch.object(up.Uploader, "process_item", side_effect=fake_process_item),
    ):
        runner = CliRunner()
        with runner.isolated_filesystem():
            # ids_file is a click.File argument; create a throwaway file
            # (load_ids is mocked, so contents don't matter).
            with open("ids.csv", "w") as fh:
                fh.write("id_a\nid_b\nid_c\n")
            # ``--workers 1`` forces the legacy single-process for-loop
            # path this test targets — the ``workers > 1`` branch dispatches
            # into a multiprocessing.spawn Pool, which would re-import the
            # test module and hang under CliRunner.
            result = runner.invoke(
                up.main,
                ["ids.csv", "nara", "--workers-budget", "16", "--workers", "1"],
            )

    assert result.exit_code == 0, result.output
    assert processed == ["id_a", "id_b", "id_c"], "all items must be processed"
    # The uploader builds two budgets when --workers-budget > 0:
    #   * a shared-pool instance sized at --workers-budget (16), no fallback
    #   * a priority-pool instance sized at UPLOADER_PRIORITY_SLOTS, with
    #     the shared instance wired as its fallback
    # The per-item loop must acquire on the OUTER (priority) instance so
    # the priority>fallback acquisition order kicks in.
    from ingest_wikimedia.worker_slots import (
        UPLOADER_PRIORITY_SLOT_DIR,
        UPLOADER_PRIORITY_SLOTS,
    )

    assert len(instances) == 2, f"expected priority + shared budgets; got {instances}"
    shared = next(b for b in instances if b.fallback is None)
    priority = next(b for b in instances if b.fallback is not None)
    assert shared.budget == 16, "shared pool not sized from --workers-budget"
    assert priority.budget == UPLOADER_PRIORITY_SLOTS
    assert priority.slot_dir == UPLOADER_PRIORITY_SLOT_DIR
    assert priority.fallback is shared, "priority pool must point at shared as fallback"
    assert len(acquire_calls) == 3, (
        f"expected one slot acquire per item; got {len(acquire_calls)}"
    )
    assert all(b is priority for b in acquire_calls), (
        "per-item loop must acquire on the priority (outer) budget, not the shared one"
    )


def test_main_dispatches_to_pool_when_workers_gt_one():
    """When ``--workers > 1``, main() must skip the single-process for-loop
    and hand the item list to ``_run_upload_pool`` — the parent process
    should NOT call ``Uploader.process_item`` itself, since the workers
    do that inside their own processes."""
    from click.testing import CliRunner

    import tools.uploader as up

    pool_calls: list[dict] = []

    def fake_run_pool(**kwargs):
        pool_calls.append(kwargs)
        # Simulate the pool having populated the parent's newly_created
        # set (each worker's CategoryEnsurer union).
        kwargs["newly_created"].add("Q_worker_created")

    fake_ctx = MagicMock()
    fake_ctx.get_tracker.return_value = Tracker()
    fake_ctx.get_local_fs.return_value = MagicMock()
    fake_ctx.get_s3_client.return_value = MagicMock()
    fake_dpla = MagicMock()
    fake_dpla.get_providers_data.return_value = {}
    fake_ctx.get_dpla.return_value = fake_dpla

    fake_ensurer = MagicMock()
    fake_ensurer._newly_created = set()

    process_item_calls = []

    def track_process_item(*args, **kwargs):
        process_item_calls.append(args)

    with (
        patch.object(up.ToolsContext, "init", return_value=fake_ctx),
        patch.object(up, "get_site", return_value=MagicMock()),
        patch.object(up, "CategoryEnsurer", return_value=fake_ensurer),
        patch.object(up, "setup_logging"),
        patch.object(up, "notify_phase_start"),
        patch.object(up, "notify_upload_complete"),
        patch.object(up, "_post_upload_touch_new_institutions"),
        patch.object(up, "load_ids", return_value=["id_a", "id_b", "id_c"]),
        patch.object(up.Uploader, "process_item", side_effect=track_process_item),
        patch.object(up, "_run_upload_pool", side_effect=fake_run_pool),
    ):
        runner = CliRunner()
        with runner.isolated_filesystem():
            with open("ids.csv", "w") as fh:
                fh.write("id_a\nid_b\nid_c\n")
            result = runner.invoke(
                up.main,
                ["ids.csv", "nara", "--workers-budget", "16", "--workers", "3"],
            )

    assert result.exit_code == 0, result.output
    assert len(pool_calls) == 1, "pool dispatch must fire exactly once"
    kwargs = pool_calls[0]
    assert kwargs["workers"] == 3
    assert kwargs["dpla_ids"] == ["id_a", "id_b", "id_c"]
    assert kwargs["partner"] == "nara"
    # The deferred-count path is gone (the drain sidecar was removed); the
    # pool signature no longer carries a ``deferred`` dict.
    assert "deferred" not in kwargs, (
        "_run_upload_pool must no longer receive a deferred dict "
        "(drain sidecar removed in the SHA1-uniqueness redesign)"
    )
    assert kwargs["newly_created"] == {"Q_worker_created"}, (
        "pool must have received the parent-owned newly_created set "
        "so post-upload touches cover institutions any worker created"
    )
    assert process_item_calls == [], (
        "parent process must NOT call process_item itself when the pool runs"
    )
    assert "Q_worker_created" in fake_ensurer._newly_created, (
        "worker-created institution QIDs must be folded back into the "
        "parent ensurer so _post_upload_touch_new_institutions sees them"
    )


# ---------------------------------------------------------------------------
# Maintain-mode no-create fence: _safe_upload is the single sanctioned upload
# path. In no_create mode it must refuse to write to a File page that does not
# already exist (blocking new-page creation), while still allowing overwrites
# of existing pages. This is the safety backbone of maintain mode.
# ---------------------------------------------------------------------------


def _uploader(no_create: bool) -> Uploader:
    return Uploader(
        tracker=MagicMock(),
        local_fs=MagicMock(),
        s3_client=MagicMock(),
        dpla=MagicMock(),
        site=MagicMock(),
        category_ensurer=None,
        no_create=no_create,
    )


def _filepage(exists: bool, is_redirect: bool = False) -> MagicMock:
    page = MagicMock()
    page.exists.return_value = exists
    page.isRedirectPage.return_value = is_redirect
    page.title.return_value = "File:Example - DPLA - deadbeef (page 1).jpg"
    return page


def test_safe_upload_blocks_new_filepage_in_no_create_mode():
    uploader = _uploader(no_create=True)
    page = _filepage(exists=False)
    with pytest.raises(NewFilePageBlocked):
        uploader._safe_upload(
            filepage=page,
            source_filename="/tmp/x.jpg",
            comment="c",
            text="t",
            ignore_warnings=True,
            asynchronous=True,
            chunk_size=0,
        )
    # The fence must prevent the actual Commons write entirely.
    uploader.site.upload.assert_not_called()


def test_safe_upload_blocks_redirect_title_in_no_create_mode():
    # A redirect page reports exists() == True but holds no file of its own;
    # uploading there would create file content at a title that had none.
    uploader = _uploader(no_create=True)
    page = _filepage(exists=True, is_redirect=True)
    with pytest.raises(NewFilePageBlocked):
        uploader._safe_upload(filepage=page, source_filename="/tmp/x.jpg")
    uploader.site.upload.assert_not_called()


def test_safe_upload_allows_overwrite_of_existing_in_no_create_mode():
    uploader = _uploader(no_create=True)
    page = _filepage(exists=True)
    uploader.site.upload.return_value = "ok"
    result = uploader._safe_upload(
        filepage=page, source_filename="/tmp/x.jpg", comment="c", text="t"
    )
    assert result == "ok"
    uploader.site.upload.assert_called_once_with(
        filepage=page, source_filename="/tmp/x.jpg", comment="c", text="t"
    )


def test_safe_upload_allows_new_filepage_when_not_in_no_create_mode():
    # Default (normal upload) mode: creating a new page is allowed, so the
    # fence is inert and the existence check is never even consulted.
    uploader = _uploader(no_create=False)
    page = _filepage(exists=False)
    uploader.site.upload.return_value = "ok"
    result = uploader._safe_upload(filepage=page, source_filename="/tmp/x.jpg")
    assert result == "ok"
    uploader.site.upload.assert_called_once()
    page.exists.assert_not_called()


# ---------------------------------------------------------------------------
# process_file per-ordinal exception counting.
#
# Regression pin for the silent-failure bug: any uncaught exception in
# process_file that reaches the outer ``except Exception`` handler must
# increment ``Result.FAILED``. Pre-fix, ``handle_upload_exception`` logged
# a ``Failed: <reason>`` line but did NOT bump the counter, so entire
# classes of upload failures were absent from ``COUNTS: FAILED`` and the
# Slack summary.
#
# Concrete observed impact: the NARA Washington DC general-records run had
# 22,712 ``Failed: Unknown`` tracebacks in the upload log (all from CSRF
# ``KeyError`` after the pywikibot session invalidated) but ``COUNTS:
# FAILED: 13``. Downloader saw 43,439 ordinals; uploader accounted for
# 20,629 in its counters. The 22,810 gap = silent failures.
# ---------------------------------------------------------------------------


def _process_file_uploader(tracker: Tracker) -> Uploader:
    """Build an ``Uploader`` with a real ``Tracker`` (so counters are
    inspectable) and mocks for the S3 / site / dpla dependencies. Tests
    then arrange one of those mocks to raise the exception under test."""
    return Uploader(
        tracker=tracker,
        local_fs=MagicMock(),
        s3_client=MagicMock(),
        dpla=MagicMock(),
        site=MagicMock(),
        category_ensurer=None,
    )


def test_process_file_csrf_style_keyerror_counts_as_failed():
    """A ``KeyError`` raised deep inside process_file (mimicking the
    CSRF-token invalidation pywikibot emits when the session lapses)
    must land as ``Result.FAILED += 1`` — not as a silent log-only
    ``Failed: Unknown`` line. This is the whole point of the fix; if
    this test ever regresses, the operator's Slack summary starts
    lying about failure counts again."""
    tracker = Tracker()
    uploader = _process_file_uploader(tracker)
    # get_media_s3_path is the first S3 call after the try: — arranging it
    # to raise gets us into the outer ``except Exception`` catch without
    # having to mock the full upload pipeline. The specific exception type
    # doesn't matter here (the catch is bare); a KeyError models the
    # CSRF-invalid-session shape.
    uploader.s3_client.get_media_s3_path.side_effect = KeyError(
        "Invalid token 'csrf' for user 'DPLA bot' on commons:commons wiki."
    )
    with patch("tools.uploader.get_wiki_text", return_value="wt"):
        result = uploader.process_file(
            dpla_id="abc123",
            title="Some Title",
            item_metadata={},
            provider={},
            data_provider={},
            ordinal=1,
            partner="nara",
            page_label="",
            verbose=False,
            dry_run=False,
        )
    assert result["status"] == "FAILED"
    assert tracker.count(Result.FAILED) == 1, (
        f"expected FAILED == 1 after CSRF-style KeyError; got "
        f"{tracker.count(Result.FAILED)}. This is the silent-failure "
        f"regression — the generic catch must increment FAILED."
    )


def test_process_file_generic_exception_counts_as_failed():
    """Same as the CSRF test but for a generic ``RuntimeError`` — any
    exception falling into the outer catch must be counted, not just
    the specific pywikibot-session shape. Guards against a future
    fix that special-cases KeyError alone."""
    tracker = Tracker()
    uploader = _process_file_uploader(tracker)
    uploader.s3_client.get_media_s3_path.side_effect = RuntimeError(
        "some transient network error"
    )
    with patch("tools.uploader.get_wiki_text", return_value="wt"):
        result = uploader.process_file(
            dpla_id="abc",
            title="T",
            item_metadata={},
            provider={},
            data_provider={},
            ordinal=1,
            partner="nara",
            page_label="",
            verbose=False,
            dry_run=False,
        )
    assert result["status"] == "FAILED"
    assert tracker.count(Result.FAILED) == 1


def test_process_file_backend_fail_retry_exhaustion_counts_once():
    """The removed line-766 increment fired when the retry loop
    exhausted attempts on a backend-fail error. Its re-raised exception
    propagates to the outer catch at end-of-process_file (which now
    owns the counter increment), so leaving line 766 in would
    double-count. This test drives execution through the retry loop
    itself — mocks are wired to reach ``_safe_upload``, which raises a
    backend-fail-marker exception on every attempt so the loop
    exhausts and takes the else branch (the removed-increment site).
    Post-fix: FAILED == 1 exactly. Pre-fix: would have been 2.
    """
    from ingest_wikimedia.wikimedia import ERROR_BACKEND_FAIL

    tracker = Tracker()
    uploader = _process_file_uploader(tracker)

    # Make every retry attempt fail with a message containing the
    # backend-fail marker (triggers the ``is_backend_fail`` branch in
    # the retry loop).
    uploader._safe_upload = MagicMock(
        side_effect=RuntimeError(
            f"stashfailed: {ERROR_BACKEND_FAIL} simulated for test"
        )
    )

    # Pre-retry pipeline: mock just enough to reach the retry loop.
    #   * S3: ``get_media_s3_path`` → path, ``s3_file_exists`` → True,
    #     ``get_s3().Object(...)`` → 100 bytes of image/jpeg with a
    #     sha1 in metadata.
    #   * Hash-drift lookup: ``find_file_by_hash`` returns None so we
    #     skip the drift-correction branch and land in the plain-upload
    #     path. That path calls ``get_page(...)`` on a title from
    #     ``get_page_title(...)`` — mocked to a FilePage that isn't a
    #     redirect.
    #   * ``time.sleep`` is mocked so the retry backoff doesn't stall
    #     the test suite for 15+ seconds per test.
    uploader.s3_client.get_media_s3_path.return_value = "nara/images/a/b/c/d/abc/1_abc"
    uploader.s3_client.s3_file_exists.return_value = True
    fake_s3_object = MagicMock()
    fake_s3_object.content_length = 100
    fake_s3_object.metadata = {"sha1": "deadbeef" * 5}
    fake_s3_object.content_type = "image/jpeg"
    uploader.s3_client.get_s3.return_value.Object.return_value = fake_s3_object

    fake_page = MagicMock()
    fake_page.exists.return_value = False
    fake_page.isRedirectPage.return_value = False
    fake_page.title.return_value = (
        "File:Something - DPLA - abcdef1234567890abcdef1234567890 (page 1).jpg"
    )
    fake_page.pageid = 0

    with (
        patch("tools.uploader.get_wiki_text", return_value="wt"),
        patch("tools.uploader.find_file_by_hash", return_value=None),
        patch(
            "tools.uploader.get_page_title",
            return_value="File:Something - DPLA - abcdef1234567890abcdef1234567890 (page 1).jpg",
        ),
        patch("tools.uploader.get_page", return_value=fake_page),
        patch("tools.uploader.time.sleep"),  # skip retry backoff
    ):
        result = uploader.process_file(
            dpla_id="abc",
            title="Something",
            item_metadata={},
            provider={},
            data_provider={},
            ordinal=1,
            partner="nara",
            page_label="",
            verbose=False,
            dry_run=False,
        )

    assert result["status"] == "FAILED"
    # The retry loop exhausted MAX_UPLOAD_RETRIES attempts, then
    # re-raised into the outer ``except Exception`` catch. Exactly
    # one FAILED increment — pre-fix line 766 would have added a
    # second when ``is_backend_fail`` was true.
    assert tracker.count(Result.FAILED) == 1, (
        f"expected FAILED == 1 after backend-fail retry exhaustion; got "
        f"{tracker.count(Result.FAILED)}. If this is 2, the removed "
        f"line-766 increment came back and is double-counting with the "
        f"outer catch's increment."
    )
    # Sanity check: retry loop actually ran multiple attempts, not just
    # one. ``_safe_upload`` should have been submitted to the executor
    # ``MAX_UPLOAD_RETRIES`` times.
    from tools.uploader import MAX_UPLOAD_RETRIES

    assert uploader._safe_upload.call_count == MAX_UPLOAD_RETRIES, (
        f"retry loop didn't exhaust attempts — only "
        f"{uploader._safe_upload.call_count} of "
        f"{MAX_UPLOAD_RETRIES} tries. Test isn't exercising the "
        f"removed-increment site."
    )


def test_process_file_octet_stream_redetected_still_uploads():
    """An octet-stream S3 object whose MIME re-detects to a real image type
    (``file_downloaded=True``) must still flow through the collision/upload
    block and upload — not fall through to ORDINAL_INELIGIBLE. The bytes
    already fetched for re-detection are NOT re-downloaded (the fresh-upload
    download is guarded by ``if not file_downloaded``). Regression pin for the
    line-845 gate fix."""
    tracker = Tracker()
    uploader = _process_file_uploader(tracker)

    uploader.s3_client.get_media_s3_path.return_value = "nara/images/a/b/c/d/abc/1_abc"
    uploader.s3_client.s3_file_exists.return_value = True
    fake_s3_object = MagicMock()
    fake_s3_object.content_length = 100
    fake_s3_object.metadata = {"sha1": "deadbeef" * 5}
    fake_s3_object.content_type = "application/octet-stream"
    uploader.s3_client.get_s3.return_value.Object.return_value = fake_s3_object
    # MIME re-detection succeeds: octet-stream -> image/jpeg.
    uploader.local_fs.get_content_type.return_value = "image/jpeg"

    fake_page = MagicMock()
    fake_page.exists.return_value = False
    fake_page.isRedirectPage.return_value = False
    fake_page.pageid = 0

    # Upload succeeds; the post-upload pageid refresh returns a real id.
    uploader._safe_upload = MagicMock(return_value="ok")
    uploader._refresh_pageid_with_retries = MagicMock(return_value=4242)

    title = "File:Something - DPLA - abcdef1234567890abcdef1234567890 (page 1).jpg"
    with (
        patch("tools.uploader.get_wiki_text", return_value="wt"),
        patch("tools.uploader.find_file_by_hash", return_value=None),
        patch("tools.uploader.get_page_title", return_value=title),
        patch("tools.uploader.get_page", return_value=fake_page),
    ):
        result = uploader.process_file(
            dpla_id="abc",
            title="Something",
            item_metadata={},
            provider={},
            data_provider={},
            ordinal=1,
            partner="nara",
            page_label="",
            verbose=False,
            dry_run=False,
        )

    assert result["status"] == "UPLOADED"
    assert result["pageid"] == 4242
    # download_file called exactly once — the re-detection fetch. The
    # fresh-upload download must be skipped because the bytes are on disk.
    assert fake_s3_object.download_file.call_count == 1


def test_process_file_merge_and_redirect_none_expected_titles_no_crash():
    """A cross-item MERGE_AND_REDIRECT when ``expected_item_titles`` is None
    (the parameter default) must not raise TypeError on ``x in None`` — the
    within_item derivation is guarded, yielding within_item=False. Regression
    pin for the line-945 guard fix."""
    tracker = Tracker()
    uploader = _process_file_uploader(tracker)

    uploader.s3_client.get_media_s3_path.return_value = "nara/images/a/b/c/d/abc/1_abc"
    uploader.s3_client.s3_file_exists.return_value = True
    fake_s3_object = MagicMock()
    fake_s3_object.content_length = 100
    fake_s3_object.metadata = {"sha1": "deadbeef" * 5}
    fake_s3_object.content_type = "image/jpeg"
    uploader.s3_client.get_s3.return_value.Object.return_value = fake_s3_object

    existing = MagicMock()
    existing.title.return_value = (
        "Other - DPLA - ffffffffffffffffffffffffffffffff (page 1).jpg"
    )

    title = "File:Ours - DPLA - abcdef1234567890abcdef1234567890 (page 1).jpg"
    captured = {}

    def fake_merge(**kwargs):
        captured.update(kwargs)
        return {"status": "MERGED", "title": "x", "pageid": 1}

    with (
        patch("tools.uploader.get_wiki_text", return_value="wt"),
        patch("tools.uploader.find_file_by_hash", return_value=existing),
        patch("tools.uploader.get_page_title", return_value=title),
        patch.object(uploader, "_is_community_file", return_value=False),
        patch(
            "tools.uploader.is_dup_sha1_sibling_at_expected_title",
            return_value=False,
        ),
        patch.object(
            uploader,
            "_resolve_hash_drift",
            return_value=DriftResolution.MERGE_AND_REDIRECT,
        ),
        patch.object(uploader, "_merge_and_redirect", side_effect=fake_merge),
    ):
        result = uploader.process_file(
            dpla_id="abc",
            title="Ours",
            item_metadata={},
            provider={},
            data_provider={},
            ordinal=1,
            partner="nara",
            page_label="",
            verbose=False,
            dry_run=False,
            expected_item_titles=None,
        )

    assert result["status"] == "MERGED"
    assert captured["within_item"] is False


def test_process_file_sha1_lock_recheck_skips_on_concurrent_upload(tmp_path):
    """Double-checked locking, SKIP branch. The fast-path SHA1 lookup finds
    nothing, but the RE-CHECK under the per-SHA1 lock (enabled via
    sha1_lock_dir) finds the file now present at our intended title — a sibling
    worker uploaded it while we waited. process_file must SKIP (invariant
    already satisfied) rather than attempt a duplicate upload, and the real
    lock must be acquired then released (finally) without error."""
    tracker = Tracker()
    uploader = _process_file_uploader(tracker)
    uploader.sha1_lock_dir = str(tmp_path)  # enable the lock for this test

    uploader.s3_client.get_media_s3_path.return_value = "nara/images/a/b/c/d/abc/1_abc"
    uploader.s3_client.s3_file_exists.return_value = True
    fake_s3_object = MagicMock()
    fake_s3_object.content_length = 100
    fake_s3_object.metadata = {"sha1": "deadbeef" * 5}
    fake_s3_object.content_type = "image/jpeg"
    uploader.s3_client.get_s3.return_value.Object.return_value = fake_s3_object

    page_title = "Ours - DPLA - abcdef1234567890abcdef1234567890 (page 1).jpg"
    concurrent = MagicMock()
    concurrent.title.return_value = page_title  # title(with_ns=False) == intended
    concurrent.pageid = 4242

    with (
        patch("tools.uploader.get_wiki_text", return_value="wt"),
        # 1st call (fast path) -> None; 2nd call (re-check under lock) -> file.
        patch(
            "tools.uploader.find_file_by_hash", side_effect=[None, concurrent]
        ) as fbh,
        patch("tools.uploader.get_page_title", return_value=page_title),
        patch.object(uploader, "_safe_upload") as safe_upload,
    ):
        result = uploader.process_file(
            dpla_id="abc",
            title="Ours",
            item_metadata={},
            provider={},
            data_provider={},
            ordinal=1,
            partner="nara",
            page_label="",
            verbose=False,
            dry_run=False,
        )

    assert result["status"] == "SKIPPED"
    assert result["pageid"] == 4242
    assert fbh.call_count == 2  # fast-path lookup + under-lock re-check
    safe_upload.assert_not_called()  # no duplicate upload attempted
    assert tracker.count(Result.SKIPPED) == 1


def test_process_file_sha1_lock_recheck_resolves_collision_on_concurrent_drift(
    tmp_path,
):
    """Double-checked locking, collision branch. The re-check under the lock
    finds the SHA1 now on Commons at a DIFFERENT title (a concurrent sibling
    landed it there), so process_file resolves it via the normal collision
    path (merge-and-redirect) instead of uploading a duplicate."""
    tracker = Tracker()
    uploader = _process_file_uploader(tracker)
    uploader.sha1_lock_dir = str(tmp_path)

    uploader.s3_client.get_media_s3_path.return_value = "nara/images/a/b/c/d/abc/1_abc"
    uploader.s3_client.s3_file_exists.return_value = True
    fake_s3_object = MagicMock()
    fake_s3_object.content_length = 100
    fake_s3_object.metadata = {"sha1": "deadbeef" * 5}
    fake_s3_object.content_type = "image/jpeg"
    uploader.s3_client.get_s3.return_value.Object.return_value = fake_s3_object

    page_title = "Ours - DPLA - abcdef1234567890abcdef1234567890 (page 1).jpg"
    concurrent = MagicMock()
    concurrent.title.return_value = (
        "Other - DPLA - ffffffffffffffffffffffffffffffff (page 1).jpg"
    )

    with (
        patch("tools.uploader.get_wiki_text", return_value="wt"),
        patch(
            "tools.uploader.find_file_by_hash", side_effect=[None, concurrent]
        ) as fbh,
        patch("tools.uploader.get_page_title", return_value=page_title),
        patch.object(uploader, "_is_community_file", return_value=False),
        patch(
            "tools.uploader.is_dup_sha1_sibling_at_expected_title",
            return_value=False,
        ),
        patch.object(
            uploader,
            "_resolve_hash_drift",
            return_value=DriftResolution.MERGE_AND_REDIRECT,
        ),
        patch.object(
            uploader,
            "_merge_and_redirect",
            return_value={"status": "MERGED", "title": "x", "pageid": 1},
        ) as merge,
        patch.object(uploader, "_safe_upload") as safe_upload,
    ):
        result = uploader.process_file(
            dpla_id="abc",
            title="Ours",
            item_metadata={},
            provider={},
            data_provider={},
            ordinal=1,
            partner="nara",
            page_label="",
            verbose=False,
            dry_run=False,
        )

    assert result["status"] == "MERGED"
    assert fbh.call_count == 2
    merge.assert_called_once()
    safe_upload.assert_not_called()  # resolved as collision, not uploaded


def test_process_file_sha1_lock_acquire_failure_degrades_to_lockless_upload(tmp_path):
    """The per-SHA1 lock is an optimization, not a correctness dependency, so an
    acquire failure (e.g. a foreign-owned lock dir) must degrade to a lock-less
    upload — NOT fail the ordinal. On acquire error process_file logs and
    proceeds without the re-check; the upload still runs and Commons' own dedup
    remains the invariant backstop."""
    tracker = Tracker()
    uploader = _process_file_uploader(tracker)
    uploader.sha1_lock_dir = str(tmp_path)  # lock enabled...

    uploader.s3_client.get_media_s3_path.return_value = "nara/images/a/b/c/d/abc/1_abc"
    uploader.s3_client.s3_file_exists.return_value = True
    fake_s3_object = MagicMock()
    fake_s3_object.content_length = 100
    fake_s3_object.metadata = {"sha1": "deadbeef" * 5}
    fake_s3_object.content_type = "image/jpeg"
    uploader.s3_client.get_s3.return_value.Object.return_value = fake_s3_object

    fake_page = MagicMock()
    fake_page.exists.return_value = False
    fake_page.isRedirectPage.return_value = False
    fake_page.pageid = 0
    uploader._safe_upload = MagicMock(return_value="ok")
    uploader._refresh_pageid_with_retries = MagicMock(return_value=4242)

    title = "File:Something - DPLA - abcdef1234567890abcdef1234567890 (page 1).jpg"
    with (
        patch("tools.uploader.get_wiki_text", return_value="wt"),
        patch("tools.uploader.find_file_by_hash", return_value=None) as fbh,
        patch("tools.uploader.get_page_title", return_value=title),
        patch("tools.uploader.get_page", return_value=fake_page),
        # ...but acquiring it blows up (simulated lock-infra failure).
        patch(
            "tools.uploader.acquire_sha1_lock",
            side_effect=RuntimeError("lock dir owned by another uid"),
        ) as acquire,
    ):
        result = uploader.process_file(
            dpla_id="abc",
            title="Something",
            item_metadata={},
            provider={},
            data_provider={},
            ordinal=1,
            partner="nara",
            page_label="",
            verbose=False,
            dry_run=False,
        )

    acquire.assert_called_once()  # we tried to lock
    assert result["status"] == "UPLOADED"  # degraded, did NOT fail the ordinal
    assert tracker.count(Result.FAILED) == 0
    assert fbh.call_count == 1  # re-check skipped (acquire failed) — only fast path


# ---------------------------------------------------------------------------
# CSRF token recovery in the retry loop.
#
# Pins the contract: (a) recover the session on the first CSRF error,
# (b) cap total recoveries per run so an unrecoverable state can't loop,
# (c) escalate to ``CsrfRecoveryFailed`` that propagates past
# process_file's and process_item's generic ``except Exception`` catches
# — otherwise the fatal would be swallowed as "one FAILED ordinal" and
# the main loop would keep going.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Commons-dedup byte-drift skip + unhandled-drift-shape reporting.
# When ``site.upload()`` returns None, process_file used to raise
# ``RuntimeError("File linked to another page (possible ID drift)")`` and let
# the catch-all report it as FAILED + "Failed: Unknown". Empirically this
# was 91k+ Ohio PDFs with 1-byte S3-vs-Commons drift — Commons treats our
# re-upload as a duplicate of what it already has, nothing to repair, but
# every retry counted as FAILED. This suite pins the split:
#
#   * ``_detect_commons_dedup_skip`` returns SKIPPED when the target exists
#     at the intended title with a different SHA1 (increments a dedicated
#     ``UPLOAD_SKIPPED_COMMONS_DEDUP`` breakdown).
#   * The RuntimeError still raises when the target is a redirect, missing,
#     or has no readable SHA1 — genuine drift-repair gaps.
#   * ``handle_upload_exception`` maps the RuntimeError to a distinct
#     "unhandled drift shape" message so it doesn't collapse into
#     "Failed: Unknown".
# ---------------------------------------------------------------------------


def _dedup_uploader(tracker: Tracker) -> Uploader:
    return _process_file_uploader(tracker)


def _fake_filepage(
    *,
    exists: bool,
    is_redirect: bool,
    sha1: str | None,
    pageid: int = 42,
    canonical_title: str = "File:Foo (page 2).pdf",
):
    """Build a MagicMock that quacks like ``pywikibot.FilePage`` for the
    narrow surface ``_detect_commons_dedup_skip`` reads: ``exists()``,
    ``isRedirectPage()``, ``latest_file_info.sha1``, ``pageid``, and
    ``title(with_ns=False)``.

    The ``sha1=None`` branch simulates a fetch failure via a
    per-instance subclass whose ``latest_file_info`` property raises.
    Instance-scoped rather than mutating ``type(MagicMock)`` directly
    — a class-level assignment would persist on the shared MagicMock
    class and leak into unrelated tests, causing order-dependent
    failures wherever another test later reads ``latest_file_info``
    on any MagicMock."""
    if sha1 is None:

        class _RaisingLatestFileInfo(MagicMock):
            @property
            def latest_file_info(self):
                raise RuntimeError("no info")

        page = _RaisingLatestFileInfo()
    else:
        page = MagicMock()
        page.latest_file_info.sha1 = sha1
    page.exists.return_value = exists
    page.isRedirectPage.return_value = is_redirect
    page.pageid = pageid
    page.title.return_value = canonical_title
    return page


def test_detect_commons_dedup_skip_fires_when_target_has_different_sha1():
    """The observed 91k-event class: target exists at intended title with
    a real file whose SHA1 differs from ours. Return SKIPPED and bump the
    dedicated dedup counter (plus the generic SKIPPED, mirroring how
    UPLOAD_SKIPPED_NOT_PRESENT / _INELIGIBLE are counted)."""
    tracker = Tracker()
    uploader = _dedup_uploader(tracker)
    fake_page = _fake_filepage(exists=True, is_redirect=False, sha1="commons-sha1")
    with patch("tools.uploader.get_page", return_value=fake_page):
        result = uploader._detect_commons_dedup_skip(
            page_title="File:Foo (page 2).pdf",
            our_sha1="s3-sha1",
            dpla_id="abc",
            ordinal=2,
        )
    assert result is not None
    assert result["status"] == "SKIPPED"
    assert result["title"] == "File:Foo (page 2).pdf"
    assert result["pageid"] == 42
    assert tracker.count(Result.UPLOAD_SKIPPED_COMMONS_DEDUP) == 1
    assert tracker.count(Result.SKIPPED) == 1


def test_detect_commons_dedup_skip_returns_canonical_title_not_raw():
    """Regression: the skip-result ``title`` must be the pywikibot-
    normalized ``existing.title(with_ns=False)`` rather than the raw
    constructed ``page_title``. Downstream sidecars + SDC sync key on
    the Commons-stored title, so returning the raw form breaks
    equality checks — same rationale as
    ``_resolve_hash_drift``'s ALREADY_CORRECT branch. Fixture builds
    a canonical title that differs from the raw ``page_title`` argument
    so the assertion can distinguish the two."""
    tracker = Tracker()
    uploader = _dedup_uploader(tracker)
    fake_page = _fake_filepage(
        exists=True,
        is_redirect=False,
        sha1="commons-sha1",
        canonical_title="File:Foo  (page 2).pdf",  # double-space normalised
    )
    with patch("tools.uploader.get_page", return_value=fake_page):
        result = uploader._detect_commons_dedup_skip(
            page_title="File:Foo (page 2).pdf",  # single-space raw
            our_sha1="s3-sha1",
            dpla_id="abc",
            ordinal=2,
        )
    assert result is not None
    assert result["title"] == "File:Foo  (page 2).pdf", (
        f"expected canonical title from ``existing.title(with_ns=False)``; "
        f"got {result['title']!r}"
    )


def test_detect_commons_dedup_skip_returns_none_when_target_is_redirect():
    """Target is a redirect — this is NOT the byte-drift class. Fall
    through so the RuntimeError still raises and the shape stays
    visible for future drift-resolution work."""
    tracker = Tracker()
    uploader = _dedup_uploader(tracker)
    fake_page = _fake_filepage(exists=True, is_redirect=True, sha1="commons-sha1")
    with patch("tools.uploader.get_page", return_value=fake_page):
        result = uploader._detect_commons_dedup_skip(
            page_title="File:Foo (page 2).pdf",
            our_sha1="s3-sha1",
            dpla_id="abc",
            ordinal=2,
        )
    assert result is None
    assert tracker.count(Result.UPLOAD_SKIPPED_COMMONS_DEDUP) == 0


def test_detect_commons_dedup_skip_returns_none_when_target_missing():
    """Target doesn't exist. Also NOT byte-drift — probably a real
    upload-attempt failure. Fall through."""
    tracker = Tracker()
    uploader = _dedup_uploader(tracker)
    fake_page = _fake_filepage(exists=False, is_redirect=False, sha1=None)
    with patch("tools.uploader.get_page", return_value=fake_page):
        result = uploader._detect_commons_dedup_skip(
            page_title="File:Foo (page 2).pdf",
            our_sha1="s3-sha1",
            dpla_id="abc",
            ordinal=2,
        )
    assert result is None
    assert tracker.count(Result.UPLOAD_SKIPPED_COMMONS_DEDUP) == 0


def test_detect_commons_dedup_skip_returns_none_when_sha1_matches():
    """Target's SHA1 matches our S3 SHA1 — the pre-check should have
    caught this earlier. This branch guards against the (theoretical)
    case where the pre-check missed but the target genuinely holds
    our bytes. Not byte-drift; fall through."""
    tracker = Tracker()
    uploader = _dedup_uploader(tracker)
    fake_page = _fake_filepage(exists=True, is_redirect=False, sha1="same-sha1")
    with patch("tools.uploader.get_page", return_value=fake_page):
        result = uploader._detect_commons_dedup_skip(
            page_title="File:Foo (page 2).pdf",
            our_sha1="same-sha1",
            dpla_id="abc",
            ordinal=2,
        )
    assert result is None


def test_detect_commons_dedup_skip_returns_none_when_sha1_unreadable():
    """Can't read the target's SHA1 — either the FilePage lookup threw
    or ``latest_file_info`` raised. Don't classify as byte-drift when
    we can't verify; fall through to the RuntimeError so the shape
    stays visible."""
    tracker = Tracker()
    uploader = _dedup_uploader(tracker)
    fake_page = _fake_filepage(exists=True, is_redirect=False, sha1=None)
    with patch("tools.uploader.get_page", return_value=fake_page):
        result = uploader._detect_commons_dedup_skip(
            page_title="File:Foo (page 2).pdf",
            our_sha1="s3-sha1",
            dpla_id="abc",
            ordinal=2,
        )
    assert result is None


def test_detect_commons_dedup_skip_returns_none_when_get_page_raises():
    """API failure on the FilePage fetch itself — treat as
    can't-verify, fall through. Prevents a transient Commons hiccup
    from turning FAILEDs into false SKIPPEDs."""
    tracker = Tracker()
    uploader = _dedup_uploader(tracker)
    with patch("tools.uploader.get_page", side_effect=RuntimeError("network down")):
        result = uploader._detect_commons_dedup_skip(
            page_title="File:Foo (page 2).pdf",
            our_sha1="s3-sha1",
            dpla_id="abc",
            ordinal=2,
        )
    assert result is None


# ---------------------------------------------------------------------------
# ``_detect_commons_dedup_from_nochange_error`` — direct-upload path.
# Extends the byte-drift skip logic to the ``APIError(fileexists-no-change)``
# response shape (direct upload), symmetric to PR #383's None-return handling
# (chunked upload). Trust Commons's authoritative "your upload equals the
# current version of [[:File:X]]" statement: if X is our intended title, the
# upload invariant is satisfied at the correct title after server-side
# normalisation, and we skip cleanly with the dedup counter.
# ---------------------------------------------------------------------------


def _nochange_error(title: str) -> Exception:
    """Build an exception with the same message shape pywikibot raises
    from Commons's ``fileexists-no-change`` API response."""
    return RuntimeError(
        f"fileexists-no-change: The upload is an exact duplicate of the "
        f"current version of [[:{title}]]."
    )


def test_nochange_matches_intended_title_skips_with_dedup_counter():
    """b2bc51b… motivating case: direct upload → Commons responds
    fileexists-no-change naming our target title → invariant is
    satisfied at the correct title (Commons has our bytes post-
    normalisation). Skip cleanly."""
    tracker = Tracker()
    uploader = _dedup_uploader(tracker)
    fake_page = _fake_filepage(
        exists=True,
        is_redirect=False,
        sha1="commons-sha1",
        canonical_title="Final Report - DPLA - b2bc.pdf",
    )
    ex = _nochange_error("File:Final Report - DPLA - b2bc.pdf")
    with patch("tools.uploader.get_page", return_value=fake_page):
        result = uploader._detect_commons_dedup_from_nochange_error(
            ex,
            page_title="Final Report - DPLA - b2bc.pdf",
            our_sha1="s3-sha1",
            dpla_id="abc",
            ordinal=1,
        )
    assert result is not None
    assert result["status"] == "SKIPPED"
    assert tracker.count(Result.UPLOAD_SKIPPED_COMMONS_DEDUP) == 1
    assert tracker.count(Result.SKIPPED) == 1


def test_nochange_naming_different_title_falls_through_to_failed():
    """Defense-in-depth: if Commons's message names a title that
    isn't ours, DON'T classify as skip — it's a real cross-title
    situation that should stay as FAILED for investigation. The
    check is cheap and prevents mis-classifying real drift."""
    tracker = Tracker()
    uploader = _dedup_uploader(tracker)
    ex = _nochange_error("File:Some Other Unrelated File.pdf")
    with patch("tools.uploader.get_page") as get_page:
        result = uploader._detect_commons_dedup_from_nochange_error(
            ex,
            page_title="Our Intended - DPLA - xyz.pdf",
            our_sha1="s3-sha1",
            dpla_id="abc",
            ordinal=1,
        )
    assert result is None
    # get_page must not have been called — we fell through BEFORE the
    # target-state fetch. Otherwise, a mis-titled response could still
    # short-circuit into a skip if the target happened to exist.
    get_page.assert_not_called()
    assert tracker.count(Result.UPLOAD_SKIPPED_COMMONS_DEDUP) == 0


def test_nochange_without_nochange_marker_returns_none():
    """The helper must only fire on the specific ``no-change`` marker
    Commons emits. Any other exception falls through to the current
    FAILED handling."""
    tracker = Tracker()
    uploader = _dedup_uploader(tracker)
    ex = RuntimeError("some unrelated CSRF token failure")
    with patch("tools.uploader.get_page") as get_page:
        result = uploader._detect_commons_dedup_from_nochange_error(
            ex,
            page_title="Our Intended - DPLA - xyz.pdf",
            our_sha1="s3-sha1",
            dpla_id="abc",
            ordinal=1,
        )
    assert result is None
    get_page.assert_not_called()


def test_nochange_with_marker_but_unparseable_title_falls_through():
    """If the message somehow contains the ``no-change`` marker but
    doesn't parse into a title (unexpected message format), don't
    guess — return None so the caller keeps FAILED. Preserves the
    invariant that any classification-to-SKIP is backed by an
    explicit target-title match."""
    tracker = Tracker()
    uploader = _dedup_uploader(tracker)
    # Contains ``no-change`` but not the full ``exact duplicate of
    # the current version of [[:File:X]]`` pattern.
    ex = RuntimeError("fileexists-no-change: garbled response body")
    with patch("tools.uploader.get_page") as get_page:
        result = uploader._detect_commons_dedup_from_nochange_error(
            ex,
            page_title="Our Intended - DPLA - xyz.pdf",
            our_sha1="s3-sha1",
            dpla_id="abc",
            ordinal=1,
        )
    assert result is None
    get_page.assert_not_called()


def test_handle_upload_exception_maps_possible_id_drift_to_distinct_message(caplog):
    """A ``RuntimeError`` carrying the "possible ID drift" marker gets a
    distinct log message ("unhandled drift shape") instead of collapsing
    into the generic "Failed: Unknown". Reached only after
    ``_detect_commons_dedup_skip`` ruled out the byte-drift class, so
    the message intentionally reads as an actionable "we still need to
    handle this shape" flag."""
    import logging as _logging

    with caplog.at_level(_logging.ERROR):
        Uploader.handle_upload_exception(
            RuntimeError("File linked to another page (possible ID drift)")
        )
    text = caplog.text
    assert "Failed: File linked to another page (unhandled drift shape)" in text
    assert "Failed: Unknown" not in text


def _csrf_keyerror() -> KeyError:
    """Build a KeyError whose ``str()`` matches pywikibot's CSRF-invalid
    TokenWallet message. Uses the production marker constant so the
    detector and the test data can never drift apart."""
    return KeyError(
        f"{CSRF_TOKEN_ERROR_MARKER} for user 'DPLA bot' on commons:commons wiki."
    )


def test_is_csrf_token_error_matches_pywikibot_shape():
    """Substring match on ``str(KeyError(...))`` — Python wraps KeyError
    args in quotes on stringify, so the marker must appear inside those
    wrapping quotes."""
    assert is_csrf_token_error(_csrf_keyerror())


def test_is_csrf_token_error_rejects_unrelated_keyerror():
    """A KeyError from any other code path (e.g. dict miss on 'title')
    must not be treated as a CSRF error — otherwise a bug elsewhere
    could accidentally trip the whole session-abort escalation."""
    assert not is_csrf_token_error(KeyError("title"))
    assert not is_csrf_token_error(KeyError("csrf"))  # bare, no "Invalid token"


def test_is_csrf_token_error_rejects_non_keyerror():
    """A RuntimeError whose message happens to contain the CSRF marker
    isn't a real CSRF token error — pywikibot only raises this as
    KeyError. Guard against a false positive from log-string
    contamination."""
    assert not is_csrf_token_error(RuntimeError(CSRF_TOKEN_ERROR_MARKER))


def _csrf_retry_loop_uploader(tracker: Tracker) -> Uploader:
    """Mirrors ``_process_file_uploader`` but wires up the extra mocks
    needed to reach the retry loop (the CSRF handling site) rather than
    landing in the pre-loop outer catch."""
    uploader = Uploader(
        tracker=tracker,
        local_fs=MagicMock(),
        s3_client=MagicMock(),
        dpla=MagicMock(),
        site=MagicMock(),
        category_ensurer=None,
    )
    uploader.s3_client.get_media_s3_path.return_value = "p/a/t/h/abc/1_abc"
    uploader.s3_client.s3_file_exists.return_value = True
    fake_s3_object = MagicMock()
    fake_s3_object.content_length = 100
    fake_s3_object.metadata = {"sha1": "deadbeef" * 5}
    fake_s3_object.content_type = "image/jpeg"
    uploader.s3_client.get_s3.return_value.Object.return_value = fake_s3_object
    return uploader


def _csrf_process_file(uploader: Uploader):
    """Drive execution into the retry loop with all pre-retry mocks
    patched in. Returns the tuple ``(fake_page, result_or_exc)`` where
    the latter is the process_file return value or ``None`` if an
    exception was raised (caught outside this helper)."""
    fake_page = MagicMock()
    fake_page.exists.return_value = False
    fake_page.isRedirectPage.return_value = False
    fake_page.title.return_value = (
        "File:X - DPLA - abcdef1234567890abcdef1234567890 (page 1).jpg"
    )
    fake_page.pageid = 0
    with (
        patch("tools.uploader.get_wiki_text", return_value="wt"),
        patch("tools.uploader.find_file_by_hash", return_value=None),
        patch(
            "tools.uploader.get_page_title",
            return_value=(
                "File:X - DPLA - abcdef1234567890abcdef1234567890 (page 1).jpg"
            ),
        ),
        patch("tools.uploader.get_page", return_value=fake_page),
        patch("tools.uploader.time.sleep"),
    ):
        return uploader.process_file(
            dpla_id="abc",
            title="X",
            item_metadata={},
            provider={},
            data_provider={},
            ordinal=1,
            partner="nara",
            page_label="",
            verbose=False,
            dry_run=False,
        )


def test_csrf_error_triggers_session_recovery_and_retry():
    """First CSRF error → ``recover_commons_session`` fires and the
    retry loop advances to the next attempt. Verifies the recovery +
    re-attempt contract only — the second attempt is arranged to fail
    with a non-CSRF error so the test doesn't have to fake the
    post-upload FilePage refresh."""
    tracker = Tracker()
    uploader = _csrf_retry_loop_uploader(tracker)
    # First upload attempt raises CSRF KeyError; second raises a
    # non-CSRF error so the retry loop exits without exercising the
    # post-upload machinery. The point of this test is the RE-ATTEMPT
    # after recovery, not what happens on the second attempt.
    uploader._safe_upload = MagicMock(
        side_effect=[_csrf_keyerror(), RuntimeError("second attempt marker")]
    )
    with patch("tools.uploader.recover_commons_session") as recover_mock:
        _csrf_process_file(uploader)
    recover_mock.assert_called_once_with(uploader.site)
    assert uploader._safe_upload.call_count == 2, (
        f"retry loop did not re-attempt after CSRF recovery — "
        f"_safe_upload called {uploader._safe_upload.call_count} time(s), "
        f"expected 2. If this is 1, the recovery path returned instead "
        f"of continuing the loop."
    )
    from ingest_wikimedia import csrf

    assert csrf.session_recoveries_used() == 1


def test_csrf_errors_beyond_cap_raise_csrf_recovery_failed():
    """Once ``MAX_CSRF_RECOVERIES`` is reached, the next CSRF error
    raises ``CsrfRecoveryFailed`` — the exception must not be silently
    counted as FAILED. Bounded abort under a persistently invalid
    session."""
    tracker = Tracker()
    uploader = _csrf_retry_loop_uploader(tracker)
    # Pre-consume the cap so the very next CSRF error escalates. This is
    # a cleaner test than driving through many attempts (each of which
    # would exhaust MAX_UPLOAD_RETRIES per ordinal) and pins the cap
    # behavior specifically.
    from ingest_wikimedia import csrf

    csrf._session_recoveries_used = MAX_CSRF_RECOVERIES
    uploader._safe_upload = MagicMock(side_effect=_csrf_keyerror())
    with patch("tools.uploader.recover_commons_session") as recover_mock:
        with pytest.raises(CsrfRecoveryFailed):
            _csrf_process_file(uploader)
    # Recovery must NOT have been attempted — the cap was already at the
    # ceiling, so trying again would just loop the same failure.
    recover_mock.assert_not_called()
    # And nothing was silently counted as FAILED — the exception
    # propagates past process_file's outer catch (the whole point).
    assert tracker.count(Result.FAILED) == 0, (
        "CsrfRecoveryFailed must propagate past process_file's outer "
        "``except Exception`` — if this is nonzero, the generic catch "
        "swallowed the session-fatal escalation and the run would keep "
        "going, defeating the abort."
    )


def test_csrf_recovery_that_throws_escalates_to_csrf_recovery_failed():
    """If ``recover_commons_session`` itself throws (network down,
    credentials rotated, etc.), escalate to ``CsrfRecoveryFailed``
    immediately rather than looping recovery attempts against an
    unreachable auth endpoint."""
    tracker = Tracker()
    uploader = _csrf_retry_loop_uploader(tracker)
    uploader._safe_upload = MagicMock(side_effect=_csrf_keyerror())
    with patch(
        "tools.uploader.recover_commons_session",
        side_effect=RuntimeError("network unreachable"),
    ):
        with pytest.raises(CsrfRecoveryFailed):
            _csrf_process_file(uploader)
    assert tracker.count(Result.FAILED) == 0


def test_csrf_recovery_failed_propagates_past_process_item_outer_catch():
    """process_item wraps its body in an outer ``except Exception`` that
    increments FAILED. Without an explicit ``except CsrfRecoveryFailed:
    raise`` before it, the fatal would be swallowed as a per-ordinal
    FAILED and the main() loop would move on to the next item — every
    one of which would hit the same broken session. This test pins that
    CsrfRecoveryFailed propagates all the way out of process_item, so
    main() sees it and aborts the run.

    Raises CsrfRecoveryFailed from the very first call inside the try
    (``get_item_metadata``) — the propagation contract is about outer
    handler ordering (specific-before-generic), which is testable at
    the boundary without driving through the full ordinal pipeline.
    """
    tracker = Tracker()
    uploader = _csrf_retry_loop_uploader(tracker)
    uploader.s3_client.get_item_metadata.side_effect = CsrfRecoveryFailed(
        "session unrecoverable"
    )
    with pytest.raises(CsrfRecoveryFailed):
        uploader.process_item(
            dpla_id="abc",
            providers_json={},
            partner="nara",
            verbose=False,
            dry_run=False,
        )
    # The fatal must not have been counted as a plain FAILED — that
    # counter is reserved for per-ordinal failures the generic outer
    # catch legitimately owns after PR #349.
    assert tracker.count(Result.FAILED) == 0, (
        "CsrfRecoveryFailed was caught by process_item's generic "
        "``except Exception`` and counted as FAILED. Add an explicit "
        "``except CsrfRecoveryFailed: raise`` before the generic "
        "handler so the run aborts instead of looping the next item."
    )


# ---------------------------------------------------------------------------
# Phantom-drift defense-in-depth.
#
# ``get_page_title``'s ``item_title[:181]`` truncation used to leak a
# trailing space when the 181st character landed on whitespace, producing
# a raw Python title with a double-space run around the ``- DPLA -``
# separator. ``process_file``'s identity check
# (``existing_file.title(with_ns=False) == page_title``) then failed on
# the raw-string comparison, and ``_resolve_hash_drift`` risked falling
# through to a destructive branch (move-to-self / hand-fix) against what is
# really the same page. The canonicalized-equality guard returns
# ALREADY_CORRECT instead; these tests pin that guard for any future
# normalisation drift.
# ---------------------------------------------------------------------------


def test_canonicalize_commons_title_collapses_whitespace_runs():
    """The guard helper mirrors MediaWiki's ``.trim()`` +
    whitespace-run collapse on file titles."""
    from tools.uploader import _canonicalize_commons_title

    assert _canonicalize_commons_title("Foo  Bar") == "Foo Bar"
    assert _canonicalize_commons_title("  Foo Bar  ") == "Foo Bar"
    assert _canonicalize_commons_title("Foo\tBar") == "Foo Bar"
    assert _canonicalize_commons_title("Foo Bar") == "Foo Bar"


def test_canonicalize_commons_title_treats_underscores_as_space_equivalent():
    """MediaWiki treats ``_`` and space as equivalent in page titles
    (``File:X_Y`` and ``File:X Y`` are the same page). The guard MUST
    fold both forms — otherwise an uploader-constructed underscore
    title vs a previously-uploaded space title (same DPLA ID, same
    SHA1) reads as drift and misroutes through ``_resolve_hash_drift``
    instead of the ALREADY_CORRECT skip it deserves.
    """
    from tools.uploader import _canonicalize_commons_title

    # Direct underscore ↔ space equivalence.
    assert _canonicalize_commons_title("Foo_Bar") == _canonicalize_commons_title(
        "Foo Bar"
    )
    # Mixed underscore + space runs collapse to a single space.
    assert _canonicalize_commons_title("Foo _ Bar") == "Foo Bar"
    assert _canonicalize_commons_title("Foo__Bar") == "Foo Bar"
    assert _canonicalize_commons_title("Foo_ _Bar") == "Foo Bar"
    # Leading/trailing underscores are stripped alongside whitespace.
    assert _canonicalize_commons_title("_Foo Bar_") == "Foo Bar"
    # Realistic file-title shape with an underscore/space mismatch
    # between the uploader-constructed form and the already-uploaded
    # Commons form — must canonicalize to the same string.
    space_form = "COLL FRAZIER AUGUSTUS PH119 BX4 IMG100 - DPLA - aef63c89.jpg"
    underscore_form = "COLL_FRAZIER_AUGUSTUS_PH119_BX4_IMG100 - DPLA - aef63c89.jpg"
    assert _canonicalize_commons_title(space_form) == _canonicalize_commons_title(
        underscore_form
    )


def test_resolve_hash_drift_returns_already_correct_on_whitespace_normalized_match():
    """Regression: when the SHA1-lookup returns the file already at the
    intended title (after whitespace normalisation), ``_resolve_hash_drift``
    must NOT fall through to Case 2 (which would try to upload same-hash
    bytes and tag the file as duplicate of itself). Return the new
    ``already_correct`` sentinel so the caller records SKIPPED.

    Uses a raw ``page_title`` with a double-space run (the exact shape
    the pre-fix ``get_page_title`` truncation-lands-on-whitespace bug
    used to produce) against a pywikibot-normalized ``existing_file``
    title with the single-space form. Byte equality is false; the
    canonicalized-equality guard is what catches this case. If the
    guard regresses to raw equality (which would coincide with the
    line-468 identity check ``process_file`` already runs before
    calling here), this test fails.
    """
    uploader = _build_uploader_with_dpla()
    commons_title = "Item - DPLA - bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.gif"
    raw_page_title = "Item  - DPLA - bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.gif"  # 2 spaces
    assert commons_title != raw_page_title, (
        "test premise: titles must differ byte-wise so byte-equality won't fire"
    )
    intended_page = MagicMock()
    intended_page.exists.return_value = True
    intended_page.isRedirectPage.return_value = False
    intended_page.title.return_value = commons_title
    with patch("tools.uploader.get_page", return_value=intended_page):
        action = uploader._resolve_hash_drift(
            existing_file=_drift_existing_file(commons_title),
            page_title=raw_page_title,
            dpla_id="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ordinal=1,
        )
    assert action == "already_correct"


# ---------------------------------------------------------------------------
# _record_hand_fix_and_skip — the HAND_FIX terminal outcome.
#
# Our S3 SHA1 lives at a wrong Commons title and the intended title is
# occupied by a DIFFERENT file, so the bot can neither upload a duplicate nor
# clobber the occupant. Record the case to the hand-fix.jsonl sidecar, count
# it, and return ORDINAL_HAND_FIX — no upload.
# ---------------------------------------------------------------------------


def test_record_hand_fix_and_skip_records_sidecar_and_returns_hand_fix():
    """Returns ORDINAL_HAND_FIX (title/pageid None), records the occupant's
    title + SHA1 to the sidecar, bumps ``UPLOAD_HAND_FIX``, and never
    uploads."""
    tracker = Tracker()
    uploader = _process_file_uploader(tracker)
    dpla_id = "abcdef0123456789abcdef0123456789"
    intended_title = f"Item - DPLA - {dpla_id} (page 1).jpg"
    current_title = f"Legacy Title - NAID 12345 - {dpla_id}.jpg"

    occupant = MagicMock()
    occupant.exists.return_value = True
    occupant.isRedirectPage.return_value = False
    occupant.latest_file_info.sha1 = "occupant-sha1"
    occupant.title.return_value = intended_title

    our_current_file = _drift_existing_file(current_title)

    with (
        patch("tools.uploader.get_page", return_value=occupant),
        patch("tools.uploader.hand_fix_sidecar.record_hand_fix") as record_mock,
    ):
        result = uploader._record_hand_fix_and_skip(
            partner="nara",
            dpla_id=dpla_id,
            ordinal=1,
            our_sha1="our-sha1",
            intended_title=intended_title,
            our_current_file=our_current_file,
        )

    assert result == {"status": "HAND_FIX", "title": None, "pageid": None}
    assert tracker.count(Result.UPLOAD_HAND_FIX) == 1
    # No upload ever happens on the hand-fix path.
    uploader.site.upload.assert_not_called()
    record_mock.assert_called_once()
    kwargs = record_mock.call_args.kwargs
    assert record_mock.call_args.args == ("nara",)
    assert kwargs["dpla_id"] == dpla_id
    assert kwargs["ordinal"] == 1
    assert kwargs["our_sha1"] == "our-sha1"
    assert kwargs["intended_title"] == intended_title
    assert kwargs["occupying_title"] == intended_title
    assert kwargs["occupying_sha1"] == "occupant-sha1"
    assert kwargs["current_title"] == current_title


def test_record_hand_fix_and_skip_survives_occupant_inspection_failure():
    """If inspecting the occupant at the intended title raises (Commons
    hiccup), the method swallows it, records the case with null occupant
    fields, still counts it, and returns HAND_FIX."""
    tracker = Tracker()
    uploader = _process_file_uploader(tracker)
    dpla_id = "abcdef0123456789abcdef0123456789"
    intended_title = f"Item - DPLA - {dpla_id} (page 1).jpg"

    with (
        # Occupant inspection raises — the method must swallow it and still
        # record (occupying_title/sha1 = None) and count.
        patch("tools.uploader.get_page", side_effect=RuntimeError("commons down")),
        patch("tools.uploader.hand_fix_sidecar.record_hand_fix") as record_mock,
    ):
        result = uploader._record_hand_fix_and_skip(
            partner="nara",
            dpla_id=dpla_id,
            ordinal=1,
            our_sha1="our-sha1",
            intended_title=intended_title,
            our_current_file=_drift_existing_file("Wrong - DPLA - x.jpg"),
        )

    assert result["status"] == "HAND_FIX"
    assert tracker.count(Result.UPLOAD_HAND_FIX) == 1
    uploader.site.upload.assert_not_called()
    record_mock.assert_called_once()
    kwargs = record_mock.call_args.kwargs
    assert kwargs["intended_title"] == intended_title
    assert kwargs["occupying_title"] is None
    assert kwargs["occupying_sha1"] is None


# ---------------------------------------------------------------------------
# _merge_and_redirect / _merge_sdc_onto_canonical / _create_redirect_to_canonical
# — the MERGE_AND_REDIRECT terminal outcome (SHA1 centralization).
# ---------------------------------------------------------------------------


def _canonical_file(title: str, pageid: int) -> MagicMock:
    f = MagicMock()
    f.title.return_value = title
    f.pageid = pageid
    return f


def test_merge_and_redirect_merges_sdc_and_creates_redirect():
    """With a resolvable canonical pageid: merge SDC onto ``M<pageid>``,
    create the redirect, bump ``UPLOAD_MERGED_TO_CANONICAL``, and return an
    ORDINAL_MERGED result carrying the canonical title + pageid."""
    tracker = Tracker()
    uploader = _build_uploader_with_dpla()
    uploader.tracker = tracker
    canonical = _canonical_file("Canonical - DPLA - xxxx (page 1).jpg", 777)

    with (
        patch.object(uploader, "_merge_sdc_onto_canonical") as merge_mock,
        patch.object(
            uploader, "_create_redirect_to_canonical", return_value="created"
        ) as redirect_mock,
    ):
        result = uploader._merge_and_redirect(
            canonical_file=canonical,
            intended_title="Ours - DPLA - yyyy (page 1).jpg",
            dpla_id="yyyy",
            ordinal=1,
            partner="nara",
            page_label="1",
            within_item=True,
            sha1="a" * 40,
        )

    assert result == {
        "status": "MERGED",
        "title": "Canonical - DPLA - xxxx (page 1).jpg",
        "pageid": 777,
    }
    assert tracker.count(Result.UPLOAD_MERGED_TO_CANONICAL) == 1
    merge_mock.assert_called_once()
    assert merge_mock.call_args.kwargs["canonical_mediaid"] == "M777"
    assert merge_mock.call_args.kwargs["within_item"] is True
    assert merge_mock.call_args.kwargs["page_label"] == "1"
    redirect_mock.assert_called_once()
    assert (
        redirect_mock.call_args.kwargs["canonical_title"]
        == "Canonical - DPLA - xxxx (page 1).jpg"
    )


def test_merge_and_redirect_fails_when_canonical_pageid_falsy():
    """No resolvable canonical pageid → the SDC can't be merged, so return a
    retryable ORDINAL_FAILED and write NO redirect. Reporting MERGED (or leaving
    a redirect) here would strand the item's metadata: the redirect hides our
    title and MERGED excludes the ordinal from the SDC-sync phase, so the data
    would appear nowhere. Not counted as MERGED."""
    tracker = Tracker()
    uploader = _build_uploader_with_dpla()
    uploader.tracker = tracker
    canonical = _canonical_file("Canonical - DPLA - xxxx.jpg", 0)

    with (
        patch.object(uploader, "_merge_sdc_onto_canonical") as merge_mock,
        patch.object(uploader, "_create_redirect_to_canonical") as redirect_mock,
    ):
        result = uploader._merge_and_redirect(
            canonical_file=canonical,
            intended_title="Ours - DPLA - yyyy.jpg",
            dpla_id="yyyy",
            ordinal=1,
            partner="nara",
            page_label="",
            within_item=False,
            sha1="b" * 40,
        )

    merge_mock.assert_not_called()  # can't target M0
    redirect_mock.assert_not_called()  # no redirect over an unmerged item
    assert result["status"] == "FAILED"
    assert tracker.count(Result.UPLOAD_MERGED_TO_CANONICAL) == 0
    # The FAILED status is returned normally (not via process_file's except
    # block), so this branch must bump the tracker itself or COUNTS/Slack
    # would underreport the failure.
    assert tracker.count(Result.FAILED) == 1


def test_merge_and_redirect_fails_when_sdc_merge_fails():
    """The SDC merge reports failure (False) → return a retryable ORDINAL_FAILED
    and write NO redirect, so the whole MERGE_AND_REDIRECT re-runs next pass
    instead of terminally reporting MERGED for data that never landed."""
    tracker = Tracker()
    uploader = _build_uploader_with_dpla()
    uploader.tracker = tracker
    canonical = _canonical_file("Canonical - DPLA - xxxx (page 1).jpg", 777)

    with (
        patch.object(
            uploader, "_merge_sdc_onto_canonical", return_value=False
        ) as merge_mock,
        patch.object(uploader, "_create_redirect_to_canonical") as redirect_mock,
    ):
        result = uploader._merge_and_redirect(
            canonical_file=canonical,
            intended_title="Ours - DPLA - yyyy (page 1).jpg",
            dpla_id="yyyy",
            ordinal=1,
            partner="nara",
            page_label="1",
            within_item=True,
            sha1="a" * 40,
        )

    merge_mock.assert_called_once()
    redirect_mock.assert_not_called()
    assert result["status"] == "FAILED"
    assert tracker.count(Result.UPLOAD_MERGED_TO_CANONICAL) == 0
    # Returned normally, so this branch must count the failure itself.
    assert tracker.count(Result.FAILED) == 1


def test_merge_sdc_onto_canonical_within_item_passes_page_numbers():
    """Reads the item's staged sdc.json, json-parses it, points sdc_sync at
    our site, and calls merge_item_onto_canonical with the within-item page
    number as a P304 qualifier set."""
    uploader = _build_uploader_with_dpla()
    uploader.s3_client.get_sdc_json.return_value = '{"claims": {"P170": []}}'

    with patch("tools.sdc_sync.merge_item_onto_canonical") as merge_mock:
        import tools.sdc_sync as sdc_sync

        uploader._merge_sdc_onto_canonical(
            canonical_mediaid="M42",
            dpla_id="yyyy",
            partner="nara",
            page_label="3",
            within_item=True,
        )
        assert sdc_sync.site is uploader.site

    uploader.s3_client.get_sdc_json.assert_called_once_with("nara", "yyyy")
    merge_mock.assert_called_once()
    args = merge_mock.call_args.args
    assert args[0] == "M42"
    assert args[1] == "yyyy"
    assert args[2] == {"claims": {"P170": []}}
    assert merge_mock.call_args.kwargs["page_numbers"] == {"3"}


def test_merge_sdc_onto_canonical_cross_item_passes_no_page_numbers():
    """Cross-item duplication carries no page number → page_numbers=None."""
    uploader = _build_uploader_with_dpla()
    uploader.s3_client.get_sdc_json.return_value = "{}"

    with patch("tools.sdc_sync.merge_item_onto_canonical") as merge_mock:
        uploader._merge_sdc_onto_canonical(
            canonical_mediaid="M42",
            dpla_id="yyyy",
            partner="nara",
            page_label="3",  # ignored because within_item is False
            within_item=False,
        )

    assert merge_mock.call_args.kwargs["page_numbers"] is None


def test_merge_sdc_onto_canonical_skips_when_no_staged_sdc():
    """Missing / blank sdc.json → best-effort skip, merge not attempted."""
    uploader = _build_uploader_with_dpla()
    uploader.s3_client.get_sdc_json.return_value = None

    with patch("tools.sdc_sync.merge_item_onto_canonical") as merge_mock:
        uploader._merge_sdc_onto_canonical(
            canonical_mediaid="M42",
            dpla_id="yyyy",
            partner="nara",
            page_label="1",
            within_item=True,
        )

    merge_mock.assert_not_called()


def _redirect_intended_page(
    *, exists: bool, is_redirect: bool = False, redirect_target: str | None = None
) -> MagicMock:
    page = MagicMock()
    page.exists.return_value = exists
    page.isRedirectPage.return_value = is_redirect
    page.title.return_value = "File:Ours - DPLA - yyyy.jpg"
    if redirect_target is not None:
        rt = MagicMock()
        rt.title.return_value = redirect_target
        page.getRedirectTarget.return_value = rt
    return page


def test_create_redirect_to_canonical_creates_when_missing():
    """Missing intended title (not maintain mode) → write the #REDIRECT and
    save via with_csrf_recovery."""
    uploader = _uploader(no_create=False)
    page = _redirect_intended_page(exists=False)

    with (
        patch("tools.uploader.get_page", return_value=page),
        patch("tools.uploader.with_csrf_recovery", side_effect=lambda s, d, fn: fn()),
    ):
        uploader._create_redirect_to_canonical(
            intended_title="Ours - DPLA - yyyy.jpg",
            canonical_title="Canonical - DPLA - xxxx.jpg",
            dpla_id="yyyy",
            ordinal=1,
        )

    assert page.text == "#REDIRECT [[File:Canonical - DPLA - xxxx.jpg]]"
    page.save.assert_called_once()


def test_create_redirect_to_canonical_refuses_to_clobber_real_file():
    """A real (non-redirect) file at the intended title is left for a human —
    never overwritten with a redirect."""
    uploader = _uploader(no_create=False)
    page = _redirect_intended_page(exists=True, is_redirect=False)

    with (
        patch("tools.uploader.get_page", return_value=page),
        patch("tools.uploader.with_csrf_recovery") as csrf_mock,
    ):
        uploader._create_redirect_to_canonical(
            intended_title="Ours - DPLA - yyyy.jpg",
            canonical_title="Canonical - DPLA - xxxx.jpg",
            dpla_id="yyyy",
            ordinal=1,
        )

    page.save.assert_not_called()
    csrf_mock.assert_not_called()


def test_create_redirect_to_canonical_noops_when_already_redirect_to_canonical():
    """An intended title already redirecting to the canonical file is a
    no-op — no re-save."""
    uploader = _uploader(no_create=False)
    page = _redirect_intended_page(
        exists=True,
        is_redirect=True,
        redirect_target="Canonical - DPLA - xxxx.jpg",
    )

    with (
        patch("tools.uploader.get_page", return_value=page),
        patch("tools.uploader.with_csrf_recovery") as csrf_mock,
    ):
        uploader._create_redirect_to_canonical(
            intended_title="Ours - DPLA - yyyy.jpg",
            canonical_title="Canonical - DPLA - xxxx.jpg",
            dpla_id="yyyy",
            ordinal=1,
        )

    page.save.assert_not_called()
    csrf_mock.assert_not_called()


def test_create_redirect_to_canonical_respects_no_create_fence():
    """Maintain mode (no_create) must not create a redirect at a not-yet-
    existing intended title."""
    uploader = _uploader(no_create=True)
    page = _redirect_intended_page(exists=False)

    with (
        patch("tools.uploader.get_page", return_value=page),
        patch("tools.uploader.with_csrf_recovery", side_effect=lambda s, d, fn: fn()),
    ):
        outcome = uploader._create_redirect_to_canonical(
            intended_title="Ours - DPLA - yyyy.jpg",
            canonical_title="Canonical - DPLA - xxxx.jpg",
            dpla_id="yyyy",
            ordinal=1,
        )

    page.save.assert_not_called()
    assert outcome == "fenced"


def test_create_redirect_to_canonical_preserves_ancillary_wikitext():
    """Re-pointing an EXISTING redirect that carries a trailing category
    replaces ONLY the redirect line and keeps the category. Regression pin for
    the wikitext-preservation fix."""
    uploader = _uploader(no_create=False)
    page = _redirect_intended_page(
        exists=True, is_redirect=True, redirect_target="Old - DPLA - zzzz.jpg"
    )
    page.text = "#REDIRECT [[File:Old - DPLA - zzzz.jpg]]\n[[Category:Some category]]"

    with (
        patch("tools.uploader.get_page", return_value=page),
        patch("tools.uploader.with_csrf_recovery", side_effect=lambda s, d, fn: fn()),
    ):
        outcome = uploader._create_redirect_to_canonical(
            intended_title="Ours - DPLA - yyyy.jpg",
            canonical_title="Canonical - DPLA - xxxx.jpg",
            dpla_id="yyyy",
            ordinal=1,
        )

    assert outcome == "created"
    assert page.text == (
        "#REDIRECT [[File:Canonical - DPLA - xxxx.jpg]]\n[[Category:Some category]]"
    )
    page.save.assert_called_once()


def test_create_redirect_to_canonical_returns_created_for_new_page():
    """A brand-new redirect page reports the ``created`` outcome."""
    uploader = _uploader(no_create=False)
    page = _redirect_intended_page(exists=False)

    with (
        patch("tools.uploader.get_page", return_value=page),
        patch("tools.uploader.with_csrf_recovery", side_effect=lambda s, d, fn: fn()),
    ):
        outcome = uploader._create_redirect_to_canonical(
            intended_title="Ours - DPLA - yyyy.jpg",
            canonical_title="Canonical - DPLA - xxxx.jpg",
            dpla_id="yyyy",
            ordinal=1,
        )

    assert outcome == "created"


def test_merge_and_redirect_hand_fix_when_intended_title_holds_real_file():
    """If the intended title is occupied by a DIFFERENT real file, the redirect
    can't be established → report HAND_FIX (rename_blocked), NOT MERGED. The SDC
    merge still ran (add-only / idempotent, safe to leave in place). Regression
    pin for the blocked-redirect outcome routing."""
    tracker = Tracker()
    uploader = _build_uploader_with_dpla()
    uploader.tracker = tracker
    canonical = _canonical_file("Canonical - DPLA - xxxx (page 1).jpg", 777)

    # A DIFFERENT real (non-redirect) file occupies the intended title.
    occupant = MagicMock()
    occupant.exists.return_value = True
    occupant.isRedirectPage.return_value = False
    occupant.title.return_value = "Ours - DPLA - yyyy (page 1).jpg"
    occupant.latest_file_info.sha1 = "ffff" * 10

    with (
        patch.object(uploader, "_merge_sdc_onto_canonical") as merge_mock,
        patch("tools.uploader.get_page", return_value=occupant),
        patch("tools.uploader.hand_fix_sidecar.record_hand_fix") as record_mock,
    ):
        result = uploader._merge_and_redirect(
            canonical_file=canonical,
            intended_title="Ours - DPLA - yyyy (page 1).jpg",
            dpla_id="yyyy",
            ordinal=1,
            partner="nara",
            page_label="1",
            within_item=False,
            sha1="abcd" * 10,
        )

    merge_mock.assert_called_once()  # SDC merge still ran
    assert result["status"] == "HAND_FIX"
    assert tracker.count(Result.UPLOAD_MERGED_TO_CANONICAL) == 0
    assert tracker.count(Result.UPLOAD_HAND_FIX) == 1
    record_mock.assert_called_once()
    assert record_mock.call_args.kwargs["reason"] == "rename_blocked"
    # Our SHA1's canonical home is recorded as the current title.
    assert (
        record_mock.call_args.kwargs["current_title"]
        == "Canonical - DPLA - xxxx (page 1).jpg"
    )


def test_merge_and_redirect_fenced_in_maintain_mode_is_not_merged():
    """maintain-mode no-create fence blocks the net-new redirect page → report
    a would-create skip (INELIGIBLE), NOT MERGED."""
    tracker = Tracker()
    uploader = _uploader(no_create=True)
    uploader.tracker = tracker
    canonical = _canonical_file("Canonical - DPLA - xxxx (page 1).jpg", 777)
    page = _redirect_intended_page(exists=False)

    with (
        patch.object(uploader, "_merge_sdc_onto_canonical") as merge_mock,
        patch("tools.uploader.get_page", return_value=page),
    ):
        result = uploader._merge_and_redirect(
            canonical_file=canonical,
            intended_title="Ours - DPLA - yyyy (page 1).jpg",
            dpla_id="yyyy",
            ordinal=1,
            partner="nara",
            page_label="1",
            within_item=False,
            sha1="abcd" * 10,
        )

    merge_mock.assert_called_once()
    assert result["status"] == "INELIGIBLE"
    assert tracker.count(Result.UPLOAD_MERGED_TO_CANONICAL) == 0
    assert tracker.count(Result.UPLOAD_SKIPPED_WOULD_CREATE) == 1
