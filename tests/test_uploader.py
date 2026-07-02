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

from tools.uploader import (
    LARGE_FILE_DIRECT_UPLOAD_LIMIT_BYTES,
    MAX_CSRF_RECOVERIES,
    CsrfRecoveryFailed,
    NewFilePageBlocked,
    Uploader,
    _CSRF_TOKEN_ERROR_MARKER,
    _is_csrf_token_error,
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
# _post_item_orphan_check
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
        return page

    return _factory


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


def test_orphan_check_tags_trailing_orphan_with_matching_sha1():
    """Item has 3 jpgs, (page 4).jpg exists with SHA1 of (page 3) → tag it."""
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({1: "aaa", 2: "bbb", 3: "ccc"})
    ordinal_exts = {1: ".jpg", 2: ".jpg", 3: ".jpg"}
    page_labels = {1: "1", 2: "2", 3: "3"}
    base = "Some Item - DPLA - abcd1234abcd1234abcd1234abcd1234"
    orphan_title = f"{base} (page 4).jpg"
    expected_keep = f"{base} (page 3).jpg"

    with (
        patch("tools.uploader.pywikibot.FilePage") as fp,
        patch("tools.uploader.tag_as_duplicate") as tag_mock,
    ):
        # keep_title must also exist on Commons — otherwise we'd flag, not tag.
        fp.side_effect = _make_file_page_factory(
            existing={orphan_title: "ccc", expected_keep: "ccc"}
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

    assert tracker.count(Result.ORPHANS_TAGGED) == 1
    assert tracker.count(Result.ORPHANS_FLAGGED) == 0
    tag_mock.assert_called_once()
    kwargs = tag_mock.call_args.kwargs
    assert kwargs["correct_filename"] == expected_keep


def test_orphan_check_flags_orphan_with_unknown_sha1():
    """Orphan exists but SHA1 isn't one of this item's S3 assets → flag, don't tag."""
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({1: "aaa", 2: "bbb"})
    ordinal_exts = {1: ".jpg", 2: ".jpg"}
    page_labels = {1: "1", 2: "2"}
    orphan_title = "T - DPLA - abcd1234abcd1234abcd1234abcd1234 (page 3).jpg"

    with (
        patch("tools.uploader.pywikibot.FilePage") as fp,
        patch("tools.uploader.tag_as_duplicate") as tag_mock,
    ):
        fp.side_effect = _make_file_page_factory(existing={orphan_title: "zzz_unknown"})
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
    tag_mock.assert_not_called()


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

    with (
        patch("tools.uploader.pywikibot.FilePage") as fp,
        patch("tools.uploader.tag_as_duplicate") as tag_mock,
    ):
        fp.side_effect = _make_file_page_factory(existing=existing)
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

    assert tracker.count(Result.ORPHANS_TAGGED) == 2
    assert tag_mock.call_count == 2
    keep_targets = {c.kwargs["correct_filename"] for c in tag_mock.call_args_list}
    assert keep_targets == {f"{base} (page 1).jpg", f"{base} (page 2).jpg"}


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

    with (
        patch("tools.uploader.pywikibot.FilePage") as fp,
        patch("tools.uploader.tag_as_duplicate") as tag_mock,
    ):
        fp.side_effect = _make_file_page_factory(existing=existing)
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

    assert tracker.count(Result.ORPHANS_TAGGED) == 1
    tag_mock.assert_called_once()
    assert tag_mock.call_args.kwargs["correct_filename"] == f"{base}.jpg"


def test_orphan_check_dry_run_does_not_save():
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({1: "aaa", 2: "bbb"})
    ordinal_exts = {1: ".jpg", 2: ".jpg"}
    page_labels = {1: "1", 2: "2"}
    base = "Item - DPLA - abcd1234abcd1234abcd1234abcd1234"
    existing = {
        f"{base} (page 2).jpg": "bbb",  # kept target must exist on Commons
        f"{base} (page 3).jpg": "bbb",  # orphan
    }

    with (
        patch("tools.uploader.pywikibot.FilePage") as fp,
        patch("tools.uploader.tag_as_duplicate") as tag_mock,
    ):
        fp.side_effect = _make_file_page_factory(existing=existing)
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

    # Dry-run still increments TAGGED (it counts the intent), but does not
    # actually call tag_as_duplicate.
    assert tracker.count(Result.ORPHANS_TAGGED) == 1
    tag_mock.assert_not_called()


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

    with (
        patch("tools.uploader.pywikibot.FilePage") as fp,
        patch("tools.uploader.tag_as_duplicate") as tag_mock,
    ):
        fp.side_effect = _make_file_page_factory(existing=existing)
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

    assert tracker.count(Result.ORPHANS_TAGGED) == 1
    tag_mock.assert_called_once()
    assert tag_mock.call_args.kwargs["correct_filename"] == f"{base}.jpg"


def test_orphan_check_flags_when_keep_title_does_not_exist():
    """If the S3 asset whose SHA1 matches the orphan was never actually
    uploaded (e.g. process_file SKIPPED it or aborted on timeout), the
    keep_title we'd point at doesn't exist on Commons.  Don't create a
    duplicate tag pointing at a phantom file — flag instead.
    """
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({1: "aaa", 2: "bbb"})
    ordinal_exts = {1: ".jpg", 2: ".jpg"}
    page_labels = {1: "1", 2: "2"}
    base = "Item - DPLA - abcd1234abcd1234abcd1234abcd1234"
    # Orphan exists, but the kept target (page 2).jpg does NOT exist on Commons
    # (e.g. process_file skipped that ordinal).
    existing = {f"{base} (page 3).jpg": "bbb"}

    with (
        patch("tools.uploader.pywikibot.FilePage") as fp,
        patch("tools.uploader.tag_as_duplicate") as tag_mock,
    ):
        fp.side_effect = _make_file_page_factory(existing=existing)
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
    tag_mock.assert_not_called()


def test_orphan_check_flags_when_tag_save_fails():
    """tag_as_duplicate raising shouldn't leave the orphan uncounted —
    record it as FLAGGED so the run summary captures follow-up work."""
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({1: "aaa", 2: "bbb"})
    ordinal_exts = {1: ".jpg", 2: ".jpg"}
    page_labels = {1: "1", 2: "2"}
    base = "Item - DPLA - abcd1234abcd1234abcd1234abcd1234"
    existing = {
        f"{base} (page 2).jpg": "bbb",  # keep target exists
        f"{base} (page 3).jpg": "bbb",  # orphan
    }

    with (
        patch("tools.uploader.pywikibot.FilePage") as fp,
        patch(
            "tools.uploader.tag_as_duplicate",
            side_effect=RuntimeError("simulated save failure"),
        ) as tag_mock,
    ):
        fp.side_effect = _make_file_page_factory(existing=existing)
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

    assert tag_mock.called  # we did try
    assert tracker.count(Result.ORPHANS_TAGGED) == 0
    assert tracker.count(Result.ORPHANS_FLAGGED) == 1


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

    with (
        patch("tools.uploader.pywikibot.FilePage") as fp,
        patch("tools.uploader.tag_as_duplicate") as tag_mock,
    ):
        fp.side_effect = _make_file_page_factory(existing=existing)
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
    tag_mock.assert_not_called()
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
    """Regression: a (page N+1) title that is already a #REDIRECT to a kept
    asset must NOT be tagged as a duplicate.

    The bug: pywikibot's `latest_file_info.sha1` on a redirect *follows*
    the redirect and returns the target file's sha1, so the orphan check's
    `orphan_sha1 in sha1_to_kept` lookup always matched, and the bot tagged
    the redirect page with a `{{Duplicate}}` template — producing:

        {{Duplicate|<target>|Trailing-page orphan: ...}}
        #REDIRECT [[<target>]]

    which is meaningless (the redirect already does what {{Duplicate}}
    flags) and pollutes the page with a stray template above the redirect.

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

    with (
        patch("tools.uploader.pywikibot.FilePage") as fp,
        patch("tools.uploader.tag_as_duplicate") as tag_mock,
    ):
        # (page 4) exists, is a redirect, and pywikibot's latest_file_info
        # would (mis)report the target's sha1 ("ccc"). (page 3) exists as a
        # real file with the same sha1.
        fp.side_effect = _make_file_page_factory(
            existing={redirect_title: "ccc", keep_title: "ccc"},
            redirects={redirect_title},
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

    # The redirect must NOT have been tagged.
    tag_mock.assert_not_called()
    assert tracker.count(Result.ORPHANS_TAGGED) == 0
    # Nor was it flagged for follow-up — a redirect is already doing what
    # we'd flag it for; no manual review needed.
    assert tracker.count(Result.ORPHANS_FLAGGED) == 0


def test_orphan_check_skips_redirect_but_continues_probing():
    """A redirect at (page N+1) doesn't stop the probe — if (page N+2) is
    a real orphan file, it should still get tagged."""
    tracker = Tracker()
    s3_client = _stub_s3_client_for_assets({1: "aaa", 2: "bbb", 3: "ccc"})
    ordinal_exts = {1: ".jpg", 2: ".jpg", 3: ".jpg"}
    page_labels = {1: "1", 2: "2", 3: "3"}
    base = "Some Item - DPLA - abcd1234abcd1234abcd1234abcd1234"
    redirect_title = f"{base} (page 4).jpg"
    real_orphan_title = f"{base} (page 5).jpg"
    keep_title = f"{base} (page 3).jpg"

    with (
        patch("tools.uploader.pywikibot.FilePage") as fp,
        patch("tools.uploader.tag_as_duplicate") as tag_mock,
    ):
        # (page 4) is a redirect (will be skipped).
        # (page 5) is a real file with the matching sha1 — should be tagged.
        # (page 3) exists as the kept target.
        fp.side_effect = _make_file_page_factory(
            existing={
                redirect_title: "ccc",
                real_orphan_title: "ccc",
                keep_title: "ccc",
            },
            redirects={redirect_title},
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

    # Exactly one orphan should be tagged (the real one at page 5), and it
    # must point at the kept title.
    assert tag_mock.call_count == 1
    kwargs = tag_mock.call_args.kwargs
    assert kwargs["correct_filename"] == keep_title
    # The first positional arg is the candidate FilePage; verify it's the
    # real orphan, not the redirect.
    candidate_arg = tag_mock.call_args.args[1]
    assert candidate_arg.title() == real_orphan_title
    assert tracker.count(Result.ORPHANS_TAGGED) == 1
    assert tracker.count(Result.ORPHANS_FLAGGED) == 0


# --------------------------------------------------------------------------
# Uploader._resolve_hash_drift — Case 2 skips tag when title is in asset list
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


def test_resolve_hash_drift_case2_skips_tag_when_existing_in_expected_titles():
    """The bug fix: when existing file's title is one of THIS item's other
    current asset positions, it's a sibling, not an orphan. The duplicate tag
    would be wasted (the sibling's content will be overwritten by its own
    ordinal in this same run), so route to leave_others_alone."""
    uploader = _build_uploader_with_dpla()
    # Existing file: page N+1 of the SAME item, will be processed soon.
    existing_title = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 5).jpg"
    intended_title = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 4).jpg"
    expected_titles = {
        f"Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page {n}).jpg"
        for n in range(1, 10)
    }
    # intended_page: exists with different content (Case 2 territory).
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
            wiki_markup="",
            expected_item_titles=expected_titles,
        )
    assert action == "leave_others_alone"


def test_resolve_hash_drift_case2_still_tags_when_existing_is_true_orphan():
    """When the existing file's title is NOT in the current asset list (i.e.
    it's a trailing orphan beyond what the source authorizes), the tag is
    still correct and Case 2 should fire as before."""
    uploader = _build_uploader_with_dpla()
    # Existing file at (page 99) — way beyond our asset list of pages 1..9.
    existing_title = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 99).jpg"
    intended_title = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 4).jpg"
    expected_titles = {
        f"Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page {n}).jpg"
        for n in range(1, 10)
    }
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
            wiki_markup="",
            expected_item_titles=expected_titles,
        )
    assert action == "upload_and_tag"


def test_resolve_hash_drift_case2_tags_when_expected_titles_is_none():
    """Backward-compat: callers that don't pass expected_item_titles see
    Case 2 firing as before (no behavior change in that path)."""
    uploader = _build_uploader_with_dpla()
    existing_title = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 5).jpg"
    intended_title = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 4).jpg"
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
            wiki_markup="",
        )
    assert action == "upload_and_tag"


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
    """REGRESSION: when the colliding file's DPLA ID returns 404, the
    bot used to silently fall back to ``leave_others_alone`` — producing a
    duplicate on Commons beside the orphaned older title. The 404 is
    in fact the strongest possible signal that the existing file is
    an orphan from a removed DPLA item; we now legitimately own this
    content under the new ID and should migrate (move/tag), not
    duplicate. This is the bug that flooded the Indiana Riley Old
    Home Society session with duplicates on 2026-06-15."""
    uploader = _build_uploader_with_dpla_raising(_make_http_404_error())
    existing_title = "Item - DPLA - bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb (page 2).jpg"
    intended_title = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 2).jpg"
    # Case 3 setup: nothing at the intended title → simple move. We
    # patch _move_to_correct_title so the test doesn't drive a real
    # pywikibot move; the assertion is just that we don't bail out
    # to ``leave_others_alone``.
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
            wiki_markup="",
        )
    assert action == "moved", (
        "404 on the colliding DPLA ID is the rename signal — the orphan "
        "must be migrated to our new title, not left in place beside a "
        "duplicate upload."
    )


def test_resolve_hash_drift_non_404_exception_stays_on_leave_others_alone():
    """Conservative fallback: non-404 exceptions (network timeout, 5xx,
    JSON parse error, etc.) don't carry the same "old ID is gone"
    meaning a 404 does. Keep these on the existing ``leave_others_alone``
    path so a transient DPLA API blip doesn't trigger a destructive
    move on a file that still has a valid sibling item."""

    class FlakyConnError(Exception):
        pass

    uploader = _build_uploader_with_dpla_raising(FlakyConnError("connection reset"))
    existing_title = "Item - DPLA - bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb (page 2).jpg"
    intended_title = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 2).jpg"
    # No need to set up intended_page — leave_others_alone returns before get_page.
    action = uploader._resolve_hash_drift(
        existing_file=_drift_existing_file(existing_title),
        page_title=intended_title,
        dpla_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ordinal=2,
        wiki_markup="",
    )
    assert action == "leave_others_alone"


def test_resolve_hash_drift_5xx_response_stays_on_leave_others_alone():
    """Sister case to the 404 test: a 5xx-shaped HTTPError must NOT
    take the 404 branch. Pin the exact status check so a refactor that
    broadens the gate (e.g. to ``status >= 400``) is caught."""
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
        wiki_markup="",
    )
    assert action == "leave_others_alone"


def test_resolve_hash_drift_valid_cross_item_collision_leaves_others_alone():
    """Existing positive case (no regression): when the colliding DPLA
    item IS still valid, the cross-item-collision branch returns
    ``leave_others_alone`` — our hash gets its own title and we leave the
    other valid item's file untouched."""
    uploader = _build_uploader_with_dpla(other_item_exists=True)
    existing_title = "Item - DPLA - bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb (page 2).jpg"
    intended_title = "Item - DPLA - aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa (page 2).jpg"
    action = uploader._resolve_hash_drift(
        existing_file=_drift_existing_file(existing_title),
        page_title=intended_title,
        dpla_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ordinal=2,
        wiki_markup="",
    )
    assert action == "leave_others_alone"


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
    """Hash-drift leave_others_alone on a small file → direct upload (warning bypass)."""
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
    """Hash-drift leave_others_alone on a 211 MB file (the NARA incident): chunked,
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
# is_dup_sha1_sibling_at_expected_title — the leave_others_alone short-circuit gate
#
# Pin the contract that prevents the orphan-duplicate bug observed on item
# fe6f59a29fddf8e3483e91ad805bf039 (Zuni in costume). NARA's mediaMaster
# listed the same TIF URL at two positions, so duplicate_source_sha1s
# contained that SHA1. The pre-fix code unconditionally treated any
# existing file with a matching SHA1 as a sibling, leaving the 2011 legacy
# NARA-bot title in place and uploading (page 1).tiff and (page 2).tiff
# beside it — three Commons files with the same SHA1. The helper must
# return True only when the existing title is one of THIS item's own
# expected current titles.
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
            wiki_markup="",
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
            wiki_markup="",
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
            wiki_markup="",
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
            result = runner.invoke(
                up.main, ["ids.csv", "nara", "--workers-budget", "16"]
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


# ---------------------------------------------------------------------------
# _tag_drift_duplicate — rescue + tag combined operation
#
# Case 2 hash-drift (``upload_and_tag``) is the only title-correction path
# where the bot must explicitly preserve community-contributed metadata
# from the file it's about to queue for deletion (the move and redirect
# paths do this via merge_preserved_wikitext inline). _tag_drift_duplicate
# now does both: rescue community categories/licenses/assessments into the
# new (correct-title) file, then tag the old one. The two steps are
# independently best-effort so a rescue failure does not block the tag.


_NEW_WIKI_MARKUP = "{{DPLA metadata}}\n"
_DPLA_ID = "abcdef0123456789abcdef0123456789"


def _drift_uploader_with_pages(
    old_text: str,
    *,
    old_exists: bool = True,
    save_raises: Exception | None = None,
):
    """Build ``(uploader, old_page, new_page)`` for direct invocation of
    ``_tag_drift_duplicate``. The new page is a write-only sink — we
    just upload + save into it; nothing reads its state.

    Tag-step failures are injected via ``patch("tools.uploader.tag_as_duplicate")``
    at the call site, not here — the helper only needs to control the
    rescue-side mocks."""
    from tools.uploader import Uploader

    uploader = Uploader.__new__(Uploader)
    uploader.site = MagicMock(name="site")

    old_page = MagicMock(name="old_page")
    old_page.text = old_text
    old_page.exists.return_value = old_exists

    new_page = MagicMock(name="new_page")
    if save_raises is not None:
        new_page.save.side_effect = save_raises
    return uploader, old_page, new_page


def _patch_get_page(old_page, new_page):
    """Side-effect for ``get_page`` that returns the right mock by title.
    Tests use ``File:old.jpg`` and ``File:new.jpg`` consistently."""

    def _side(_site, title):
        if title == "File:old.jpg":
            return old_page
        if title == "File:new.jpg":
            return new_page
        raise AssertionError(f"unexpected get_page title: {title!r}")

    return _side


def _call_tag_drift(uploader, old_page, new_page, **patches):
    """Invoke ``_tag_drift_duplicate`` with the standard arg shape so
    each test asserts on outcomes instead of plumbing the call."""
    extras = {f"tools.uploader.{name}": value for name, value in patches.items()}
    with patch(
        "tools.uploader.get_page",
        side_effect=_patch_get_page(old_page, new_page),
    ):
        if extras:
            from contextlib import ExitStack

            with ExitStack() as stack:
                for target, value in extras.items():
                    stack.enter_context(patch(target, value))
                uploader._tag_drift_duplicate(
                    "old.jpg", "new.jpg", _NEW_WIKI_MARKUP, _DPLA_ID
                )
        else:
            uploader._tag_drift_duplicate(
                "old.jpg", "new.jpg", _NEW_WIKI_MARKUP, _DPLA_ID
            )


def test_rescue_preserves_community_categories_into_new_file():
    """The headline rescue: a community-curated category on the old
    file must end up in the new file's wikitext."""
    old_text = (
        "{{Information|description=Old description}}\n"
        "[[Category:Curated by community]]\n"
        "[[Category:Some 1842 thing]]\n"
    )
    uploader, old_page, new_page = _drift_uploader_with_pages(old_text)

    with patch("tools.uploader.tag_as_duplicate"):
        _call_tag_drift(uploader, old_page, new_page)

    assert new_page.save.called, (
        "Save must be called when the old page has community categories"
    )
    saved_text = new_page.text
    assert "[[Category:Curated by community]]" in saved_text
    assert "[[Category:Some 1842 thing]]" in saved_text
    assert saved_text.lstrip().startswith("{{DPLA metadata}}")


def test_rescue_preserves_assessment_and_license_templates():
    """Featured-picture / PD-USGov templates survive the rescue."""
    old_text = (
        "=={{Assessment}}==\n"
        "{{Featured picture|com|nominator=Curator}}\n"
        "{{PD-USGov-Military}}\n"
        "[[Category:Notable images]]\n"
    )
    uploader, old_page, new_page = _drift_uploader_with_pages(old_text)

    with patch("tools.uploader.tag_as_duplicate"):
        _call_tag_drift(uploader, old_page, new_page)

    saved_text = new_page.text
    assert "{{Featured picture|com|nominator=Curator}}" in saved_text
    assert "{{PD-USGov-Military}}" in saved_text
    assert "[[Category:Notable images]]" in saved_text


def test_rescue_no_save_when_nothing_to_preserve():
    """If the old page has nothing in merge_preserved_wikitext's
    pattern set, the new page must NOT be saved — calling save() on
    identical content would emit a no-op revision on Commons."""
    # Information template alone is NOT in the preserve set.
    old_text = "{{Information|description=Plain text}}\n"
    uploader, old_page, new_page = _drift_uploader_with_pages(old_text)

    with patch("tools.uploader.tag_as_duplicate"):
        _call_tag_drift(uploader, old_page, new_page)

    assert not new_page.save.called


def test_rescue_skips_when_old_page_does_not_exist():
    """Defensive: if the old page is gone (concurrent admin action),
    don't crash — skip the rescue and proceed with the tag attempt."""
    uploader, old_page, new_page = _drift_uploader_with_pages(
        old_text="", old_exists=False
    )

    with patch("tools.uploader.tag_as_duplicate") as tag_mock:
        _call_tag_drift(uploader, old_page, new_page)

    assert not new_page.save.called
    # Tag still runs — best-effort independence.
    assert tag_mock.called


def test_rescue_save_failure_does_not_block_tag(caplog):
    """A save() failure mid-rescue must not block the subsequent tag.
    The old page history still has the community contributions, so
    manual recovery remains possible. Tag must still fire so the
    duplicate-resolution path completes."""
    import logging

    uploader, old_page, new_page = _drift_uploader_with_pages(
        old_text="[[Category:Community]]\n",
        save_raises=RuntimeError("API timeout"),
    )

    with (
        patch("tools.uploader.tag_as_duplicate") as tag_mock,
        caplog.at_level(logging.WARNING),
    ):
        _call_tag_drift(uploader, old_page, new_page)

    assert any("Failed to rescue" in r.message for r in caplog.records)
    # Tag must still fire — independent best-effort.
    assert tag_mock.called


def test_tag_failure_does_not_swallow_successful_rescue(caplog):
    """Symmetric: if the tag step fails, the rescue is still recorded
    in logs and the new page's saved text reflects the merge."""
    import logging

    uploader, old_page, new_page = _drift_uploader_with_pages(
        old_text="[[Category:Community]]\n",
    )

    with (
        patch(
            "tools.uploader.tag_as_duplicate",
            side_effect=RuntimeError("Commons API down"),
        ),
        caplog.at_level(logging.WARNING),
    ):
        _call_tag_drift(uploader, old_page, new_page)

    # Rescue succeeded.
    assert new_page.save.called
    assert "[[Category:Community]]" in new_page.text
    # Tag failure is logged but didn't raise.
    assert any("Failed to tag" in r.message for r in caplog.records)


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


def _csrf_keyerror() -> KeyError:
    """Build a KeyError whose ``str()`` matches pywikibot's CSRF-invalid
    TokenWallet message. Uses the production marker constant so the
    detector and the test data can never drift apart."""
    return KeyError(
        f"{_CSRF_TOKEN_ERROR_MARKER} for user 'DPLA bot' on commons:commons wiki."
    )


def test_is_csrf_token_error_matches_pywikibot_shape():
    """Substring match on ``str(KeyError(...))`` — Python wraps KeyError
    args in quotes on stringify, so the marker must appear inside those
    wrapping quotes."""
    assert _is_csrf_token_error(_csrf_keyerror())


def test_is_csrf_token_error_rejects_unrelated_keyerror():
    """A KeyError from any other code path (e.g. dict miss on 'title')
    must not be treated as a CSRF error — otherwise a bug elsewhere
    could accidentally trip the whole session-abort escalation."""
    assert not _is_csrf_token_error(KeyError("title"))
    assert not _is_csrf_token_error(KeyError("csrf"))  # bare, no "Invalid token"


def test_is_csrf_token_error_rejects_non_keyerror():
    """A RuntimeError whose message happens to contain the CSRF marker
    isn't a real CSRF token error — pywikibot only raises this as
    KeyError. Guard against a false positive from log-string
    contamination."""
    assert not _is_csrf_token_error(RuntimeError(_CSRF_TOKEN_ERROR_MARKER))


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
    """First CSRF error → ``_recover_commons_session`` fires and the
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
    with patch("tools.uploader._recover_commons_session") as recover_mock:
        _csrf_process_file(uploader)
    recover_mock.assert_called_once_with(uploader.site)
    assert uploader._safe_upload.call_count == 2, (
        f"retry loop did not re-attempt after CSRF recovery — "
        f"_safe_upload called {uploader._safe_upload.call_count} time(s), "
        f"expected 2. If this is 1, the recovery path returned instead "
        f"of continuing the loop."
    )
    assert uploader._csrf_recoveries_used == 1


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
    uploader._csrf_recoveries_used = MAX_CSRF_RECOVERIES
    uploader._safe_upload = MagicMock(side_effect=_csrf_keyerror())
    with patch("tools.uploader._recover_commons_session") as recover_mock:
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
    """If ``_recover_commons_session`` itself throws (network down,
    credentials rotated, etc.), escalate to ``CsrfRecoveryFailed``
    immediately rather than looping recovery attempts against an
    unreachable auth endpoint."""
    tracker = Tracker()
    uploader = _csrf_retry_loop_uploader(tracker)
    uploader._safe_upload = MagicMock(side_effect=_csrf_keyerror())
    with patch(
        "tools.uploader._recover_commons_session",
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
# Phantom-drift defense-in-depth (this PR).
#
# ``get_page_title``'s ``item_title[:181]`` truncation used to leak a
# trailing space when the 181st character landed on whitespace, producing
# a raw Python title with a double-space run around the ``- DPLA -``
# separator. ``process_file``'s line-468 identity check
# (``existing_file.title(with_ns=False) == page_title``) then failed on
# the raw-string comparison, and ``_resolve_hash_drift`` fell through to
# Case 2 tagging the file as a duplicate of itself. Commons's
# ``fileexists-no-change`` server-side check happened to reject the
# upload, so ``_tag_drift_duplicate`` was never called — but the guard
# path is defense-in-depth for any future normalisation drift.
# ---------------------------------------------------------------------------


def test_canonicalize_commons_title_collapses_whitespace_runs():
    """The guard helper mirrors MediaWiki's ``.trim()`` +
    whitespace-run collapse on file titles."""
    from tools.uploader import _canonicalize_commons_title

    assert _canonicalize_commons_title("Foo  Bar") == "Foo Bar"
    assert _canonicalize_commons_title("  Foo Bar  ") == "Foo Bar"
    assert _canonicalize_commons_title("Foo\tBar") == "Foo Bar"
    assert _canonicalize_commons_title("Foo Bar") == "Foo Bar"


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
            wiki_markup="",
        )
    assert action == "already_correct"


def test_tag_drift_duplicate_refuses_self_tag_byte_equal():
    """Belt: even if some caller passes the same title as both
    ``old_filename`` and ``new_filename``, ``_tag_drift_duplicate``
    refuses to call ``tag_as_duplicate`` — that would flag the file
    for admin deletion of itself."""
    uploader = _build_uploader_with_dpla()
    same = "Foo - DPLA - cccccccccccccccccccccccccccccccc.jpg"
    with (
        patch("tools.uploader.tag_as_duplicate") as tag_mock,
        patch("tools.uploader.get_page") as get_page_mock,
    ):
        uploader._tag_drift_duplicate(
            old_filename=same,
            new_filename=same,
            wiki_markup="wt",
            dpla_id="cccccccccccccccccccccccccccccccc",
        )
    tag_mock.assert_not_called()
    get_page_mock.assert_not_called()


def test_tag_drift_duplicate_refuses_self_tag_whitespace_run_equal():
    """Suspenders: two titles that differ only in whitespace runs
    (the exact shape ``get_page_title``'s truncation bug used to
    produce) resolve to the same Commons page under MediaWiki
    normalisation. Must not tag."""
    uploader = _build_uploader_with_dpla()
    single_space = "Foo Bar - DPLA - dddddddddddddddddddddddddddddddd.jpg"
    double_space = "Foo Bar  - DPLA - dddddddddddddddddddddddddddddddd.jpg"
    with (
        patch("tools.uploader.tag_as_duplicate") as tag_mock,
        patch("tools.uploader.get_page") as get_page_mock,
    ):
        uploader._tag_drift_duplicate(
            old_filename=single_space,
            new_filename=double_space,
            wiki_markup="wt",
            dpla_id="dddddddddddddddddddddddddddddddd",
        )
    tag_mock.assert_not_called()
    get_page_mock.assert_not_called()


def test_tag_drift_duplicate_still_tags_when_names_genuinely_differ():
    """Positive: a genuine hash-drift case (different filenames) still
    reaches ``tag_as_duplicate`` — the self-tag guard doesn't over-fire."""
    uploader = _build_uploader_with_dpla()
    old_page = MagicMock()
    old_page.exists.return_value = True
    old_page.text = ""
    with (
        patch("tools.uploader.tag_as_duplicate") as tag_mock,
        patch("tools.uploader.get_page", return_value=old_page),
        patch("tools.uploader.merge_preserved_wikitext", return_value="wt"),
    ):
        uploader._tag_drift_duplicate(
            old_filename="Old title - DPLA - eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee.jpg",
            new_filename="New title - DPLA - eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee.jpg",
            wiki_markup="wt",
            dpla_id="eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        )
    tag_mock.assert_called_once()
