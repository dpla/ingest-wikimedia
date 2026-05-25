"""Tests for tools/uploader.py helpers.

Currently focused on the end-of-run touch flow added to close the Wikidata
replication-lag race that lands first-batch files in the unknown-institution
category.
"""

from unittest.mock import MagicMock, patch

from ingest_wikimedia.tracker import Result, Tracker
from tools.uploader import (
    _post_item_orphan_check,
    _post_upload_touch_new_institutions,
)


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
        _post_upload_touch_new_institutions(MagicMock(), None, dry_run=False)
    sleep_mock.assert_not_called()
    touch_mock.assert_not_called()


def test_skips_when_dry_run():
    ensurer = _ensurer_with({"Q1", "Q2"})
    with (
        patch("tools.uploader.time.sleep") as sleep_mock,
        patch("tools.uploader.touch_institution_files") as touch_mock,
    ):
        _post_upload_touch_new_institutions(MagicMock(), ensurer, dry_run=True)
    sleep_mock.assert_not_called()
    touch_mock.assert_not_called()


def test_skips_when_newly_created_is_empty():
    ensurer = _ensurer_with(set())
    with (
        patch("tools.uploader.time.sleep") as sleep_mock,
        patch("tools.uploader.touch_institution_files") as touch_mock,
    ):
        _post_upload_touch_new_institutions(MagicMock(), ensurer, dry_run=False)
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
        _post_upload_touch_new_institutions(site, ensurer, dry_run=False)

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
        _post_upload_touch_new_institutions(MagicMock(), ensurer, dry_run=False)

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
    ordinal in this same run), so route to upload_only."""
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
    assert action == "upload_only"


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
