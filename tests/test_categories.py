"""Tests for ingest_wikimedia.categories.

Focus is on the two pieces of new state/behaviour we rely on for the post-
upload touch flow:

* ``CategoryEnsurer.newly_created`` populates only when ``ensure()`` actually
  writes new P8464 infrastructure — not on any of the three fast-paths.
* ``touch_institution_files()`` iterates Commons' search results and calls
  ``touch()`` on each, surviving per-page errors.
"""

from unittest.mock import MagicMock, patch

from ingest_wikimedia.categories import CategoryEnsurer, touch_institution_files


def _new_ensurer():
    """A CategoryEnsurer with a mocked commons_site, ready for unit tests."""
    return CategoryEnsurer(commons_site=MagicMock())


def test_newly_created_is_empty_initially():
    e = _new_ensurer()
    assert e.newly_created == set()


def test_newly_created_skips_already_ensured_session_cache():
    e = _new_ensurer()
    e._ensured.add("Q100")
    # ensure() should short-circuit on the session cache without touching
    # _newly_created.
    e.ensure("Q100", "Foo Institution", "Q999")
    assert e.newly_created == set()


def test_newly_created_skips_when_commons_category_already_exists():
    e = _new_ensurer()
    with patch.object(e, "_commons_category_exists", return_value=True):
        e.ensure("Q200", "Foo Institution", "Q999")
    assert e.newly_created == set()
    assert "Q200" in e._ensured  # still marked as ensured for session


def test_newly_created_skips_when_wikidata_p8464_already_set():
    e = _new_ensurer()
    with (
        patch.object(e, "_commons_category_exists", return_value=False),
        patch.object(e, "_institution_has_category", return_value=True),
    ):
        e.ensure("Q300", "Foo Institution", "Q999")
    assert e.newly_created == set()
    assert "Q300" in e._ensured


def test_newly_created_populates_on_actual_creation():
    e = _new_ensurer()
    with (
        patch.object(e, "_commons_category_exists", return_value=False),
        patch.object(e, "_institution_has_category", return_value=False),
        patch.object(e, "_get_hub_category_qid", return_value="Q888"),
        patch.object(e, "_create_commons_category"),
        patch.object(e, "_get_or_create_wikidata_category_item", return_value="Q777"),
        patch.object(e, "_add_p8464_to_institution"),
    ):
        e.ensure("Q400", "Foo Institution", "Q999")
    assert e.newly_created == {"Q400"}


def test_newly_created_is_a_copy_not_the_internal_set():
    e = _new_ensurer()
    e._newly_created.add("Q500")
    snapshot = e.newly_created
    snapshot.add("Q501")
    assert "Q501" not in e._newly_created


def test_dry_run_does_not_count_as_newly_created():
    """Dry-run goes through the slow path but doesn't actually write P8464.

    The flag exists so callers know "if you touch these files now, they'll
    pick up the new claim".  Dry-run means no claim was written, so post-run
    touching would be a no-op at best and misleading at worst.
    """
    e = CategoryEnsurer(commons_site=MagicMock(), dry_run=True)
    with (
        patch.object(e, "_commons_category_exists", return_value=False),
        patch.object(e, "_institution_has_category", return_value=False),
        patch.object(e, "_get_hub_category_qid", return_value="Q888"),
    ):
        e.ensure("Q600", "Foo Institution", "Q999")
    assert e.newly_created == set()


def test_touch_institution_files_touches_each_search_hit():
    site = MagicMock()
    pages = [MagicMock(), MagicMock(), MagicMock()]
    for i, p in enumerate(pages):
        p.title.return_value = f"File:Foo page {i}.jpg"
    site.search.return_value = iter(pages)

    n = touch_institution_files(site, "Q123")

    assert n == 3
    for p in pages:
        p.touch.assert_called_once()


def test_touch_institution_files_continues_after_per_page_failure():
    site = MagicMock()
    good_a, bad, good_b = MagicMock(), MagicMock(), MagicMock()
    for i, p in enumerate((good_a, bad, good_b)):
        p.title.return_value = f"File:p{i}.jpg"
    bad.touch.side_effect = RuntimeError("boom")
    site.search.return_value = iter([good_a, bad, good_b])

    n = touch_institution_files(site, "Q123")

    assert n == 2
    good_a.touch.assert_called_once()
    good_b.touch.assert_called_once()


def test_touch_institution_files_uses_expected_search_query():
    site = MagicMock()
    site.search.return_value = iter([])
    touch_institution_files(site, "Q42")
    site.search.assert_called_once_with(
        'insource:"Institution" insource:"wikidata = Q42"', namespaces=[6]
    )


def test_touch_institution_files_log_each_logs_per_touch(caplog):
    """``log_each=True`` (used by fix-unknown-categories --verbose) logs each touched title."""
    import logging as _logging

    site = MagicMock()
    page_a, page_b = MagicMock(), MagicMock()
    page_a.title.return_value = "File:A.jpg"
    page_b.title.return_value = "File:B.jpg"
    site.search.return_value = iter([page_a, page_b])

    with caplog.at_level(_logging.INFO):
        touch_institution_files(site, "Q1", log_each=True)

    messages = " | ".join(r.message for r in caplog.records)
    assert "File:A.jpg" in messages
    assert "File:B.jpg" in messages


def test_touch_institution_files_default_does_not_log_per_touch(caplog):
    import logging as _logging

    site = MagicMock()
    page = MagicMock()
    page.title.return_value = "File:Only.jpg"
    site.search.return_value = iter([page])

    with caplog.at_level(_logging.INFO):
        touch_institution_files(site, "Q1")

    messages = " | ".join(r.message for r in caplog.records)
    # The "Touching: ..." per-page line should NOT appear by default; this is
    # the regression guard for the new opt-in `log_each` flag.
    assert "Touching" not in messages
