"""Tests for tools/uploader.py helpers.

Currently focused on the end-of-run touch flow added to close the Wikidata
replication-lag race that lands first-batch files in the unknown-institution
category.
"""

from unittest.mock import MagicMock, patch

from tools.uploader import _post_upload_touch_new_institutions


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
