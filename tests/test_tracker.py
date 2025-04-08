import pytest
from ingest_wikimedia.tracker import Tracker, Result


@pytest.fixture
def tracker():
    return Tracker()


def test_initial_counts(tracker: Tracker):
    for result in Result:
        assert tracker.count(result) == 0


def test_increment(tracker: Tracker):
    tracker.increment(Result.DOWNLOADED)
    assert tracker.count(Result.DOWNLOADED) == 1

    tracker.increment(Result.DOWNLOADED, 5)
    assert tracker.count(Result.DOWNLOADED) == 6


def test_str_representation(tracker: Tracker):
    tracker.increment(Result.FAILED, 2)
    tracker.increment(Result.SKIPPED, 3)
    expected_output = "COUNTS:\nFAILED: 2\nSKIPPED: 3\n"

    assert str(tracker) == expected_output
