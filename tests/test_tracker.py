import pytest
from ingest_wikimedia.tracker import Tracker, Result


@pytest.fixture
def tracker():
    return Tracker()


def test_singleton_instance(tracker):
    another_tracker = Tracker()
    assert tracker is another_tracker


def test_initial_counts(tracker):
    for result in Result:
        assert tracker.count(result) == 0


def test_increment(tracker):
    tracker.increment(Result.DOWNLOADED)
    assert tracker.count(Result.DOWNLOADED) == 1

    tracker.increment(Result.DOWNLOADED, 5)
    assert tracker.count(Result.DOWNLOADED) == 6


def test_thread_safety(tracker):
    import threading

    def increment_tracker():
        for _ in range(1000):
            tracker.increment(Result.UPLOADED)

    threads = [threading.Thread(target=increment_tracker) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert tracker.count(Result.UPLOADED) == 10000


def test_str_representation(tracker):
    tracker.increment(Result.FAILED, 2)
    tracker.increment(Result.SKIPPED, 3)
    expected_output = (
        "COUNTS:\nDOWNLOADED: 0\nFAILED: 2\nSKIPPED: 3\nUPLOADED: 0\nBYTES: 0\n"
    )
    assert str(tracker) == expected_output
