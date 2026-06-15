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


# ---------------------------------------------------------------------------
# snapshot / diff / merge — the per-task delta API used by the
# sdc-sync partner-mode multiprocessing.Pool. Each worker takes a snapshot
# before processing an item, runs the item (mutating its module-level
# tracker), and returns the diff to the parent. The parent merges the
# delta into its own tracker.
# ---------------------------------------------------------------------------


def test_snapshot_returns_independent_copy(tracker: Tracker):
    """Snapshot must be a copy — mutating the tracker afterwards
    can't change the snapshot's recorded counts."""
    tracker.increment(Result.DOWNLOADED, 5)
    snap = tracker.snapshot()
    tracker.increment(Result.DOWNLOADED, 10)
    assert snap[Result.DOWNLOADED] == 5
    assert tracker.count(Result.DOWNLOADED) == 15


def test_diff_returns_per_counter_delta(tracker: Tracker):
    """``diff`` compares the current state against a prior snapshot
    and returns the per-counter delta. Counters that didn't change
    appear as 0; this matches the shape the parent's ``merge`` expects."""
    prior = tracker.snapshot()
    tracker.increment(Result.SDC_CLAIMS_ADDED, 7)
    tracker.increment(Result.SDC_REFS_ADDED, 30)
    delta = tracker.diff(prior)
    assert delta[Result.SDC_CLAIMS_ADDED] == 7
    assert delta[Result.SDC_REFS_ADDED] == 30
    assert delta[Result.DOWNLOADED] == 0


def test_diff_handles_decrement_across_reset(tracker: Tracker):
    """If the tracker was reset between snapshot and diff (e.g. the
    worker reused its module-level tracker across tasks via reset()),
    the diff goes negative. This is by design — callers can detect it
    and decide to log a warning. ``merge`` then just subtracts."""
    tracker.increment(Result.DOWNLOADED, 5)
    prior = tracker.snapshot()
    tracker.reset()
    delta = tracker.diff(prior)
    assert delta[Result.DOWNLOADED] == -5


def test_merge_aggregates_deltas_from_multiple_workers(tracker: Tracker):
    """Parent merges per-task deltas from several workers — counts
    add up across tasks. Mirrors the imap_unordered loop pattern in
    ``_run_partner_mode_parallel``."""
    deltas_from_workers = [
        {Result.SDC_CLAIMS_ADDED: 3, Result.SDC_REFS_ADDED: 12},
        {Result.SDC_CLAIMS_ADDED: 5, Result.SDC_REFS_ADDED: 18},
        {Result.SDC_CLAIMS_ADDED: 2, Result.SDC_REMOVALS: 1},
    ]
    for delta in deltas_from_workers:
        tracker.merge(delta)
    assert tracker.count(Result.SDC_CLAIMS_ADDED) == 10
    assert tracker.count(Result.SDC_REFS_ADDED) == 30
    assert tracker.count(Result.SDC_REMOVALS) == 1


def test_merge_ignores_unknown_keys(tracker: Tracker):
    """A worker on a slightly newer Tracker schema may emit a counter
    the parent doesn't have. Silently ignore — don't raise. Rolling
    deploys can have brief windows of skew between parent and
    workers."""

    class _FakeResult:
        name = "TOTALLY_FAKE_COUNTER"

    fake = _FakeResult()
    tracker.merge({fake: 999, Result.SDC_CLAIMS_ADDED: 4})
    assert tracker.count(Result.SDC_CLAIMS_ADDED) == 4
    # No exception raised — the fake key was silently dropped.


def test_snapshot_diff_merge_round_trip(tracker: Tracker):
    """End-to-end: parent snapshot → worker mutates → diff → parent
    merges. After the cycle, parent should have exactly the worker's
    counts even though the worker's tracker started non-empty."""
    worker = Tracker()
    worker.increment(Result.DOWNLOADED, 100)  # pre-existing state from prior tasks
    prior = worker.snapshot()
    worker.increment(Result.SDC_CLAIMS_ADDED, 2)
    worker.increment(Result.SDC_REFS_ADDED, 8)
    delta = worker.diff(prior)

    tracker.merge(delta)
    assert tracker.count(Result.SDC_CLAIMS_ADDED) == 2
    assert tracker.count(Result.SDC_REFS_ADDED) == 8
    # The pre-existing 100 from the worker's earlier task DID NOT
    # bleed into the parent — diff isolated only this-task's delta.
    assert tracker.count(Result.DOWNLOADED) == 0
