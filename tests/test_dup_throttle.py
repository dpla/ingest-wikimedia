"""Tests for ingest_wikimedia/dup_throttle.py — the cached-headroom gate that
caps how many ``{{duplicate}}`` tags a single uploader run may add to the
human-maintained Category:Duplicate.

All tests inject ``size_fn`` so nothing touches pywikibot or the network. The
key behaviours pinned here:

  * ``try_acquire`` queries the category size at most once per ``recheck_cap``
    grants (amortized cost — the whole point of the design).
  * at/over ``threshold`` it defers (returns False) without granting.
  * ``wait_for_capacity`` polls until the category drains below
    ``resume_below`` (hysteresis), refills headroom, and times out cleanly.
"""

import pytest

from ingest_wikimedia.dup_throttle import DuplicateCategoryThrottle


class _Sizes:
    """Callable returning a scripted sequence of sizes, recording call count.

    The last value sticks once the script is exhausted, so a test can describe
    just the transitions it cares about.
    """

    def __init__(self, *values: int):
        self._values = list(values)
        self.calls = 0

    def __call__(self) -> int:
        self.calls += 1
        idx = min(self.calls - 1, len(self._values) - 1)
        return self._values[idx]


def test_grants_until_threshold_then_defers():
    sizes = _Sizes(0)
    t = DuplicateCategoryThrottle(
        threshold=1000, resume_below=900, recheck_cap=100, size_fn=sizes
    )
    # Far below threshold: first call queries, the next 99 ride cached headroom.
    assert all(t.try_acquire() for _ in range(100))
    assert sizes.calls == 1


def test_requeries_only_after_headroom_exhausted():
    sizes = _Sizes(0)
    t = DuplicateCategoryThrottle(
        threshold=1000, resume_below=900, recheck_cap=100, size_fn=sizes
    )
    for _ in range(100):
        t.try_acquire()
    assert sizes.calls == 1
    t.try_acquire()  # 101st grant forces a fresh size query
    assert sizes.calls == 2


def test_defers_at_threshold():
    sizes = _Sizes(1000)
    t = DuplicateCategoryThrottle(threshold=1000, resume_below=900, size_fn=sizes)
    assert t.try_acquire() is False
    # A deferral does not bank headroom — every subsequent attempt re-queries.
    assert t.try_acquire() is False
    assert sizes.calls == 2


def test_headroom_capped_near_threshold():
    # 50 below threshold with a recheck_cap of 100 → only 50 grants before the
    # next query, so a stale cached size can never let the run overshoot
    # threshold. Second query shows the category filled to threshold → defer.
    sizes = _Sizes(950, 1000)
    t = DuplicateCategoryThrottle(
        threshold=1000, resume_below=900, recheck_cap=100, size_fn=sizes
    )
    assert all(t.try_acquire() for _ in range(50))
    assert sizes.calls == 1
    # 51st grant must re-query, and the now-full category forces a defer.
    assert t.try_acquire() is False
    assert sizes.calls == 2


def test_wait_for_capacity_returns_when_drained():
    # Full, full, then drained below resume_below.
    sizes = _Sizes(1000, 950, 880)
    t = DuplicateCategoryThrottle(
        threshold=1000, resume_below=900, poll_secs=120, size_fn=sizes
    )
    slept: list[float] = []
    clock = iter([0, 0, 120, 240]).__next__
    drained = t.wait_for_capacity(10_000, sleep=lambda s: slept.append(s), clock=clock)
    assert drained is True
    assert slept == [120, 120]
    # Headroom was refilled, so the next tag rides the cache (no new query).
    before = sizes.calls
    assert t.try_acquire() is True
    assert sizes.calls == before


def test_wait_for_capacity_times_out():
    sizes = _Sizes(1000)  # never drains
    t = DuplicateCategoryThrottle(
        threshold=1000, resume_below=900, poll_secs=120, size_fn=sizes
    )
    slept: list[float] = []
    clock = iter([0, 300]).__next__  # second check is past the 100s deadline
    timed_out = t.wait_for_capacity(100, sleep=lambda s: slept.append(s), clock=clock)
    assert timed_out is False
    assert slept == []  # deadline hit before any sleep


def test_wait_for_capacity_unbounded_never_times_out():
    """``max_wait_secs=None`` disables the deadline — appropriate for
    the drain-deferred loop, which waits patiently on human-admin
    category clearing. Category stays full for many polls, then drains;
    the wait returns True whenever the category actually falls."""
    sizes = _Sizes(1000, 1000, 1000, 1000, 880)
    t = DuplicateCategoryThrottle(
        threshold=1000, resume_below=900, poll_secs=60, size_fn=sizes
    )
    slept: list[float] = []
    # Clock never advances past any deadline because there is no
    # deadline; the loop is size-driven, not time-driven.
    drained = t.wait_for_capacity(
        max_wait_secs=None,
        sleep=lambda s: slept.append(s),
        clock=lambda: 0,
    )
    assert drained is True
    # 4 polls at 1000, 1 poll at 880: 4 sleeps of the full poll interval.
    assert slept == [60, 60, 60, 60]


def test_resume_below_must_not_exceed_threshold():
    try:
        DuplicateCategoryThrottle(threshold=1000, resume_below=1001)
    except ValueError:
        return
    raise AssertionError("expected ValueError for resume_below > threshold")


def test_recheck_cap_must_be_positive():
    try:
        DuplicateCategoryThrottle(threshold=1000, resume_below=900, recheck_cap=0)
    except ValueError:
        return
    raise AssertionError("expected ValueError for recheck_cap < 1")


def test_construction_without_site_or_size_fn_raises():
    """The MWDL 2026-07-02 drain-deferred crash: ``drain_deferred.py``
    called ``DuplicateCategoryThrottle()`` with no arguments, so the
    site defaulted to ``None`` and the first ``category_size`` call
    exploded on ``pywikibot.Category(None, ...)``. Fail fast at
    construction so a similarly broken future caller can't silently
    sit dormant until the first API call."""
    with pytest.raises(ValueError) as exc_info:
        DuplicateCategoryThrottle()
    message = str(exc_info.value).lower()
    assert "site" in message or "size_fn" in message, (
        "error message should mention site or size_fn to point the "
        f"caller at the fix; got: {exc_info.value!r}"
    )


def test_construction_with_size_fn_only_is_accepted():
    """Tests inject ``size_fn`` to avoid the real pywikibot path;
    that use case must remain valid (no ``site`` required)."""
    t = DuplicateCategoryThrottle(size_fn=lambda: 0)
    assert t.category_size() == 0


def test_construction_with_site_only_is_accepted():
    """Production callers pass a real pywikibot Site; that use case
    must remain valid (no ``size_fn`` required). The site is not used
    at construction, only when ``category_size`` runs, so any non-None
    sentinel proves the guard passes."""
    DuplicateCategoryThrottle(site=object())  # no exception → guard passes
