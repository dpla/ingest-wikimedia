"""Throttle emission of ``{{duplicate}}`` tags against the human-maintained
Category:Duplicate.

A single uploader run over a partner with heavy orphaned-duplicate content can
try to emit tens of thousands of ``{{duplicate}}`` tags on the Case-2 hash-drift
path (an existing file's bytes are re-homed to the canonical DPLA-ID title and
the stranded copy is tagged). Each tag adds a file to Category:Duplicate, which
Commons volunteers process by hand — flooding it is antisocial and risks the bot
being blocked. This gate caps how much that path may contribute: at or above
``threshold`` category members it refuses (defers) further tags until the
category drains back below ``resume_below`` (hysteresis).

Scope: this gates the hash-drift duplicate-tag path only — the volume source
behind the flooding it was built to prevent. The trailing-page orphan path
(``uploader._post_item_orphan_check``) also tags into Category:Duplicate but is
bounded to truncated trailing pages per item (low volume) and has no defer/drain
channel, so it is intentionally left un-gated: deferring an orphan tag would
leave the orphan unresolved with no retry, which is worse than emitting it.

Cost is the design point. The category size is fetched at most once per
``recheck_cap`` grants, via a cached "headroom" budget — never per upload, and
never per tag in steady state. Callers consult the gate only on the (rare)
tag-emitting path, so ordinary uploads pay nothing.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

import pywikibot

# Commons admins asked us (2026-07) to keep Category:Duplicate lean. Defer new
# hash-drift {{duplicate}} tags once the category reaches DEFAULT_THRESHOLD, and
# don't resume draining the deferred backlog until it falls back below
# DEFAULT_RESUME_BELOW — a 50-member hysteresis band so the drain works a real
# batch each cycle instead of thrashing at the ceiling.
DEFAULT_THRESHOLD = 190
DEFAULT_RESUME_BELOW = 140
# Max tags emitted per cached size reading. Kept <= the hysteresis band so
# concurrent partner runs can't overshoot the ceiling by much before
# re-querying (worst case ~= recheck_cap x concurrent sessions).
DEFAULT_RECHECK_CAP = 50
# Category:Duplicate drains on human-admin timescales — days, not
# minutes — so polling every 5 minutes is fast enough that we won't sit
# idle after a batch clears while still being a good API citizen. The
# category has no user-visible latency requirement here; a session that
# notices a drop 5 minutes late is indistinguishable from one that
# notices instantly.
DEFAULT_POLL_SECS = 300
DUPLICATE_CATEGORY = "Category:Duplicate"


class DuplicateCategoryThrottle:
    """Cap a run's contribution to Category:Duplicate.

    ``try_acquire`` is the non-blocking gate for the main upload pass;
    ``wait_for_capacity`` is the blocking helper the drain pass uses to wait
    for the category to fall back below ``resume_below``.
    """

    def __init__(
        self,
        site=None,
        *,
        threshold: int = DEFAULT_THRESHOLD,
        resume_below: int = DEFAULT_RESUME_BELOW,
        recheck_cap: int = DEFAULT_RECHECK_CAP,
        poll_secs: float = DEFAULT_POLL_SECS,
        category: str = DUPLICATE_CATEGORY,
        size_fn: Callable[[], int] | None = None,
    ):
        if resume_below > threshold:
            raise ValueError("resume_below must be <= threshold")
        if recheck_cap < 1:
            raise ValueError("recheck_cap must be >= 1")
        if site is None and size_fn is None:
            raise ValueError(
                "DuplicateCategoryThrottle requires either a pywikibot "
                "``site`` (for the default categoryinfo query) or an "
                "injected ``size_fn`` (for tests)."
            )
        self._site = site
        self.threshold = threshold
        self.resume_below = resume_below
        self.recheck_cap = recheck_cap
        self.poll_secs = poll_secs
        self.category = category
        # Injectable for tests; defaults to a single pywikibot categoryinfo call.
        self._size_fn = size_fn
        # Number of further tags we believe we can emit before the category
        # could reach ``threshold`` — decremented per grant, refilled (after a
        # size query) when it hits zero. Starts at 0 so the first grant queries.
        self._headroom = 0

    def category_size(self) -> int:
        """Current live size of the tracked category — one ``categoryinfo``
        API query per call (or the injected ``size_fn``). Public so
        callers outside the throttle (e.g. the drain-deferred phase's
        start-of-drain notification) can observe the category without
        reaching into throttle internals."""
        if self._size_fn is not None:
            return self._size_fn()
        info = pywikibot.Category(self._site, self.category).categoryinfo
        # ``size`` is total members (files + pages + subcats); for
        # Category:Duplicate that is effectively the file count. Fall back to
        # ``files`` if ``size`` is absent.
        return int(info.get("size", info.get("files", 0)))

    def _headroom_for(self, size: int) -> int:
        """Tags we may emit before another size query: the room left below
        ``threshold``, clamped to ``recheck_cap`` (which bounds both how stale
        the cached size can get and cross-session overshoot). Callers pass a
        ``size`` already known to be below ``threshold``, so this is >= 1."""
        return min(self.threshold - size, self.recheck_cap)

    def try_acquire(self) -> bool:
        """Return ``True`` if a ``{{duplicate}}`` tag may be emitted now
        (accounting for it), ``False`` if the category is at/over ``threshold``
        and the tag must be deferred. Never blocks.

        Queries the category size at most once per ``recheck_cap`` grants: while
        local headroom remains it just decrements a counter (no API call).
        """
        if self._headroom > 0:
            self._headroom -= 1
            return True
        # Headroom is exhausted (0), so query the live size to decide.
        size = self.category_size()
        if size >= self.threshold:
            return False  # defer; banks no headroom, so the next attempt re-queries
        # Refill headroom, consuming one grant for this tag.
        self._headroom = self._headroom_for(size) - 1
        return True

    def wait_for_capacity(
        self,
        max_wait_secs: float | None = None,
        *,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> bool:
        """Block (polling every ``poll_secs``) until the category is below
        ``resume_below``, then refill headroom and return ``True``.

        ``max_wait_secs`` bounds the wait; ``None`` disables the timeout so
        the caller waits indefinitely — appropriate for the drain-deferred
        loop, which patiently waits out human-admin category-clearing.
        Returns ``False`` iff a finite ``max_wait_secs`` elapses first.

        The first poll fires immediately (no initial sleep). This matters
        when a caller re-enters the wait after a subprocess pass: the
        category may already have room, and sleeping first would waste
        ``poll_secs`` of latency for no observation.
        """
        deadline = clock() + max_wait_secs if max_wait_secs is not None else None
        while True:
            size = self.category_size()
            if size < self.resume_below:
                self._headroom = self._headroom_for(size)
                return True
            if deadline is not None:
                now = clock()
                if now >= deadline:
                    return False
                # Don't oversleep the deadline: a sub-``poll_secs`` budget remaining
                # should wake for its final check on time, not ~poll_secs late.
                nap = min(self.poll_secs, deadline - now)
            else:
                nap = self.poll_secs
            logging.info(
                "Category:Duplicate at %d (>= resume threshold %d); waiting %ds "
                "before retrying deferred duplicate-tags.",
                size,
                self.resume_below,
                self.poll_secs,
            )
            sleep(nap)
