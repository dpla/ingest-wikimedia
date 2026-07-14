"""Cross-session box-wide budget for concurrent Commons-writing work.

The SDC sync ``--workers N`` flag (PR #308) parallelizes one partner run
across N worker processes. But the wiki EC2 box typically runs several
concurrent wikimedia sessions, and Commons' MediaWiki parser pool only
tolerates ~16 concurrent bot writes before maxlag starts to bind. A
per-session worker count can't see the other sessions, so N sessions ×
K workers each would oversubscribe once N × K > the box-wide budget.

This module provides a *box-wide* semaphore that all sessions share:
a fixed directory of N slot files, each acquired with a non-blocking
``fcntl.flock``. A process checks out a slot before its per-item Commons
work and releases it on return. When more would-be writers than slots
exist across all sessions, the excess block until a slot frees.

Both Commons-writing phases draw from the slot pool, but with a tier:

  * **Shared pool** (``DEFAULT_SLOT_DIR``) — the box-wide budget every
    SDC-sync worker contends over. Each pool worker (``--workers N``)
    holds one slot per item it processes.
  * **Uploader priority pool** (``UPLOADER_PRIORITY_SLOT_DIR``) — a
    smaller dedicated pool (``UPLOADER_PRIORITY_SLOTS``) that ONLY
    uploaders use. The uploader's :class:`WorkerSlotBudget` is wired
    with the priority pool as primary and the shared pool as
    ``fallback``, so it acquires from priority first and spills into
    shared only when all priority slots are held by other simultaneous
    uploaders. SDC sync constructs no fallback, so it can never lock
    a priority slot.

    Net effect: an uploader is never blocked by SDC-sync workers as
    long as fewer than ``UPLOADER_PRIORITY_SLOTS`` uploader items are
    box-wide in flight. The priority pool is additive (not carved out
    of the shared budget), trading ~4 more concurrent Commons writers
    under saturation for guaranteed uploader headroom — a deliberate
    choice given uploaders are slow-moving and SDC sessions are
    memory-cheap.

  * **Downloader** is deliberately not in either pool: it writes to
    source sites, not Commons, so it contends for a different resource.

So the cap bounds the number of DPLA *items* being actively written to
Commons at once across the whole box — summed over every upload and
SDC-sync session — to N. Because each item's writer issues its
per-ordinal Commons writes sequentially, that also bounds the number of
concurrent write *streams* to N: a deliberately loose proxy for
"concurrent writes", not a tight per-write rate limit. A 1-ordinal item
and a 1500-ordinal item each occupy exactly one slot, so a slot held by
a large item represents far more write work than one held by a small
item. That's acceptable here: the real per-process safety net against
overrunning Commons is pywikibot's own ``maxlag`` backoff (each process
has its own session and honors it independently); the slot budget is the
coarser, cross-session throttle that keeps the *number* of
simultaneously-writing processes bounded so they don't collectively
stampede the parser pool. Per-write slot acquisition would make the cap
a tight write-rate bound but at the cost of ~1 lock cycle per ordinal
(1500+ on a large item) for no practical gain over maxlag.

Note: a long-running uploader holding a slot for the duration of a big
item can make SDC-sync workers (or other uploaders) block waiting for
capacity — that is the intended cooperative throttle, not a bug. Size N
with the combined upload + SDC-sync population in mind.

Why flock slot files rather than a SysV/POSIX semaphore:

  * **Crash-safe.** ``flock`` locks release automatically when the
    holding fd is closed *or the process dies* — so a worker that
    segfaults or is OOM-killed mid-item frees its slot immediately,
    with no leaked-permit accounting to repair. A SysV semaphore
    leaks a permit on holder death unless SEM_UNDO is set up
    perfectly, which is exactly the failure mode that bites at 3am.
  * **Inspectable.** ``lslocks`` / ``ls`` show the live state; an
    operator can see and reason about the budget without special
    tooling.
  * **No cleanup contract.** Slot files are created once and left in
    place; there's no "last one out frees the IPC object" dance.

Consistency note: the budget value N must be the same across all
concurrent sessions for the cap to mean what it says. In practice it
comes from a single launch-time default, so all sessions agree. If two
sessions disagree (N=16 vs N=8), the effective cap is the larger N but
the smaller-N session only ever competes for the lower slots — a
benign degradation, not a correctness break.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import logging
import os
import time

# Mode for slot files: owner read/write only. They carry no data (the
# flock is the whole signal), so group/other access buys nothing and a
# stricter mode keeps another local account from interfering with the
# lock files on a shared host.
_SLOT_FILE_MODE = 0o600

# errnos ``flock(LOCK_NB)`` raises specifically for "another holder has
# it" — the only condition that warrants moving on to the next slot /
# polling. Any other OSError (EACCES, ENOLCK, EIO, …) is a real fault
# that must propagate rather than spin forever.
_FLOCK_CONTENDED_ERRNOS = frozenset({errno.EAGAIN, errno.EWOULDBLOCK})

# Box-wide shared directory holding the slot lock files. Fixed path so
# every ``sdc-sync`` process on the host contends over the same set.
# Under /tmp because the locks are inherently ephemeral — a reboot that
# clears /tmp also kills every holder, so there's nothing to preserve.
DEFAULT_SLOT_DIR = "/tmp/sdc-sync-worker-slots"

# Dedicated priority-pool directory for uploader sessions. Smaller pool
# (UPLOADER_PRIORITY_SLOTS) that ONLY uploaders contend over, kept on a
# separate path so SDC-sync workers — which never use this directory —
# can't lock its slots. Uploaders try this pool first per acquisition
# pass and fall back to ``DEFAULT_SLOT_DIR`` only when all priority
# slots are held by other uploaders. Net effect: an uploader is never
# blocked by SDC-sync workers as long as fewer than
# ``UPLOADER_PRIORITY_SLOTS`` uploader items are simultaneously in
# flight box-wide.
UPLOADER_PRIORITY_SLOT_DIR = "/tmp/dpla-uploader-priority-slots"

# Default size of the dedicated uploader priority pool. Sized for
# typical box concurrency (1–4 uploader sessions, each holding ≤1 slot
# at any moment): four priority slots fully absorbs the common case
# without ever queueing behind SDC-sync workers.
#
# This pool is ADDITIVE to the shared pool — total writers on the box
# = shared budget + UPLOADER_PRIORITY_SLOTS — because uploaders are
# slow-moving (one item per session at a time, big multi-page items
# hold a slot for many minutes) and SDC sessions are memory-cheap, so
# carving out of the existing shared budget would shrink SDC concurrency
# without buying much for uploads. Adding fresh capacity costs ~4 more
# concurrent Commons writers under saturation, which Commons' parser
# pool tolerates comfortably.
UPLOADER_PRIORITY_SLOTS = 4

# Poll interval when every slot is currently held. The poll only fires
# when a scan finds ZERO free slots, so raising this doesn't add wall
# time to any acquire that finds a slot on the first pass — it only
# lengthens the average wait during full saturation. Under saturation,
# a released slot is picked up either by the releasing worker's next
# acquire (which fires immediately, no poll) or by whichever contender's
# scan happens to align with the release, so the freed slot is almost
# never idle for a full poll cycle. 1 s keeps latency well below the
# per-item work time (median ~1–2 s for a single-file item, tens of
# seconds to minutes for multi-page items) while cutting the poll-storm
# rate by half at high concurrency.
_POLL_INTERVAL_SECONDS = 1.0

# Stable fragment of the "all slots held" log line. Status tooling greps
# the sdc-sync log tail for this to tell that a session's workers are
# blocked on the cap; keep it in the format string below so the two can't
# drift.
SLOTS_BUSY_LOG_MARKER = "worker slots busy"


class WorkerSlotBudget:
    """A box-wide N-permit semaphore backed by ``fcntl.flock`` slot files.

    ``budget <= 0`` disables the semaphore entirely: :meth:`acquire`
    becomes a no-op context manager. This is the single-process /
    unlimited path — callers don't special-case it.

    Optional ``fallback``: when set, :meth:`acquire` tries this budget
    first (single non-blocking pass per iteration) and only consults
    ``fallback`` once the primary pool is fully held. Used for the
    uploader's two-tier setup: a small dedicated pool (this budget)
    that the uploader tries first, with the box-wide shared pool
    (``fallback``) as overflow. The fallback's slots come from a
    DIFFERENT directory and a DIFFERENT lock-file set, so they're
    accounted for separately and SDC workers using only the fallback
    can never lock priority slots.

    Optional ``fallback_gate`` + ``priority_holdings`` (uploader
    parallel-workers case): when a session runs multiple workers
    (``--workers N``), we want it to greedily take priority slots
    (up to N) while capping its shared-pool usage at AT MOST ONE slot
    per session — so 4 concurrent uploader sessions occupy 4 priority
    + 3 shared = 7 slots worst-case, not 4 + 12. ``fallback_gate`` is
    a ``multiprocessing.Semaphore(1)`` shared across the session's
    pool workers; a worker MUST hold this gate to attempt the
    fallback pool. ``priority_holdings`` is a ``multiprocessing.Value
    ('i', 0)`` shared across the same pool, incremented on priority-
    slot grab and decremented on release. When ``priority_holdings >
    0`` (i.e. this session already holds at least one priority slot),
    subsequent workers skip the fallback path entirely and wait for
    priority — the shared slot naturally drops out of the session's
    holdings on its next item boundary, without needing an active
    "yield my shared slot now" signal.
    """

    def __init__(
        self,
        budget: int,
        slot_dir: str = DEFAULT_SLOT_DIR,
        fallback: "WorkerSlotBudget | None" = None,
        fallback_gate=None,
        priority_holdings=None,
    ):
        self.budget = budget
        self.slot_dir = slot_dir
        self.fallback = fallback
        self.fallback_gate = fallback_gate
        self.priority_holdings = priority_holdings
        # Cumulative seconds this process spent blocked in acquire() waiting
        # for a free slot. ~0 when the budget isn't contended; grows under
        # saturation. Callers read it to report slot contention.
        self.total_wait_seconds = 0.0
        if budget > 0:
            self._ensure_slot_files()

    def _ensure_slot_files(self) -> None:
        """Create the slot directory and the N slot files if absent.

        Idempotent and safe to call concurrently from multiple
        sessions: ``makedirs(exist_ok=True)`` and ``open(..., "a")``
        both tolerate the file/dir already existing, and neither
        truncates — so a session starting up never disturbs the locks
        another session is already holding on the same files.

        Validates that an existing slot directory is owned by the
        current user before reusing it. Catches a future deployment-
        model change where the host gains other local accounts — an
        attacker who can write into our slot dir can `unlink` a held
        lock file, forcing a new inode the next time we ``os.open``
        it and silently breaking the exclusion invariant. On today's
        single-tenant EC2 the check is a no-op; the cost is one
        ``os.stat`` per session start.
        """
        os.makedirs(self.slot_dir, exist_ok=True)
        st = os.stat(self.slot_dir)
        if st.st_uid != os.getuid():
            raise RuntimeError(
                f"Worker slot dir {self.slot_dir!r} is owned by uid "
                f"{st.st_uid}, not the current user (uid {os.getuid()}); "
                "refusing to use it (an attacker with write access could "
                "unlink held lock files and break the exclusion invariant)."
            )
        for i in range(self.budget):
            path = os.path.join(self.slot_dir, f"slot-{i}")
            # "a" creates-if-absent without truncating; immediately
            # closed because we only need the file to exist. Acquisition
            # opens its own fd per attempt.
            with open(path, "a"):
                pass

    @contextlib.contextmanager
    def acquire(self):
        """Acquire one slot for the duration of the ``with`` block.

        Scans the slot files for one not currently flock'd, taking the
        first free one. If every slot is held, sleeps
        ``_POLL_INTERVAL_SECONDS`` and rescans — blocking until capacity
        frees. The held fd is closed on exit, which releases the flock
        (also released automatically if this process dies while
        holding it).

        When ``budget <= 0`` the budget is disabled, AND no fallback is
        set, this yields immediately without touching the filesystem.
        When ``budget <= 0`` but a fallback exists, defers to the
        fallback's normal acquisition — preserving the "primary disabled,
        still capped by fallback" semantics callers may rely on.
        """
        if self.budget <= 0 and self.fallback is None:
            yield
            return

        fd = None
        got_priority = False
        gate_held = False
        try:
            start = time.monotonic()
            fd, got_priority, gate_held = self._acquire_slot_fd()
            self.total_wait_seconds += time.monotonic() - start
            yield
        finally:
            if fd is not None:
                # Closing the fd releases the flock. No explicit
                # LOCK_UN needed — close is the documented release.
                os.close(fd)
            if got_priority and self.priority_holdings is not None:
                with self.priority_holdings.get_lock():
                    self.priority_holdings.value -= 1
            if gate_held and self.fallback_gate is not None:
                self.fallback_gate.release()

    def _try_acquire_one_pass(self) -> int | None:
        """One non-blocking scan over this budget's slot files. Returns
        the held fd on success, ``None`` if every slot is currently held.

        Doesn't sleep, doesn't log, doesn't recurse into ``fallback`` —
        that policy lives in ``_acquire_slot_fd``. A disabled budget
        (``budget <= 0``) returns ``None`` immediately so callers
        composing tiers can move on to the next pool.
        """
        if self.budget <= 0:
            return None
        for i in range(self.budget):
            path = os.path.join(self.slot_dir, f"slot-{i}")
            fd = os.open(path, os.O_RDWR | os.O_CREAT, _SLOT_FILE_MODE)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return fd
            except OSError as e:
                os.close(fd)
                if e.errno in _FLOCK_CONTENDED_ERRNOS:
                    # Slot held by another worker/session — try the
                    # next one. Any other errno (permission, ENOLCK,
                    # I/O) is a real fault: re-raise rather than spin
                    # forever masking it.
                    continue
                raise
        return None

    def _acquire_slot_fd(self) -> tuple[int, bool, bool]:
        """Block until a slot is free; return ``(fd, got_priority, gate_held)``.

        Tries the primary pool first (single non-blocking pass), then —
        only if all primary slots are held — the fallback pool. Sleeps
        and retries when both are saturated. Caller owns closing the
        returned fd; closing releases the flock regardless of which
        pool's directory it came from.

        ``got_priority`` is True iff the returned fd came from THIS
        budget (primary). Caller uses it to know whether to decrement
        :attr:`priority_holdings` when releasing.

        ``gate_held`` is True iff the returned fd came from the fallback
        pool via successfully acquiring :attr:`fallback_gate`. Caller
        uses it to know whether to release the gate alongside the fd.

        Fallback path is gated by two conditions when both are
        configured: :attr:`priority_holdings` must be 0 at check time
        AND :attr:`fallback_gate` must be acquirable non-blockingly.
        Re-checks ``priority_holdings`` after grabbing the gate to
        close the race window where another worker took a priority
        slot between the initial check and the gate acquisition — a
        session that has ANY priority holding must never also take a
        shared slot per the invariant.
        """
        waited_logged = False
        while True:
            fd = self._try_acquire_one_pass()
            if fd is not None:
                if self.priority_holdings is not None:
                    with self.priority_holdings.get_lock():
                        self.priority_holdings.value += 1
                return fd, True, False
            if self.fallback is not None and self._session_may_use_fallback():
                fd, gate_held = self._try_fallback_with_gate()
                if fd is not None:
                    return fd, False, gate_held
            # Every slot busy across primary AND fallback. Log once so an
            # operator watching the log can see the budget is saturated
            # (expected under heavy concurrency), then poll.
            if not waited_logged:
                total = self.budget + (self.fallback.budget if self.fallback else 0)
                logging.info(
                    " -- All %d %s; waiting for capacity.",
                    total,
                    SLOTS_BUSY_LOG_MARKER,
                )
                waited_logged = True
            time.sleep(_POLL_INTERVAL_SECONDS)

    def _session_may_use_fallback(self) -> bool:
        """True iff this session hasn't already claimed a priority slot.

        Returning False here forces the caller to keep waiting on the
        primary pool instead of spilling into fallback — preserving the
        invariant that a session holding ANY priority slot must not also
        hold a shared-pool slot. When :attr:`priority_holdings` isn't
        configured (single-worker uploaders, sdc-sync), unconditionally
        True — the invariant only applies to the parallel-worker case.
        """
        if self.priority_holdings is None:
            return True
        with self.priority_holdings.get_lock():
            return self.priority_holdings.value == 0

    def _try_fallback_with_gate(self) -> tuple[int | None, bool]:
        """Attempt one fallback-pool acquisition, gated by
        :attr:`fallback_gate` (per-session Semaphore(1)) when set.

        Returns ``(fd, gate_held)``. ``fd`` is None on failure (gate
        contended, or gate acquired but no fallback slot free, or
        holdings-recheck aborted the attempt). Never blocks — polling
        is the caller's job.
        """
        # No gate configured (sdc-sync single-worker legacy path):
        # try fallback directly. gate_held stays False.
        if self.fallback_gate is None:
            fd = self.fallback._try_acquire_one_pass()
            return fd, False

        # Non-blocking gate acquire. Another worker in this session
        # already holds it → skip fallback this iteration.
        if not self.fallback_gate.acquire(False):
            return None, False

        # Gate held. Re-check priority_holdings to close the race
        # window where another worker got a priority slot between the
        # caller's initial check and this point.
        if self.priority_holdings is not None:
            with self.priority_holdings.get_lock():
                if self.priority_holdings.value > 0:
                    self.fallback_gate.release()
                    return None, False

        fd = self.fallback._try_acquire_one_pass()
        if fd is None:
            self.fallback_gate.release()
            return None, False
        return fd, True
