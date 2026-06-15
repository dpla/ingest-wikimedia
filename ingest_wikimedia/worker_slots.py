"""Cross-session box-wide budget for concurrent Commons-writing work.

The SDC sync ``--workers N`` flag (PR #308) parallelizes one partner run
across N worker processes. But the wiki EC2 box typically runs 6+
concurrent wikimedia sessions, and Commons' MediaWiki parser pool only
tolerates ~16 concurrent bot writes before maxlag starts to bind. A
per-session worker count can't see the other sessions, so 6 sessions ×
4 workers = 24 writers would oversubscribe.

This module provides a *box-wide* semaphore that all sessions share:
a fixed directory of N slot files, each acquired with a non-blocking
``fcntl.flock``. A process checks out a slot before its per-item Commons
work and releases it on return. When more would-be writers than slots
exist across all sessions, the excess block until a slot frees.

Both Commons-writing phases draw from the same pool:

  * **SDC sync** — each pool worker (``--workers N``) holds one slot per
    item it processes.
  * **Uploader** — single-process (no parallelism), but holds one slot
    per item too, so it counts as one writer against the shared cap
    alongside SDC-sync workers. (The downloader is deliberately NOT in
    the pool: it writes to source sites, not Commons, so it contends for
    a different resource.)

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

# Poll interval when every slot is currently held. Slots turn over on
# the order of seconds (one per-item SDC sync), so a sub-second poll
# keeps latency low without busy-spinning.
_POLL_INTERVAL_SECONDS = 0.5


class WorkerSlotBudget:
    """A box-wide N-permit semaphore backed by ``fcntl.flock`` slot files.

    ``budget <= 0`` disables the semaphore entirely: :meth:`acquire`
    becomes a no-op context manager. This is the single-process /
    unlimited path — callers don't special-case it.
    """

    def __init__(self, budget: int, slot_dir: str = DEFAULT_SLOT_DIR):
        self.budget = budget
        self.slot_dir = slot_dir
        if budget > 0:
            self._ensure_slot_files()

    def _ensure_slot_files(self) -> None:
        """Create the slot directory and the N slot files if absent.

        Idempotent and safe to call concurrently from multiple
        sessions: ``makedirs(exist_ok=True)`` and ``open(..., "a")``
        both tolerate the file/dir already existing, and neither
        truncates — so a session starting up never disturbs the locks
        another session is already holding on the same files.
        """
        os.makedirs(self.slot_dir, exist_ok=True)
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

        When ``budget <= 0`` the budget is disabled and this yields
        immediately without touching the filesystem.
        """
        if self.budget <= 0:
            yield
            return

        fd = None
        try:
            fd = self._acquire_slot_fd()
            yield
        finally:
            if fd is not None:
                # Closing the fd releases the flock. No explicit
                # LOCK_UN needed — close is the documented release.
                os.close(fd)

    def _acquire_slot_fd(self) -> int:
        """Block until a slot is free; return the held fd.

        Returns the open file descriptor whose flock this call now
        holds. Caller owns closing it.
        """
        waited_logged = False
        while True:
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
            # Every slot busy. Log once so an operator watching the log
            # can see the budget is saturated (expected under heavy
            # concurrency), then poll.
            if not waited_logged:
                logging.info(
                    " -- All %d worker slots busy; waiting for capacity.",
                    self.budget,
                )
                waited_logged = True
            time.sleep(_POLL_INTERVAL_SECONDS)
