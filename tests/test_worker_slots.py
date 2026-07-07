"""Tests for the box-wide WorkerSlotBudget semaphore.

These exercise the flock-slot-file primitive directly with a temp slot
dir so they don't touch the real ``/tmp/sdc-sync-worker-slots`` other
processes on a dev box might use. Cross-process behaviour (the part that
actually matters) is verified by holding raw flocks on the slot files
to simulate other sessions, rather than spawning real subprocesses.

The flock helpers below open/close their fds within a single lexical
scope (``_foreign_flock`` is a context manager; ``_slot_is_held`` opens
and closes inside one call) so resource lifetime is obvious — no bare
fd is ever returned for the caller to remember to close.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import os
import threading
import time

import pytest

from ingest_wikimedia.worker_slots import WorkerSlotBudget


@contextlib.contextmanager
def _foreign_flock(slot_dir, index):
    """Hold an exclusive flock on slot-<index> the way a foreign session
    would, releasing it (closing the fd) on context exit."""
    fd = os.open(os.path.join(slot_dir, f"slot-{index}"), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


def _slot_is_held(slot_dir, index) -> bool:
    """Return True iff slot-<index> can't be exclusively flock'd right
    now (i.e. something else holds it). Opens and closes its own fd, so
    it never perturbs the lock state it's probing."""
    fd = os.open(os.path.join(slot_dir, f"slot-{index}"), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return False  # we got the lock → nobody else holds it
    except OSError:
        return True
    finally:
        os.close(fd)


def test_creates_slot_files_for_budget(tmp_path):
    """Construction with budget N creates exactly N slot files."""
    WorkerSlotBudget(budget=4, slot_dir=str(tmp_path))
    slots = sorted(p.name for p in tmp_path.iterdir())
    assert slots == ["slot-0", "slot-1", "slot-2", "slot-3"]


def test_rejects_slot_dir_owned_by_a_different_user(tmp_path, monkeypatch):
    """An existing slot dir whose owner doesn't match the current uid is
    refused. An attacker with write access to our slot directory can
    ``unlink`` a held lock file, forcing a new inode the next time we
    ``os.open`` it and silently breaking the exclusion invariant — so
    we fail closed on any ownership mismatch rather than running on a
    potentially-hostile directory."""
    import os as real_os

    real_uid = real_os.getuid()
    monkeypatch.setattr("os.getuid", lambda: real_uid + 1)
    with pytest.raises(RuntimeError, match="owned by uid"):
        WorkerSlotBudget(budget=2, slot_dir=str(tmp_path))


def test_disabled_budget_creates_no_files_and_acquires_freely(tmp_path):
    """budget <= 0 is the disabled / unlimited path: no slot files, and
    acquire() is a no-op context manager that never blocks."""
    budget = WorkerSlotBudget(budget=0, slot_dir=str(tmp_path))
    assert list(tmp_path.iterdir()) == []
    # Many concurrent acquires must all succeed instantly — no cap.
    with budget.acquire(), budget.acquire(), budget.acquire():
        pass  # no exception, no block


def test_acquire_holds_a_slot_for_block_duration(tmp_path):
    """While inside acquire(), one slot file is flock'd and therefore
    can't be flock'd by a simulated foreign session; after the block,
    it frees."""
    budget = WorkerSlotBudget(budget=1, slot_dir=str(tmp_path))
    with budget.acquire():
        assert _slot_is_held(str(tmp_path), 0), (
            "slot-0 should be held while inside acquire()"
        )
    # Outside the block the slot is free again.
    assert not _slot_is_held(str(tmp_path), 0)


def test_acquire_picks_a_free_slot_when_some_are_held(tmp_path):
    """With budget 3 and slots 0 and 1 held by foreign sessions,
    acquire() takes slot 2 rather than blocking."""
    budget = WorkerSlotBudget(budget=3, slot_dir=str(tmp_path))
    with _foreign_flock(str(tmp_path), 0), _foreign_flock(str(tmp_path), 1):
        acquired = []

        def worker():
            with budget.acquire():
                acquired.append(True)
                time.sleep(0.1)

        t = threading.Thread(target=worker)
        t.start()
        t.join(timeout=2)
        assert acquired == [True], "should have taken the one free slot (slot-2)"
        assert not t.is_alive()


def test_acquire_blocks_until_a_slot_frees(tmp_path):
    """budget 1, slot already held by a foreign session: acquire()
    blocks, then proceeds once the foreign holder releases."""
    budget = WorkerSlotBudget(budget=1, slot_dir=str(tmp_path))
    proceeded = threading.Event()

    def worker():
        with budget.acquire():
            proceeded.set()

    # ExitStack so the foreign hold can be released mid-test (not just at
    # block end) while still guaranteeing release if an assert fails.
    stack = contextlib.ExitStack()
    stack.enter_context(_foreign_flock(str(tmp_path), 0))
    t = threading.Thread(target=worker)
    t.start()
    try:
        # While the foreign session holds the only slot, the worker must
        # NOT have proceeded.
        assert not proceeded.wait(timeout=0.6), "worker entered before a slot was free"
    finally:
        stack.close()  # release the foreign hold
    assert proceeded.wait(timeout=3), "worker did not proceed after slot freed"
    t.join(timeout=1)


def test_dead_holder_auto_releases_slot(tmp_path):
    """The crash-safety property: a slot held by an fd that gets closed
    (simulating a dead process) frees automatically — no leaked permit.
    flock release on close IS the mechanism that protects against a
    crashed worker, so assert it directly."""
    budget = WorkerSlotBudget(budget=1, slot_dir=str(tmp_path))
    # Grab the slot then immediately release the fd — process death is,
    # from the kernel's view, just the fd closing.
    with _foreign_flock(str(tmp_path), 0):
        pass

    proceeded = []

    def worker():
        with budget.acquire():
            proceeded.append(True)

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=3)
    assert proceeded == [True], "slot should have been free after holder fd closed"


def test_acquire_reraises_non_contention_oserror(tmp_path, monkeypatch):
    """A non-contention OSError from flock (e.g. ENOLCK / EACCES) must
    propagate, not be swallowed as "slot busy" and spun on forever. Only
    EAGAIN/EWOULDBLOCK mean contention."""
    budget = WorkerSlotBudget(budget=1, slot_dir=str(tmp_path))

    def boom(fd, op):
        raise OSError(errno.ENOLCK, "no locks available")

    monkeypatch.setattr(fcntl, "flock", boom)
    raised = None
    try:
        with budget.acquire():
            pass
    except OSError as e:
        raised = e
    assert raised is not None and raised.errno == errno.ENOLCK, (
        "non-contention flock errno must propagate, not loop forever"
    )


def test_acquire_releases_on_exception_in_block(tmp_path):
    """If the with-block raises, the slot still frees (finally clause).
    Otherwise one bad item would permanently burn a slot."""
    budget = WorkerSlotBudget(budget=1, slot_dir=str(tmp_path))
    try:
        with budget.acquire():
            raise ValueError("boom")
    except ValueError:
        pass
    # Slot must be free despite the exception.
    assert not _slot_is_held(str(tmp_path), 0)


def test_total_wait_seconds_zero_when_uncontended(tmp_path):
    """An acquire that finds a free slot on the first scan adds ~0 to the
    cumulative wait counter, so an uncontended session reports no
    contention."""
    budget = WorkerSlotBudget(budget=2, slot_dir=str(tmp_path))
    with budget.acquire():
        pass
    assert budget.total_wait_seconds < 0.5


def test_total_wait_seconds_accumulates_when_blocked(tmp_path):
    """When the only slot is held by a foreign session, acquire() blocks
    and the blocked time is added to total_wait_seconds once it proceeds."""
    budget = WorkerSlotBudget(budget=1, slot_dir=str(tmp_path))
    proceeded = threading.Event()

    def worker():
        with budget.acquire():
            proceeded.set()

    stack = contextlib.ExitStack()
    stack.enter_context(_foreign_flock(str(tmp_path), 0))
    t = threading.Thread(target=worker)
    t.start()
    try:
        # Hold the only slot long enough that the worker must poll-wait.
        assert not proceeded.wait(timeout=0.8)
    finally:
        stack.close()  # release so the worker can proceed
    assert proceeded.wait(timeout=3)
    t.join(timeout=1)
    assert not t.is_alive(), "worker thread should exit after slot release"
    assert budget.total_wait_seconds > 0.3


# ---- Two-tier (uploader priority + shared fallback) behaviour ----


def test_two_tier_prefers_priority_pool_over_fallback(tmp_path):
    """Uploader budget with a priority pool AND a shared fallback must
    consume a priority slot first whenever one is free, leaving the
    shared pool entirely untouched. This is the whole point of the
    dedicated pool — SDC workers using the shared pool must never see
    an uploader holding one of their slots when there's priority
    capacity to spare."""
    priority_dir = tmp_path / "priority"
    shared_dir = tmp_path / "shared"
    priority_dir.mkdir()
    shared_dir.mkdir()
    shared = WorkerSlotBudget(budget=3, slot_dir=str(shared_dir))
    uploader = WorkerSlotBudget(budget=2, slot_dir=str(priority_dir), fallback=shared)
    with uploader.acquire():
        # Priority pool: slot-0 must be held; shared pool: nothing held.
        assert _slot_is_held(str(priority_dir), 0), (
            "uploader should consume a priority slot first"
        )
        assert not any(_slot_is_held(str(shared_dir), i) for i in range(3)), (
            "shared pool must remain untouched while priority capacity is free"
        )


def test_two_tier_falls_back_to_shared_when_priority_saturated(tmp_path):
    """When every priority slot is held (simulating the uploader's
    priority pool being fully consumed by other simultaneous uploader
    sessions), the next uploader acquire MUST fall back to a shared
    slot rather than block — overflow is the point of the fallback."""
    priority_dir = tmp_path / "priority"
    shared_dir = tmp_path / "shared"
    priority_dir.mkdir()
    shared_dir.mkdir()
    shared = WorkerSlotBudget(budget=3, slot_dir=str(shared_dir))
    uploader = WorkerSlotBudget(budget=2, slot_dir=str(priority_dir), fallback=shared)
    with (
        _foreign_flock(str(priority_dir), 0),
        _foreign_flock(str(priority_dir), 1),
    ):
        acquired = []

        def worker():
            with uploader.acquire():
                # Priority pool is fully held by foreigners; the
                # uploader must be in the shared pool now.
                acquired.append(
                    any(_slot_is_held(str(shared_dir), i) for i in range(3))
                )

        t = threading.Thread(target=worker)
        t.start()
        t.join(timeout=2)
        assert not t.is_alive(), "uploader must not block when fallback has room"
        assert acquired == [True], "uploader fallback should have taken a shared slot"


def test_two_tier_blocks_only_when_both_pools_saturated(tmp_path):
    """If both the priority pool AND the shared pool are fully held,
    the uploader must poll-wait — the two-tier composition shouldn't
    accidentally bypass the cap. Once any slot in either pool frees,
    acquisition proceeds."""
    priority_dir = tmp_path / "priority"
    shared_dir = tmp_path / "shared"
    priority_dir.mkdir()
    shared_dir.mkdir()
    shared = WorkerSlotBudget(budget=1, slot_dir=str(shared_dir))
    uploader = WorkerSlotBudget(budget=1, slot_dir=str(priority_dir), fallback=shared)
    proceeded = threading.Event()

    def worker():
        with uploader.acquire():
            proceeded.set()

    stack = contextlib.ExitStack()
    stack.enter_context(_foreign_flock(str(priority_dir), 0))
    stack.enter_context(_foreign_flock(str(shared_dir), 0))
    t = threading.Thread(target=worker)
    t.start()
    try:
        # Both pools fully held → worker must block.
        assert not proceeded.wait(timeout=0.8)
    finally:
        stack.close()  # release both, worker should proceed
    assert proceeded.wait(timeout=3), "should proceed once any pool frees"
    t.join(timeout=1)
    assert not t.is_alive()


def test_two_tier_sdc_path_never_touches_priority_pool(tmp_path):
    """SDC sync constructs its budget with NO fallback, so it can only
    ever consume shared slots — the priority pool is uploader-only by
    construction. Pin this so a future refactor doesn't accidentally
    wire SDC into the priority pool and undo the whole guarantee."""
    priority_dir = tmp_path / "priority"
    shared_dir = tmp_path / "shared"
    priority_dir.mkdir()
    shared_dir.mkdir()
    # SDC's budget is plain — no fallback, single pool.
    sdc = WorkerSlotBudget(budget=2, slot_dir=str(shared_dir))
    assert sdc.fallback is None
    with sdc.acquire():
        assert not any(_slot_is_held(str(priority_dir), i) for i in range(2)), (
            "SDC sync must not lock priority slots even by accident"
        )


def test_priority_pool_release_does_not_leak_shared_slot(tmp_path):
    """Closing the acquired fd releases the flock regardless of which
    pool's directory the slot lives in. Spec-pin so a stray bookkeeping
    bug can't leak a shared slot when the uploader had been in the
    priority pool, or vice versa."""
    priority_dir = tmp_path / "priority"
    shared_dir = tmp_path / "shared"
    priority_dir.mkdir()
    shared_dir.mkdir()
    shared = WorkerSlotBudget(budget=2, slot_dir=str(shared_dir))
    uploader = WorkerSlotBudget(budget=2, slot_dir=str(priority_dir), fallback=shared)
    # Force the fallback path by holding all priority slots.
    with _foreign_flock(str(priority_dir), 0), _foreign_flock(str(priority_dir), 1):
        with uploader.acquire():
            held = [_slot_is_held(str(shared_dir), i) for i in range(2)]
            assert sum(held) == 1, "exactly one shared slot held during the block"
    # After the block both shared slots must be free again.
    assert all(not _slot_is_held(str(shared_dir), i) for i in range(2)), (
        "shared slot must release when the uploader's with-block exits"
    )


# ---- Parallel-worker gate + priority-holdings behaviour ----
#
# When an uploader session runs multiple workers (``--workers N``), the
# session's slot budget carries a ``multiprocessing.Semaphore(1)`` gate
# and a ``multiprocessing.Value('i', 0)`` counter to enforce:
#
#   (a) at most ONE shared-pool slot held per session, and
#   (b) NO shared-pool slot held when the session also holds any
#       priority slot.
#
# Tests below simulate the shared-across-workers state with plain
# ``threading`` primitives (a threading.Semaphore(1) and a small object
# with the same value/get_lock() surface as multiprocessing.Value) —
# WorkerSlotBudget only uses those attributes, so a threading double is
# a valid substitute in-process. Real cross-process coordination is
# covered by mypy/importability + the multiprocessing.Value contract.


class _FakeMPValue:
    """Threading substitute for ``multiprocessing.Value('i', 0)``.

    ``WorkerSlotBudget`` only touches ``.value`` and ``.get_lock()``.
    Using a threading.Lock here lets in-process tests exercise the
    same code paths without spawning subprocesses; the invariant the
    class actually enforces (increment on grab, decrement on release,
    atomically via the lock) is the same either way.
    """

    def __init__(self, initial: int = 0):
        self.value = initial
        self._lock = threading.Lock()

    def get_lock(self):
        return self._lock


def test_fallback_gate_serialises_shared_pool_acquisition(tmp_path):
    """When two workers of the SAME session both try to fall back to
    shared, only one succeeds — the other is blocked by the gate
    Semaphore and waits for priority instead. This is the invariant
    that caps a 4-uploader worst case at 4 priority + 3 shared = 7
    slots rather than 4 + 12."""
    priority_dir = tmp_path / "priority"
    shared_dir = tmp_path / "shared"
    priority_dir.mkdir()
    shared_dir.mkdir()
    shared = WorkerSlotBudget(budget=4, slot_dir=str(shared_dir))
    gate = threading.Semaphore(1)
    holdings = _FakeMPValue(0)
    uploader = WorkerSlotBudget(
        budget=2,
        slot_dir=str(priority_dir),
        fallback=shared,
        fallback_gate=gate,
        priority_holdings=holdings,
    )
    # Force fallback: hold both priority slots foreign.
    with _foreign_flock(str(priority_dir), 0), _foreign_flock(str(priority_dir), 1):
        # Worker 1 grabs a shared slot; hold it while probing Worker 2.
        in_block = threading.Event()
        release = threading.Event()

        def worker1():
            with uploader.acquire():
                in_block.set()
                release.wait(timeout=5)

        t1 = threading.Thread(target=worker1)
        t1.start()
        assert in_block.wait(timeout=2), "worker1 never entered its acquire block"

        # Exactly one shared slot must be held right now.
        shared_held = sum(_slot_is_held(str(shared_dir), i) for i in range(4))
        assert shared_held == 1, (
            f"expected 1 shared slot held by worker1, got {shared_held}"
        )

        # Worker 2 tries to fall back — gate is taken, so it must NOT
        # succeed while worker1 is still holding.
        worker2_got_slot = threading.Event()

        def worker2():
            with uploader.acquire():
                worker2_got_slot.set()

        t2 = threading.Thread(target=worker2)
        t2.start()
        assert not worker2_got_slot.wait(timeout=0.6), (
            "worker2 acquired a shared slot while worker1 already held one; "
            "gate should have blocked it"
        )
        # Shared holdings still at 1, not 2.
        assert sum(_slot_is_held(str(shared_dir), i) for i in range(4)) == 1

        # Release worker1 → worker2 can now proceed (grabs gate + shared).
        release.set()
        assert worker2_got_slot.wait(timeout=3), (
            "worker2 did not proceed after worker1 released the gate"
        )
        t1.join(timeout=1)
        t2.join(timeout=1)


def test_fallback_skipped_when_session_already_holds_priority(tmp_path):
    """A session that already holds a priority slot must NOT spill
    into shared for a subsequent worker's acquire, even if all
    remaining priority slots are held by other sessions. Enforces the
    invariant "hold priority OR shared, never both" per session."""
    priority_dir = tmp_path / "priority"
    shared_dir = tmp_path / "shared"
    priority_dir.mkdir()
    shared_dir.mkdir()
    shared = WorkerSlotBudget(budget=4, slot_dir=str(shared_dir))
    gate = threading.Semaphore(1)
    holdings = _FakeMPValue(0)
    uploader = WorkerSlotBudget(
        budget=2,
        slot_dir=str(priority_dir),
        fallback=shared,
        fallback_gate=gate,
        priority_holdings=holdings,
    )

    in_block = threading.Event()
    release = threading.Event()

    # Worker 1 grabs a priority slot (slot-0). While it holds:
    def worker1():
        with uploader.acquire():
            in_block.set()
            release.wait(timeout=5)

    t1 = threading.Thread(target=worker1)
    t1.start()
    assert in_block.wait(timeout=2)
    assert holdings.value == 1, "priority_holdings should increment on grab"

    # Simulate another session holding the remaining priority slot so
    # Worker 2 would spill to shared under the OLD semantics.
    with _foreign_flock(str(priority_dir), 1):
        worker2_took_slot = threading.Event()
        worker2_took_shared = []

        def worker2():
            with uploader.acquire():
                worker2_took_shared.append(
                    any(_slot_is_held(str(shared_dir), i) for i in range(4))
                )
                worker2_took_slot.set()

        t2 = threading.Thread(target=worker2)
        t2.start()
        # Session already holds a priority slot via worker1, so worker2
        # must wait for priority — MUST NOT fall back to shared.
        assert not worker2_took_slot.wait(timeout=0.6), (
            "worker2 acquired while session already holds priority — "
            "shared-pool fallback must be inhibited"
        )
        # Shared pool must remain untouched.
        assert not any(_slot_is_held(str(shared_dir), i) for i in range(4)), (
            "shared pool should remain empty when session already holds priority"
        )

        # Release worker1's priority slot → worker2 can grab it (still
        # priority, not shared, because holdings drops to 0 and slot-0
        # is free while slot-1 stays foreign-held).
        release.set()
        assert worker2_took_slot.wait(timeout=3)
        assert worker2_took_shared == [False], (
            f"worker2 should have taken a priority slot, not shared; "
            f"took_shared={worker2_took_shared}"
        )
    t1.join(timeout=1)
    t2.join(timeout=1)
    assert holdings.value == 0, (
        "priority_holdings should decrement back to 0 after all workers release"
    )


def test_gate_released_alongside_fd(tmp_path):
    """The gate acquired for a shared-pool grab must release when the
    with-block exits, so a subsequent acquire on the same session can
    grab shared again. Otherwise a session that took shared once
    would permanently lock out its own future shared attempts."""
    priority_dir = tmp_path / "priority"
    shared_dir = tmp_path / "shared"
    priority_dir.mkdir()
    shared_dir.mkdir()
    shared = WorkerSlotBudget(budget=2, slot_dir=str(shared_dir))
    gate = threading.Semaphore(1)
    holdings = _FakeMPValue(0)
    uploader = WorkerSlotBudget(
        budget=1,
        slot_dir=str(priority_dir),
        fallback=shared,
        fallback_gate=gate,
        priority_holdings=holdings,
    )
    # Force fallback both times.
    with _foreign_flock(str(priority_dir), 0):
        with uploader.acquire():
            # Gate is held here; a non-blocking acquire would fail.
            assert not gate.acquire(blocking=False), (
                "gate must be held while inside the with-block"
            )
        # Gate must be released now.
        assert gate.acquire(blocking=False), (
            "gate must be released when the with-block exits"
        )
        gate.release()
        # A second acquire should succeed (gate free).
        with uploader.acquire():
            pass


def test_priority_holdings_counter_decrements_on_exception(tmp_path):
    """A with-block that raises still decrements ``priority_holdings``
    so the session's counter doesn't leak on transient errors."""
    priority_dir = tmp_path / "priority"
    priority_dir.mkdir()
    holdings = _FakeMPValue(0)
    uploader = WorkerSlotBudget(
        budget=2,
        slot_dir=str(priority_dir),
        priority_holdings=holdings,
    )
    try:
        with uploader.acquire():
            assert holdings.value == 1
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert holdings.value == 0, (
        "priority_holdings must decrement even when the with-block raises"
    )
