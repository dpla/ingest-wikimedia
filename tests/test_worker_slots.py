"""Tests for the box-wide WorkerSlotBudget semaphore.

These exercise the flock-slot-file primitive directly with a temp slot
dir so they don't touch the real ``/tmp/sdc-sync-worker-slots`` other
processes on a dev box might use. Cross-process behaviour (the part that
actually matters) is verified by holding raw flocks on the slot files
to simulate other sessions, rather than spawning real subprocesses.
"""

from __future__ import annotations

import fcntl
import os
import threading
import time

from ingest_wikimedia.worker_slots import WorkerSlotBudget


def _hold_raw_flock(slot_dir, index):
    """Open + non-blocking-flock slot-<index> the way a foreign session
    would, returning the held fd. Caller closes it to release."""
    fd = os.open(os.path.join(slot_dir, f"slot-{index}"), os.O_RDWR | os.O_CREAT, 0o644)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    return fd


def test_creates_slot_files_for_budget(tmp_path):
    """Construction with budget N creates exactly N slot files."""
    WorkerSlotBudget(budget=4, slot_dir=str(tmp_path))
    slots = sorted(p.name for p in tmp_path.iterdir())
    assert slots == ["slot-0", "slot-1", "slot-2", "slot-3"]


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
        # The single slot is now held by us — a foreign non-blocking
        # flock attempt must fail.
        fd = os.open(os.path.join(tmp_path, "slot-0"), os.O_RDWR)
        try:
            failed = False
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                failed = True
            assert failed, "slot-0 should be held while inside acquire()"
        finally:
            os.close(fd)
    # Outside the block the slot is free again — foreign flock succeeds.
    fd = _hold_raw_flock(str(tmp_path), 0)
    os.close(fd)


def test_acquire_picks_a_free_slot_when_some_are_held(tmp_path):
    """With budget 3 and slots 0 and 1 held by foreign sessions,
    acquire() takes slot 2 rather than blocking."""
    budget = WorkerSlotBudget(budget=3, slot_dir=str(tmp_path))
    # Build incrementally so a fd from a successful open is tracked for
    # cleanup even if a later open raises (no leak window).
    held = []
    held.append(_hold_raw_flock(str(tmp_path), 0))
    held.append(_hold_raw_flock(str(tmp_path), 1))
    try:
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
    finally:
        for fd in held:
            os.close(fd)


def test_acquire_blocks_until_a_slot_frees(tmp_path):
    """budget 1, slot already held by a foreign session: acquire()
    blocks, then proceeds once the foreign holder releases."""
    budget = WorkerSlotBudget(budget=1, slot_dir=str(tmp_path))
    foreign_fd = _hold_raw_flock(str(tmp_path), 0)

    proceeded = threading.Event()

    def worker():
        with budget.acquire():
            proceeded.set()

    t = threading.Thread(target=worker)
    t.start()
    # While the foreign session holds the only slot, the worker must NOT
    # have proceeded.
    assert not proceeded.wait(timeout=0.6), "worker entered before a slot was free"
    # Release the foreign hold; the worker should now acquire promptly.
    os.close(foreign_fd)
    assert proceeded.wait(timeout=3), "worker did not proceed after slot freed"
    t.join(timeout=1)


def test_dead_holder_auto_releases_slot(tmp_path):
    """The crash-safety property: a slot held by an fd that gets closed
    (simulating a dead process) frees automatically — no leaked permit.
    flock release on close IS the mechanism that protects against a
    crashed worker, so assert it directly."""
    budget = WorkerSlotBudget(budget=1, slot_dir=str(tmp_path))
    # Simulate a worker that grabbed the slot then died without an
    # orderly release — i.e. just the fd vanishing.
    dead_fd = _hold_raw_flock(str(tmp_path), 0)
    os.close(dead_fd)  # process death == fd close, from the kernel's view

    proceeded = []

    def worker():
        with budget.acquire():
            proceeded.append(True)

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=3)
    assert proceeded == [True], "slot should have been free after holder fd closed"


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
    fd = _hold_raw_flock(str(tmp_path), 0)
    os.close(fd)
