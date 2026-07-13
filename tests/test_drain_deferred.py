"""Tests for the ``drain-deferred`` command (``tools.drain_deferred``)."""

from __future__ import annotations

import contextlib
import fcntl
import os
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from ingest_wikimedia import await_target_free_sidecar, drain_sidecar
from tools import drain_deferred


@pytest.fixture(autouse=True)
def override_root(tmp_path, monkeypatch):
    """Redirect ``drain_sidecar``'s absolute-anchor root into ``tmp_path``
    so tests don't touch (or depend on) the real
    ``/home/ec2-user/ingest-wikimedia/`` tree."""
    monkeypatch.setattr(drain_sidecar, "INGEST_WIKI_ROOT", tmp_path)
    return tmp_path


@pytest.fixture(autouse=True)
def stub_host_lock(monkeypatch, tmp_path):
    """Redirect the host-level lock file into ``tmp_path`` so tests
    don't clobber (or block on) the real ``/home/ec2-user/...`` lock."""
    monkeypatch.setattr(
        drain_deferred, "_DRAIN_LOCK_PATH", str(tmp_path / ".drain-lock")
    )


@pytest.fixture(autouse=True)
def stub_setup_logging_and_get_site(monkeypatch):
    """The real ``setup_logging`` writes to ``<partner>/logs/...`` and
    expects a partner dir; tests just need it to be a no-op. Also
    stub ``get_site`` — the real helper runs ``pywikibot.Site(...)``
    plus ``.login()`` and requires a ``user-config.py``, which tests
    don't have. Every test in this file patches
    ``DuplicateCategoryThrottle``, so the return value is only used
    as an opaque argument to the patched throttle constructor."""
    monkeypatch.setattr(drain_deferred, "setup_logging", lambda *a, **kw: None)
    monkeypatch.setattr(drain_deferred, "get_site", MagicMock)


@pytest.fixture
def lock_fd():
    """A real (closable) file descriptor for stubbing
    ``_acquire_host_lock`` — ``main()`` closes the lock fd in its
    ``finally`` via ``os.close()``, so a MagicMock won't do. The
    fixture guarantees cleanup itself in case a test bails before
    ``main()`` gets to close it (suppressing the EBADF from the
    already-closed happy path)."""
    fd = os.open(os.devnull, os.O_RDONLY)
    try:
        yield fd
    finally:
        with contextlib.suppress(OSError):
            os.close(fd)


def test_empty_sidecar_early_exits_without_starting_the_loop(monkeypatch):
    """The common case: no items deferred. Command must exit without
    acquiring the host lock, without polling the category, and
    without invoking uploader/sdc-sync subprocesses."""
    lock_mock = MagicMock()
    with (
        patch("tools.drain_deferred._acquire_host_lock", lock_mock),
        patch("tools.drain_deferred.subprocess.run") as subproc,
        patch("tools.drain_deferred.DuplicateCategoryThrottle") as throttle_ctor,
        patch("tools.drain_deferred.notify_drain_phase_start") as start_ping,
        patch("tools.drain_deferred.notify_drain_phase_complete") as done_ping,
    ):
        result = CliRunner().invoke(drain_deferred.main, ["nara"])
    assert result.exit_code == 0, result.output
    lock_mock.assert_not_called()
    subproc.assert_not_called()
    throttle_ctor.assert_not_called()
    start_ping.assert_not_called()
    done_ping.assert_not_called()


def test_populated_sidecar_runs_drain_loop_until_empty(monkeypatch, lock_fd):
    """Non-empty sidecar: acquire host lock, poll category, run
    uploader + sdc-sync subprocesses, and loop until the sidecar is
    empty. The drain itself removes the round's IDs from the sidecar
    before invoking the subprocesses; a successful re-run (nothing
    re-deferred) leaves it empty, so the fake uploader is a no-op —
    as the real uploader would be when every item completes.
    """
    drain_sidecar.write_sidecar("nara", ["id-1"])

    def fake_wait(*a, **kw):
        return True  # capacity immediately available

    throttle = MagicMock()
    throttle.wait_for_capacity.side_effect = fake_wait
    throttle.resume_below = 900
    throttle.poll_secs = 300
    throttle.category_size.return_value = 850

    def fake_subprocess_run(argv, **kwargs):
        # Every item completes: the real uploader would touch the
        # sidecar only to merge re-deferrals back in, so do nothing.
        return MagicMock(returncode=0)

    with (
        patch("tools.drain_deferred._acquire_host_lock", return_value=lock_fd),
        patch(
            "tools.drain_deferred.subprocess.run", side_effect=fake_subprocess_run
        ) as sp,
        patch(
            "tools.drain_deferred.DuplicateCategoryThrottle",
            return_value=throttle,
        ),
        patch("tools.drain_deferred.notify_drain_phase_start") as start_ping,
        patch("tools.drain_deferred.notify_drain_phase_complete") as done_ping,
    ):
        result = CliRunner().invoke(drain_deferred.main, ["nara"])
    assert result.exit_code == 0, result.output
    # Two subprocess calls per round: uploader + sdc-sync.
    assert sp.call_count == 2
    assert sp.call_args_list[0].args[0][0] == "uploader"
    assert sp.call_args_list[1].args[0][0] == "sdc-sync"
    # Drain phase pings.
    start_ping.assert_called_once()
    args, _ = start_ping.call_args
    assert args[0] == "nara"
    assert args[1] == 1  # deferred_count
    assert args[2] == 850  # category_size
    done_ping.assert_called_once()


def test_drain_loop_re_reads_sidecar_between_rounds(lock_fd):
    """If a round processes N items but a later round finds new items
    in the sidecar (e.g., a concurrent uploader session appended
    while we were mid-round), the loop picks them up rather than
    exiting on the pre-round snapshot.
    """
    drain_sidecar.write_sidecar("nara", ["id-1"])

    throttle = MagicMock()
    throttle.wait_for_capacity.return_value = True
    throttle.resume_below = 900
    throttle.poll_secs = 300
    throttle.category_size.return_value = 850

    call_state = {"round": 0}

    def fake_subprocess_run(argv, **kwargs):
        if argv[0] == "uploader":
            call_state["round"] += 1
            if call_state["round"] == 1:
                # Round 1: id-1 completes (the drain already removed it
                # from the sidecar); a concurrent uploader session
                # appends id-2 while we're mid-round.
                drain_sidecar.merge_sidecar("nara", ["id-2"])
            # Round 2: id-2 completes — nothing merged back.
        return MagicMock(returncode=0)

    with (
        patch("tools.drain_deferred._acquire_host_lock", return_value=lock_fd),
        patch(
            "tools.drain_deferred.subprocess.run", side_effect=fake_subprocess_run
        ) as sp,
        patch(
            "tools.drain_deferred.DuplicateCategoryThrottle",
            return_value=throttle,
        ),
        patch("tools.drain_deferred.notify_drain_phase_start"),
        patch("tools.drain_deferred.notify_drain_phase_complete"),
    ):
        result = CliRunner().invoke(drain_deferred.main, ["nara"])
    assert result.exit_code == 0, result.output
    # Two rounds → 4 subprocess calls (2× uploader + 2× sdc-sync).
    assert sp.call_count == 4


def test_drain_loop_continues_on_no_progress_round(lock_fd):
    """A round that made no progress despite reported capacity (e.g.
    category refilled while we were mid-round) is not fatal — the
    loop continues waiting rather than aborting.
    """
    drain_sidecar.write_sidecar("nara", ["id-1"])

    throttle = MagicMock()
    throttle.wait_for_capacity.return_value = True
    throttle.resume_below = 900
    throttle.poll_secs = 300
    throttle.category_size.return_value = 850

    call_state = {"round": 0}

    def fake_subprocess_run(argv, **kwargs):
        if argv[0] == "uploader":
            call_state["round"] += 1
            if call_state["round"] < 2:
                # Round 1: no progress — the uploader re-defers id-1,
                # merging it back into the sidecar the drain cleared.
                drain_sidecar.merge_sidecar("nara", ["id-1"])
            # Round 2: id-1 completes — nothing merged back.
        return MagicMock(returncode=0)

    with (
        patch("tools.drain_deferred._acquire_host_lock", return_value=lock_fd),
        patch("tools.drain_deferred.subprocess.run", side_effect=fake_subprocess_run),
        patch(
            "tools.drain_deferred.DuplicateCategoryThrottle",
            return_value=throttle,
        ),
        patch("tools.drain_deferred.notify_drain_phase_start"),
        patch("tools.drain_deferred.notify_drain_phase_complete") as done_ping,
    ):
        result = CliRunner().invoke(drain_deferred.main, ["nara"])
    assert result.exit_code == 0, result.output
    done_ping.assert_called_once()
    # Two rounds ran (no-progress round did NOT abort).
    assert call_state["round"] == 2


def test_drain_removes_round_ids_from_sidecar_before_invoking_uploader(lock_fd):
    """The drain — not the uploader — must clear the round's IDs from
    the sidecar before the subprocess pass. The uploader only ever
    *merges* deferred IDs back in (it never removes completed ones), so
    if the drain left the round's IDs in place, completed items would
    replay every round and the loop would never terminate. Pin the
    ordering by observing the sidecar from inside the uploader
    subprocess: the round's IDs must already be gone."""
    drain_sidecar.write_sidecar("nara", ["id-1", "id-2"])

    throttle = MagicMock()
    throttle.wait_for_capacity.return_value = True
    throttle.resume_below = 900
    throttle.poll_secs = 300
    throttle.category_size.return_value = 850

    seen_by_uploader: list[list[str]] = []

    def fake_subprocess_run(argv, **kwargs):
        if argv[0] == "uploader":
            seen_by_uploader.append(drain_sidecar.read_sidecar("nara"))
        return MagicMock(returncode=0)

    with (
        patch("tools.drain_deferred._acquire_host_lock", return_value=lock_fd),
        patch("tools.drain_deferred.subprocess.run", side_effect=fake_subprocess_run),
        patch(
            "tools.drain_deferred.DuplicateCategoryThrottle",
            return_value=throttle,
        ),
        patch("tools.drain_deferred.notify_drain_phase_start"),
        patch("tools.drain_deferred.notify_drain_phase_complete"),
    ):
        result = CliRunner().invoke(drain_deferred.main, ["nara"])
    assert result.exit_code == 0, result.output
    # Exactly one round ran, and the uploader saw an already-cleared
    # sidecar — with a merge-only uploader this is what lets completed
    # items leave the queue.
    assert seen_by_uploader == [[]]


# ---------------------------------------------------------------------------
# ``--no-wait`` opportunistic mode: single best-effort round per invocation.
# Runs inside each target's chain (per-target opportunistic phase) so a
# partner whose Category:Duplicate cleared mid-batch gets its deferrals
# actioned before subsequent targets start. The batch-terminal patient
# drain (invoked WITHOUT ``--no-wait``, per unique partner, after every
# target's chain) still handles anything the opportunistic pass left.
# ---------------------------------------------------------------------------


def test_no_wait_exits_immediately_when_category_at_capacity(lock_fd):
    """When Category:Duplicate is at capacity, the opportunistic pass
    must exit WITHOUT running any uploader/sdc-sync subprocess and
    WITHOUT emitting the patient-mode Slack notifications — that
    milestone is the terminal drain's, not this bonus pass."""
    drain_sidecar.write_sidecar("nara", ["id-1"])

    throttle = MagicMock()
    # ``wait_for_capacity(0)`` returns False when the category is at
    # or above threshold — mirrors ``DuplicateCategoryThrottle`` real
    # behavior when the deadline is already past on first check.
    throttle.wait_for_capacity.return_value = False
    throttle.resume_below = 900
    throttle.poll_secs = 300

    with (
        patch("tools.drain_deferred._acquire_host_lock", return_value=lock_fd),
        patch("tools.drain_deferred.subprocess.run") as sp,
        patch(
            "tools.drain_deferred.DuplicateCategoryThrottle",
            return_value=throttle,
        ),
        patch("tools.drain_deferred.notify_drain_phase_start") as start_ping,
        patch("tools.drain_deferred.notify_drain_phase_complete") as done_ping,
    ):
        result = CliRunner().invoke(drain_deferred.main, ["--no-wait", "nara"])

    assert result.exit_code == 0, result.output
    sp.assert_not_called()
    # Slack pings are patient-mode milestones; opportunistic passes
    # are silent so a run with no capacity doesn't spam #tech-alerts.
    start_ping.assert_not_called()
    done_ping.assert_not_called()
    # Sidecar left intact for the terminal drain.
    assert drain_sidecar.read_sidecar("nara") == ["id-1"]


def test_no_wait_drains_single_round_when_under_capacity(lock_fd):
    """When Category:Duplicate is under capacity, the opportunistic
    pass DOES run one round — same clear-then-invoke pattern as the
    patient loop — then exits without waiting for another round.
    Contrast with patient mode, which loops until the sidecar is
    empty (potentially many rounds)."""
    drain_sidecar.write_sidecar("nara", ["id-1", "id-2"])

    throttle = MagicMock()
    throttle.wait_for_capacity.return_value = True  # capacity available now
    throttle.resume_below = 900
    throttle.poll_secs = 300

    def fake_subprocess_run(argv, **kwargs):
        return MagicMock(returncode=0)

    with (
        patch("tools.drain_deferred._acquire_host_lock", return_value=lock_fd),
        patch(
            "tools.drain_deferred.subprocess.run", side_effect=fake_subprocess_run
        ) as sp,
        patch(
            "tools.drain_deferred.DuplicateCategoryThrottle",
            return_value=throttle,
        ),
        patch("tools.drain_deferred.notify_drain_phase_start") as start_ping,
        patch("tools.drain_deferred.notify_drain_phase_complete") as done_ping,
    ):
        result = CliRunner().invoke(drain_deferred.main, ["--no-wait", "nara"])

    assert result.exit_code == 0, result.output
    # Exactly two subprocess calls: one uploader + one sdc-sync for
    # the single round. Contrast with patient mode which could loop.
    assert sp.call_count == 2
    assert sp.call_args_list[0].args[0][0] == "uploader"
    assert sp.call_args_list[1].args[0][0] == "sdc-sync"
    # No patient-mode milestones — opportunistic passes are silent.
    start_ping.assert_not_called()
    done_ping.assert_not_called()


def test_no_wait_empty_sidecar_early_exits(lock_fd):
    """``--no-wait`` on an empty sidecar is the same no-op as patient
    mode — no lock, no throttle, no subprocess."""
    lock_mock = MagicMock()

    with (
        patch("tools.drain_deferred._acquire_host_lock", lock_mock),
        patch("tools.drain_deferred.subprocess.run") as sp,
        patch(
            "tools.drain_deferred.DuplicateCategoryThrottle",
        ) as throttle_ctor,
    ):
        result = CliRunner().invoke(drain_deferred.main, ["--no-wait", "nara"])

    assert result.exit_code == 0, result.output
    lock_mock.assert_not_called()
    sp.assert_not_called()
    throttle_ctor.assert_not_called()


def test_acquire_host_lock_nonblocking_skips_when_held():
    """The opportunistic pass acquires the host lock NON-BLOCKING: if another
    drain already holds it, ``_acquire_host_lock(blocking=False)`` returns None
    (so the pass skips) instead of blocking behind a patient drain that can
    hold the lock for hours. Regression for the drain-lock-blocking bug."""

    # Hold the lock via an independent fd (separate open file description, so
    # flock genuinely conflicts even within this process).
    held = os.open(drain_deferred._DRAIN_LOCK_PATH, os.O_RDWR | os.O_CREAT, 0o644)
    fcntl.flock(held, fcntl.LOCK_EX)
    try:
        assert drain_deferred._acquire_host_lock(blocking=False) is None
    finally:
        fcntl.flock(held, fcntl.LOCK_UN)
        os.close(held)
    # Lock now free → non-blocking acquire succeeds and returns an fd.
    fd = drain_deferred._acquire_host_lock(blocking=False)
    assert fd is not None
    os.close(fd)


def test_no_wait_skips_category_when_host_lock_held_but_still_attempts_await():
    """Opportunistic pass, host lock held by a peer: the category round
    is skipped (the holder covers it) but the await round is still
    attempted — await work is not gated behind category work / the host
    lock. (The await round then also skips if it can't get the lock; the
    point is that main() TRIES it.)"""
    drain_sidecar.write_sidecar("nara", ["id-1"])
    with (
        patch("tools.drain_deferred._acquire_host_lock", return_value=None),
        patch("tools.drain_deferred._drain_opportunistic_once") as opp,
        patch("tools.drain_deferred._run_await_round") as await_round,
        patch("tools.drain_deferred.DuplicateCategoryThrottle"),
        patch("tools.drain_deferred.subprocess.run") as sp,
    ):
        result = CliRunner().invoke(drain_deferred.main, ["--no-wait", "nara"])
    assert result.exit_code == 0, result.output
    opp.assert_not_called()  # category round skipped (lock held)
    await_round.assert_called_once_with("nara", blocking=False)  # still attempted
    sp.assert_not_called()
    assert drain_sidecar.read_sidecar("nara") == ["id-1"]


def test_no_wait_runs_one_await_round(lock_fd):
    """Opportunistic pass with await work and a free lock: exactly one
    await round runs (re-invoke uploader + sdc-sync on the awaiting IDs),
    no patient loop."""
    await_target_free_sidecar.add_key("nara", "id-1", 1)

    def clear_on_rerun(partner, ids):
        for dpla_id in ids:
            await_target_free_sidecar.remove_key(partner, dpla_id, 1)

    with (
        patch("tools.drain_deferred._acquire_host_lock", return_value=lock_fd),
        patch(
            "tools.drain_deferred._run_deferred_items", side_effect=clear_on_rerun
        ) as run,
        patch("tools.drain_deferred.DuplicateCategoryThrottle"),
    ):
        result = CliRunner().invoke(drain_deferred.main, ["--no-wait", "nara"])
    assert result.exit_code == 0, result.output
    run.assert_called_once()
    assert run.call_args.args[1] == ["id-1"]
    assert await_target_free_sidecar.awaiting_dpla_ids("nara") == []


def test_await_round_skips_when_host_lock_held(override_root):
    """_run_await_round returns False without running the uploader when
    the (non-blocking) host lock is held by another drain."""
    await_target_free_sidecar.add_key("nara", "id-1", 1)
    with (
        patch("tools.drain_deferred._acquire_host_lock", return_value=None),
        patch("tools.drain_deferred._run_deferred_items") as run,
    ):
        ran = drain_deferred._run_await_round("nara", blocking=False)
    assert ran is False
    run.assert_not_called()
    # Set untouched, left for the terminal drain.
    assert await_target_free_sidecar.awaiting_dpla_ids("nara") == ["id-1"]


def test_await_round_reruns_uploader_on_unique_dpla_ids(override_root, lock_fd):
    """_run_await_round dedupes per-ordinal keys to unique DPLA IDs (the
    uploader's unit of work is the item) and re-runs the uploader on
    them under the host lock."""
    await_target_free_sidecar.add_key("nara", "id-1", 1)
    await_target_free_sidecar.add_key("nara", "id-1", 2)  # same item, 2nd ordinal
    await_target_free_sidecar.add_key("nara", "id-2", 1)
    with (
        patch("tools.drain_deferred._acquire_host_lock", return_value=lock_fd),
        patch("tools.drain_deferred._run_deferred_items") as run,
    ):
        ran = drain_deferred._run_await_round("nara", blocking=True)
    assert ran is True
    run.assert_called_once()
    assert run.call_args.args[1] == ["id-1", "id-2"]


def test_patient_drains_await_when_uploader_rerun_clears_keys(lock_fd, monkeypatch):
    """Patient mode, await-only: re-running the uploader resolves the
    awaiting items (the uploader clears the keys — e.g. an admin freed
    the canonical and the empty-canonical Case-3 move promoted the
    community file). The loop terminates once the set empties."""
    await_target_free_sidecar.add_key("nara", "id-1", 1)
    await_target_free_sidecar.add_key("nara", "id-2", 1)

    def clear_on_rerun(partner, ids):
        # Simulate the uploader resolving every awaiting item this round.
        for dpla_id in ids:
            await_target_free_sidecar.remove_key(partner, dpla_id, 1)

    throttle = MagicMock()
    throttle.poll_secs = 0

    with (
        patch("tools.drain_deferred._acquire_host_lock", return_value=lock_fd),
        patch(
            "tools.drain_deferred._run_deferred_items", side_effect=clear_on_rerun
        ) as run,
        patch("tools.drain_deferred.DuplicateCategoryThrottle", return_value=throttle),
        patch("tools.drain_deferred.notify_drain_phase_start") as start_ping,
        patch("tools.drain_deferred.notify_drain_phase_complete") as done_ping,
    ):
        result = CliRunner().invoke(drain_deferred.main, ["nara"])
    assert result.exit_code == 0, result.output
    run.assert_called_once()  # one round cleared both items
    assert await_target_free_sidecar.awaiting_dpla_ids("nara") == []
    # Await-only drain (no category work): start/complete pings are for
    # category work, so neither fires here.
    start_ping.assert_not_called()
    done_ping.assert_not_called()


class _StopLoop(Exception):
    pass


def test_patient_await_loop_waits_and_does_not_falsely_complete(lock_fd, monkeypatch):
    """When the awaiting items are NOT resolved by a re-run (admin hasn't
    acted), the patient loop keeps waiting — it must NOT signal
    completion while items remain. We break the otherwise-unbounded loop
    on the first sleep and assert no completion ping fired."""
    await_target_free_sidecar.add_key("nara", "id-1", 1)

    throttle = MagicMock()
    throttle.poll_secs = 0

    def boom(*_a, **_k):
        raise _StopLoop

    with (
        patch("tools.drain_deferred._acquire_host_lock", return_value=lock_fd),
        patch("tools.drain_deferred._run_deferred_items"),  # no-op: key remains
        patch("tools.drain_deferred.DuplicateCategoryThrottle", return_value=throttle),
        patch("tools.drain_deferred.notify_drain_phase_complete") as done_ping,
        patch("tools.drain_deferred.time.sleep", side_effect=boom),
    ):
        result = CliRunner().invoke(drain_deferred.main, ["nara"])
    # The loop kept waiting (hit sleep) rather than completing.
    assert isinstance(result.exception, _StopLoop)
    done_ping.assert_not_called()
    assert await_target_free_sidecar.awaiting_dpla_ids("nara") == ["id-1"]


def test_patient_category_then_await_both_drain_and_complete(lock_fd, monkeypatch):
    """Patient mode with BOTH queues: category drains first (under the
    host lock, start ping fires), then await drains; completion pings
    once both are empty."""
    drain_sidecar.write_sidecar("nara", ["cat-1"])
    await_target_free_sidecar.add_key("nara", "await-1", 1)

    throttle = MagicMock()
    throttle.poll_secs = 0
    throttle.resume_below = 900
    throttle.category_size.return_value = 100
    throttle.wait_for_capacity.return_value = True

    def rerun(partner, ids):
        # Category round: the ids came from drain_sidecar (already removed
        # by _run_one_round before calling us) — nothing to re-defer.
        # Await round: clear the awaiting key.
        for dpla_id in ids:
            await_target_free_sidecar.remove_key(partner, dpla_id, 1)

    # main() acquires the host lock once for the category phase and again
    # per await round, closing each fd — so hand out a FRESH fd per call
    # (the real _acquire_host_lock opens a new fd each time).
    def fresh_lock(**_kw):
        return os.open(os.devnull, os.O_RDONLY)

    with (
        patch("tools.drain_deferred._acquire_host_lock", side_effect=fresh_lock),
        patch("tools.drain_deferred._run_deferred_items", side_effect=rerun),
        patch("tools.drain_deferred.DuplicateCategoryThrottle", return_value=throttle),
        patch("tools.drain_deferred.notify_drain_phase_start") as start_ping,
        patch("tools.drain_deferred.notify_drain_phase_complete") as done_ping,
    ):
        result = CliRunner().invoke(drain_deferred.main, ["nara"])
    assert result.exit_code == 0, result.output
    start_ping.assert_called_once()
    done_ping.assert_called_once()
    assert drain_sidecar.read_sidecar("nara") == []
    assert await_target_free_sidecar.awaiting_dpla_ids("nara") == []
