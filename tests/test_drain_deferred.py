"""Tests for the ``drain-deferred`` command (``tools.drain_deferred``)."""

from __future__ import annotations

import contextlib
import os
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from ingest_wikimedia import drain_sidecar
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
