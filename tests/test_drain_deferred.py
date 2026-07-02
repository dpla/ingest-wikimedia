"""Tests for the ``drain-deferred`` command (``tools.drain_deferred``)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from ingest_wikimedia import drain_sidecar
from tools import drain_deferred


@pytest.fixture(autouse=True)
def chdir_tmp(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.fixture(autouse=True)
def stub_host_lock(monkeypatch, tmp_path):
    """Redirect the host-level lock file into ``tmp_path`` so tests
    don't clobber (or block on) the real ``/home/ec2-user/...`` lock."""
    monkeypatch.setattr(
        drain_deferred, "_DRAIN_LOCK_PATH", str(tmp_path / ".drain-lock")
    )


@pytest.fixture(autouse=True)
def stub_setup_logging(monkeypatch):
    """The real setup_logging writes to <partner>/logs/... and expects
    a partner dir; tests just need it to be a no-op."""
    monkeypatch.setattr(drain_deferred, "setup_logging", lambda *a, **kw: None)


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


def test_populated_sidecar_runs_drain_loop_until_empty(monkeypatch):
    """Non-empty sidecar: acquire host lock, poll category, run
    uploader + sdc-sync subprocesses, and loop until the sidecar is
    empty. The subprocess side-effect removes the ID from the sidecar
    (as the real uploader would after a successful re-run).
    """
    drain_sidecar.write_sidecar("nara", ["id-1"])

    lock_fd = MagicMock()

    def fake_wait(*a, **kw):
        return True  # capacity immediately available

    throttle = MagicMock()
    throttle.wait_for_capacity.side_effect = fake_wait
    throttle.resume_below = 900
    throttle.poll_secs = 300
    throttle._category_size.return_value = 850

    def fake_subprocess_run(argv, **kwargs):
        # After the first (uploader) subprocess, the deferred item is
        # "processed". Empty the sidecar so the loop exits after this
        # round.
        if argv[0] == "uploader":
            drain_sidecar.write_sidecar("nara", [])
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


def test_drain_loop_re_reads_sidecar_between_rounds():
    """If a round processes N items but a later round finds new items
    in the sidecar (e.g., a concurrent uploader session appended
    while we were mid-round), the loop picks them up rather than
    exiting on the pre-round snapshot.
    """
    drain_sidecar.write_sidecar("nara", ["id-1"])
    lock_fd = MagicMock()

    throttle = MagicMock()
    throttle.wait_for_capacity.return_value = True
    throttle.resume_below = 900
    throttle.poll_secs = 300
    throttle._category_size.return_value = 850

    call_state = {"round": 0}

    def fake_subprocess_run(argv, **kwargs):
        if argv[0] == "uploader":
            call_state["round"] += 1
            if call_state["round"] == 1:
                # First round clears id-1 but appends id-2 (concurrent).
                drain_sidecar.write_sidecar("nara", ["id-2"])
            else:
                drain_sidecar.write_sidecar("nara", [])
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


def test_drain_loop_continues_on_no_progress_round():
    """A round that made no progress despite reported capacity (e.g.
    category refilled while we were mid-round) is not fatal — the
    loop continues waiting rather than aborting.
    """
    drain_sidecar.write_sidecar("nara", ["id-1"])
    lock_fd = MagicMock()

    throttle = MagicMock()
    throttle.wait_for_capacity.return_value = True
    throttle.resume_below = 900
    throttle.poll_secs = 300
    throttle._category_size.return_value = 850

    call_state = {"round": 0}

    def fake_subprocess_run(argv, **kwargs):
        if argv[0] == "uploader":
            call_state["round"] += 1
            if call_state["round"] < 2:
                # Round 1: no progress (id-1 still in sidecar).
                return MagicMock(returncode=0)
            # Round 2: cleared.
            drain_sidecar.write_sidecar("nara", [])
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
