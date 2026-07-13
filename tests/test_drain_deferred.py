"""Tests for the ``drain-deferred`` command (``tools.drain_deferred``)."""

from __future__ import annotations

import contextlib
import fcntl
import os
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

import pywikibot

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
    import fcntl

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


def test_no_wait_skips_when_host_lock_held():
    """When another drain holds the host lock, the opportunistic pass must skip
    immediately — no throttle, no subprocesses — leaving the sidecar for the
    terminal drain. Regression: the blocking acquire kept finished sessions
    open for hours behind a patient drain."""
    drain_sidecar.write_sidecar("nara", ["id-1"])
    with (
        patch(
            "tools.drain_deferred._acquire_host_lock", return_value=None
        ) as lock_mock,
        patch("tools.drain_deferred._drain_opportunistic_once") as opp,
        patch("tools.drain_deferred.DuplicateCategoryThrottle") as throttle_ctor,
        patch("tools.drain_deferred.subprocess.run") as sp,
    ):
        result = CliRunner().invoke(drain_deferred.main, ["--no-wait", "nara"])
    assert result.exit_code == 0, result.output
    lock_mock.assert_called_once_with(blocking=False)
    opp.assert_not_called()
    throttle_ctor.assert_not_called()
    sp.assert_not_called()
    assert drain_sidecar.read_sidecar("nara") == ["id-1"]


# ---------------------------------------------------------------------------
# await-target-free stage-2 handler
# ---------------------------------------------------------------------------


def _await_entry(**overrides) -> dict:
    """Sidecar entry factory for stage-2 tests. Default is the fully
    queued state (``tag_emitted=True``) — the phase drain is designed
    to advance from; tests exercising the in-progress phase override
    to False.
    """
    base = {
        "dpla_id": "22412cd0",
        "ordinal": 1,
        "tagged_title": "Angus - DPLA - 22412cd0.jpg",
        "community_title": "Angus.jpg",
        "tag_emitted": True,
        "expected_sha1": "9719e05ab718aac6d400b239792ceeb45a766954",
    }
    base.update(overrides)
    return base


def _tagged_page(*, exists=True, is_redirect=False, text="") -> MagicMock:
    """MagicMock ``pywikibot.FilePage`` for the drift-target (our tagged
    DPLA-canonical file). Callers customise via kwargs."""
    p = MagicMock()
    p.exists.return_value = exists
    p.isRedirectPage.return_value = is_redirect
    type(p).text = property(lambda self: text)
    p.title.return_value = "File:Angus - DPLA - 22412cd0.jpg"
    return p


def _community_page(
    *,
    exists=True,
    is_redirect=False,
    sha1="9719e05ab718aac6d400b239792ceeb45a766954",
) -> MagicMock:
    """MagicMock for the community file — pre-loaded with a valid sha1."""
    p = MagicMock()
    p.exists.return_value = exists
    p.isRedirectPage.return_value = is_redirect
    p.latest_file_info.sha1 = sha1
    p.title.return_value = "File:Angus.jpg"
    return p


def test_advance_pending_when_tag_still_present():
    """State machine: tagged file exists, {{Duplicate}} template still
    on the page → keep entry queued for the next drain round."""
    site = MagicMock()
    tagged = _tagged_page(
        exists=True,
        is_redirect=False,
        text="{{Duplicate|Angus.jpg}}\n== filedesc ==\n",
    )
    with patch("tools.drain_deferred.get_page", return_value=tagged):
        should_remove, note = drain_deferred._advance_await_target_free_entry(
            _await_entry(), site
        )
    assert should_remove is False
    assert note is not None and note.startswith("PENDING:")


def test_advance_fail_when_tag_removed_but_page_exists():
    """An editor decline: tag stripped but page kept. Drop entry and
    log FAIL — the community-file rename can't proceed against a page
    the community affirmatively kept."""
    site = MagicMock()
    tagged = _tagged_page(
        exists=True,
        is_redirect=False,
        text="== filedesc ==\n{{DPLA metadata}}\n",  # no {{Duplicate}}
    )
    with patch("tools.drain_deferred.get_page", return_value=tagged):
        should_remove, note = drain_deferred._advance_await_target_free_entry(
            _await_entry(), site
        )
    assert should_remove is True
    assert note is not None and note.startswith("FAIL:")
    assert "Duplicate" in note


def test_advance_executes_move_when_tagged_file_deleted():
    """Deletion (page no longer exists on Commons) → execute move of
    community file into the freed canonical title, post the
    CommonsDelinker relink."""
    site = MagicMock()
    tagged = _tagged_page(exists=False, is_redirect=False)
    community = _community_page(exists=True)
    with (
        patch(
            "tools.drain_deferred.get_page",
            side_effect=lambda _site, title: (
                tagged if "22412cd0" in title else community
            ),
        ),
        patch("tools.drain_deferred.file_has_inbound_usage", return_value=True),
        patch("tools.drain_deferred.post_commonsdelinker_request") as delinker,
        patch(
            "tools.drain_deferred.with_csrf_recovery",
            side_effect=lambda _s, _l, fn: fn(),
        ),
        patch(
            "tools.drain_deferred.build_title_drift_move_reason",
            return_value="reason",
        ),
    ):
        should_remove, note = drain_deferred._advance_await_target_free_entry(
            _await_entry(), site
        )
    assert should_remove is True
    assert note is None
    community.move.assert_called_once()
    delinker.assert_called_once()


def test_advance_executes_move_when_tagged_file_became_redirect():
    """Redirect (admin merged rather than deleting) is a first-class
    advance signal — move-onto-a-redirect is a normal pywikibot
    operation, same as move-into-empty-slot. Per the design, this is
    NOT an edge case; admins commonly redirect."""
    site = MagicMock()
    tagged = _tagged_page(exists=True, is_redirect=True)
    community = _community_page(exists=True)
    with (
        patch(
            "tools.drain_deferred.get_page",
            side_effect=lambda _site, title: (
                tagged if "22412cd0" in title else community
            ),
        ),
        patch("tools.drain_deferred.file_has_inbound_usage", return_value=False),
        patch("tools.drain_deferred.post_commonsdelinker_request"),
        patch(
            "tools.drain_deferred.with_csrf_recovery",
            side_effect=lambda _s, _l, fn: fn(),
        ),
        patch(
            "tools.drain_deferred.build_title_drift_move_reason",
            return_value="reason",
        ),
    ):
        should_remove, note = drain_deferred._advance_await_target_free_entry(
            _await_entry(), site
        )
    assert should_remove is True
    assert note is None
    community.move.assert_called_once()


def test_advance_fail_when_community_file_missing():
    """Third-party action removed the community file during the wait
    window — nothing to move. Drop entry with FAIL."""
    site = MagicMock()
    tagged = _tagged_page(exists=False)
    community = _community_page(exists=False)
    with patch(
        "tools.drain_deferred.get_page",
        side_effect=lambda _site, title: tagged if "22412cd0" in title else community,
    ):
        should_remove, note = drain_deferred._advance_await_target_free_entry(
            _await_entry(), site
        )
    assert should_remove is True
    assert note is not None and note.startswith("FAIL:")
    assert "community file" in note


def test_advance_fail_when_move_blocked_by_article_exists_conflict():
    """A stale redirect or page history at the tagged title makes the
    move raise ``ArticleExistsConflictError``. The invariant is already
    satisfied via the tagged title's current state, and retrying every
    poll would spam the API. Drop the entry with a decisive FAIL."""
    site = MagicMock()
    tagged = _tagged_page(exists=True, is_redirect=True)
    community = _community_page(exists=True)

    def raise_conflict(_title, **_kwargs):
        raise pywikibot.exceptions.ArticleExistsConflictError("blocked")

    community.move.side_effect = raise_conflict
    with (
        patch(
            "tools.drain_deferred.get_page",
            side_effect=lambda _site, title: (
                tagged if "22412cd0" in title else community
            ),
        ),
        patch("tools.drain_deferred.file_has_inbound_usage", return_value=False),
        patch("tools.drain_deferred.post_commonsdelinker_request") as relink,
        patch(
            "tools.drain_deferred.with_csrf_recovery",
            side_effect=lambda _s, _l, fn: fn(),
        ),
        patch(
            "tools.drain_deferred.build_title_drift_move_reason",
            return_value="reason",
        ),
    ):
        should_remove, note = drain_deferred._advance_await_target_free_entry(
            _await_entry(), site
        )
    assert should_remove is True
    assert note is not None and note.startswith("FAIL:")
    assert "ArticleExistsConflictError" in note
    relink.assert_not_called()


def test_advance_pending_when_tag_emitted_is_false():
    """Workflow phase gate: an entry recorded by the uploader but whose
    tag_as_duplicate call never completed carries ``tag_emitted=False``.
    Drain MUST NOT treat the missing tag as an editor-decline and drop
    the entry — that would strand community history. Return PENDING so
    the next uploader run retries the tag and flips the phase.
    """
    site = MagicMock()
    # No page fetches should happen when we short-circuit on the phase gate.
    with patch("tools.drain_deferred.get_page") as get_page:
        should_remove, note = drain_deferred._advance_await_target_free_entry(
            _await_entry(tag_emitted=False), site
        )
    assert should_remove is False
    assert note is not None and note.startswith("PENDING:")
    assert "tag_emitted=False" in note
    # No Commons state was consulted; the gate lives above every fetch.
    get_page.assert_not_called()


def test_advance_fail_when_community_page_is_redirect():
    """An admin (or an editor) redirected the community file during the
    wait window — most likely, redirected it to our tagged canonical.
    The invariant is satisfied either way (our canonical holds the S3
    SHA1); there is no move to perform. Drop the entry as a decisive
    FAIL rather than trying to move a redirect page around.
    """
    site = MagicMock()
    tagged = _tagged_page(exists=False)
    community = _community_page(exists=True, is_redirect=True)
    with patch(
        "tools.drain_deferred.get_page",
        side_effect=lambda _site, title: tagged if "22412cd0" in title else community,
    ):
        should_remove, note = drain_deferred._advance_await_target_free_entry(
            _await_entry(), site
        )
    assert should_remove is True
    assert note is not None and note.startswith("FAIL:")
    assert "redirected" in note
    # The move must not have been attempted.
    community.move.assert_not_called()


def test_advance_fail_when_community_sha1_drifted():
    """Content drift on the community side during the wait window —
    the file no longer holds the S3 SHA1 we saw at tag time. Refuse
    to move stale bytes into the canonical title."""
    site = MagicMock()
    tagged = _tagged_page(exists=False)
    community = _community_page(
        exists=True, sha1="ffffffffffffffffffffffffffffffffffffffff"
    )
    with patch(
        "tools.drain_deferred.get_page",
        side_effect=lambda _site, title: tagged if "22412cd0" in title else community,
    ):
        should_remove, note = drain_deferred._advance_await_target_free_entry(
            _await_entry(), site
        )
    assert should_remove is True
    assert note is not None and note.startswith("FAIL:")
    assert "SHA1 drifted" in note


def test_process_await_target_free_advances_and_removes(override_root):
    """End-to-end: an entry that advances is removed from the sidecar."""
    await_target_free_sidecar.write_sidecar("nara", [_await_entry()])
    with patch(
        "tools.drain_deferred._advance_await_target_free_entry",
        return_value=(True, None),
    ):
        drain_deferred._process_await_target_free("nara", MagicMock())
    assert await_target_free_sidecar.read_sidecar("nara") == []


def test_process_await_target_free_keeps_pending_entries(override_root):
    """Entry that returns PENDING stays queued for the next drain round."""
    await_target_free_sidecar.write_sidecar("nara", [_await_entry()])
    with patch(
        "tools.drain_deferred._advance_await_target_free_entry",
        return_value=(False, "PENDING: tagged file still present"),
    ):
        drain_deferred._process_await_target_free("nara", MagicMock())
    assert len(await_target_free_sidecar.read_sidecar("nara")) == 1


def test_patient_mode_loops_stage2_until_sidecar_empty(lock_fd, monkeypatch):
    """Patient mode must poll stage-2 (await-target-free) until its
    sidecar is empty, sleeping between rounds — mirroring the
    category-capacity loop's semantics. Two entries here; round 1
    advances one and leaves one queued, round 2 advances the last.
    """
    # Seed only the await sidecar; the category-capacity sidecar is empty
    # so the outer drain loop skips (but patient-mode still runs stage 2).
    await_target_free_sidecar.write_sidecar(
        "nara", [_await_entry(dpla_id="aaa"), _await_entry(dpla_id="bbb")]
    )

    throttle = MagicMock()
    throttle.wait_for_capacity.return_value = True
    throttle.resume_below = 900
    throttle.poll_secs = 0  # keep the test fast — no real waiting
    throttle.category_size.return_value = 0

    advance_calls = []

    def fake_advance(entry, _site):
        advance_calls.append(entry["dpla_id"])
        # Round 1: only "aaa" advances; "bbb" stays PENDING.
        # Round 2: "bbb" advances too.
        if entry["dpla_id"] == "bbb" and advance_calls.count("bbb") == 1:
            return (False, "PENDING: waiting")
        return (True, None)

    with (
        patch("tools.drain_deferred._acquire_host_lock", return_value=lock_fd),
        patch(
            "tools.drain_deferred.DuplicateCategoryThrottle",
            return_value=throttle,
        ),
        patch("tools.drain_deferred.notify_drain_phase_start"),
        patch("tools.drain_deferred.notify_drain_phase_complete"),
        patch(
            "tools.drain_deferred._advance_await_target_free_entry",
            side_effect=fake_advance,
        ),
        # Prevent time.sleep from stalling the test (poll_secs=0 already,
        # but be defensive if anything else calls sleep).
        monkeypatch.context() as m,
    ):
        m.setattr("tools.drain_deferred.time.sleep", lambda *_a, **_k: None)
        result = CliRunner().invoke(drain_deferred.main, ["nara"])
    assert result.exit_code == 0, result.output
    # Sidecar drained.
    assert await_target_free_sidecar.read_sidecar("nara") == []
    # bbb was polled twice (once per round); aaa only once.
    assert advance_calls.count("bbb") == 2
    assert advance_calls.count("aaa") == 1


def test_patient_mode_stops_when_only_unemitted_entries_remain(lock_fd, monkeypatch):
    """A ``tag_emitted=False`` entry can only be finished by a future
    uploader run (stage-2 does not emit tags). Patient mode must NOT
    spin forever waiting on admin action that can't apply — if every
    still-pending entry is unemitted, it breaks and leaves them queued.
    """
    # One entry, stuck tag_emitted=False. Real _advance would return
    # PENDING for it (no admin advance possible), so use the real path.
    await_target_free_sidecar.write_sidecar(
        "nara", [_await_entry(dpla_id="stuck", tag_emitted=False)]
    )

    throttle = MagicMock()
    throttle.wait_for_capacity.return_value = True
    throttle.resume_below = 900
    throttle.poll_secs = 0
    throttle.category_size.return_value = 0

    sleep_calls = {"n": 0}

    with (
        patch("tools.drain_deferred._acquire_host_lock", return_value=lock_fd),
        patch("tools.drain_deferred.DuplicateCategoryThrottle", return_value=throttle),
        patch("tools.drain_deferred.notify_drain_phase_start"),
        patch("tools.drain_deferred.notify_drain_phase_complete"),
        patch("tools.drain_deferred.get_page") as get_page,
        monkeypatch.context() as m,
    ):
        # If the loop ever sleeps, it's spinning — fail loudly instead
        # of hanging the suite.
        def _tracked_sleep(*_a, **_k):
            sleep_calls["n"] += 1
            if sleep_calls["n"] > 3:
                raise AssertionError("patient loop is spinning on an unemitted entry")

        m.setattr("tools.drain_deferred.time.sleep", _tracked_sleep)
        result = CliRunner().invoke(drain_deferred.main, ["nara"])
    assert result.exit_code == 0, result.output
    # Phase gate short-circuits before any Commons fetch.
    get_page.assert_not_called()
    # Entry left queued for the next uploader run; loop did not spin.
    remaining = await_target_free_sidecar.read_sidecar("nara")
    assert [e["dpla_id"] for e in remaining] == ["stuck"]
    assert sleep_calls["n"] == 0


def test_run_stage2_once_skips_when_partner_lock_held(override_root):
    """The partner-scoped stage-2 lock serializes same-partner stage-2
    rounds. When a foreign holder has it, ``_run_stage2_once`` must
    non-blocking-skip rather than blocking — the peer round covers our
    work and we shouldn't wait on it.
    """
    await_target_free_sidecar.write_sidecar("nara", [_await_entry()])
    partner_dir = drain_sidecar.partner_dir_path("nara")
    partner_dir.mkdir(parents=True, exist_ok=True)
    lock_path = partner_dir / ".stage2-drain.lock"
    foreign_fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)
    fcntl.flock(foreign_fd, fcntl.LOCK_EX)
    try:
        with patch("tools.drain_deferred._process_await_target_free") as inner:
            drain_deferred._run_stage2_once("nara", MagicMock())
        inner.assert_not_called()
    finally:
        fcntl.flock(foreign_fd, fcntl.LOCK_UN)
        os.close(foreign_fd)


def test_run_stage2_once_acquires_and_releases_the_partner_lock(override_root):
    """Sanity check on the fd lifecycle: after ``_run_stage2_once``
    completes, the partner-scoped lock is released so a peer drain
    can acquire it — a leaked fd would break same-partner
    serialization for the whole process lifetime.
    """
    await_target_free_sidecar.write_sidecar("nara", [_await_entry()])
    with patch("tools.drain_deferred._process_await_target_free"):
        drain_deferred._run_stage2_once("nara", MagicMock())
    # If the lock leaked, a subsequent non-blocking acquire would fail.
    fd = drain_deferred._acquire_stage2_lock("nara", blocking=False)
    assert fd is not None, "stage-2 lock leaked across the call boundary"
    os.close(fd)


def test_process_await_target_free_isolates_per_entry_exceptions(override_root, caplog):
    """A single entry that raises must NOT stop the loop from
    processing the others — same isolation contract as ``_run_one_round``.
    The failing entry stays queued."""
    entries = [_await_entry(dpla_id="bbb"), _await_entry(dpla_id="ccc")]
    await_target_free_sidecar.write_sidecar("nara", entries)

    def side_effect(entry, _site):
        if entry["dpla_id"] == "bbb":
            raise RuntimeError("boom")
        return (True, None)  # ccc advances cleanly

    with patch(
        "tools.drain_deferred._advance_await_target_free_entry",
        side_effect=side_effect,
    ):
        drain_deferred._process_await_target_free("nara", MagicMock())

    remaining_ids = [
        e["dpla_id"] for e in await_target_free_sidecar.read_sidecar("nara")
    ]
    # bbb stayed (raised); ccc removed (advanced).
    assert remaining_ids == ["bbb"]
