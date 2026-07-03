"""Tests for the pure helpers in ingest_wikimedia.slack.

The network-touching paths (`post_message`, `notify_pipeline_fail`'s actual
HTTP call) are not covered here — these tests exercise the decoding /
log-summary logic that produces the message body.
"""

import os
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ingest_wikimedia.slack import (
    _PHASE_EMOJI,
    _decode_exit_code,
    _find_latest_log,
    _read_download_failed_count,
    _summarize_log,
    notify_phase_start,
    notify_pipeline_fail,
    notify_sdc_complete,
    notify_upload_complete,
)
from ingest_wikimedia.tracker import Result, Tracker


def test_notify_phase_start_supports_sdc_sync_phase():
    """Regression: ``sdc-sync`` is a valid Phase value, with a distinct
    emoji, so the SDC phase posts its own "starting" notification to
    Slack the same way get-ids-es / downloader / uploader do.

    Without this, a multi-target run reports `[label] Upload complete …`
    and then goes silent for hours during SDC sync — the operator has
    no Slack signal that the SDC phase actually started.
    """
    assert "sdc-sync" in _PHASE_EMOJI
    assert _PHASE_EMOJI["sdc-sync"] != _PHASE_EMOJI["upload"]

    posted = []

    def _capture(token, text):
        posted.append((token, text))

    with (
        patch.dict(os.environ, {"DPLA_SLACK_BOT_TOKEN": "x"}, clear=False),
        patch("ingest_wikimedia.slack.post_message", side_effect=_capture),
    ):
        notify_phase_start("nara", "sdc-sync")

    assert len(posted) == 1
    _, text = posted[0]
    # Emoji + session label + phase label, mirroring the other phases.
    assert _PHASE_EMOJI["sdc-sync"] in text
    assert "starting sdc-sync" in text
    assert "wikimedia-nara" in text


@pytest.mark.parametrize(
    "rc, expected",
    [
        ("0", ""),
        ("", ""),
        (None, ""),
        ("not-a-number", ""),
        ("1", " (exit 1 — uncaught exception — see traceback)"),
        ("2", " (exit 2 — rejected its arguments)"),
        ("124", " (exit 124 — timed out)"),
        ("137", " (exit 137 — SIGKILL — likely OOM)"),
        ("143", " (exit 143 — SIGTERM)"),
        ("139", " (exit 139 — SIGSEGV)"),
        ("134", " (exit 134 — SIGABRT)"),
        ("130", " (exit 130 — SIGINT (Ctrl-C))"),
        # >128 but not in the named table → generic "signal N" hint
        ("150", " (exit 150 — signal 22)"),
        ("129", " (exit 129 — signal 1)"),
    ],
)
def test_decode_exit_code(rc, expected):
    assert _decode_exit_code(rc) == expected


def test_find_latest_log_picks_most_recent_match(tmp_path):
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    label = "nara+center-for-legislative-archives"

    # Two matching logs at different mtimes plus one non-matching log.
    older = logs_dir / f"20260520-100000-{label}-download.log"
    newer = logs_dir / f"20260522-211856-{label}-upload.log"
    decoy = logs_dir / "20260522-220000-other-label-upload.log"
    for p in (older, newer, decoy):
        p.write_text("x")
    os.utime(older, (time.time() - 3600, time.time() - 3600))
    os.utime(newer, (time.time(), time.time()))

    found = _find_latest_log(str(tmp_path), label)
    assert found == str(newer)


def test_find_latest_log_returns_none_when_no_match(tmp_path):
    (tmp_path / "logs").mkdir()
    assert _find_latest_log(str(tmp_path), "nope") is None


def test_find_latest_log_handles_missing_dir():
    with tempfile.TemporaryDirectory() as t:
        # No logs/ subdir, no matching files.
        assert _find_latest_log(t, "anything") is None
    # Non-existent path.
    assert _find_latest_log("/nonexistent/path", "anything") is None
    # Empty partner_dir.
    assert _find_latest_log("", "anything") is None


def test_summarize_log_counts_markers_and_tails_last_lines(tmp_path):
    log = tmp_path / "test.log"
    lines = [
        "[INFO] Starting upload",
        "[INFO] Uploaded to https://commons.wikimedia.org/wiki/File:Foo.jpg",
        "[INFO] Uploaded to https://commons.wikimedia.org/wiki/File:Bar.jpg",
        "[INFO] Skipping abc 1: Already exists on commons.",
        "[INFO] Skipping abc 2: Already exists on commons.",
        "[INFO] Skipping abc 3: Already exists on commons.",
        "[INFO] Skipping def 1: Bad content type.",
        "[ERROR] Failed: Upload error for xyz",
        "[INFO] Page 100",
    ]
    log.write_text("\n".join(lines) + "\n")

    summary = _summarize_log(str(log), tail_lines=3)
    assert summary is not None
    assert "test.log" in summary
    assert "2 uploaded" in summary
    # All four "Skipping" lines (3x already-exists + 1x bad-content-type).
    assert "4 skipped" in summary
    assert "1 failed" in summary
    # Tail should show the last 3 lines.
    assert "Page 100" in summary
    assert "Failed: Upload error" in summary


def test_summarize_log_missing_file_returns_none(tmp_path):
    assert _summarize_log(str(tmp_path / "nope.log")) is None


def _capture_message(env: dict) -> str:
    """Run notify_pipeline_fail() with `env` and return the posted message."""
    sent = {}

    def fake_post(token, text):
        sent["text"] = text

    with (
        patch.dict(os.environ, env, clear=True),
        patch("ingest_wikimedia.slack.post_message", side_effect=fake_post),
    ):
        notify_pipeline_fail()
    return sent.get("text", "")


def test_drain_deferred_opportunistic_step_name_is_wired_consistently():
    """Three files must agree on the literal ``drain-deferred-opportunistic``
    for the per-target opportunistic drain to route its failure summary
    to the right log file:

      1. ``scripts.wikimedia_launch._wrap_step_with_marker`` sets it as
         the ``WIKIMEDIA_STEP`` value when the command carries
         ``--no-wait``.
      2. ``ingest_wikimedia.slack._PHASE_LOG_SUFFIX`` maps it to a
         log-file suffix that the failure handler tails.
      3. ``tools.drain_deferred.main`` calls ``setup_logging(partner,
         "drain-deferred-opportunistic", …)`` under ``--no-wait``, so
         the log file the failure handler tails is the one the drain
         actually wrote to.

    Any divergence between the three would silently break the failure-
    summary path: Slack would show either the wrong log's tail or no
    tail at all. Pin the three-way agreement so a future rename in
    one place can't slip.
    """
    from scripts.wikimedia_launch import _wrap_step_with_marker
    from ingest_wikimedia.slack import _PHASE_LOG_SUFFIX

    wrapped = _wrap_step_with_marker("drain-deferred --no-wait nara")
    assert "export WIKIMEDIA_STEP=drain-deferred-opportunistic &&" in wrapped, (
        "launcher must emit ``drain-deferred-opportunistic`` as the "
        "WIKIMEDIA_STEP value for the per-target ``--no-wait`` drain"
    )
    assert "drain-deferred-opportunistic" in _PHASE_LOG_SUFFIX, (
        "slack's _PHASE_LOG_SUFFIX map must include the opportunistic "
        "step so the failure handler tails the right log"
    )
    # The log-file suffix drain_deferred.py writes to must match what
    # slack.py's failure handler will search for.
    drain_source = (
        Path(__file__).resolve().parent.parent / "tools" / "drain_deferred.py"
    ).read_text()
    assert '"drain-deferred-opportunistic"' in drain_source, (
        "tools/drain_deferred.py must call setup_logging with the "
        "``drain-deferred-opportunistic`` event_type under --no-wait, "
        "matching what slack.py tails and the launcher emits"
    )


def test_notify_pipeline_fail_says_aborting_this_target_when_not_last():
    """Wording on the not-last branch must unambiguously frame the
    message as a failure — the prior ``skipping to next target`` was
    OK but the new phrasing keeps ``aborting`` in both branches so an
    operator scanning Slack can't misread either as a normal-completion
    state (which the prior ``no further targets in batch`` on the
    last-target branch did suggest)."""
    msg = _capture_message(
        {
            "DPLA_SLACK_BOT_TOKEN": "x",
            "WIKIMEDIA_SESSION_LABEL": "nara+foo",
            "WIKIMEDIA_LAST_EXIT": "1",
        }
    )
    assert "aborting this target" in msg
    assert "batch continues with the next" in msg
    assert "aborting batch" not in msg
    # The OLD wording must be gone — a soft-completion phrasing on a
    # failure notification is what motivated the reword.
    assert "no further targets in batch" not in msg
    assert "skipping to next target" not in msg


def test_notify_pipeline_fail_says_aborting_batch_when_last():
    """On the final target, the failure ends the batch — the wording
    now says so explicitly (rather than the prior ``no further targets
    in batch``, which read like a normal-completion summary)."""
    msg = _capture_message(
        {
            "DPLA_SLACK_BOT_TOKEN": "x",
            "WIKIMEDIA_SESSION_LABEL": "nara+foo",
            "WIKIMEDIA_LAST_EXIT": "137",
            "WIKIMEDIA_TARGET_IS_LAST": "1",
        }
    )
    assert "aborting batch" in msg
    assert "this was the final target" in msg
    assert "aborting this target" not in msg
    # Old wording is gone.
    assert "no further targets in batch" not in msg
    # The OOM hint should still be included.
    assert "SIGKILL" in msg


def test_notify_pipeline_fail_treats_any_value_other_than_1_as_not_last():
    # Defensive: an empty or "0" value should be treated as "not last" so a
    # half-set env doesn't accidentally claim there are no more targets.
    for value in ("", "0", "false", "no"):
        msg = _capture_message(
            {
                "DPLA_SLACK_BOT_TOKEN": "x",
                "WIKIMEDIA_SESSION_LABEL": "x",
                "WIKIMEDIA_TARGET_IS_LAST": value,
            }
        )
        assert "aborting this target" in msg, (
            f"value {value!r} should be treated as not-last"
        )


# ---------------------------------------------------------------------------
# Step-aware failure messages: include WIKIMEDIA_STEP in the header so the
# operator sees WHICH phase failed instead of a bare "pipeline step failed".
# Also pin the new exit-code hints for 1 ("uncaught exception") and 2
# ("rejected its arguments"), since those are the two codes the user's
# Duke maintain run hit and the bare-number message was opaque.
# ---------------------------------------------------------------------------


def test_decode_exit_code_interprets_python_exit_1():
    """Exit 1 ≈ Python uncaught exception. Including the hint makes the
    Slack message actionable — "look for a CRITICAL traceback in the log"
    instead of "the pipeline failed somehow with exit 1"."""
    assert _decode_exit_code("1") == " (exit 1 — uncaught exception — see traceback)"


def test_decode_exit_code_interprets_click_exit_2():
    """Exit 2 ≈ Click usage error (e.g. ``click.BadParameter`` from a
    failed precheck like ``DPLA.check_partner``). Points the operator
    at config/CLI args, never at a code bug."""
    assert _decode_exit_code("2") == " (exit 2 — rejected its arguments)"


def test_notify_pipeline_fail_names_the_failing_step():
    """The failure message header should include the step name when
    ``WIKIMEDIA_STEP`` is set. Pre-change the message just said
    "pipeline step failed" and left the operator to grep four log
    files to find which one."""
    msg = _capture_message(
        {
            "DPLA_SLACK_BOT_TOKEN": "x",
            "WIKIMEDIA_SESSION_LABEL": "georgia+duke-university-library",
            "WIKIMEDIA_LAST_EXIT": "1",
            "WIKIMEDIA_STEP": "id-generation",
        }
    )
    assert "`id-generation` step failed" in msg, msg
    assert "uncaught exception" in msg, msg
    assert "pipeline step failed" not in msg, (
        "step-aware header must replace the generic phrasing"
    )


def test_notify_pipeline_fail_falls_back_to_generic_when_step_unset():
    """A stale launcher (no WIKIMEDIA_STEP export) must not crash — the
    handler should fall back to the legacy "pipeline step failed"
    wording. Pre-step-tracking deployments will still produce this
    while the new launcher rolls out."""
    msg = _capture_message(
        {
            "DPLA_SLACK_BOT_TOKEN": "x",
            "WIKIMEDIA_SESSION_LABEL": "georgia+x",
            "WIKIMEDIA_LAST_EXIT": "1",
        }
    )
    assert "pipeline step failed" in msg, msg


def test_notify_pipeline_fail_id_generation_step_includes_stderr_tail(
    tmp_path, monkeypatch
):
    """For the id-generation step there's no per-phase log file (the
    ``get-ids-es`` CLI doesn't call ``setup_logging``), so the failure
    handler reads stderr-tail directly from the file the launcher
    tees to. This is exactly the digitalnc case from the user's Duke
    run: ``click.BadParameter`` printed to stderr, no log produced,
    Slack would otherwise show only a bare "exit 2"."""
    # Per-session-label path: the file lives under /tmp/ in production
    # but tests redirect it to a tmp_path via monkeypatching the helper
    # itself — so we control the read path without writing to /tmp.
    label = "digitalnc+duke-university-libraries"
    stderr_path = tmp_path / f"wm-id-generation-stderr-{label}.log"
    stderr_path.write_text(
        "Usage: get-ids-es [OPTIONS] PARTNER\n"
        "Try 'get-ids-es --help' for help.\n"
        "\n"
        "Error: Invalid value: Hub 'digitalnc' has no upload-eligible institutions in"
        " institutions_v2.json — edit that file to opt in\n"
    )
    monkeypatch.setattr(
        "ingest_wikimedia.slack.id_generation_stderr_tail_file",
        lambda label_arg: str(tmp_path / f"wm-id-generation-stderr-{label_arg}.log"),
    )
    msg = _capture_message(
        {
            "DPLA_SLACK_BOT_TOKEN": "x",
            "WIKIMEDIA_SESSION_LABEL": label,
            "WIKIMEDIA_LAST_EXIT": "2",
            "WIKIMEDIA_STEP": "id-generation",
        }
    )
    # The header names the failing step + interprets exit 2.
    assert "`id-generation` step failed" in msg
    assert "rejected its arguments" in msg
    # The actual stderr message must be in the body so the operator
    # doesn't have to SSM in to find out WHY the precheck rejected.
    assert "Hub 'digitalnc' has no upload-eligible institutions" in msg


def test_id_generation_stderr_tail_file_is_per_session():
    """The stderr tail path must be per-session-label, not a single
    shared ``/tmp`` file. The EC2 box runs many concurrent tmux
    sessions; a shared path would let two id-generation steps clobber
    each other's stderr and serve the wrong failure body to Slack."""
    from ingest_wikimedia.slack import id_generation_stderr_tail_file

    a = id_generation_stderr_tail_file("georgia+duke-university-library")
    b = id_generation_stderr_tail_file("ia+duke-university-libraries")
    assert a != b, "different labels must produce different paths"
    assert "georgia+duke-university-library" in a
    assert "ia+duke-university-libraries" in b


def test_id_generation_stderr_tail_file_no_label_fallback_matches_bash():
    """The Python read-side and bash write-side must agree on the path
    when ``WIKIMEDIA_SESSION_LABEL`` is unset. The launcher's tee
    redirect uses ``${WIKIMEDIA_SESSION_LABEL:-unknown}``, so the
    helper's empty-label fallback MUST produce the same
    ``…-unknown.log`` suffix — otherwise a partial-env failure would
    have Python looking for a file at a path bash never wrote to.

    Both ``None`` and empty string represent "unset" and must collapse
    to the same fallback path. This is the dead-branch regression CR
    caught on the first pass: previously the helper's empty-string
    branch returned a no-suffix path, agreeing with bash only by
    accident because the caller never actually fed it an empty value.
    """
    from ingest_wikimedia.slack import id_generation_stderr_tail_file

    expected_fallback = "/tmp/wm-id-generation-stderr-unknown.log"
    assert id_generation_stderr_tail_file("") == expected_fallback
    assert id_generation_stderr_tail_file(None) == expected_fallback


def test_notify_pipeline_fail_phase_log_scopes_to_failing_step(tmp_path):
    """When ``WIKIMEDIA_STEP`` is ``upload`` (or ``download`` / ``sdc``),
    the handler must tail the *matching* phase log — not the most
    recent log of any phase. Pre-step-tracking the lookup matched any
    phase (``*``), so a failure during the upload step that came
    after a longer-running download could end up tailing the older
    download log instead. Pin the scope so the failing step's tail
    actually goes to Slack."""
    base = tmp_path
    logs = base / "logs"
    logs.mkdir()
    label = "georgia+x"
    older_upload = logs / f"20260101-100000-{label}-upload.log"
    newer_download = logs / f"20260101-110000-{label}-download.log"
    older_upload.write_text("[INFO] start\n[ERROR] Failed: boom in uploader\n")
    newer_download.write_text("[INFO] downloader ran fine, nothing to flag\n")
    # Force ``older_upload`` to be older than ``newer_download`` so the
    # legacy any-phase lookup would have picked the newer download.
    os.utime(str(older_upload), (time.time() - 7200, time.time() - 7200))

    msg = _capture_message(
        {
            "DPLA_SLACK_BOT_TOKEN": "x",
            "WIKIMEDIA_SESSION_LABEL": label,
            "WIKIMEDIA_PARTNER_DIR": str(base),
            "WIKIMEDIA_LAST_EXIT": "1",
            "WIKIMEDIA_STEP": "upload",
        }
    )
    # The upload-phase log content should be reflected; the unrelated
    # download log should NOT be the tailed file.
    assert "boom in uploader" in msg
    assert "downloader ran fine" not in msg


# ---------------------------------------------------------------------------
# notify_upload_complete: combined retry summary
# ---------------------------------------------------------------------------


def test_read_download_failed_count_parses_counts_section(tmp_path):
    """Reads `FAILED: N` out of a downloader log's terminal COUNTS section."""
    log = tmp_path / "20260524-201558-si-download.log"
    log.write_text(
        "[INFO] 20:15:58: Starting download for si\n"
        "[INFO] 20:46:51: \n"
        "COUNTS:\n"
        "DOWNLOADED: 6937\n"
        "FAILED: 12\n"
        "SKIPPED: 6\n"
        "BYTES: 6785972036\n"
        "\n"
        "[INFO] 20:46:51: 1852.87 seconds.\n"
    )
    assert _read_download_failed_count(str(log)) == 12


def test_read_download_failed_count_none_path_returns_none():
    """Unset env var → None (no combination)."""
    assert _read_download_failed_count(None) is None
    assert _read_download_failed_count("") is None


def test_read_download_failed_count_missing_file_returns_none(tmp_path):
    """Path that doesn't exist → None, no crash."""
    assert _read_download_failed_count(str(tmp_path / "does-not-exist.log")) is None


def test_read_download_failed_count_no_counts_section_returns_none(tmp_path):
    """Log file without a COUNTS section → None (downloader bombed early).

    No usable counts to combine; the upper layer falls back to the
    upload-only header and logs a warning so the missing data is visible.
    """
    log = tmp_path / "partial.log"
    log.write_text("[INFO] 20:15:58: Starting download for si\n")
    assert _read_download_failed_count(str(log)) is None


def test_read_download_failed_count_counts_without_failed_returns_zero(tmp_path):
    """COUNTS section present but no FAILED line → returns 0 (clean run).

    Tracker.__str__ only emits counter lines whose value > 0, so a clean
    download (no failures) writes a COUNTS section with no FAILED line at
    all. Returning 0 here (rather than None) is what keeps the retry
    summary titled "Retry Complete" instead of falling back to
    "Upload Complete" on a clean run.
    """
    log = tmp_path / "20260524-220233-retry-si-download.log"
    log.write_text(
        "[INFO] 22:02:33: Starting download for si\n"
        "[INFO] 22:02:36: \n"
        "COUNTS:\n"
        "DOWNLOADED: 50\n"
        "SKIPPED: 30\n"
        "BYTES: 5000\n"
        "\n"
        "[INFO] 22:02:36: 3.0 seconds.\n"
    )
    assert _read_download_failed_count(str(log)) == 0


def test_read_download_failed_count_ignores_failed_outside_counts_section(tmp_path):
    """A stray "FAILED:" earlier in the log (e.g. inside an [ERROR] line)
    must NOT be picked up as the tracker's FAILED count. The COUNTS dump
    is the last thing the downloader writes; anchor the lookup there."""
    log = tmp_path / "noisy.log"
    log.write_text(
        "[INFO] 22:02:33: Starting download for si\n"
        "[ERROR] 22:02:34: HTTPError 500; FAILED: 999 retries exhausted\n"
        "[INFO] 22:02:36: \n"
        "COUNTS:\n"
        "DOWNLOADED: 50\n"
        "SKIPPED: 30\n"
    )
    # Only the COUNTS section's FAILED line counts. Absent → 0.
    assert _read_download_failed_count(str(log)) == 0


def _capture_completion_message(env: dict, tracker_counts: dict) -> dict:
    """Run notify_upload_complete with a mocked Tracker + env, return the
    keyword arguments passed to `_post_completion_notice` (header,
    plain_text, stats_lines) so the test can assert on them."""
    tracker = MagicMock(spec=Tracker)
    tracker.count.side_effect = lambda result: tracker_counts.get(result, 0)
    captured: dict = {}

    with (
        patch.dict(os.environ, env, clear=True),
        patch("ingest_wikimedia.slack._post_completion_notice") as mock_post,
    ):
        mock_post.side_effect = lambda **kwargs: captured.update(kwargs)
        notify_upload_complete(
            tracker=tracker,
            partner_label="si",
            elapsed_seconds=16.0,
            dry_run=False,
        )
    return captured


def _write_retry_download_log(partner_dir, label: str, body: str):
    """Write a download log into `{partner_dir}/logs/` with the filename
    pattern produced by ingest_wikimedia.logs.setup_logging — that's what
    `_find_retry_download_log` globs for."""
    logs_dir = partner_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log = logs_dir / f"20260524-220233-{label}-download.log"
    log.write_text(body)
    return log


def test_notify_upload_complete_non_retry_label_keeps_upload_only_header(tmp_path):
    """A non-retry session label (no `retry-` prefix) keeps the existing
    "Wikimedia Upload Complete" header and FAILED is the uploader
    tracker's count alone, even if a download log happens to be present
    in the partner dir — non-retry runs have their own download Slack
    message and must not be combined."""
    _write_retry_download_log(tmp_path, "si", "[INFO] foo\nCOUNTS:\nFAILED: 999\n")
    captured = _capture_completion_message(
        env={
            "DPLA_SLACK_BOT_TOKEN": "x",
            "WIKIMEDIA_SESSION_LABEL": "si",
            "WIKIMEDIA_PARTNER_DIR": str(tmp_path),
        },
        tracker_counts={
            Result.UPLOADED: 5,
            Result.SKIPPED: 10,
            Result.UPLOAD_SKIPPED_NOT_PRESENT: 3,
            Result.UPLOAD_SKIPPED_INELIGIBLE: 7,
            Result.FAILED: 2,
        },
    )
    assert "Wikimedia Upload Complete" in captured["header"]
    assert "Retry Complete" not in captured["header"]
    failed_lines = [s for s in captured["stats_lines"] if s.startswith("FAILED:")]
    assert failed_lines == ["FAILED:        2"]
    # Granular skip-class breakdown shows up under the aggregate
    # SKIPPED. Lets operators distinguish upstream-gap (downloader
    # didn't stage) from MIME / eligibility skips.
    assert any("not present: 3" in s for s in captured["stats_lines"])
    assert any("ineligible:  7" in s for s in captured["stats_lines"])


def test_notify_upload_complete_with_retry_label_combines_failed_count(tmp_path):
    """With WIKIMEDIA_SESSION_LABEL=retry-* and a matching download log
    in WIKIMEDIA_PARTNER_DIR/logs/ that recorded 1 failure, FAILED in
    the Slack summary is the *sum* of the upload tracker's failures and
    the download log's failures, and the header is re-titled to
    "Retry Complete"."""
    _write_retry_download_log(
        tmp_path, "retry-si", "[INFO] foo\nCOUNTS:\nFAILED: 1\nSKIPPED: 79\n"
    )
    captured = _capture_completion_message(
        env={
            "DPLA_SLACK_BOT_TOKEN": "x",
            "WIKIMEDIA_SESSION_LABEL": "retry-si",
            "WIKIMEDIA_PARTNER_DIR": str(tmp_path),
            "WIKIMEDIA_RETRY_HAS_DOWNLOAD": "1",
        },
        # Upload tracker: 0 uploaded, 80 skipped (already on Commons), 0 failed.
        # The retry summary must surface the download-phase FAILED=1.
        tracker_counts={Result.UPLOADED: 0, Result.SKIPPED: 80, Result.FAILED: 0},
    )
    assert "Wikimedia Retry Complete" in captured["header"]
    assert "Upload Complete" not in captured["header"]
    failed_lines = [s for s in captured["stats_lines"] if s.startswith("FAILED:")]
    assert failed_lines == ["FAILED:        1"]
    # SKIPPED and UPLOADED stay as upload-phase only — each phase's SKIPPED
    # means a different thing (download = "already in S3"; upload = "already
    # on Commons") and conflating them would obscure the picture.
    skipped_lines = [s for s in captured["stats_lines"] if s.startswith("SKIPPED:")]
    assert skipped_lines == ["SKIPPED:       80"]


def test_notify_upload_complete_with_retry_label_but_no_log_file(tmp_path):
    """If the retry label is set but no matching download log exists in
    the partner dir's logs/, gracefully fall back to the upload-only
    header rather than crashing the notification."""
    (tmp_path / "logs").mkdir()  # empty logs dir, no matching file
    captured = _capture_completion_message(
        env={
            "DPLA_SLACK_BOT_TOKEN": "x",
            "WIKIMEDIA_SESSION_LABEL": "retry-si",
            "WIKIMEDIA_PARTNER_DIR": str(tmp_path),
            "WIKIMEDIA_RETRY_HAS_DOWNLOAD": "1",
        },
        tracker_counts={Result.UPLOADED: 0, Result.SKIPPED: 80, Result.FAILED: 0},
    )
    # Falls back gracefully — upload-only header, FAILED unchanged.
    assert "Wikimedia Upload Complete" in captured["header"]
    failed_lines = [s for s in captured["stats_lines"] if s.startswith("FAILED:")]
    assert failed_lines == ["FAILED:        0"]


def test_notify_upload_complete_upload_only_retry_ignores_stale_download_log(
    tmp_path,
):
    """An upload-only retry (WIKIMEDIA_RETRY_HAS_DOWNLOAD unset) must NOT
    pick up a stale `*-retry-<slug>-download.log` from a prior retry run.

    Retry session labels are reused across runs, so without the gate the
    most-recent matching download log on disk — from a previous session
    that already shipped its own Slack summary — would be folded in,
    inflating FAILED and incorrectly switching to "Retry Complete"."""
    _write_retry_download_log(
        tmp_path, "retry-si", "[INFO] stale\nCOUNTS:\nFAILED: 999\nSKIPPED: 1\n"
    )
    captured = _capture_completion_message(
        env={
            "DPLA_SLACK_BOT_TOKEN": "x",
            "WIKIMEDIA_SESSION_LABEL": "retry-si",
            "WIKIMEDIA_PARTNER_DIR": str(tmp_path),
            # WIKIMEDIA_RETRY_HAS_DOWNLOAD intentionally unset.
        },
        tracker_counts={Result.UPLOADED: 0, Result.SKIPPED: 80, Result.FAILED: 0},
    )
    assert "Wikimedia Upload Complete" in captured["header"]
    assert "Retry Complete" not in captured["header"]
    failed_lines = [s for s in captured["stats_lines"] if s.startswith("FAILED:")]
    assert failed_lines == ["FAILED:        0"]


def _capture_sdc_completion_message(env: dict, tracker_counts: dict) -> dict:
    """Mirror of `_capture_completion_message` for `notify_sdc_complete`."""
    tracker = MagicMock(spec=Tracker)
    tracker.count.side_effect = lambda result: tracker_counts.get(result, 0)
    captured: dict = {}

    with (
        patch.dict(os.environ, env, clear=True),
        patch("ingest_wikimedia.slack._post_completion_notice") as mock_post,
    ):
        mock_post.side_effect = lambda **kwargs: captured.update(kwargs)
        notify_sdc_complete(
            tracker=tracker,
            partner_label="minnesota",
            elapsed_seconds=42.0,
            dry_run=False,
        )
    return captured


def test_notify_sdc_complete_header_uses_session_label():
    """Confirms the SDC Slack summary header is "Wikimedia SDC Complete:
    wikimedia-<session label>", matching the shape of upload-complete."""
    captured = _capture_sdc_completion_message(
        env={
            "DPLA_SLACK_BOT_TOKEN": "x",
            "WIKIMEDIA_SESSION_LABEL": "minnesota",
        },
        tracker_counts={
            Result.SDC_ITEMS_SYNCED: 3,
            Result.SDC_CLAIMS_ADDED: 7,
            Result.SDC_REFS_ADDED: 5,
        },
    )
    assert captured["header"] == "*Wikimedia SDC Complete: wikimedia-minnesota*"
    assert captured["plain_text"] == "Wikimedia SDC complete: wikimedia-minnesota"


def test_notify_sdc_complete_stats_reflect_tracker_counts():
    """ITEMS SYNCED / CLAIMS ADDED / REFS ADDED / REMOVALS / SKIPPED lines
    pull from the corresponding Result enum members, and Runtime formats
    elapsed_seconds."""
    captured = _capture_sdc_completion_message(
        env={
            "DPLA_SLACK_BOT_TOKEN": "x",
            "WIKIMEDIA_SESSION_LABEL": "minnesota",
        },
        tracker_counts={
            Result.SDC_ITEMS_SYNCED: 100,
            Result.SDC_ITEMS_PARTIALLY_SYNCED: 9,
            Result.SDC_PAGES_EDITED: 1234,
            Result.SDC_CLAIMS_ADDED: 250,
            Result.SDC_REFS_ADDED: 30,
            Result.SDC_REMOVALS: 4,
            Result.SDC_ITEMS_SKIPPED_NO_SIDECAR: 2,
            Result.SDC_ITEMS_SKIPPED_MAPPING: 1,
            Result.SDC_ITEMS_SKIPPED_ERROR: 3,
            Result.SDC_ORDINALS_SKIPPED_MISSING_ENTITY: 5,
            Result.SDC_ORDINALS_SKIPPED_MISSING_PAGEID: 6,
            Result.SDC_ORDINALS_SKIPPED_ERROR: 7,
        },
    )
    stats = captured["stats_lines"]
    assert any(s.startswith("ITEMS SYNCED:") and "100" in s for s in stats)
    assert any(s.startswith("ITEMS PARTIAL:") and "9" in s for s in stats)
    assert any(s.startswith("PAGES EDITED:") and "1,234" in s for s in stats)
    assert any(s.startswith("CLAIMS ADDED:") and "250" in s for s in stats)
    assert any(s.startswith("REFS ADDED:") and "30" in s for s in stats)
    assert any(s.startswith("REMOVALS:") and "4" in s for s in stats)
    assert any("SKIPPED (no sidecar)" in s and "2" in s for s in stats)
    assert any("SKIPPED (mapping)" in s and "1" in s for s in stats)
    assert any("SKIPPED (error)" in s and "3" in s for s in stats)
    assert any("ORDINAL MISSING:" in s and "5" in s for s in stats)
    assert any("ORDINAL NO PAGEID:" in s and "6" in s for s in stats)
    assert any("ORDINAL ERRORS:" in s and "7" in s for s in stats)
    assert any("Runtime:" in s and "42s" in s for s in stats)

    # PAGES EDITED belongs in the leading "scope" block (ITEMS SYNCED /
    # ITEMS PARTIAL / PAGES EDITED) — it's the per-file-page batch size,
    # paired with the per-item counts. Pin the order so a stray refactor
    # doesn't bury it next to the SKIPPED footer where operators won't
    # notice it.
    def _idx(prefix: str) -> int:
        return next(i for i, s in enumerate(stats) if s.startswith(prefix))

    assert _idx("ITEMS SYNCED:") < _idx("ITEMS PARTIAL:") < _idx("PAGES EDITED:")
    assert _idx("PAGES EDITED:") < _idx("CLAIMS ADDED:")


def test_notify_sdc_complete_reports_slot_wait_contention():
    """SLOT WAIT shows the per-worker average (aggregate worker-seconds ÷
    workers) as a share of runtime — a stable whole-run contention figure,
    not a point-in-time slot-count snapshot."""
    tracker = MagicMock(spec=Tracker)
    # 240 worker-seconds aggregate ÷ 4 workers = 60s avg/worker; over a
    # 300s runtime that's 20%.
    counts = {Result.SDC_SLOT_WAIT_SECONDS: 240}
    tracker.count.side_effect = lambda r: counts.get(r, 0)
    captured: dict = {}
    with (
        patch.dict(os.environ, {"DPLA_SLACK_BOT_TOKEN": "x"}, clear=True),
        patch("ingest_wikimedia.slack._post_completion_notice") as mock_post,
    ):
        mock_post.side_effect = lambda **kwargs: captured.update(kwargs)
        notify_sdc_complete(
            tracker=tracker,
            partner_label="nara",
            elapsed_seconds=300.0,
            workers=4,
        )
    line = next(s for s in captured["stats_lines"] if s.startswith("SLOT WAIT"))
    assert "20%" in line


def test_notify_sdc_complete_dry_run_adds_suffix():
    """`dry_run=True` appends the same italicized note as upload-complete."""
    tracker = MagicMock(spec=Tracker)
    tracker.count.return_value = 0
    captured: dict = {}
    with (
        patch.dict(
            os.environ,
            {"DPLA_SLACK_BOT_TOKEN": "x", "WIKIMEDIA_SESSION_LABEL": "minnesota"},
            clear=True,
        ),
        patch("ingest_wikimedia.slack._post_completion_notice") as mock_post,
    ):
        mock_post.side_effect = lambda **kwargs: captured.update(kwargs)
        notify_sdc_complete(
            tracker=tracker,
            partner_label="minnesota",
            elapsed_seconds=1.0,
            dry_run=True,
        )
    assert captured["header"].endswith("_(dry run)_")


def test_notify_sdc_complete_skipped_without_token(monkeypatch, caplog):
    """Without DPLA_SLACK_BOT_TOKEN we must skip silently (just warn)."""
    monkeypatch.delenv("DPLA_SLACK_BOT_TOKEN", raising=False)
    tracker = MagicMock(spec=Tracker)
    tracker.count.return_value = 0
    with patch("ingest_wikimedia.slack._post_completion_notice") as mock_post:
        notify_sdc_complete(
            tracker=tracker, partner_label="minnesota", elapsed_seconds=1.0
        )
        mock_post.assert_not_called()


def test_notify_upload_complete_clean_retry_still_titled_retry_complete(tmp_path):
    """A retry session with ZERO failures must still be titled
    "Retry Complete" — not fall back to "Upload Complete" just because
    the download log's COUNTS section omitted the FAILED line.

    Tracker.__str__ only emits counter lines whose value > 0, so clean
    download runs legitimately have no FAILED line in their COUNTS
    section. `_read_download_failed_count` returns 0 (not None) in that
    case so the retry-aware code path still fires."""
    _write_retry_download_log(
        tmp_path,
        "retry-si",
        "[INFO] 22:02:33: Starting download for si\n"
        "COUNTS:\n"
        "DOWNLOADED: 80\n"
        "SKIPPED: 0\n"
        "BYTES: 8000\n",
    )
    captured = _capture_completion_message(
        env={
            "DPLA_SLACK_BOT_TOKEN": "x",
            "WIKIMEDIA_SESSION_LABEL": "retry-si",
            "WIKIMEDIA_PARTNER_DIR": str(tmp_path),
            "WIKIMEDIA_RETRY_HAS_DOWNLOAD": "1",
        },
        tracker_counts={Result.UPLOADED: 0, Result.SKIPPED: 80, Result.FAILED: 0},
    )
    assert "Wikimedia Retry Complete" in captured["header"]
    assert "Upload Complete" not in captured["header"]
    failed_lines = [s for s in captured["stats_lines"] if s.startswith("FAILED:")]
    assert failed_lines == ["FAILED:        0"]


def _capture_sdc_completion(env: dict, tracker_counts: dict, maintain: bool) -> dict:
    """Run notify_sdc_complete with a mocked Tracker + env, returning the
    kwargs passed to _post_completion_notice so tests can assert on
    stats_lines."""
    tracker = MagicMock(spec=Tracker)
    tracker.count.side_effect = lambda result: tracker_counts.get(result, 0)
    captured: dict = {}
    with (
        patch.dict(os.environ, env, clear=True),
        patch("ingest_wikimedia.slack._post_completion_notice") as mock_post,
    ):
        mock_post.side_effect = lambda **kwargs: captured.update(kwargs)
        notify_sdc_complete(
            tracker=tracker,
            partner_label="digitalnc",
            elapsed_seconds=10.0,
            workers=1,
            maintain=maintain,
        )
    return captured


def test_notify_sdc_complete_maintain_reports_rename_counters():
    captured = _capture_sdc_completion(
        env={"DPLA_SLACK_BOT_TOKEN": "x"},
        tracker_counts={
            Result.MAINTAIN_RENAMED: 7,
            Result.MAINTAIN_RENAME_BLOCKED: 2,
        },
        maintain=True,
    )
    lines = captured["stats_lines"]
    assert any(s.startswith("RENAMED:") and s.endswith("7") for s in lines)
    assert any(s.startswith("RENAME BLOCKED:") and s.endswith("2") for s in lines)


def test_notify_sdc_complete_non_maintain_omits_rename_counters():
    captured = _capture_sdc_completion(
        env={"DPLA_SLACK_BOT_TOKEN": "x"},
        tracker_counts={},
        maintain=False,
    )
    lines = captured["stats_lines"]
    assert not any(s.startswith("RENAMED:") for s in lines)
    assert not any(s.startswith("RENAME BLOCKED:") for s in lines)
