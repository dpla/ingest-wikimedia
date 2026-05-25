"""Tests for the pure helpers in ingest_wikimedia.slack.

The network-touching paths (`post_message`, `notify_pipeline_fail`'s actual
HTTP call) are not covered here — these tests exercise the decoding /
log-summary logic that produces the message body.
"""

import os
import tempfile
import time
from unittest.mock import MagicMock, patch

import pytest

from ingest_wikimedia.slack import (
    _decode_exit_code,
    _find_latest_log,
    _read_download_failed_count,
    _summarize_log,
    notify_pipeline_fail,
    notify_upload_complete,
)
from ingest_wikimedia.tracker import Result, Tracker


@pytest.mark.parametrize(
    "rc, expected",
    [
        ("0", ""),
        ("", ""),
        (None, ""),
        ("not-a-number", ""),
        ("1", " (exit 1)"),
        ("2", " (exit 2)"),
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


def test_notify_pipeline_fail_says_skipping_to_next_when_not_last():
    msg = _capture_message(
        {
            "DPLA_SLACK_BOT_TOKEN": "x",
            "WIKIMEDIA_SESSION_LABEL": "nara+foo",
            "WIKIMEDIA_LAST_EXIT": "1",
        }
    )
    assert "skipping to next target" in msg
    assert "no further targets in batch" not in msg


def test_notify_pipeline_fail_says_no_further_when_last():
    msg = _capture_message(
        {
            "DPLA_SLACK_BOT_TOKEN": "x",
            "WIKIMEDIA_SESSION_LABEL": "nara+foo",
            "WIKIMEDIA_LAST_EXIT": "137",
            "WIKIMEDIA_TARGET_IS_LAST": "1",
        }
    )
    assert "no further targets in batch" in msg
    assert "skipping to next target" not in msg
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
        assert "skipping to next target" in msg, (
            f"value {value!r} should be treated as not-last"
        )


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
        tracker_counts={Result.UPLOADED: 5, Result.SKIPPED: 10, Result.FAILED: 2},
    )
    assert "Wikimedia Upload Complete" in captured["header"]
    assert "Retry Complete" not in captured["header"]
    failed_lines = [s for s in captured["stats_lines"] if s.startswith("FAILED:")]
    assert failed_lines == ["FAILED:   2"]


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
        },
        # Upload tracker: 0 uploaded, 80 skipped (already on Commons), 0 failed.
        # The retry summary must surface the download-phase FAILED=1.
        tracker_counts={Result.UPLOADED: 0, Result.SKIPPED: 80, Result.FAILED: 0},
    )
    assert "Wikimedia Retry Complete" in captured["header"]
    assert "Upload Complete" not in captured["header"]
    failed_lines = [s for s in captured["stats_lines"] if s.startswith("FAILED:")]
    assert failed_lines == ["FAILED:   1"]
    # SKIPPED and UPLOADED stay as upload-phase only — each phase's SKIPPED
    # means a different thing (download = "already in S3"; upload = "already
    # on Commons") and conflating them would obscure the picture.
    skipped_lines = [s for s in captured["stats_lines"] if s.startswith("SKIPPED:")]
    assert skipped_lines == ["SKIPPED:  80"]


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
        },
        tracker_counts={Result.UPLOADED: 0, Result.SKIPPED: 80, Result.FAILED: 0},
    )
    # Falls back gracefully — upload-only header, FAILED unchanged.
    assert "Wikimedia Upload Complete" in captured["header"]
    failed_lines = [s for s in captured["stats_lines"] if s.startswith("FAILED:")]
    assert failed_lines == ["FAILED:   0"]


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
        },
        tracker_counts={Result.UPLOADED: 0, Result.SKIPPED: 80, Result.FAILED: 0},
    )
    assert "Wikimedia Retry Complete" in captured["header"]
    assert "Upload Complete" not in captured["header"]
    failed_lines = [s for s in captured["stats_lines"] if s.startswith("FAILED:")]
    assert failed_lines == ["FAILED:   0"]
