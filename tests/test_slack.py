"""Tests for the pure helpers in ingest_wikimedia.slack.

The network-touching paths (`post_message`, `notify_pipeline_fail`'s actual
HTTP call) are not covered here — these tests exercise the decoding /
log-summary logic that produces the message body.
"""

import os
import tempfile
import time

import pytest

from ingest_wikimedia.slack import (
    _decode_exit_code,
    _find_latest_log,
    _summarize_log,
)


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
