import glob
import logging
import os
import re
from typing import Literal

import requests

from ingest_wikimedia.tracker import Result, Tracker

SLACK_CHANNEL = "C02HEU2L3"
SLACK_API_URL = "https://slack.com/api/chat.postMessage"

Phase = Literal["id-generation", "download", "upload"]

_PHASE_EMOJI: dict[str, str] = {
    "id-generation": "🔍",
    "download": "⬇",
    "upload": "⬆",
}


def post_message(token: str, text: str) -> None:
    resp = requests.post(
        SLACK_API_URL,
        headers={"Authorization": f"Bearer {token}"},
        json={"channel": SLACK_CHANNEL, "text": text},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Slack API error: {data.get('error')}")


# exit code → short hint shown in the Slack failure message.  Anything >128 is
# a bash-encoded signal (128 + signal number).  137 in particular is SIGKILL,
# which the OOM killer uses — so seeing it in the message is a strong "this was
# probably an OOM" hint without having to SSM in and check dmesg.
_EXIT_CODE_HINTS: dict[int, str] = {
    137: "SIGKILL — likely OOM",
    143: "SIGTERM",
    139: "SIGSEGV",
    134: "SIGABRT",
    130: "SIGINT (Ctrl-C)",
}


def _decode_exit_code(rc_str: str | None) -> str:
    """Render a `(exit N — meaning)` suffix for the failure message."""
    if not rc_str:
        return ""
    try:
        rc = int(rc_str)
    except ValueError:
        return ""
    if rc == 0:
        return ""
    hint = _EXIT_CODE_HINTS.get(rc)
    if hint is None and rc > 128:
        hint = f"signal {rc - 128}"
    return f" (exit {rc}" + (f" — {hint})" if hint else ")")


def _find_latest_log(partner_dir: str, label: str) -> str | None:
    """Return the most recently modified log file matching this label, or None.

    Logs are named `{timestamp}-{label}-{phase}.log` under `<partner_dir>/logs/`.
    """
    if not partner_dir or not os.path.isdir(partner_dir):
        return None
    pattern = os.path.join(partner_dir, "logs", f"*-{label}-*.log")
    candidates = glob.glob(pattern)
    if not candidates:
        return None
    return max(candidates, key=os.path.getmtime)


# Counted markers shown in the failure summary.  Patterns match what the
# downloader and uploader currently emit, anchored loosely so log-format
# tweaks don't silently zero them out.  Skip subcategories aren't split out
# because "Skipping ... Already exists" would double-count under a generic
# "Skipping" pattern — the log tail conveys reasons more usefully anyway.
_LOG_MARKERS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("uploaded", re.compile(r"Uploaded to https://commons", re.IGNORECASE)),
    ("skipped", re.compile(r"^\[INFO\].*Skipping ", re.MULTILINE)),
    ("downloaded", re.compile(r"^\[INFO\].*Downloaded ", re.MULTILINE)),
    ("failed", re.compile(r"^\[(ERROR|WARNING)\].*Failed", re.MULTILINE)),
)


def _summarize_log(log_path: str, tail_lines: int = 8) -> str | None:
    """Read the log and produce a short multi-line summary.

    Tries to be cheap: only reads up to ~2 MB from the end of the file.  Counts
    common markers and tails the last N lines so the cause of the failure is
    visible without SSM-ing in.
    """
    try:
        size = os.path.getsize(log_path)
        read_size = min(size, 2 * 1024 * 1024)
        with open(log_path, "rb") as f:
            f.seek(size - read_size)
            data = f.read().decode("utf-8", errors="replace")
    except OSError:
        return None

    counts = []
    for label, pat in _LOG_MARKERS:
        n = len(pat.findall(data))
        if n:
            counts.append(f"{n} {label}")

    tail = "\n".join(data.splitlines()[-tail_lines:])

    parts = [f"Log: `{os.path.basename(log_path)}`"]
    if counts:
        parts.append("Counts so far: " + ", ".join(counts))
    if tail:
        parts.append("Last lines:\n```\n" + tail + "\n```")
    return "\n".join(parts)


def notify_pipeline_fail() -> None:
    """Post a pipeline-step failure notification to Slack.

    Reads from the environment:
      DPLA_SLACK_BOT_TOKEN     — required to post
      WIKIMEDIA_SESSION_LABEL  — identifies the target in the message
      WIKIMEDIA_LAST_EXIT      — exit code of the failed step (best-effort)
      WIKIMEDIA_PARTNER_DIR    — absolute path to the partner dir, used to
                                 locate the most recent log for tailing
      WIKIMEDIA_TARGET_IS_LAST — "1" iff this is the final target in the
                                 batch; switches the message suffix from
                                 "skipping to next target" to "no further
                                 targets in batch", which is accurate even
                                 for single-target sessions

    Designed to be called as a one-liner from a shell failure handler:
        rc=$?; WIKIMEDIA_LAST_EXIT=$rc python3 -c \\
          'from ingest_wikimedia.slack import notify_pipeline_fail; notify_pipeline_fail()'
    """
    token = (os.environ.get("DPLA_SLACK_BOT_TOKEN") or "").strip()
    if not token:
        logging.warning(
            "DPLA_SLACK_BOT_TOKEN not set — skipping pipeline failure notification"
        )
        return
    label = os.environ.get("WIKIMEDIA_SESSION_LABEL") or "unknown"
    rc_suffix = _decode_exit_code(os.environ.get("WIKIMEDIA_LAST_EXIT"))

    is_last = os.environ.get("WIKIMEDIA_TARGET_IS_LAST") == "1"
    tail_phrase = (
        "no further targets in batch" if is_last else "skipping to next target"
    )
    msg = f"❌ `wikimedia-{label}`: pipeline step failed{rc_suffix} — {tail_phrase}"

    log_path = _find_latest_log(os.environ.get("WIKIMEDIA_PARTNER_DIR", ""), label)
    if log_path is not None:
        summary = _summarize_log(log_path)
        if summary:
            msg += "\n" + summary

    try:
        post_message(token, msg)
    except Exception:
        logging.warning(
            "Failed to post pipeline failure notification to Slack", exc_info=True
        )


def notify_phase_start(partner: str, phase: Phase) -> None:
    # Single-item targets post only one launch notification and one completion
    # notification; suppress per-phase messages to avoid cluttering #tech-alerts.
    if os.environ.get("WIKIMEDIA_SINGLE_ITEM") == "1":
        return
    token = os.environ.get("DPLA_SLACK_BOT_TOKEN")
    if not token:
        logging.warning("DPLA_SLACK_BOT_TOKEN not set — skipping Slack notification")
        return
    session_label = os.environ.get("WIKIMEDIA_SESSION_LABEL") or partner
    emoji = _PHASE_EMOJI.get(phase, "▶")
    try:
        post_message(token, f"{emoji} `wikimedia-{session_label}`: starting {phase}")
    except Exception:
        logging.warning("Slack phase notification failed", exc_info=True)


def _format_bytes(num_bytes: int) -> str:
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024:
            return f"{value:,.1f} {unit}"
        value /= 1024
    return f"{value:,.1f} PB"


def _format_runtime(elapsed_seconds: float) -> str:
    hours, remainder = divmod(int(elapsed_seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _post_completion_notice(
    token: str,
    header: str,
    plain_text: str,
    stats_lines: list[str],
) -> None:
    """Post a completion summary block to #tech-alerts. Logs warnings on failure."""
    body = "```" + "\n".join(stats_lines) + "```"
    payload = {
        "channel": SLACK_CHANNEL,
        "text": plain_text,
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": header}},
            {"type": "section", "text": {"type": "mrkdwn", "text": body}},
        ],
    }
    try:
        response = requests.post(
            SLACK_API_URL,
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        if not data.get("ok"):
            logging.warning(f"Slack notification failed: {data.get('error')}")
    except requests.exceptions.HTTPError as ex:
        logging.warning(f"Slack API returned HTTP {ex.response.status_code}")
    except Exception as ex:
        logging.warning("Failed to send Slack notification", exc_info=ex)


def notify_download_complete(
    tracker: Tracker,
    partner_label: str,
    elapsed_seconds: float,
    dry_run: bool = False,
) -> None:
    token = os.environ.get("DPLA_SLACK_BOT_TOKEN")
    if not token:
        logging.warning("DPLA_SLACK_BOT_TOKEN not set — skipping Slack notification")
        return

    effective_label = (
        f"wikimedia-{os.environ.get('WIKIMEDIA_SESSION_LABEL') or partner_label}"
    )
    runtime = _format_runtime(elapsed_seconds)
    dry_run_note = " _(dry run)_" if dry_run else ""

    _post_completion_notice(
        token=token,
        header=f"*Wikimedia Download Refresh Complete: {effective_label}*{dry_run_note}",
        plain_text=f"Wikimedia download refresh complete: {effective_label}",
        stats_lines=[
            f"REFRESHED: {tracker.count(Result.DOWNLOADED):,}",
            f"SKIPPED:   {tracker.count(Result.SKIPPED):,}",
            f"FAILED:    {tracker.count(Result.FAILED):,}",
            f"BYTES:     {_format_bytes(tracker.count(Result.BYTES))}",
            f"Runtime:   {runtime}",
        ],
    )


def notify_upload_complete(
    tracker: Tracker,
    partner_label: str,
    elapsed_seconds: float,
    dry_run: bool = False,
) -> None:
    token = os.environ.get("DPLA_SLACK_BOT_TOKEN")
    if not token:
        logging.warning("DPLA_SLACK_BOT_TOKEN not set — skipping Slack notification")
        return

    effective_label = (
        f"wikimedia-{os.environ.get('WIKIMEDIA_SESSION_LABEL') or partner_label}"
    )
    runtime = _format_runtime(elapsed_seconds)
    dry_run_note = " _(dry run)_" if dry_run else ""

    _post_completion_notice(
        token=token,
        header=f"*Wikimedia Upload Complete: {effective_label}*{dry_run_note}",
        plain_text=f"Wikimedia upload complete: {effective_label}",
        stats_lines=[
            f"UPLOADED: {tracker.count(Result.UPLOADED):,}",
            f"SKIPPED:  {tracker.count(Result.SKIPPED):,}",
            f"FAILED:   {tracker.count(Result.FAILED):,}",
            f"BYTES:    {_format_bytes(tracker.count(Result.BYTES))}",
            f"Runtime:  {runtime}",
        ],
    )
