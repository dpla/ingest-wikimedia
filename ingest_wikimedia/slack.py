import logging
import os
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


def notify_pipeline_fail() -> None:
    """Post a pipeline-step failure notification to Slack.

    Reads DPLA_SLACK_BOT_TOKEN and WIKIMEDIA_SESSION_LABEL from the environment.
    Designed to be called as a one-liner from a shell failure handler:
        python3 -c 'from ingest_wikimedia.slack import notify_pipeline_fail; notify_pipeline_fail()'
    """
    token = (os.environ.get("DPLA_SLACK_BOT_TOKEN") or "").strip()
    if not token:
        logging.warning(
            "DPLA_SLACK_BOT_TOKEN not set — skipping pipeline failure notification"
        )
        return
    label = os.environ.get("WIKIMEDIA_SESSION_LABEL") or "unknown"
    try:
        post_message(
            token,
            f"❌ `wikimedia-{label}`: pipeline step failed — skipping to next target",
        )
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
