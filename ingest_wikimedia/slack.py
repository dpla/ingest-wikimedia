import logging
import os

import requests

from ingest_wikimedia.tracker import Result, Tracker

SLACK_CHANNEL = "C02HEU2L3"
SLACK_API_URL = "https://slack.com/api/chat.postMessage"


def _format_bytes(num_bytes: int) -> str:
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024:
            return f"{value:,.1f} {unit}"
        value /= 1024
    return f"{value:,.1f} PB"


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

    hours, remainder = divmod(int(elapsed_seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        runtime = f"{hours}h {minutes}m {seconds}s"
    elif minutes:
        runtime = f"{minutes}m {seconds}s"
    else:
        runtime = f"{seconds}s"

    dry_run_note = " _(dry run)_" if dry_run else ""
    header = f"*Wikimedia Upload Complete: {partner_label}*{dry_run_note}"

    lines = [
        f"UPLOADED: {tracker.count(Result.UPLOADED):,}",
        f"SKIPPED:  {tracker.count(Result.SKIPPED):,}",
        f"FAILED:   {tracker.count(Result.FAILED):,}",
        f"BYTES:    {_format_bytes(tracker.count(Result.BYTES))}",
        f"Runtime:  {runtime}",
    ]
    body = "```" + "\n".join(lines) + "```"

    payload = {
        "channel": SLACK_CHANNEL,
        "text": f"Wikimedia upload complete: {partner_label}",
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
