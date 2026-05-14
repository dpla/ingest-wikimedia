#!/usr/bin/env python3
"""Check Wikimedia upload session status on EC2 and post a summary to Slack.

Runs as a GitHub Action on a schedule and on workflow_dispatch (triggered by
the /wikimedia-status Slack slash command via Lambda).
"""

import logging
import os
import shlex
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import requests

from ingest_wikimedia.partners import PARTNER_DIR
from ingest_wikimedia.ssm import REGION, ssm_run

SLACK_CHANNEL = "C02HEU2L3"
SLACK_API_URL = "https://slack.com/api/chat.postMessage"


def get_phase_and_progress(client, session: str, hub: str) -> str:
    def _safe_int(s: str) -> int:
        try:
            return int(s)
        except ValueError:
            return 0

    pdir = PARTNER_DIR.get(hub, hub)
    base = f"/home/ec2-user/ingest-wikimedia/{pdir}"
    log_dir = shlex.quote(f"{base}/logs")
    session_name = shlex.quote(session)

    # Get session creation time and most recent log filename — no shell variables
    # needed, avoiding outer-bash expansion of $f inside the bash -c double-quote.
    precheck = ssm_run(
        client,
        f"tmux display-message -t {session_name} -p '#{{session_created}}' 2>/dev/null || echo 0; "
        f"ls -t {log_dir}/ 2>/dev/null | head -1",
    )
    precheck_lines = precheck.splitlines()
    session_created = _safe_int(precheck_lines[0]) if precheck_lines else 0
    log_file = precheck_lines[1].strip() if len(precheck_lines) > 1 else ""

    if not log_file:
        return "Generating IDs"

    log_path = shlex.quote(f"{base}/logs/{log_file}")
    csv_path = shlex.quote(f"{base}/{hub}.csv")

    sep = "__WM_SEP__"
    out = ssm_run(
        client,
        f"stat -c %Y {log_path} 2>/dev/null || echo 0; "
        f"echo {sep}; "
        f"tail -5 {log_path}; "
        f"echo {sep}; "
        f"grep -c 'DPLA ID:' {log_path} 2>/dev/null || true; "
        f"grep -c 'Uploaded to' {log_path} 2>/dev/null || true; "
        f"grep -c 'Skipping.*Already exists on commons' {log_path} 2>/dev/null || true; "
        f"wc -l < {csv_path} 2>/dev/null || echo 0; "
        f"grep -c 'COUNTS:' {log_path} 2>/dev/null || true",
    )

    sections = out.split(f"{sep}\n", 2)
    log_mtime = _safe_int(sections[0].strip()) if sections else 0

    # Log predates this session → downloader hasn't started yet.
    if session_created > 0 and log_mtime < session_created:
        return "Generating IDs"

    tail = sections[1].strip() if len(sections) > 1 else ""
    count_lines = sections[2].strip().splitlines() if len(sections) > 2 else []

    dpla_id_count = _safe_int(count_lines[0]) if len(count_lines) > 0 else 0
    uploaded_count = _safe_int(count_lines[1]) if len(count_lines) > 1 else 0
    skipped_count = _safe_int(count_lines[2]) if len(count_lines) > 2 else 0
    total = _safe_int(count_lines[3]) if len(count_lines) > 3 else 0
    counts_marker = _safe_int(count_lines[4]) if len(count_lines) > 4 else 0

    def pct(n: int) -> str:
        return f"{n / total * 100:.1f}" if total > 0 else "?"

    if log_file.endswith("-download.log"):
        if "Downloading" in tail or "Key already in S3" in tail:
            return f"Downloading ({dpla_id_count:,} / {total:,} items, ~{pct(dpla_id_count)}%)"
        return "Generating IDs"

    if log_file.endswith("-upload.log"):
        if dpla_id_count == 0:
            return "Uploading (starting...)"
        # Use the COUNTS: terminal marker as the definitive completion signal.
        # dpla_id_count is logged at the start of each item, not after all its
        # files finish, so count arithmetic alone can fire too early.
        if counts_marker > 0:
            return f"Upload complete ({uploaded_count:,} uploaded, {skipped_count:,} already on Commons)"
        return f"Uploading ({dpla_id_count:,} / {total:,}, ~{pct(dpla_id_count)}%)"

    return "Unknown"


def post_to_slack(token: str, rows: list[tuple[str, str]]) -> None:
    lines = "\n".join(f"`{session:<32}` {phase}" for session, phase in rows)
    payload = {
        "channel": SLACK_CHANNEL,
        "text": "Wikimedia Upload Status",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "Wikimedia Upload Status"},
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": lines}},
        ],
    }
    resp = requests.post(
        SLACK_API_URL,
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"Slack API error: {data.get('error')}")


def main() -> None:
    token = (os.environ.get("DPLA_SLACK_BOT_TOKEN") or "").strip()
    if not token:
        raise RuntimeError(
            "Missing required environment variable: DPLA_SLACK_BOT_TOKEN"
        )

    ssm = boto3.client("ssm", region_name=REGION)

    notify_if_idle = os.environ.get("NOTIFY_IF_IDLE", "false").lower() == "true"

    try:
        session_out = ssm_run(
            ssm, "tmux ls 2>/dev/null | grep '^wikimedia-' || echo NONE"
        )
    except TimeoutError as e:
        logging.error("SSM poll timed out: %s", e)
        post_to_slack(
            token,
            [
                (
                    "(error)",
                    "Status check timed out — SSM did not respond. Try again shortly.",
                )
            ],
        )
        return

    sessions = (
        [line.split(":")[0].strip() for line in session_out.splitlines()]
        if session_out and session_out != "NONE"
        else []
    )

    if not sessions:
        print("No active wikimedia sessions.")
        if notify_if_idle:
            post_to_slack(token, [("(none)", "No active Wikimedia upload sessions.")])
            print("Posted idle status to Slack.")
        return

    results: dict[str, str] = {}

    def fetch(session: str) -> tuple[str, str]:
        hub = session.removeprefix("wikimedia-").split("+")[0]
        try:
            phase = get_phase_and_progress(ssm, session, hub)
        except Exception:
            logging.exception("Failed to get status for %s", session)
            phase = "Unknown (error)"
        return session, phase

    with ThreadPoolExecutor(max_workers=min(len(sessions), 8)) as executor:
        futures = {executor.submit(fetch, s): s for s in sessions}
        for future in as_completed(futures):
            session, phase = future.result()
            results[session] = phase
            print(f"{session}: {phase}")

    rows = [(s, results[s]) for s in sessions if s in results]
    post_to_slack(token, rows)
    print("Posted to Slack.")


if __name__ == "__main__":
    main()
