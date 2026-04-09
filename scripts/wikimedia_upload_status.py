#!/usr/bin/env python3
"""Check Wikimedia upload session status on EC2 and post a summary to Slack.

Runs as a GitHub Action on a schedule and on workflow_dispatch (triggered by
the /wikimedia-status Slack slash command via Lambda).
"""

import json
import logging
import os
import shlex
import time

import boto3
import requests

INSTANCE_ID = "i-033eff6c8c168f999"
SLACK_CHANNEL = "C02HEU2L3"
SLACK_API_URL = "https://slack.com/api/chat.postMessage"
REGION = "us-east-1"
SSM_POLL_INTERVAL = 5
SSM_MAX_POLLS = 24  # 2 minutes


def ssm_run(client, cmd: str) -> str:
    resp = client.send_command(
        InstanceIds=[INSTANCE_ID],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [f"sudo -u ec2-user bash -c {json.dumps(cmd)}"]},
    )
    cmd_id = resp["Command"]["CommandId"]
    for _ in range(SSM_MAX_POLLS):
        time.sleep(SSM_POLL_INTERVAL)
        try:
            inv = client.get_command_invocation(
                CommandId=cmd_id, InstanceId=INSTANCE_ID
            )
        except client.exceptions.InvocationDoesNotExist:
            continue
        status = inv["Status"]
        if status == "Success":
            return inv.get("StandardOutputContent", "").strip()
        if status in ("Failed", "TimedOut", "Cancelled"):
            stderr = inv.get("StandardErrorContent", "").strip()
            raise RuntimeError(
                f"SSM command {cmd_id} ended with {status}: {stderr or 'no stderr'}"
            )
    raise TimeoutError(f"SSM command {cmd_id} did not complete within polling window")


def get_phase_and_progress(client, partner: str) -> str:
    base = f"/home/ec2-user/ingest-wikimedia/{partner}"
    log_dir = shlex.quote(f"{base}/logs")
    log_file = ssm_run(client, f"ls -t {log_dir}/ 2>/dev/null | head -1")
    if not log_file:
        return "Generating IDs"

    log_path = shlex.quote(f"/home/ec2-user/ingest-wikimedia/{partner}/logs/{log_file}")
    csv_path = shlex.quote(f"/home/ec2-user/ingest-wikimedia/{partner}/{partner}.csv")

    # Get tail + item count + CSV total in one round-trip
    out = ssm_run(
        client,
        f"tail -5 {log_path}; "
        f"echo '---'; "
        f"grep -c 'DPLA ID:' {log_path} 2>/dev/null || echo 0; "
        f"grep -c 'Uploaded to' {log_path} 2>/dev/null || echo 0; "
        f"grep -c 'Skipping.*Already exists on commons' {log_path} 2>/dev/null || echo 0; "
        f"wc -l < {csv_path} 2>/dev/null || echo 0",
    )

    parts = out.split("---\n", 1)
    tail = parts[0].strip()
    count_lines = parts[1].strip().splitlines() if len(parts) > 1 else []

    dpla_id_count = int(count_lines[0]) if len(count_lines) > 0 else 0
    uploaded_count = int(count_lines[1]) if len(count_lines) > 1 else 0
    skipped_count = int(count_lines[2]) if len(count_lines) > 2 else 0
    total = int(count_lines[3]) if len(count_lines) > 3 else 0

    def pct(n: int) -> str:
        return f"{n / total * 100:.1f}" if total > 0 else "?"

    if "download" in log_file:
        if "Downloading" in tail or "Key already in S3" in tail:
            return f"Downloading ({dpla_id_count:,} / {total:,} items, ~{pct(dpla_id_count)}%)"
        return "Generating IDs"

    if "upload" in log_file:
        processed_count = uploaded_count + skipped_count
        if processed_count == 0:
            return "Uploading (starting...)"
        return f"Uploading ({processed_count:,} / {total:,}, ~{pct(processed_count)}%)"

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
    ssm = boto3.client("ssm", region_name=REGION)

    session_out = ssm_run(ssm, "tmux ls 2>/dev/null | grep wikimedia- || echo NONE")
    if not session_out or session_out == "NONE":
        print("No active wikimedia sessions — skipping Slack post.")
        return

    sessions = [
        line.split(":")[0].strip()
        for line in session_out.splitlines()
        if line.startswith("wikimedia-")
    ]

    rows = []
    for session in sessions:
        partner = session.removeprefix("wikimedia-")
        try:
            phase = get_phase_and_progress(ssm, partner)
        except Exception:
            logging.exception("Failed to get status for %s", session)
            phase = "Unknown (error)"
        rows.append((session, phase))
        print(f"{session}: {phase}")

    token = os.environ["DPLA_SLACK_BOT_TOKEN"]
    post_to_slack(token, rows)
    print("Posted to Slack.")


if __name__ == "__main__":
    main()
