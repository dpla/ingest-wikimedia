#!/usr/bin/env python3
"""Launch a Wikimedia upload pipeline session on EC2 for a partner hub.

Runs as a GitHub Actions workflow step triggered by workflow_dispatch or the
/wikimedia-upload Slack slash command via Lambda. Updates EC2 code, checks for
a conflicting session, launches the full pipeline in tmux, and posts a Slack
confirmation to #tech-alerts on success.

Environment variables:
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY  — IAM credentials with ssm:SendCommand
  DPLA_SLACK_BOT_TOKEN                       — optional; skips Slack post if absent
"""

import argparse
import json
import logging
import os
import sys
import time

import boto3
import requests

from ingest_wikimedia.dpla import DPLA_PARTNERS

INSTANCE_ID = "i-033eff6c8c168f999"
REGION = "us-east-1"
SLACK_CHANNEL = "C02HEU2L3"
SLACK_API_URL = "https://slack.com/api/chat.postMessage"
SSM_POLL_INTERVAL = 5
SSM_MAX_POLLS = 60  # 5 minutes

# NARA requires a separate process and is excluded from automated launch
VALID_PARTNERS = frozenset(DPLA_PARTNERS) - {"nara"}

# Partners whose EC2 directory name differs from their partner key
PARTNER_DIR = {
    "si": "smithsonian",
}


def ssm_run(client, cmd: str) -> str:
    resp = client.send_command(
        InstanceIds=[INSTANCE_ID],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [f"sudo -u ec2-user bash -c {json.dumps(cmd)}"]},
    )
    cmd_id = resp["Command"]["CommandId"]
    for attempt in range(SSM_MAX_POLLS):
        if attempt > 0:
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


def post_to_slack(token: str, text: str) -> None:
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--partner", required=True)
    parser.add_argument("--force", default="false")
    args = parser.parse_args()

    partner = args.partner.strip().lower()
    force = args.force.lower() == "true"

    if partner not in VALID_PARTNERS:
        print(
            f"Unknown partner: {partner}. Valid: {', '.join(sorted(VALID_PARTNERS))}",
            file=sys.stderr,
        )
        sys.exit(1)

    pdir = PARTNER_DIR.get(partner, partner)
    base = f"/home/ec2-user/ingest-wikimedia/{pdir}"
    slack_token = (os.environ.get("DPLA_SLACK_BOT_TOKEN") or "").strip()
    ssm = boto3.client("ssm", region_name=REGION)

    print("Updating EC2 code...")
    update_cmd = (
        "cd /tmp && rm -rf ingest-wikimedia-update && "
        "git clone --depth 1 https://github.com/dpla/ingest-wikimedia.git ingest-wikimedia-update && "
        "cp -r ingest-wikimedia-update/ingest_wikimedia/* /home/ec2-user/ingest-wikimedia/ingest_wikimedia/ && "
        "cp -r ingest-wikimedia-update/tools/* /home/ec2-user/ingest-wikimedia/tools/ && "
        "cp ingest-wikimedia-update/pyproject.toml /home/ec2-user/ingest-wikimedia/pyproject.toml && "
        "cp ingest-wikimedia-update/uv.lock /home/ec2-user/ingest-wikimedia/uv.lock && "
        "/home/ec2-user/.local/bin/uv sync --project /home/ec2-user/ingest-wikimedia && echo UPDATE_DONE"
    )
    out = ssm_run(ssm, update_cmd)
    if "UPDATE_DONE" not in out:
        print(f"EC2 update did not confirm completion. Output: {out}", file=sys.stderr)
        sys.exit(1)
    print("EC2 code updated.")

    print(f"Checking for existing wikimedia-{partner} session...")
    existing = ssm_run(
        ssm, f"tmux ls 2>/dev/null | grep -E '^wikimedia-{partner}(:|$)' || echo NONE"
    )
    if f"wikimedia-{partner}" in existing:
        if force:
            print("Existing session found; killing it (--force).")
            ssm_run(ssm, f"tmux kill-session -t wikimedia-{partner}")
        else:
            print(
                f"Session wikimedia-{partner} is already running. "
                "Set force=true to override.",
                file=sys.stderr,
            )
            sys.exit(1)

    print(f"Launching wikimedia-{partner} pipeline...")
    pipeline_cmd = (
        f"source ~/.bashrc && "
        f"source /home/ec2-user/ingest-wikimedia/.venv/bin/activate && "
        f"get-ids-es {partner} > {partner}.csv && "
        f"downloader {partner}.csv {partner} && "
        f"uploader {partner}.csv {partner}"
    )
    tmux_cmd = f"tmux new-session -d -s wikimedia-{partner} -c {base}/ '{pipeline_cmd}'"
    ssm_run(ssm, tmux_cmd)

    print("Verifying session started...")
    result = ssm_run(
        ssm, f"tmux ls 2>/dev/null | grep -E '^wikimedia-{partner}(:|$)' || echo NONE"
    )
    if f"wikimedia-{partner}" not in result:
        print(f"Session wikimedia-{partner} did not start.", file=sys.stderr)
        sys.exit(1)
    print(f"Session wikimedia-{partner} confirmed running.")

    if slack_token:
        try:
            post_to_slack(
                slack_token,
                f"▶ Started `wikimedia-{partner}` pipeline (ID generation → download → upload).",
            )
        except Exception as e:
            logging.warning("Slack notification failed: %s", e)


if __name__ == "__main__":
    main()
