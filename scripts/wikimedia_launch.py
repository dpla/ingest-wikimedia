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
import logging
import os
import sys

import boto3
import requests

from ingest_wikimedia.partners import is_upload_eligible, resolve_slug
from ingest_wikimedia.ssm import REGION, ssm_run

SLACK_CHANNEL = "C02HEU2L3"
SLACK_API_URL = "https://slack.com/api/chat.postMessage"
# Each ingest session peaks at ~300–500 MB; 30% of 7.6 GB leaves headroom for 4–5 concurrent sessions.
MEMORY_HEADROOM_PCT = 30

# Partners whose EC2 directory name differs from their partner key
PARTNER_DIR = {
    "si": "smithsonian",
}


def _slack_fail(slack_token: str, msg: str) -> None:
    """Print msg to stderr, optionally post to Slack, then exit 1."""
    print(msg, file=sys.stderr)
    if slack_token:
        try:
            post_to_slack(slack_token, msg)
        except Exception as e:
            logging.warning("Slack notification failed: %s", e)
    sys.exit(1)


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

    force = args.force.lower() == "true"

    canonical = resolve_slug(args.partner)
    if canonical is None:
        print(f"Unknown hub: {args.partner.strip()}", file=sys.stderr)
        sys.exit(1)
    if canonical == "nara":
        print(
            "NARA requires a separate process and cannot be launched here.",
            file=sys.stderr,
        )
        sys.exit(1)
    try:
        eligible = is_upload_eligible(canonical)
    except Exception as e:
        print(
            f"Failed to check upload eligibility for '{canonical}': {e}",
            file=sys.stderr,
        )
        sys.exit(1)
    if not eligible:
        print(
            f"Hub '{canonical}' is not upload-eligible per institutions_v2.json.",
            file=sys.stderr,
        )
        sys.exit(1)

    pdir = PARTNER_DIR.get(canonical, canonical)
    base = f"/home/ec2-user/ingest-wikimedia/{pdir}"
    slack_token = (os.environ.get("DPLA_SLACK_BOT_TOKEN") or "").strip()
    ssm = boto3.client("ssm", region_name=REGION)

    print("Checking instance memory...")
    mem_out = ssm_run(ssm, "free -m | awk 'NR==2{print $2, $7}'")
    parts = mem_out.split()
    if len(parts) != 2:
        print(f"Unexpected memory output: {mem_out!r}", file=sys.stderr)
        sys.exit(1)
    try:
        total_mb, available_mb = int(parts[0]), int(parts[1])
        pct_available = available_mb * 100 // total_mb
    except (ValueError, ZeroDivisionError) as e:
        print(f"Could not parse memory output ({mem_out!r}): {e}", file=sys.stderr)
        sys.exit(1)
    print(f"Memory: {pct_available}% available ({available_mb} MB of {total_mb} MB).")
    if pct_available < MEMORY_HEADROOM_PCT:
        _slack_fail(
            slack_token,
            f"⚠️ Cannot launch `wikimedia-{canonical}`: only {pct_available}% memory available"
            f" ({available_mb} MB of {total_mb} MB). Threshold is {MEMORY_HEADROOM_PCT}%.",
        )

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

    print(f"Checking for existing wikimedia-{canonical} session...")
    existing = ssm_run(
        ssm, f"tmux ls 2>/dev/null | grep -E '^wikimedia-{canonical}(:|$)' || echo NONE"
    )
    if f"wikimedia-{canonical}" in existing:
        if force:
            print("Existing session found; killing it (--force).")
            ssm_run(ssm, f"tmux kill-session -t wikimedia-{canonical}")
        else:
            _slack_fail(
                slack_token,
                f"⚠️ `wikimedia-{canonical}` is already running."
                " To restart it, trigger the launch workflow from GitHub Actions with force=true.",
            )

    print(f"Launching wikimedia-{canonical} pipeline...")
    pipeline_cmd = (
        f"source ~/.bashrc && "
        f"source /home/ec2-user/ingest-wikimedia/.venv/bin/activate && "
        f"get-ids-es {canonical} > {canonical}.csv && "
        f"downloader {canonical}.csv {canonical} && "
        f"uploader {canonical}.csv {canonical}"
    )
    tmux_cmd = (
        f"tmux new-session -d -s wikimedia-{canonical} -c {base}/ '{pipeline_cmd}'"
    )
    ssm_run(ssm, tmux_cmd)

    print("Verifying session started...")
    result = ssm_run(
        ssm, f"tmux ls 2>/dev/null | grep -E '^wikimedia-{canonical}(:|$)' || echo NONE"
    )
    if f"wikimedia-{canonical}" not in result:
        _slack_fail(
            slack_token,
            f"⚠️ `wikimedia-{canonical}` failed to start — tmux session not found after launch."
            " Check the GitHub Actions run for details.",
        )
    print(f"Session wikimedia-{canonical} confirmed running.")

    if slack_token:
        try:
            post_to_slack(
                slack_token,
                f"▶ Started `wikimedia-{canonical}` pipeline (ID generation → download → upload).",
            )
        except Exception as e:
            logging.warning("Slack notification failed: %s", e)


if __name__ == "__main__":
    main()
