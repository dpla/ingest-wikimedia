#!/usr/bin/env python3
"""Launch a Wikimedia upload pipeline session on EC2 for one or more partner hubs.

Runs as a GitHub Actions workflow step triggered by workflow_dispatch or the
/wikimedia-upload Slack slash command via Lambda. Updates EC2 code, checks for
conflicting sessions, launches the full pipeline in a single tmux session (with
all targets chained via &&), and posts a Slack confirmation to #tech-alerts.

Each target in --partner is either a hub slug ("bpl") or a hub|institution pair
("indiana|Indiana State Library"). Multiple targets run sequentially in one tmux
session; if any step fails the chain stops.

Environment variables:
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY  — IAM credentials with ssm:SendCommand
  DPLA_SLACK_BOT_TOKEN                       — optional; skips Slack post if absent
"""

import argparse
import logging
import os
import re
import shlex
import sys

import boto3
import requests

from ingest_wikimedia.partners import is_upload_eligible, resolve_slug
from ingest_wikimedia.slack import post_message
from ingest_wikimedia.ssm import REGION, ssm_run

# Each ingest session peaks at ~300–500 MB; 30% of 7.6 GB leaves headroom for 4–5 concurrent sessions.
MEMORY_HEADROOM_PCT = 30

# Partners whose EC2 directory name differs from their partner key
PARTNER_DIR = {
    "si": "smithsonian",
}


def _slack_fail(response_url: str, msg: str) -> None:
    """Print msg to stderr, post ephemeral reply to response_url if set, then exit 1."""
    print(msg, file=sys.stderr)
    if response_url:
        try:
            resp = requests.post(
                response_url,
                json={"response_type": "ephemeral", "text": msg},
                timeout=5,
            )
            resp.raise_for_status()
        except Exception as e:
            logging.warning("Failed to post to Slack response_url: %s", e)
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--partner", required=True)
    parser.add_argument("--force", default="false")
    parser.add_argument("--response-url", default="")
    args = parser.parse_args()

    force = args.force.lower() == "true"
    raw_url = args.response_url.strip()
    # Only accept genuine Slack response_url values — reject arbitrary POST targets.
    response_url = (
        raw_url if raw_url.startswith("https://hooks.slack.com/commands/") else ""
    )
    if raw_url and not response_url:
        print(f"Ignoring invalid response_url: {raw_url!r}", file=sys.stderr)

    # --partner may be a shlex-encoded list: 'bpl "indiana|Indiana State Library"'
    try:
        target_tokens = shlex.split(args.partner)
    except ValueError as e:
        _slack_fail(response_url, f"Could not parse --partner: {e}")

    # Validate each target and build (canonical, institution_or_None) pairs.
    seen_hubs: set[str] = set()
    targets: list[tuple[str, str | None]] = []
    for token in target_tokens:
        if "|" in token:
            hub_part, institution = token.split("|", 1)
            canonical = resolve_slug(hub_part)
        else:
            canonical = resolve_slug(token)
            institution = None

        if canonical is None:
            _slack_fail(response_url, f"Unknown hub: {token!r}")
        if canonical == "nara":
            _slack_fail(
                response_url,
                "NARA requires a separate process and cannot be launched here.",
            )
        try:
            eligible = is_upload_eligible(canonical)
        except Exception as e:
            _slack_fail(
                response_url,
                f"Failed to check upload eligibility for '{canonical}': {e}",
            )
        if not eligible:
            _slack_fail(
                response_url,
                f"Hub '{canonical}' is not upload-eligible per institutions_v2.json.",
            )
        if canonical in seen_hubs:
            _slack_fail(
                response_url,
                f"Hub '{canonical}' appears more than once in the target list.",
            )
        seen_hubs.add(canonical)
        targets.append((canonical, institution))

    if not targets:
        _slack_fail(response_url, "No targets specified.")

    # Session name uses + as separator (unambiguous since slugs use -).
    session_name = "wikimedia-" + "+".join(c for c, _ in targets)

    slack_token = (os.environ.get("DPLA_SLACK_BOT_TOKEN") or "").strip()
    ssm = boto3.client("ssm", region_name=REGION)

    print("Checking instance memory...")
    try:
        mem_out = ssm_run(ssm, "free -m | awk 'NR==2{print $2, $7}'")
    except Exception as e:
        _slack_fail(response_url, f"⚠️ Failed to check instance memory: {e}")
    parts = mem_out.split()
    if len(parts) != 2:
        _slack_fail(response_url, f"⚠️ Unexpected memory output: {mem_out!r}")
    try:
        total_mb, available_mb = int(parts[0]), int(parts[1])
        pct_available = available_mb * 100 // total_mb
    except (ValueError, ZeroDivisionError) as e:
        _slack_fail(response_url, f"⚠️ Could not parse memory output ({mem_out!r}): {e}")
    print(f"Memory: {pct_available}% available ({available_mb} MB of {total_mb} MB).")
    if pct_available < MEMORY_HEADROOM_PCT:
        _slack_fail(
            response_url,
            f"⚠️ Cannot launch `{session_name}`: only {pct_available}% memory available"
            f" ({available_mb} MB of {total_mb} MB). Threshold is {MEMORY_HEADROOM_PCT}%.",
        )

    # Check for any existing session that includes one of the requested hubs.
    print(f"Checking for existing sessions that overlap with {session_name}...")
    try:
        tmux_list = ssm_run(ssm, "tmux ls 2>/dev/null || true")
    except Exception as e:
        _slack_fail(response_url, f"⚠️ Failed to list tmux sessions: {e}")
    conflicts = []
    for line in tmux_list.splitlines():
        existing_name = line.split(":")[0].strip()
        if not existing_name.startswith("wikimedia-"):
            continue
        existing_hubs = set(existing_name[len("wikimedia-") :].split("+"))
        overlap = seen_hubs & existing_hubs
        if overlap:
            conflicts.append((existing_name, overlap))
    if conflicts:
        if force:
            for existing_name, _ in conflicts:
                print(f"Existing session found: {existing_name}; killing it (--force).")
                ssm_run(ssm, f"tmux kill-session -t {shlex.quote(existing_name)}")
        else:
            conflict_names = ", ".join(f"`{n}`" for n, _ in conflicts)
            _slack_fail(
                response_url,
                f"⚠️ Session(s) already running with overlapping hubs: {conflict_names}."
                " To restart, trigger the launch workflow from GitHub Actions with force=true.",
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

    # Build a chained pipeline command for all targets.
    # Each target block: cd into partner dir, run get-ids-es (with optional --institution),
    # downloader, uploader. The cd is required because config.toml is read from CWD.
    steps = [
        "source ~/.bashrc",
        "source /home/ec2-user/ingest-wikimedia/.venv/bin/activate",
    ]
    for canonical, institution in targets:
        pdir = PARTNER_DIR.get(canonical, canonical)
        base = f"/home/ec2-user/ingest-wikimedia/{pdir}"
        get_ids_cmd = f"get-ids-es {canonical}"
        if institution is not None:
            get_ids_cmd += f" --institution {shlex.quote(institution)}"
        get_ids_cmd += f" > {canonical}.csv"
        steps += [
            f"cd {base}",
            get_ids_cmd,
            f"downloader {canonical}.csv {canonical}",
            f"uploader {canonical}.csv {canonical}",
        ]
    pipeline_cmd = " && ".join(steps)

    print(f"Launching {session_name} pipeline...")
    # Use double quotes around the pipeline so single-quoted institution names inside are preserved.
    tmux_cmd = (
        f"tmux new-session -d -s {shlex.quote(session_name)} -c /home/ec2-user/ingest-wikimedia/"
        f' "{pipeline_cmd}"'
    )
    ssm_run(ssm, tmux_cmd)

    print("Verifying session started...")
    result = ssm_run(
        ssm,
        f"tmux ls 2>/dev/null | grep -E '^{re.escape(session_name)}(:|$)' || echo NONE",
    )
    if session_name not in result:
        _slack_fail(
            response_url,
            f"⚠️ `{session_name}` failed to start — tmux session not found after launch."
            " Check the GitHub Actions run for details.",
        )
    print(f"Session {session_name} confirmed running.")

    if slack_token:
        target_labels = [f"`{c}|{inst}`" if inst else f"`{c}`" for c, inst in targets]
        try:
            post_message(
                slack_token,
                f"▶ Started `{session_name}` pipeline: {', '.join(target_labels)}"
                " (ID generation → download → upload).",
            )
        except Exception as e:
            logging.warning("Slack notification failed: %s", e)


if __name__ == "__main__":
    main()
