#!/usr/bin/env python3
"""Run retry passes for failed Wikimedia upload and download items.

Triggered by the /wikimedia-upload retry <days> [<partner>] Slack command.
Scans EC2 logs for transient failures, then launches downloader+uploader (for
download failures) or uploader only (for upload failures) for each affected partner.

Environment variables:
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY  — IAM credentials with ssm:SendCommand
  DPLA_SLACK_BOT_TOKEN                       — optional; skips Slack post if absent
"""

import argparse
import logging
import os
import shlex
import sys
from typing import NoReturn

import boto3
import requests

from ingest_wikimedia.partners import PARTNER_DIR, resolve_slug
from ingest_wikimedia.slack import post_message
from ingest_wikimedia.ssm import REGION, ssm_run

RETRY_DIR = "/home/ec2-user/ingest-wikimedia/retry"
MEMORY_HEADROOM_PCT = 30


def _slack_fail(response_url: str, msg: str) -> NoReturn:
    """Print msg to stderr, post ephemeral reply to response_url if set, then exit 1."""
    print(msg, file=sys.stderr)
    if response_url:
        try:
            requests.post(
                response_url,
                json={"response_type": "ephemeral", "text": msg},
                timeout=5,
            ).raise_for_status()
        except Exception as e:
            logging.warning("Failed to post to Slack response_url: %s", e)
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", required=True, type=int)
    parser.add_argument("--partner", default="")
    parser.add_argument("--response-url", default="")
    args = parser.parse_args()

    days = args.days
    if days <= 0:
        print("--days must be a positive integer.", file=sys.stderr)
        sys.exit(1)
    partner = args.partner.strip()
    raw_url = args.response_url.strip()
    # Only accept genuine Slack response_url values — reject arbitrary POST targets.
    response_url = (
        raw_url if raw_url.startswith("https://hooks.slack.com/commands/") else ""
    )
    if raw_url and not response_url:
        print("Ignoring invalid response_url (not a Slack hooks URL).", file=sys.stderr)

    slack_token = (os.environ.get("DPLA_SLACK_BOT_TOKEN") or "").strip()
    ssm = boto3.client("ssm", region_name=REGION)

    # Update EC2 code first so get-ids-retry and the pipeline run the latest version.
    # Pin to GITHUB_SHA when available (always set in GitHub Actions).
    print("Updating EC2 code...")
    github_sha = (os.environ.get("GITHUB_SHA") or "").strip()
    pin_step = (
        f"cd /tmp/ingest-wikimedia-update && "
        f"git fetch --depth 1 origin {shlex.quote(github_sha)} && "
        f"git checkout --detach {shlex.quote(github_sha)} && "
        "cd /tmp && "
        if github_sha
        else ""
    )
    update_cmd = (
        "cd /tmp && rm -rf ingest-wikimedia-update && "
        "git clone --depth 1 https://github.com/dpla/ingest-wikimedia.git ingest-wikimedia-update && "
        + pin_step
        + "cp -r ingest-wikimedia-update/ingest_wikimedia/* /home/ec2-user/ingest-wikimedia/ingest_wikimedia/ && "
        "cp -r ingest-wikimedia-update/tools/* /home/ec2-user/ingest-wikimedia/tools/ && "
        "cp ingest-wikimedia-update/pyproject.toml /home/ec2-user/ingest-wikimedia/pyproject.toml && "
        "cp ingest-wikimedia-update/uv.lock /home/ec2-user/ingest-wikimedia/uv.lock && "
        "/home/ec2-user/.local/bin/uv sync --project /home/ec2-user/ingest-wikimedia && echo UPDATE_DONE"
    )
    update_out = ""
    try:
        update_out = ssm_run(ssm, update_cmd)
    except Exception as e:
        _slack_fail(response_url, f"⚠️ Failed to update EC2 code: {e}")
    if "UPDATE_DONE" not in update_out:
        _slack_fail(
            response_url,
            "⚠️ EC2 code update did not confirm completion. Check the GitHub Actions run for details.",
        )
    print("EC2 code updated.")

    # Scan logs for retryable failures. Pass the EC2 directory name rather than the
    # canonical slug so get-ids-retry can find the logs dir (e.g. "smithsonian" for "si").
    partner_desc = f" for `{partner}`" if partner else ""
    print(
        f"Scanning logs for retryable failures in the last {days} day(s){partner_desc}..."
    )
    scan_cmd = (
        f"mkdir -p {shlex.quote(RETRY_DIR)} && "
        "source /home/ec2-user/ingest-wikimedia/.venv/bin/activate && "
        "cd /home/ec2-user/ingest-wikimedia && "
        f"get-ids-retry {days}"
    )
    if partner:
        # PARTNER_DIR maps canonical slugs to EC2 directory names (e.g. si → smithsonian).
        # get-ids-retry discovers partners by directory name, so we must pass the dir name.
        dir_name = PARTNER_DIR.get(partner, partner)
        scan_cmd += f" --partner {shlex.quote(dir_name)}"
    scan_cmd += f" --output-dir {shlex.quote(RETRY_DIR)}"
    scan_out = ""
    try:
        scan_out = ssm_run(ssm, scan_cmd)
    except Exception as e:
        _slack_fail(response_url, f"⚠️ Failed to scan logs for retryable failures: {e}")

    print(f"Scan output:\n{scan_out}")

    if "No retryable failures found" in scan_out:
        msg = (
            f"🔍 No retryable failures found in the last {days} day"
            f"{'s' if days != 1 else ''}{partner_desc}."
        )
        if slack_token:
            try:
                post_message(slack_token, msg)
            except Exception as e:
                logging.warning("Slack notification failed: %s", e)
        print(msg)
        sys.exit(0)

    # List retry CSVs and check memory in a single SSM round-trip.
    print("Checking instance memory...")
    combined_out = ""
    try:
        combined_out = ssm_run(
            ssm,
            f"find {shlex.quote(RETRY_DIR)} -name '*-retry.csv' | sort && "
            "echo __MEM_CHECK__ && "
            "free -m | awk 'NR==2{print $2, $7}'",
        )
    except Exception as e:
        _slack_fail(response_url, f"⚠️ Failed to list retry CSVs and check memory: {e}")
    if "__MEM_CHECK__" not in combined_out:
        _slack_fail(response_url, "⚠️ Unexpected output from find + memory check.")
    find_part, _, mem_out = combined_out.partition("__MEM_CHECK__")

    # Parse retry CSVs into a per-hub map: slug → {retry_type: csv_path}.
    # Filenames use EC2 directory names (e.g. "smithsonian-upload-retry.csv") which may
    # differ from canonical slugs (e.g. "si"); resolve via alias table.
    # Insertion order is preserved (find | sort), so hubs run in a stable sequence.
    hub_csvs: dict[str, dict[str, str]] = {}
    for line in find_part.splitlines():
        csv_path = line.strip()
        if not csv_path:
            continue
        filename = csv_path.rsplit("/", 1)[-1]
        if filename.endswith("-download-retry.csv"):
            csv_partner_dir = filename[: -len("-download-retry.csv")]
            retry_type = "download"
        elif filename.endswith("-upload-retry.csv"):
            csv_partner_dir = filename[: -len("-upload-retry.csv")]
            retry_type = "upload"
        else:
            logging.warning("Unexpected CSV filename: %s", filename)
            continue
        slug = resolve_slug(csv_partner_dir)
        if slug is None:
            logging.warning(
                "Unknown partner in retry CSV filename: %s", csv_partner_dir
            )
            continue
        hub_csvs.setdefault(slug, {})[retry_type] = csv_path

    if not hub_csvs:
        _slack_fail(
            response_url,
            "⚠️ Log scan found failures but no retry CSVs were created.",
        )

    mem_parts = mem_out.split()
    if len(mem_parts) != 2:
        _slack_fail(response_url, f"⚠️ Unexpected memory output: {mem_out!r}")
    try:
        total_mb, available_mb = int(mem_parts[0]), int(mem_parts[1])
        pct_available = available_mb * 100 // total_mb
    except (ValueError, ZeroDivisionError) as e:
        _slack_fail(response_url, f"⚠️ Could not parse memory output ({mem_out!r}): {e}")
    print(f"Memory: {pct_available}% available ({available_mb} MB of {total_mb} MB).")
    if pct_available < MEMORY_HEADROOM_PCT:
        _slack_fail(
            response_url,
            f"⚠️ Only {pct_available}% memory available"
            f" ({available_mb} MB of {total_mb} MB)."
            f" Threshold is {MEMORY_HEADROOM_PCT}%.",
        )

    # Build tmux pipeline command. Each target block exports WIKIMEDIA_SESSION_LABEL,
    # runs the appropriate commands in the partner directory, and posts a failure
    # notification if the block exits non-zero before continuing to the next target.
    session_name = f"wikimedia-retry-{days}d"
    if partner:
        session_name += f"-{partner}"

    # `rc=$?` captures the failing step's exit code so the Slack message can
    # decode signals like 137 (SIGKILL / probable OOM) and 143 (SIGTERM).
    notify_fail_cmd = (
        "rc=$?; WIKIMEDIA_LAST_EXIT=$rc python3 -c "
        "'from ingest_wikimedia.slack import notify_pipeline_fail; notify_pipeline_fail()'"
    )
    setup = " && ".join(
        [
            "source ~/.bashrc",
            "source /home/ec2-user/ingest-wikimedia/.venv/bin/activate",
        ]
    )

    # Build one tmux block per hub. Download and upload failures for the same hub
    # are combined: downloader runs on the download CSV, then uploader runs on each
    # CSV in turn. Running the uploader on the download CSV then the upload CSV is
    # safe: the uploader checks each file's SHA-1 against Commons before uploading
    # and skips files already present, so re-running on overlapping IDs is harmless.
    target_blocks = []
    for slug, type_csvs in hub_csvs.items():
        pdir = PARTNER_DIR.get(slug, slug)
        base = shlex.quote(f"/home/ec2-user/ingest-wikimedia/{pdir}")
        session_label = f"retry-{slug}"
        # WIKIMEDIA_PARTNER_DIR is read by notify_pipeline_fail() to locate the
        # most recent log for this target and include a tail + counts in the
        # Slack failure message.
        label_export = (
            f"export WIKIMEDIA_SESSION_LABEL={shlex.quote(session_label)}; "
            f"export WIKIMEDIA_PARTNER_DIR={base}; "
            "unset WIKIMEDIA_SINGLE_ITEM"
        )
        download_csv = type_csvs.get("download")
        upload_csv = type_csvs.get("upload")
        steps = [f"cd {base}"]
        if download_csv:
            steps.append(f"downloader {shlex.quote(download_csv)} {slug}")
            steps.append(f"uploader {shlex.quote(download_csv)} {slug}")
        if upload_csv:
            steps.append(f"uploader {shlex.quote(upload_csv)} {slug}")
        target_steps = " && ".join(steps)
        target_blocks.append(
            f"{label_export}; {{ {target_steps}; }}"
            f" || {{ {notify_fail_cmd} >/dev/null 2>&1 || true; }}"
        )

    pipeline_cmd = f"{setup} && {{ {'; '.join(target_blocks)}; }}"

    # Post launch notification before starting the session.
    if slack_token:
        target_summary = ", ".join(f"`{slug}`" for slug in hub_csvs)
        msg = (
            f"🔁 Launching `{session_name}`: {target_summary}"
            f" ({days} day{'s' if days != 1 else ''} of log history)."
        )
        try:
            post_message(slack_token, msg)
        except Exception as e:
            logging.warning("Slack notification failed: %s", e)

    # Kill any existing session with the same name (retries are idempotent) and launch.
    print(f"Launching {session_name}...")
    tmux_cmd = (
        f"tmux kill-session -t {shlex.quote(session_name)} 2>/dev/null || true; "
        f"tmux new-session -d -s {shlex.quote(session_name)}"
        " -c /home/ec2-user/ingest-wikimedia/"
        f' "{pipeline_cmd}" && echo SESSION_STARTED'
    )
    tmux_out = ""
    try:
        tmux_out = ssm_run(ssm, tmux_cmd)
    except Exception as e:
        _slack_fail(
            response_url, f"⚠️ Failed to launch tmux session `{session_name}`: {e}"
        )
    if "SESSION_STARTED" not in tmux_out:
        _slack_fail(
            response_url,
            f"⚠️ `{session_name}` failed to start — tmux could not create session."
            " Check the GitHub Actions run for details.",
        )
    print(f"Session {session_name} confirmed running.")


if __name__ == "__main__":
    main()
