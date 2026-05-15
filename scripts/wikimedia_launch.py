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

from ingest_wikimedia.partners import (
    PARTNER_DIR,
    is_upload_eligible,
    is_wikidata_id,
    parse_session_labels,
    resolve_slug,
    resolve_wikidata_id,
)
from ingest_wikimedia.slack import post_message
from ingest_wikimedia.ssm import REGION, ssm_run

# Each ingest session peaks at ~300–500 MB; 30% of 7.6 GB leaves headroom for 4–5 concurrent sessions.
MEMORY_HEADROOM_PCT = 30


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
    # Dedup by full target string so the same hub may appear with different institutions
    # (e.g. two QIDs that both resolve into the same hub but different institutions).
    seen_target_strs: set[str] = set()
    seen_canonicals: dict[str, None] = {}  # insertion-ordered; for conflict detection
    seen_session_labels: dict[
        str, None
    ] = {}  # insertion-ordered; drives session naming
    targets: list[tuple[str, str | None, str]] = []

    def _add_target(canonical: str, institution: str | None) -> None:
        if institution is not None:
            institution = institution.strip()
            if not institution:
                _slack_fail(
                    response_url,
                    f"Target '{canonical}|' has an empty institution name.",
                )
        if canonical != "nara":
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
        target_str = (
            f"{canonical}|{institution}" if institution is not None else canonical
        )
        if target_str in seen_target_strs:
            _slack_fail(
                response_url,
                f"Target '{target_str}' appears more than once in the target list.",
            )
        seen_target_strs.add(target_str)
        seen_canonicals[canonical] = None
        inst_label = (
            re.sub(r"[^a-z0-9-]", "", institution.lower().replace(" ", "-"))
            if institution is not None
            else None
        )
        if institution is not None and not inst_label:
            _slack_fail(
                response_url,
                f"Target '{canonical}|{institution}' normalizes to an empty institution slug.",
            )
        label = f"{canonical}+{inst_label}" if inst_label is not None else canonical
        seen_session_labels[label] = None
        targets.append((canonical, institution, label))

    for token in target_tokens:
        if is_wikidata_id(token):
            resolved = resolve_wikidata_id(token)
            if not resolved:
                _slack_fail(
                    response_url,
                    f"No hub or institution found for Wikidata ID {token!r} in institutions_v2.json.",
                )
            for canonical, institution in resolved:
                _add_target(canonical, institution)
        elif "|" in token:
            hub_part, institution = token.split("|", 1)
            canonical = resolve_slug(hub_part)
            if canonical is None:
                _slack_fail(response_url, f"Unknown hub: {token!r}")
            _add_target(canonical, institution)
        else:
            canonical = resolve_slug(token)
            if canonical is None:
                _slack_fail(response_url, f"Unknown hub: {token!r}")
            _add_target(canonical, None)

    if not targets:
        _slack_fail(response_url, "No targets specified.")

    # Session name uses + as separator (unambiguous since slugs/institution names use -).
    # Institution-level targets include the hub slug as a prefix so the status script
    # can derive the EC2 directory: "indiana|Indiana State Library" → wikimedia-indiana+indiana-state-library.
    session_name = "wikimedia-" + "+".join(seen_session_labels)

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
        existing_labels = set(parse_session_labels(existing_name[len("wikimedia-") :]))
        overlap: set[str] = set()
        for canonical, institution, label in targets:
            if institution is None:
                # Hub-level request conflicts with any existing session touching this hub
                # (hub-level or any institution-level for the same hub).
                if any(
                    lbl == canonical or lbl.startswith(f"{canonical}+")
                    for lbl in existing_labels
                ):
                    overlap.add(canonical)
            else:
                # Institution-level request conflicts only with:
                #   1. An existing hub-level session for the same hub (would run all institutions)
                #   2. An existing session for the exact same hub+institution
                # Two institution-level sessions for the same hub but different institutions
                # do NOT conflict.
                if canonical in existing_labels or label in existing_labels:
                    overlap.add(canonical)
        if overlap:
            conflicts.append((existing_name, overlap))
    if conflicts:
        if force:
            for existing_name, _ in conflicts:
                print(f"Existing session found: {existing_name}; killing it (--force).")
                try:
                    ssm_run(ssm, f"tmux kill-session -t {shlex.quote(existing_name)}")
                except Exception as e:
                    _slack_fail(
                        response_url, f"⚠️ Failed to kill session `{existing_name}`: {e}"
                    )
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
    out = ""
    try:
        out = ssm_run(ssm, update_cmd)
    except Exception as e:
        _slack_fail(response_url, f"⚠️ Failed to update EC2 code: {e}")
    if "UPDATE_DONE" not in out:
        _slack_fail(
            response_url,
            "⚠️ EC2 code update did not confirm completion. Check the GitHub Actions run for details.",
        )
    print("EC2 code updated.")

    # Build a chained pipeline command for all targets.
    # Each target block: cd into partner dir, run get-ids-es (with optional --institution),
    # downloader, uploader. The cd is required because config.toml is read from CWD.
    steps = [
        "source ~/.bashrc",
        "source /home/ec2-user/ingest-wikimedia/.venv/bin/activate",
    ]
    for canonical, institution, session_label in targets:
        pdir = PARTNER_DIR.get(canonical, canonical)
        base = f"/home/ec2-user/ingest-wikimedia/{pdir}"
        if canonical == "nara" and institution is None:
            get_ids_cmd = f"get-ids-nara > {canonical}.csv"
        else:
            get_ids_cmd = f"get-ids-es {canonical}"
            if institution is not None:
                get_ids_cmd += f" --institution {shlex.quote(institution)}"
            get_ids_cmd += f" > {canonical}.csv"
        steps += [
            f"cd {base}",
            f"export WIKIMEDIA_SESSION_LABEL={shlex.quote(session_label)}",
            get_ids_cmd,
            f"downloader {canonical}.csv {canonical}",
            f"uploader {canonical}.csv {canonical}",
        ]
    pipeline_cmd = " && ".join(steps)

    if slack_token:
        target_labels = [
            f"`{c}|{inst}`" if inst else f"`{c}`" for c, inst, _ in targets
        ]
        try:
            post_message(
                slack_token,
                f"▶ Launching `{session_name}` pipeline: {', '.join(target_labels)}"
                " (ID generation → download → upload).",
            )
        except Exception as e:
            logging.warning("Slack notification failed: %s", e)

    print(f"Launching {session_name} pipeline...")
    # Use double quotes around the pipeline so single-quoted institution names inside are preserved.
    tmux_cmd = (
        f"tmux new-session -d -s {shlex.quote(session_name)} -c /home/ec2-user/ingest-wikimedia/"
        f' "{pipeline_cmd}"'
    )
    try:
        ssm_run(ssm, tmux_cmd)
    except Exception as e:
        _slack_fail(
            response_url, f"⚠️ Failed to launch tmux session `{session_name}`: {e}"
        )

    print("Verifying session started...")
    result = ""
    try:
        result = ssm_run(
            ssm,
            f"tmux ls 2>/dev/null | grep -E '^{re.escape(session_name)}(:|$)' || echo NONE",
        )
    except Exception as e:
        _slack_fail(
            response_url, f"⚠️ Failed to verify session `{session_name}` started: {e}"
        )
    if session_name not in result:
        _slack_fail(
            response_url,
            f"⚠️ `{session_name}` failed to start — tmux session not found after launch."
            " Check the GitHub Actions run for details.",
        )
    print(f"Session {session_name} confirmed running.")


if __name__ == "__main__":
    main()
