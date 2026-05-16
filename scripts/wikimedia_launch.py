#!/usr/bin/env python3
"""Launch a Wikimedia upload pipeline session on EC2 for one or more partner hubs.

Runs as a GitHub Actions workflow step triggered by workflow_dispatch or the
/wikimedia-upload Slack slash command via Lambda. Updates EC2 code, checks for
conflicting sessions, launches the full pipeline in a single tmux session (with
all targets run sequentially), and posts a Slack confirmation to #tech-alerts.

Each target in --partner is either a hub slug ("bpl") or a hub|institution pair
("indiana|Indiana State Library"). Multiple targets run sequentially in one tmux
session; a failing target posts a Slack error and continues with the next target.

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
from typing import NoReturn

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


def _slack_fail(response_url: str, msg: str) -> NoReturn:
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
    # Per-target validation warnings; populated by _add_target and the conflict
    # check. If some targets are skipped but others remain valid, a summary is
    # posted to the caller before launch rather than aborting the whole run.
    skipped_warnings: list[str] = []

    def _add_target(canonical: str, institution: str | None) -> None:
        if institution is not None:
            institution = institution.strip()
            if not institution:
                skipped_warnings.append(f"'{canonical}|': empty institution name.")
                return
        if canonical != "nara" and institution is None:
            # Hub-level target: check that any institution in the hub is eligible.
            # Institution-level eligibility is not checked here — get-ids-es enforces
            # it at runtime, and the per-target failure handler (notify_pipeline_fail)
            # catches any rejection and continues with remaining targets.
            # NARA hub-level runs use get-ids-nara which filters eligibility itself.
            try:
                eligible = is_upload_eligible(canonical)
            except Exception as e:
                skipped_warnings.append(
                    f"'{canonical}': failed to check upload eligibility: {e}"
                )
                return
            if not eligible:
                skipped_warnings.append(
                    f"'{canonical}': not upload-eligible per institutions_v2.json."
                )
                return
        target_str = (
            f"{canonical}|{institution}" if institution is not None else canonical
        )
        if target_str in seen_target_strs:
            skipped_warnings.append(f"'{target_str}': duplicate target.")
            return
        seen_target_strs.add(target_str)
        seen_canonicals[canonical] = None
        inst_label = (
            re.sub(r"[^a-z0-9-]", "", institution.lower().replace(" ", "-"))
            if institution is not None
            else None
        )
        if institution is not None and not inst_label:
            skipped_warnings.append(
                f"'{canonical}|{institution}': institution name normalizes to an empty slug."
            )
            return
        label = f"{canonical}+{inst_label}" if inst_label is not None else canonical
        if label in seen_session_labels:
            skipped_warnings.append(
                f"'{target_str}': normalizes to the same session label ('{label}') as a previous target."
            )
            return
        seen_session_labels[label] = None
        targets.append((canonical, institution, label))

    for token in target_tokens:
        if is_wikidata_id(token):
            resolved = resolve_wikidata_id(token)
            if not resolved:
                skipped_warnings.append(
                    f"Wikidata ID {token!r}: not found in institutions_v2.json."
                )
                continue
            for canonical, institution in resolved:
                _add_target(canonical, institution)
        elif "|" in token:
            hub_part, institution = token.split("|", 1)
            canonical = resolve_slug(hub_part)
            if canonical is None:
                skipped_warnings.append(f"'{token}': unknown hub '{hub_part}'.")
                continue
            _add_target(canonical, institution)
        else:
            canonical = resolve_slug(token)
            if canonical is None:
                skipped_warnings.append(f"'{token}': unknown hub slug.")
                continue
            _add_target(canonical, None)

    if not targets:
        detail = ("\n• " + "\n• ".join(skipped_warnings)) if skipped_warnings else ""
        _slack_fail(response_url, f"No valid targets to launch.{detail}")

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

    # Check for any existing session that includes one of the requested targets.
    # Maps each requested label to the existing session name(s) it conflicts with.
    print(f"Checking for existing sessions that overlap with {session_name}...")
    try:
        tmux_list = ssm_run(ssm, "tmux ls 2>/dev/null || true")
    except Exception as e:
        _slack_fail(response_url, f"⚠️ Failed to list tmux sessions: {e}")
    label_conflicts: dict[str, list[str]] = {}
    for line in tmux_list.splitlines():
        existing_name = line.split(":")[0].strip()
        if not existing_name.startswith("wikimedia-"):
            continue
        existing_labels = set(parse_session_labels(existing_name[len("wikimedia-") :]))
        for canonical, institution, label in targets:
            if institution is None:
                # Hub-level request conflicts with any existing session touching this hub
                # (hub-level or any institution-level for the same hub).
                conflicts_this = any(
                    lbl == canonical or lbl.startswith(f"{canonical}+")
                    for lbl in existing_labels
                )
            else:
                # Institution-level request conflicts only with:
                #   1. An existing hub-level session for the same hub (would run all institutions)
                #   2. An existing session for the exact same hub+institution
                # Two institution-level sessions for the same hub but different institutions
                # do NOT conflict.
                conflicts_this = (
                    canonical in existing_labels or label in existing_labels
                )
            if conflicts_this:
                label_conflicts.setdefault(label, []).append(existing_name)
    if label_conflicts:
        if force:
            sessions_to_kill = {
                name for names in label_conflicts.values() for name in names
            }
            for existing_name in sessions_to_kill:
                print(f"Existing session found: {existing_name}; killing it (--force).")
                try:
                    ssm_run(ssm, f"tmux kill-session -t {shlex.quote(existing_name)}")
                except Exception as e:
                    _slack_fail(
                        response_url, f"⚠️ Failed to kill session `{existing_name}`: {e}"
                    )
        else:
            for label, existing_names in label_conflicts.items():
                existing_str = ", ".join(f"`{n}`" for n in existing_names)
                skipped_warnings.append(
                    f"`{label}`: conflicts with already-running session(s) {existing_str}."
                    " Use force=true to restart."
                )
            conflicting_labels = set(label_conflicts)
            targets[:] = [
                (c, i, lbl) for c, i, lbl in targets if lbl not in conflicting_labels
            ]
            for lbl in conflicting_labels:
                seen_session_labels.pop(lbl, None)
            if not targets:
                detail = "\n• " + "\n• ".join(skipped_warnings)
                _slack_fail(
                    response_url,
                    f"No valid targets remain — all conflict with running sessions.{detail}",
                )

    if skipped_warnings:
        warning_text = "⚠️ Skipped targets:\n• " + "\n• ".join(skipped_warnings)
        print(warning_text, file=sys.stderr)
        if response_url:
            try:
                requests.post(
                    response_url,
                    json={"response_type": "ephemeral", "text": warning_text},
                    timeout=5,
                ).raise_for_status()
            except Exception as e:
                logging.warning("Failed to post skip warnings to Slack: %s", e)

    # Recompute after conflict filtering — derive from targets to guarantee
    # the label order in the session name matches the surviving target list.
    session_name = "wikimedia-" + "+".join(lbl for _, _, lbl in targets)

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

    # Build the pipeline command for all targets.
    # Setup (sourcing) runs once and gates all targets via &&.
    # Each target block runs its three steps (get-ids, downloader, uploader)
    # chained with &&. A failing block posts a Slack error notification then
    # continues to the next target (|| handler + ; separator).
    # The cd is required because config.toml is read from CWD.
    #
    # notify_fail_cmd uses single-quoted Python inside a double-quoted tmux argument.
    # Single quotes are literal characters inside double-quoted bash strings, so the
    # inner Python code reaches the interpreter verbatim without needing any escaping.
    notify_fail_cmd = (
        "python3 -c "
        "'from ingest_wikimedia.slack import notify_pipeline_fail; notify_pipeline_fail()'"
    )
    setup = " && ".join(
        [
            "source ~/.bashrc",
            "source /home/ec2-user/ingest-wikimedia/.venv/bin/activate",
        ]
    )
    target_blocks = []
    for canonical, institution, session_label in targets:
        pdir = PARTNER_DIR.get(canonical, canonical)
        base = f"/home/ec2-user/ingest-wikimedia/{pdir}"
        # Use a per-target CSV filename so concurrent institution-level sessions
        # for the same hub don't clobber each other's ID lists between the
        # get-ids and downloader/uploader steps.
        csv_file = f"{session_label}.csv"
        if canonical == "nara" and institution is None:
            get_ids_cmd = f"get-ids-nara > {csv_file}"
        else:
            get_ids_cmd = f"get-ids-es {canonical}"
            if institution is not None:
                get_ids_cmd += f" --institution {shlex.quote(institution)}"
            get_ids_cmd += f" > {csv_file}"
        # Export the session label before the group so the failure handler has the
        # correct label even when cd itself fails.
        label_export = f"export WIKIMEDIA_SESSION_LABEL={shlex.quote(session_label)}"
        target_steps = " && ".join(
            [
                f"cd {base}",
                get_ids_cmd,
                f"downloader {csv_file} {canonical}",
                f"uploader {csv_file} {canonical}",
            ]
        )
        target_blocks.append(
            f"{label_export}; {{ {target_steps}; }}"
            f" || {{ {notify_fail_cmd} >/dev/null 2>&1 || true; }}"
        )
    pipeline_cmd = f"{setup} && {{ {'; '.join(target_blocks)}; }}"

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
    # Use double quotes around the pipeline so single-quoted institution names inside are
    # preserved when the outer shell (bash -c) processes the tmux command. The sentinel
    # echo runs immediately after tmux creates the detached session — before any pipeline
    # commands execute — so it is not subject to a race with fast-completing pipelines.
    tmux_cmd = (
        f"tmux new-session -d -s {shlex.quote(session_name)} -c /home/ec2-user/ingest-wikimedia/"
        f' "{pipeline_cmd}" && echo SESSION_STARTED'
    )
    out = ""
    try:
        out = ssm_run(ssm, tmux_cmd)
    except Exception as e:
        _slack_fail(
            response_url, f"⚠️ Failed to launch tmux session `{session_name}`: {e}"
        )
    if "SESSION_STARTED" not in out:
        _slack_fail(
            response_url,
            f"⚠️ `{session_name}` failed to start — tmux could not create session."
            " Check the GitHub Actions run for details.",
        )
    print(f"Session {session_name} confirmed running.")


if __name__ == "__main__":
    main()
