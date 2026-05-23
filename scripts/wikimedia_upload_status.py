#!/usr/bin/env python3
"""Check Wikimedia upload session status on EC2 and post a summary to Slack.

Runs as a GitHub Action on a schedule and on workflow_dispatch (triggered by
the /wikimedia-status Slack slash command via Lambda).
"""

import logging
import os
import re
import shlex
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import requests

from ingest_wikimedia.partners import PARTNER_DIR, parse_session_labels, resolve_slug
from ingest_wikimedia.ssm import REGION, ssm_run

SLACK_CHANNEL = "C02HEU2L3"
SLACK_API_URL = "https://slack.com/api/chat.postMessage"
_UPLOAD_COMPLETE_PREFIX = "Upload complete"


def log_filename_pattern_for_label(label: str) -> str:
    """Anchored regex matching log filenames for exactly this label.

    Log filenames follow "{YYYYMMDD}-{HHMMSS}-{label}-(download|upload).log".
    The pattern must match `…-bpl+phillips-academy-download.log` and NOT
    `…-bpl+phillips-academy-andover-download.log` — otherwise sibling
    labels whose names extend this one steal the log selection and the
    status report sticks on the wrong target. See lessons.md
    "Log filename phase detection".
    """
    return rf"-{re.escape(label)}-(download|upload)\.log$"


_DOWNLOAD_COMPLETE_PREFIX = "Download complete"
# A session that hasn't written a log line in this many seconds is considered hung.
# Uploads normally complete items in seconds; downloads in seconds to low minutes.
_STALE_SECONDS = 1800  # 30 minutes


def get_phase_and_progress(client, session: str, hub: str, label: str) -> str | None:
    def _safe_int(s: str) -> int:
        try:
            return int(s)
        except ValueError:
            return 0

    pdir = PARTNER_DIR.get(hub, hub)
    base = f"/home/ec2-user/ingest-wikimedia/{pdir}"
    log_dir = shlex.quote(f"{base}/logs")
    session_name = shlex.quote(session)

    # Get session creation time and most recent log for this label — no shell
    # variables needed, avoiding outer-bash expansion of $f inside bash -c.
    #
    # The anchored regex from log_filename_pattern_for_label starts with `-`,
    # which without `--` makes grep interpret the pattern as a command-line
    # option flag (e.g. `-b`/`-p`/`-l`...) and emit "invalid option" errors.
    # The `--` terminator forces grep to treat the next argument as the
    # pattern. See lessons.md "grep patterns starting with `-`".
    label_pattern = shlex.quote(log_filename_pattern_for_label(label))
    precheck = ssm_run(
        client,
        f"tmux display-message -t {session_name} -p '#{{session_created}}' 2>/dev/null || echo 0; "
        f"ls -t {log_dir}/ 2>/dev/null | grep -E -- {label_pattern} | head -1",
    )
    precheck_lines = precheck.splitlines()
    session_created = _safe_int(precheck_lines[0]) if precheck_lines else 0
    log_file = precheck_lines[1].strip() if len(precheck_lines) > 1 else ""

    # Backward compat: sessions launched before the session-label log naming change
    # use hub-slug-only filenames (e.g. nara-download.log). If no label-prefixed log
    # is found, fall back to the most recent hub-slug log, excluding new-format files
    # (which contain '+' in the name and belong to a different institution).
    if not log_file:
        hub_prefix = shlex.quote(hub + "-")
        log_file = ssm_run(
            client,
            f"ls -t {log_dir}/ 2>/dev/null | grep -F {hub_prefix} | grep -vF '+' | head -1 || true",
        ).strip()

    if not log_file:
        # No log file at all: the label may not have started yet, or it may have
        # been skipped (e.g. ineligible institution — get-ids-es exits 1 without
        # ever launching the downloader). Return None so the caller can decide
        # whether to keep looking at later labels.
        return None

    log_path = shlex.quote(f"{base}/logs/{log_file}")
    csv_path = shlex.quote(f"{base}/{label}.csv")

    sep = "__WM_SEP__"
    out = ssm_run(
        client,
        f"date +%s; "
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
    pre_sep = sections[0].strip().splitlines() if sections else []
    now = _safe_int(pre_sep[0]) if pre_sep else 0
    log_mtime = _safe_int(pre_sep[1]) if len(pre_sep) > 1 else 0

    # Log predates this session — no new log yet, treat same as no log.
    if session_created > 0 and log_mtime < session_created:
        return None

    tail = sections[1].strip() if len(sections) > 1 else ""
    count_lines = sections[2].strip().splitlines() if len(sections) > 2 else []

    dpla_id_count = _safe_int(count_lines[0]) if len(count_lines) > 0 else 0
    uploaded_count = _safe_int(count_lines[1]) if len(count_lines) > 1 else 0
    skipped_count = _safe_int(count_lines[2]) if len(count_lines) > 2 else 0
    total = _safe_int(count_lines[3]) if len(count_lines) > 3 else 0
    counts_marker = _safe_int(count_lines[4]) if len(count_lines) > 4 else 0

    def pct(n: int) -> str:
        return f"{n / total * 100:.1f}" if total > 0 else "?"

    # Append a staleness warning to any active (non-complete) phase whose log
    # hasn't been updated in _STALE_SECONDS. Completed phases never get this.
    stale_suffix = ""
    if counts_marker == 0 and now > 0 and log_mtime > 0:
        idle = now - log_mtime
        if idle > _STALE_SECONDS:
            idle_min = idle // 60
            idle_str = (
                f"{idle_min // 60}h{idle_min % 60:02d}m"
                if idle_min >= 60
                else f"{idle_min}m"
            )
            stale_suffix = f" ⚠ idle {idle_str}"

    if log_file.endswith("-download.log"):
        # Use the COUNTS: terminal marker as the definitive completion signal —
        # "Downloading" may still appear in the tail even after the run finishes.
        if counts_marker > 0:
            return f"{_DOWNLOAD_COMPLETE_PREFIX} ({dpla_id_count:,} / {total:,} items)"
        if "Downloading" in tail or "Key already in S3" in tail:
            return f"Downloading ({dpla_id_count:,} / {total:,} items, ~{pct(dpla_id_count)}%){stale_suffix}"
        # Log exists for this session but no active download indicators and no COUNTS
        # marker — downloader likely crashed. Report item count without implying
        # get-ids-es is running (the old "Generating IDs" fallback was wrong here).
        return f"Stalled ({dpla_id_count:,} / {total:,} items, ~{pct(dpla_id_count)}%){stale_suffix}"

    if log_file.endswith("-upload.log"):
        if dpla_id_count == 0:
            # dpla_id_count == 0 means no items logged yet — uploader just started.
            # Staleness here would be a false positive from the normal start-up lag.
            return "Uploading (starting...)"
        # Use the COUNTS: terminal marker as the definitive completion signal.
        # dpla_id_count is logged at the start of each item, not after all its
        # files finish, so count arithmetic alone can fire too early.
        if counts_marker > 0:
            return f"{_UPLOAD_COMPLETE_PREFIX} ({uploaded_count:,} uploaded, {skipped_count:,} already on Commons)"
        return f"Uploading ({dpla_id_count:,} / {total:,}, ~{pct(dpla_id_count)}%){stale_suffix}"

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
        suffix = session.removeprefix("wikimedia-")

        # Retry sessions are named wikimedia-retry-<days>d[-<partner>].
        # parse_session_labels doesn't recognise the retry- prefix, so resolve
        # the active hub directly from the session name when a partner is encoded
        # there, or by finding the most recently modified retry-* log otherwise.
        if suffix.startswith("retry-"):
            # suffix format: "retry-<days>d" or "retry-<days>d-<partner>"
            _, _, explicit_partner = suffix.removeprefix("retry-").partition("-")

            if explicit_partner:
                # Partner encoded in session name — use it directly to avoid
                # picking up a stale log from a different partner's prior run.
                hub = resolve_slug(explicit_partner) or explicit_partner
                label = f"retry-{hub}"
            else:
                # No explicit partner — discover the active hub from the most
                # recently modified retry-* log across all partner directories.
                try:
                    find_out = ssm_run(
                        ssm,
                        "find /home/ec2-user/ingest-wikimedia"
                        " -mindepth 3 -maxdepth 3 -path '*/logs/retry-*'"
                        r" -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1",
                    )
                except Exception:
                    logging.exception("Failed to find retry logs for %s", session)
                    return session, "Unknown (error)"
                line = find_out.strip()
                if not line:
                    return session, "Starting..."
                # Output format: "<epoch.ns> <absolute-path>"
                # e.g. "1747601234.0000000000 /home/ec2-user/ingest-wikimedia/indiana/logs/retry-indiana-upload.log"
                _, _, log_path = line.partition(" ")
                log_filename = log_path.rsplit("/", 1)[-1]
                if log_filename.endswith("-download.log"):
                    label = log_filename[: -len("-download.log")]
                elif log_filename.endswith("-upload.log"):
                    label = log_filename[: -len("-upload.log")]
                else:
                    return session, f"Unknown (unrecognised log: {log_filename!r})"
                raw_hub = label.removeprefix("retry-")
                hub = resolve_slug(raw_hub) or raw_hub

            try:
                phase = get_phase_and_progress(ssm, session, hub, label)
            except Exception:
                logging.exception(
                    "Failed to get retry status for %s (%s)", session, label
                )
                return session, "Unknown (error)"
            return (
                session,
                f"[{label}] {phase}" if phase is not None else f"[{label}] Starting...",
            )

        labels = parse_session_labels(suffix)
        if not labels:
            return session, "Unknown (unrecognised session name)"
        multi = len(labels) > 1

        # Walk labels in pipeline order. Skip past labels with no log file — they
        # may have been skipped (e.g. ineligible institution errored during ID
        # generation). Track the first no-log label after the last completed one
        # as a fallback in case all remaining labels lack logs (meaning the session
        # genuinely hasn't started the next phase yet).
        first_pending: str | None = None
        last_complete_label: str | None = None
        last_complete_phase: str = ""

        for label in labels:
            hub = label.split("+")[0]
            try:
                phase = get_phase_and_progress(ssm, session, hub, label)
            except Exception:
                logging.exception("Failed to get status for %s (%s)", session, label)
                phase = "Unknown (error)"

            if phase is None:
                # No log: either skipped or not yet started. Keep looking at
                # later labels — if any of them have a log, this one was skipped.
                if first_pending is None:
                    first_pending = label
                continue

            # This label has a log. Any earlier no-log labels were skipped.
            first_pending = None

            if not (
                phase.startswith(_UPLOAD_COMPLETE_PREFIX)
                or phase.startswith(_DOWNLOAD_COMPLETE_PREFIX)
            ):
                # Active or stalled label — this is the one to report.
                return session, f"[{label}] {phase}" if multi else phase

            last_complete_label = label
            last_complete_phase = phase

        # Loop exhausted. Either the pipeline is waiting to start the next phase
        # (first_pending), or everything finished (last_complete_label).
        if first_pending is not None:
            return (
                session,
                f"[{first_pending}] Generating IDs" if multi else "Generating IDs",
            )
        if last_complete_label is not None:
            return (
                session,
                f"[{last_complete_label}] {last_complete_phase}"
                if multi
                else last_complete_phase,
            )
        return session, "Generating IDs"

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
