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
from ingest_wikimedia.ssm import REGION, fetch_memory_snapshot, ssm_run

SLACK_CHANNEL = "C02HEU2L3"
SLACK_API_URL = "https://slack.com/api/chat.postMessage"
_UPLOAD_COMPLETE_PREFIX = "Upload complete"
_SDC_COMPLETE_PREFIX = "SDC complete"


def log_filename_pattern_for_label(label: str) -> str:
    """Anchored regex matching log filenames for exactly this label.

    Log filenames follow "{YYYYMMDD}-{HHMMSS}-{label}-(download|upload|sdc).log".
    The pattern must match `…-bpl+phillips-academy-download.log` and NOT
    `…-bpl+phillips-academy-andover-download.log` — otherwise sibling
    labels whose names extend this one steal the log selection and the
    status report sticks on the wrong target. See lessons.md
    "Log filename phase detection".
    """
    return rf"-{re.escape(label)}-(download|upload|sdc)\.log$"


_DOWNLOAD_COMPLETE_PREFIX = "Download complete"
# A session that hasn't written a log line in this many seconds is considered hung.
# Uploads normally complete items in seconds; downloads in seconds to low minutes.
_STALE_SECONDS = 1800  # 30 minutes


def get_phase_and_progress(
    client, session: str, hub: str, label: str
) -> tuple[str | None, int]:
    """Return ``(phase_str, log_mtime)`` for this label.

    ``phase_str`` is ``None`` when no log exists for this label yet (label
    skipped or pipeline hasn't started it).  ``log_mtime`` is the unix
    epoch seconds of the most recent write to the log file (0 when no
    log exists).  The mtime is used by ``main`` to break ties between
    multiple non-complete labels — a phase that aborted hours ago but
    didn't write a ``COUNTS:`` terminal marker (so it doesn't look
    "complete" via the count-marker test) must not eclipse a subsequent
    label that's actively progressing now.
    """

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
        return None, 0

    log_path = shlex.quote(f"{base}/logs/{log_file}")
    # Resolve the CSV(s) backing this label so `wc -l` returns a meaningful
    # "items in scope" denominator.
    #
    # Launch sessions write a single per-target CSV at `{base}/{label}.csv`
    # (e.g. `northwest-heritage/northwest-heritage+local-history.csv`).
    #
    # Retry sessions are different: the retry pipeline writes its CSVs into
    # a shared `retry/` directory using the partner *directory* name plus a
    # phase suffix (matching wikimedia_retry.py's RETRY_DIR layout). For a
    # retry label like `retry-northwest-heritage` the relevant CSVs are
    # `retry/northwest-heritage-download-retry.csv` and/or
    # `retry/northwest-heritage-upload-retry.csv`. Sum lines from whichever
    # exist — `cat` silently skips missing files (stderr suppressed) and
    # `wc -l` of an empty stream is 0. Without this, the status script was
    # always reporting "/ 0 items" for retry sessions because no file at
    # `{base}/retry-<slug>.csv` existed.
    if label.startswith("retry-"):
        retry_dir = "/home/ec2-user/ingest-wikimedia/retry"
        download_csv = shlex.quote(f"{retry_dir}/{pdir}-download-retry.csv")
        upload_csv = shlex.quote(f"{retry_dir}/{pdir}-upload-retry.csv")
        csv_count_cmd = f"cat {download_csv} {upload_csv} 2>/dev/null | wc -l"
    else:
        csv_path = shlex.quote(f"{base}/{label}.csv")
        csv_count_cmd = f"wc -l < {csv_path} 2>/dev/null || echo 0"

    sep = "__WM_SEP__"
    # One awk pass counts all four marker lines in a single sequential read
    # of the log; the previous code ran four separate `grep -c` invocations
    # over the same file (plus the COUNTS: probe), which on multi-GB NARA
    # logs translated to four full sequential reads and four SSM round-trips
    # of pipeline setup overhead. Output is still four lines (dpla_id,
    # uploaded, skipping, counts) in the same order as before, followed by
    # the CSV total from `wc -l`. The `|| printf` fallback covers the
    # missing-file case symmetrically with the old `|| true`.
    out = ssm_run(
        client,
        f"date +%s; "
        f"stat -c %Y {log_path} 2>/dev/null || echo 0; "
        f"echo {sep}; "
        f"tail -5 {log_path}; "
        f"echo {sep}; "
        f"awk '"
        f"/DPLA ID:/ {{d++}} "
        f"/Uploaded to/ {{u++}} "
        f"/Skipping.*Already exists on commons/ {{s++}} "
        f"/COUNTS:/ {{c++}} "
        f"END {{ print d+0; print u+0; print s+0; print c+0 }}"
        f"' {log_path} 2>/dev/null || printf '0\\n0\\n0\\n0\\n'; "
        f"{csv_count_cmd}",
    )

    sections = out.split(f"{sep}\n", 2)
    pre_sep = sections[0].strip().splitlines() if sections else []
    now = _safe_int(pre_sep[0]) if pre_sep else 0
    log_mtime = _safe_int(pre_sep[1]) if len(pre_sep) > 1 else 0

    # Log predates this session — no new log yet, treat same as no log.
    if session_created > 0 and log_mtime < session_created:
        return None, 0

    tail = sections[1].strip() if len(sections) > 1 else ""
    count_lines = sections[2].strip().splitlines() if len(sections) > 2 else []

    # Layout matches the awk-then-wc shell command above: the awk pass emits
    # four counts (DPLA-ID, Uploaded, Skipping, COUNTS) and then `wc -l`
    # emits the CSV total — five lines in total.
    dpla_id_count = _safe_int(count_lines[0]) if len(count_lines) > 0 else 0
    uploaded_count = _safe_int(count_lines[1]) if len(count_lines) > 1 else 0
    skipped_count = _safe_int(count_lines[2]) if len(count_lines) > 2 else 0
    counts_marker = _safe_int(count_lines[3]) if len(count_lines) > 3 else 0
    total = _safe_int(count_lines[4]) if len(count_lines) > 4 else 0

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
            return (
                f"{_DOWNLOAD_COMPLETE_PREFIX} ({dpla_id_count:,} / {total:,} items)",
                log_mtime,
            )
        if "Downloading" in tail or "Key already in S3" in tail:
            return (
                f"Downloading ({dpla_id_count:,} / {total:,} items, ~{pct(dpla_id_count)}%){stale_suffix}",
                log_mtime,
            )
        # Log exists for this session but no active download indicators and no COUNTS
        # marker — downloader likely crashed. Report item count without implying
        # get-ids-es is running (the old "Generating IDs" fallback was wrong here).
        return (
            f"Stalled ({dpla_id_count:,} / {total:,} items, ~{pct(dpla_id_count)}%){stale_suffix}",
            log_mtime,
        )

    if log_file.endswith("-upload.log"):
        if dpla_id_count == 0:
            # dpla_id_count == 0 means no items logged yet — uploader just started.
            # Staleness here would be a false positive from the normal start-up lag.
            return "Uploading (starting...)", log_mtime
        # Use the COUNTS: terminal marker as the definitive completion signal.
        # dpla_id_count is logged at the start of each item, not after all its
        # files finish, so count arithmetic alone can fire too early.
        if counts_marker > 0:
            return (
                f"{_UPLOAD_COMPLETE_PREFIX} ({uploaded_count:,} uploaded, {skipped_count:,} already on Commons)",
                log_mtime,
            )
        return (
            f"Uploading ({dpla_id_count:,} / {total:,}, ~{pct(dpla_id_count)}%){stale_suffix}",
            log_mtime,
        )

    if log_file.endswith("-sdc.log"):
        # sdc-sync's _run_partner_mode logs `DPLA ID: <id> (n/total)` per
        # item — same `DPLA ID:` marker the awk pass already counts. Uses
        # the COUNTS: terminal marker as the completion signal, matching
        # the downloader/uploader convention. The reported figure is
        # "items processed" (i.e. iterated by the loop) rather than
        # "items synced" because some processed items may have been
        # skipped for missing sidecars or mapping issues — the Slack
        # summary surfaces the real synced count via the tracker's
        # SDC_ITEMS_SYNCED line.
        if dpla_id_count == 0:
            return "SDC syncing (starting...)", log_mtime
        if counts_marker > 0:
            return (
                f"{_SDC_COMPLETE_PREFIX} ({dpla_id_count:,} items processed)",
                log_mtime,
            )
        return (
            f"SDC syncing ({dpla_id_count:,} / {total:,} items, ~{pct(dpla_id_count)}%){stale_suffix}",
            log_mtime,
        )

    return "Unknown", log_mtime


def _format_memory_line(snapshot: tuple[int, int] | None) -> str | None:
    """Format a ``fetch_memory_snapshot`` pair as the Slack readout
    line, or return ``None`` if no snapshot was obtained.

    Caller-side formatting (rather than baking the string into the
    shared helper) keeps ``wikimedia_launch`` / ``wikimedia_retry``
    free to apply their own headroom-threshold semantics on the same
    raw pair without ferrying a parsed format back through a regex.
    """
    if snapshot is None:
        return None
    total_mb, available_mb = snapshot
    used_mb = total_mb - available_mb
    pct_available = available_mb * 100 // total_mb
    return f"Memory: {used_mb:,} / {total_mb:,} MB used ({pct_available}% available)"


def post_to_slack(
    token: str,
    rows: list[tuple[str, str]],
    memory_line: str | None = None,
) -> None:
    lines = "\n".join(f"`{session:<32}` {phase}" for session, phase in rows)
    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Wikimedia Upload Status"},
        },
        {"type": "section", "text": {"type": "mrkdwn", "text": lines}},
    ]
    if memory_line:
        blocks.append(
            {"type": "context", "elements": [{"type": "mrkdwn", "text": memory_line}]}
        )
    payload = {
        "channel": SLACK_CHANNEL,
        "text": "Wikimedia Upload Status",
        "blocks": blocks,
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
            post_to_slack(
                token,
                [("(none)", "No active Wikimedia upload sessions.")],
                memory_line=_format_memory_line(fetch_memory_snapshot(ssm)),
            )
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
                elif log_filename.endswith("-sdc.log"):
                    label = log_filename[: -len("-sdc.log")]
                else:
                    return session, f"Unknown (unrecognised log: {log_filename!r})"
                raw_hub = label.removeprefix("retry-")
                hub = resolve_slug(raw_hub) or raw_hub

            try:
                phase, _ = get_phase_and_progress(ssm, session, hub, label)
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

        # Walk every label in the pipeline and collect each one's phase
        # string and log mtime. We need ALL labels (not a forward-walk that
        # short-circuits at the first non-complete one) because a label
        # earlier in the pipeline can be "non-complete" — e.g. its SDC
        # phase aborted without writing the COUNTS: terminal marker — yet
        # a later label is actively progressing right now. Reporting the
        # first non-complete label in that case freezes the Slack notice
        # on the aborted phase forever.  Pick the label with the LATEST
        # log mtime among those still in-flight; an active label always
        # wins over an aborted earlier one because its log was just
        # written, while the abort's last log write is hours stale.
        per_label: list[tuple[str, str | None, int]] = []
        for label in labels:
            hub = label.split("+")[0]
            try:
                phase, log_mtime = get_phase_and_progress(ssm, session, hub, label)
            except Exception:
                logging.exception("Failed to get status for %s (%s)", session, label)
                phase, log_mtime = "Unknown (error)", 0
            per_label.append((label, phase, log_mtime))

        def _is_complete(phase: str | None) -> bool:
            return phase is not None and (
                phase.startswith(_UPLOAD_COMPLETE_PREFIX)
                or phase.startswith(_DOWNLOAD_COMPLETE_PREFIX)
                or phase.startswith(_SDC_COMPLETE_PREFIX)
            )

        # Labels with a log AND not in a "complete" terminal state.
        active = [
            (lbl, phase, mtime)
            for lbl, phase, mtime in per_label
            if phase is not None and not _is_complete(phase)
        ]
        if active:
            # Latest write wins — see comment above for the rationale.
            lbl, phase, _ = max(active, key=lambda t: t[2])
            return session, f"[{lbl}] {phase}" if multi else phase

        # No active labels.  Either everything completed, or the
        # pipeline is between phases (no log yet for the next label).
        # Prefer the LAST completed label in pipeline order when present;
        # otherwise fall back to the first no-log label as "Generating IDs"
        # (the pipeline's resting state before any phase writes a log).
        completed = [(lbl, phase) for lbl, phase, _ in per_label if _is_complete(phase)]
        if completed:
            lbl, phase = completed[-1]
            return session, f"[{lbl}] {phase}" if multi else phase

        first_pending = next(
            (lbl for lbl, phase, _ in per_label if phase is None), None
        )
        if first_pending is not None:
            return (
                session,
                f"[{first_pending}] Generating IDs" if multi else "Generating IDs",
            )
        # `per_label` is built directly from `labels`, which we already
        # checked is non-empty, so every reachable path above returned.
        # This is a defensive fallback that should never fire.
        return session, "Generating IDs"

    # Memory snapshot is independent of every per-session fetch — submit it
    # to the same executor so the SSM round-trip overlaps with the session
    # phase resolution rather than serializing after it.
    with ThreadPoolExecutor(max_workers=min(len(sessions) + 1, 8)) as executor:
        memory_future = executor.submit(fetch_memory_snapshot, ssm)
        futures = {executor.submit(fetch, s): s for s in sessions}
        for future in as_completed(futures):
            session, phase = future.result()
            results[session] = phase
            print(f"{session}: {phase}")
        memory_line = _format_memory_line(memory_future.result())

    rows = [(s, results[s]) for s in sessions if s in results]
    post_to_slack(token, rows, memory_line=memory_line)
    print("Posted to Slack.")


if __name__ == "__main__":
    main()
