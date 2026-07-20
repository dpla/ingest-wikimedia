#!/usr/bin/env python3
"""Check Wikimedia upload session status on EC2 and post a summary to Slack.

Runs as a GitHub Action on a schedule and on workflow_dispatch (triggered by
the /wikimedia-status Slack slash command via Lambda).
"""

import logging
import os
import re
import shlex
import statistics
from concurrent.futures import Future, ThreadPoolExecutor, as_completed

import boto3
import requests

from typing import NamedTuple

from ingest_wikimedia.partners import PARTNER_DIR, parse_session_labels, resolve_slug
from ingest_wikimedia.session_state import (
    _PHASE_ALT,
    find_active_label,
    log_filename_pattern_for_label,
    snapshot_running_active_labels,
)
from ingest_wikimedia.ssm import REGION, fetch_memory_snapshot, ssm_run
from ingest_wikimedia.worker_slots import (
    DEFAULT_SLOT_DIR,
    SLOTS_BUSY_LOG_MARKER,
    UPLOADER_PRIORITY_SLOT_DIR,
)

# Bash regexes used inside :func:`_fetch_slot_snapshot` to filter
# ``lslocks`` rows. The shared-pool regex drives the ``free``/``held``
# aggregate line (bounded by the shared pool's known ``TOTAL``); the
# both-pools regex drives per-session attribution (a Case-2 uploader
# holding a priority-pool slot should still be visible in the per-session
# ``[Slots: 1]`` readout even though the shared pool's aggregate ignores
# it). Interpolated from the shared ``worker_slots`` constants so a
# rename of either directory can't leave this file silently wrong.
_SHARED_SLOT_DIR_BASENAME = os.path.basename(DEFAULT_SLOT_DIR)
_ALL_SLOT_DIR_BASENAME_RE = "|".join(
    re.escape(os.path.basename(p))
    for p in (DEFAULT_SLOT_DIR, UPLOADER_PRIORITY_SLOT_DIR)
)


def _strip_batch_suffix(display_id: str) -> str:
    """Return ``display_id`` with any trailing ``" [n/m]"`` batch-position
    annotation removed.

    Multi-target sessions render ``"partner+institution [pos/total]"`` as
    their row label (see ``_with_batch_suffix`` in ``main``), but the
    ``WIKIMEDIA_SESSION_LABEL`` env var carries only the raw
    ``partner+institution`` slug. Any keyed lookup against a dict of
    session-labels must strip the batch suffix first — otherwise
    multi-target rows will miss their own slot-holder counts.
    """
    return _BATCH_SUFFIX_RE.sub("", display_id)


_BATCH_SUFFIX_RE = re.compile(r" \[\d+/\d+\]$")


class SlotSnapshot(NamedTuple):
    """Aggregate + per-session view of the box-wide slot pool at snapshot time.

    ``line`` is the human-readable summary rendered under the row block.
    ``free`` is the free-slot count in the shared 24-slot pool (used to gate
    the per-session ``[Slots: N]`` augmentation — we only show it under
    saturation so the row block stays quiet during headroom).
    ``holds_by_label`` maps WIKIMEDIA_SESSION_LABEL → number of slots that
    session currently holds. Includes holders from BOTH the shared pool and
    the uploader priority pool. Labels are extracted from
    ``/proc/<pid>/environ`` on each holder PID; a session appears with
    ``holds == 0`` only implicitly (via absence from the dict).
    """

    line: str
    free: int
    holds_by_label: dict[str, int]


# Phase-string prefixes that indicate a session is CURRENTLY in a
# slot-consuming phase (upload / SDC-sync). Used to decide whether
# ``[Awaiting slot]`` is applicable when a saturated snapshot shows the
# session holds zero slots — a session in "Downloading" or "Generating IDs"
# is legitimately not waiting for a Commons-write slot, so no suffix.
_SLOT_CONSUMING_PHASE_PREFIXES: tuple[str, ...] = (
    "Uploading",
    "SDC syncing",
)

SLACK_CHANNEL = "C02HEU2L3"
SLACK_API_URL = "https://slack.com/api/chat.postMessage"
_UPLOAD_COMPLETE_PREFIX = "Upload complete"
_SDC_COMPLETE_PREFIX = "SDC complete"

# One-pass awk over a phase log, emitting eight integers in the fixed order the
# parser indexes: DPLA-ID, Uploaded, Skipping, COUNTS, Ordinal, HAND-FIX,
# MERGED, maintain-scope. HAND-FIX/MERGED read $2 — they come from the
# prefix-less continuation lines of the multi-line ``COUNTS:`` record. The
# maintain-scope marker is instead a normal PREFIXED log line
# (``[INFO] <time>: maintain scope: N files``), so its number is ``$(NF-1)`` —
# the field before the trailing "files", robust to however wide the log-line
# prefix is (counting from the message start would miss the prefix, which
# silently zeroed this out in the first draft).
# CONTRACT: the "maintain scope: N files" wording is shared with
# tools/sdc_sync.py's _log_maintain_scope(); change them together.
_SDC_COUNTS_AWK = (
    "/DPLA ID:/ {d++} "
    "/Uploaded to/ {u++} "
    "/Skipping.*Already exists on commons/ {s++} "
    "/COUNTS:/ {c++} "
    "/-- Ordinal [0-9]+:/ {o++} "
    "/UPLOAD_HAND_FIX:/ {hf=$2} "
    "/UPLOAD_MERGED_TO_CANONICAL:/ {mg=$2} "
    "/maintain scope: [0-9]+ files/ {mt=$(NF-1)} "
    "END { print d+0; print u+0; print s+0; print c+0; print o+0; "
    "print hf+0; print mg+0; print mt+0 }"
)

# Slack Block Kit caps a single ``section`` block's text element at 3000
# characters. A hub-busy day with many active sessions can collectively
# exceed that on row count alone, so the formatter splits across multiple
# ``section`` blocks rather than dropping rows. Keep a safety margin
# under the hard cap so a single row with an unexpectedly long phase
# string can't tip a near-full block over.
_SLACK_BLOCK_SOFT_LIMIT = 2800


_DOWNLOAD_COMPLETE_PREFIX = "Download complete"
# A session that hasn't written a log line in this many seconds is considered hung.
# Uploads normally complete items in seconds; downloads in seconds to low minutes.
_STALE_SECONDS = 1800  # 30 minutes


def _idle_suffix(now: int, log_mtime: int) -> str:
    """Return a `` ⚠ idle {duration}`` suffix when a log hasn't been written
    in over ``_STALE_SECONDS``, else ``""``.

    Shared by every active (non-complete) phase so a hung run reads distinctly
    instead of looking active. Callers gate the call on phase-specific
    conditions (not yet complete, not blocked on slots); this helper only owns
    the threshold check and the ``h``/``m`` duration formatting.
    """
    if now <= 0 or log_mtime <= 0:
        return ""
    idle = now - log_mtime
    if idle <= _STALE_SECONDS:
        return ""
    idle_min = idle // 60
    idle_str = (
        f"{idle_min // 60}h{idle_min % 60:02d}m" if idle_min >= 60 else f"{idle_min}m"
    )
    return f" ⚠ idle {idle_str}"


# Labels constructed by ``parse_session_labels`` and ``PARTNER_HUBS`` are
# slug-form: hub-name-or-institution-name lowercase plus ``+`` and ``-``.
# The download-log glob below interpolates ``label`` directly into a
# shell command (unquoted, so the ``*`` expands), so we enforce the
# slug shape at runtime to keep that interpolation safe regardless of
# how callers obtain the label.
_LABEL_SLUG_RE = re.compile(r"[a-z0-9+\-]+")


def _files_progress(num: int, den: int) -> str:
    """``N / M files, ~P%`` — the shared file-level progress string used by the
    Upload and SDC branches (maintain-scope, download-log ordinals). Callers
    guard ``den > 0`` (it also selects which branch applies)."""
    return f"{num:,} / {den:,} files, ~{num / den * 100:.1f}%"


def get_phase_and_progress(
    client,
    session: str,
    hub: str,
    label: str,
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

    ``label`` MUST be slug-shaped (``[a-z0-9+\\-]+``) — the download-log
    glob below interpolates it unquoted into a shell command.
    """
    if not _LABEL_SLUG_RE.fullmatch(label):
        raise ValueError(
            f"label must be slug-shaped ([a-z0-9+-]+) for safe shell "
            f"interpolation; got {label!r}"
        )

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
    if not log_file and "+" not in label:
        # Backward-compat ONLY for bare-hub labels: pre-session-label sessions
        # used hub-slug-only filenames (e.g. nara-download.log). An institution
        # label ("hub+institution") with no matching log is in id-generation,
        # not a legacy hub-slug run, so exclude new-format ('+') logs from the
        # fallback to avoid attaching an unrelated institution's log.
        # Match ONLY legacy phase filenames (e.g. nara-download.log). A bare
        # ``<hub>-`` prefix would also select a retired ``<hub>-drain.log`` left
        # over from the old moving-window apparatus, after which no phase branch
        # matches and status reads "Unknown". The ('+') exclusion is redundant
        # with the anchored pattern but kept for clarity.
        legacy_pattern = shlex.quote(rf"^{re.escape(hub)}-({_PHASE_ALT})\.log$")
        log_file = ssm_run(
            client,
            f"ls -t {log_dir}/ 2>/dev/null | grep -E -- {legacy_pattern} "
            "| head -1 || true",
        ).strip()

    if not log_file:
        # No log file at all: the label may not have started yet, or it may have
        # been skipped (e.g. ineligible institution — get-ids-es exits 1 without
        # ever launching the downloader). Return None so the caller can decide
        # whether to keep looking at later labels.
        return None, 0

    log_path = shlex.quote(f"{base}/logs/{log_file}")

    if log_file.endswith("-id-generation.log"):
        # get-ids-es logs a scope line + periodic "N items enumerated so far".
        # Surface the latest count so a long enumeration reads as progress, not
        # a hang — and so an id-gen session isn't misclassified (previously it
        # had no recognized log and fell through to a mislabel).
        out = ssm_run(
            client,
            f"date +%s; "
            f"stat -c %Y {log_path} 2>/dev/null || echo 0; "
            f"grep -oE '[0-9,]+ items enumerated' {log_path} 2>/dev/null | tail -1",
        )
        id_lines = out.splitlines()
        id_now = _safe_int(id_lines[0]) if id_lines else 0
        id_mtime = _safe_int(id_lines[1]) if len(id_lines) > 1 else 0
        # Log predates this session — a stale id-generation log from a prior
        # run of this label, picked before the new session has written its
        # own. Same "predates this session" sentinel as the
        # download/upload/sdc branches (return None → caller renders a bare
        # "Generating IDs" rather than a stale enumerated count).
        if session_created > 0 and id_mtime < session_created:
            return None, 0
        enumerated = id_lines[2].strip() if len(id_lines) > 2 else ""
        label_txt = f"Generating IDs ({enumerated})" if enumerated else "Generating IDs"
        # Same idle/staleness signal as the download/upload/sdc phases: a hung
        # enumeration (no log write in _STALE_SECONDS) reads distinctly instead
        # of looking active.
        return label_txt + _idle_suffix(id_now, id_mtime), id_mtime
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
    # Locate the corresponding -download.log for this label so we can sum
    # total ordinals across all items — gives Upload- and SDC-phase
    # progress a file-level denominator instead of the item-level one
    # that makes multi-page items vastly under-represent work done (a
    # 100-page newspaper counts the same as a 1-image photo). Empty
    # when no download log exists for this label yet (sessions still
    # in get-ids-es or the legacy single-log layout); the file-level
    # branches fall back to item-count in that case.
    #
    # The label is interpolated directly into the glob below (no
    # shlex.quote) — single-quoting would disable shell glob expansion
    # so the ``*`` would no longer expand. The slug-shape guard at the
    # top of this function makes the unquoted interpolation safe.
    # Total-ordinals denominator from the download log. Prefer the per-item
    # summary ``Item <id>: N ordinals`` line (downloader.py:563) — emitted for
    # EVERY item regardless of whether its media was freshly fetched or already
    # staged/skipped — and sum its N. Fall back to counting the per-ordinal
    # ``Downloading <partner> <id> <ordinal> from <url>`` line (downloader.py:543)
    # ONLY when no Item-summary lines exist, i.e. old pre-#272 download logs.
    #
    # The previous code counted ``Downloading`` alone on the assumption it was
    # "emitted unconditionally for every ordinal" — but it fires only on an
    # actual fetch ATTEMPT, not for already-staged skips. So any run whose media
    # was already downloaded (re-runs, SDC-only relaunches, download-once-then-
    # iterate hubs like NARA) had 0 ``Downloading`` lines, collapsing the total
    # to 0 and wrongly dropping the status row from file- to item-granularity —
    # even though the ``Item`` summaries carried the true counts.
    ordinals_awk = (
        "BEGIN{item=0; dl=0} "
        "/Item [a-f0-9]+: [0-9]+ ordinals/ "
        '{for(i=1;i<=NF;i++) if($i=="ordinals"){item+=$(i-1); break}} '
        "/Downloading [a-z0-9-]+ [a-f0-9]+ [0-9]+ from / {dl++} "
        "END {print (item>0 ? item : dl)}"
    )
    # One awk pass (``_SDC_COUNTS_AWK``) counts all the marker lines in a single
    # sequential read of the log; the previous code ran several separate
    # `grep -c` invocations over the same file, which on multi-GB NARA logs
    # translated to that many full sequential reads and SSM round-trips of
    # pipeline setup overhead. Output is eight lines (dpla_id, uploaded,
    # skipping, counts, ordinal, hand-fix, merged, maintain-scope), followed by
    # the CSV total from `wc -l`. The download-log ordinal sum is emitted after a
    # fresh separator so the output sections stay self-describing.
    out = ssm_run(
        client,
        f"date +%s; "
        f"stat -c %Y {log_path} 2>/dev/null || echo 0; "
        f"echo {sep}; "
        f"tail -5 {log_path}; "
        f"echo {sep}; "
        f"awk '{_SDC_COUNTS_AWK}' {log_path} 2>/dev/null "
        f"|| printf '0\\n0\\n0\\n0\\n0\\n0\\n0\\n0\\n'; "
        f"{csv_count_cmd}; "
        f"echo {sep}; "
        f"DOWNLOG=$(ls -t {log_dir}/*-{label}-download.log 2>/dev/null | head -1); "
        f'if [ -n "$DOWNLOG" ]; then '
        f"awk '{ordinals_awk}' \"$DOWNLOG\" 2>/dev/null || echo 0; "
        f"else echo 0; fi",
    )

    sections = out.split(f"{sep}\n", 3)
    pre_sep = sections[0].strip().splitlines() if sections else []
    now = _safe_int(pre_sep[0]) if pre_sep else 0
    log_mtime = _safe_int(pre_sep[1]) if len(pre_sep) > 1 else 0

    # Log predates this session — no new log yet, treat same as no log.
    if session_created > 0 and log_mtime < session_created:
        return None, 0

    tail = sections[1].strip() if len(sections) > 1 else ""
    count_lines = sections[2].strip().splitlines() if len(sections) > 2 else []

    # Layout matches the awk-then-wc shell command above: the awk pass emits
    # eight counts (DPLA-ID, Uploaded, Skipping, COUNTS, Ordinal, HAND-FIX,
    # MERGED, maintain-scope) and then `wc -l` emits the CSV total — nine lines
    # in total. HAND-FIX / MERGED are read from the terminal ``COUNTS:`` block
    # (the authoritative tracker values, one per SHA1-uniqueness outcome) so the
    # completion summary reports those alongside uploaded/skipped rather than
    # only the two happy-path counts. ``ordinal_count`` is the count of
    # ``-- Ordinal N:`` markers in the active log, which the SDC phase emits
    # one of per file (numerator for SDC's file-level progress).
    # ``maintain_total`` is the enumerated scope a maintain --cat/--file run
    # logs ("maintain scope: N files"); 0 for partner-mode runs, which have no
    # such marker (they fall through to the download-log / CSV denominators).
    dpla_id_count = _safe_int(count_lines[0]) if len(count_lines) > 0 else 0
    uploaded_count = _safe_int(count_lines[1]) if len(count_lines) > 1 else 0
    skipped_count = _safe_int(count_lines[2]) if len(count_lines) > 2 else 0
    counts_marker = _safe_int(count_lines[3]) if len(count_lines) > 3 else 0
    ordinal_count = _safe_int(count_lines[4]) if len(count_lines) > 4 else 0
    hand_fix_count = _safe_int(count_lines[5]) if len(count_lines) > 5 else 0
    merged_count = _safe_int(count_lines[6]) if len(count_lines) > 6 else 0
    maintain_total = _safe_int(count_lines[7]) if len(count_lines) > 7 else 0
    total = _safe_int(count_lines[8]) if len(count_lines) > 8 else 0

    # Sum of `Item <id>: N ordinals` lines from the download log — the true
    # file count once downloads have completed. 0 when no download log was
    # found (legacy sessions, or the session is still in get-ids-es).
    total_ordinals = _safe_int(sections[3].strip()) if len(sections) > 3 else 0

    def pct(n: int) -> str:
        return f"{n / total * 100:.1f}" if total > 0 else "?"

    # Append a staleness warning to any active (non-complete) phase whose log
    # hasn't been updated in _STALE_SECONDS. Completed phases never get this.
    # A session is "waiting on slots" when its *last* log line is the box-wide
    # budget's wait message — i.e. all its workers are currently blocked on the
    # cap. Keyed on the last line (not anywhere in the tail) so a session that
    # waited briefly and then resumed isn't flagged.
    last_log_line = tail.splitlines()[-1] if tail else ""
    waiting_on_slots = SLOTS_BUSY_LOG_MARKER in last_log_line
    slot_suffix = " ⏸ waiting on slots" if waiting_on_slots else ""

    # A blocked session legitimately stops writing to its log while polling,
    # so don't also flag it as idle/hung — the slot suffix already explains
    # the silence.
    stale_suffix = (
        _idle_suffix(now, log_mtime)
        if counts_marker == 0 and not waiting_on_slots
        else ""
    )

    if log_file.endswith("-download.log"):
        # Use the COUNTS: terminal marker as the definitive completion signal —
        # "Downloading" may still appear in the tail even after the run finishes.
        if counts_marker > 0:
            return (
                f"{_DOWNLOAD_COMPLETE_PREFIX} ({dpla_id_count:,} / {total:,} items)",
                log_mtime,
            )
        if (
            "Downloading" in tail
            or "Key already in S3" in tail
            or "No media; skipping." in tail
        ):
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
            # No items logged yet. Distinguish a genuine just-started session
            # ("starting...") from one parked behind the box-wide cap before
            # its first item ("queued"). Staleness is suppressed either way —
            # no progress yet is expected, not a stall.
            start_state = "queued" if waiting_on_slots else "starting..."
            return f"Uploading ({start_state}){slot_suffix}", log_mtime
        # Use the COUNTS: terminal marker as the definitive completion signal.
        # dpla_id_count is logged at the start of each item, not after all its
        # files finish, so count arithmetic alone can fire too early.
        if counts_marker > 0:
            return (
                f"{_UPLOAD_COMPLETE_PREFIX} ({uploaded_count:,} uploaded, "
                f"{skipped_count:,} already on Commons, {merged_count:,} merged, "
                f"{hand_fix_count:,} hand-fix)",
                log_mtime,
            )
        # File-level progress: the download log gives us the true total
        # ordinal count, and the upload log tells us how many ordinals
        # have terminated (uploaded or skipped). Falls back to the
        # item-level item-count denominator when no download log was
        # found, so legacy sessions still get a readout.
        files_done = uploaded_count + skipped_count
        if total_ordinals > 0:
            progress = _files_progress(files_done, total_ordinals)
        else:
            progress = f"{dpla_id_count:,} / {total:,} items, ~{pct(dpla_id_count)}%"
        return (
            f"Uploading ({progress}){slot_suffix}{stale_suffix}",
            log_mtime,
        )

    if log_file.endswith("-sdc.log"):
        # sdc-sync's _run_partner_mode logs `DPLA ID: <id> (n/total)` per
        # item and `-- Ordinal N: <mediaid>` per ordinal. Uses the
        # COUNTS: terminal marker as the completion signal, matching the
        # downloader/uploader convention. The reported in-progress
        # figure is file-level (`ordinal_count` numerator over
        # `total_ordinals` denominator from the download log) when both
        # are available — same rationale as the Upload branch, since
        # multi-page items take proportionally more SDC-write work than
        # 1-image items. Falls back to item-level against the per-partner
        # ids CSV for legacy no-download-log partner sessions. Maintain
        # --cat/--file runs report file-level against their enumerated scope
        # ("maintain scope: N files", logged at scan time) when present, and
        # a bare file count otherwise (streaming --cat / legacy logs) — the
        # per-partner CSV is never a valid denominator for a Commons category
        # / file-list scope. Terminal completion still reports items, since the
        # tracker's SDC_ITEMS_SYNCED is per item.
        if dpla_id_count == 0:
            start_state = "queued" if waiting_on_slots else "starting..."
            return f"SDC syncing ({start_state}){slot_suffix}", log_mtime
        if counts_marker > 0:
            return (
                f"{_SDC_COMPLETE_PREFIX} ({dpla_id_count:,} items processed)",
                log_mtime,
            )
        if maintain_total > 0:
            # Maintain --cat/--file: the run logged its enumerated scope
            # ("maintain scope: N files") — a file-level denominator in the same
            # unit as the per-file "DPLA ID:" markers counted here. Prefer it: it
            # describes the actual Commons category / file-list scope, unlike the
            # per-partner CSV (whose mismatch produced the old >100% read). The
            # scope is a snapshot of a live category, so the % is an estimate.
            progress = _files_progress(dpla_id_count, maintain_total)
        elif total_ordinals > 0 and ordinal_count > 0:
            progress = _files_progress(ordinal_count, total_ordinals)
        elif dpla_id_count <= total:
            # Legacy partner-mode SDC with no download log: the per-partner ids
            # CSV is a valid item denominator and each "DPLA ID:" marker is one
            # item. (dpla_id_count > 0 here — the == 0 case returned above.)
            progress = f"{dpla_id_count:,} / {total:,} items, ~{pct(dpla_id_count)}%"
        else:
            # Maintain with no scope marker (streaming --cat, or a legacy log
            # predating the marker): report the bare per-file count rather than a
            # ratio against the per-partner CSV, which doesn't describe the
            # category / file-list scope (the mismatch produced the old >100%
            # readout). sdc_sync logs one "DPLA ID:" marker per category-member
            # FILE on this path (tools/sdc_sync.py).
            progress = f"{dpla_id_count:,} files processed"
        return (
            f"SDC syncing ({progress}){slot_suffix}{stale_suffix}",
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


def _fetch_slot_snapshot(ssm) -> SlotSnapshot | None:
    """Return a :class:`SlotSnapshot` for the box-wide worker-slot pool, or
    ``None`` if no budget-enabled session has created the slot dir.

    The cap is shared by every Commons-writing phase — uploader (uploads,
    renames, template migrations, purges) as well as sdc-sync — so the
    reported line is not SDC-specific.

    One SSM roundtrip collects both the median-smoothed held-count (three
    count-only polls) AND the final PID→session-label mapping (one
    lslocks pass with the PIDs looked up in ``/proc/<pid>/environ``).
    Consolidating into a single roundtrip avoids double-charging Slack's
    3-second slash-command budget when the saturated per-session view is
    also needed for the readout.
    """
    try:
        out = ssm_run(
            ssm,
            f"D={DEFAULT_SLOT_DIR}; "
            f'if [ ! -d "$D" ]; then echo NODIR; exit 0; fi; '
            # Without lslocks, grep -c on empty stdin returns 0 and we'd
            # silently report "all free" — so bail to NODATA instead of lying.
            f"command -v lslocks >/dev/null 2>&1 || {{ echo NODATA; exit 0; }}; "
            f'echo "TOTAL $(ls "$D" 2>/dev/null | wc -l)"; '
            # Three quick count-only samples for median smoothing (transient
            # all-held/all-free blips are common on churn). Counts the
            # SHARED pool only — the ``TOTAL`` line above is the shared
            # pool's file count, so ``free = TOTAL - held`` only balances
            # when both operands sample the same pool.
            f"SHARED_RE='{_SHARED_SLOT_DIR_BASENAME}'; "
            f"ALL_RE='{_ALL_SLOT_DIR_BASENAME_RE}'; "
            f"for i in 1 2 3; do "
            f'  lslocks 2>/dev/null | grep -cE "$SHARED_RE"; '
            f"  sleep 1; "
            f"done; "
            # Final structured pass: both pools for per-session attribution
            # (a Case-2 uploader in the priority pool should still appear in
            # ``[Slots: 1]`` for its row) plus a 4th shared-only count that
            # feeds the median alongside the earlier samples.
            f"HOLDERS=$(lslocks -n -o PID,PATH 2>/dev/null "
            f'  | grep -E "$ALL_RE" || true); '
            f'echo "$HOLDERS" | grep -cE "$SHARED_RE" '
            f"  | (read -r n; echo COUNT $n); "
            f'echo "$HOLDERS" | awk "{{print \\$1}}" | while read pid; do '
            f'  [ -z "$pid" ] && continue; '
            # Each pipeline process (uploader, sdc-sync main and its pool
            # workers) inherits WIKIMEDIA_SESSION_LABEL from the tmux
            # environ set by the launcher — a robust per-target signal that
            # survives multiprocessing.Pool forks.
            f"  label=$(tr '\\0' '\\n' < /proc/$pid/environ 2>/dev/null "
            f"    | grep -m1 '^WIKIMEDIA_SESSION_LABEL=' | cut -d= -f2-); "
            f'  [ -n "$label" ] && echo "HOLDER $label"; '
            f"done",
        )
    except Exception as e:
        logging.warning("Could not read slot snapshot: %s", e)
        return None

    # Parse guarded by its own try/except: unexpected output (a malformed
    # ``TOTAL``/``COUNT`` sample, an ``lslocks`` upgrade that reshapes a
    # column) MUST NOT propagate through ``slots_future.result()`` in
    # ``main`` and abort the entire status post — the slot line is
    # optional context, not load-bearing. Degrades to ``None`` the same
    # way :func:`fetch_memory_snapshot` does for its own parse failures.
    try:
        total: int | None = None
        held_samples: list[int] = []
        holds_by_label: dict[str, int] = {}
        final_holder_count: int | None = None
        for ln in (out or "").splitlines():
            ln = ln.strip()
            if ln in ("NODIR", "NODATA"):
                return None
            if ln.startswith("TOTAL "):
                total = int(ln.split()[1])
            elif ln.startswith("COUNT "):
                final_holder_count = int(ln.split()[1])
            elif ln.startswith("HOLDER "):
                label = ln[len("HOLDER ") :]
                holds_by_label[label] = holds_by_label.get(label, 0) + 1
            elif ln.isdigit():
                held_samples.append(int(ln))
        if not total or not held_samples:
            return None
        # Feed the final structured pass into the median alongside the
        # count-only samples so a run that transiently drops between samples
        # still smooths.
        if final_holder_count is not None:
            held_samples.append(final_holder_count)
        held = round(statistics.median(held_samples))
        free = max(0, total - held)
        line = f"Worker slots: ~{free} free of {total} ({held} held)"
        return SlotSnapshot(line=line, free=free, holds_by_label=holds_by_label)
    except Exception as e:
        logging.warning("Could not parse slot snapshot output: %s", e)
        return None


def _slot_suffix_for_row(
    display_id: str, phase: str, holds_by_label: dict[str, int]
) -> str:
    """Return the ``[Slots: N]`` / ``[Awaiting slot]`` suffix (with leading
    space) to append to a row's phase text, or an empty string when the
    row is not in a slot-consuming phase.

    Only called when the pool is fully saturated — see the ``free == 0``
    gate at the caller. In the headroom regime every slot-phase session
    trivially holds its full allotment, so surfacing the number is noise.

    Suppresses ``[Awaiting slot]`` when the row's phase text already
    carries the ``⏸ waiting on slots`` marker (appended earlier by
    :func:`get_phase_and_progress` when the session's last log line is
    ``SLOTS_BUSY_LOG_MARKER``). Otherwise a fully-blocked uploader would
    render both indicators back-to-back — same signal, twice.
    """
    if not any(phase.startswith(p) for p in _SLOT_CONSUMING_PHASE_PREFIXES):
        return ""
    # ``display_id`` may carry a trailing ``" [n/m]"`` for multi-target
    # sessions; ``holds_by_label`` is keyed by the raw
    # ``WIKIMEDIA_SESSION_LABEL`` (no such suffix), so strip before lookup
    # or every multi-target row would miss its own hold count and render
    # ``[Awaiting slot]`` even while its workers are actively uploading.
    held = holds_by_label.get(_strip_batch_suffix(display_id), 0)
    if held > 0:
        return f" [Slots: {held}]"
    if "waiting on slots" in phase:
        return ""
    return " [Awaiting slot]"


def _format_rows_into_blocks(rows: list[tuple[str, str]]) -> list[dict]:
    """Format ``rows`` into one or more Slack ``section`` blocks, each
    holding at most ``_SLACK_BLOCK_SOFT_LIMIT`` chars of text.

    Splitting across multiple blocks keeps a busy-day readout renderable
    when the cumulative row count would push a single block past the
    3000-char per-text cap (which produced the ``invalid_blocks``
    failure that motivated this helper). Individual rows are already
    bounded by ``fetch``'s display contract (active label, not tmux
    session name), so per-row truncation isn't needed.
    """
    chunks: list[list[str]] = [[]]
    cur_len = 0
    for display_id, phase in rows:
        line = f"`{display_id}` {phase}"
        if chunks[-1] and cur_len + len(line) + 1 > _SLACK_BLOCK_SOFT_LIMIT:
            chunks.append([])
            cur_len = 0
        chunks[-1].append(line)
        cur_len += len(line) + 1
    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}}
        for lines in chunks
        if lines
    ]


def post_to_slack(
    token: str,
    rows: list[tuple[str, str]],
    memory_line: str | None = None,
    slots_line: str | None = None,
) -> None:
    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "Wikimedia Upload Status"},
        },
    ]
    blocks.extend(_format_rows_into_blocks(rows))
    context_bits = [b for b in (slots_line, memory_line) if b]
    if context_bits:
        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": "   •   ".join(context_bits)}],
            }
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
        # ``-F`` returns structured ``name|epoch`` lines instead of the
        # verbose default format. Pinning the fields we want avoids
        # fragile string parsing of the "created Sun Jul 5 …" text and
        # gives us the session-creation epoch we need to time-bound
        # :func:`find_active_label`'s log lookup.
        session_out = ssm_run(
            ssm,
            "tmux ls -F '#{session_name}|#{session_created}' 2>/dev/null "
            "| grep '^wikimedia-' || echo NONE",
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

    # Each entry: ``(session_name, session_created_epoch)``.
    # session_created_epoch is used to bound the log-mtime lookup so a
    # concurrent session writing to one of this session's completed
    # labels can't hijack the "active" row.
    def _parse_session_line(line: str) -> tuple[str, int]:
        name, _, epoch = line.partition("|")
        try:
            return name.strip(), int(epoch.strip())
        except ValueError:
            return name.strip(), 0

    sessions_with_created = (
        [_parse_session_line(line) for line in session_out.splitlines()]
        if session_out and session_out != "NONE"
        else []
    )
    sessions = [name for name, _ in sessions_with_created]

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

    # Maps original session name → (display_id, phase) pair. ``fetch``
    # now returns ``display_id`` (the active label) as the first
    # element, distinct from the tmux session name we used to index
    # by, so the session-name → result mapping is rebuilt here from
    # the ``futures`` dict (which remembers the submitting session for
    # each future) to preserve the order of the original ``tmux ls``
    # output in the Slack readout.
    results: dict[str, tuple[str, str]] = {}

    session_created_by_name = dict(sessions_with_created)
    # ``fetch`` waits on this future for the subprocess-based active-
    # label signal (see :func:`snapshot_running_active_labels`). Assigned
    # inside the executor block below; declared here so the closure
    # captures the name and can be mutated at runtime.
    active_labels_future: Future[dict[str, str]] | None = None

    def fetch(session: str) -> tuple[str, str]:
        suffix = session.removeprefix("wikimedia-")
        session_created = session_created_by_name.get(session, 0)

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
                return label, "Unknown (error)"
            return label, phase if phase is not None else "Starting..."

        labels = parse_session_labels(suffix)
        if not labels:
            return session, "Unknown (unrecognised session name)"
        multi = len(labels) > 1

        # Prefer the subprocess-based active-label snapshot; fall back
        # to the log-mtime lookup only when no running child was found
        # (id-generation cold start, between steps, or chain finished).
        try:
            snapshot = active_labels_future.result() if active_labels_future else {}
        except Exception:
            logging.exception("Snapshot future failed for %s", session)
            snapshot = {}
        subprocess_label = snapshot.get(session)
        if subprocess_label is not None:
            active: tuple[str, int] | None = (subprocess_label, 0)
        else:
            try:
                active = find_active_label(ssm, labels, session_created=session_created)
            except Exception:
                logging.exception("Failed to find active label for %s", session)
                return labels[0], "Unknown (error)"

        # For multi-label batches, suffix a `[<pos>/<total>]` position
        # annotation so the reader can tell at a glance how far along
        # the institution chain this row is. The earlier `(+N more)`
        # form counted batch size − 1 and was ambiguous: a session
        # showing `(+72 more)` could be on the FIRST institution or
        # the LAST one (73 of 73). `[73/73]` makes it unambiguous.
        def _with_batch_suffix(label: str) -> str:
            if not multi:
                return label
            try:
                pos = labels.index(label) + 1
            except ValueError:
                # Defensive: should never happen — every label we route
                # through this helper came from `labels` itself.
                return f"{label} [?/{len(labels)}]"
            return f"{label} [{pos}/{len(labels)}]"

        if active is None:
            # No log file matches any label yet — pipeline is in
            # get-ids-es, before any downstream phase has written.
            return _with_batch_suffix(labels[0]), "Generating IDs"

        label, _ = active
        hub = label.split("+")[0]
        display_label = _with_batch_suffix(label)
        try:
            phase, _ = get_phase_and_progress(ssm, session, hub, label)
        except Exception:
            logging.exception("Failed to get status for %s (%s)", session, label)
            return display_label, "Unknown (error)"
        return display_label, phase if phase is not None else "Generating IDs"

    # Memory snapshot is independent of every per-session fetch — submit it
    # to the same executor so the SSM round-trip overlaps with the session
    # phase resolution rather than serializing after it. Same treatment
    # for the active-label snapshot: submitted first so it can overlap
    # with memory/slots/per-session lookups; each ``fetch`` blocks on
    # its result only if the snapshot hasn't landed by the time the
    # thread needs it.
    with ThreadPoolExecutor(max_workers=min(len(sessions) + 3, 8)) as executor:
        active_labels_future = executor.submit(snapshot_running_active_labels, ssm)
        memory_future = executor.submit(fetch_memory_snapshot, ssm)
        slots_future = executor.submit(_fetch_slot_snapshot, ssm)
        futures = {executor.submit(fetch, s): s for s in sessions}
        for future in as_completed(futures):
            session = futures[future]
            display_id, phase = future.result()
            results[session] = (display_id, phase)
            print(f"{display_id}: {phase}")
        memory_line = _format_memory_line(memory_future.result())
        slot_snapshot = slots_future.result()

    rows = [results[s] for s in sessions if s in results]
    slots_line = slot_snapshot.line if slot_snapshot is not None else None
    # Under saturation (0 shared slots free), attach a per-session [Slots: N]
    # or [Awaiting slot] suffix so an operator can see at a glance which
    # sessions are actually holding the pool vs. blocked on acquire. Skipped
    # in the headroom regime because every session in a slot-consuming phase
    # trivially holds its full allotment there.
    if slot_snapshot is not None and slot_snapshot.free == 0:
        rows = [
            (
                display_id,
                phase
                + _slot_suffix_for_row(display_id, phase, slot_snapshot.holds_by_label),
            )
            for display_id, phase in rows
        ]
    post_to_slack(token, rows, memory_line=memory_line, slots_line=slots_line)
    print("Posted to Slack.")


if __name__ == "__main__":
    main()
