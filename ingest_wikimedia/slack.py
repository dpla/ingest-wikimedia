import glob
import logging
import os
import re
from typing import Literal

import requests

from ingest_wikimedia.tracker import Result, Tracker

SLACK_CHANNEL = "C02HEU2L3"
SLACK_API_URL = "https://slack.com/api/chat.postMessage"

Phase = Literal["id-generation", "download", "upload", "sdc-sync"]

_PHASE_EMOJI: dict[str, str] = {
    "id-generation": "🔍",
    "download": "⬇",
    "upload": "⬆",
    # SDC sync follows upload in the pipeline. Without a phase-start
    # notification for it, the gap between the last "upload complete"
    # message and the eventual "SDC complete" summary can stretch hours
    # on a large hub with no indication that work has actually moved on
    # to the SDC phase.
    "sdc-sync": "🔗",
}


def post_message(token: str, text: str) -> None:
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


# exit code → short hint shown in the Slack failure message. Anything >128 is
# a bash-encoded signal (128 + signal number). 137 in particular is SIGKILL,
# which the OOM killer uses — so seeing it in the message is a strong "this was
# probably an OOM" hint without having to SSM in and check dmesg.
#
# Common interpreter-level exits also have universally-recognised meanings:
#   * 1 — Python uncaught exception (CRITICAL traceback printed to stderr)
#   * 2 — Click misuse: bad CLI arguments / failed Click-side validation
#         (e.g. ``click.BadParameter`` from a precheck like
#         ``DPLA.check_partner``). Always points at config / args, never code.
#   * 124 — GNU ``timeout`` (1) wrapper killed the child for exceeding its
#         deadline. Not used in the pipeline today but cheap to include.
_EXIT_CODE_HINTS: dict[int, str] = {
    1: "uncaught exception — see traceback",
    2: "rejected its arguments",
    124: "timed out",
    130: "SIGINT (Ctrl-C)",
    134: "SIGABRT",
    137: "SIGKILL — likely OOM",
    139: "SIGSEGV",
    143: "SIGTERM",
}


def _decode_exit_code(rc_str: str | None) -> str:
    """Render a `(exit N — meaning)` suffix for the failure message."""
    if not rc_str:
        return ""
    try:
        rc = int(rc_str)
    except ValueError:
        return ""
    if rc == 0:
        return ""
    hint = _EXIT_CODE_HINTS.get(rc)
    if hint is None and rc > 128:
        hint = f"signal {rc - 128}"
    return f" (exit {rc}" + (f" — {hint})" if hint else ")")


def _find_latest_log(partner_dir: str, label: str, phase: str = "*") -> str | None:
    """Return the most recently modified log file matching this label, or None.

    Logs are named `{timestamp}-{label}-{phase}.log` under `<partner_dir>/logs/`.
    Pass `phase` (e.g. "download", "upload") to restrict the match to a single
    phase; the default matches any phase.

    Tolerates the rare race where a candidate disappears between glob and stat —
    raising OSError here would suppress the Slack failure notification entirely.
    """
    if not partner_dir or not os.path.isdir(partner_dir):
        return None
    pattern = os.path.join(partner_dir, "logs", f"*-{label}-{phase}.log")
    newest: tuple[float, str] | None = None
    for path in glob.glob(pattern):
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            continue
        if newest is None or mtime > newest[0]:
            newest = (mtime, path)
    return newest[1] if newest is not None else None


# Counted markers shown in the failure summary.  Patterns match what the
# downloader and uploader currently emit, anchored loosely so log-format
# tweaks don't silently zero them out.  Skip subcategories aren't split out
# because "Skipping ... Already exists" would double-count under a generic
# "Skipping" pattern — the log tail conveys reasons more usefully anyway.
_LOG_MARKERS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("uploaded", re.compile(r"Uploaded to https://commons", re.IGNORECASE)),
    ("skipped", re.compile(r"^\[INFO\].*Skipping ", re.MULTILINE)),
    ("downloaded", re.compile(r"^\[INFO\].*Downloaded ", re.MULTILINE)),
    ("failed", re.compile(r"^\[(ERROR|WARNING)\].*Failed", re.MULTILINE)),
)


def _summarize_log(log_path: str, tail_lines: int = 8) -> str | None:
    """Read the log and produce a short multi-line summary.

    Tries to be cheap: only reads up to ~2 MB from the end of the file.  Counts
    common markers and tails the last N lines so the cause of the failure is
    visible without SSM-ing in.
    """
    try:
        size = os.path.getsize(log_path)
        read_size = min(size, 2 * 1024 * 1024)
        with open(log_path, "rb") as f:
            f.seek(size - read_size)
            data = f.read().decode("utf-8", errors="replace")
    except OSError:
        return None

    counts = []
    for label, pat in _LOG_MARKERS:
        n = len(pat.findall(data))
        if n:
            counts.append(f"{n} {label}")

    tail = "\n".join(data.splitlines()[-tail_lines:])

    parts = [f"Log: `{os.path.basename(log_path)}`"]
    if counts:
        parts.append("Counts so far: " + ", ".join(counts))
    if tail:
        parts.append("Last lines:\n```\n" + tail + "\n```")
    return "\n".join(parts)


# Map WIKIMEDIA_STEP (set by the launcher per pipeline step) → the phase
# suffix the corresponding tool uses for ``setup_logging(...)``. Used by
# :func:`notify_pipeline_fail` to scope log lookup to the failing step.
#
# ``id-generation`` deliberately has no entry: ``tools/get_ids_es.py`` does
# NOT call ``setup_logging``, so there's no log file to tail. The launcher
# instead tees its stderr to the per-session path returned by
# :func:`id_generation_stderr_tail_file` and the failure handler reads
# that file directly.
_PHASE_LOG_SUFFIX: dict[str, str] = {
    "download": "download",
    "upload": "upload",
    "sdc-sync": "sdc",
    "drain-deferred": "drain-deferred",
    # Per-target opportunistic (``--no-wait``) drain runs inside each
    # target's chain; a distinct log-file suffix keeps it from
    # colliding with the batch-terminal patient drain of the same
    # partner.
    "drain-deferred-opportunistic": "drain-deferred-opportunistic",
}

# Per-session-label path the launcher tees ``get-ids-es`` stderr to.
# Per-label, not a single shared path, because the EC2 box runs many
# concurrent tmux sessions and a shared ``/tmp/wm-id-generation-stderr.log``
# would race: two id-generation steps starting near-simultaneously would
# clobber each other's stderr, and a later failure handler would pick up
# whichever ran second. The launcher's bash side composes the same path
# via ``${WIKIMEDIA_SESSION_LABEL}`` interpolation in the tee redirect,
# so write-side and read-side agree without any extra env var.
#
# A label is the per-target slug (e.g. ``georgia+duke-university-library``);
# it's already passed through ``slugify_session_label_component`` so it's
# filesystem-safe (lowercase + digits + ``+`` + ``-`` only — no shell
# metacharacters and no path separators).
_ID_GENERATION_STDERR_TAIL_BASENAME_PREFIX = "wm-id-generation-stderr"
# Matches the bash ``${WIKIMEDIA_SESSION_LABEL:-unknown}`` fallback in
# ``scripts/wikimedia_launch.py``'s tee redirect, so when the env var
# isn't set on either side both write/read paths produce the same file
# name. Without this agreement, a partial-env failure handler would
# look for ``…-stderr.log`` while the launcher wrote
# ``…-stderr-unknown.log`` and the operator would see no stderr body.
_ID_GENERATION_STDERR_NO_LABEL_FALLBACK = "unknown"


def id_generation_stderr_tail_file(session_label: str | None) -> str:
    """Return the per-session path the launcher tees ``get-ids-es`` /
    ``get-ids-nara`` stderr to. The launcher's bash side interpolates
    the same shape via ``${WIKIMEDIA_SESSION_LABEL:-unknown}``; this
    helper is the Python read-side counterpart used by
    :func:`notify_pipeline_fail` and must produce the SAME path for
    the SAME env state.

    Callers must pass the *raw* ``WIKIMEDIA_SESSION_LABEL`` value (or
    ``None``/empty when unset), not a display-fallback like ``"unknown"``
    — otherwise the no-label branch here is dead and the helper agrees
    with bash only by accident.
    """
    label = session_label or _ID_GENERATION_STDERR_NO_LABEL_FALLBACK
    return f"/tmp/{_ID_GENERATION_STDERR_TAIL_BASENAME_PREFIX}-{label}.log"


def _tail_text_file(
    path: str, tail_lines: int = 8, max_bytes: int = 64 * 1024
) -> str | None:
    """Read up to ``max_bytes`` from the end of ``path`` and return the last
    ``tail_lines`` non-empty lines, or None if the file is missing/unreadable.

    Used by the failure handler for steps without a dedicated log file
    (currently just id-generation, which has no ``setup_logging`` call).
    """
    try:
        size = os.path.getsize(path)
        read_size = min(size, max_bytes)
        with open(path, "rb") as f:
            f.seek(size - read_size)
            data = f.read().decode("utf-8", errors="replace")
    except OSError:
        return None
    lines = [line for line in data.splitlines() if line.strip()]
    if not lines:
        return None
    return "\n".join(lines[-tail_lines:])


def notify_pipeline_fail() -> None:
    """Post a pipeline-step failure notification to Slack.

    Reads from the environment:
      DPLA_SLACK_BOT_TOKEN     — required to post
      WIKIMEDIA_SESSION_LABEL  — identifies the target in the message
      WIKIMEDIA_STEP           — name of the pipeline step that was running
                                 when the chain broke (``id-generation``,
                                 ``download``, ``upload``, ``sdc-sync``,
                                 ``drain-deferred``).
                                 Empty / unset → "pipeline step" generic
                                 wording. Set by the launcher's per-step
                                 ``export`` so the latest assignment before
                                 the failure sticks in the shell.
      WIKIMEDIA_LAST_EXIT      — exit code of the failed step (best-effort)
      WIKIMEDIA_PARTNER_DIR    — absolute path to the partner dir, used to
                                 locate the most recent log for tailing
      WIKIMEDIA_TARGET_IS_LAST — "1" iff this is the final target in the
                                 batch; switches the failure message
                                 suffix between "aborting this target;
                                 batch continues with the next" and
                                 "aborting batch (this was the final
                                 target)". Accurate even for
                                 single-target sessions.

    Designed to be called as a one-liner from a shell failure handler:
        rc=$?; WIKIMEDIA_LAST_EXIT=$rc python3 -c \\
          'from ingest_wikimedia.slack import notify_pipeline_fail; notify_pipeline_fail()'
    """
    token = (os.environ.get("DPLA_SLACK_BOT_TOKEN") or "").strip()
    if not token:
        logging.warning(
            "DPLA_SLACK_BOT_TOKEN not set — skipping pipeline failure notification"
        )
        return
    # Two views of the same env var:
    #   * ``raw_label`` — the actual value (or ``None`` / empty when
    #     unset). Passed to :func:`id_generation_stderr_tail_file` so
    #     the helper's own fallback decides the path. Mixing in a
    #     display-text fallback here would skip that branch and only
    #     agree with the bash side by coincidence.
    #   * ``label`` — display-only, used in Slack header text where an
    #     empty session label would render ugly backticks.
    raw_label = os.environ.get("WIKIMEDIA_SESSION_LABEL")
    label = raw_label or "unknown"
    rc_suffix = _decode_exit_code(os.environ.get("WIKIMEDIA_LAST_EXIT"))
    step = (os.environ.get("WIKIMEDIA_STEP") or "").strip()

    is_last = os.environ.get("WIKIMEDIA_TARGET_IS_LAST") == "1"
    tail_phrase = (
        "aborting batch (this was the final target)"
        if is_last
        else "aborting this target; batch continues with the next"
    )
    # Step-aware header: tells the operator WHICH phase failed
    # (id-generation / download / upload / sdc-sync), not just "the
    # pipeline". Pre-step-tracking this said only "pipeline step
    # failed" — leaving the operator to SSM in and grep four log
    # files to find the actual phase. WIKIMEDIA_STEP is exported by
    # the launcher right before each step, and the bash ``&&`` chain
    # halts on the failing step, so its value survives into this
    # handler.
    step_phrase = f"`{step}` step failed" if step else "pipeline step failed"
    msg = f"❌ `wikimedia-{label}`: {step_phrase}{rc_suffix} — {tail_phrase}"

    # Scope log lookup to the failing step's log file when possible.
    # id-generation has no dedicated log file (get_ids_es.py never
    # calls setup_logging), so for that step we fall back to reading
    # the tee'd stderr file the launcher writes — without this, a
    # failure inside get-ids-es would only ever surface as a bare
    # "exit N" in Slack.
    partner_dir = os.environ.get("WIKIMEDIA_PARTNER_DIR", "")
    summary: str | None = None
    log_suffix = _PHASE_LOG_SUFFIX.get(step)
    if log_suffix is not None:
        log_path = _find_latest_log(partner_dir, label, phase=log_suffix)
        if log_path is not None:
            summary = _summarize_log(log_path)
    elif step == "id-generation":
        stderr_tail = _tail_text_file(id_generation_stderr_tail_file(raw_label))
        if stderr_tail:
            summary = "Last stderr lines:\n```\n" + stderr_tail + "\n```"
    else:
        # Unknown step (or step unset on a stale launcher): fall back to
        # the legacy any-phase log lookup so we don't regress on the
        # info-density of the pre-step-tracking message.
        log_path = _find_latest_log(partner_dir, label)
        if log_path is not None:
            summary = _summarize_log(log_path)
    if summary:
        msg += "\n" + summary

    try:
        post_message(token, msg)
    except Exception:
        logging.warning(
            "Failed to post pipeline failure notification to Slack", exc_info=True
        )


def notify_phase_start(partner: str, phase: Phase) -> None:
    # Single-item targets post only one launch notification and one completion
    # notification; suppress per-phase messages to avoid cluttering #tech-alerts.
    if os.environ.get("WIKIMEDIA_SINGLE_ITEM") == "1":
        return
    token = os.environ.get("DPLA_SLACK_BOT_TOKEN")
    if not token:
        logging.warning("DPLA_SLACK_BOT_TOKEN not set — skipping Slack notification")
        return
    session_label = os.environ.get("WIKIMEDIA_SESSION_LABEL") or partner
    emoji = _PHASE_EMOJI.get(phase, "▶")
    try:
        post_message(token, f"{emoji} `wikimedia-{session_label}`: starting {phase}")
    except Exception:
        logging.warning("Slack phase notification failed", exc_info=True)


def _format_bytes(num_bytes: int) -> str:
    value = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024:
            return f"{value:,.1f} {unit}"
        value /= 1024
    return f"{value:,.1f} PB"


def _format_runtime(elapsed_seconds: float) -> str:
    hours, remainder = divmod(int(elapsed_seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _post_completion_notice(
    token: str,
    header: str,
    plain_text: str,
    stats_lines: list[str],
) -> None:
    """Post a completion summary block to #tech-alerts. Logs warnings on failure."""
    body = "```" + "\n".join(stats_lines) + "```"
    payload = {
        "channel": SLACK_CHANNEL,
        "text": plain_text,
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": header}},
            {"type": "section", "text": {"type": "mrkdwn", "text": body}},
        ],
    }
    try:
        response = requests.post(
            SLACK_API_URL,
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        if not data.get("ok"):
            logging.warning(f"Slack notification failed: {data.get('error')}")
    except requests.exceptions.HTTPError as ex:
        logging.warning(f"Slack API returned HTTP {ex.response.status_code}")
    except Exception as ex:
        logging.warning("Failed to send Slack notification", exc_info=ex)


def notify_download_complete(
    tracker: Tracker,
    partner_label: str,
    elapsed_seconds: float,
    dry_run: bool = False,
) -> None:
    token = os.environ.get("DPLA_SLACK_BOT_TOKEN")
    if not token:
        logging.warning("DPLA_SLACK_BOT_TOKEN not set — skipping Slack notification")
        return

    effective_label = (
        f"wikimedia-{os.environ.get('WIKIMEDIA_SESSION_LABEL') or partner_label}"
    )
    runtime = _format_runtime(elapsed_seconds)
    dry_run_note = " _(dry run)_" if dry_run else ""

    _post_completion_notice(
        token=token,
        header=f"*Wikimedia Download Refresh Complete: {effective_label}*{dry_run_note}",
        plain_text=f"Wikimedia download refresh complete: {effective_label}",
        stats_lines=[
            f"REFRESHED: {tracker.count(Result.DOWNLOADED):,}",
            f"SKIPPED:   {tracker.count(Result.SKIPPED):,}",
            f"FAILED:    {tracker.count(Result.FAILED):,}",
            f"BYTES:     {_format_bytes(tracker.count(Result.BYTES))}",
            f"Runtime:   {runtime}",
        ],
    )


_COUNTS_FAILED_RE = re.compile(r"^FAILED:\s*(\d+)\s*$", re.MULTILINE)


def _read_download_failed_count(log_path: str | None) -> int | None:
    """Read the FAILED count from a downloader log's terminal COUNTS section.

    Returns:
      * the int FAILED count when the COUNTS section names FAILED explicitly
      * 0 when the COUNTS section exists but omits FAILED — a clean run.
        `Tracker.__str__` only emits counter lines whose value is > 0, so
        zero-failure downloads legitimately have no FAILED line at all.
        Treating that as the unambiguous 0 it is (rather than as a parse
        failure) keeps clean retries titled "Retry Complete" instead of
        misleadingly falling back to "Upload Complete".
      * None when the path is unset, the file can't be read, or there is
        no COUNTS section at all — the downloader bombed before printing
        the terminal tracker dump, so we don't have a usable count to
        combine.

    Used by `notify_upload_complete` to roll the download phase's failures
    into a single retry-session Slack summary (see the retry pipeline in
    scripts/wikimedia_retry.py).
    """
    if not log_path:
        return None
    try:
        with open(log_path, encoding="utf-8", errors="replace") as f:
            content = f.read()
    except OSError as e:
        logging.warning(f"Could not read retry download log {log_path}: {e}")
        return None
    # Anchor the FAILED lookup to the COUNTS section so we don't accidentally
    # pick up a stray "FAILED:" earlier in the log (e.g. an [ERROR] line).
    # rfind because the tracker dump is the last thing the downloader writes.
    counts_idx = content.rfind("COUNTS:")
    if counts_idx < 0:
        logging.warning(
            f"No COUNTS section found in retry download log {log_path}; "
            f"download-phase failures will not be reflected in the Slack summary"
        )
        return None
    counts_block = content[counts_idx:]
    match = _COUNTS_FAILED_RE.search(counts_block)
    if not match:
        return 0
    return int(match.group(1))


def _find_retry_download_log() -> str | None:
    """Locate this retry session's download log, if any.

    Only fires for retry sessions (label prefixed `retry-`) that actually
    ran a download phase this run (WIKIMEDIA_RETRY_HAS_DOWNLOAD=1, set by
    the retry pipeline iff a download CSV is present for the target).

    The HAS_DOWNLOAD gate is important: retry session labels are reused
    across runs, so without it an upload-only retry would happily pick
    up a stale `*-retry-<slug>-download.log` from a prior run and inflate
    FAILED with counts that already shipped in an earlier message.
    """
    label = (os.environ.get("WIKIMEDIA_SESSION_LABEL") or "").strip()
    partner_dir = (os.environ.get("WIKIMEDIA_PARTNER_DIR") or "").strip()
    if not label.startswith("retry-"):
        return None
    if os.environ.get("WIKIMEDIA_RETRY_HAS_DOWNLOAD") != "1":
        return None
    return _find_latest_log(partner_dir, label, phase="download")


def notify_upload_complete(
    tracker: Tracker,
    partner_label: str,
    elapsed_seconds: float,
    dry_run: bool = False,
) -> None:
    token = os.environ.get("DPLA_SLACK_BOT_TOKEN")
    if not token:
        logging.warning("DPLA_SLACK_BOT_TOKEN not set — skipping Slack notification")
        return

    effective_label = (
        f"wikimedia-{os.environ.get('WIKIMEDIA_SESSION_LABEL') or partner_label}"
    )
    runtime = _format_runtime(elapsed_seconds)
    dry_run_note = " _(dry run)_" if dry_run else ""

    # In a retry session the user cares about whether *anything* failed in
    # the whole download+upload round-trip — not which phase produced the
    # failure. Fold the download phase's FAILED count into this summary so
    # the user sees one combined notification instead of the misleading
    # "Upload Complete: 0 failed" when downloads bombed.
    #
    # SKIPPED/UPLOADED/BYTES are not combined: each phase's "skipped" means
    # a different thing (download = "already in S3"; upload = "already on
    # Commons") and conflating them would obscure the picture. FAILED, by
    # contrast, is universally a failure regardless of where it happened.
    download_log = _find_retry_download_log()
    download_failed = _read_download_failed_count(download_log)
    is_retry_summary = download_failed is not None
    total_failed = tracker.count(Result.FAILED) + (download_failed or 0)

    header_phrase = "Retry Complete" if is_retry_summary else "Upload Complete"
    plain_phrase = "retry complete" if is_retry_summary else "upload complete"

    _post_completion_notice(
        token=token,
        header=f"*Wikimedia {header_phrase}: {effective_label}*{dry_run_note}",
        plain_text=f"Wikimedia {plain_phrase}: {effective_label}",
        stats_lines=[
            f"UPLOADED:      {tracker.count(Result.UPLOADED):,}",
            f"SKIPPED:       {tracker.count(Result.SKIPPED):,}",
            # Per-class breakdown of the aggregate SKIPPED above. Lets
            # operators tell upstream-gap skips (no S3 asset, downloader
            # didn't stage) from MIME / ineligibility skips so the fix
            # routes to the right team.
            f"  not present: {tracker.count(Result.UPLOAD_SKIPPED_NOT_PRESENT):,}",
            f"  ineligible:  {tracker.count(Result.UPLOAD_SKIPPED_INELIGIBLE):,}",
            f"FAILED:        {total_failed:,}",
            f"BYTES:         {_format_bytes(tracker.count(Result.BYTES))}",
            f"Runtime:       {runtime}",
        ],
    )


def notify_sdc_complete(
    tracker: Tracker,
    partner_label: str,
    elapsed_seconds: float,
    dry_run: bool = False,
    workers: int = 1,
    maintain: bool = False,
) -> None:
    """Post the SDC phase's completion summary to #tech-alerts.

    Same channel and block shape as `notify_upload_complete` — the user
    sees one message per phase. Counter set differs because SDC writes
    statements + references to existing MediaInfo entities rather than
    uploading new files; "items synced" is the per-DPLA-ID count, and the
    SKIPPED_* lines surface items the partner-mode loop bailed on because
    their sidecars were missing or malformed.

    ``workers`` turns the aggregate worker-seconds in ``SDC_SLOT_WAIT_SECONDS``
    into a per-worker average for the SLOT WAIT line.
    """
    token = os.environ.get("DPLA_SLACK_BOT_TOKEN")
    if not token:
        logging.warning("DPLA_SLACK_BOT_TOKEN not set — skipping Slack notification")
        return

    effective_label = (
        f"wikimedia-{os.environ.get('WIKIMEDIA_SESSION_LABEL') or partner_label}"
    )
    runtime = _format_runtime(elapsed_seconds)
    dry_run_note = " _(dry run)_" if dry_run else ""

    # Box-wide-slot contention: aggregate worker-seconds waited / workers =
    # average per worker; expressed as a share of runtime so it reads as
    # "this session spent ~X% of its time throttled by the budget."
    avg_wait = tracker.count(Result.SDC_SLOT_WAIT_SECONDS) / max(1, workers)
    wait_pct = (avg_wait / elapsed_seconds * 100) if elapsed_seconds > 0 else 0

    stats_lines = [
        f"ITEMS SYNCED:         {tracker.count(Result.SDC_ITEMS_SYNCED):,}",
        f"ITEMS PARTIAL:        {tracker.count(Result.SDC_ITEMS_PARTIALLY_SYNCED):,}",
        f"PAGES EDITED:         {tracker.count(Result.SDC_PAGES_EDITED):,}",
        f"CLAIMS ADDED:         {tracker.count(Result.SDC_CLAIMS_ADDED):,}",
        f"REFS ADDED:           {tracker.count(Result.SDC_REFS_ADDED):,}",
        f"REMOVALS:             {tracker.count(Result.SDC_REMOVALS):,}",
        f"SKIPPED (no sidecar): {tracker.count(Result.SDC_ITEMS_SKIPPED_NO_SIDECAR):,}",
        f"SKIPPED (mapping):    {tracker.count(Result.SDC_ITEMS_SKIPPED_MAPPING):,}",
        f"SKIPPED (error):      {tracker.count(Result.SDC_ITEMS_SKIPPED_ERROR):,}",
        f"ORDINAL MISSING:      {tracker.count(Result.SDC_ORDINALS_SKIPPED_MISSING_ENTITY):,}",
        f"ORDINAL NO PAGEID:    {tracker.count(Result.SDC_ORDINALS_SKIPPED_MISSING_PAGEID):,}",
        f"ORDINAL ERRORS:       {tracker.count(Result.SDC_ORDINALS_SKIPPED_ERROR):,}",
    ]
    # Maintain mode also renames title-drifted files to their canonical title;
    # surface those outcomes (only on maintain runs, to keep the regular SDC
    # summary uncluttered). RENAME BLOCKED counts files left non-canonical
    # because the canonical title was occupied — flagged for DPLA follow-up.
    if maintain:
        stats_lines.extend(
            [
                f"RENAMED:              {tracker.count(Result.MAINTAIN_RENAMED):,}",
                f"RENAME BLOCKED:       {tracker.count(Result.MAINTAIN_RENAME_BLOCKED):,}",
            ]
        )
    stats_lines.extend(
        [
            f"SLOT WAIT (avg/wkr):  {_format_runtime(avg_wait)} ({wait_pct:.0f}% of runtime)",
            f"Runtime:              {runtime}",
        ]
    )

    _post_completion_notice(
        token=token,
        header=f"*Wikimedia SDC Complete: {effective_label}*{dry_run_note}",
        plain_text=f"Wikimedia SDC complete: {effective_label}",
        stats_lines=stats_lines,
    )


def notify_upload_aborted(
    tracker: Tracker,
    partner_label: str,
    elapsed_seconds: float,
    reason: str,
) -> None:
    """Warn #tech-alerts that the upload phase aborted mid-run.

    Emitted on unrecoverable session-level failures (e.g. pywikibot CSRF
    token invalidated and recovery ceiling exhausted). The caller
    suppresses the normal ``notify_upload_complete`` so this message
    stands on its own.
    """
    token = os.environ.get("DPLA_SLACK_BOT_TOKEN")
    if not token:
        logging.warning("DPLA_SLACK_BOT_TOKEN not set — skipping Slack notification")
        return
    effective_label = (
        f"wikimedia-{os.environ.get('WIKIMEDIA_SESSION_LABEL') or partner_label}"
    )
    runtime = _format_runtime(elapsed_seconds)
    _post_completion_notice(
        token=token,
        header=f"🛑 *Wikimedia upload ABORTED: {effective_label}*",
        plain_text=f"Wikimedia upload aborted: {effective_label}",
        stats_lines=[
            f"UPLOADED:      {tracker.count(Result.UPLOADED):,}",
            f"SKIPPED:       {tracker.count(Result.SKIPPED):,}",
            f"  not present: {tracker.count(Result.UPLOAD_SKIPPED_NOT_PRESENT):,}",
            f"  ineligible:  {tracker.count(Result.UPLOAD_SKIPPED_INELIGIBLE):,}",
            f"FAILED:        {tracker.count(Result.FAILED):,}",
            f"BYTES:         {_format_bytes(tracker.count(Result.BYTES))}",
            f"Runtime:       {runtime}",
            "",
            f"Reason: {reason}",
        ],
    )


def notify_drain_phase_start(
    partner_label: str, deferred_count: int, category_size: int
) -> None:
    """Ping #tech-alerts that a session has entered the deferred-drain
    phase — the uploader has finished processing every non-deferred
    item, sdc-sync has (or is about to) run on those, and the session
    is now patiently waiting for Category:Duplicate to fall below the
    resume threshold so it can finish the deferred items.

    Emitted at drain-phase start so the operator knows the session is
    in this state (rather than hung or making per-item progress). The
    tmux session stays alive during the wait; kill via the existing
    Slack kill-session command if you need to abandon.
    """
    token = (os.environ.get("DPLA_SLACK_BOT_TOKEN") or "").strip()
    if not token:
        logging.warning("DPLA_SLACK_BOT_TOKEN not set — skipping Slack notification")
        return
    effective_label = (
        f"wikimedia-{os.environ.get('WIKIMEDIA_SESSION_LABEL') or partner_label}"
    )
    msg = (
        f"⏳ `{effective_label}`: entering deferred-drain phase — "
        f"{deferred_count:,} item(s) waiting for Category:Duplicate to "
        f"drain (currently at {category_size:,}). No worker slots held; "
        f"other partner sessions unaffected."
    )
    try:
        post_message(token, msg)
    except Exception:
        logging.warning(
            "Failed to post drain-phase-start notification to Slack", exc_info=True
        )


def notify_drain_phase_complete(
    partner_label: str, elapsed_seconds: float, emitted_count: int
) -> None:
    """Ping #tech-alerts that the deferred-drain phase drained cleanly —
    every deferred upload + ``{{duplicate}}`` tag has landed. Emitted at
    the drain phase's normal exit; a killed / aborted drain leaves the
    sidecar in place for a future session to resume from.
    """
    token = (os.environ.get("DPLA_SLACK_BOT_TOKEN") or "").strip()
    if not token:
        logging.warning("DPLA_SLACK_BOT_TOKEN not set — skipping Slack notification")
        return
    effective_label = (
        f"wikimedia-{os.environ.get('WIKIMEDIA_SESSION_LABEL') or partner_label}"
    )
    runtime = _format_runtime(elapsed_seconds)
    msg = (
        f"✅ `{effective_label}`: deferred-drain phase complete — "
        f"{emitted_count:,} item(s) emitted their upload + "
        f"`{{{{duplicate}}}}` tag over {runtime}."
    )
    try:
        post_message(token, msg)
    except Exception:
        logging.warning(
            "Failed to post drain-phase-complete notification to Slack", exc_info=True
        )
