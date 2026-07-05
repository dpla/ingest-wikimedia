"""Session-state utilities for the ``wikimedia-*`` scripts.

A wikimedia-upload tmux session runs its per-target chain sequentially —
downloader → uploader → sdc-sync per label, then the next label. At any
moment **at most one label is active**; the label whose log file was most
recently written uniquely identifies it. This module exposes the log-filename
helpers that both ``scripts/wikimedia_upload_status.py`` (for the periodic
status post) and ``scripts/wikimedia_launch.py`` (for scoping conflict
detection to still-active targets in a chained session) need.

Kept in ``ingest_wikimedia/`` rather than ``scripts/`` because the two
consumers run as top-level scripts (``python scripts/foo.py``) and don't share
a package namespace — cross-script imports fail at runtime under that entry-
point shape even though they work in tests via ``conftest`` path-fixup.
"""

import logging
import re
import shlex

from ingest_wikimedia.partners import PARTNER_DIR
from ingest_wikimedia.ssm import ssm_run


def log_filename_pattern_for_label(label: str) -> str:
    """Anchored regex matching log filenames for exactly this label.

    Log filenames follow ``{YYYYMMDD}-{HHMMSS}-{label}-<phase>.log`` where
    ``<phase>`` is one of ``download``, ``upload``, ``sdc``,
    ``drain-deferred``, or ``drain-deferred-opportunistic``.
    The pattern must match ``…-bpl+phillips-academy-download.log`` and NOT
    ``…-bpl+phillips-academy-andover-download.log`` — otherwise sibling
    labels whose names extend this one steal the log selection and the
    caller sticks on the wrong target. See lessons.md
    "Log filename phase detection".

    ``drain-deferred`` is included so the status reporter can see the
    post-SDC deferred-tagging phase — a session that has completed
    upload+SDC and moved on to draining its Case-2 duplicate-tag
    sidecar was previously invisible here and misreported as
    ``SDC complete`` while actually holding a host-level flock and
    polling ``Category:Duplicate`` capacity.
    """
    return (
        rf"-{re.escape(label)}-"
        r"(download|upload|sdc|drain-deferred(?:-opportunistic)?)"
        r"\.log$"
    )


def find_active_label(client, labels: list[str]) -> tuple[str, int] | None:
    """Return ``(label, log_mtime)`` for the most-recently-written log file
    across all ``labels`` in a chained session, or ``None`` if no matching
    log exists yet.

    A wikimedia-upload session runs its labels sequentially (downloader →
    uploader → sdc-sync per label, then on to the next label), so at any
    moment **at most one label is active**. The freshest log file across
    all labels in the session uniquely identifies that label — an aborted
    earlier label's last log write is hours stale, while the running one
    is being written right now.

    Picking the active label this way takes one SSM round-trip per
    session regardless of label count. Previously the status-post script
    polled ``get_phase_and_progress`` once per label, which scaled the
    SSM round trips linearly with batch size and pushed multi-institution
    sessions past Slack's three-second slash-command ack deadline. See
    PR #325-vintage multi-institution batches accumulating 50+ labels each.
    """
    if not labels:
        return None

    # Group labels by hub so we touch each partner log directory exactly once.
    hubs = sorted({lbl.split("+")[0] for lbl in labels})
    paths = " ".join(
        shlex.quote(f"/home/ec2-user/ingest-wikimedia/{PARTNER_DIR.get(h, h)}/logs")
        for h in hubs
    )
    label_alt = "|".join(re.escape(lbl) for lbl in labels)
    # Phase alternation MUST stay in sync with
    # :func:`log_filename_pattern_for_label` — drain-deferred(-opportunistic)
    # logs are legitimate "newest phase" candidates for a session whose
    # download/upload/sdc chain finished and moved on to drain.
    cmd = (
        f"find {paths} -maxdepth 1 -type f -name '*.log' "
        f"-regextype posix-extended "
        f"-regex '.*-({label_alt})-(download|upload|sdc|drain-deferred(-opportunistic)?)\\.log' "
        f"-printf '%T@ %f\\n' 2>/dev/null | sort -rn | head -1"
    )
    out = ssm_run(client, cmd).strip()
    if not out:
        return None
    mtime_str, _, filename = out.partition(" ")
    # Identify which of our labels matched via the per-label helper —
    # anchored-pattern logic so suffix-collision (e.g.
    # ``bpl+phillips-academy`` vs ``bpl+phillips-academy-andover``) is
    # handled exactly once.
    for lbl in labels:
        if re.search(log_filename_pattern_for_label(lbl), filename):
            return lbl, int(float(mtime_str))
    return None


def active_and_upcoming_labels(ssm, labels: list[str]) -> set[str]:
    """Return the subset of ``labels`` that a chained session hasn't yet
    completed — the currently-active label plus everything after it in
    the chain-run order.

    A wikimedia-upload tmux session runs its targets sequentially. Once
    a target's ``sdc-sync`` phase finishes and the ``&&`` chain moves on,
    that target is done; a new incoming request naming that same target
    is NOT a conflict and should be allowed to run alongside the ongoing
    session. Pre-fix, the launcher's conflict check naively compared
    against **every** label in the tmux session name, causing a 54-target
    chained session to block every new request naming any of its 54
    institutions — even institutions whose target completed hours ago.

    Uses :func:`find_active_label`'s log-mtime heuristic: the freshest
    log across the label set identifies the currently-running target;
    everything at or after that position is still upcoming, everything
    before is done. If no log exists yet (session is in id-generation
    for its first target) OR the lookup raises (transient SSM error),
    fall back to treating all labels as active — the conservative
    choice, so a failed lookup can't silently let a real conflict
    through. The exception path is logged at warning level so silent
    regressions to the old over-conflicting behavior are visible in
    operator diagnostics.
    """
    if not labels:
        return set()
    try:
        active = find_active_label(ssm, labels)
    except Exception as e:
        logging.warning(
            "active_and_upcoming_labels: find_active_label raised %r; "
            "falling back to conservative all-labels conflict scope. "
            "Some completed targets may over-conflict until the SSM "
            "lookup recovers.",
            e,
        )
        return set(labels)
    if active is None:
        return set(labels)
    try:
        start_idx = labels.index(active[0])
    except ValueError:
        # ``find_active_label`` returned a label not in the input list —
        # shouldn't happen (its selection loop only returns labels from
        # this list), but treat as unknown → conservative all-labels.
        return set(labels)
    return set(labels[start_idx:])
