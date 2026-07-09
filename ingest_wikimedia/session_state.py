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


# Valid launcher-exported ``WIKIMEDIA_SESSION_LABEL`` shapes:
# ``<hub>+<institution>`` (per-target), ``drain-<hub>`` (terminal
# partner drain), ``retry-<hub>`` (retry pipeline).
def _valid_session_label(label: str) -> bool:
    return bool(label) and ("+" in label or label.startswith(("drain-", "retry-")))


def snapshot_running_active_labels(client) -> dict[str, str]:
    """One SSM roundtrip: for every ``wikimedia-*`` tmux session, return
    the active session label read from its currently-running direct-
    child subprocess's ``WIKIMEDIA_SESSION_LABEL`` env var. Sessions
    with no running child are omitted; callers should fall back to
    :func:`find_active_label` for those.

    Direct-evidence signal: the launcher exports
    ``WIKIMEDIA_SESSION_LABEL`` on every step, so every pipeline
    subprocess inherits it. Beats :func:`find_active_label`'s log-mtime
    heuristic when two sessions share a target label — mtime can't
    tell which session's subprocess is writing the log; env-var reads
    directly attribute the write.

    Sorted numerically by ``etimes`` (elapsed seconds) ascending — the
    smallest value is the most recently started direct child, which
    is the correct pick when a step has transient helper forks
    alongside the main step. ``lstart`` output is calendar text and
    doesn't sort chronologically.
    """
    out = ssm_run(
        client,
        r"""tmux list-panes -aF '#{session_name}|#{pane_pid}' 2>/dev/null | while IFS='|' read name pane_pid; do
  case "$name" in wikimedia-*) : ;; *) continue ;; esac
  child_pid=$(ps --ppid "$pane_pid" -o pid=,etimes= 2>/dev/null | sort -k2 -n | head -1 | awk '{print $1}')
  [ -z "$child_pid" ] && continue
  label=$(tr '\0' '\n' < /proc/"$child_pid"/environ 2>/dev/null | grep -m1 '^WIKIMEDIA_SESSION_LABEL=' | cut -d= -f2-)
  [ -n "$label" ] && echo "$name|$label"
done""",
    )
    result: dict[str, str] = {}
    for line in (out or "").splitlines():
        name, sep, label = line.partition("|")
        if not sep:
            continue
        name = name.strip()
        label = label.strip()
        if name and _valid_session_label(label):
            result[name] = label
    return result


# Ordered phase suffixes in log filenames (``…-<label>-<phase>.log``). Shared by
# the two patterns below so a new phase is a one-line change; only the trailing
# optional ``-opportunistic`` differs by regex dialect (Python ``re`` uses a
# non-capturing ``(?:…)`` group; ``find``'s POSIX ERE can't, so it uses ``(…)``).
_PHASE_ALT = "id-generation|download|upload|sdc|drain-deferred"


def log_filename_pattern_for_label(label: str) -> str:
    """Anchored regex matching log filenames for exactly this label.

    Log filenames follow ``{YYYYMMDD}-{HHMMSS}-{label}-<phase>.log`` where
    ``<phase>`` is one of ``id-generation``, ``download``, ``upload``,
    ``sdc``, ``drain-deferred``, or ``drain-deferred-opportunistic``.
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
        rf"({_PHASE_ALT}(?:-opportunistic)?)"
        r"\.log$"
    )


def find_active_label(
    client,
    labels: list[str],
    session_created: int = 0,
) -> tuple[str, int] | None:
    """Return ``(label, log_mtime)`` for the most-recently-written log file
    that plausibly belongs to this session, or ``None`` if no matching
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

    ``session_created`` (Unix epoch seconds; 0 = no filter) bounds the
    lookup to log files whose mtime is at or after the tmux session's
    creation time. Without this bound, a concurrent second session that
    happens to be writing to a log file with a label THIS session's
    chain has already completed would be picked up as "active" here —
    stealing the row's identity and progress numbers. The filter is a
    strict prefix on the ``find`` command (``-newermt "@N"``); when 0
    it's omitted and behavior matches the original unbounded query.

    Also matches ``drain-<hub>`` logs for each unique hub appearing in
    ``labels``. The launcher's terminal partner-level drain step
    exports ``WIKIMEDIA_SESSION_LABEL=drain-<partner>``, so its log
    file is ``…-drain-<partner>-drain-deferred.log`` — that name is
    NOT one of the per-target labels callers pass, and before this
    change the reporter's "which label is active" lookup silently
    ignored it, misreporting sessions in terminal drain as
    ``SDC complete``. On a match the synthetic ``drain-<hub>`` label
    is returned; downstream callers can either display it as-is or
    strip the prefix for the row label.
    """
    if not labels:
        return None

    # Group labels by hub so we touch each partner log directory exactly once.
    hubs = sorted({lbl.split("+")[0] for lbl in labels})
    paths = " ".join(
        shlex.quote(f"/home/ec2-user/ingest-wikimedia/{PARTNER_DIR.get(h, h)}/logs")
        for h in hubs
    )
    # Include synthetic ``drain-<hub>`` alternatives so the terminal
    # partner-level drain step (launcher exports
    # ``WIKIMEDIA_SESSION_LABEL=drain-<partner>`` before that step) is
    # visible here. Combined regex alternation avoids doubling the
    # SSM round trip count.
    drain_hub_labels = [f"drain-{h}" for h in hubs]
    label_alt = "|".join(re.escape(lbl) for lbl in [*labels, *drain_hub_labels])
    # Phase alternation MUST stay in sync with
    # :func:`log_filename_pattern_for_label` — drain-deferred(-opportunistic)
    # logs are legitimate "newest phase" candidates for a session whose
    # download/upload/sdc chain finished and moved on to drain.
    cmd_parts = [
        f"find {paths} -maxdepth 1 -type f -name '*.log'",
        "-regextype posix-extended",
        f"-regex '.*-({label_alt})-({_PHASE_ALT}(-opportunistic)?)\\.log'",
    ]
    if session_created > 0:
        # Time-bound the lookup to files created after this session's
        # tmux session began. Guards against a concurrent second session
        # incidentally writing to one of THIS session's already-completed
        # labels (and thereby stealing the "active label" position).
        cmd_parts.append(f"-newermt '@{session_created}'")
    cmd_parts.append("-printf '%T@ %f\\n' 2>/dev/null | sort -rn | head -1")
    cmd = " ".join(cmd_parts)
    out = ssm_run(client, cmd).strip()
    if not out:
        return None
    mtime_str, _, filename = out.partition(" ")
    # Identify which of our labels matched via the per-label helper —
    # anchored-pattern logic so suffix-collision (e.g.
    # ``bpl+phillips-academy`` vs ``bpl+phillips-academy-andover``) is
    # handled exactly once. Try synthetic drain-hub labels first because
    # a ``drain-<hub>`` file name may otherwise be matched against a
    # per-target label whose prefix coincidentally aligns.
    for lbl in [*drain_hub_labels, *labels]:
        if re.search(log_filename_pattern_for_label(lbl), filename):
            return lbl, int(float(mtime_str))
    return None


def active_and_upcoming_labels(
    ssm,
    labels: list[str],
    session_created: int = 0,
    active_label: str | None = None,
) -> set[str]:
    """Return the subset of ``labels`` a chained session hasn't yet
    completed — the currently-active label plus everything after it
    in chain-run order.

    A wikimedia-upload session runs its targets sequentially, so a
    target the session has passed cannot conflict with a new request
    naming that same target. Pre-fix, the launcher's conflict check
    naively compared against every label in the tmux session name,
    causing a 54-target chained session to block every new request
    naming any of its 54 institutions — even institutions whose target
    completed hours ago.

    ``active_label`` (from
    :func:`snapshot_running_active_labels`) is the direct-evidence
    signal — one ``WIKIMEDIA_SESSION_LABEL`` env-var read from the
    session's running child. When ``None``, falls back to
    :func:`find_active_label`'s log-mtime heuristic
    (``session_created``-bounded so a concurrent session's write can't
    steal identity). Both a ``drain-<hub>`` active label and a lookup
    that finds no log yet return the empty and all-labels sets
    respectively; a transient SSM error is logged and treated as the
    latter so a lookup failure can't silently let a real conflict
    through.
    """
    if not labels:
        return set()
    if active_label is None:
        try:
            found = find_active_label(ssm, labels, session_created=session_created)
        except Exception as e:
            logging.warning(
                "active_and_upcoming_labels: find_active_label raised %r; "
                "falling back to conservative all-labels conflict scope.",
                e,
            )
            return set(labels)
        if found is None:
            return set(labels)
        active_label = found[0]
    if active_label.startswith("drain-"):
        return set()
    try:
        start_idx = labels.index(active_label)
    except ValueError:
        return set(labels)
    return set(labels[start_idx:])
