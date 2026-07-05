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

import re
import shlex

from ingest_wikimedia.partners import PARTNER_DIR
from ingest_wikimedia.ssm import ssm_run


def log_filename_pattern_for_label(label: str) -> str:
    """Anchored regex matching log filenames for exactly this label.

    Log filenames follow ``{YYYYMMDD}-{HHMMSS}-{label}-(download|upload|sdc).log``.
    The pattern must match ``…-bpl+phillips-academy-download.log`` and NOT
    ``…-bpl+phillips-academy-andover-download.log`` — otherwise sibling
    labels whose names extend this one steal the log selection and the
    caller sticks on the wrong target. See lessons.md
    "Log filename phase detection".
    """
    return rf"-{re.escape(label)}-(download|upload|sdc)\.log$"


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
    cmd = (
        f"find {paths} -maxdepth 1 -type f -name '*.log' "
        f"-regextype posix-extended "
        f"-regex '.*-({label_alt})-(download|upload|sdc)\\.log' "
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
