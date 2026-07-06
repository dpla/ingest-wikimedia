"""Drain the per-partner deferred-drain sidecar.

The uploader defers Case-2 hash-drift upload+tag operations as an
atomic unit whenever ``Category:Duplicate`` on Commons is at capacity,
persisting the deferred DPLA IDs to a per-partner sidecar (see
``ingest_wikimedia.drain_sidecar``). The drain-deferred command comes
in two modes:

  * **Patient (default)** — the terminal phase of a batch. Acquires a
    host-level ``flock`` and loops until the sidecar is empty,
    polling ``Category:Duplicate`` (every ``DEFAULT_POLL_SECS`` = 5
    min by default) with no time budget — designed to wait days or
    weeks on human-admin category clearing. Emits Slack
    start/complete notifications so the operator knows the session
    is in this state.

  * **``--no-wait`` (opportunistic)** — the interstitial phase
    embedded in each target's upload chain. Runs a single best-effort
    round: if ``Category:Duplicate`` is currently below the
    throttle's resume threshold, drain what fits and return; if it's
    at capacity, exit immediately without waiting. Never blocks
    subsequent targets in the batch. No Slack notifications — this
    pass is a bonus, not a milestone. Items that don't clear here
    stay in the sidecar for the batch's final patient drain.

Cancellation is operator-driven — ``tmux kill-session`` at any time.
The sidecar persists across kills, so a subsequent partner run picks
up wherever this left off.

See the launcher (``scripts/wikimedia_launch.py``) for the chain
ordering: per-target opportunistic drains run inside each target's
chain (so a partner whose Category:Duplicate happened to clear
mid-run gets its deferrals actioned before the next partner starts),
and a per-partner patient drain runs at the end of the whole batch
(so no partner sits idle waiting on Commons volunteers).
"""

from __future__ import annotations

import fcntl
import logging
import os
import subprocess
import tempfile
import time
from pathlib import Path

import click

from ingest_wikimedia import drain_sidecar
from ingest_wikimedia.dup_throttle import DuplicateCategoryThrottle
from ingest_wikimedia.logs import setup_logging
from ingest_wikimedia.slack import (
    notify_drain_phase_complete,
    notify_drain_phase_start,
)
from ingest_wikimedia.wikimedia import get_site

# Host-level lock file. One drain-deferred process at a time across the
# shared EC2 instance — see module docstring for the concurrency
# rationale. Path is host-scoped (not partner-scoped) so cross-partner
# drains serialize.
_DRAIN_LOCK_PATH = "/home/ec2-user/ingest-wikimedia/.drain-lock"


def _acquire_host_lock():
    """Return a file descriptor holding an exclusive ``flock`` on
    ``_DRAIN_LOCK_PATH``. Blocks until acquired.

    Advisory ``flock`` is released automatically on process exit, so a
    crashed drain doesn't leave a stuck lock. The lock file itself is
    created (empty) if missing; it never grows.
    """
    Path(_DRAIN_LOCK_PATH).parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(_DRAIN_LOCK_PATH, os.O_RDWR | os.O_CREAT, 0o644)
    logging.info(
        "Acquiring drain-phase host lock at %s (blocking until available)…",
        _DRAIN_LOCK_PATH,
    )
    fcntl.flock(fd, fcntl.LOCK_EX)
    logging.info("Drain-phase host lock acquired.")
    return fd


def _run_deferred_items(partner: str, dpla_ids: list[str]) -> None:
    """Write ``dpla_ids`` to a temp CSV and invoke ``uploader`` +
    ``sdc-sync`` on that CSV, both as subprocesses. Each subprocess
    inherits our environment, so DPLA_SLACK_BOT_TOKEN and the
    pywikibot user-config resolve the same as they do in a normal
    partner run.

    Errors from either subprocess are logged but not re-raised — a
    partial-success round leaves the still-deferred items in the
    sidecar for the next loop iteration, which is the intended
    behaviour.
    """
    partner_base = drain_sidecar.partner_dir_path(partner)
    with tempfile.NamedTemporaryFile(
        "w",
        dir=partner_base,
        prefix=".drain-ids-",
        suffix=".csv",
        delete=False,
    ) as tf:
        for dpla_id in dpla_ids:
            tf.write(dpla_id + "\n")
        csv_path = tf.name
    try:
        logging.info(
            "Drain round: invoking uploader on %d deferred item(s) via %s",
            len(dpla_ids),
            csv_path,
        )
        result = subprocess.run(
            ["uploader", os.path.basename(csv_path), partner],
            cwd=partner_base,
            check=False,
        )
        if result.returncode != 0:
            logging.warning(
                "Drain round: uploader exited %d for partner %s (ids file %s); "
                "any still-deferred items remain in the sidecar for the next round.",
                result.returncode,
                partner,
                csv_path,
            )
        logging.info(
            "Drain round: invoking sdc-sync on the same %d item(s)",
            len(dpla_ids),
        )
        result = subprocess.run(
            [
                "sdc-sync",
                "--partner",
                partner,
                "--ids-file",
                os.path.basename(csv_path),
            ],
            cwd=partner_base,
            check=False,
        )
        if result.returncode != 0:
            logging.warning(
                "Drain round: sdc-sync exited %d for partner %s (ids file %s).",
                result.returncode,
                partner,
                csv_path,
            )
    finally:
        try:
            os.unlink(csv_path)
        except OSError as exc:
            # Non-fatal — a stray temp ids file in the partner dir is only
            # cosmetic — but record it rather than failing silently.
            logging.warning(
                "Drain round: could not remove temp ids file %s: %s", csv_path, exc
            )


def _run_one_round(partner: str, pending: list[str]) -> int:
    """Execute one drain round: clear ``pending`` from the sidecar,
    invoke uploader + sdc-sync on those IDs, return how many the
    round emitted (``pre - post``).

    Taking the round's IDs out BEFORE the subprocess is load-bearing
    — the uploader only ever *merges* deferred IDs back in, never
    removes completed ones. Leaving the round's IDs in place would
    make the queue permanent: completed items would replay every
    round and the loop would never terminate. Items the uploader
    re-defers reappear via its ``merge_sidecar``; completed (or
    hard-failed) items stay out. IDs a concurrent session appends
    mid-round are untouched and picked up on the next round's read.
    """
    pre_count = len(pending)
    drain_sidecar.remove_from_sidecar(partner, pending)
    _run_deferred_items(partner, pending)
    post_round = drain_sidecar.read_sidecar(partner)
    return pre_count - len(post_round)


def _drain_loop(partner: str, throttle: DuplicateCategoryThrottle) -> int:
    """Patient drain loop for ``partner``. Blocks on
    :meth:`DuplicateCategoryThrottle.wait_for_capacity` between rounds;
    returns the total number of items emitted (drained from the
    sidecar) once it's empty. See module docstring for the rationale
    on the unbounded wait."""
    total_emitted = 0
    while True:
        pending = drain_sidecar.read_sidecar(partner)
        if not pending:
            return total_emitted
        logging.info(
            "Drain-deferred: %d item(s) still pending; waiting for "
            "Category:Duplicate to drop below %d (currently polled every %ds).",
            len(pending),
            throttle.resume_below,
            throttle.poll_secs,
        )
        # Unlimited wait — patient by design. See module docstring.
        throttle.wait_for_capacity(max_wait_secs=None)
        emitted = _run_one_round(partner, pending)
        if emitted > 0:
            total_emitted += emitted
            logging.info(
                "Drain-deferred: round emitted %d item(s); %d still pending.",
                emitted,
                len(pending) - emitted,
            )
        else:
            # A round that made no progress despite reported capacity is
            # not fatal here (unlike the old bounded drain that would
            # abort). Category:Duplicate may have refilled while we were
            # processing, or the round hit unrelated per-item failures.
            # Loop back to wait_for_capacity; next round will re-observe.
            logging.warning(
                "Drain-deferred: round made no progress (%d item(s) still "
                "pending). Continuing to wait; category may have refilled.",
                len(pending),
            )


def _drain_opportunistic_once(partner: str, throttle: DuplicateCategoryThrottle) -> int:
    """Single best-effort drain round for ``partner``. If
    ``Category:Duplicate`` is below the resume threshold RIGHT NOW,
    drain what's queued; if it's at capacity, exit immediately without
    waiting. Never blocks the caller.

    Returns the number of items drained (0 if the round didn't fire,
    positive if it did).

    The opportunistic pass exists so a partner whose category clears
    mid-batch (a Commons volunteer processes some entries between the
    partner's upload phase and end-of-batch) gets its deferrals
    actioned as part of its own chain — before the next target starts
    — rather than waiting on the batch's terminal patient drain.
    """
    pending = drain_sidecar.read_sidecar(partner)
    if not pending:
        return 0
    if not throttle.wait_for_capacity(max_wait_secs=0):
        logging.info(
            "Drain-deferred (opportunistic): Category:Duplicate at capacity; "
            "%d item(s) remain in sidecar for the batch's terminal drain.",
            len(pending),
        )
        return 0
    emitted = _run_one_round(partner, pending)
    logging.info(
        "Drain-deferred (opportunistic): emitted %d item(s); %d still pending.",
        emitted,
        len(pending) - emitted,
    )
    return emitted


@click.command()
@click.option(
    "--no-wait",
    is_flag=True,
    help=(
        "Best-effort single-round drain: if Category:Duplicate is at capacity, "
        "exit immediately rather than waiting. For the per-target opportunistic "
        "phase; the batch's terminal patient drain runs without this flag."
    ),
)
@click.argument("partner")
def main(no_wait: bool, partner: str) -> None:
    """Drain the deferred-drain sidecar for ``partner``. See module docstring."""
    event_type = "drain-deferred-opportunistic" if no_wait else "drain-deferred"
    setup_logging(partner, event_type, logging.INFO)

    initial_ids = drain_sidecar.read_sidecar(partner)
    if not initial_ids:
        logging.info(
            "Drain-deferred: sidecar for partner %s is empty (or missing); nothing to do.",
            partner,
        )
        return

    mode_label = "opportunistic" if no_wait else "patient"
    logging.info(
        "Drain-deferred: sidecar for partner %s has %d item(s) queued; "
        "acquiring host lock (mode: %s).",
        partner,
        len(initial_ids),
        mode_label,
    )

    # Advisory host-level lock. Held for the entire drain (potentially
    # days in patient mode; usually milliseconds in opportunistic
    # mode). The ``finally`` below closes the fd (releasing the flock)
    # on normal exit or exception; a kill releases it on process exit.
    lock_fd = _acquire_host_lock()
    try:
        started_at = time.monotonic()
        # ``get_site()`` — same helper the uploader/retirer/fix-categories
        # tools use; also runs ``site.login()``. The throttle requires a
        # non-None site (see :class:`DuplicateCategoryThrottle` guard).
        throttle = DuplicateCategoryThrottle(get_site())

        if no_wait:
            _drain_opportunistic_once(partner, throttle)
            return

        # Patient mode: Slack-notify start (so the operator knows the
        # session is waiting on human-admin category clearing), loop
        # until empty, Slack-notify complete.
        initial_category_size = throttle.category_size()
        notify_drain_phase_start(partner, len(initial_ids), initial_category_size)
        total_emitted = _drain_loop(partner, throttle)
        elapsed = time.monotonic() - started_at
        logging.info(
            "Drain-deferred: complete. Emitted %d item(s) over %.0f seconds.",
            total_emitted,
            elapsed,
        )
        notify_drain_phase_complete(partner, elapsed, total_emitted)
    finally:
        os.close(lock_fd)


if __name__ == "__main__":
    main()
