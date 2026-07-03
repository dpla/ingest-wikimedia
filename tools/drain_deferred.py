"""Drain the per-partner deferred-drain sidecar.

The uploader defers Case-2 hash-drift upload+tag operations as an
atomic unit whenever ``Category:Duplicate`` on Commons is at capacity,
persisting the deferred DPLA IDs to a per-partner sidecar (see
``ingest_wikimedia.drain_sidecar``). This command runs as the final
step of the wikimedia-upload pipeline:

    cd → get-ids-es → downloader → uploader → sdc-sync → drain-deferred

If the sidecar is empty (or missing), the command exits immediately —
the common case where no items deferred is a no-op.

If the sidecar is non-empty, the command:

  1. Acquires a host-level ``flock`` at
     ``/home/ec2-user/ingest-wikimedia/.drain-lock`` so only one drain
     runs concurrently on the box. The existing throttle's per-session
     ``try_acquire`` cap on Case-2 tag emissions was designed for a
     single active writer; letting multiple sessions drain in parallel
     could overshoot Category:Duplicate's threshold before either
     session re-queried. The flock serialises the work; other
     drain-deferred processes wait their turn.
  2. Enters a loop that patiently polls Category:Duplicate (every
     ``DEFAULT_POLL_SECS`` = 5 min by default) until the size falls
     below the resume threshold. There is no time budget — this can
     wait days or weeks. Volunteers clear the category on human-admin
     timescales and this command is designed to be as patient as they
     are.
  3. When capacity is available, removes the round's IDs from the
     sidecar and re-invokes ``uploader`` and ``sdc-sync`` (as
     subprocesses) on them. The uploader only ever *merges* deferred
     IDs into the sidecar (it never removes completed ones), so the
     drain clears the round up front and the uploader's re-run merges
     back whichever items re-deferred — those stay queued for the next
     loop iteration; completed items stay out.
  4. Loops until the sidecar is empty.

Cancellation is operator-driven — ``tmux kill-session`` at any time.
The sidecar persists across kills, so a subsequent partner run picks
up wherever this left off.
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
from ingest_wikimedia.wikimedia import get_site
from ingest_wikimedia.slack import (
    notify_drain_phase_complete,
    notify_drain_phase_start,
)

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
    with tempfile.NamedTemporaryFile(
        "w",
        dir=partner,
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
            cwd=partner,
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
            cwd=partner,
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


@click.command()
@click.argument("partner")
def main(partner: str) -> None:
    """Drain the deferred-drain sidecar for ``partner`` — waits
    indefinitely on ``Category:Duplicate`` capacity, retrying deferred
    uploads until the sidecar is empty. See module docstring."""
    setup_logging(partner, "drain-deferred", logging.INFO)

    initial_ids = drain_sidecar.read_sidecar(partner)
    if not initial_ids:
        logging.info(
            "Drain-deferred: sidecar for partner %s is empty (or missing); nothing to do.",
            partner,
        )
        return

    logging.info(
        "Drain-deferred: sidecar for partner %s has %d item(s) queued; "
        "acquiring host lock and beginning drain loop.",
        partner,
        len(initial_ids),
    )

    # Advisory host-level lock. Held for the entire drain (potentially
    # days). The ``finally`` below closes the fd (releasing the flock)
    # on normal exit or exception; a kill releases it on process exit.
    lock_fd = _acquire_host_lock()
    try:
        started_at = time.monotonic()
        # ``get_site()`` — same helper the uploader/retirer/fix-categories
        # tools use; also runs ``site.login()``. The throttle requires a
        # non-None site (see :class:`DuplicateCategoryThrottle` guard).
        throttle = DuplicateCategoryThrottle(get_site())
        initial_category_size = throttle.category_size()
        notify_drain_phase_start(partner, len(initial_ids), initial_category_size)

        total_emitted = 0
        while True:
            pending = drain_sidecar.read_sidecar(partner)
            if not pending:
                break
            logging.info(
                "Drain-deferred: %d item(s) still pending; waiting for "
                "Category:Duplicate to drop below %d (currently polled every %ds).",
                len(pending),
                throttle.resume_below,
                throttle.poll_secs,
            )
            # Unlimited wait — patient by design. See module docstring.
            throttle.wait_for_capacity(max_wait_secs=None)
            pre_round_count = len(pending)
            # Take this round's IDs out of the sidecar BEFORE the
            # subprocess pass. The uploader only ever merges deferred IDs
            # back in — it never removes completed ones — so leaving the
            # round's IDs in place would make the queue permanent:
            # completed items would replay every round and the loop would
            # never terminate. Items the uploader re-defers reappear via
            # its ``merge_sidecar``; completed (or hard-failed) items stay
            # out. IDs a concurrent session appends mid-round are
            # untouched and picked up on the next loop iteration.
            drain_sidecar.remove_from_sidecar(partner, pending)
            _run_deferred_items(partner, pending)
            post_round = drain_sidecar.read_sidecar(partner)
            emitted_this_round = pre_round_count - len(post_round)
            if emitted_this_round > 0:
                total_emitted += emitted_this_round
                logging.info(
                    "Drain-deferred: round emitted %d item(s); %d still pending.",
                    emitted_this_round,
                    len(post_round),
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
                    len(post_round),
                )

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
