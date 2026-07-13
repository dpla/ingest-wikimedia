"""Drain the per-partner deferred queues by re-running the uploader.

Two independent queues, both per-partner, both drained by the same
mechanism — **re-invoke the idempotent uploader (and sdc-sync) on the
queued DPLA IDs until the queue empties.** The uploader re-derives all
state from live S3 / Commons on each run, so the drain holds no per-item
logic of its own.

  * **Category-capacity queue** (``ingest_wikimedia.drain_sidecar``) —
    Case-2 hash-drift upload+tag operations the uploader deferred
    because ``Category:Duplicate`` was at capacity. Drained only while
    the category is below its resume threshold (the throttle gate).

  * **Await-target-free set** (``ingest_wikimedia.await_target_free_sidecar``)
    — Case-2 community-target items where we uploaded our bytes to the
    DPLA-canonical title and self-tagged ``{{Duplicate|<community>}}``,
    now waiting on a Commons admin to delete or redirect our tagged
    file. A re-run of the uploader resolves whatever the admin did:
    once the canonical title is freed, the empty-canonical Case-3
    title-drift move promotes the community file into it (history
    intact); if the admin removed the tag instead (declining), the
    uploader drops the key and the two files coexist. No capacity gate
    — an awaiting re-run emits no new tag, so category size is
    irrelevant to it.

The command has two modes:

  * **Patient (default)** — the terminal phase of a batch. Drains the
    category queue (waiting on ``Category:Duplicate`` with no time
    budget) then the await set (waiting on Commons admins), looping for
    as long as it takes. Emits Slack start/complete notifications.

  * **``--no-wait`` (opportunistic)** — the interstitial phase in each
    target's chain. One best-effort round of each queue; never blocks.

Concurrency: uploader/sdc-sync invocations across the box are serialized
by a host-level ``flock`` (``_DRAIN_LOCK_PATH``) held only while a round
runs — released across the patient waits so one partner's multi-day wait
never blocks another's drain. Cancellation is operator-driven
(``tmux kill-session``); both queues persist across kills.
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

from ingest_wikimedia import await_target_free_sidecar, drain_sidecar
from ingest_wikimedia.dup_throttle import DuplicateCategoryThrottle
from ingest_wikimedia.logs import setup_logging
from ingest_wikimedia.slack import (
    notify_drain_phase_complete,
    notify_drain_phase_start,
)
from ingest_wikimedia.wikimedia import get_site

# Host-level lock file. One uploader-heavy drain round at a time across
# the shared EC2 instance, so concurrent rounds don't oversubscribe
# Commons. Path is host-scoped (not partner-scoped) so cross-partner
# drains serialize. Held only for the duration of a round — never across
# a patient wait.
_DRAIN_LOCK_PATH = "/home/ec2-user/ingest-wikimedia/.drain-lock"


def _acquire_host_lock(blocking: bool = True):
    """Return a file descriptor holding an exclusive ``flock`` on
    ``_DRAIN_LOCK_PATH``, or ``None`` when ``blocking=False`` and another
    drain already holds it.

    Advisory ``flock`` is released automatically on process exit, so a
    crashed drain doesn't leave a stuck lock. The lock file itself is
    created (empty) if missing; it never grows.

    ``blocking=True`` (patient drain) waits until the lock frees.
    ``blocking=False`` (opportunistic ``--no-wait`` pass) tries once
    (``LOCK_NB``) and returns ``None`` if held: the opportunistic pass is
    fire-and-forget and must never block its target's chain behind
    another drain.
    """
    Path(_DRAIN_LOCK_PATH).parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(_DRAIN_LOCK_PATH, os.O_RDWR | os.O_CREAT, 0o644)
    flags = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
    logging.info(
        "Acquiring drain-phase host lock at %s (%s)…",
        _DRAIN_LOCK_PATH,
        "blocking until available" if blocking else "non-blocking, skip if held",
    )
    try:
        fcntl.flock(fd, flags)
    except BlockingIOError:
        os.close(fd)
        return None
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


# --------------------------------------------------------------------------
# Category-capacity queue (drain_sidecar)
# --------------------------------------------------------------------------


def _run_one_round(partner: str, pending: list[str]) -> int:
    """Execute one category-queue round: clear ``pending`` from the
    sidecar, invoke uploader + sdc-sync on those IDs, return how many the
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
    """Patient category-queue loop for ``partner``. Blocks on
    :meth:`DuplicateCategoryThrottle.wait_for_capacity` between rounds;
    returns the total number of items emitted once the sidecar is empty.
    Caller holds the host lock for the duration."""
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
            # not fatal. Category:Duplicate may have refilled while we
            # were processing, or the round hit unrelated per-item
            # failures. Loop back to wait_for_capacity; next round
            # re-observes.
            logging.warning(
                "Drain-deferred: round made no progress (%d item(s) still "
                "pending). Continuing to wait; category may have refilled.",
                len(pending),
            )


def _drain_opportunistic_once(partner: str, throttle: DuplicateCategoryThrottle) -> int:
    """Single best-effort category-queue round for ``partner``. If
    ``Category:Duplicate`` is below the resume threshold RIGHT NOW, drain
    what's queued; if it's at capacity, exit immediately. Never blocks.
    Returns the number of items drained (0 if the round didn't fire)."""
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


# --------------------------------------------------------------------------
# Await-target-free set
# --------------------------------------------------------------------------
#
# Draining this set is *exactly* the category-queue pattern — re-run the
# idempotent uploader on the queued DPLA IDs — with two differences:
#   * no Category:Duplicate capacity gate (an awaiting re-run emits no
#     new tag; it only moves the community file into a freed canonical
#     title, skips while still waiting, or drops a declined key), and
#   * the uploader OWNS the set (it adds/removes keys as it resolves each
#     ordinal), so the drain never removes-before-run; it just re-runs
#     and re-reads until the set empties.


def _run_await_round(partner: str, blocking: bool) -> bool:
    """Run one await round: re-invoke the uploader + sdc-sync on the
    DPLA IDs with at least one awaiting ordinal, under the host lock.

    Returns ``True`` if the round ran, ``False`` if the set was empty or
    (opportunistic) the host lock was held by another drain. The host
    lock is acquired only for the round and released before returning, so
    a patient await wait never holds it across sleeps.
    """
    ids = await_target_free_sidecar.awaiting_dpla_ids(partner)
    if not ids:
        return False
    lock_fd = _acquire_host_lock(blocking=blocking)
    if lock_fd is None:
        logging.info(
            "Await drain (opportunistic): host lock held by another drain; "
            "skipping — %d item(s) remain awaiting for the terminal drain.",
            len(ids),
        )
        return False
    try:
        logging.info(
            "Await drain: re-running uploader on %d awaiting item(s) for %s.",
            len(ids),
            partner,
        )
        _run_deferred_items(partner, ids)
    finally:
        os.close(lock_fd)
    return True


def _drain_await_patient(partner: str, throttle: DuplicateCategoryThrottle) -> None:
    """Patient await-target-free loop: re-run the uploader on awaiting
    items until the set empties, sleeping ``throttle.poll_secs`` between
    rounds while items remain (they're waiting on a Commons admin).

    Terminates only when the set is empty — the uploader clears each key
    as it resolves (admin deletion → Case-3 move; admin de-tag → key
    dropped). A key whose admin never acts keeps the loop alive by
    design (same unbounded-patience contract as the category loop);
    the operator ends it via ``tmux kill-session`` (the set persists).
    """
    while True:
        if not await_target_free_sidecar.awaiting_dpla_ids(partner):
            return
        _run_await_round(partner, blocking=True)
        remaining = await_target_free_sidecar.awaiting_dpla_ids(partner)
        if not remaining:
            return
        logging.info(
            "Await drain: %d item(s) for %s still awaiting Commons admin "
            "action; sleeping %ds before the next round.",
            len(remaining),
            partner,
            int(throttle.poll_secs),
        )
        time.sleep(throttle.poll_secs)


@click.command()
@click.option(
    "--no-wait",
    is_flag=True,
    help=(
        "Best-effort single round of each queue: if Category:Duplicate is at "
        "capacity (or the host lock is held) skip rather than waiting. For the "
        "per-target opportunistic phase; the batch's terminal drain runs "
        "without this flag."
    ),
)
@click.argument("partner")
def main(no_wait: bool, partner: str) -> None:
    """Drain the deferred queues for ``partner``. See module docstring."""
    event_type = "drain-deferred-opportunistic" if no_wait else "drain-deferred"
    setup_logging(partner, event_type, logging.INFO)

    initial_ids = drain_sidecar.read_sidecar(partner)
    initial_await = await_target_free_sidecar.awaiting_dpla_ids(partner)
    if not initial_ids and not initial_await:
        logging.info(
            "Drain-deferred: both queues for partner %s are empty (or missing); "
            "nothing to do.",
            partner,
        )
        return

    mode_label = "opportunistic" if no_wait else "patient"
    logging.info(
        "Drain-deferred: partner %s has %d category-capacity item(s) and %d "
        "await-target-free item(s) queued (mode: %s).",
        partner,
        len(initial_ids),
        len(initial_await),
        mode_label,
    )

    # ``get_site()`` also runs ``site.login()``; the throttle requires a
    # non-None site (see :class:`DuplicateCategoryThrottle` guard).
    site = get_site()
    throttle = DuplicateCategoryThrottle(site)

    if no_wait:
        # One best-effort round of each queue, each acquiring the host
        # lock non-blocking (skip if a peer holds it). The two queues are
        # independent: a held lock or a full category doesn't stop the
        # await round from being attempted.
        if initial_ids:
            lock_fd = _acquire_host_lock(blocking=False)
            if lock_fd is None:
                logging.info(
                    "Drain-deferred (opportunistic): host lock held; skipping "
                    "the category round — %d item(s) remain for the terminal "
                    "drain.",
                    len(initial_ids),
                )
            else:
                try:
                    _drain_opportunistic_once(partner, throttle)
                finally:
                    os.close(lock_fd)
        _run_await_round(partner, blocking=False)
        return

    # Patient mode. Slack-notify start only when there's category work to
    # announce (the start ping carries the category size / deferred
    # count). Each phase acquires the host lock only while a round runs.
    started_at = time.monotonic()
    notified_start = False
    total_emitted = 0
    if initial_ids:
        lock_fd = _acquire_host_lock(blocking=True)
        try:
            notify_drain_phase_start(
                partner, len(initial_ids), throttle.category_size()
            )
            notified_start = True
            total_emitted = _drain_loop(partner, throttle)
        finally:
            os.close(lock_fd)

    _drain_await_patient(partner, throttle)

    # Only claim completion when BOTH queues are actually empty. The await
    # loop returns only when its set is empty, but the category sidecar
    # could have been re-populated by a concurrent session; check both.
    remaining_ids = drain_sidecar.read_sidecar(partner)
    remaining_await = await_target_free_sidecar.awaiting_dpla_ids(partner)
    elapsed = time.monotonic() - started_at
    if remaining_ids or remaining_await:
        logging.warning(
            "Drain-deferred: finished this pass with %d category + %d await "
            "item(s) still queued for partner %s; NOT signalling completion.",
            len(remaining_ids),
            len(remaining_await),
            partner,
        )
    elif notified_start:
        logging.info(
            "Drain-deferred: complete. Emitted %d item(s) over %.0f seconds.",
            total_emitted,
            elapsed,
        )
        notify_drain_phase_complete(partner, elapsed, total_emitted)
    else:
        logging.info(
            "Drain-deferred: await-only drain complete for partner %s.", partner
        )


if __name__ == "__main__":
    main()
