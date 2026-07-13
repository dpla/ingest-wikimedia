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
import pywikibot

from ingest_wikimedia import await_target_free_sidecar, drain_sidecar
from ingest_wikimedia.csrf import with_csrf_recovery
from ingest_wikimedia.dup_throttle import DuplicateCategoryThrottle
from ingest_wikimedia.logs import setup_logging
from ingest_wikimedia.slack import (
    notify_drain_phase_complete,
    notify_drain_phase_start,
)
from ingest_wikimedia.wikimedia import (
    DUPLICATE_TAG_RE,
    build_title_drift_move_reason,
    file_has_inbound_usage,
    get_page,
    get_site,
    post_commonsdelinker_request,
)

# Host-level lock file. One drain-deferred process at a time across the
# shared EC2 instance — see module docstring for the concurrency
# rationale. Path is host-scoped (not partner-scoped) so cross-partner
# drains serialize.
_DRAIN_LOCK_PATH = "/home/ec2-user/ingest-wikimedia/.drain-lock"

# Partner-scoped stage-2 lock file name. Placed under each partner's
# working directory. Held only for the duration of a single stage-2
# poll round so multiple partners' stage-2 drains run concurrently
# (the host-wide lock is released before stage-2 begins). Serializes
# stage-2 rounds for THE SAME partner against another drain-deferred
# or a concurrent uploader that races on the same sidecar entry —
# without this, two rounds could both read an entry with
# tag_emitted=True, both call ``community_page.move``, and one would
# race a torn "no such source" error on Commons.
_STAGE2_LOCK_FILENAME = ".stage2-drain.lock"


def _acquire_host_lock(blocking: bool = True):
    """Return a file descriptor holding an exclusive ``flock`` on
    ``_DRAIN_LOCK_PATH``, or ``None`` when ``blocking=False`` and another
    drain already holds it.

    Advisory ``flock`` is released automatically on process exit, so a
    crashed drain doesn't leave a stuck lock. The lock file itself is
    created (empty) if missing; it never grows.

    ``blocking=True`` (patient/terminal drain) waits until the lock frees —
    it legitimately holds the lock for hours while a human clears
    ``Category:Duplicate``. ``blocking=False`` (opportunistic ``--no-wait``
    pass) tries once (``LOCK_NB``) and returns ``None`` if the lock is held:
    the opportunistic pass is a fire-and-forget interstitial round and must
    NOT block its target's chain behind a long-running patient drain (that
    was keeping finished sessions open for hours — see the drain-lock bug).
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


def _acquire_stage2_lock(partner: str, blocking: bool = True):
    """Acquire the partner-scoped stage-2 ``flock``. Returns a held fd
    or ``None`` when ``blocking=False`` and another drain holds it.

    Scope is one partner: stage-2 for partner A runs concurrently with
    stage-2 for partner B (they touch different sidecars and different
    Commons files). What this lock serializes is same-partner rounds —
    without it, two drain-deferred processes running stage-2 for the
    same partner could both read an entry with tag_emitted=True, both
    call community_page.move, and race on Commons.

    Companion behaviour to :func:`_acquire_host_lock`: created lazily,
    never deleted, released on fd close (so crashes free it).
    """
    partner_dir = drain_sidecar.partner_dir_path(partner)
    partner_dir.mkdir(parents=True, exist_ok=True)
    lock_path = partner_dir / _STAGE2_LOCK_FILENAME
    fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)
    flags = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
    try:
        fcntl.flock(fd, flags)
    except BlockingIOError:
        os.close(fd)
        return None
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


def _advance_await_target_free_entry(
    entry: dict, site: object
) -> tuple[bool, str | None]:
    """Attempt to advance one await-target-free entry.

    Returns ``(should_remove, note)``:

      * ``(True, None)`` — entry advanced successfully (community file
        moved into the freed DPLA-canonical title). Caller drops it.
      * ``(True, "FAIL: <reason>")`` — entry is a dead-end and must NOT
        stay queued (tag was removed by an editor without deletion,
        community file has disappeared, etc.). Caller drops it and logs
        the reason.
      * ``(False, "PENDING: <reason>")`` — entry is still waiting (tag
        still present, admin hasn't acted). Caller keeps it queued for
        the next drain round.

    The advancement action itself — moving the community file into the
    freed canonical title — mirrors ``Uploader._move_to_correct_title``
    line-for-line: the same reason string, same CommonsDelinker relink,
    same title-drift semantics. The two implementations share a
    conceptual contract (invariant satisfied post-move) even though the
    code isn't unified — a shared helper is a reasonable future
    refactor.
    """
    dpla_id = entry["dpla_id"]
    tagged_title = entry["tagged_title"]
    community_title = entry["community_title"]
    expected_sha1 = entry["expected_sha1"]

    # Workflow phase gate: an entry with tag_emitted=False was recorded
    # by the uploader but the corresponding tag_as_duplicate call never
    # completed. Drain must NOT try to advance it — the canonical file
    # carries no tag, no admin action is possible, and treating the
    # absent tag as an editor-decline (the untagged FAIL path below)
    # would prematurely drop a still-in-progress workflow. Keep the
    # entry queued; the next uploader run for this partner will retry
    # the tag and flip the phase to True.
    if not entry.get("tag_emitted", True):
        return False, (
            "PENDING: tag not yet emitted by the uploader "
            "(tag_emitted=False); leaving queued for the uploader to retry."
        )

    tagged_page = get_page(site, f"File:{tagged_title}")
    exists = tagged_page.exists()

    # State machine over the tagged page. Poll order matters:
    #   1. Doesn't exist → admin deleted, advance.
    #   2. Redirect → admin merged, advance (move_to_title handles redirects).
    #   3. Exists with tag → still waiting.
    #   4. Exists WITHOUT tag → admin (or an editor) removed the tag
    #      without deleting the page. Treat as decisive decline: drop
    #      the entry and log FAIL.
    if exists and not tagged_page.isRedirectPage():
        text = tagged_page.text or ""
        if DUPLICATE_TAG_RE.search(text):
            return False, "PENDING: tagged file still present with Duplicate template"
        return True, (
            "FAIL: tagged file still exists but {{Duplicate}} template was "
            "removed by a Commons editor without deletion — treating as a "
            "decline and giving up on the rename."
        )

    # Advance path (deletion OR redirect).
    community_page = get_page(site, f"File:{community_title}")
    if not community_page.exists():
        return True, (
            "FAIL: community file no longer exists on Commons — a third-party "
            "action removed it during the wait window."
        )

    # An admin (or an editor) may have redirected the community file
    # during the wait window — most likely, redirected it to our tagged
    # canonical. That means admin already did the merge, just in the
    # opposite direction from the one we intended. The invariant is
    # satisfied either way (our canonical holds the S3 SHA1); there is
    # no move for us to perform. Drop the entry as a decisive
    # "already-actioned" FAIL rather than blindly moving a redirect
    # page around and risking a broken redirect chain.
    if community_page.isRedirectPage():
        return True, (
            "FAIL: community file was redirected during the wait window "
            "— admin (or an editor) already merged it; there is nothing "
            "for the drain phase to move. Invariant satisfied via the "
            "existing redirect."
        )

    # Re-validate SHA1 against content drift on the community side.
    actual_sha1 = community_page.latest_file_info.sha1
    if actual_sha1 != expected_sha1:
        return True, (
            f"FAIL: community file SHA1 drifted during the wait window "
            f"(expected {expected_sha1[:16]}…, now {actual_sha1[:16]}…) — "
            f"refusing to move stale bytes into the canonical title."
        )

    # Move community file into the freed canonical title.
    intended_page = get_page(site, f"File:{tagged_title}")
    reason = build_title_drift_move_reason(
        community_title, tagged_title, dpla_id, site.user()
    )
    needs_relink = file_has_inbound_usage(site, community_title)
    logging.info(
        "Await-target-free advance for %s: moving [[File:%s]] → [[File:%s]]",
        dpla_id,
        community_title,
        tagged_title,
    )
    try:
        with_csrf_recovery(
            site,
            f"move {community_page.title()} → {intended_page.title()}",
            lambda: community_page.move(
                intended_page.title(),
                reason=reason,
                movetalk=False,
                noredirect=False,
            ),
        )
    except pywikibot.exceptions.ArticleExistsConflictError as ex:
        # A stale redirect or page history at the tagged title blocks
        # the move. The invariant is still satisfied — either the
        # redirect resolves to the community file (which holds the S3
        # SHA1) or admin left content we cannot displace. Retrying every
        # poll would spam the API with a call that cannot succeed, so
        # treat this as a decisive FAIL: drop the sidecar entry and let
        # the community file remain at its original title.
        return True, (
            f"FAIL: move to canonical title blocked by "
            f"ArticleExistsConflictError ({ex}) — community file left "
            f"in place; invariant already satisfied via the tagged "
            f"title's current state."
        )
    if needs_relink:
        post_commonsdelinker_request(
            site, community_title, tagged_title, check_usage=False
        )
    return True, None


def _process_await_target_free(partner: str, site: object) -> None:
    """Poll every await-target-free sidecar entry and advance the ones
    whose tagged file has been actioned by a Commons admin.

    Idempotent — safe to run whether patient or opportunistic. An entry
    that isn't ready this round stays queued for the next round. Same
    isolation contract as ``_run_one_round``: a per-item failure
    (network blip, pywikibot exception) is logged and the entry stays
    queued.
    """
    pending = await_target_free_sidecar.read_sidecar(partner)
    if not pending:
        return
    logging.info(
        "Await-target-free: polling %d entry(ies) for partner %s.",
        len(pending),
        partner,
    )
    advanced = 0
    failed = 0
    for entry in pending:
        try:
            should_remove, note = _advance_await_target_free_entry(entry, site)
        except Exception as ex:
            logging.warning(
                "Await-target-free: entry %s (%s → %s) raised %s; leaving queued.",
                entry["dpla_id"],
                entry["community_title"],
                entry["tagged_title"],
                ex,
            )
            continue
        if not should_remove:
            logging.info(
                "Await-target-free: entry %s (%s → %s) %s.",
                entry["dpla_id"],
                entry["community_title"],
                entry["tagged_title"],
                note,
            )
            continue
        await_target_free_sidecar.remove_entry(
            partner, entry["dpla_id"], entry["ordinal"]
        )
        if note and note.startswith("FAIL:"):
            failed += 1
            logging.warning(
                "Await-target-free: entry %s (%s → %s) dropped — %s",
                entry["dpla_id"],
                entry["community_title"],
                entry["tagged_title"],
                note,
            )
        else:
            advanced += 1
            logging.info(
                "Await-target-free: entry %s (%s → %s) advanced — community "
                "file moved into freed canonical title.",
                entry["dpla_id"],
                entry["community_title"],
                entry["tagged_title"],
            )
    logging.info(
        "Await-target-free: %d advanced, %d failed, %d still pending.",
        advanced,
        failed,
        len(pending) - advanced - failed,
    )


def _run_stage2_once(partner: str, site: object) -> None:
    """Run one stage-2 round under the partner-scoped stage-2 lock.

    Held only for the duration of the round (not across sleeps), so
    concurrent drains for OTHER partners are unaffected and same-
    partner rounds serialize cleanly. Non-blocking: if another drain
    is mid-round for this partner, skip — its work covers ours.
    """
    stage2_fd = _acquire_stage2_lock(partner, blocking=False)
    if stage2_fd is None:
        logging.info(
            "Await-target-free: partner-scoped stage-2 lock held by another "
            "drain for partner %s; skipping this round.",
            partner,
        )
        return
    try:
        _process_await_target_free(partner, site)
    finally:
        os.close(stage2_fd)


def _run_stage2_patient_loop(
    partner: str, site: object, throttle: DuplicateCategoryThrottle
) -> None:
    """Patient stage-2 wait loop: drain until the sidecar is empty,
    sleeping ``throttle.poll_secs`` between rounds.

    The partner-scoped lock is acquired PER ROUND and released before
    sleeping — so a peer drain-deferred can also run stage-2 for this
    partner during our sleep window, and neither has to wait on the
    other for a full poll cycle.

    Two distinct "pending" states, and the loop treats them
    differently:

      * ``tag_emitted=True`` — the {{Duplicate}} tag is on Commons and
        the entry is waiting on a human admin to action it. This is the
        genuine patient wait: loop and sleep, possibly for days/weeks,
        until admin acts (operator ends it via ``tmux kill-session`` if
        needed; the sidecar persists across kills).

      * ``tag_emitted=False`` — the uploader recorded the intent but the
        tag write never landed. Stage-2 CANNOT advance these and does
        NOT emit tags itself; only a subsequent uploader pass finishes
        them (via ``_resume_self_tag_if_pending``). Looping on them here
        would spin forever with no possible progress. So if EVERY
        still-pending entry is ``tag_emitted=False``, stop waiting and
        leave them queued for the next uploader run — no data loss, and
        no infinite in-batch loop.
    """
    while True:
        pending = await_target_free_sidecar.read_sidecar(partner)
        if not pending:
            break
        _run_stage2_once(partner, site)
        still_pending = await_target_free_sidecar.read_sidecar(partner)
        if not still_pending:
            break
        advanceable = [e for e in still_pending if e.get("tag_emitted", True)]
        if not advanceable:
            logging.warning(
                "Await-target-free: %d entry(ies) for partner %s are still "
                "tag_emitted=False (the {{Duplicate}} tag never landed); "
                "stage-2 cannot advance these — it does not emit tags. "
                "Leaving them queued for the next uploader run to finish "
                "rather than waiting on admin action that can't apply.",
                len(still_pending),
                partner,
            )
            break
        logging.info(
            "Await-target-free: %d entry(ies) still pending for partner "
            "%s (%d awaiting admin action); sleeping %ds before next round.",
            len(still_pending),
            partner,
            len(advanceable),
            int(throttle.poll_secs),
        )
        time.sleep(throttle.poll_secs)


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
    initial_await = await_target_free_sidecar.read_sidecar(partner)
    if not initial_ids and not initial_await:
        logging.info(
            "Drain-deferred: both sidecars for partner %s are empty (or missing); "
            "nothing to do.",
            partner,
        )
        return

    mode_label = "opportunistic" if no_wait else "patient"
    logging.info(
        "Drain-deferred: partner %s has %d category-capacity item(s) and %d "
        "await-target-free entry(ies) queued; acquiring host lock (mode: %s).",
        partner,
        len(initial_ids),
        len(initial_await),
        mode_label,
    )

    # Advisory host-level lock. The patient (terminal) drain BLOCKS until
    # it's free (it can hold the lock for hours/days while a human clears
    # Category:Duplicate). The opportunistic (--no-wait) pass acquires
    # NON-BLOCKING and skips if another drain holds it — it must never block
    # its target's chain behind a patient drain. The ``finally`` below closes
    # the fd (releasing the flock) on normal exit or exception; a kill
    # releases it on process exit.
    lock_fd = _acquire_host_lock(blocking=not no_wait)
    if lock_fd is None:
        logging.info(
            "Drain-deferred (opportunistic): host lock held by another drain; "
            "skipping this pass — %d category-capacity item(s) and %d "
            "await-target-free entry(ies) remain queued for the terminal drain.",
            len(initial_ids),
            len(initial_await),
        )
        return
    try:
        started_at = time.monotonic()
        # ``get_site()`` — same helper the uploader/retirer/fix-categories
        # tools use; also runs ``site.login()``. The throttle requires a
        # non-None site (see :class:`DuplicateCategoryThrottle` guard).
        site = get_site()
        throttle = DuplicateCategoryThrottle(site)

        if no_wait:
            _drain_opportunistic_once(partner, throttle)
            # Release the host lock before stage-2. Stage-2 talks to
            # partner-local resources only (the await sidecar and this
            # partner's Commons files); holding the host-wide lock
            # across it would prevent other partners' drains from
            # running for minutes. Serialization for same-partner
            # stage-2 rounds is handled by the partner-scoped lock.
            os.close(lock_fd)
            lock_fd = None
            _run_stage2_once(partner, site)
            return

        # Patient mode: Slack-notify start (so the operator knows the
        # session is waiting on human-admin category clearing), loop
        # until empty, Slack-notify complete.
        initial_category_size = throttle.category_size()
        notify_drain_phase_start(partner, len(initial_ids), initial_category_size)
        total_emitted = _drain_loop(partner, throttle)
        # Release the host-wide lock before entering the patient
        # stage-2 wait loop — holding it across days/weeks of polling
        # for one partner's Commons-admin action would block every
        # other partner's patient drain and force opportunistic
        # passes to skip. Stage-2 uses a partner-scoped lock instead
        # (per-round, so it doesn't hold across sleep).
        os.close(lock_fd)
        lock_fd = None
        _run_stage2_patient_loop(partner, site, throttle)
        elapsed = time.monotonic() - started_at
        logging.info(
            "Drain-deferred: complete. Emitted %d item(s) over %.0f seconds.",
            total_emitted,
            elapsed,
        )
        notify_drain_phase_complete(partner, elapsed, total_emitted)
    finally:
        if lock_fd is not None:
            os.close(lock_fd)


if __name__ == "__main__":
    main()
