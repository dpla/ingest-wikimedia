#!/usr/bin/env python3
"""Launch a Wikimedia upload pipeline session on EC2 for one or more partner hubs.

Runs as a GitHub Actions workflow step triggered by workflow_dispatch or the
/wikimedia-upload Slack slash command via Lambda. Updates EC2 code, checks for
conflicting sessions, launches the full pipeline in a single tmux session (with
all targets run sequentially), and posts a Slack confirmation to #tech-alerts.

Each target in --partner is a hub slug ("bpl"), a hub|institution pair
("indiana|Indiana State Library"), a hub|institution|collection triple
("bpl|Digital Commonwealth|Boston City Archives"), a hub||collection triple with
an empty institution slot ("nara||General Records of the United States
Government") that matches the collection across every upload-eligible
institution in the hub, a Wikidata QID ("Q1234567"), or a 32-hex-char DPLA item
ID ("abc123def456789012345678901234ab").  Multiple
targets run sequentially in one tmux session; a failing target posts a Slack
error and continues with the next target.

For DPLA item ID targets the ID generation phase is skipped — the ID is written
directly to the CSV.  Eligibility (rights statement + media presence) is verified
on EC2 before launch.  Per-phase Slack notifications are suppressed; only one
launch message and one completion message are posted.

Environment variables:
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY  — IAM credentials with ssm:SendCommand
  DPLA_SLACK_BOT_TOKEN                       — optional; skips Slack post if absent
"""

import argparse
import logging
import os
import shlex
import sys
from typing import NoReturn

import boto3
import requests

from ingest_wikimedia.partners import (
    PARTNER_DIR,
    commons_has_files_for_qid,
    is_dpla_id,
    is_upload_eligible,
    is_wikidata_id,
    parse_session_labels,
    resolve_commons_category,
    resolve_slug,
    resolve_wikidata_id,
    slugify_session_label_component,
    wikidata_qid_for_target,
)
from ingest_wikimedia.session_state import find_active_label
from ingest_wikimedia.slack import post_message
from ingest_wikimedia.ssm import REGION, ssm_run, stage_and_launch_tmux

# Each ingest session peaks at ~300–500 MB; 30% of 7.6 GB leaves headroom for 4–5 concurrent sessions.
MEMORY_HEADROOM_PCT = 30

# Tmux-safe session-label slugifier (lowercase alphanumeric + hyphens) lives in
# ingest_wikimedia.partners as slugify_session_label_component so that
# wikimedia_kill.py uses the IDENTICAL function — otherwise institutions whose
# names contain stripped characters (e.g. ``AT&T``) would launch under one
# slug and never match on kill.  Keep a short local alias for readability of
# the call sites below.
_slugify = slugify_session_label_component


def _parse_bool(value: str) -> bool:
    """Parse a GitHub Actions boolean-string input into a real bool.

    GH Actions passes ``workflow_dispatch`` boolean inputs as the literal
    strings ``"true"`` / ``"false"`` (case-insensitive). Anything else
    (including empty, ``"yes"``, ``"1"``) is treated as falsy. Used for
    ``--force``, ``--refresh-only``, and ``--sdc-only`` so the polarity
    stays consistent across all three flags.
    """
    return value.lower() == "true"


def _slack_fail(response_url: str, msg: str, *, operational: bool = False) -> NoReturn:
    """Print msg to stderr, post ephemeral reply to response_url, then exit 1.

    By default this is the ONLY delivery — appropriate for user-error
    failures (typos, unparseable arguments, bad target syntax, ineligible
    institution, mutually-exclusive flags). Those should stay private
    between the runner and the user who issued the slash command; the
    public `#tech-alerts` channel must not get flooded with them.

    Pass ``operational=True`` for infrastructure-style failures (EC2
    update timeout, mem/dir check failures, tmux launch failures, etc.)
    — i.e. anything the user can't fix by re-typing the command. If the
    response_url post fails for an operational error, fall back to
    posting to `#tech-alerts` via ``DPLA_SLACK_BOT_TOKEN``. The fallback
    uses ``api.slack.com/chat.postMessage`` (a different host and TLS
    handshake than ``hooks.slack.com``), which buys some resilience to
    correlated network failures from the GitHub runner — observed
    behaviour: when the runner couldn't reach SSM, it also couldn't
    reach hooks.slack.com, and the entire failure went unannounced.

    The bot-token fallback is operational-only by design: user errors
    are intentionally allowed to slip into silence rather than spam the
    channel if the user's response_url happens to be flaky.
    """
    print(msg, file=sys.stderr)
    delivered = False
    if response_url:
        try:
            resp = requests.post(
                response_url,
                json={"response_type": "ephemeral", "text": msg},
                timeout=5,
            )
            resp.raise_for_status()
            delivered = True
        except Exception as e:
            logging.warning("Failed to post to Slack response_url: %s", e)
    if not delivered and operational:
        token = (os.environ.get("DPLA_SLACK_BOT_TOKEN") or "").strip()
        if token:
            try:
                post_message(
                    token,
                    f"❌ Launch failure (response_url unreachable): {msg}",
                )
            except Exception as e:
                logging.warning("Fallback post to #tech-alerts also failed: %s", e)
    sys.exit(1)


# Maps the first token of a pipeline command to the step name reported to
# Slack on failure via ``WIKIMEDIA_STEP``. ``id-generation`` covers both
# ``get-ids-es`` and the bespoke NARA catalog walker (``get-ids-nara``);
# Slack's failure handler reads the env var directly so a stale launcher
# without this map still produces a generic "pipeline step failed" message
# (the legacy wording) rather than crashing.
_STEP_BY_FIRST_TOKEN: dict[str, str] = {
    "get-ids-es": "id-generation",
    "get-ids-nara": "id-generation",
    "downloader": "download",
    "uploader": "upload",
    "sdc-sync": "sdc-sync",
    "drain-deferred": "drain-deferred",
}


def _active_and_upcoming_labels(ssm, labels: list[str]) -> set[str]:
    """Return the subset of ``labels`` that a chained session hasn't yet
    completed — the currently-active label plus everything after it in
    the chain-run order.

    A wikimedia-upload tmux session runs its targets sequentially. Once
    a target's ``sdc-sync`` phase finishes and the ``&&`` chain moves on,
    that target is done; a new incoming request naming that same target
    is NOT a conflict and should be allowed to run alongside the ongoing
    session. Pre-fix (before this helper existed), the conflict check
    naively compared against **every** label in the tmux session name,
    causing the 54-target texas chain to block every new request naming
    any of its 54 institutions — even institutions whose target
    completed hours ago.

    Uses :func:`ingest_wikimedia.session_state.find_active_label`'s
    log-mtime heuristic: the freshest log across the label set
    identifies the currently-running target; everything AT or AFTER
    that position is still upcoming, everything before is done. If no
    log exists yet (session is in id-generation for its first target
    or a transient SSM error prevented the lookup), fall back to
    treating all labels as active — the conservative choice, so a
    failed lookup can't silently let a real conflict through.
    """
    if not labels:
        return set()
    try:
        active = find_active_label(ssm, labels)
    except Exception:
        return set(labels)
    if active is None:
        return set(labels)
    try:
        start_idx = labels.index(active[0])
    except ValueError:
        # ``find_active_label`` returned a label not in the input list —
        # shouldn't happen, but treat as unknown → fall back to all.
        return set(labels)
    return set(labels[start_idx:])


def _wrap_step_with_marker(cmd: str) -> str:
    """Prefix ``cmd`` with ``export WIKIMEDIA_STEP=<name> WIKIMEDIA_STEP_START=<epoch> &&``
    so the bash shell's WIKIMEDIA_STEP reflects which step was running when the
    ``&&``-chain breaks. The export goes BEFORE the step's command in the
    same chain, so the failed-step's name is the most recent assignment
    when ``notify_pipeline_fail`` reads it.

    ``WIKIMEDIA_STEP_START`` (Unix epoch seconds, evaluated at export time via
    ``$(date +%s)``) scopes the failure-log lookup to files created after
    this step began, so a step that dies before ``setup_logging`` runs
    doesn't get a stale log from a prior day attached to its Slack message.
    Read side: :func:`ingest_wikimedia.slack._find_latest_log`.

    For ``get-ids-es`` / ``get-ids-nara`` (the id-generation step), also
    tees stderr to the per-session path returned by
    :func:`id_generation_stderr_tail_file` (interpolated by bash from
    ``WIKIMEDIA_SESSION_LABEL`` at runtime, so concurrent tmux sessions
    don't clobber each other's stderr file). That's the one step in the
    pipeline whose tool doesn't call ``setup_logging``, so without this
    its stderr (e.g. ``click.BadParameter`` from a failed
    ``DPLA.check_partner`` precheck) would be visible only in the tmux
    pane and never reach the Slack failure message. Process substitution
    keeps the live stream going to the pane too.

    Commands not in :data:`_STEP_BY_FIRST_TOKEN` (``cd``, ``echo``, the
    case-2 skip step) are returned unchanged.
    """
    if not cmd:
        return cmd
    first = cmd.split(None, 1)[0]
    step = _STEP_BY_FIRST_TOKEN.get(first)
    if step is None:
        return cmd
    # ``drain-deferred --no-wait`` is the per-target opportunistic
    # phase; a distinct step name routes the failure handler to a
    # distinct log-file suffix, avoiding collision with the terminal
    # patient drain's ``drain-deferred`` log. The slack failure
    # handler's ``_PHASE_LOG_SUFFIX`` map has the matching entry.
    if step == "drain-deferred" and "--no-wait" in shlex.split(cmd):
        step = "drain-deferred-opportunistic"
    wrapped = cmd
    if step == "id-generation":
        # The existing build emits ``... > csv_file`` for stdout; appending
        # ``2> >(tee FILE >&2)`` here keeps stdout on the CSV and tees
        # stderr both to the tmux pane (via the trailing ``>&2``) and to
        # the failure-tail file. Bash parses the redirects in order:
        # stdout to the CSV, stderr through the substitution.
        #
        # Path is interpolated at runtime from
        # ``${WIKIMEDIA_SESSION_LABEL}`` so concurrent tmux sessions
        # running id-generation don't clobber each other's stderr file
        # in ``/tmp``. The launcher's per-target preamble exports
        # ``WIKIMEDIA_SESSION_LABEL`` BEFORE this tee runs, so the
        # interpolation always resolves. The ``:-unknown`` fallback is
        # belt-and-braces against a future caller that doesn't set the
        # var; in that case the read-side
        # (:func:`ingest_wikimedia.slack.id_generation_stderr_tail_file`)
        # would not match — accepting that as a fail-soft. Mirrors the
        # Python helper's filename shape.
        per_session_path = (
            '"/tmp/wm-id-generation-stderr-${WIKIMEDIA_SESSION_LABEL:-unknown}.log"'
        )
        wrapped = f"{cmd} 2> >(tee {per_session_path} >&2)"
    return (
        f'export WIKIMEDIA_STEP={step} WIKIMEDIA_STEP_START="$(date +%s)" && {wrapped}'
    )


def _build_get_ids_command(
    canonical: str,
    institutions: tuple[str, ...],
    collection: str | None,
    dpla_id: str | None,
    csv_file: str,
    maintain: bool = False,
) -> str:
    """Build the get-ids command that stages the ID CSV for one target.

    Routing rules:
    - A ``--single-id`` target re-runs get-ids-es so dpla-map.json and sdc.json
      are restaged with the current mapping code before download/upload/sync.
    - Hub-level NARA with no institution and no collection uses get-ids-nara
      (its bespoke NARA catalog walk). A NARA *collection* target (e.g.
      ``nara||General Records…``) must instead go through get-ids-es, which is
      the only path that filters on ``sourceResource.collection.title``.
    - Every other target uses get-ids-es, repeating ``--institution`` per
      resolved name (ORed in the ES dataProvider filter) and adding
      ``--collection`` when set. Omitting ``--institution`` matches the
      collection across every upload-eligible institution in the hub.

    ``maintain`` adds ``--maintain`` to the get-ids-es scan (relax the
    institution upload gate to QID-only) so already-uploaded items of
    no-longer-opted-in institutions are still enumerated. The per-item
    media/rights filters still apply (we download), so it is NOT combined with
    ``--skip-media-filter`` here — this builds the id list for the *id-list-
    anchored* maintain sub-path (single-DPLA-id / collection targets). The
    category-anchored path stages via :func:`_maintain_stage_cmd` instead.
    """
    if dpla_id is not None:
        return f"get-ids-es {canonical} --single-id {shlex.quote(dpla_id)} > {csv_file}"
    if canonical == "nara" and not institutions and collection is None:
        return f"get-ids-nara > {csv_file}"
    cmd = f"get-ids-es {canonical}"
    for inst in institutions:
        cmd += f" --institution {shlex.quote(inst)}"
    if collection is not None:
        cmd += f" --collection {shlex.quote(collection)}"
    if maintain:
        cmd += " --maintain"
    return cmd + f" > {csv_file}"


def _maintain_stage_cmd(
    canonical: str, institutions: tuple[str, ...], csv_file: str
) -> str:
    """``get-ids-es`` scan that stages each item's ``dpla-map.json`` +
    ``sdc.json`` for the whole QID-bearing institution scope, with the
    media/rights item filters dropped (``--skip-media-filter``).

    Maintain reconciles whatever is already on Commons, so staging must not
    exclude items whose current index doc has lost a fetchable media URL or
    free-rights category — otherwise their sidecars are absent and the
    ``--cat`` sync falls back to a live ``api.dp.la`` read per file. Shared by
    the lite and hash maintain routes.
    """
    # Bare-hub NARA stages via its bespoke catalog walk, not get-ids-es —
    # mirror the routing in _build_get_ids_command so a hub-level NARA maintain
    # target stages sidecars the same way the normal pipeline does. (Collection
    # / single-id NARA targets never reach here — they take the id-list-anchored
    # path in the hash builder.)
    if canonical == "nara" and not institutions:
        return f"get-ids-nara > {csv_file}"
    cmd = f"get-ids-es {canonical}"
    for inst in institutions:
        cmd += f" --institution {shlex.quote(inst)}"
    return cmd + f" --maintain --skip-media-filter > {csv_file}"


def _maintain_sdc_cat_steps(
    canonical: str, institutions: tuple[str, ...], sync_tail: str
) -> list[str]:
    """One ``sdc-sync --cat <category> --maintain <sync_tail>`` per institution,
    resolving each to its exact Commons category via Wikidata (P8464 →
    commonswiki sitelink — never derived from the display name).

    When a category can't be resolved (no P8464 sitelink on the institution's
    Wikidata item), branch on whether any DPLA-bot uploads exist for the QID:

    * **No files exist on Commons** (case-2): nothing to maintain. Emit an
      info-log step that exits 0 (``echo … ; true``) so the target succeeds
      and the batch proceeds — we don't materialise empty category
      infrastructure pre-emptively, matching the file-driven contract the
      uploader's ``CategoryEnsurer`` and ``tools/fix_unknown_categories``
      already follow (don't create a category until there's a file for it).
    * **Files exist** (case-1, e.g. institution-name drift since the upload):
      fall through to a per-target failure (``echo … >&2; false``).  The
      eventual fix is for the launcher to ``CategoryEnsurer.ensure(qid)``
      before the SDC step in this case, but that path needs a live case to
      test and isn't merged yet — for now we fail loudly per-target (notify
      runs, batch continues) instead of silently skipping a target that
      actually has work.

    Shared by the lite and hash maintain routes — both anchor the SDC phase on
    the *live category*, so reconciliation covers every file already on
    Commons, not just a get-ids id list.
    """
    steps: list[str] = []
    # A bare-hub target (no institutions) is one category-resolution pass with
    # inst=None; wikidata_qid_for_target and the `who` fallback both accept it.
    for inst in institutions or (None,):
        qid = wikidata_qid_for_target(canonical, inst)
        category = resolve_commons_category(qid) if qid else None
        if category:
            steps.append(
                f"sdc-sync --cat {shlex.quote(category)} --maintain{sync_tail}"
            )
            continue
        who = inst or canonical
        if qid and not commons_has_files_for_qid(qid):
            # Case 2: live institution, brand-new — no Commons category and
            # no files referencing the QID. Maintain pass is a no-op for this
            # target; succeed it and continue.
            msg = (
                f"maintain: no Commons category and no existing files for {who}"
                f" ({qid}); nothing to maintain — skipping."
            )
            steps.append(f"echo {shlex.quote(msg)} >&2; true")
            continue
        # Case 1 (files exist, no category yet) OR QID-less target — fall
        # through to per-target failure. ``false`` (not ``exit 1``) so only
        # this target fails; the outer ``|| { notify_fail; }`` runs and the
        # batch proceeds. ``exit 1`` would terminate the whole shell (PR #343).
        msg = (
            f"maintain: could not resolve a Commons category for {who}"
            " (missing Wikidata QID or P8464 Commons-category link); skipping."
        )
        steps.append(f"echo {shlex.quote(msg)} >&2; false")
    return steps


def _build_maintain_lite_pipeline_steps(
    canonical: str,
    institutions: tuple[str, ...],
    collection: str | None,
    dpla_id: str | None,
    base: str,
    csv_file: str,
    count_only: bool,
    worker_opts: str = "",
) -> list[str]:
    """Pipeline steps for one LITE maintain target (`cd` + the maintain
    commands). Lite = the quick, no-download sidecar route (the default
    hash-maintain path runs the full download+uploader pipeline instead).

    Lite re-links + SDC-syncs the EXISTING Commons files for this scope in
    place: no download, no upload, no new File pages. Each hub/institution is
    resolved to its exact Commons category via Wikidata (P8464 → commonswiki
    sitelink — authoritative, never derived from the display name) and walked
    with ``sdc-sync --cat … --maintain``.

    The sync reads each (re-linked) item's claims from its precomputed
    ``sdc.json`` sidecar (``--from-s3``) rather than calling ``api.dp.la`` per
    file, so a real run first stages those sidecars with ONE ``get-ids-es
    --maintain`` ES scan over the scope (drops only the upload gate, keeps the
    QID requirement). ``count_only`` is a pre-flight that just resolves how each
    file would re-link and writes nothing — it needs neither the staging step
    nor ``--from-s3``.

    A single-DPLA-id or collection-scoped target has no whole-category to walk,
    so it's rejected loudly rather than silently widening the write scope.
    """
    if dpla_id is not None or collection is not None:
        unit = "single DPLA-ID" if dpla_id is not None else "collection-scoped"
        msg = (
            f"maintain mode does not support {unit} targets; it operates"
            " on a whole hub/institution Commons category."
        )
        # ``false`` not ``exit 1`` — see the note on the
        # missing-category branch below; same reasoning applies here.
        return [f"cd {base}", f"echo {shlex.quote(msg)} >&2; false"]

    # count-only and --from-s3 are mutually exclusive: pre-flight sizing only
    # resolves the re-link (no sidecar read, no write), so it skips both the
    # staging scan and --from-s3.
    # The write path runs the parallel pool (one worker per group of files
    # sharing a DPLA id) under the box-wide slot budget — same --workers /
    # --workers-budget as partner mode. count-only is read-only pre-flight
    # sizing: serial, no sidecar read, no write, no workers.
    sync_tail = (
        " --count-only" if count_only else f" --from-s3 {canonical}{worker_opts}"
    )
    steps = []
    if not count_only:
        steps.append(_maintain_stage_cmd(canonical, institutions, csv_file))
    steps += _maintain_sdc_cat_steps(canonical, institutions, sync_tail)
    return [f"cd {base}", *steps]


def _build_maintain_hash_pipeline_steps(
    canonical: str,
    institutions: tuple[str, ...],
    collection: str | None,
    dpla_id: str | None,
    base: str,
    csv_file: str,
    max_age_days: int | None,
    upload_opts: str,
    sdc_opts: str,
) -> list[str]:
    """Pipeline steps for one DEFAULT (hash) maintain target.

    The SDC phase is anchored on the **live Commons category** (``sdc-sync
    --cat … --maintain``, identical to the lite route), so it reconciles SDC +
    legacy templates for *every* file already on Commons — the primary maintain
    goal. Inserted before it, a ``downloader`` + ``uploader --no-create`` pass
    repairs *content drift* for the media-bearing subset: the uploader's
    hash-drift machinery re-links orphaned files by content and overwrites
    byte-drifted ones, fenced (``--no-create``) so no NEW Commons file is ever
    created for these (possibly no-longer-opted-in) institutions.

    Staging uses the broad ``--skip-media-filter`` scan (same as lite) so the
    ``--cat`` sync sees the whole category, not just currently-fetchable items;
    the downloader simply skips items with no fetchable master. This is the
    delta from lite: lite is stage + ``--cat`` sync only; hash inserts the
    download + ``--no-create`` upload pass between them.

    A single-DPLA-id or collection-scoped target has no whole category to walk,
    so it keeps the id-list-anchored route (``sdc-sync --partner --ids-file``):
    download + ``uploader --no-create`` + SDC over exactly the matched items,
    for targeted drift repair of one item or collection. These go through
    :func:`_build_get_ids_command` — a single-id re-stages that one item with
    ``--single-id`` (no ``--maintain``); a collection uses ``--collection …
    --maintain`` — keeping the media/rights filter (no ``--skip-media-filter``),
    since there's no category to reconcile beyond what's fetched.
    """
    dl_age_opt = f"--max-age-days {max_age_days} " if max_age_days is not None else ""
    # ``--maintain`` on the downloader bypasses its
    # ``DPLA.check_partner`` upload-eligibility precheck, so a hub like
    # ``digitalnc`` (no opted-in institutions but real prior uploads)
    # can still run the hash-drift download pass. Behaviour is otherwise
    # unchanged — items the CSV passes through are downloaded the same.
    # The uploader gets the same effect via its existing ``--no-create``
    # flag (the launcher's maintain pipelines already pass it), so no
    # extra flag there.
    if dpla_id is not None or collection is not None:
        maintain_get_ids = _build_get_ids_command(
            canonical, institutions, collection, dpla_id, csv_file, maintain=True
        )
        return [
            f"cd {base}",
            maintain_get_ids,
            f"downloader --maintain {dl_age_opt}{csv_file} {canonical}",
            f"uploader {csv_file} {canonical} --no-create{upload_opts}",
            f"sdc-sync --partner {canonical} --ids-file {csv_file}{sdc_opts}",
        ]
    return [
        f"cd {base}",
        _maintain_stage_cmd(canonical, institutions, csv_file),
        f"downloader --maintain {dl_age_opt}{csv_file} {canonical}",
        f"uploader {csv_file} {canonical} --no-create{upload_opts}",
        *_maintain_sdc_cat_steps(
            canonical, institutions, f" --from-s3 {canonical}{sdc_opts}"
        ),
    ]


def _target_label(
    canonical: str, institutions: tuple[str, ...], collection: str | None
) -> str:
    """Format a batch target as the pipe-separated string shown in Slack.

    For a combined-institution target (multiple institutions from a single
    QID under one hub), shows the first institution + a ``(+N more)`` hint —
    the full list would blow up the Slack message width for QIDs with many
    sub-institutions. A collection target is either institution-scoped
    (``hub|institution|collection``) or hub-wide with an empty institution
    slot (``hub||collection``).
    """
    if collection:
        if institutions:
            return f"`{canonical}|{institutions[0]}|{collection}`"
        return f"`{canonical}||{collection}`"
    if len(institutions) == 1:
        return f"`{canonical}|{institutions[0]}`"
    if institutions:
        return f"`{canonical}|{institutions[0]} (+{len(institutions) - 1} more)`"
    return f"`{canonical}`"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--partner", required=True)
    parser.add_argument("--force", default="false")
    parser.add_argument("--response-url", default="")
    parser.add_argument("--max-age-days", default="")
    parser.add_argument("--refresh-only", default="false")
    parser.add_argument("--sdc-only", default="false")
    parser.add_argument("--maintain", default="false")
    parser.add_argument("--lite", default="false")
    parser.add_argument("--count-only", default="false")
    # Keep in sync with .github/workflows/wikimedia-launch.yml inputs.workers
    # / inputs.workers_budget (currently 6 / 24), which the workflow always
    # passes explicitly. These defaults only apply to a bare manual launch —
    # they let it behave like a workflow launch (6 SDC workers under a
    # box-wide cap of 24) rather than silently running single-worker, no cap.
    parser.add_argument("--workers", default="6")
    parser.add_argument("--workers-budget", default="24")
    args = parser.parse_args()

    force = _parse_bool(args.force)
    refresh_only = _parse_bool(args.refresh_only)
    sdc_only = _parse_bool(args.sdc_only)
    maintain = _parse_bool(args.maintain)
    count_only = _parse_bool(args.count_only)
    # Lite = the quick no-download sidecar route. count-only is a lite-only
    # pre-flight (re-link sizing), so it forces lite. Default maintain (lite
    # False) is the hash route: full download + uploader --no-create.
    lite = _parse_bool(args.lite) or count_only

    # Normalize the response_url first so the mutual-exclusion check below
    # can use the validated value rather than re-implementing the same
    # `startswith("https://hooks.slack.com/commands/")` guard inline.
    raw_url = args.response_url.strip()
    # Only accept genuine Slack response_url values — reject arbitrary POST targets.
    response_url = (
        raw_url if raw_url.startswith("https://hooks.slack.com/commands/") else ""
    )
    if raw_url and not response_url:
        print(f"Ignoring invalid response_url: {raw_url!r}", file=sys.stderr)

    if sum([refresh_only, sdc_only, maintain]) > 1:
        _slack_fail(
            response_url,
            "Cannot combine --refresh-only, --sdc-only, and --maintain — they're"
            " mutually exclusive run modes (refresh skips upload + SDC; sdc-only"
            " skips download + upload; maintain re-links + SDC-syncs existing"
            " Commons files only, creating nothing).",
        )
    if count_only and not maintain:
        _slack_fail(
            response_url,
            "--count-only is a maintain-mode pre-flight (it sizes the re-link"
            " without writing); it has no meaning outside --maintain.",
        )
    # Guard on the RAW --lite flag, not the derived ``lite`` above: ``lite`` is
    # also True under --count-only, so using it here would wrongly reject a bare
    # ``--count-only`` (which is validated separately just above).
    if _parse_bool(args.lite) and not maintain:
        _slack_fail(
            response_url,
            "--lite selects the quick no-download maintain route; it has no"
            " meaning outside --maintain.",
        )
    max_age_days: int | None = None
    if args.max_age_days.strip():
        try:
            max_age_days = int(args.max_age_days)
            if max_age_days <= 0:
                raise ValueError
        except ValueError:
            _slack_fail(
                response_url,
                f"Invalid --max-age-days value: {args.max_age_days!r} (must be a positive integer).",
            )

    # SDC-sync parallelism knobs. Empty string (workflow input left
    # blank) falls back to the conservative default rather than failing
    # the whole launch. --workers must be >= 1; --workers-budget >= 0
    # (0 = unlimited / disabled).
    sdc_workers = 6
    if args.workers.strip():
        try:
            sdc_workers = int(args.workers)
            if sdc_workers < 1:
                raise ValueError
        except ValueError:
            _slack_fail(
                response_url,
                f"Invalid --workers value: {args.workers!r} (must be an integer >= 1).",
            )
    sdc_workers_budget = 24
    if args.workers_budget.strip():
        try:
            sdc_workers_budget = int(args.workers_budget)
            if sdc_workers_budget < 0:
                raise ValueError
        except ValueError:
            _slack_fail(
                response_url,
                f"Invalid --workers-budget value: {args.workers_budget!r}"
                " (must be an integer >= 0; 0 disables the budget).",
            )

    # --partner may be a shlex-encoded list: 'bpl "indiana|Indiana State Library"'
    try:
        target_tokens = shlex.split(args.partner)
    except ValueError as e:
        _slack_fail(response_url, f"Could not parse --partner: {e}")

    # Validate each target and build
    # (canonical, institutions_tuple, label, dpla_id_or_None, collection_or_None) tuples.
    # ``institutions_tuple`` is ``()`` for hub-level, ``(name,)`` for single-
    # institution, or ``(n1, n2, …)`` for a combined session covering multiple
    # institutions under one hub (used when a single Wikidata QID resolves to
    # multiple institutions in the same hub).
    # Dedup by full target string so the same hub may appear with different institutions
    # (e.g. two QIDs that both resolve into the same hub but different institutions).
    seen_target_strs: set[str] = set()
    seen_canonicals: dict[str, None] = {}  # insertion-ordered; for conflict detection
    seen_session_labels: dict[
        str, None
    ] = {}  # insertion-ordered; drives session naming
    targets: list[tuple[str, tuple[str, ...], str, str | None, str | None]] = []
    # DPLA item ID tokens collected separately; resolved via EC2 before target building.
    dpla_id_tokens: list[str] = []
    # Per-target validation warnings; populated by _add_target and the conflict
    # check. If some targets are skipped but others remain valid, a summary is
    # posted to the caller before launch rather than aborting the whole run.
    skipped_warnings: list[str] = []

    def _add_target(
        canonical: str,
        institutions: tuple[str, ...] = (),
        dpla_id: str | None = None,
        collection: str | None = None,
    ) -> None:
        # ``institutions`` semantics:
        #   ``()``         → hub-level (no institution filter at get-ids-es time)
        #   ``(name,)``    → single institution (the historical / pipe-target case)
        #   ``(n1, n2, …)`` → combined session covering multiple institutions
        #                     under the same hub. Used when a single Wikidata
        #                     QID resolves to N institutions in one hub; the
        #                     pipeline runs ONE ``get-ids-es`` (with N
        #                     ``--institution`` flags), one downloader, one
        #                     uploader, one sdc-sync — so the operator sees
        #                     one tmux session and one set of Slack
        #                     notifications instead of N chained sessions.
        if collection is not None and len(institutions) > 1:
            skipped_warnings.append(
                f"Collection '{collection}' cannot be combined with multiple"
                " institutions. Use one institution (hub|institution|collection)"
                " or none (hub||collection, matched across the whole hub)."
            )
            return
        stripped: list[str] = []
        for name in institutions:
            name = name.strip()
            if not name:
                skipped_warnings.append(f"'{canonical}|': empty institution name.")
                return
            stripped.append(name)
        institutions = tuple(stripped)
        if (
            not maintain
            and dpla_id is None
            and canonical != "nara"
            and not institutions
        ):
            # Hub-level target: check that any institution in the hub is eligible.
            # Skipped in maintain mode — maintain operates ONLY on files already
            # on Commons (no uploads), so an institution being upload-ineligible
            # is exactly when it applies, not a reason to skip.
            # Skipped for DPLA item ID targets — item-level eligibility (rights
            # statement + media presence) was already verified by resolve-dpla-ids.
            # Institution-level and collection-level eligibility is not checked here —
            # get-ids-es enforces it at runtime, and the per-target failure handler
            # (notify_pipeline_fail) catches any rejection and continues with remaining
            # targets. NARA hub-level runs use get-ids-nara which filters itself.
            try:
                eligible = is_upload_eligible(canonical)
            except Exception as e:
                skipped_warnings.append(
                    f"'{canonical}': failed to check upload eligibility: {e}"
                )
                return
            if not eligible:
                skipped_warnings.append(
                    f"'{canonical}': not upload-eligible per institutions_v2.json."
                )
                return
        if collection is not None and institutions:
            target_str = f"{canonical}|{institutions[0]}|{collection}"
        elif collection is not None:
            # Hub-wide collection: empty institution slot (hub||collection).
            target_str = f"{canonical}||{collection}"
        elif len(institutions) == 1:
            target_str = f"{canonical}|{institutions[0]}"
        elif institutions:
            # Multi-institution: dedup key joins all names so two different
            # institution sets under the same hub produce two different
            # sessions, but the same QID resolved twice in one command
            # produces a "duplicate target" warning rather than two
            # near-identical sessions.
            target_str = f"{canonical}|" + "+".join(institutions)
        elif dpla_id is not None:
            target_str = dpla_id
        else:
            target_str = canonical
        if target_str in seen_target_strs:
            skipped_warnings.append(f"'{target_str}': duplicate target.")
            return
        seen_target_strs.add(target_str)
        seen_canonicals[canonical] = None
        if dpla_id is not None:
            # Use the first 8 hex chars of the ID as the label's institution component.
            # This is long enough to distinguish concurrent single-item sessions for
            # the same hub while keeping session names readable.
            inst_label: str | None = dpla_id[:8]
        elif institutions:
            first_slug = _slugify(institutions[0])
            if not first_slug:
                skipped_warnings.append(
                    f"'{canonical}|{institutions[0]}': institution name normalizes to an empty slug."
                )
                return
            if len(institutions) == 1:
                inst_label = first_slug
            else:
                # Multi-institution session label: keep it short and parseable.
                # ``-and-N-more`` is human-readable in tmux ls output and in
                # Slack notifications, and the leading slug pins which hub +
                # at-least-one-institution this session is for.
                inst_label = f"{first_slug}-and-{len(institutions) - 1}-more"
        else:
            inst_label = None
        if collection is not None:
            coll_label = _slugify(collection)
            if not coll_label:
                skipped_warnings.append(
                    f"'{target_str}': collection name normalizes to an empty slug."
                )
                return
            # Hub-wide collection has no institution slug (inst_label is None,
            # filtered out below) → hub+collection; the session-label parser
            # treats the collection slug as the hub's suffix, same shape as an
            # institution slug.
            label = "+".join(p for p in (canonical, inst_label, coll_label) if p)
        else:
            label = "+".join(p for p in (canonical, inst_label) if p)
        if label in seen_session_labels:
            skipped_warnings.append(
                f"'{target_str}': normalizes to the same session label ('{label}') as a previous target."
            )
            return
        seen_session_labels[label] = None
        targets.append((canonical, institutions, label, dpla_id, collection))

    for token in target_tokens:
        if is_wikidata_id(token):
            resolved = resolve_wikidata_id(token, maintain=maintain)
            if not resolved:
                skipped_warnings.append(
                    f"Wikidata ID {token!r}: not found in institutions_v2.json."
                )
                continue
            # Group resolved pairs by canonical hub so multiple institutions
            # under the same hub collapse into a single combined session
            # (one tmux session, one get-ids-es with N --institution flags,
            # one downloader / uploader / sdc-sync run, one set of Slack
            # notifications). Cross-hub QIDs — rare; same QID present under
            # institutions in different hubs — still produce one session per
            # hub, but each of those sessions is itself combined across all
            # institutions matching the QID in that hub.
            by_hub: dict[str, list[str | None]] = {}
            for canonical, institution in resolved:
                by_hub.setdefault(canonical, []).append(institution)
            for canonical, inst_list in by_hub.items():
                if None in inst_list:
                    # The QID matched the hub itself — that's strictly broader
                    # than any per-institution match, so the hub-level scope
                    # wins (no --institution filter, all eligible institutions
                    # processed).
                    _add_target(canonical, institutions=())
                else:
                    # All matches are institution-level; combine them.
                    # ``inst_list`` only contains non-None strings at this
                    # branch, so the cast is safe.
                    _add_target(
                        canonical,
                        institutions=tuple(
                            name for name in inst_list if name is not None
                        ),
                    )
        elif is_dpla_id(token):
            # Collect for batch resolution via EC2 after local parsing is done.
            # Normalise to lowercase — ES and S3 paths expect the canonical form.
            dpla_id_tokens.append(token.strip().lower())
        elif "|" in token:
            token_parts = token.split("|", 2)
            hub_part = token_parts[0]
            canonical = resolve_slug(hub_part)
            if canonical is None:
                skipped_warnings.append(f"'{token}': unknown hub '{hub_part}'.")
                continue
            if len(token_parts) == 3:
                _, institution, collection = token_parts
                if institution.strip() == "":
                    # hub||collection — empty institution slot means "match the
                    # collection across every upload-eligible institution in the
                    # hub" (some collections span multiple institutions). No real
                    # item has an empty institution, so the empty slot is
                    # unambiguous.
                    _add_target(canonical, institutions=(), collection=collection)
                else:
                    _add_target(
                        canonical, institutions=(institution,), collection=collection
                    )
            else:
                _, institution = token_parts
                _add_target(canonical, institutions=(institution,))
        else:
            canonical = resolve_slug(token)
            if canonical is None:
                skipped_warnings.append(f"'{token}': unknown hub slug.")
                continue
            _add_target(canonical, institutions=())

    slack_token = (os.environ.get("DPLA_SLACK_BOT_TOKEN") or "").strip()
    ssm = boto3.client("ssm", region_name=REGION)

    # Update EC2 code first so that resolve-dpla-ids and the pipeline both run
    # the latest version from this branch, not whatever was previously deployed.
    # Pin to GITHUB_SHA when available (always set in GitHub Actions) so that
    # feature-branch dispatches deploy the branch code, not the default branch.
    print("Updating EC2 code...")
    github_sha = (os.environ.get("GITHUB_SHA") or "").strip()
    pin_step = (
        f"cd /tmp/ingest-wikimedia-update && "
        f"git fetch --depth 1 origin {shlex.quote(github_sha)} && "
        f"git checkout --detach {shlex.quote(github_sha)} && "
        "cd /tmp && "
        if github_sha
        else ""
    )
    # `ssm_run` wraps every command in `sudo -u ec2-user bash -c` by
    # default. That's correct for the actual update (we want git clone, cp,
    # and uv sync to run as ec2-user so they never create root-owned
    # files), but `chown -R` of root-owned files needs CAP_CHOWN, which
    # ec2-user does not have. So the heal step uses ssm_run(..., as_root=True)
    # to bypass the wrapper and run with the AWS-RunShellScript document's
    # default root context.
    #
    # The earlier "Permission denied: ... lxml/__pycache__" failure mode
    # came from prior root-context Python imports (back when the update
    # itself ran as root) leaving root-owned `__pycache__/` inside an
    # otherwise ec2-user-owned venv. The heal step cleans those up; the
    # ec2-user-run update step prevents them from being recreated.
    heal_cmd = (
        "chown -R ec2-user:ec2-user /home/ec2-user/ingest-wikimedia/ && "
        "(chown -R ec2-user:ec2-user /tmp/ingest-wikimedia-update 2>/dev/null || true)"
    )
    # Non-fatal: the heal step is best-effort cleanup of incidental
    # root-owned files. If it times out (SSM scheduling latency — observed
    # ~5min queue time on a single-item launch where the chown itself ran
    # in 0 seconds but SSM held the command pending past our polling
    # window), or fails for any other reason, log a warning and proceed.
    # If a previous root-owned file actually blocks the downstream update,
    # `uv sync` will fail loudly with a specific permission error — which
    # is more actionable than a generic "heal timed out" abort that
    # forces the user to re-launch over an SSM-side hiccup.
    try:
        ssm_run(ssm, heal_cmd, as_root=True)
    except Exception as e:
        logging.warning(
            "EC2 file-ownership heal step failed or timed out (continuing): %s",
            e,
        )

    update_cmd = (
        "cd /tmp && rm -rf ingest-wikimedia-update && "
        "git clone --depth 1 https://github.com/dpla/ingest-wikimedia.git ingest-wikimedia-update && "
        + pin_step
        + "cp -r ingest-wikimedia-update/ingest_wikimedia/* /home/ec2-user/ingest-wikimedia/ingest_wikimedia/ && "
        "cp -r ingest-wikimedia-update/tools/* /home/ec2-user/ingest-wikimedia/tools/ && "
        "cp ingest-wikimedia-update/pyproject.toml /home/ec2-user/ingest-wikimedia/pyproject.toml && "
        "cp ingest-wikimedia-update/uv.lock /home/ec2-user/ingest-wikimedia/uv.lock && "
        "/home/ec2-user/.local/bin/uv sync --project /home/ec2-user/ingest-wikimedia && echo UPDATE_DONE"
    )
    out = ""
    try:
        out = ssm_run(ssm, update_cmd)
    except Exception as e:
        _slack_fail(response_url, f"⚠️ Failed to update EC2 code: {e}", operational=True)
    if "UPDATE_DONE" not in out:
        _slack_fail(
            response_url,
            "⚠️ EC2 code update did not confirm completion. Check the GitHub Actions run for details.",
            operational=True,
        )
    print("EC2 code updated.")

    # Resolve any DPLA item ID tokens via EC2. resolve-dpla-ids checks the ES index
    # for rights eligibility and media presence, then stages the item metadata to S3
    # (same as get-ids-es) so the downloader and uploader can proceed without changes.
    if dpla_id_tokens:
        print(f"Resolving {len(dpla_id_tokens)} DPLA ID(s)...")
        resolve_cmd = (
            "/home/ec2-user/ingest-wikimedia/.venv/bin/resolve-dpla-ids "
            + " ".join(shlex.quote(i) for i in dpla_id_tokens)
        )
        resolve_out = None
        try:
            resolve_out = ssm_run(ssm, resolve_cmd)
        except Exception as e:
            for item_id in dpla_id_tokens:
                skipped_warnings.append(
                    f"DPLA ID `{item_id}`: could not resolve ({e})."
                )
        if resolve_out is not None:
            unresolved_ids = set(dpla_id_tokens)
            for line in resolve_out.splitlines():
                parts = line.strip().split(" ", 1)
                if len(parts) < 2:
                    skipped_warnings.append(f"Unexpected resolver output: {line!r}.")
                    continue
                item_id, status = parts
                unresolved_ids.discard(item_id)
                if status == "NOT_FOUND":
                    skipped_warnings.append(f"DPLA ID `{item_id}`: not found in index.")
                elif status.startswith("INELIGIBLE:"):
                    reason = status.removeprefix("INELIGIBLE:")
                    skipped_warnings.append(
                        f"DPLA ID `{item_id}`: not eligible ({reason})."
                    )
                elif status.startswith("HUB="):
                    # DPLA-ID single-item target: no institution filter.
                    # ``institutions=()`` is the new "hub-level / no filter"
                    # signal — passing ``None`` would crash because the
                    # body iterates ``institutions``.
                    _add_target(
                        status.removeprefix("HUB="),
                        institutions=(),
                        dpla_id=item_id,
                    )
                elif status.startswith("ERROR:"):
                    skipped_warnings.append(
                        f"DPLA ID `{item_id}`: resolve error — {status.removeprefix('ERROR:')}."
                    )
                else:
                    skipped_warnings.append(
                        f"DPLA ID `{item_id}`: unexpected resolver status `{status}`."
                    )
            for item_id in sorted(unresolved_ids):
                skipped_warnings.append(
                    f"DPLA ID `{item_id}`: resolver returned no status."
                )

    if not targets:
        detail = ("\n• " + "\n• ".join(skipped_warnings)) if skipped_warnings else ""
        _slack_fail(response_url, f"No valid targets to launch.{detail}")

    # `--sdc-only` re-runs the ID-generation step (which now re-stages
    # sdc.json for every target type — get-ids-es for hub/institution and
    # single-item, get-ids-nara for NARA hub-level) so operators
    # backfilling for upstream mapping changes pick up the latest claims.
    # Previously NARA hub-level and single-item DPLA-ID targets
    # short-circuited their respective Phase 3 (sdc.json staging) — that
    # gap was closed in this PR, so no "stale sdc.json" warning is needed
    # anymore.
    if sdc_only and max_age_days is not None:
        print(
            "Warning: --max-age-days is ignored in --sdc-only mode"
            " (no download phase runs).",
            file=sys.stderr,
        )

    # Session name uses + as separator (unambiguous since slugs/institution names use -).
    # Institution-level targets include the hub slug as a prefix so the status script
    # can derive the EC2 directory: "indiana|Indiana State Library" → wikimedia-indiana+indiana-state-library.
    session_name = "wikimedia-" + "+".join(lbl for _, _, lbl, _, _ in targets)

    print("Checking instance memory...")
    try:
        mem_out = ssm_run(ssm, "free -m | awk 'NR==2{print $2, $7}'")
    except Exception as e:
        _slack_fail(
            response_url,
            f"⚠️ Failed to check instance memory: {e}",
            operational=True,
        )
    parts = mem_out.split()
    if len(parts) != 2:
        _slack_fail(
            response_url,
            f"⚠️ Unexpected memory output: {mem_out!r}",
            operational=True,
        )
    try:
        total_mb, available_mb = int(parts[0]), int(parts[1])
        pct_available = available_mb * 100 // total_mb
    except (ValueError, ZeroDivisionError) as e:
        _slack_fail(
            response_url,
            f"⚠️ Could not parse memory output ({mem_out!r}): {e}",
            operational=True,
        )
    print(f"Memory: {pct_available}% available ({available_mb} MB of {total_mb} MB).")
    if pct_available < MEMORY_HEADROOM_PCT:
        _slack_fail(
            response_url,
            f"⚠️ Cannot launch `{session_name}`: only {pct_available}% memory available"
            f" ({available_mb} MB of {total_mb} MB). Threshold is {MEMORY_HEADROOM_PCT}%.",
            operational=True,
        )

    # Verify every requested partner has a working directory on EC2. Pywikibot
    # expects per-partner config.toml / user-config.py / user-password.py /
    # apicache/; without them the pipeline's first `cd {base}` silently fails.
    # Any missing partner dir is auto-initialized by copying the required
    # pywikibot config from a template partner (bpl) — the bot account and
    # config are the same across all partners, so this is a safe one-shot
    # bootstrap. apicache/ is populated by pywikibot at first login.
    unique_pdirs = sorted({PARTNER_DIR.get(c, c) for c, _, _, _, _ in targets})
    print(f"Checking partner directories exist: {unique_pdirs}")
    check_cmd = "; ".join(
        f"test -d /home/ec2-user/ingest-wikimedia/{shlex.quote(d)} || echo MISSING:{d}"
        for d in unique_pdirs
    )
    try:
        dir_check_out = ssm_run(ssm, check_cmd)
    except Exception as e:
        _slack_fail(
            response_url,
            f"⚠️ Failed to check partner directories: {e}",
            operational=True,
        )
    missing_dirs = sorted(
        line.split(":", 1)[1]
        for line in dir_check_out.splitlines()
        if line.startswith("MISSING:")
    )
    if missing_dirs:
        # The bpl directory is the bootstrap template — it's a primary partner
        # that always exists in practice. Refuse to bootstrap if even bpl is
        # missing; that's a much bigger setup problem worth surfacing loudly.
        if "bpl" in missing_dirs:
            _slack_fail(
                response_url,
                f"⚠️ Cannot launch `{session_name}`: template partner directory"
                f" `bpl` is missing on EC2. The bot relies on it as the source"
                f" of pywikibot configuration when bootstrapping new partner"
                f" directories. Manual setup required.",
                operational=True,
            )
        print(f"Bootstrapping missing partner directories: {missing_dirs}")
        # Single SSM round-trip: mkdir + cp -np for each missing dir. -n
        # ("no-clobber") makes the copy idempotent if a config file already
        # happens to be in place.
        init_cmd = " && ".join(
            f"mkdir -p /home/ec2-user/ingest-wikimedia/{shlex.quote(d)} && "
            f"cp -np /home/ec2-user/ingest-wikimedia/bpl/config.toml"
            f" /home/ec2-user/ingest-wikimedia/bpl/user-config.py"
            f" /home/ec2-user/ingest-wikimedia/bpl/user-password.py"
            f" /home/ec2-user/ingest-wikimedia/{shlex.quote(d)}/"
            for d in missing_dirs
        )
        try:
            ssm_run(ssm, init_cmd)
        except Exception as e:
            _slack_fail(
                response_url,
                f"⚠️ Cannot launch `{session_name}`: failed to bootstrap"
                f" partner directories {missing_dirs}: {e}",
                operational=True,
            )
        bootstrapped_list = ", ".join(f"`{d}`" for d in missing_dirs)
        print(f"Auto-bootstrapped partner directories: {bootstrapped_list}")
        # Surface the bootstrap to Slack so the user sees what was created.
        if slack_token:
            try:
                post_message(
                    slack_token,
                    f"🛠️ Bootstrapped new partner directories on EC2 for"
                    f" `{session_name}`: {bootstrapped_list} (copied pywikibot"
                    f" config from `bpl`).",
                )
            except Exception as e:
                print(f"Failed to post bootstrap notification: {e}")

    # Check for any existing session that includes one of the requested targets.
    # Maps each requested label to the existing session name(s) it conflicts with.
    print(f"Checking for existing sessions that overlap with {session_name}...")
    try:
        tmux_list = ssm_run(ssm, "tmux ls 2>/dev/null || true")
    except Exception as e:
        _slack_fail(
            response_url,
            f"⚠️ Failed to list tmux sessions: {e}",
            operational=True,
        )
    label_conflicts: dict[str, list[str]] = {}
    for line in tmux_list.splitlines():
        existing_name = line.split(":")[0].strip()
        if not existing_name.startswith("wikimedia-"):
            continue
        existing_labels_ordered = parse_session_labels(
            existing_name[len("wikimedia-") :]
        )
        existing_labels = _active_and_upcoming_labels(ssm, existing_labels_ordered)
        for canonical, institutions, label, dpla_id, collection in targets:
            if not institutions and dpla_id is None:
                # Hub-level request conflicts with any existing session touching this hub
                # (hub-level, institution-level, or collection-level for the same hub).
                conflicts_this = any(
                    lbl == canonical or lbl.startswith(f"{canonical}+")
                    for lbl in existing_labels
                )
            elif collection is not None:
                # Collection-level request conflicts with:
                #   1. An existing hub-level session for the same hub
                #   2. An existing institution-level session for the institution the
                #      collection is scoped to (the collection is a subset; running
                #      both duplicates work, though it is technically idempotent)
                #   3. An existing session for the exact same collection label
                # A collection does NOT conflict with a different collection from the
                # same institution — those are independent, disjoint subsets.
                # A hub-wide collection (hub||collection) has no single institution
                # — ``institutions`` is empty — so only the hub-level and exact-label
                # conflicts apply.
                conflicts_this = (
                    canonical in existing_labels or label in existing_labels
                )
                if institutions:
                    inst_label = f"{canonical}+{_slugify(institutions[0])}"
                    conflicts_this = conflicts_this or inst_label in existing_labels
            elif dpla_id is None:
                # Institution-level (or multi-institution-combined) requests
                # conflict with:
                #   1. An existing hub-level session for the same hub
                #   2. An existing session for the exact same label
                #   3. Any existing collection-level session for the same label
                #      (the collection is a strict subset; the institution run would
                #      duplicate work on those items, symmetric with collection→institution)
                # Two institution-level sessions for different institution sets
                # within the same hub do NOT conflict — combined and single sets
                # are treated symmetrically.
                conflicts_this = (
                    canonical in existing_labels
                    or label in existing_labels
                    or any(lbl.startswith(f"{label}+") for lbl in existing_labels)
                )
            else:
                # Single-item requests conflict only with a hub-wide session for the
                # same hub or an exact duplicate single-item label.
                conflicts_this = (
                    canonical in existing_labels or label in existing_labels
                )
            if conflicts_this:
                label_conflicts.setdefault(label, []).append(existing_name)
    if label_conflicts:
        if force:
            sessions_to_kill = {
                name for names in label_conflicts.values() for name in names
            }
            for existing_name in sessions_to_kill:
                print(f"Existing session found: {existing_name}; killing it (--force).")
                try:
                    ssm_run(ssm, f"tmux kill-session -t {shlex.quote(existing_name)}")
                except Exception as e:
                    _slack_fail(
                        response_url,
                        f"⚠️ Failed to kill session `{existing_name}`: {e}",
                        operational=True,
                    )
        else:
            for label, existing_names in label_conflicts.items():
                existing_str = ", ".join(f"`{n}`" for n in existing_names)
                skipped_warnings.append(
                    f"`{label}`: conflicts with already-running session(s) {existing_str}."
                    " Use force=true to restart."
                )
            conflicting_labels = set(label_conflicts)
            targets[:] = [
                (c, i, lbl, did, col)
                for c, i, lbl, did, col in targets
                if lbl not in conflicting_labels
            ]
            for lbl in conflicting_labels:
                seen_session_labels.pop(lbl, None)
            if not targets:
                detail = "\n• " + "\n• ".join(skipped_warnings)
                _slack_fail(
                    response_url,
                    f"No valid targets remain — all conflict with running sessions.{detail}",
                )

    if skipped_warnings:
        warning_text = "⚠️ Skipped targets:\n• " + "\n• ".join(skipped_warnings)
        print(warning_text, file=sys.stderr)
        if response_url:
            try:
                requests.post(
                    response_url,
                    json={"response_type": "ephemeral", "text": warning_text},
                    timeout=5,
                ).raise_for_status()
            except Exception as e:
                logging.warning("Failed to post skip warnings to Slack: %s", e)

    # Recompute after conflict filtering — derive from targets to guarantee
    # the label order in the session name matches the surviving target list.
    session_name = "wikimedia-" + "+".join(lbl for _, _, lbl, _, _ in targets)

    # Build the pipeline command for all targets.
    # Setup (sourcing) runs once and gates all targets via &&.
    # Each target block runs its steps chained with &&. For hub targets these are
    # get-ids, downloader, and uploader. For DPLA item ID targets, the ID is written
    # directly to the CSV (metadata was staged by resolve-dpla-ids), so only
    # downloader and uploader run.
    # A failing block posts a Slack error notification then continues to the next
    # target (|| handler + ; separator).
    # The cd is required because config.toml is read from CWD.
    #
    # notify_fail_cmd captures the failing step's exit code via `rc=$?` so the
    # Slack message can decode signals like 137 (SIGKILL / probable OOM) and
    # 143 (SIGTERM). No backslash escaping needed on `$?` / `$rc`: pipeline_cmd
    # is staged to a script file via stage_and_launch_tmux below and read
    # directly by bash, so the inner shell sees raw `$?` / `$rc` and expands
    # them at runtime against the actual failing step.
    notify_fail_cmd = (
        "rc=$?; WIKIMEDIA_LAST_EXIT=$rc python3 -c "
        "'from ingest_wikimedia.slack import notify_pipeline_fail; notify_pipeline_fail()'"
    )
    setup = " && ".join(
        [
            "source ~/.bashrc",
            "source /home/ec2-user/ingest-wikimedia/.venv/bin/activate",
        ]
    )
    target_blocks = []
    last_idx = len(targets) - 1
    # A batch of full upload runs (not maintain, not sdc-only, not
    # refresh-only) queues a terminal patient-drain phase per unique
    # partner AFTER every target's chain finishes — so no partner sits
    # idle waiting on Commons volunteers while later targets could be
    # uploading. When those drains exist, the last TARGET is not the
    # last thing in the batch — the last drain block is — so
    # ``WIKIMEDIA_TARGET_IS_LAST`` moves to that drain block instead
    # of the last target.
    batch_has_terminal_drain = not maintain and not sdc_only and not refresh_only
    for idx, (canonical, institutions, session_label, dpla_id, collection) in enumerate(
        targets
    ):
        pdir = PARTNER_DIR.get(canonical, canonical)
        base = f"/home/ec2-user/ingest-wikimedia/{pdir}"
        # Use a per-target CSV filename so concurrent institution-level and
        # collection-level sessions don't clobber each other's ID lists.
        csv_file = f"{session_label}.csv"
        get_ids_cmd = _build_get_ids_command(
            canonical, institutions, collection, dpla_id, csv_file
        )
        # Export the session label (and single-item flag) before the group so the
        # failure handler and logging have the correct context even if cd itself fails.
        # WIKIMEDIA_SINGLE_ITEM must be explicitly unset for hub targets so that it
        # does not bleed across blocks when targets are mixed in the same session.
        single_item_env = (
            "export WIKIMEDIA_SINGLE_ITEM=1"
            if dpla_id is not None
            else "unset WIKIMEDIA_SINGLE_ITEM"
        )
        # WIKIMEDIA_PARTNER_DIR is read by notify_pipeline_fail() to locate
        # the most recent log for this target and include a tail + counts in
        # the Slack failure message.  WIKIMEDIA_TARGET_IS_LAST switches the
        # failure suffix language ("no further targets in batch" vs
        # "skipping to next target"); the unset is required so a failure
        # earlier in the batch doesn't inherit a stale "is last" flag.
        is_last_env = (
            "export WIKIMEDIA_TARGET_IS_LAST=1"
            if idx == last_idx and not batch_has_terminal_drain
            else "unset WIKIMEDIA_TARGET_IS_LAST"
        )
        # ``unset WIKIMEDIA_STEP`` first so a failure in this target's
        # un-wrapped preamble (``cd $base``, currently the only step that
        # ``_wrap_step_with_marker`` leaves alone) doesn't inherit the
        # previous target's last-step name in the Slack failure message.
        # Each subsequent step's ``export WIKIMEDIA_STEP=<name>`` then
        # sets it freshly. Without this, a target whose ``cd`` fails on a
        # missing partner directory would be reported as e.g. "`sdc-sync`
        # step failed" — naming the prior target's tail step instead of
        # the actual failure point.
        label_export = (
            f"export WIKIMEDIA_SESSION_LABEL={shlex.quote(session_label)}; "
            f"export WIKIMEDIA_PARTNER_DIR={shlex.quote(base)}; "
            f"{is_last_env}; "
            f"{single_item_env}; "
            f"unset WIKIMEDIA_STEP"
        )
        # SDC-sync parallelism options. --workers > 1 enables the
        # multiprocessing pool; --workers-budget caps concurrent worker
        # slots box-wide. Both values are already validated above.
        sdc_opts = f" --workers {sdc_workers} --workers-budget {sdc_workers_budget}"
        # The uploader is single-process but shares the same box-wide
        # Commons-write budget (one slot per item) so concurrent upload
        # and SDC-sync sessions across the host don't collectively
        # overrun maxlag. Only the budget — no --workers (no upload
        # parallelism).
        upload_opts = f" --workers-budget {sdc_workers_budget}"
        if maintain and lite:
            pipeline_steps = _build_maintain_lite_pipeline_steps(
                canonical,
                institutions,
                collection,
                dpla_id,
                base,
                csv_file,
                count_only,
                worker_opts=sdc_opts,
            )
        elif maintain:
            pipeline_steps = _build_maintain_hash_pipeline_steps(
                canonical,
                institutions,
                collection,
                dpla_id,
                base,
                csv_file,
                max_age_days,
                upload_opts,
                sdc_opts,
            )
        elif sdc_only:
            # SDC-only backfill: re-enumerate the partner's items (which
            # also refreshes sdc.json sidecars from the latest ingestion3
            # data) then run sdc-sync. No downloader, no uploader — the
            # caller is reconciling SDC for items the uploader already
            # processed in a prior session.
            pipeline_steps = [
                f"cd {base}",
                get_ids_cmd,
                f"sdc-sync --partner {canonical} --ids-file {csv_file}{sdc_opts}",
            ]
        else:
            dl_age_opt = (
                f"--max-age-days {max_age_days} " if max_age_days is not None else ""
            )
            dl_notify_opt = "--notify-complete " if refresh_only else ""
            pipeline_steps = [
                f"cd {base}",
                get_ids_cmd,
                f"downloader {dl_age_opt}{dl_notify_opt}{csv_file} {canonical}",
            ]
            if not refresh_only:
                pipeline_steps.append(f"uploader {csv_file} {canonical}{upload_opts}")
                # SDC sync is the final step of every upload run: it reads
                # the per-item sdc.json (staged by get-ids-es) and
                # upload-result.json (written by uploader) sidecars and
                # posts MediaInfo statements to Commons. Skipped for
                # refresh_only because no upload phase ran — there's no
                # upload-result.json to drive from.
                pipeline_steps.append(
                    f"sdc-sync --partner {canonical} --ids-file {csv_file}{sdc_opts}"
                )
                # Opportunistic drain: single best-effort round that
                # runs if Category:Duplicate happens to be below the
                # resume threshold RIGHT NOW, otherwise exits without
                # waiting. Never blocks the next target. The batch's
                # patient drain (per partner, at the end of the whole
                # pipeline — see the ``final_drain_blocks`` build
                # below) picks up whatever this pass left in the
                # sidecar.
                pipeline_steps.append(f"drain-deferred --no-wait {canonical}")
        # Wrap each step with an ``export WIKIMEDIA_STEP=<name>`` prefix so
        # the per-target failure handler (``notify_pipeline_fail``) can name
        # the failing phase in Slack instead of just reporting an exit code.
        # See :func:`_wrap_step_with_marker` for the per-step details
        # (including the id-generation stderr tee).
        target_steps = " && ".join(_wrap_step_with_marker(s) for s in pipeline_steps)
        target_blocks.append(
            f"{label_export}; {{ {target_steps}; }}"
            f" || {{ {notify_fail_cmd} >/dev/null 2>&1 || true; }}"
        )

    # Terminal patient-drain phase — one block per unique partner
    # in the batch. Runs AFTER every target's chain has finished, so
    # no partner sits idle waiting on Commons volunteers to clear
    # Category:Duplicate while later targets could be uploading.
    # The per-target opportunistic drain (``--no-wait``) inside each
    # chain already actioned anything that fit; this phase does the
    # unbounded patient wait. Dedup by canonical: two collection-
    # scoped targets for the same partner share one terminal drain
    # (a second drain would see an empty sidecar and no-op).
    final_drain_blocks: list[str] = []
    if batch_has_terminal_drain:
        # ``dict.fromkeys`` gives an order-preserving unique iterator —
        # same idiom used elsewhere in the tree (wikimedia.py, maintain.py).
        unique_canonical = list(dict.fromkeys(t[0] for t in targets))
        partners_to_drain: list[tuple[str, str]] = [
            (
                canonical,
                f"/home/ec2-user/ingest-wikimedia/{PARTNER_DIR.get(canonical, canonical)}",
            )
            for canonical in unique_canonical
        ]
        for i, (canonical, base) in enumerate(partners_to_drain):
            is_last_drain = i == len(partners_to_drain) - 1
            drain_is_last_env = (
                "export WIKIMEDIA_TARGET_IS_LAST=1"
                if is_last_drain
                else "unset WIKIMEDIA_TARGET_IS_LAST"
            )
            # Distinct session label per partner drain so the
            # failure-handler's log lookup finds
            # ``…-drain-{canonical}-drain-deferred.log`` (via
            # ``setup_logging(canonical, "drain-deferred")`` in the
            # drain tool).
            drain_label = f"drain-{canonical}"
            drain_label_export = (
                f"export WIKIMEDIA_SESSION_LABEL={shlex.quote(drain_label)}; "
                f"export WIKIMEDIA_PARTNER_DIR={shlex.quote(base)}; "
                f"{drain_is_last_env}; "
                f"unset WIKIMEDIA_SINGLE_ITEM; "
                f"unset WIKIMEDIA_STEP"
            )
            drain_step = _wrap_step_with_marker(f"drain-deferred {canonical}")
            final_drain_blocks.append(
                f"{drain_label_export}; {{ cd {base} && {drain_step}; }}"
                f" || {{ {notify_fail_cmd} >/dev/null 2>&1 || true; }}"
            )

    all_blocks = target_blocks + final_drain_blocks
    pipeline_cmd = f"{setup} && {{ {'; '.join(all_blocks)}; }}"

    if slack_token:
        single_item_targets = [
            (c, lbl, did) for c, _, lbl, did, _ in targets if did is not None
        ]
        batch_targets = [
            (c, i, lbl, col) for c, i, lbl, did, col in targets if did is None
        ]
        refresh_suffix = ""
        if refresh_only:
            age_note = f">{max_age_days}d" if max_age_days is not None else ">365d"
            refresh_suffix = f" (ID generation → download, refreshing files {age_note})"
        if single_item_targets and not batch_targets:
            # All single-item targets: use the eligibility-confirmed format.
            item_descs = [f"`{did}` ({c})" for c, _, did in single_item_targets]
            if refresh_only:
                msg = f"✅ {', '.join(item_descs)} — eligible, download refresh starting{refresh_suffix}."
            elif sdc_only:
                msg = (
                    f"✅ {', '.join(item_descs)} — eligible,"
                    " SDC-only backfill starting."
                )
            else:
                msg = (
                    f"✅ {', '.join(item_descs)} — eligible,"
                    " download + upload + SDC starting."
                )
        elif batch_targets and not single_item_targets:
            # All batch targets (hub, institution, or collection).
            batch_labels = [_target_label(c, i, col) for c, i, _, col in batch_targets]
            if refresh_only:
                msg = f"▶ Launching `{session_name}` refresh: {', '.join(batch_labels)}{refresh_suffix}."
            elif maintain:
                if lite:
                    detail = (
                        "re-linking + SDC-syncing existing Commons files in place"
                        " (lite: no download)"
                    )
                else:
                    detail = (
                        "download + content-reconcile (hash re-link + overwrite) +"
                        " SDC on existing Commons files (no new files)"
                    )
                msg = (
                    f"▶ Launching `{session_name}` maintain:"
                    f" {', '.join(batch_labels)} — {detail}."
                )
            elif sdc_only:
                msg = (
                    f"▶ Launching `{session_name}` SDC backfill:"
                    f" {', '.join(batch_labels)} (ID generation → SDC)."
                )
            else:
                msg = (
                    f"▶ Launching `{session_name}` pipeline: {', '.join(batch_labels)}"
                    " (ID generation → download → upload → SDC)."
                )
        else:
            # Mixed: list all targets with a note distinguishing items from batches.
            all_descs = []
            for c, i, lbl, did, col in targets:
                if did is not None:
                    all_descs.append(f"`{did[:8]}…` ({c}, item)")
                else:
                    all_descs.append(_target_label(c, i, col))
            if refresh_only:
                msg = f"▶ Launching `{session_name}` refresh: {', '.join(all_descs)}{refresh_suffix}."
            elif sdc_only:
                msg = (
                    f"▶ Launching `{session_name}` SDC backfill:"
                    f" {', '.join(all_descs)} (ID generation → SDC)."
                )
            else:
                msg = (
                    f"▶ Launching `{session_name}` pipeline: {', '.join(all_descs)}"
                    " (ID generation → download → upload → SDC)."
                )
        try:
            post_message(slack_token, msg)
        except Exception as e:
            logging.warning("Slack notification failed: %s", e)

    print(f"Launching {session_name} pipeline...")
    # Stage pipeline_cmd to a script file on EC2 and launch tmux against it
    # rather than inlining as a `"PIPELINE_CMD"` argument to tmux. The
    # inline form ran into SSM's per-command size limit ("command too long"
    # on a 22-target batch — single-quoted institution names plus the
    # shlex.quote multiplier add up fast). stage_and_launch_tmux base64-
    # encodes the script and decodes it on the instance, keeping the SSM
    # payload compact regardless of pipeline length. See ssm.py for the
    # full rationale.
    out = ""
    try:
        out = stage_and_launch_tmux(
            ssm,
            script=pipeline_cmd,
            session_name=session_name,
            cwd="/home/ec2-user/ingest-wikimedia/",
        )
    except Exception as e:
        _slack_fail(
            response_url,
            f"⚠️ Failed to launch tmux session `{session_name}`: {e}",
            operational=True,
        )
    if "SESSION_STARTED" not in out:
        _slack_fail(
            response_url,
            f"⚠️ `{session_name}` failed to start — tmux could not create session."
            " Check the GitHub Actions run for details.",
            operational=True,
        )
    print(f"Session {session_name} confirmed running.")


if __name__ == "__main__":
    main()
