# TODO caption, date, page, iiif manifest, url

import copy
import logging
import pywikibot
import requests
import re
import json
import datetime
import argparse
import random
import os
import time
import tomllib
import urllib.parse
from pywikibot import pagegenerators
from ingest_wikimedia.logs import setup_logging
from ingest_wikimedia.sdc import (
    CHUNKABLE_PROPS,
    casefold_for_compare,
    ingest_date_from_doc,
    parse_date_range,
    parse_dpla_date,
    parse_nara_access_level,
    parse_other_date_template,
)
from ingest_wikimedia import legacy_artwork, wikimedia, wikitext_normalize
from ingest_wikimedia.common import get_dict, get_list
from ingest_wikimedia.csrf import CsrfRecoveryFailed, with_csrf_recovery
from ingest_wikimedia.dpla import DC_TITLE_FIELD_NAME, SOURCE_RESOURCE_FIELD_NAME
from ingest_wikimedia.maintain import resolve_current_dpla_id
from ingest_wikimedia.slack import notify_phase_start, notify_sdc_complete
from ingest_wikimedia.tracker import Result, Tracker
from ingest_wikimedia.wikimedia import extract_dpla_id_from_commons_title
from ingest_wikimedia.worker_slots import WorkerSlotBudget

# Module-level SDC tracker. Counters accumulate across one invocation of
# main() — the helpers (`_post_new_refs`, `_post_new_claims`,
# `_reconcile_existing_claims`) increment as their POSTs succeed, and
# `_run_partner_mode` increments the per-item skip / sync counters.
tracker = Tracker()

# Module-level handles populated by `_initialize()` inside main(). Importing
# this module does no I/O — only running it (or calling main() from the
# `sdc-sync` console-script entry point) triggers argparse, config reads,
# Commons login, and the institutions_v2.json/subjects.json fetches.
parser: argparse.ArgumentParser
args: argparse.Namespace
method: str = "livecat"
dpla_api: str
site: pywikibot.site.BaseSite
hubs: dict
rights: dict
subject_ids: dict
_s3_partner: str | None = None
_s3_client = None
# Maintain mode: {institution Wikidata QID -> dataProvider name}, built lazily
# from ``hubs`` (institutions_v2.json) the first time the anchor-3 wildcard
# needs to scope a re-link to the file's own institution. None until built.
_maintain_qid_to_name: dict[str, str] | None = None
# Legacy-mode (``--file`` / ``--cat`` / ``--list``) DPLA-doc cache.
# ``parsed()`` populates it during ``process_one``; the post-SDC
# cleanup helper pops on read so the same S3 / api.dp.la doc isn't
# re-fetched. One entry per in-flight ``dpla_id``; the pop keeps the
# cache from growing past one entry in practice.
_legacy_mode_doc_cache: dict[str, dict] = {}
# When True (the default; see ``--normalize-wikitext`` in ``_build_parser``),
# ``_run_partner_mode`` runs a post-SDC wikitext-cleanup edit per item,
# stripping ``{{DPLA metadata}}`` params whose values are now redundant
# against the SDC just written. This is the documented end of the
# per-file lifecycle (upload → SDC → wikitext cleanup). ``--no-normalize-
# wikitext`` disables it for diagnostic runs that need the pre-strip
# wikitext intact.
_normalize_wikitext_enabled: bool = True

# Number of worker processes for partner-mode SDC sync. Default 1 keeps the
# pre-PR single-process behaviour; values > 1 enable the multiprocessing
# Pool dispatch in ``_run_partner_mode``. Set from ``args.workers`` at main()
# time so tests can monkeypatch the module global directly without needing
# to construct an argparse Namespace.
_workers: int = 1

# Box-wide cap on concurrent SDC worker slots across ALL sdc-sync sessions
# on the host — see ingest_wikimedia.worker_slots.WorkerSlotBudget. Workers
# in parallel mode (workers > 1) check out a slot before their per-item
# Commons work, so the total concurrent Commons-write load is bounded by
# this value no matter how many sessions run or how many workers each was
# launched with. 0 disables the budget (unlimited). Set from
# ``args.workers_budget`` at main() time.
_workers_budget: int = 0

# Per-worker WorkerSlotBudget instance. Defaults to a disabled (no-op)
# budget so ``_worker_slot_budget.acquire()`` is always safe to call even
# before a worker runs its initializer; _init_partner_worker replaces it
# with a real budget built from _workers_budget. _worker_partner_task
# acquires a slot from it around each item's Commons work.
_worker_slot_budget = WorkerSlotBudget(0)


def _build_parser() -> argparse.ArgumentParser:
    """Build the argparse parser. Pure — no side effects."""
    p = argparse.ArgumentParser()
    p.add_argument(
        "--cat",
        dest="cat",
        metavar="CAT",
        action="store",
        help="Commons category name (without 'Category:' prefix) to enumerate File: pages from",
    )
    p.add_argument(
        "--recurse",
        dest="recurse",
        action="store_true",
        help="When using --cat, also walk subcategories",
    )
    p.add_argument("--method", dest="method", metavar="METHOD", action="store")
    p.add_argument("--lists", dest="lists", metavar="LISTS", action="store")
    p.add_argument(
        "--file",
        dest="files",
        metavar="FILE",
        action="append",
        help="Commons file title to process directly (repeatable)",
    )
    p.add_argument(
        "--limit",
        dest="limit",
        type=int,
        default=0,
        help="When using --cat, stop after this many files (0 = no limit)",
    )
    p.add_argument(
        "--from-s3",
        dest="from_s3",
        metavar="PARTNER",
        action="store",
        default=None,
        help=(
            "Read each DPLA item's metadata from the dpla-map.json staged in S3 "
            "by get-ids-es (under the partner's sharded item prefix; resolved by "
            "S3Client.get_item_metadata) instead of calling api.dp.la. Falls back "
            "to api.dp.la when an item's dpla-map.json is missing."
        ),
    )
    p.add_argument(
        "--partner",
        dest="partner",
        metavar="PARTNER",
        action="store",
        default=None,
        help=(
            "Partner-driven SDC sync from precomputed S3 sidecars. Iterates the "
            "partner's IDs CSV (defaults to <PARTNER>/<PARTNER>.csv) and for each "
            "DPLA ID reads sdc.json (staged by get-ids-es) and upload-result.json "
            "(written by uploader). Posts SDC only for ordinals whose uploader "
            "status is UPLOADED or SKIPPED. No api.dp.la calls."
        ),
    )
    p.add_argument(
        "--ids-file",
        dest="ids_file",
        metavar="PATH",
        action="store",
        default=None,
        help=(
            "When using --partner, the IDs CSV path. Defaults to "
            "<PARTNER>/<PARTNER>.csv (matching the uploader's input convention)."
        ),
    )
    p.add_argument(
        "--migrate-legacy",
        dest="migrate_legacy",
        action="store_true",
        default=False,
        help=(
            "Instead of running SDC sync, migrate legacy {{Artwork}} files to "
            "{{DPLA metadata}} for the supplied --partner. Walks each file's "
            "revision history to distinguish DPLA-bot values (overwrite-safe) "
            "from community contributions (preserved as SDC statements with "
            "P887→Q131783016 + P4656 refs), then rewrites the wikitext."
        ),
    )
    p.add_argument(
        "--normalize-wikitext",
        dest="normalize_wikitext",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "After SDC sync, strip {{DPLA metadata}} template params whose "
            "values are now redundant with SDC. On by default — the "
            "strip is the documented end of the per-file lifecycle "
            "(upload → SDC → wikitext cleanup). Pass --no-normalize-wikitext "
            "to disable for diagnostic runs that need the pre-strip wikitext "
            "intact."
        ),
    )
    p.add_argument(
        "--maintain",
        dest="maintain",
        action="store_true",
        default=False,
        help=(
            "Maintain mode (for --cat / --file). Before syncing each file, "
            "re-link its DPLA id to the current record via the provider source "
            "URL (ingest_wikimedia.maintain), so files whose embedded DPLA id "
            "has gone dead (ID drift → 404 dp.la links) sync against the live "
            "record instead. SDC sync + template migration only; no uploads. "
            "Use to maintain already-uploaded files of institutions no longer "
            "authorized for new uploads."
        ),
    )
    p.add_argument(
        "--count-only",
        dest="count_only",
        action="store_true",
        default=False,
        help=(
            "Maintain-mode pre-flight sizing: walk the --cat/--file scope, "
            "resolve how each file would re-link (embedded id still live / "
            "isShownAt-recovered / institution-wildcard / unresolved), print a "
            "per-anchor breakdown, and write nothing. Run this before a real "
            "maintain pass to size the work and spot a scope that re-links "
            "poorly. No effect without --maintain."
        ),
    )
    p.add_argument(
        "--workers",
        dest="workers",
        type=int,
        default=1,
        help=(
            "Number of worker processes for partner-mode SDC sync. "
            "Default 1 keeps the single-process behaviour unchanged. "
            "N>1 dispatches per-DPLA-item work to a multiprocessing "
            "Pool, with each worker holding its own pywikibot session "
            "and per-task counter deltas merged into the parent's "
            "Tracker. Wall-clock scales roughly linearly with N up to "
            "Commons-side parser-pool headroom (~8-16 across all "
            "concurrent sessions before maxlag starts to bind). Items "
            "are independent: every ordinal of every item has a "
            "unique M-id, so workers never write to the same Commons "
            "MediaInfo entity."
        ),
    )
    p.add_argument(
        "--workers-budget",
        dest="workers_budget",
        type=int,
        default=0,
        help=(
            "Box-wide cap on concurrent SDC worker slots across ALL "
            "sdc-sync sessions on the host. In partner mode, every "
            "item-processing path checks out a flock-backed slot before "
            "its per-item Commons work — the parallel workers (--workers "
            ">1) and the single-process path (--workers 1) alike — so the "
            "total concurrent Commons-write load is bounded by this value "
            "regardless of how many sessions run or how many workers each "
            "was launched with; excess workers block until a slot frees. "
            "0 (default) disables the budget (acquire is a no-op). Set to "
            "~16 in production so 6+ concurrent sessions cooperatively "
            "share Commons capacity without oversubscribing the parser "
            "pool. The single-purpose manual modes (--list / --file / "
            "--cat) do NOT participate in the budget."
        ),
    )
    return p


class _MissingEntityError(Exception):
    """Commons returned ``no-such-entity`` for the staged M-id.

    Raised by the wbeditentity / wbremoveclaims POST helpers when
    pywikibot's ``simple_request`` translates Commons'
    ``{"error": {"code": "no-such-entity", ...}}`` response into an
    ``APIError`` with ``code == "no-such-entity"``; the helpers
    re-raise it as this dedicated class. This is not a failure of the
    SDC phase — it just means the file page is gone (most commonly
    because a Commons curator deleted it as a duplicate, or because
    this is an SDC-only run for an M-id whose upload was never
    confirmed). The per-ordinal handler in ``_run_partner_mode``
    catches this distinctly from generic ``Exception`` so it can:

    - log at INFO instead of ERROR (it isn't an error to log against),
    - increment ``SDC_ORDINALS_SKIPPED_MISSING_ENTITY`` instead of
      ``SDC_ORDINALS_SKIPPED_ERROR``,
    - leave ``had_ordinal_error`` unchanged so the item's bucket
      classification (SYNCED / SKIPPED_ERROR / SKIPPED_MAPPING) is
      decided by the other ordinals' outcomes, not by these skips.

    Re-uploading the file or re-resolving the M-id is upstream work
    (upload phase, drift handling) — not something this phase can or
    should try to fix.
    """


def _fetch_entity_for_cleanup_guard(mediaid: str) -> dict:
    """Read ``mediaid``'s MediaInfo entity, bypassing the per-file cache.

    The post-SDC cleanup's defensive guard wants the post-write entity
    state, not a snapshot from earlier in the same process_one run.
    ``get_entity`` caches by M-id within a file's processing window;
    this helper goes straight to wbgetentities. Returns an empty dict
    on no-such-entity (caller treats that as "no DPLA SDC", which is
    correct — a missing entity has zero of anything).
    """
    try:
        raw = site.simple_request(action="wbgetentities", ids=mediaid).submit()
    except Exception:
        raise
    entities = raw.get("entities", {})
    if not isinstance(entities, dict):
        return {}
    ent = entities.get(mediaid, {})
    if not isinstance(ent, dict):
        return {}
    return ent


def _entity_has_dpla_attributed_claims(entity: dict) -> bool:
    """True iff ``entity`` carries at least one statement with the
    DPLA-attribution qualifier (``P459 = Q61848113``, "heuristic" →
    "DPLA"). Mirrors :func:`Module:DPLA`'s ``isDplaDetermined`` filter
    on Commons so the guard's notion of "has DPLA SDC" matches the
    template renderer's notion exactly.
    """
    if not entity:
        return False
    statements = entity.get("statements") or entity.get("claims") or {}
    if not isinstance(statements, dict):
        return False
    for stmt_list in statements.values():
        if not isinstance(stmt_list, list):
            continue
        for stmt in stmt_list:
            if not isinstance(stmt, dict):
                continue
            quals = stmt.get("qualifiers") or {}
            for q in quals.get("P459", []):
                dv = q.get("datavalue") if isinstance(q, dict) else None
                if not isinstance(dv, dict):
                    continue
                val = dv.get("value")
                if isinstance(val, dict) and val.get("id") == "Q61848113":
                    return True
    return False


def _classify_item_outcome(synced_this_item: bool, had_ordinal_error: bool) -> Result:
    """Map (synced_this_item, had_ordinal_error) to the item-level
    tracker bucket the partner-mode loop should increment.

    Three "made-progress" outcomes — full sync, partial sync, all-
    ordinals-errored — plus one no-progress outcome (mapping skip).
    The partial-sync case is broken out from full-sync so dashboards
    keying on ``SDC_ITEMS_SYNCED`` as "items fully done" don't
    silently treat mixed-result items (one ordinal synced, sibling
    null-pageid / runtime-error ordinal skipped) as healthy. Both
    full and partial outcomes still get the post-SDC cleanup pass
    on whatever ordinals did sync; the partial state is real
    progress, not a failure to retry wholesale. (Per CR review on
    PR #302.)
    """
    if synced_this_item and had_ordinal_error:
        return Result.SDC_ITEMS_PARTIALLY_SYNCED
    if synced_this_item:
        return Result.SDC_ITEMS_SYNCED
    if had_ordinal_error:
        return Result.SDC_ITEMS_SKIPPED_ERROR
    return Result.SDC_ITEMS_SKIPPED_MAPPING


def _resolve_pageid_from_title(title: str) -> int | None:
    """Look up the Commons page id for ``title`` via the live wiki API.

    The fallback for upload-result.json sidecars where the uploader
    recorded ``pageid: null`` or ``pageid: 0`` for a successfully
    uploaded file (an upstream pywikibot FilePage-cache-invalidation
    quirk). The file exists on Commons under ``title`` — Commons can
    tell us the M-id; the uploader just failed to capture it.

    Returns the pageid integer when the page exists and the API
    answered with one. Returns ``None`` on any failure mode — page
    actually doesn't exist (deleted, never uploaded), API error,
    network blip — so the caller's existing skip path runs and the
    skip is counted under :class:`Result.SDC_ORDINALS_SKIPPED_MISSING_PAGEID`.

    Idempotent and safe to call from a hot loop — single ``query``
    action against the live Commons API, no writes.
    """
    if not title:
        return None
    # ``upload-result.json`` records page titles WITHOUT the ``File:``
    # namespace prefix — that's the form pywikibot's
    # ``FilePage.title(with_ns=False)`` returns and what the uploader
    # serialises into the sidecar. A bare ``titles=`` query against
    # the Commons API resolves to the *main* namespace, which never
    # contains DPLA file pages — so without this prefix the lookup
    # always reports the title as missing and the fallback returns
    # ``None`` on files that DO exist. Prepend the namespace
    # explicitly when absent so the query lands in the File:
    # namespace where the uploaded media actually lives.
    if not title.lower().startswith("file:"):
        title = "File:" + title
    try:
        result = site.simple_request(
            action="query",
            titles=title,
            prop="info",
        ).submit()
    except Exception:
        # Logged at the call site; let the caller drop into its
        # existing skip path on any API failure.
        return None
    pages = result.get("query", {}).get("pages", {})
    if not isinstance(pages, dict):
        return None
    for _api_key, page_info in pages.items():
        if not isinstance(page_info, dict):
            continue
        if "missing" in page_info:
            # Page genuinely doesn't exist on Commons — the upload
            # presumably failed silently after recording UPLOADED.
            return None
        pid = page_info.get("pageid")
        if isinstance(pid, int) and pid > 0:
            return pid
    return None


def _find_existing_commons_files_by_dpla_id(dpla_id: str) -> dict[str, dict]:
    """Find every Commons file whose title carries ``dpla_id`` and map
    each by its ordinal-from-filename.

    Returns ``{ord_str: {"title": <title-without-File:>, "pageid": <int>}}``.
    Empty dict on any failure mode — caller's existing skip path runs.

    Uses CirrusSearch ``intitle:<dpla_id>`` which exact-matches the
    32-hex DPLA ID anywhere in the title. The DPLA ID is unique enough
    that the result set is bounded by the item's file count
    (typically 1, occasionally 20–30 for multi-page archival items).
    Within-item ordinal collisions are impossible — Commons titles
    must be unique, and the bot produces deterministic (page N)
    suffixes per ordinal.

    Used by the SDC eligibility-discovery fallback: when the
    uploader couldn't confirm an ordinal (NOT_PRESENT / INELIGIBLE /
    FAILED) but a Commons file with that DPLA ID and ordinal already
    exists from a prior successful run, sync SDC against the existing
    file rather than skipping the data-side work because of a transient
    binary-side failure.
    """
    if not dpla_id:
        return {}
    try:
        result = site.simple_request(
            action="query",
            list="search",
            srnamespace=6,
            srsearch=f'intitle:"{dpla_id}"',
            # 50 hits comfortably exceeds the largest multi-file items
            # in DPLA's corpus (P.D. records / microfilm reels run
            # 20–30 pages). Bumping further has no cost when the actual
            # hit count is small.
            srlimit=50,
            srprop="",
        ).submit()
    except Exception:
        return {}
    out: dict[str, dict] = {}
    for hit in result.get("query", {}).get("search", []) or []:
        if not isinstance(hit, dict):
            continue
        title = hit.get("title")
        pageid = hit.get("pageid")
        if not isinstance(title, str) or not isinstance(pageid, int) or pageid <= 0:
            continue
        # ``upload-result.json`` records titles without the ``File:``
        # prefix; mirror that here so downstream lookups (which
        # re-prepend ``File:`` themselves) work uniformly.
        bare_title = title[5:] if title.lower().startswith("file:") else title
        # The intitle search can surface files where the DPLA ID
        # appears outside the canonical `- DPLA - <id> [(page N)].<ext>`
        # tail (e.g. a hand-named file that happened to mention the
        # ID in its title). Confirm via the shared extractor that
        # this hit's title actually carries the DPLA-tail anchor we
        # produce; skip otherwise rather than guess the ordinal.
        if extract_dpla_id_from_commons_title(bare_title) != dpla_id:
            continue
        ordinal = wikimedia.extract_page_ordinal_from_commons_title(bare_title)
        # Single-file items have no `(page N)` suffix — the uploader
        # serialises them as ordinal "1" in upload-result.json, so
        # mirror that here.
        ord_str = str(ordinal) if ordinal is not None else "1"
        out[ord_str] = {"title": bare_title, "pageid": pageid}
    return out


def _truncate(text: str | None, limit: int = 500) -> str:
    """Return ``text`` shortened to ``limit`` chars with an ellipsis suffix.

    Used to keep RuntimeError messages from the SDC POST helpers readable
    when Commons returns a verbose error body — the full response still
    ends up in the per-ordinal traceback via ``logging.exception``; the
    truncated version is just for the one-line message.
    """
    if text is None:
        return ""
    text = str(text)
    if len(text) <= limit:
        return text
    return text[:limit] + f"... [truncated, {len(text)} chars total]"


def _normalize_rights_uri(uri):
    """Canonicalize a DPLA rights URI for lookup against rights.json.

    DPLA emits rights URIs in a few minor variants — http vs https and with or
    without a trailing slash. Keying on a single canonical form means we don't
    silently miss licenses just because of scheme/slash drift on either side.
    """
    if not uri:
        return uri
    canonical = uri.replace("https://", "http://").rstrip("/")
    return canonical


# Resolve every load (config + vendored JSONs) relative to the repo root so the
# script behaves the same regardless of the caller's working directory.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)


def _initialize() -> None:
    """Parse args, load config, log in to Commons, fetch live ingestion3 JSONs.

    Populates the module-level globals the helper functions read at call
    time. Kept out of import-time so `import tools.sdc_sync` (e.g. by the
    console-script entry point) does no I/O.
    """
    global parser, args, method, dpla_api, site, hubs, rights, subject_ids
    global _s3_partner, _s3_client, _normalize_wikitext_enabled, _workers
    global _workers_budget

    parser = _build_parser()
    args = parser.parse_args()
    if args.method:
        method = args.method

    with open(os.path.join(_REPO_ROOT, "config.toml"), "rb") as f:
        dpla_api = tomllib.load(f)["dpla_api_key"]

    # Bound pywikibot's retry budget. Defaults let a single hung Commons
    # endpoint stall ``simple_request().submit()`` for ~30 minutes
    # (max_retries=15 × retry_max=120s backoff). One stuck connection
    # then holds up the whole 50K-file partner batch behind it. The
    # values below cap worst-case stall at ~9 min and still ride through
    # the 1-2 retries needed for transient blips. Applies process-wide,
    # including to ``site.login()`` immediately below and to all
    # subsequent ``simple_request`` / ``FilePage.touch`` calls.
    pywikibot.config.max_retries = _PYWIKIBOT_MAX_RETRIES
    pywikibot.config.retry_wait = _PYWIKIBOT_RETRY_WAIT
    pywikibot.config.retry_max = _PYWIKIBOT_RETRY_MAX

    site = pywikibot.Site()
    site.login()
    # CSRF tokens are managed by pywikibot's ``site.tokens["csrf"]`` —
    # lazy-loaded on first read, refreshed automatically on ``badtoken``
    # responses. No manual token fetch or relogin loops here; the
    # ``_wbeditentity_via_pywikibot``, ``postqual``, and removals helpers
    # all pull from the same auto-managed source at write time.

    # Hubs and subject mappings are fetched live from ingestion3 on every run,
    # by design: this sync exists precisely to propagate upstream changes to
    # that data, and a vendored snapshot would defeat the point. rights.json
    # is the exception — it lives here because it's a small, slow-moving
    # SDC-specific mapping not maintained in ingestion3.
    hubs = requests.get(
        "https://raw.githubusercontent.com/dpla/ingestion3/develop/src/main/resources/wiki/institutions_v2.json",
        timeout=30,
    ).json()
    with open(os.path.join(_REPO_ROOT, "rights.json")) as f:
        rights = {_normalize_rights_uri(k): v for k, v in json.load(f).items()}
    subject_ids = requests.get(
        "https://raw.githubusercontent.com/dpla/ingestion3/develop/src/main/resources/subjects.json",
        timeout=30,
    ).json()

    # When --from-s3 <partner> is set, parsed() reads each item's dpla-map.json
    # from S3 instead of calling api.dp.la. Imported lazily so this module
    # doesn't pay the boto3 import cost when nothing needs S3.
    _s3_partner = args.from_s3
    _s3_client = None
    if _s3_partner is not None:
        from ingest_wikimedia.s3 import S3Client

        _s3_client = S3Client()

    _normalize_wikitext_enabled = bool(args.normalize_wikitext)
    _workers = max(1, int(getattr(args, "workers", 1) or 1))
    _workers_budget = max(0, int(getattr(args, "workers_budget", 0) or 0))


# This is the JSON used for formatting a claim. The P459 -> Q61848113 (determination method) qualifier is hardcoded in for everything DPLA adds. Not all data types have the same format for value, so this is formatted in the function for each property added.


def _set_claim_target(claim, repo, value, value_type):
    """Apply ``setTarget`` to ``claim`` using the right pywikibot type
    for ``value_type``.

    Mirrors the value-type → wire-format mapping the previous
    hand-built ``formattedclaim`` dict expressed inline. Only the four
    types our 17 add_* helpers actually use are handled — anything else
    raises so a new caller can't silently miss a translation.
    """
    if value_type == "wikibase-entityid":
        qid = f"Q{value['numeric-id']}"
        claim.setTarget(pywikibot.ItemPage(repo, qid))
    elif value_type == "string":
        claim.setTarget(value)
    elif value_type == "monolingualtext":
        claim.setTarget(
            pywikibot.WbMonolingualText(text=value["text"], language=value["language"])
        )
    else:
        # The only ``"time"`` callsite (``add_date``) passes ``"somevalue"``
        # as the value, which is handled by the caller before we get here.
        # If a future helper passes a real time value, add the case.
        raise ValueError(f"formattedclaim: unsupported value_type {value_type!r}")


def formattedclaim(prop, value, value_type, dpla_id):
    """Build a DPLA-authored SDC statement, returned as a wbeditentity
    wire-format dict.

    Constructed via ``pywikibot.Claim`` for type safety (the right
    pywikibot value class is matched to the property's expected data
    type at build time, not at POST time) and then serialised via
    ``Claim.toJSON()`` so the result still plugs straight into the bulk
    ``claims["claims"]`` accumulator that ``_post_new_claims`` submits.

    Every statement carries the standard DPLA qualifier (P459=Q61848113,
    determination method = "inferred from heuristic") and the 3-snak
    DPLA reference (P854 source URL, P123 publisher=DPLA, P813 retrieved
    date). The ``"somevalue"`` special case sets the mainsnak's
    ``snaktype`` to ``"somevalue"`` and omits the datavalue — used for
    claims where DPLA records that a value exists but doesn't know what
    it is (e.g. ``add_date`` with a missing date).
    """
    repo = site.data_repository()
    claim = pywikibot.Claim(site, prop)

    if value == "somevalue":
        claim.setSnakType("somevalue")
    else:
        _set_claim_target(claim, repo, value, value_type)

    # P459 = Q61848113 (determination method = inferred from heuristic).
    # This is DPLA's universal "we set this" marker — `check()` and
    # `_is_safe_to_amend_in_place` both depend on it to recognise
    # DPLA-authored claims on read-back.
    qualifier = pywikibot.Claim(site, "P459", is_qualifier=True)
    qualifier.setTarget(pywikibot.ItemPage(repo, "Q61848113"))
    claim.addQualifier(qualifier)

    # Standard DPLA reference: P854 source URL + P123 publisher + P813
    # retrieved date. ``_is_dpla_reference`` keys on the P123 publisher
    # snak to recognise this reference shape on read-back.
    ref_url = pywikibot.Claim(site, "P854", is_reference=True)
    ref_url.setTarget(_dpla_item_url(dpla_id))
    ref_publisher = pywikibot.Claim(site, "P123", is_reference=True)
    ref_publisher.setTarget(pywikibot.ItemPage(repo, "Q2944483"))
    ingest_date = _require_ingest_date()
    ref_retrieved = pywikibot.Claim(site, "P813", is_reference=True)
    ref_retrieved.setTarget(
        pywikibot.WbTime(
            year=ingest_date.year, month=ingest_date.month, day=ingest_date.day
        )
    )
    claim.addSources([ref_url, ref_publisher, ref_retrieved])

    serialized = claim.toJSON()
    # Strip the ``qualifiers-order`` and per-reference ``snaks-order``
    # keys ``Claim.toJSON()`` produces. Several add_* helpers append
    # extra inline qualifiers by mutating the returned dict
    # (``claim["qualifiers"][P1932] = [...]`` etc.), which would leave
    # the order keys out of sync with the actual qualifier set.
    # Wikibase accepts dicts without order keys (the previous hand-built
    # ``formattedclaim`` never produced them) and falls back to the
    # natural dict iteration order, so stripping them is the
    # lowest-risk way to keep the existing mutation pattern correct.
    serialized.pop("qualifiers-order", None)
    for ref in serialized.get("references", []):
        ref.pop("snaks-order", None)
    return serialized


# NOTE: ``postqual`` (wbsetqualifier POST) and ``wbremovequalifiers``
# direct POSTs have been removed. The qualifier add and remove
# operations route through the per-file dispatcher
# (``_submit_per_item_edit``), which folds them into a single
# wbeditentity per file. See ``_build_qualifier_update_fragments`` and
# ``_flush_per_file_edits``.


# This function performs an initial GET request on the given Wikimedia file to check if the statement we will be adding is already in the page. It returns a boolean, with True if the statement is not found and can be added. "qid" is passed as a tuple with both the value and the data type, so this check can handle the formatting for different data types. If statements are found in the entity with the prop and value, but no qualifiers, we return the statement id instead, so that the qualifier can be added to that statement instead of creating a new one using postqual().


# Pywikibot retry budget — applied process-wide in ``_initialize()``.
# Worst-case single-call stall ≈ max_retries × (read_timeout + retry_max)
# = 5 × (45s + 60s) ≈ 9 min, vs pywikibot's ~30-min default.
_PYWIKIBOT_MAX_RETRIES = 5
_PYWIKIBOT_RETRY_WAIT = 5
_PYWIKIBOT_RETRY_MAX = 60


# Per-file cache for wbgetentities. Populated at the start of process_one()
# / process_one_from_sdc() and consulted by check() and the various amend_*
# helpers for all subsequent reads of the same mediaid. Avoids ~25 redundant
# round-trips per file. ``invalidate_entity(mediaid)`` is called after writes
# to the same file so the next read sees post-write state. The cache is
# cleared in full at every file-boundary entry point (``_entity_cache.clear()``
# at the top of process_one / process_one_from_sdc) so a long-running session
# doesn't accumulate one entity per file processed indefinitely — that
# unbounded growth caused multi-GB RSS on long NARA runs.
_entity_cache = {}


# Per-file accumulators populated by the builders during the per-claim walk
# and drained by ``_submit_per_item_edit`` at the end of each
# ``process_one`` / ``process_one_from_sdc`` call so the whole file's edits
# land as a single wbeditentity revision. Initialized at module level so
# helpers that read these globals (e.g. add_ref, add_det, the amend
# helpers) don't NameError when called from tests that don't first run a
# process_one* entry point. Cleared at the top of each process_one* call.
claims = {"claims": []}
refclaims = {"claims": []}
qualifier_amends = []  # list of (claimid, prop, snak_to_add)
qualifier_removals = []  # list of (claimid, snak_hash_to_remove)
removals = []  # list of statement IDs to remove from Commons

# Per-file DPLA ingest date. Set at the top of each ``process_one`` /
# ``process_one_from_sdc`` call from the item's ``ingestDate`` and used by
# ``formattedclaim`` (P813 stamp) and ``_flush_per_file_edits`` (reference
# refresh). ``None`` outside a process_one* call — reading it in that state
# is a programming error and raises so a rogue caller can't silently fall
# back to today. See ``ingest_wikimedia.sdc.ingest_date_from_doc`` for the
# rationale on pinning to the ingest date rather than today.
_current_ingest_date: datetime.date | None = None


def _reset_per_file_accumulators():
    """Drop every per-file accumulator at the start of a new file's
    processing. Used by both ``process_one`` and ``process_one_from_sdc``
    so the dispatcher only ever sees the current file's fragments."""
    global claims, refclaims, qualifier_amends, qualifier_removals, removals
    global _current_ingest_date
    claims = {"claims": []}
    refclaims = {"claims": []}
    qualifier_amends = []
    qualifier_removals = []
    removals = []
    _current_ingest_date = None


def _require_ingest_date() -> datetime.date:
    """Return the per-file ingest date set by the current ``process_one*``
    call. Raises ``RuntimeError`` when called outside a process_one* run —
    fallback to today would defeat the whole point of ingest-date pinning."""
    if _current_ingest_date is None:
        raise RuntimeError(
            "sdc_sync: no ingest_date set — a P813-producing helper was called "
            "outside process_one / process_one_from_sdc, or the entry point "
            "didn't set _current_ingest_date before the call."
        )
    return _current_ingest_date


def _url_snak(prop, url):
    """Build a Wikibase URL-typed qualifier snak (used for P2699 / P6108
    qualifier additions on P7482 via the per-file dispatcher)."""
    return {
        "snaktype": "value",
        "property": prop,
        "datavalue": {"value": url, "type": "string"},
        "datatype": "url",
    }


def _string_snak(prop, value):
    """Build a Wikibase string-typed qualifier snak (used for P304 page-
    number qualifier additions on P760 via the per-file dispatcher)."""
    return {
        "snaktype": "value",
        "property": prop,
        "datavalue": {"value": value, "type": "string"},
    }


def _merge_qualifier_snaks(existing_qualifiers, additions):
    """Build a Wikibase ``qualifiers`` dict that combines ``additions`` on
    top of ``existing_qualifiers``, preserving the existing snaks'
    ``hash`` fields so Wikibase recognises them as unchanged on
    ``wbeditentity``.

    ``existing_qualifiers`` is the claim's current ``qualifiers`` dict
    (property → list of snak dicts, as returned by ``wbgetentities``).
    ``additions`` is a list of ``(property, snak_dict)`` tuples to ADD;
    the new snaks are appended after any existing snaks for the same
    property.

    Wikibase's ``wbeditentity`` replaces the qualifier set wholesale, so
    callers must include every snak they want to keep. Including the
    existing snaks with their original hashes is what keeps the diff
    tight: Wikibase recognises unchanged snaks by hash and only shows
    the new ones in the per-file edit diff.
    """
    import copy as _copy

    merged = {
        prop: [_copy.deepcopy(snak) for snak in snaks]
        for prop, snaks in (existing_qualifiers or {}).items()
    }
    for prop, new_snak in additions:
        merged.setdefault(prop, []).append(_copy.deepcopy(new_snak))
    return merged


def _exclude_qualifier_snaks(existing_qualifiers, excluded_snak_hashes):
    """Build a Wikibase ``qualifiers`` dict that drops the snaks whose
    ``hash`` is in ``excluded_snak_hashes``, preserving everything else
    with its original hash.

    Wholesale-replace semantics on ``wbeditentity``: we send only the
    snaks we want to keep. Properties that end up with an empty snak
    list are dropped entirely (Wikibase rejects empty qualifier-property
    arrays as invalid).
    """
    import copy as _copy

    out = {}
    excluded = set(excluded_snak_hashes or ())
    for prop, snaks in (existing_qualifiers or {}).items():
        kept = [
            _copy.deepcopy(snak) for snak in snaks if snak.get("hash") not in excluded
        ]
        if kept:
            out[prop] = kept
    return out


def _raise_if_missing_entity(error, mediaid):
    """Raise :class:`_MissingEntityError` for ``APIError(code='no-such-entity')``;
    return silently otherwise.

    The partner-mode boundary catches ``_MissingEntityError`` distinctly
    so deleted-file skips can be counted as
    ``SDC_ORDINALS_SKIPPED_MISSING_ENTITY`` instead of folded into the
    generic error bucket. Callers MUST be inside an
    ``except pywikibot.exceptions.APIError`` block and MUST follow the
    silent-return path with their own ``raise`` (re-raise the original)
    or ``raise SomeRuntimeError(...) from error`` — otherwise the
    non-missing APIError is silently swallowed."""
    if (
        isinstance(error, pywikibot.exceptions.APIError)
        and error.code == "no-such-entity"
    ):
        raise _MissingEntityError(mediaid) from error


def get_entity(mediaid):
    """Return the wbgetentities response for mediaid, caching per process_one run."""
    cached = _entity_cache.get(mediaid)
    if cached is not None:
        return cached
    try:
        raw = site.simple_request(action="wbgetentities", ids=mediaid).submit()
    except pywikibot.exceptions.APIError as e:
        # A file deleted between upload and SDC sync surfaces here as
        # ``no-such-entity``; without the translation it would bubble
        # up as a generic Exception and be miscounted as an error in
        # the partner-mode Slack summary.
        _raise_if_missing_entity(e, mediaid)
        raise
    entity = raw.get("entities", {}).get(mediaid, {})
    _entity_cache[mediaid] = entity
    return entity


def invalidate_entity(mediaid):
    _entity_cache.pop(mediaid, None)


# Per-statement-property registry of additional qualifier properties DPLA
# writes via the add_* helpers below. Every formattedclaim() also stamps
# P459=Q61848113 (the determination-method marker), so P459 is always
# included implicitly via _allowed_qualifier_props.
#
# Keep this aligned with the add_* functions in this file:
#   * add_creator (P170)        — P2093 (author name string)
#   * add_date (P571)           — P1932 (stated as), P1480 (circa marker
#                                  when the source carried a
#                                  circa/[]/?/~/c./ca./approximately
#                                  decorator)
#   * add_contributed (P9126)   — P3831 (object has role)
#   * add_local_id (P217)       — P195 (collection)
#   * add_source (P7482)        — P973 (described at URL), P137 (operator),
#                                  P2699 (direct file download URL — per-
#                                  ordinal; materialized by sdc-sync from
#                                  file-list.txt at write time),
#                                  P6108 (IIIF manifest URL — per-item;
#                                  emitted by build_claims_for_doc when
#                                  the source carries iiifManifest)
# Every chunked claim (one whose mainsnak value was split across multiple
# statements by sdc.py) carries a P1545 (series ordinal) qualifier so the
# Lua template on Commons can reassemble the chunks. P1545 is therefore a
# DPLA-authored qualifier on every chunkable property and must be in the
# safe-to-amend allowed set. ``CHUNKABLE_PROPS`` is the single source of
# truth (imported from ingest_wikimedia.sdc); the dict below is built from
# it so adding a new chunkable property in sdc.py automatically widens the
# allowed-qualifier set here.
_DPLA_EXTRA_QUALIFIER_PROPS = {
    "P170": {"P2093"},
    "P571": {"P1932", "P1480"},
    "P9126": {"P3831"},
    "P217": {"P195"},
    "P7482": {"P973", "P137", "P2699", "P6108"},
    # P304 is the per-ordinal page-number qualifier on P760, materialized at
    # write time from upload-result.json by sdc-sync (multipage items only,
    # grouped per file extension).
    "P760": {"P304"},
}
for _chunkable_prop in CHUNKABLE_PROPS:
    _DPLA_EXTRA_QUALIFIER_PROPS.setdefault(_chunkable_prop, set()).add("P1545")


def _allowed_qualifier_props(prop):
    """Return the set of qualifier property IDs DPLA writes for a given
    statement property. Always includes P459 (the determination-method
    marker stamped by formattedclaim)."""
    return {"P459"} | _DPLA_EXTRA_QUALIFIER_PROPS.get(prop, set())


def _is_dpla_reference(reference):
    """Return True iff `reference` is a DPLA-authored reference, identified
    by a `P123 = Q2944483` snak (publisher = "Digital Public Library of
    America"). DPLA stamps that snak on every reference it writes via
    formattedclaim, so it's a sufficient marker for "we authored this".
    """
    snaks = (reference or {}).get("snaks") or {}
    for snak in snaks.get("P123") or []:
        try:
            if snak["datavalue"]["value"]["id"] == "Q2944483":
                return True
        except (KeyError, TypeError):
            continue
    return False


def _is_safe_to_amend_in_place(statement, prop):
    """Return True iff `statement` is safe to amend via
    wbeditentity-with-id without losing user-authored data.

    wbeditentity-with-id replaces a claim's qualifiers and references
    wholesale with what we send. The round-trip is data-preserving iff
    every existing qualifier/reference is already DPLA-authored — then
    our outgoing claim shape is a superset of the existing one and the
    write only adds the missing pieces.

    A statement is safe to amend iff:
      * every qualifier property is one DPLA writes for `prop`
        (P459 universally, plus the per-property extras tracked in
        `_DPLA_EXTRA_QUALIFIER_PROPS`), AND
      * every reference is a DPLA reference (carries the publisher
        marker checked by `_is_dpla_reference`).

    An empty qualifier dict and an empty references list both pass
    vacuously, so this also covers the "truly bare" case.

    The previous, looser gate `_is_dpla_shaped` returned True as soon
    as one P459 snak matched, ignoring other qualifiers on the same
    claim. A claim like {P459=Q61848113 (DPLA), P1001=Q30 (user)}
    was mis-classified as DPLA-shaped; amending it via
    wbeditentity-with-id silently erased the P1001 qualifier.
    """
    allowed = _allowed_qualifier_props(prop)
    for qualifier_prop in (statement.get("qualifiers") or {}).keys():
        if qualifier_prop not in allowed:
            return False
    for reference in statement.get("references") or []:
        if not _is_dpla_reference(reference):
            return False
    return True


def _qualifier_values(statement, prop):
    """Return the list of value-typed qualifier values for ``prop`` on
    ``statement``, skipping any snak that isn't well-formed (snaktype
    other than ``"value"``, missing or non-dict ``datavalue``, missing
    ``"value"`` key).

    Module-level helper so check(), _amend_p7482_url_qualifiers,
    _amend_p760_page_qualifier, and _extract_p1545_value all read
    qualifiers through one safe extraction path. Returning a list keeps
    callers flexible: ``[0]`` for first-match, ``in expected`` for
    presence, ``== [target]`` for exact-match.
    """
    out = []
    for q in (statement.get("qualifiers") or {}).get(prop, []) or []:
        if q.get("snaktype") != "value":
            continue
        dv = q.get("datavalue")
        if isinstance(dv, dict) and "value" in dv:
            out.append(dv["value"])
    return out


def _first_qualifier_value(statement, prop):
    """Return the first well-formed value-typed qualifier value for
    ``prop`` on ``statement``, or ``None`` when no such qualifier
    exists."""
    values = _qualifier_values(statement, prop)
    return values[0] if values else None


def _extract_p1545_value(statement):
    """Return the first P1545 (series ordinal) qualifier value on
    ``statement``, or ``None`` when no P1545 qualifier is present.

    P1545 marks chunked claims emitted by
    :func:`ingest_wikimedia.sdc._chunk_and_emit_claims`. Chunked-claim
    matching is chunk-by-chunk: a sdc.json claim with P1545="A1" only
    matches an existing Commons claim with the same mainsnak value AND
    the same P1545="A1" — distinct chunks (A1 vs A2) and chunked-vs-
    unchunked variants of the same text are kept separate so the Lua
    template can reassemble each series independently.
    """
    return _first_qualifier_value(statement, "P1545")


def check(mediaid, qid, prop):
    ref = ""
    existing_data = get_entity(mediaid)
    if not existing_data.get("pageid"):
        return True, ""
    try:
        if existing_data.get("statements").get(prop):
            statements = existing_data.get("statements").get(prop)
        else:
            return True, ""
    except Exception:
        return True, ""

    # Inspect existing statements that match `prop` and decide what to do.
    # The amend-in-place gate is `_is_safe_to_amend_in_place`: amend only
    # when every existing qualifier and reference is DPLA-authored, so the
    # wbeditentity-with-id round-trip cannot erase user-authored data.
    #
    #   * Capture `ref` from a matching no-reference statement that is
    #     safe to amend (truly bare, or contains only DPLA-authored
    #     qualifiers). The caller will stamp our reference via
    #     wbeditentity-with-id.
    #   * If a matching statement has no qualifiers at all, stamp our
    #     P459=Q61848113 qualifier onto it via wbsetqualifier
    #     (non-destructive — does not touch references).
    #   * If a matching statement is safe to amend AND has qualifiers,
    #     we already wrote this claim — don't add a duplicate; the
    #     missing ref (if any) is stamped via the captured `ref`.
    #   * If a matching statement contains any user-authored qualifier
    #     or reference, leave it untouched and add the DPLA-authored
    #     statement alongside as a separate claim, so the DPLA
    #     reference is scoped only to the DPLA-authored qualifiers.
    if qid[0] == "item":
        for statement in statements:
            if (
                statement["mainsnak"]["datavalue"]["value"]["id"] == qid[1]
                and not statement.get("references")
                and _is_safe_to_amend_in_place(statement, prop)
            ):
                ref = statement["id"]
                break
        for statement in statements:
            if statement["mainsnak"]["datavalue"]["value"]["id"] == qid[
                1
            ] and not statement.get("qualifiers"):
                if statement["id"] == ref:
                    # This is the same statement loop 1 captured for
                    # ref-stamping (no references, safe to amend). The caller
                    # rewrites it as a full formattedclaim — mainsnak + P459
                    # qualifier + DPLA reference — in one reference-update
                    # fragment. Calling add_det too would queue a SECOND,
                    # qualifier-only fragment for the same claim id, built from
                    # the cached (still reference-less) statement; the dispatcher
                    # concatenates it after the reference fragment, and
                    # wbeditentity applies same-id fragments as wholesale
                    # replacements in array order — so the qualifier fragment's
                    # empty references silently erase the reference we just
                    # added (the file then renders no license until a second
                    # pass). Deferring to add_ref adds both in one fragment.
                    return False, ref
                return add_det(mediaid, statement["id"]), ref

        if any(
            statement["mainsnak"]["datavalue"]["value"]["id"] == qid[1]
            and _is_safe_to_amend_in_place(statement, prop)
            for statement in statements
        ):
            print(
                f" -- There already exists a DPLA-authored statement with a {prop} > {qid[1]} claim for {mediaid}."
            )
            return False, ref

        if any(
            statement["mainsnak"]["datavalue"]["value"]["id"] == qid[1]
            for statement in statements
        ):
            print(
                f" -- A foreign {prop} > {qid[1]} statement exists for {mediaid}; adding the DPLA-authored statement alongside."
            )
            return True, ""

        return True, ref
    if qid[0] == "string":
        # qid[1] is (value, p1545) from _extract_comparable_value on the new
        # sdc.json path. Legacy callers (dpla_claims) still pass the raw
        # string — accept that shape too by wrapping to the tuple form with
        # p1545=None, which correctly preserves their pre-chunking behavior
        # (those code paths never produce chunked claims).
        target_value, target_p1545 = (
            qid[1] if isinstance(qid[1], tuple) else (qid[1], None)
        )
        for statement in statements:
            if (
                statement["mainsnak"]["datavalue"]["value"] == target_value
                and _extract_p1545_value(statement) == target_p1545
                and not statement.get("references")
                and _is_safe_to_amend_in_place(statement, prop)
            ):
                ref = statement["id"]
                break
        # The bare-add-det branch (stamp P459 onto an existing qualifier-less
        # statement) is meaningful only for unchunked values — a chunked
        # sdc.json claim shouldn't graft itself onto a pre-existing bare
        # statement, since they represent different chunks of different
        # series. Restrict the branch to target_p1545 is None.
        if target_p1545 is None:
            for statement in statements:
                if statement["mainsnak"]["datavalue"][
                    "value"
                ] == target_value and not statement.get("qualifiers"):
                    if statement["id"] == ref:
                        # See the item branch: add_ref already rewrites this
                        # statement as a full claim with the P459 qualifier and
                        # the DPLA reference; also calling add_det would queue a
                        # competing same-id qualifier-only fragment whose stale
                        # empty references erase the reference. Defer to add_ref.
                        return False, ref
                    return add_det(mediaid, statement["id"]), ref

        if any(
            statement["mainsnak"]["datavalue"]["value"] == target_value
            and _extract_p1545_value(statement) == target_p1545
            and _is_safe_to_amend_in_place(statement, prop)
            for statement in statements
        ):
            print(
                f" -- There already exists a DPLA-authored statement with a {prop} > {target_value} claim for {mediaid}."
            )
            return False, ref

        if any(
            statement["mainsnak"]["datavalue"]["value"] == target_value
            and _extract_p1545_value(statement) == target_p1545
            for statement in statements
        ):
            print(
                f" -- A foreign {prop} > {target_value} statement exists for {mediaid}; adding the DPLA-authored statement alongside."
            )
            return True, ""

        return True, ref
    if qid[0] == "monolingualtext":
        # qid[1] is (text, p1545); legacy callers still pass raw text — same
        # backwards-compat shim as the string branch above.
        target_value, target_p1545 = (
            qid[1] if isinstance(qid[1], tuple) else (qid[1], None)
        )
        for statement in statements:
            if (
                statement["mainsnak"]["datavalue"]["value"]["text"] == target_value
                and _extract_p1545_value(statement) == target_p1545
                and not statement.get("references")
                and _is_safe_to_amend_in_place(statement, prop)
            ):
                ref = statement["id"]
                break
        if target_p1545 is None:
            for statement in statements:
                if statement["mainsnak"]["datavalue"]["value"][
                    "text"
                ] == target_value and not statement.get("qualifiers"):
                    if statement["id"] == ref:
                        # See the item branch: add_ref already rewrites this
                        # statement as a full claim with the P459 qualifier and
                        # the DPLA reference; also calling add_det would queue a
                        # competing same-id qualifier-only fragment whose stale
                        # empty references erase the reference. Defer to add_ref.
                        return False, ref
                    return add_det(mediaid, statement["id"]), ref

        if any(
            statement["mainsnak"]["datavalue"]["value"]["text"] == target_value
            and _extract_p1545_value(statement) == target_p1545
            and _is_safe_to_amend_in_place(statement, prop)
            for statement in statements
        ):
            print(
                f" -- There already exists a DPLA-authored statement with a {prop} > {target_value} claim for {mediaid}."
            )
            return False, ref

        if any(
            statement["mainsnak"]["datavalue"]["value"]["text"] == target_value
            and _extract_p1545_value(statement) == target_p1545
            for statement in statements
        ):
            print(
                f" -- A foreign {prop} > {qid[1]} statement exists for {mediaid}; adding the DPLA-authored statement alongside."
            )
            return True, ""

        return True, ref
    if qid[0] == "somevalue":
        p = "P1932" if prop == "P571" else "P2093"
        try:
            if any(statement.get("qualifiers", {}).get(p) for statement in statements):
                # Capture ref only from DPLA-shaped matching statements.
                # somevalue claims always have at least the P1932/P2093
                # qualifier we matched on, so "no qualifiers" is impossible;
                # the DPLA-shaped check is the only safe gate.
                for statement in statements:
                    qualifiers = statement.get("qualifiers", {}).get(p) or []
                    if (
                        any(
                            q.get("datavalue", {}).get("value") == qid[1]
                            for q in qualifiers
                        )
                        and _is_safe_to_amend_in_place(statement, prop)
                        and not statement.get("references")
                    ):
                        ref = statement["id"]
                        break
                # Already-our-write check: a DPLA-shaped statement with the
                # matching qualifier value means we wrote this claim before.
                # Don't add a duplicate.
                for statement in statements:
                    qualifiers = statement.get("qualifiers", {}).get(p) or []
                    if any(
                        q.get("datavalue", {}).get("value") == qid[1]
                        for q in qualifiers
                    ) and _is_safe_to_amend_in_place(statement, prop):
                        print(
                            f" -- There already exists a DPLA-authored statement with a {prop} > {qid[1]} claim for {mediaid}."
                        )
                        return False, ref
                # Foreign matching statement: leave alone, add ours as a
                # separate claim.
                for statement in statements:
                    qualifiers = statement.get("qualifiers", {}).get(p) or []
                    if any(
                        q.get("datavalue", {}).get("value") == qid[1]
                        for q in qualifiers
                    ):
                        print(
                            f" -- A foreign {prop} > {qid[1]} statement exists for {mediaid}; adding the DPLA-authored statement alongside."
                        )
                        return True, ""
                return True, ref
            else:
                return True, ref
        except KeyError:
            return True, ref
    if qid[0] == "time":
        # Mirrors the item/string/somevalue branches above for value-typed
        # time claims (P571 when ``parse_dpla_date`` succeeded). Compares
        # canonical (time, precision) keys via ``_time_comparable``.
        #
        # A Commons statement with the OLD somevalue+P1932 shape does NOT
        # match here, so the new value-typed claim is added; the
        # corresponding old somevalue claim will be queued for removal by
        # ``_reconcile_existing_claims`` (its P1932 string isn't in
        # ``expected`` once the sdc.json carries the value-typed
        # equivalent). One reconcile cycle migrates the file from old to
        # new without leaving the date duplicated.
        target = qid[1]

        def _statement_value_time_matches(statement) -> bool:
            if statement["mainsnak"].get("snaktype") != "value":
                return False
            dv = statement["mainsnak"].get("datavalue") or {}
            if dv.get("type") != "time":
                return False
            try:
                return _time_claim_comparable(statement) == target
            except (KeyError, TypeError):
                # Malformed Commons time datavalue (missing ``time`` or
                # ``precision``). Treat as non-matching so check()
                # returns True and the new claim is still added —
                # ``_reconcile_existing_claims`` will queue the malformed
                # statement for removal via its own defensive guard.
                # Without this, the KeyError would bubble past check()'s
                # APIError-only handler in ``process_one_from_sdc`` and
                # abort the whole ordinal, leaving the bad statement
                # on Commons.
                return False

        for statement in statements:
            if (
                _statement_value_time_matches(statement)
                and not statement.get("references")
                and _is_safe_to_amend_in_place(statement, prop)
            ):
                ref = statement["id"]
                break
        for statement in statements:
            if _statement_value_time_matches(statement) and not statement.get(
                "qualifiers"
            ):
                if statement["id"] == ref:
                    # See the item branch: add_ref already rewrites this
                    # statement as a full claim with the P459 qualifier and the
                    # DPLA reference; also calling add_det would queue a
                    # competing same-id qualifier-only fragment whose stale empty
                    # references erase the reference. Defer to add_ref.
                    return False, ref
                return add_det(mediaid, statement["id"]), ref

        if any(
            _statement_value_time_matches(statement)
            and _is_safe_to_amend_in_place(statement, prop)
            for statement in statements
        ):
            print(
                f" -- There already exists a DPLA-authored statement with a {prop} > {target} claim for {mediaid}."
            )
            return False, ref

        if any(_statement_value_time_matches(statement) for statement in statements):
            print(
                f" -- A foreign {prop} > {target} statement exists for {mediaid}; adding the DPLA-authored statement alongside."
            )
            return True, ""

        return True, ref
    if qid[0] == "source":
        try:
            if any(
                statement.get("qualifiers", {}).get("P973") for statement in statements
            ):
                # Same logic as the somevalue branch: P7482 source claims
                # always have at least the P973 qualifier we matched on,
                # so amend-safety hinges on whether the statement is
                # DPLA-shaped (carries P459=Q61848113).
                for statement in statements:
                    qualifiers = statement.get("qualifiers", {}).get("P973") or []
                    if (
                        any(
                            q.get("datavalue", {}).get("value") == qid[1]
                            for q in qualifiers
                        )
                        and _is_safe_to_amend_in_place(statement, prop)
                        and not statement.get("references")
                    ):
                        ref = statement["id"]
                        break
                # Already-our-write check.
                for statement in statements:
                    qualifiers = statement.get("qualifiers", {}).get("P973") or []
                    if any(
                        q.get("datavalue", {}).get("value") == qid[1]
                        for q in qualifiers
                    ) and _is_safe_to_amend_in_place(statement, prop):
                        print(
                            f" -- There already exists a DPLA-authored statement with a {prop} > {qid[1]} claim for {mediaid}."
                        )
                        return False, ref
                # Foreign matching statement: leave alone, add ours alongside.
                for statement in statements:
                    qualifiers = statement.get("qualifiers", {}).get("P973") or []
                    if any(
                        q.get("datavalue", {}).get("value") == qid[1]
                        for q in qualifiers
                    ):
                        print(
                            f" -- A foreign {prop} > {qid[1]} statement exists for {mediaid}; adding the DPLA-authored statement alongside."
                        )
                        return True, ""
                return True, ref
            else:
                return True, ref
        except KeyError:
            return True, ref
    # Unrecognized qid type — treat claim as absent; no existing ref to update.
    print(
        f" -- check() fallback: unrecognized qid type '{qid[0]}' for {mediaid}, {prop}"
    )
    return True, ""


# The following functions define specific statements to add, and uses formattedclaim() to append them to the "claims" array. It first uses the check() to check if the statement is not yet in the item, and appends it the list of statements to add in the edit if not. check() returns True, False, or the string value of a statement id.


def add_rs(mediaid, rs, dpla_id):
    prop = None
    qid = None
    rs_key = _normalize_rights_uri(rs)
    rights_entry = rights.get(rs_key)
    if rights_entry:
        prop = list(rights_entry)[0]
        qid = rights_entry[prop]
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
        if prop == "P275" and qid != "Q6938433":
            prop = "P6216"
            qid = "Q50423863"

        if prop == "P6426":
            prop = "P6216"
            qid = "Q19652"

        if qid == "Q6938433":
            prop = "P6216"
            qid = "Q88088423"

    if rs_key == "http://creativecommons.org/publicdomain/mark/1.0":
        prop = "P6216"
        qid = "Q19652"

    if prop is not None:
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_collection(mediaid, hub, institution, dpla_id):
    if hub == "Q518155":
        institution = hub
    if institution:
        qid = institution
        prop = "P195"
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_access(mediaid, access, dpla_id):
    if access:
        qid = access
        prop = "P7228"
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_level(mediaid, level, dpla_id):
    if level:
        qid = level
        prop = "P6224"
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_id(mediaid, id):
    prop = "P760"
    summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
    claim = formattedclaim(prop, id, "string", id)
    checkclaim = check(mediaid, ("string", id), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] is True:
        pywikibot.output(summary)
        claims["claims"].append(claim)


def add_naid(mediaid, naid, dpla_id):
    prop = "P1225"
    summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
    claim = formattedclaim(prop, naid, "string", dpla_id)
    checkclaim = check(mediaid, ("string", naid), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] is True:
        pywikibot.output(summary)
        claims["claims"].append(claim)


def add_subject(mediaid, subject, dpla_id):
    prop = "P4272"
    summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
    claim = formattedclaim(prop, subject, "string", dpla_id)
    checkclaim = check(mediaid, ("string", subject), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] is True:
        pywikibot.output(summary)
        claims["claims"].append(claim)


def add_subject_entity(mediaid, qid, dpla_id):
    prop = "P921"
    summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
    claim = formattedclaim(
        prop,
        {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
        "wikibase-entityid",
        dpla_id,
    )
    checkclaim = check(mediaid, ("item", qid), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] is True:
        pywikibot.output(summary)
        claims["claims"].append(claim)


def add_title(mediaid, title, dpla_id):
    if title:
        prop = "P1476"
        # Normalize once: dedupe must compare what we'd actually post, not the
        # raw input. Otherwise reruns where the raw string differs from the
        # truncated/rstripped form (long titles, trailing whitespace) treat the
        # claim as missing and post a duplicate. Also skip whitespace-only
        # values so we don't post empty claims.
        normalized = title[:1499].rstrip()
        if not normalized:
            return
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(
            prop,
            {"text": normalized, "language": "en"},
            "monolingualtext",
            dpla_id,
        )
        checkclaim = check(mediaid, ("monolingualtext", normalized), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_desc(mediaid, desc, dpla_id):
    if desc:
        prop = "P10358"
        normalized = desc[:1499].rstrip()
        if not normalized:
            return
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(
            prop,
            {"text": normalized, "language": "en"},
            "monolingualtext",
            dpla_id,
        )
        checkclaim = check(mediaid, ("monolingualtext", normalized), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_creator(mediaid, creator, dpla_id):
    if creator:
        prop = "P170"
        normalized = creator[:1499].rstrip()
        if not normalized:
            return
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(prop, "somevalue", "wikibase-entityid", dpla_id)
        claim["qualifiers"]["P2093"] = [
            {
                "snaktype": "value",
                "property": "P2093",
                "datavalue": {"value": normalized, "type": "string"},
            }
        ]
        checkclaim = check(mediaid, ("somevalue", normalized), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_date(mediaid, date, dpla_id):
    prop = "P571"
    normalized = date[:1499].rstrip()
    if not normalized:
        return
    summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
    claim = formattedclaim(prop, "somevalue", "time", dpla_id)
    claim["qualifiers"]["P1932"] = [
        {
            "snaktype": "value",
            "property": "P1932",
            "datavalue": {"value": normalized, "type": "string"},
        }
    ]
    checkclaim = check(mediaid, ("somevalue", normalized), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] is True:
        pywikibot.output(summary)
        claims["claims"].append(claim)


def add_contributed(mediaid, hub, institution, dpla_id):
    prop = "P9126"
    summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
    qid = "Q2944483"
    claim = formattedclaim(
        prop,
        {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
        "wikibase-entityid",
        dpla_id,
    )
    claim["qualifiers"]["P3831"] = [
        {
            "snaktype": "value",
            "property": "P3831",
            "datavalue": {
                "value": {"entity-type": "item", "numeric-id": 393351},
                "type": "wikibase-entityid",
            },
        }
    ]
    checkclaim = check(mediaid, ("item", qid), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] is True:
        pywikibot.output(summary)
        claims["claims"].append(claim)
    if hub == "Q518155":
        qid = "Q518155"
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        claim["qualifiers"]["P3831"] = [
            {
                "snaktype": "value",
                "property": "P3831",
                "datavalue": {
                    "value": {"entity-type": "item", "numeric-id": 108296843},
                    "type": "wikibase-entityid",
                },
            }
        ]
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
        qid = institution
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        claim["qualifiers"]["P3831"] = [
            {
                "snaktype": "value",
                "property": "P3831",
                "datavalue": {
                    "value": {"entity-type": "item", "numeric-id": 108296919},
                    "type": "wikibase-entityid",
                },
            }
        ]
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
    else:
        qid = hub
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        claim["qualifiers"]["P3831"] = [
            {
                "snaktype": "value",
                "property": "P3831",
                "datavalue": {
                    "value": {"entity-type": "item", "numeric-id": 393351},
                    "type": "wikibase-entityid",
                },
            }
        ]
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)
        qid = institution
        claim = formattedclaim(
            prop,
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "wikibase-entityid",
            dpla_id,
        )
        claim["qualifiers"]["P3831"] = [
            {
                "snaktype": "value",
                "property": "P3831",
                "datavalue": {
                    "value": {"entity-type": "item", "numeric-id": 108296843},
                    "type": "wikibase-entityid",
                },
            }
        ]
        checkclaim = check(mediaid, ("item", qid), prop)
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_local_id(mediaid, id, institution, dpla_id):
    if id:
        prop = "P217"
        summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
        claim = formattedclaim(prop, id, "string", dpla_id)
        checkclaim = check(mediaid, ("string", id), prop)
        claim["qualifiers"]["P195"] = [
            {
                "snaktype": "value",
                "property": "P195",
                "datavalue": {
                    "value": {
                        "entity-type": "item",
                        "numeric-id": int(institution.replace("Q", "")),
                    },
                    "type": "wikibase-entityid",
                },
            }
        ]
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(summary)
            claims["claims"].append(claim)


def add_source(mediaid, hub, url, dpla_id):
    qid = "Q74228490"
    prop = "P7482"
    summary = f" -- Adding [[:d:Property:{prop}]] to {mediaid}."
    claim = formattedclaim(
        prop,
        {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
        "wikibase-entityid",
        dpla_id,
    )
    claim["qualifiers"]["P973"] = [
        {
            "snaktype": "value",
            "property": "P973",
            "datavalue": {"value": url, "type": "string"},
            "datatype": "url",
        }
    ]
    claim["qualifiers"]["P137"] = [
        {
            "snaktype": "value",
            "property": "P137",
            "datavalue": {
                "value": {
                    "entity-type": "item",
                    "numeric-id": int(hub.replace("Q", "")),
                },
                "type": "wikibase-entityid",
            },
        }
    ]
    checkclaim = check(mediaid, ("source", url), prop)
    if checkclaim[1]:
        add_ref(checkclaim[1], claim)
    if checkclaim[0] is True:
        pywikibot.output(summary)
        claims["claims"].append(claim)


def add_det(mediaid, claimid):
    """Queue a ``P459 = Q61848113`` (determination method = heuristic)
    qualifier addition onto an existing claim, to be flushed by the per-
    file dispatcher (``_submit_per_item_edit``) in the combined
    ``wbeditentity``.

    Pushes a single-snak ``(P459, snak)`` entry onto the module-level
    ``qualifier_amends`` accumulator under ``claimid``. The dispatcher
    later reads the existing claim's qualifier set from the cached
    entity, merges the new P459 snak in (preserving the existing snaks'
    hashes via :func:`_merge_qualifier_snaks`), and includes the
    resulting full qualifier set in the combined edit's payload.

    No POST happens here — the cache stays valid because no remote
    state has been mutated. Idempotent at the accumulator level: if
    multiple claims happen to target the same ``claimid``, the
    dispatcher de-duplicates by id before building the fragment.
    """
    if not claimid:
        return
    qid = "Q61848113"
    snak = {
        "snaktype": "value",
        "property": "P459",
        "datavalue": {
            "value": {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))},
            "type": "wikibase-entityid",
        },
        "datatype": "wikibase-item",
    }
    qualifier_amends.append((claimid, "P459", snak))


def _amend_p7482_url_qualifiers(mediaid, dpla_id, sdc_payload, download_url):
    """For an EXISTING DPLA-authored P7482 statement on Commons, stamp
    any missing ``P2699`` (download URL) or ``P6108`` (IIIF manifest URL)
    qualifier via wbsetqualifier.

    Why this exists: every file uploaded before this code shipped has a
    P7482 with only P973 / P137 / P459 qualifiers. The normal ``check()``
    path for source claims finds the existing statement, returns False
    ("don't add a duplicate"), and never gets a chance to add the new
    qualifiers. This function fills that gap — it's the same shape as
    :func:`add_det` (which stamps P459 onto bare claims), generalized to
    two new qualifier properties.

    Match-and-amend logic, in order:

      1. Look for a P7482 statement on Commons whose P973 qualifier
         matches the sdc.json's P973 (the partner catalog URL — uniquely
         identifies "our" P7482 vs. a community editor's).
      2. Confirm it's safe to amend in place (``_is_safe_to_amend_in_place``
         — every qualifier on the statement is one DPLA writes, every
         reference carries the DPLA publisher marker).
      3. For each of P2699 (when ``download_url`` is supplied) and
         P6108 (when the sdc.json carries one), check if a qualifier
         with the matching value is already present. Skip if yes; POST
         wbsetqualifier if not.

    Idempotent: re-running the same call against the same Commons state
    sends zero requests once both qualifiers are present.

    Best-effort: a postqual that fails just logs (postqual's own error
    handler) and continues — the reconciler doesn't depend on this pass
    succeeding, and the next sync attempt will retry the missing
    qualifier.

    Design notes:
      * DPLA-authored P7482 statements reference the partner's source
        metadata; they semantically belong to that reference. Any
        community-added qualifiers on those specific statements are
        out of band — a community contributor wanting to assert a
        fact about the file should add their own statement with their
        own reference, not graft onto DPLA's. So when URL drift
        triggers a remove-and-re-add cycle (via the reconciler's
        normal "P973 mismatch → not our statement → add a new one"
        path), discarding any stray user additions on the old
        DPLA-referenced statement is correct, not lossy.
      * That said, P973 matching is exact-string today. Real-world
        catalog URLs drift over time (http → https, trailing slash,
        percent-encoding) and each drift causes an unnecessary
        remove+re-add churn even though both URLs point to the same
        resource. A normalization helper here and in ``check()``
        would be a pure efficiency win; consciously out of scope
        for this PR.
      * The in-memory P2699 augmentation in ``process_one_from_sdc``
        only fires on the new-upload path (when ``check()`` decides
        to ADD a P7482). For the far more common existing-statement
        case, this helper is the path that lands P2699/P6108 onto
        Commons.
    """
    # Find sdc.json's P7482 claim to learn the expected catalog URL +
    # IIIF manifest URL (if any). sdc.json has at most one P7482 entry.
    sdc_p7482 = next(
        (
            c
            for c in sdc_payload.get("claims", [])
            if c["mainsnak"]["property"] == "P7482"
        ),
        None,
    )
    if sdc_p7482 is None:
        return

    expected_p973 = _first_qualifier_value(sdc_p7482, "P973")
    expected_p6108 = _first_qualifier_value(sdc_p7482, "P6108")

    # Read once from the cached entity. The dispatcher does no writes
    # between this helper and the entity snapshot in cache, so no
    # invalidate-before-read needed.
    entity = get_entity(mediaid)
    existing_p7482 = (entity.get("statements") or {}).get("P7482") or []

    # Find the DPLA-authored statement whose P973 qualifier matches.
    target_stmt = None
    for stmt in existing_p7482:
        if not _is_safe_to_amend_in_place(stmt, "P7482"):
            continue
        if _first_qualifier_value(stmt, "P973") == expected_p973:
            target_stmt = stmt
            break
    if target_stmt is None:
        # No existing match — either no P7482 yet (a fresh claim will be
        # added through the normal ``_post_new_claims`` accumulator), or
        # only foreign / unsafe-to-amend statements exist.
        return

    claimid = target_stmt["id"]
    existing_p2699_values = _qualifier_values(target_stmt, "P2699")
    existing_p6108_values = _qualifier_values(target_stmt, "P6108")

    if download_url and download_url not in existing_p2699_values:
        qualifier_amends.append((claimid, "P2699", _url_snak("P2699", download_url)))
    if expected_p6108 and expected_p6108 not in existing_p6108_values:
        qualifier_amends.append((claimid, "P6108", _url_snak("P6108", expected_p6108)))


def _file_extension(title):
    """Return the lowercased extension of a Commons file title, or ``""``
    when no extension is present.

    Uploader-produced titles look like ``File:Some Image - 1.jpg``; we want
    ``"jpg"`` so ordinals can be grouped per-format for P304 numbering.
    """
    # Check the separator (not the third element): rpartition returns the
    # entire string as the third element when "." is absent, which would
    # otherwise misclassify a dotless title as having an extension equal
    # to the title itself.
    _, sep, ext = (title or "").rpartition(".")
    return ext.lower() if sep else ""


def _compute_page_numbers(ordinal_items):
    """Compute per-ordinal P304 (page-number) values for a multipage item,
    grouped per file extension.

    ``ordinal_items`` is the ``[(ord_str, data), ...]`` list of eligible
    ordinals for the item, sorted by ordinal. Returns a mapping
    ``ord_str → page_number`` populated only for ordinals belonging to a
    multi-file extension group. Ordinals in single-file groups are
    omitted — that file isn't part of a multipage series within its own
    format.

    Example: an item with 3 JPGs and 2 PDFs returns
    ``{jpg_ord_1: 1, jpg_ord_2: 2, jpg_ord_3: 3, pdf_ord_1: 1, pdf_ord_2: 2}``.
    An item with one JPG and one PDF returns ``{}`` — neither file is part
    of a multipage series within its own format.

    Page numbers reset per extension group so an item's JPGs are numbered
    1, 2, 3 independently of its PDFs (per the user-confirmed rule:
    "we are only numbering within each ordinal series — JPGs and PDFs are
    numbered separately").

    Assumes ``ordinal_items`` is already sorted by integer ordinal — the
    caller in ``_run_partner_mode`` sorts before calling — so iteration
    order produces the expected per-extension sequence (the first JPG by
    ordinal gets P304=1, the second gets P304=2, etc.).
    """
    ext_groups = {}
    for ord_str, data in ordinal_items:
        ext = _file_extension(data.get("title") or "")
        ext_groups.setdefault(ext, []).append(ord_str)
    page_numbers = {}
    for ord_strs in ext_groups.values():
        if len(ord_strs) <= 1:
            continue
        for idx, ord_str in enumerate(ord_strs, start=1):
            page_numbers[ord_str] = idx
    return page_numbers


def _amend_p760_page_qualifier(mediaid, dpla_id, sdc_payload, page_number):
    """Stamp a missing ``P304`` (page-number) qualifier onto an existing
    DPLA-authored ``P760`` (DPLA ID) statement on Commons.

    Mirror of :func:`_amend_p7482_url_qualifiers` for P304. Every file
    uploaded before this code shipped has a P760 with only its DPLA
    publisher reference and P459 qualifier; without this pass, the
    normal ``check()`` path finds the existing P760, returns False
    ("don't add a duplicate"), and P304 never lands.

    Matching is by mainsnak value (the DPLA ID itself). One P760 per
    MediaInfo entity carries that DPLA ID, so the match is unambiguous.

    A no-op when no DPLA-authored P760 exists yet or when the existing
    P304 already matches the expected value. When ``page_number`` is
    ``None`` (the ordinal is no longer part of a multi-file extension
    group), still walks the existing P304 qualifiers and removes any
    stale entries — the reconciler doesn't diff P304, so without this
    cleanup pass a file would keep incorrect page metadata after its
    sibling ordinals were deleted.
    """
    # ``page_number=None`` means "no P304 is expected for this file" —
    # typically a file that was once part of a multi-file extension
    # group (with a P304 stamped) but is now a singleton in its group
    # (e.g. siblings were deleted by a Commons curator). We must still
    # walk the existing qualifiers and queue any stale P304 removal;
    # otherwise the file keeps incorrect page metadata indefinitely.
    expected_value = None if page_number is None else str(page_number)

    entity = get_entity(mediaid)
    existing_p760 = (entity.get("statements") or {}).get("P760") or []

    target_stmt = None
    for stmt in existing_p760:
        if not _is_safe_to_amend_in_place(stmt, "P760"):
            continue
        try:
            mainsnak_value = stmt["mainsnak"]["datavalue"]["value"]
        except (KeyError, TypeError):
            continue
        if mainsnak_value == dpla_id:
            target_stmt = stmt
            break
    if target_stmt is None:
        return

    # Bucket existing P304 snaks: matches-expected (skip) vs stale (queue
    # for removal). The dispatcher will assemble the wholesale-replace
    # qualifier set from the combined adds and removes across all
    # accumulators when it builds the final wbeditentity payload.
    expected_already_present = False
    for q in target_stmt.get("qualifiers", {}).get("P304", []) or []:
        if q.get("snaktype") != "value":
            continue
        dv = q.get("datavalue")
        if not (isinstance(dv, dict) and "value" in dv):
            continue
        if expected_value is not None and dv["value"] == expected_value:
            expected_already_present = True
        elif q.get("hash"):
            qualifier_removals.append((target_stmt["id"], q["hash"]))

    if expected_value is not None and not expected_already_present:
        qualifier_amends.append(
            (target_stmt["id"], "P304", _string_snak("P304", expected_value))
        )


def add_ref(claimid, claim):
    if claimid:
        claim["id"] = claimid
        refclaims["claims"].append(claim)
        print(f" -- Adding reference for {claimid}.")


# --- helpers for the new partner-mode SDC path (PR 4) ---
#
# `_extract_comparable_value` and `_check_kind_for_claim` invert the same
# shape-aware extraction the existing `dpla_claims()` reconciler performs on
# already-on-Commons statements, so a precomputed sdc.json claim and its
# Commons counterpart compare equal when they represent the same logical
# fact. They drive both:
#
#   * the per-claim `check(mediaid, (kind, value), prop)` call in
#     `process_one_from_sdc` (which decides whether to add the claim or
#     just stamp a missing DPLA reference on an existing one), and
#   * `_build_expected_from_sdc`, which produces the
#     `{prop: [comparable_values]}` map `_reconcile_existing_claims` uses to
#     decide which DPLA-published statements on Commons are no longer
#     warranted and should be removed.


def _check_kind_for_claim(claim):
    """Return the 'kind' tag (item|string|monolingualtext|somevalue|source|time)
    that `check()` uses to dispatch its per-type matcher against existing
    Commons statements.
    """
    prop = claim["mainsnak"]["property"]
    if claim["mainsnak"]["snaktype"] == "somevalue":
        return "somevalue"
    if prop == "P7482":
        # P7482 (described at) carries its variable info in a P973 qualifier,
        # not in the mainsnak (which is always Q74228490 source-catalog).
        return "source"
    datavalue = claim["mainsnak"]["datavalue"]
    dtype = datavalue["type"]
    if dtype == "wikibase-entityid":
        return "item"
    return dtype


def _time_comparable(value):
    """Canonical comparable key for a Wikibase time datavalue.

    Used wherever a P571 (or other time-typed) claim's value needs to
    compare equal to another for "is this the same fact?" purposes.
    Includes only the salient identity fields — ``time`` and
    ``precision``. ``timezone``, ``before``, ``after``, and
    ``calendarmodel`` are constants in DPLA's writes; any drift in
    those would have been a user-edit, and ``_is_safe_to_amend_in_place``
    keeps user-edited statements out of this code path anyway.

    Distinct from any P1932 stated-as string a somevalue claim could
    produce, so the migration from ``somevalue+P1932="1945"`` to a
    value-typed ``time("+1945-01-01T00:00:00Z", precision=9)`` is
    correctly detected as a different fact by the reconciler
    (old removed, new added in one cycle).
    """
    return f"{value['time']}|P{value['precision']}"


def _claim_has_circa_marker(claim):
    """True iff the claim carries a ``P1480 = Q5727902`` (circa)
    qualifier — the sourcing-circumstances marker the builder stamps
    when a DPLA display-date had a ``circa``/``[…]``/``?``-style
    decorator."""
    for q in claim.get("qualifiers", {}).get("P1480") or []:
        if q.get("snaktype") != "value":
            continue
        dv = q.get("datavalue") or {}
        if dv.get("type") != "wikibase-entityid":
            continue
        v = dv.get("value") or {}
        if not isinstance(v, dict):
            continue
        qid = v.get("id") or (f"Q{v['numeric-id']}" if "numeric-id" in v else None)
        if qid == "Q5727902":
            return True
    return False


def _time_claim_comparable(claim):
    """Canonical comparable key for a *value-typed time* claim, including
    the circa-bit derived from the P1480 qualifier.

    Without including the circa-bit, a DPLA source change
    ``"1945"`` → ``"circa 1945"`` (the time canonical key
    ``+1945-01-01T00:00:00Z|P9`` is unchanged) would not trigger the
    reconciler to add the new P1480 qualifier to the existing Commons
    claim. With the circa-bit suffix, the two versions have different
    comparables and the reconciler correctly queues old-without-P1480
    for removal so the new-with-P1480 (or vice versa) can be written
    in its place.

    Wraps :func:`_time_comparable`; the inner call may still raise
    ``KeyError`` / ``TypeError`` on a malformed datavalue, which
    callers handle as before.
    """
    base = _time_comparable(claim["mainsnak"]["datavalue"]["value"])
    return f"{base}|circa" if _claim_has_circa_marker(claim) else base


def _extract_comparable_value(claim):
    """Pull the comparable scalar value out of a precomputed sdc.json claim.

    Matches what `_reconcile_existing_claims` extracts from existing
    Commons statements so the two sides line up. Returns:

      * Q-ID string for wikibase-entityid (e.g. "Q19652")
      * the raw string for string-typed claims (P760, P217, etc.)
      * the text body for monolingualtext claims (P1476, P10358)
      * the canonical time key for time-typed claims (P571 when parseable)
      * the P1932/P2093 qualifier value for somevalue claims (P571, P170)
      * the P973 qualifier value for the P7482 source-catalog claim
      * None when the claim shape isn't one we know how to compare

    A None return signals "skip this claim in expected-building"; the
    matching `process_one_from_sdc` will still post the claim if a similar
    one isn't already on Commons.
    """
    mainsnak = claim["mainsnak"]
    prop = mainsnak["property"]
    if mainsnak["snaktype"] == "somevalue":
        qualifier_p = "P1932" if prop == "P571" else "P2093"
        try:
            return claim["qualifiers"][qualifier_p][0]["datavalue"]["value"]
        except (KeyError, IndexError, TypeError):
            return None
    if prop == "P7482":
        try:
            return claim["qualifiers"]["P973"][0]["datavalue"]["value"]
        except (KeyError, IndexError, TypeError):
            return None
    datavalue = mainsnak["datavalue"]
    dtype = datavalue["type"]
    if dtype == "wikibase-entityid":
        v = datavalue["value"]
        return v.get("id") or f"Q{v['numeric-id']}"
    if dtype == "string":
        # Tuple shape so chunk-by-chunk matching can distinguish chunks of
        # the same logical value (A1 vs A2) and chunked-vs-unchunked
        # variants of the same text. ``p1545`` is None for unchunked claims.
        return (datavalue["value"], _extract_p1545_value(claim))
    if dtype == "monolingualtext":
        return (datavalue["value"]["text"], _extract_p1545_value(claim))
    if dtype == "time":
        try:
            return _time_claim_comparable(claim)
        except (KeyError, TypeError):
            # Malformed time datavalue (missing "time" or "precision" key).
            # Skip rather than crash the whole expected-build; the
            # corresponding Commons-side extraction has the same guard
            # so the two sides stay symmetric.
            return None
    return None


def _build_expected_from_sdc(sdc_payload):
    """Build `{prop: [comparable_values]}` from a precomputed sdc.json payload.

    Same shape as `_build_expected_from_parsed` so `_reconcile_existing_claims`
    can consume either source. Skips any claim whose comparable value can't
    be extracted (shape we don't know how to diff).
    """
    expected = {}
    for claim in sdc_payload.get("claims", []):
        prop = claim["mainsnak"]["property"]
        value = _extract_comparable_value(claim)
        if value is None:
            continue
        expected.setdefault(prop, []).append(value)
    return expected


# Result counters that the SDC dispatcher bumps when ``_submit_per_item_edit``
# actually POSTs a change. The "did this ordinal write anything?" check
# in partner mode and ``_safe_process_one`` snapshots the sum of these
# before vs. after ``process_one``; centralising the tuple here keeps
# any future SDC write counter from silently slipping past the
# snapshot delta (and the ``SDC_PAGES_EDITED`` accounting that hangs
# off it).
_SDC_WRITE_COUNTERS = (
    Result.SDC_CLAIMS_ADDED,
    Result.SDC_REFS_ADDED,
    Result.SDC_REMOVALS,
    # Qualifier-only edits are real ``wbeditentity`` writes that
    # otherwise wouldn't bump any of the above. Include here so the
    # delta-detection feeding ``SDC_PAGES_EDITED`` doesn't miss them.
    Result.SDC_QUALIFIER_UPDATES,
)


def _sdc_writes_total() -> int:
    """Sum of the tracker counters that ``_submit_per_item_edit``
    increments per write. Used to detect whether a given
    ``process_one`` call actually committed anything on Commons.
    """
    return sum(tracker.count(c) for c in _SDC_WRITE_COUNTERS)


def _submit_sdc_write(action, mediaid, dpla_id, **params):
    """Submit an SDC write (wbeditentity or wbremoveclaims) through
    pywikibot's ``simple_request``.

    Shared by the three write paths (``_post_new_refs``,
    ``_post_new_claims``, removals in ``_reconcile_existing_claims``)
    so they all get pywikibot's built-in behaviours for free:

    - automatic CSRF token management (``site.tokens["csrf"]``);
    - ``maxlag`` honoring (sleep ``lag + 1``, retry; default
      ``Site.maxlag`` is 5s);
    - ``Retry-After`` header honoring on 429/503;
    - exponential backoff on ``internal_api_error_*`` / ``srvtimeout``;
    - auto-relogin on ``badtoken``;
    - structured ``APIError(code=..., info=...)`` instead of JSON
      response inspection.

    Wraps the call in :func:`with_csrf_recovery` so a stale
    ``TokenWallet`` (``KeyError: Invalid token 'csrf'``) triggers a
    session refresh (``logout`` + ``login`` + ``tokens.clear``) and
    a retry rather than bubbling up as "SDC sync failed; skipping
    ordinal" for every subsequent write in the run. See PR #350 for
    the analogous uploader fix and the Toledo Lucas 2026-06-25
    incident that surfaced the same weakness here (68,411 identical
    CSRF errors bucketed as per-ordinal skips over ~5.5 days).

    Raises :class:`_MissingEntityError` when Commons returns
    ``no-such-entity`` (the entity doesn't exist; not the SDC phase's
    problem — see PR #267). Other ``APIError`` codes propagate as a
    ``RuntimeError`` carrying the code + info, which the per-ordinal
    handler catches and treats as an ``SDC_ORDINALS_SKIPPED_ERROR``.
    Unrecoverable CSRF failures propagate as
    :class:`CsrfRecoveryFailed` — routed AROUND the per-ordinal
    generic catch so the whole run aborts (mirrors uploader
    behaviour).

    ``params`` is the per-action payload — for wbeditentity, the
    serialized ``data``; for wbremoveclaims, the pipe-joined ``claim``
    string. ``bot=True``, the CSRF token, and ``id=mediaid`` are
    injected here so call sites stay focused on the action-specific
    differences.
    """

    def _do_write():
        try:
            site.simple_request(
                action=action,
                id=mediaid,
                bot=True,
                token=site.tokens["csrf"],
                **params,
            ).submit()
        except pywikibot.exceptions.APIError as e:
            _raise_if_missing_entity(e, mediaid)
            raise RuntimeError(
                f"{action} failed for {mediaid} ({dpla_id}):"
                f" {e.code} — {_truncate(getattr(e, 'info', ''))}"
            ) from e

    with_csrf_recovery(site, f"{action} {mediaid} ({dpla_id})", _do_write)


def _snak_content_key(snak):
    """Stable content key for a snak, ignoring the volatile ``hash`` key
    Wikibase assigns. Two snaks with the same property + value compare equal
    regardless of whether one carries a server-assigned hash."""
    return json.dumps({k: v for k, v in snak.items() if k != "hash"}, sort_keys=True)


def _reference_content_key(reference):
    """Stable content key for a reference group, keyed on its snak set and
    ignoring the volatile per-reference ``hash`` / ``snaks-order`` keys. Two
    references carrying the same snaks (e.g. the same P854+P123+P813 triple)
    compare equal even if only one has a server hash."""
    return json.dumps(
        {
            prop: sorted(_snak_content_key(s) for s in slist)
            for prop, slist in (reference.get("snaks") or {}).items()
        },
        sort_keys=True,
    )


def _merge_fragment_group(frags, removed_qualifier_hashes=frozenset()):
    """Merge several non-removal claim fragments that target the same
    statement id into one, unioning their qualifiers and references.

    mainsnak/rank are taken from the first fragment that carries a mainsnak
    (the real builders always do); qualifier snaks and reference groups are
    deduplicated by content (ignoring server-assigned hashes) so a value
    present in more than one fragment lands exactly once.

    ``removed_qualifier_hashes`` is the set of existing-qualifier snak hashes
    this file's edit intends to *delete* (from the ``qualifier_removals``
    accumulator). A qualifier-update fragment has already excluded them, but
    a different colliding fragment could still carry the stale snak; skipping
    any snak whose hash is in this set keeps the union from resurrecting a
    qualifier the edit means to remove.
    """
    merged = {"id": frags[0]["id"], "type": "statement"}
    for frag in frags:
        if "mainsnak" in frag:
            merged["mainsnak"] = copy.deepcopy(frag["mainsnak"])
            merged["rank"] = frag.get("rank", "normal")
            break

    quals, qual_keys = {}, set()
    refs, ref_keys = [], set()
    for frag in frags:
        for prop, slist in (frag.get("qualifiers") or {}).items():
            for snak in slist:
                if snak.get("hash") in removed_qualifier_hashes:
                    continue
                key = (prop, _snak_content_key(snak))
                if key in qual_keys:
                    continue
                qual_keys.add(key)
                quals.setdefault(prop, []).append(copy.deepcopy(snak))
        for ref in frag.get("references") or []:
            key = _reference_content_key(ref)
            if key in ref_keys:
                continue
            ref_keys.add(key)
            cleaned = copy.deepcopy(ref)
            cleaned.pop("snaks-order", None)
            refs.append(cleaned)
    if quals:
        merged["qualifiers"] = quals
    if refs:
        merged["references"] = refs
    return merged


def _coalesce_same_id_fragments(fragments, removed_qualifier_hashes_by_id=None):
    """Fold non-removal claim fragments that target the same statement id
    into a single fragment, unioning their qualifiers and references.

    ``wbeditentity`` applies every id-bearing claim entry in ``data.claims``
    as a *wholesale replacement* of that statement, in array order — so two
    entries for one id silently clobber each other: the later one's
    (possibly empty or stale) qualifier/reference sets erase whatever the
    earlier one set. That is a silent-data-loss footgun whenever two
    independently-built fragments (e.g. an add_ref reference rewrite and an
    add_det qualifier amend) land on the same claim in one edit.

    Only ids that actually appear more than once are merged; every other
    fragment — new-claim creates (no ``id``), removals, and singleton
    id-bearing fragments — is passed through untouched, in its original
    position. So a collision-free bundle (the overwhelming common case) is
    returned byte-for-byte unchanged.
    """
    removed_by_id = removed_qualifier_hashes_by_id or {}
    groups = {}
    for frag in fragments:
        fid = frag.get("id")
        if fid and frag.get("remove") != "":
            groups.setdefault(fid, []).append(frag)
    collided = {fid for fid, group in groups.items() if len(group) > 1}
    if not collided:
        return fragments

    # Reaching here means two fragments targeted one statement id — the
    # upstream guards (check()'s ref-stamp/qualifier dedup, _build_p813_
    # refresh_fragments' touched_ids) are expected to prevent that. Merging
    # resolves it safely, but log it so a regression in those guards is
    # visible rather than silently absorbed.
    logging.warning(
        " -- Coalesced %d statement id(s) with multiple fragments in one"
        " edit (%s); merging qualifiers + references to avoid silent"
        " wbeditentity clobber.",
        len(collided),
        ", ".join(sorted(collided)),
    )

    out, emitted = [], set()
    for frag in fragments:
        fid = frag.get("id")
        if fid in collided and frag.get("remove") != "":
            if fid in emitted:
                continue  # later occurrences folded into the first
            emitted.add(fid)
            out.append(
                _merge_fragment_group(groups[fid], removed_by_id.get(fid, frozenset()))
            )
        else:
            out.append(frag)
    return out


def _submit_per_item_edit(
    mediaid,
    dpla_id,
    summary,
    *,
    new_claims=(),
    reference_updates=(),
    qualifier_updates=(),
    removals=(),
):
    """Submit one ``wbeditentity`` per file with all per-file edits bundled.

    The consolidated dispatcher behind the partner-mode and legacy file-
    processing paths. Replaces the previous pattern of issuing several
    separate API calls per file (separate ``wbeditentity`` for new
    claims, ``wbeditentity`` for reference updates, ``wbsetqualifier``
    per qualifier amend, ``wbremovequalifiers`` for stale-qualifier
    cleanup, ``wbremoveclaims`` for reconciler removals) with one
    atomic ``wbeditentity`` carrying every change.

    Each fragment is a Wikibase claim-data dict; the kind is inferred
    from its shape per ``data.claims[]`` semantics on `wbeditentity`:

    * ``new_claims`` — full claim dicts with no ``id`` field. Wikibase
      creates a fresh statement for each, assigning a new statement ID.
    * ``reference_updates`` — claim dicts with ``id`` + ``references``.
      Wikibase updates only the references of the named statement; the
      mainsnak and qualifiers are left intact.
    * ``qualifier_updates`` — claim dicts with ``id`` + ``qualifiers``.
      Wikibase updates only the qualifiers of the named statement; the
      mainsnak and references are left intact. Qualifier values are
      provided as a wholesale set, so callers must merge new qualifier
      snaks with the existing qualifier set (preserving snak hashes)
      before passing them in — otherwise the existing qualifiers would
      be erased.
    * ``removals`` — claim dicts shaped ``{"id": ..., "remove": ""}``.
      Wikibase deletes the named statement.

    Wikibase requires ``"type": "statement"`` on every non-removal claim
    entry; the dispatcher stamps it on any fragment that's missing it
    before POSTing, so callers may omit the field. (The two in-tree
    builders set it explicitly for local readability, but the guard
    here protects future builders and ad-hoc callers from re-introducing
    the bundle-wide ``invalid-claim: Type is missing`` rejection.)

    Atomicity: the entire bundle lands as a single revision on the
    file's MediaInfo entity, or none of it does. There is no partial-
    update window where new claims have been written but removals
    haven't — the previous multi-POST pattern allowed exactly that
    failure mode and could leak orphaned stale statements when an
    intermediate POST failed.

    The dispatcher tracker-counts ``new_claims`` under
    ``SDC_CLAIMS_ADDED``, ``reference_updates`` under
    ``SDC_REFS_ADDED``, ``removals`` under ``SDC_REMOVALS``, and
    ``qualifier_updates`` under ``SDC_QUALIFIER_UPDATES``. The
    qualifier counter is not surfaced in the Slack summary — it
    exists so the ``SDC_PAGES_EDITED`` write-delta detection picks
    up the rare qualifier-only commit (every DPLA claim on the file
    already carries today's ``P813``, so the opportunistic refresh
    adds no reference fragments).

    No-op when every fragment list is empty.
    """
    all_fragments = (
        list(new_claims)
        + list(reference_updates)
        + list(qualifier_updates)
        + list(removals)
    )
    if not all_fragments:
        return

    # Defense-in-depth: stamp ``type: "statement"`` on every non-removal
    # fragment that's missing it. Wikibase rejects the entire bundle
    # with ``invalid-claim: Type is missing`` if any non-removal entry
    # lacks this field, and atomicity means one malformed fragment
    # silently drops every other edit on the file. The two known
    # builders (``_build_qualifier_update_fragments`` /
    # ``_build_reference_refresh_fragments``) set it themselves, but adding
    # the guard here makes any future builder that forgets — or any
    # external caller passing hand-built fragments — fail closed
    # instead of nuking the whole edit. Removals (``{"id": ...,
    # "remove": ""}``) are exempt: Wikibase accepts them without
    # ``type``, and adding it has no effect.
    for fragment in all_fragments:
        if fragment.get("remove") == "":
            continue
        fragment.setdefault("type", "statement")

    # Coalesce fragments that target the same statement id. wbeditentity
    # treats each id-bearing claim entry as a wholesale replacement, so two
    # entries for one id silently clobber — the later one's qualifiers/
    # references erase the earlier's. Merging them into one fragment makes
    # the combined edit carry every qualifier and reference exactly once,
    # regardless of which builders contributed them. The removed-qualifier
    # hash map (from this file's ``qualifier_removals``) is passed so the
    # union can't resurrect a snak the edit intends to delete.
    removed_qualifier_hashes_by_id = {}
    for claimid, snak_hash in qualifier_removals:
        removed_qualifier_hashes_by_id.setdefault(claimid, set()).add(snak_hash)
    all_fragments = _coalesce_same_id_fragments(
        all_fragments, removed_qualifier_hashes_by_id
    )

    _submit_sdc_write(
        "wbeditentity",
        mediaid,
        dpla_id,
        data=json.dumps({"claims": all_fragments}),
        summary=summary,
    )

    if new_claims:
        tracker.increment(Result.SDC_CLAIMS_ADDED, len(new_claims))
    if reference_updates:
        tracker.increment(Result.SDC_REFS_ADDED, len(reference_updates))
    if removals:
        tracker.increment(Result.SDC_REMOVALS, len(removals))
    if qualifier_updates:
        tracker.increment(Result.SDC_QUALIFIER_UPDATES, len(qualifier_updates))


def _build_qualifier_update_fragments(mediaid):
    """Fold the per-file ``qualifier_amends`` and ``qualifier_removals``
    accumulators into a list of wbeditentity claim fragments — one per
    target statement ID — each carrying the wholesale-replace qualifier
    set computed from the cached entity's existing qualifiers plus the
    queued adds, minus the queued removals.

    The dispatcher's qualifier-update kind expects ``{id, qualifiers}``
    where the qualifier set is sent in full (it replaces the existing
    set wholesale on save). Existing snak ``hash`` values are preserved
    so Wikibase recognises unchanged snaks and surfaces only the new
    ones in the per-file edit diff.

    Returns the empty list when neither accumulator has entries.
    """
    if not qualifier_amends and not qualifier_removals:
        return []
    adds_by_id = {}
    for claimid, prop, snak in qualifier_amends:
        adds_by_id.setdefault(claimid, []).append((prop, snak))
    removes_by_id = {}
    for claimid, snak_hash in qualifier_removals:
        removes_by_id.setdefault(claimid, set()).add(snak_hash)
    target_ids = set(adds_by_id) | set(removes_by_id)

    entity = get_entity(mediaid)
    statements_by_id = {}
    for prop_stmts in (entity.get("statements") or {}).values():
        for stmt in prop_stmts:
            stmt_id = stmt.get("id")
            if stmt_id:
                statements_by_id[stmt_id] = stmt

    fragments = []
    for claimid in target_ids:
        stmt = statements_by_id.get(claimid)
        if stmt is None:
            # Statement vanished between the cached read and now —
            # rare; nothing safe to do, skip rather than crash.
            continue
        existing_qualifiers = stmt.get("qualifiers") or {}
        # Drop stale snaks first; merge new ones afterward.
        kept = _exclude_qualifier_snaks(
            existing_qualifiers, removes_by_id.get(claimid, set())
        )
        merged = _merge_qualifier_snaks(kept, adds_by_id.get(claimid, []))
        # wbeditentity treats every non-removal claim entry as a
        # wholesale-replace operation, so the fragment must carry the
        # entire statement — ``mainsnak``, ``rank``, existing
        # ``references`` — not just the field being amended. Omitting
        # ``mainsnak`` fails the bundle with ``invalid-claim:
        # Attribute "mainsnak" is missing``; omitting ``references``
        # would silently erase them. Copy from the cached statement
        # and overlay the new qualifier set; ``type`` is stamped by
        # the dispatcher as a defense-in-depth.
        fragment = {
            "id": claimid,
            "type": "statement",
            "mainsnak": copy.deepcopy(stmt["mainsnak"]),
            "rank": stmt.get("rank", "normal"),
            "qualifiers": merged,
            "references": copy.deepcopy(stmt.get("references") or []),
        }
        fragments.append(fragment)
    return fragments


def _dpla_item_url(dpla_id):
    """The dp.la item URL used as the P854 (reference URL) value in a DPLA
    reference. Single source so the value the writers stamp and the value
    ``_dpla_reference_is_canonical`` compares against can't drift apart."""
    return f"https://dp.la/item/{dpla_id}"


def _string_snak(prop, value):
    """A value-typed string snak in wbeditentity wire shape."""
    return {
        "snaktype": "value",
        "property": prop,
        "datavalue": {"type": "string", "value": value},
    }


def _entity_snak(prop, qid):
    """A value-typed wikibase-item snak; ``numeric-id`` is derived from the
    QID so the numeric and string forms can't fall out of sync."""
    return {
        "snaktype": "value",
        "property": prop,
        "datavalue": {
            "type": "wikibase-entityid",
            "value": {"entity-type": "item", "numeric-id": int(qid[1:]), "id": qid},
        },
    }


def _build_dpla_reference(dpla_id, ingest_date):
    """The canonical DPLA reference as a wbeditentity reference dict: the
    3-snak group ``P854`` (this item's dp.la URL) + ``P123`` (publisher =
    DPLA, Q2944483) + ``P813`` (retrieved date, pinned to the item's DPLA
    ``ingestDate``). Same shape ``formattedclaim`` stamps on new claims
    (P813 via the shared ``_build_p813_snak``), so a rebuilt reference is
    indistinguishable from a freshly authored one.

    See ``ingest_wikimedia.sdc.ingest_date_from_doc`` for why P813 is
    pinned to the ingest date rather than today."""
    return {
        "snaks": {
            "P854": [_string_snak("P854", _dpla_item_url(dpla_id))],
            "P123": [_entity_snak("P123", "Q2944483")],
            "P813": [_build_p813_snak(ingest_date)],
        }
    }


def _dpla_reference_is_canonical(reference, dpla_id, ingest_date):
    """True iff ``reference`` is already the complete, current DPLA reference
    — publisher = DPLA (P123), this item's dp.la URL (P854), and the P813
    retrieved date matching this item's DPLA ``ingestDate``. A DPLA
    reference missing or wrong on any of these is treated as needing a
    rebuild, so the refresh repairs a partial reference (e.g. a foreign-bot
    rights claim that only ever carried P123) in the same write — no
    separate reconcile pass.

    Identity keys on the P123 publisher marker (via ``_is_dpla_reference``);
    a reference that lost P123 entirely reads as foreign and is left
    untouched here — a deliberate known limitation, since "repairing" it
    would risk overwriting a genuinely third-party reference."""
    if not _is_dpla_reference(reference):
        return False
    expected_url = _dpla_item_url(dpla_id)
    snaks = reference.get("snaks") or {}
    p854_ok = any(
        (snak.get("datavalue") or {}).get("value") == expected_url
        for snak in snaks.get("P854") or []
    )
    return p854_ok and _p813_matches(reference, ingest_date)


def _build_reference_refresh_fragments(
    mediaid, dpla_id, already_touched_ids, ingest_date
):
    """Build reference-update fragments that make each DPLA-authored claim's
    DPLA reference *canonical and up-to-date* — the full P854+P123+P813
    triple with P813 pinned to the item's DPLA ``ingestDate`` — rewriting
    it wholesale rather than only swapping the P813 date.

    This both refreshes the retrieved date to match the current
    ``ingestDate`` AND repairs a partial or stale DPLA reference (e.g. one
    a foreign bot wrote with P123 but no P854) in a single write —
    relying on the fact that re-asserting an identical reference is a
    Wikibase no-op, so the only claims that actually change are those
    whose DPLA reference isn't already canonical. User-added (non-DPLA)
    references are preserved verbatim; a duplicate second DPLA reference
    (rare legacy state) is collapsed to the single canonical one.

    Only fires for claims NOT already covered by another fragment in this
    file's edit (``already_touched_ids``), and only when the dispatcher is
    already making some other edit on the file (the call site builds these
    only when the bundle is otherwise non-empty) — so a file where every DPLA
    reference is already canonical gets no spurious edit. Pinning P813 to
    ``ingestDate`` (rather than today) means back-to-back sync runs against
    unchanged partner data produce no reference-refresh churn at all.
    """
    entity = get_entity(mediaid)
    fragments = []
    for prop_stmts in (entity.get("statements") or {}).values():
        for stmt in prop_stmts:
            stmt_id = stmt.get("id")
            if not stmt_id or stmt_id in already_touched_ids:
                continue
            existing_refs = stmt.get("references") or []
            if not existing_refs:
                continue
            # Rebuild the (first) DPLA reference to canonical; preserve every
            # user-added reference verbatim and at its position; drop any
            # extra duplicate DPLA references.
            new_refs = []
            changed = False
            seen_dpla = False
            for ref in existing_refs:
                if not _is_dpla_reference(ref):
                    new_refs.append(ref)
                    continue
                if seen_dpla:
                    changed = True  # duplicate DPLA reference — drop it
                    continue
                seen_dpla = True
                if _dpla_reference_is_canonical(ref, dpla_id, ingest_date):
                    new_refs.append(ref)
                else:
                    new_refs.append(_build_dpla_reference(dpla_id, ingest_date))
                    changed = True
            if changed:
                # Same wholesale-replace contract as
                # _build_qualifier_update_fragments: include the statement's
                # existing mainsnak / qualifiers / rank so wbeditentity
                # preserves them and only diffs the references field.
                fragments.append(
                    {
                        "id": stmt_id,
                        "type": "statement",
                        "mainsnak": copy.deepcopy(stmt["mainsnak"]),
                        "rank": stmt.get("rank", "normal"),
                        "qualifiers": copy.deepcopy(stmt.get("qualifiers") or {}),
                        "references": new_refs,
                    }
                )
    return fragments


def _build_p813_snak(retrieval_date):
    """Build the P813 (retrieved on) snak for the standard DPLA
    reference shape — calendarmodel Gregorian, precision day."""
    return {
        "snaktype": "value",
        "property": "P813",
        "datavalue": {
            "value": {
                "time": "+" + retrieval_date.isoformat() + "T00:00:00Z",
                "timezone": 0,
                "before": 0,
                "after": 0,
                "precision": 11,
                "calendarmodel": "http://www.wikidata.org/entity/Q1985727",
            },
            "type": "time",
        },
    }


def _p813_matches(reference, target_date):
    """Return True iff the reference's P813 (retrieved on) snak already
    carries ``target_date`` in its time value. Used by the reference-refresh
    path to skip references that are already up-to-date with the item's
    DPLA ``ingestDate`` — the whole point of pinning P813 to the ingest
    date is that a re-sync of unchanged partner data produces no diff.
    """
    target_iso = "+" + target_date.isoformat() + "T00:00:00Z"
    for snak in (reference.get("snaks") or {}).get("P813") or []:
        try:
            if snak["datavalue"]["value"]["time"] == target_iso:
                return True
        except (KeyError, TypeError):
            continue
    return False


def _flush_per_file_edits(mediaid, dpla_id):
    """Drain every per-file accumulator into a single ``wbeditentity``
    POST via :func:`_submit_per_item_edit`. Called at the end of each
    ``process_one_from_sdc`` and ``process_one`` invocation; this is
    the one POST per file that replaces the previous five-to-seven
    separate API calls.

    When any other edit fragments are present, opportunistically make
    every other DPLA-authored claim's DPLA reference canonical and
    up-to-date — the full P854+P123+P813 triple with P813 pinned to
    the item's DPLA ``ingestDate`` — via
    ``_build_reference_refresh_fragments``. This both refreshes the
    "retrieved on" date (against the current ingest date, not today)
    AND repairs a partial or stale DPLA reference left by an older
    sync or a foreign bot. Re-asserting an already-canonical reference
    is a Wikibase no-op, so there are no spurious edits when the file
    has nothing else to change.

    The ingest date is read from ``_current_ingest_date``, which the
    entry point set from the item's ``ingestDate``. See
    ``ingest_wikimedia.sdc.ingest_date_from_doc`` for rationale.

    After a successful write, invalidate the cached entity once so any
    follow-up code reading from cache picks up the new revision.
    """
    qualifier_fragments = _build_qualifier_update_fragments(mediaid)
    removal_fragments = [{"id": cid, "remove": ""} for cid in removals]
    reference_updates = list(refclaims["claims"])

    has_any_other_edit = bool(
        claims["claims"]
        or reference_updates
        or qualifier_fragments
        or removal_fragments
    )
    if has_any_other_edit:
        # Only need the ingest date when we're actually building a
        # reference-refresh fragment. This keeps the "nothing to do"
        # path callable without a set ingest date (used by tests and by
        # process_one entry points that short-circuit before setting it).
        ingest_date = _require_ingest_date()
        touched_ids = set()
        for frag in qualifier_fragments + removal_fragments + reference_updates:
            fid = frag.get("id")
            if fid:
                touched_ids.add(fid)
        reference_updates.extend(
            _build_reference_refresh_fragments(
                mediaid, dpla_id, touched_ids, ingest_date
            )
        )

    _submit_per_item_edit(
        mediaid,
        dpla_id,
        summary=(
            f"Updating structured data claims from [[COM:DPLA|DPLA]] item"
            f" '[[dpla:{dpla_id}|{dpla_id}]]'."
            f" [[COM:DPLA/MOD|Leave feedback]]!"
        ),
        new_claims=claims["claims"],
        reference_updates=reference_updates,
        qualifier_updates=qualifier_fragments,
        removals=removal_fragments,
    )
    # One terminal invalidation regardless of how many fragments landed
    # — the file's MediaInfo revision now reflects the combined edit.
    invalidate_entity(mediaid)


def dpla_claims(
    mediaid,
    dpla_id,
    url,
    descs,
    dates,
    titles,
    hub,
    local_ids,
    institution,
    rs,
    creators,
    subjects,
    naids,
    access,
    level,
):
    """Legacy entry point: build `expected` from the 13-tuple, then reconcile."""
    expected = _build_expected_from_parsed(
        dpla_id,
        url,
        descs,
        dates,
        titles,
        hub,
        local_ids,
        institution,
        rs,
        creators,
        subjects,
        naids,
        access,
        level,
    )
    # Protect chunkable properties from the legacy reconciler: their
    # expected values may exist on Commons as a multi-claim chunked
    # series (P1545="A1", "A2", ...) that the legacy expected-builder
    # doesn't model. Without this, a `--file`/`--cat`/`--list` rerun
    # against a partner-mode-migrated file would queue every chunked
    # statement for removal. See _reconcile_existing_claims docstring.
    _reconcile_existing_claims(
        mediaid, dpla_id, expected, protected_props=CHUNKABLE_PROPS
    )
    _reconcile_inferred_from_wikitext_dupes(mediaid)


def _build_expected_from_parsed(
    dpla_id,
    url,
    descs,
    dates,
    titles,
    hub,
    local_ids,
    institution,
    rs,
    creators,
    subjects,
    naids,
    access,
    level,
):
    """Build `{prop: [comparable_values]}` from the parse_dpla_doc tuple.

    The legacy partner-API path (parsed() → process_one → dpla_claims) uses
    this. The new partner mode (PR 4) reads sdc.json from S3 and uses
    `_build_expected_from_sdc` instead — same shape, different source.

    P571 entries: include BOTH the raw DPLA display-date string AND
    the canonical time-comparable key the partner-mode path produces
    when ``parse_dpla_date`` succeeds. This way a legacy ``--file`` /
    ``--cat`` / ``--list`` rerun against a file that partner mode has
    already migrated from ``somevalue+P1932`` to value-typed time
    won't see the migrated claim's comparable as ``unexpected`` and
    queue it for removal. Both old-shape and new-shape Commons claims
    representing the same date are protected.
    """
    rightsprop = "P6216"
    rightsvalue = ""
    statusvalue = ""
    rs_key = _normalize_rights_uri(rs)
    rights_entry = rights.get(rs_key)
    if rights_entry:
        rightsprop = list(rights_entry)[0]
        rightsvalue = rights_entry[rightsprop]

        if rightsprop == "P275":
            statusvalue = "Q50423863"

        if rightsprop == "P6426":
            statusvalue = "Q19652"

        if rightsvalue == "Q6938433":
            statusvalue = "Q88088423"

    if rs_key == "http://creativecommons.org/publicdomain/mark/1.0":
        statusvalue = "Q19652"

    parsesubjects = []
    parsetitles = []
    parsecreators = []
    parsedescs = []
    parsesubjectentities = []
    for subject in subjects:
        parsesubjects.append(subject[0][:1499].rstrip())
        if subject[1]:
            parsesubjectentities.append(subject[1][:1499].rstrip())
    for title in titles:
        parsetitles.append(title[:1499].rstrip())
    for creator in creators:
        parsecreators.append(creator[:1499].rstrip())
    for desc in descs:
        parsedescs.append(desc[:1499].rstrip())
    titles = parsetitles
    creators = parsecreators
    descs = parsedescs
    subjects = parsesubjects
    # All values are lists so the `value not in expected[prop]` reconciliation
    # below behaves consistently (a bare string would degrade `not in` into a
    # substring check and let real DPLA claims look "unexpected").
    # P571: include both the raw DPLA strings (for OLD-shape
    # somevalue+P1932 claims) AND the canonical time-comparable keys
    # for the parseable subset (for NEW-shape value-typed claims).
    # See docstring above — legacy reruns must protect both shapes.
    p571_expected = list(dates)
    for date in dates:
        parsed = parse_dpla_date(date)
        if parsed is None:
            continue
        base = f"{parsed['value']['time']}|P{parsed['value']['precision']}"
        if parsed["approximate"]:
            base = f"{base}|circa"
        p571_expected.append(base)

    # Chunkable-prop values (P760, P217, P1476, P4272, P10358, P1225)
    # are intentionally still plain strings here even though
    # _reconcile_existing_claims extracts them from Commons as
    # (value, p1545) tuples — the legacy path passes
    # ``protected_props=CHUNKABLE_PROPS`` to skip reconciliation for
    # these properties entirely. The legacy expected-builder doesn't
    # know about chunk shape and partial-matching (value, None) versus
    # (chunk_text, "A1") would still wrongly queue partner-mode chunked
    # statements for removal. Skipping reconciliation is safer than
    # half-matching; full parity is a follow-up to this PR.
    expected = {
        "P217": local_ids,
        "P760": [dpla_id],
        "P1476": titles,
        "P195": ["Q518155" if hub == "Q518155" else institution],
        "P170": creators,
        "P9126": ["Q2944483", hub, institution],
        "P7482": [url],
        "P4272": subjects,
        "P571": p571_expected,
        "P10358": descs,
        "P1225": naids,
        "P6224": [level],
        "P7228": [access],
        "P921": parsesubjectentities,
    }
    # P6216 (copyright status) and the rights-property (P275/P6426) need to
    # coexist: when rightsprop *is* P6216 (public-domain-mark path), a single
    # P6216 entry overwrote the other and caused dpla_claims() to wrongly
    # queue the correct status claim for removal. Build them separately.
    p6216_values = []
    if statusvalue:
        p6216_values.append(statusvalue)
    if rightsprop == "P6216":
        if rightsvalue:
            p6216_values.append(rightsvalue)
    else:
        if rightsvalue:
            expected[rightsprop] = [rightsvalue]
    expected["P6216"] = p6216_values
    return expected


def _reconcile_existing_claims(mediaid, dpla_id, expected, protected_props=frozenset()):
    """Walk DPLA-referenced claims on Commons; push the IDs of any
    claims that should be removed (DPLA-authored but no longer
    expected) onto the module-level ``removals`` accumulator. The
    per-file dispatcher flushes them in the combined wbeditentity.

    Shared by ``dpla_claims`` (legacy partner-API path) and
    ``process_one_from_sdc`` (partner-mode path). Same removal logic;
    just different sources for ``expected``.

    ``protected_props`` is a set of property IDs whose existing
    DPLA-authored claims are NOT subject to reconciliation — they're
    left in place regardless of whether they appear in ``expected``.
    The legacy partner-API path uses this for ``CHUNKABLE_PROPS``
    (string/monolingualtext properties whose values may have been
    chunked into multi-statement series with P1545 ordinals by
    partner mode). Without protection, a legacy ``--file``/``--cat``/
    ``--list`` rerun would treat partner-mode chunked claims as
    unexpected — the legacy ``expected`` is keyed on un-chunked
    ``(value, None)`` tuples while Commons-side chunked statements
    extract as ``(chunk_text, "A1")``, ``(chunk_text, "A2")``... — and
    queue them all for removal. For long P217 values, ``process_one``
    additionally skips re-adding them, so the identifier would
    disappear entirely. Protecting the chunkable-prop set in legacy
    reconciliation keeps maintenance-mode reruns safe until full
    chunking parity is added to the legacy builders (out of scope
    here).

    No POST happens here — pushes onto the ``removals`` accumulator
    instead. The dispatcher flushes them via the combined
    ``wbeditentity`` payload using ``{"id": ..., "remove": ""}`` claim
    entries.
    """
    # Use pywikibot's wbgetentities (via get_entity) rather than a direct
    # requests.get to Special:EntityData: Wikimedia rejects the default
    # python-requests UA with HTTP 403 (phab T400119), and pywikibot's
    # Site.simple_request sets a compliant UA plus maxlag/CSRF handling.
    # Read from the cache populated at the file-boundary entry; no
    # mid-flow writes have happened in the consolidated dispatcher.
    print(f" -- Accessing Commons ID {mediaid}")
    entity = get_entity(mediaid)
    print(f" -- Accessed Commons ID {mediaid}")
    statements = entity.get("statements") or {}
    dpla_claim_list = []
    for prop in statements:
        for stmt in statements[prop]:
            if stmt.get("references"):
                if any(pubprop["snaks"].get("P123") for pubprop in stmt["references"]):
                    if any(
                        pubcheck["snaks"]["P123"][0]["datavalue"]["value"]["id"]
                        == "Q2944483"
                        for pubcheck in stmt["references"]
                        if pubcheck["snaks"].get("P123")
                    ):
                        if stmt["mainsnak"]["snaktype"] == "value":
                            dtype = stmt["mainsnak"]["datavalue"]["type"]
                            if stmt["mainsnak"]["property"] == "P7482":
                                try:
                                    dpla_claim_list.append(
                                        {
                                            stmt["mainsnak"]["property"]: {
                                                "id": stmt["id"],
                                                "value": stmt["qualifiers"]["P973"][0][
                                                    "datavalue"
                                                ]["value"],
                                            }
                                        }
                                    )
                                except (KeyError, IndexError, TypeError):
                                    # P7482 statement without a P973 qualifier — skip
                                    # it from reconciliation, but log the statement ID
                                    # so operators can spot malformed DPLA-authored
                                    # claims rather than have them vanish silently.
                                    logging.debug(
                                        "P7482 statement %s missing P973 qualifier; "
                                        "skipping reconciliation",
                                        stmt.get("id"),
                                    )
                            elif dtype == "wikibase-entityid":
                                dpla_claim_list.append(
                                    {
                                        stmt["mainsnak"]["property"]: {
                                            "id": stmt["id"],
                                            "value": stmt["mainsnak"]["datavalue"][
                                                "value"
                                            ]["id"],
                                        }
                                    }
                                )
                            elif dtype == "string":
                                # Tuple key — mirrors _extract_comparable_value
                                # so the reconciler diffs the chunk identity
                                # (text + P1545) rather than just the text.
                                # Without this, a chunked statement on Commons
                                # whose chunk text appears in expected[prop]
                                # but at a different P1545 ordinal would
                                # incorrectly survive reconciliation.
                                dpla_claim_list.append(
                                    {
                                        stmt["mainsnak"]["property"]: {
                                            "id": stmt["id"],
                                            "value": (
                                                stmt["mainsnak"]["datavalue"]["value"],
                                                _extract_p1545_value(stmt),
                                            ),
                                        }
                                    }
                                )
                            elif dtype == "monolingualtext":
                                dpla_claim_list.append(
                                    {
                                        stmt["mainsnak"]["property"]: {
                                            "id": stmt["id"],
                                            "value": (
                                                stmt["mainsnak"]["datavalue"]["value"][
                                                    "text"
                                                ],
                                                _extract_p1545_value(stmt),
                                            ),
                                        }
                                    }
                                )
                            elif dtype == "time":
                                # Same canonical key as
                                # `_extract_comparable_value` produces on
                                # the sdc.json side. Without this branch,
                                # a value-typed P571 on Commons would be
                                # silently ignored by the reconciler
                                # (left in dpla_claim_list as a missing
                                # entry), and a sdc.json that no longer
                                # includes that date would never trigger
                                # the corresponding removal.
                                try:
                                    comparable = _time_claim_comparable(stmt)
                                except (KeyError, TypeError):
                                    # Malformed Commons time datavalue
                                    # (missing "time" or "precision"); queue
                                    # the statement for removal — mirrors
                                    # the somevalue-missing-qualifier
                                    # branch below.
                                    removals.append(stmt["id"])
                                else:
                                    dpla_claim_list.append(
                                        {
                                            stmt["mainsnak"]["property"]: {
                                                "id": stmt["id"],
                                                "value": comparable,
                                            }
                                        }
                                    )
                        if stmt["mainsnak"]["snaktype"] == "somevalue":
                            p = "P1932" if prop == "P571" else "P2093"
                            try:
                                dpla_claim_list.append(
                                    {
                                        stmt["mainsnak"]["property"]: {
                                            "id": stmt["id"],
                                            "value": stmt["qualifiers"][p][0][
                                                "datavalue"
                                            ]["value"],
                                        }
                                    }
                                )
                            except (KeyError, IndexError, TypeError):
                                # somevalue claim missing its expected
                                # stated-as qualifier — queue for removal.
                                removals.append(stmt["id"])
    for claim in dpla_claim_list:
        for prop in claim:
            if prop in protected_props:
                # Caller has declared this property off-limits for
                # removal — typically chunkable properties on the
                # legacy path where the legacy expected-builder
                # doesn't know about chunk shape. Skip the diff.
                continue
            if prop not in expected:
                removals.append(claim[prop]["id"])
            elif claim[prop]["value"] not in expected[prop]:
                removals.append(claim[prop]["id"])


def _statement_comparable_value(stmt):
    """Comparable key for ``stmt`` that is stable across equivalent
    encoding shapes — value-typed time vs. somevalue+P1932 stated-as
    parsing to the same structured date, range-shaped P1932 across the
    "1934 - 1948" / "between 1934 and 1948" / raw-wikitext variants,
    and the simple value-typed shapes (string, monolingualtext, item).

    Every return is tagged with a shape discriminator so cross-type
    values (e.g. a P571 time and a P217 string of digits) can never
    accidentally compare equal.

    Returns ``None`` for shapes the dedup helper can't reason about
    (malformed datavalues, novalue snaks, somevalue claims with no
    P1932 qualifier). Callers treat ``None`` as "skip this statement"
    rather than as a match.

    Used by :func:`_reconcile_inferred_from_wikitext_dupes` to decide
    whether an inferred-from-Wikitext claim should be removed because
    an equivalent DPLA-authored statement now covers the same fact.

    Every non-None return is a 3-tuple ``(shape_tag, primary, secondary)``
    with ``secondary`` set to ``None`` for shapes that have no second
    field (string, item, time, range, p1932-string). Uniform arity keeps
    static analysis happy without changing equivalence semantics —
    shape_tag still discriminates type so cross-type values can't
    collide.

    Text-shape comparables (``string``, ``monolingual``, ``p1932-string``)
    fold their primary through :func:`casefold_for_compare` — trailing
    period on a DPLA-authored description vs. an inferred community copy
    without one, or wrapping ``[…]`` brackets on a supplied-title, must
    not defeat dedup. The stored claim values are unchanged; only the
    comparator key is folded.
    """
    ms = stmt.get("mainsnak") or {}
    snaktype = ms.get("snaktype")

    if snaktype == "value":
        dv = ms.get("datavalue") or {}
        dtype = dv.get("type")
        v = dv.get("value")
        if dtype == "time":
            try:
                return ("time", _time_claim_comparable(stmt), None)
            except (KeyError, TypeError):
                return None
        if dtype == "string":
            if not isinstance(v, str):
                return None
            folded = casefold_for_compare(v)
            # A punctuation-only string folds to the empty key; treating
            # two such claims as equal would silently dedup distinct
            # malformed values (e.g. ``"..."`` and ``"---"``). Skip.
            return ("string", folded, None) if folded else None
        if dtype == "monolingualtext" and isinstance(v, dict):
            text = v.get("text")
            if not isinstance(text, str):
                return None
            folded = casefold_for_compare(text)
            if not folded:
                return None
            return ("monolingual", folded, v.get("language"))
        if dtype == "wikibase-entityid" and isinstance(v, dict):
            return ("item", v.get("id"), None)
        return None

    if snaktype == "somevalue":
        for q in (stmt.get("qualifiers") or {}).get("P1932", []):
            qv = q.get("datavalue") or {}
            if qv.get("type") != "string":
                continue
            s = (qv.get("value") or "").strip()
            if not s:
                continue
            # Legacy inferred-from-Wikitext claims written before the
            # expand-then-store fix carry raw ``{{other date|~|1911}}``
            # markup here instead of its rendered text. Convert the
            # supported modifiers to the display string parse_dpla_date
            # understands so they collapse with the DPLA-sourced claim
            # (which carries the rendered "circa 1911" / value-typed
            # time). Non-template / unsupported-modifier values pass
            # through unchanged.
            parse_input = parse_other_date_template(s) or s
            parsed = parse_dpla_date(parse_input)
            if parsed and parsed.get("value"):
                # Same shape (and circa-bit suffix) as
                # ``_time_claim_comparable`` so a somevalue+P1932="1945"
                # collapses with a value-typed time mainsnak.
                base = _time_comparable(parsed["value"])
                key = f"{base}|circa" if parsed.get("approximate") else base
                return ("time", key, None)
            # Range parser sees the RAW value — it matches the
            # ``{{other date|between|X|Y}}`` markup directly.
            rng = parse_date_range(s)
            if rng:
                return ("date-range", rng[0], rng[1])
            # Literal-string fallback — only matches another P1932
            # qualifier whose stated-as text folds to the same
            # comparator key (casefold + trim leading/trailing
            # punctuation + collapse whitespace). Empty-folded values
            # skip so two punctuation-only strings can't dedup.
            folded = casefold_for_compare(s)
            return ("p1932-string", folded, None) if folded else None
        return None

    return None


def _is_inferred_from_wikitext_reference(reference):
    """Return True iff ``reference`` carries the legacy-migration import
    fingerprint — a ``P887 = Q131783016`` snak ("based on heuristic =
    inferred from Wikitext"). The legacy ``{{Artwork}}`` →
    ``{{DPLA metadata}}`` importer stamps this on every claim it writes,
    so it's a sufficient marker for "we added this during migration."
    Parallel of :func:`_is_dpla_reference` for the inferred-import side.
    """
    snaks = (reference or {}).get("snaks") or {}
    for snak in snaks.get("P887") or []:
        try:
            if snak["datavalue"]["value"]["id"] == "Q131783016":
                return True
        except (KeyError, TypeError):
            continue
    return False


def _reconcile_inferred_from_wikitext_dupes(mediaid):
    """Remove inferred-from-Wikitext claims whose comparable value
    equals a DPLA-attributed claim on the same property.

    Pushes statement IDs onto the module-level ``removals`` accumulator
    — same channel ``_reconcile_existing_claims`` uses, so the per-file
    dispatcher flushes both kinds of removal in one wbeditentity.

    Idempotency contract: a file migrated with the legacy
    ``{{Artwork}}`` importer can carry an inferred-from-Wikitext claim
    parallel to a DPLA-authored one whenever the migrator couldn't
    recognise their semantic equivalence (notably for year ranges, see
    M193555788 with ``{{other date|between|1934|1948}}`` and
    ``"1934 - 1948"``). On the next sdc-sync re-run, the improved
    comparable logic — :func:`_statement_comparable_value` plus
    :func:`ingest_wikimedia.sdc.parse_date_range` — recognises the two
    statements as the same fact and queues the inferred one for
    removal. Same rule covers the DPLA-drifts-into-match case (e.g. a
    later DPLA edit produces a value that happens to equal a
    previously-preserved community claim): nothing branches on whether
    the equivalence is new or was always there.

    Safety: only removes statements whose references carry the exact
    ``P887 → Q131783016`` fingerprint. Third-party community claims
    that happen to equal a DPLA value are never touched — this routine
    operates strictly on the import set the migration emitted.
    """
    entity = get_entity(mediaid)
    statements = entity.get("statements") or {}
    for stmts in statements.values():
        dpla_comparables = set()
        inferred_stmts = []
        for stmt in stmts:
            refs = stmt.get("references") or []
            if any(_is_dpla_reference(ref) for ref in refs):
                comp = _statement_comparable_value(stmt)
                if comp is not None:
                    dpla_comparables.add(comp)
            elif any(_is_inferred_from_wikitext_reference(ref) for ref in refs):
                inferred_stmts.append(stmt)
        if not dpla_comparables:
            continue
        for stmt in inferred_stmts:
            comp = _statement_comparable_value(stmt)
            if comp is not None and comp in dpla_comparables:
                removals.append(stmt["id"])


def _fetch_dpla_doc_from_api(dpla_id, dpla_api):
    """Fetch a single DPLA item's inner doc from the public DPLA API.

    Returns the inner doc (same shape as an ES `_source` and as the
    dpla-map.json staged in S3 under the partner's sharded item prefix) on
    success, or None on API error. Retries once after a 30-second sleep on
    the first network failure; if the retry also raises, returns None so
    parsed() can treat the item as Missing rather than aborting the batch.
    """
    try:
        response = requests.get(
            f"https://api.dp.la/v2/items/{dpla_id}?api_key={dpla_api}",
            timeout=15,
        ).json()
    except Exception:
        print(" -- Sleeping 30 seconds and retrying...")
        time.sleep(30)
        try:
            response = requests.get(
                f"https://api.dp.la/v2/items/{dpla_id}?api_key={dpla_api}",
                timeout=15,
            ).json()
        except Exception as retry_e:
            print(f" -- DPLA API retry failed for {dpla_id}: {retry_e!r}")
            return None
    try:
        return response["docs"][0]
    except Exception:
        print(response)
        print("DPLA API returned error.")
        return None


def _fetch_dpla_doc_from_s3(s3_client, partner, dpla_id):
    """Read a single DPLA item's inner doc from S3.

    `get-ids-es` stages the ES `_source` for every eligible item as
    dpla-map.json under the partner's sharded item prefix (resolved by
    S3Client.get_item_metadata). Returns the parsed doc on success, None
    when the object is missing, unparseable, or temporarily unreachable.
    A None return lets `parsed()` fall back to the DPLA API path so we
    never silently skip an item just because it hasn't been staged yet
    or S3 had a hiccup.

    `get_item_file` (the backing helper) returns None cleanly only for
    404/NoSuchKey — any other ClientError (5xx, throttling, permissions)
    re-raises. Wrap so transient infrastructure errors trigger the API
    fallback instead of aborting the whole batch.
    """
    # Lazy import keeps the --help path from pulling in botocore. Only
    # reached when --from-s3 was set, by which point boto3 is already
    # loaded via S3Client; this import is a cached no-op then.
    from botocore.exceptions import ClientError

    try:
        raw = s3_client.get_item_metadata(partner, dpla_id)
    except ClientError as e:
        print(
            f" -- S3 fetch failed for {partner}/{dpla_id} ({e!r}); "
            "falling back to api.dp.la."
        )
        return None
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception as e:
        print(f" -- S3 dpla-map.json for {dpla_id} failed to parse: {e}")
        return None


def parsed(dpla_id, dpla_api):
    """Resolve a DPLA item to the 13-tuple consumed by process_one().

    Source-of-truth selection:
      * When --from-s3 <partner> is configured, try S3 first. On miss
        (object not present, parse error), fall back to api.dp.la so a
        not-yet-staged item still syncs.
      * Otherwise hit api.dp.la directly.

    Returns None when neither source has a usable doc — process_one()
    treats that as a Missing ID.
    """
    print(f" -- Accessing DPLA ID {dpla_id}")

    dpla = None
    if _s3_partner is not None:
        dpla = _fetch_dpla_doc_from_s3(_s3_client, _s3_partner, dpla_id)
        if dpla is not None:
            print(f" -- Loaded {dpla_id} from S3 ({_s3_partner})")
    if dpla is None:
        dpla = _fetch_dpla_doc_from_api(dpla_id, dpla_api)

    print(f" -- Accessed DPLA ID {dpla_id}")

    if dpla is None:
        return None
    # Stash the raw doc so ``_post_sdc_cleanup_for_legacy_mode`` can
    # reuse it after ``process_one`` returns, skipping a redundant
    # S3 / api.dp.la fetch on the same dpla_id. Cache is per dpla_id
    # and pop-on-read in the cleanup helper so it doesn't grow.
    _legacy_mode_doc_cache[dpla_id] = dpla
    return _parse_dpla_doc(dpla, dpla_id)


def _parse_dpla_doc(dpla, dpla_id):
    """Parse a DPLA item's inner doc into the 13-tuple consumed by
    process_one().

    Pure transformation over the doc, except for the optional batched
    Wikidata reconciliation call for NARA `exactMatch` subjects (which has
    no cheaper place to live until PR 2 moves it into get-ids-es).
    """
    hub = hubs[dpla["provider"]["name"]]["Wikidata"]
    institution = hubs[dpla["provider"]["name"]]["institutions"][
        dpla["dataProvider"]["name"]
    ]["Wikidata"]
    titles = dpla["sourceResource"]["title"]
    if isinstance(titles, str):
        titles = [titles]
    rs = dpla["rights"]
    url = dpla["isShownAt"]

    try:
        dates = []
        for displaydate in dpla["sourceResource"]["date"]:
            dates.append(displaydate["displayDate"])
    except Exception:
        dates = []
    try:
        local_ids = dpla["sourceResource"]["identifier"]
        if isinstance(local_ids, str):
            local_ids = [local_ids]
    except Exception:
        local_ids = []
    try:
        descs = dpla["sourceResource"]["description"]
        if isinstance(descs, str):
            descs = [descs]
    except Exception:
        descs = []
    try:
        subjects = []
        # First pass: build the subject list and queue up any NARA exactMatch
        # entries for reconciliation. We reserve a slot in `subjects` for each
        # one and fill in the resolved Wikidata ID in a single batched HTTP
        # call below.
        reconci_slots = []  # list of (slot_index, name, naid)
        for subject in dpla["sourceResource"]["subject"]:
            added = False
            if subject.get("name") in subject_ids:
                for subjqid in subject_ids[subject.get("name")]["id"]:
                    if not (any(subjqid in i for i in subjects)):
                        subjects.append((str(subject.get("name")), subjqid))
                        added = True
                    if not (any(subject.get("name") in i for i in subjects)):
                        subjects.append((str(subject.get("name") or ""), ""))
                        added = True
            elif subject.get("exactMatch"):
                naid = subject.get("exactMatch")[0].replace(
                    "https://catalog.archives.gov/id/", ""
                )
                name = str(subject.get("name") or "")
                subjects.append([name, ""])  # mutable placeholder
                reconci_slots.append((len(subjects) - 1, name, naid))
                added = True
            if not added:
                subjects.append((str(subject.get("name") or ""), ""))

        if reconci_slots:
            queries = {
                f"q{i}": {
                    "query": name,
                    "limit": 5,
                    "properties": [{"pid": "P1225", "v": naid}],
                    "type_strict": "should",
                }
                for i, (_, name, naid) in enumerate(reconci_slots)
            }
            h = requests.get(
                "https://wikidata.reconci.link/en/api?queries="
                + urllib.parse.quote(json.dumps(queries)),
                timeout=15,
            )
            results = h.json()
            for i, (slot, name, _) in enumerate(reconci_slots):
                result = results.get(f"q{i}", {}).get("result") or []
                subjqid = result[0]["id"] if result else ""
                subjects[slot] = (name, subjqid)

        subjects = [tuple(s) for s in subjects]
    except Exception:
        subjects = []
    try:
        creators = dpla["sourceResource"]["creator"]
        if isinstance(creators, str):
            creators = [creators]
    except Exception:
        creators = []
    if dpla["provider"]["name"] == "National Archives and Records Administration":
        naids = dpla["sourceResource"]["identifier"]
        if isinstance(naids, str):
            naids = [naids]
        # Malformed NARA XML raises ET.ParseError — intentionally propagated
        # to the per-file boundary so a parse failure does NOT write a
        # partial sdc.json that the reconciler would then mis-treat as
        # "P7228/P6224 should not exist" and strip from Commons.
        access, level = parse_nara_access_level(dpla["originalRecord"]["stringValue"])
        local_ids = []
    else:
        naids = []
        access = ""
        level = ""

    return (
        url,
        descs,
        dates,
        titles,
        hub,
        local_ids,
        institution,
        rs,
        creators,
        subjects,
        naids,
        access,
        level,
    )


def _resolve_dpla_id(title, dpla_api):
    """Return the DPLA item ID for a Commons file title.

    Tries the standard DPLA filename pattern first; falls back to a NARA
    identifier lookup for National Archives files. Returns the title unchanged
    if neither resolves (parsed() will record it as a missing ID).

    The NARA fallback hits the DPLA search API; any network, timeout, or
    unexpected-payload failure here would otherwise abort the entire --cat
    batch on a single bad file, so we catch broadly, log, and fall through
    to the title-passthrough.
    """
    dpla_id = extract_dpla_id_from_commons_title(title)
    if dpla_id:
        return dpla_id
    print("Detecting NARA identifier...")
    nara_id = re.sub(r"^.*NARA - (.*?)[\.| ].*$", r"\1", title)
    try:
        nara_item = requests.get(
            f'https://api.dp.la/v2/items?api_key={dpla_api}&provider.@id="http://dp.la/api/contributor/nara"&sourceResource.identifier="{nara_id}"',
            timeout=15,
        ).json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f" -- NARA lookup failed for {nara_id!r}: {e!r}")
        return title
    docs = nara_item.get("docs") if isinstance(nara_item, dict) else None
    if isinstance(docs, list) and len(docs) == 1:
        return docs[0].get("id") or title
    return title


def _extract_source_url(file_page) -> str | None:
    """Best-effort: the provider source URL from a file's wikitext — the
    ``url=`` parameter of a legacy ``{{DPLA|...}}`` block or a flat
    ``{{DPLA metadata}}`` param. Returns None when absent or unreadable.
    Used by maintain mode to re-link an ID-drifted file to its current record.
    """
    try:
        text = file_page.text
    except Exception:
        return None
    m = re.search(r"\burl\s*=\s*(https?://[^|}\s\n]+)", text or "")
    return m.group(1).strip() if m else None


def _maintain_qid_name_map() -> dict[str, str]:
    """``{institution Wikidata QID -> dataProvider name}`` from institutions_v2.

    The dataProvider name is the institutions_v2.json institution key — the
    exact string the DPLA index stores in ``dataProvider.name`` — so a QID read
    off a file's existing P195 (collection) statement maps straight to the ES
    scope term for the anchor-3 wildcard. Built once per run from ``hubs`` and
    memoised. Only service-hub *institution* QIDs are mapped; a content hub's
    own QID (NARA/Smithsonian, where P195 carries the hub) has no single
    dataProvider name, so those files simply don't get an anchor-3 scope.
    """
    global _maintain_qid_to_name
    if _maintain_qid_to_name is None:
        mapping: dict[str, str] = {}
        for hub_data in (hubs or {}).values():
            for inst_name, inst_data in (hub_data.get("institutions") or {}).items():
                qid = (inst_data or {}).get("Wikidata")
                if qid:
                    mapping[qid] = inst_name
        _maintain_qid_to_name = mapping
    return _maintain_qid_to_name


def _existing_p195_qid(entity: dict) -> str | None:
    """The institution QID from a Commons file's existing P195 (collection)
    statement, or None. P195 is written by ``add_collection`` as the institution
    (service hub) or the hub itself (content hub), and is stable under DPLA ID
    drift — which is what makes it a reliable scope anchor when the id is dead.
    """
    statements = entity.get("statements") or entity.get("claims") or {}
    for stmt in statements.get("P195", []) or []:
        snak = stmt.get("mainsnak") if isinstance(stmt, dict) else None
        val = ((snak or {}).get("datavalue") or {}).get("value")
        if isinstance(val, dict) and val.get("id"):
            return val["id"]
    return None


def _maintain_scope_filter(mediaid: str) -> dict | None:
    """ES filter clause scoping the anchor-3 wildcard to ``mediaid``'s own
    institution, or None when it can't be derived (no P195, or a QID that isn't
    a known service-hub institution). Reads the file's *existing* P195 — so the
    wildcard never runs an unbounded whole-index scan. Invoked lazily by
    :func:`resolve_current_dpla_id`, only for files that fall through to the
    wildcard rung.
    """
    try:
        entity = get_entity(mediaid)
    except Exception:
        # A missing/deleted entity or transient API error here only costs the
        # wildcard rung for this one file; the resolver falls back to unresolved.
        return None
    qid = _existing_p195_qid(entity)
    if not qid:
        return None
    name = _maintain_qid_name_map().get(qid)
    if not name:
        return None
    return {"term": {"dataProvider.name.not_analyzed": name}}


def _maintain_resolve(title, embedded_id, file_page, mediaid):
    """Resolve the CURRENT DPLA id for a (possibly ID-drifted) Commons file and
    return the full :class:`ResolveResult` (id + which anchor resolved it).

    Walks the full ladder: embedded id still live, exact ``isShownAt`` over
    normalized URL variants, then the institution-scoped wildcard — the last
    bounded by the file's own P195 institution, derived lazily so only
    domain-drifted files pay for it.
    """
    return resolve_current_dpla_id(
        embedded_id=embedded_id,
        recorded_url=_extract_source_url(file_page),
        scope_filter=lambda: _maintain_scope_filter(mediaid),
    )


def _maintain_sidecar_payload(dpla_id):
    """Maintain mode: load the precomputed ``sdc.json`` for ``dpla_id`` from S3
    (staged by ``get-ids-es --maintain``) so the sync runs through
    :func:`process_one_from_sdc` — no ``api.dp.la`` call, no runtime claim
    build. Requires ``--from-s3 <partner>`` (which sets ``_s3_partner`` /
    ``_s3_client``); returns the parsed payload, or None when no sidecar is
    staged for this id. A None result means the caller should SKIP the file
    rather than fall back to a per-file live API call — at hub scale (100Ks of
    files, possibly parallel) that fallback is exactly the api.dp.la load this
    path exists to avoid.
    """
    if _s3_partner is None:
        return None
    raw = _s3_client.get_sdc_json(_s3_partner, dpla_id)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _maintain_canonical_title(file_page, dpla_id):
    """The canonical Commons title for a maintained file under its (re-linked)
    ``dpla_id``, or None when it can't be computed.

    Built with the same :func:`get_page_title` authority the uploader uses, so
    any difference from the file's current title reflects a real
    current-vs-canonical difference (a drifted embedded id, or an upstream
    title-text change) — never a fresh normalization rule. Maintain changes
    neither the bytes nor the page structure, so the extension and page ordinal
    are taken from the file's own existing title; only the descriptive prefix
    and the embedded id can move. The descriptive title comes from the item's
    staged ``dpla-map.json`` (``sourceResource.title``), so this requires
    ``--from-s3``.
    """
    if _s3_partner is None:
        return None
    raw = _s3_client.get_item_metadata(_s3_partner, dpla_id)
    if not raw:
        return None
    try:
        metadata = json.loads(raw)
    except json.JSONDecodeError:
        return None
    titles = get_list(
        get_dict(metadata, SOURCE_RESOURCE_FIELD_NAME), DC_TITLE_FIELD_NAME
    )
    item_title = titles[0] if titles else ""
    # No usable source title to build a filename from — leave the name as-is.
    # The isinstance guard matters because get_page_title slices/replaces the
    # title (item_title[:181].replace(...)); a non-string staged value would
    # raise there and abort the whole --cat batch. Don't strip/normalize beyond
    # the type check: the uploader builds the stored title from the raw
    # titles[0], so altering it here would break canonical-title parity and
    # provoke spurious renames.
    if not isinstance(item_title, str) or not item_title:
        return None
    current = file_page.title(with_ns=False)
    ext = os.path.splitext(current)[1]
    page = wikimedia.extract_page_ordinal_from_commons_title(current)
    return wikimedia.get_page_title(item_title, dpla_id, ext, page=page)


def _maintain_rename(file_page, dpla_id):
    """Maintain mode: move a file to its canonical title when its current title
    has drifted from it (PR B). Returns the FilePage to SDC-sync against — the
    moved page on success, or the original file on a no-op or blocked move.

    The move runs only when the canonical title is free or a single-revision
    redirect back to this same file; MediaWiki permits move-over-redirect only
    in that case, so a blind move is safe — it cannot clobber another page. When
    the canonical title is occupied by a different page the move raises, and we
    log an error for DPLA to resolve and leave the file at its non-canonical
    title (its SDC still syncs in place). Maintain has no source bytes, so it
    never inspects hashes or picks a winner. A move never creates a page, so the
    no-create fence is not at issue.
    """
    canonical = _maintain_canonical_title(file_page, dpla_id)
    current = file_page.title(with_ns=False)
    if not canonical or canonical == current:
        return file_page
    # Gate inbound-usage BEFORE the move, while ``current`` is still the live
    # file — post-move it is a redirect and the usage query is unreliable.
    needs_relink = wikimedia.file_has_inbound_usage(site, current)
    reason = wikimedia.build_title_drift_move_reason(
        current, canonical, dpla_id, site.user()
    )
    try:
        file_page.move(
            f"File:{canonical}", reason=reason, movetalk=False, noredirect=False
        )
    except pywikibot.exceptions.ArticleExistsConflictError as e:
        # The one outcome MAINTAIN_RENAME_BLOCKED is meant to count: MediaWiki
        # raises this only when the canonical title is occupied by a page that
        # isn't a redirect back to this file — a genuine collision needing DPLA
        # follow-up. Leave the file non-canonical; its SDC still syncs in place.
        logging.error(
            f"maintain: could not move [[File:{current}]] ->"
            f" [[File:{canonical}]] ({e}); leaving non-canonical for review."
        )
        tracker.increment(Result.MAINTAIN_RENAME_BLOCKED)
        return file_page
    except pywikibot.exceptions.Error as e:
        # Any other move failure (transient API, auth, invalid-name) is NOT an
        # occupancy block — don't inflate the blocked counter with it. Log and
        # continue; a later maintain run retries the rename.
        logging.error(
            f"maintain: move failed for [[File:{current}]] ->"
            f" [[File:{canonical}]] ({e}); continuing without rename."
        )
        return file_page
    logging.info(f"maintain: renamed [[File:{current}]] -> [[File:{canonical}]]")
    tracker.increment(Result.MAINTAIN_RENAMED)
    if needs_relink:
        wikimedia.post_commonsdelinker_request(
            site, current, canonical, check_usage=False
        )
    return wikimedia.get_page(site, f"File:{canonical}")


# Pre-flight sizing (``--count-only``) tally: one bucket per resolver anchor
# plus ``ambiguous`` (a multi-hit the resolver refused to auto-apply). Order is
# the ladder order so the printed breakdown reads top-to-bottom.
_MAINTAIN_TALLY_ANCHORS = ("embedded", "isShownAt", "wildcard", "unresolved")


def _maintain_process_file(mediaid, embedded_id, file_page, title, tally=None):
    """Maintain one Commons file: re-link to its current DPLA id, then either
    tally the resolver outcome (``--count-only`` pre-flight sizing — ``tally``
    given, nothing written) or SDC-sync it.

    The sync runs through the staged ``sdc.json`` sidecar (``--from-s3``) so it
    never calls ``api.dp.la`` and materialises P304 from the page ordinal in the
    title. With no sidecar staged for the (re-linked) id the file is skipped
    rather than falling back to a per-file live fetch — at hub scale that
    fallback is the api.dp.la load this path exists to avoid. When ``--from-s3``
    is not set at all (e.g. an ad-hoc ``--file`` run), it falls back to the live
    ``process_one`` for that single file.
    """
    result = _maintain_resolve(title, embedded_id, file_page, mediaid)
    dpla_id = result.dpla_id or embedded_id
    if result.dpla_id and result.dpla_id != embedded_id:
        logging.info(
            f"maintain: re-linked {title}: {embedded_id} ->"
            f" {result.dpla_id} (via {result.anchor})"
        )
    elif result.dpla_id is None:
        logging.info(
            f"maintain: could not re-link {title}"
            f" (embedded {embedded_id}); leaving as-is."
        )

    if tally is not None:
        tally[result.anchor] += 1
        if result.ambiguous:
            tally["ambiguous"] += 1
        return

    # Emit the per-item marker the status poller (wikimedia_upload_status.py)
    # counts for SDC-phase progress, so a maintain --cat run advances past
    # "starting..." like a partner-mode run does.
    logging.info(f"DPLA ID: {dpla_id}")

    if _s3_partner is not None:
        # Title-drift rename (#3): move to the canonical title for the re-linked
        # id before syncing, so SDC + wikitext cleanup land on the final page.
        # The MediaInfo entity is keyed by pageid, which a move preserves, so
        # ``mediaid`` stays valid; the page ordinal is unchanged by the move, so
        # ``title``'s ordinal still drives P304.
        file_page = _maintain_rename(file_page, dpla_id)
        payload = _maintain_sidecar_payload(dpla_id)
        if payload is None:
            logging.info(
                f"maintain: no staged sdc.json for {dpla_id} ({title}); skipping."
            )
            tracker.increment(Result.SDC_ITEMS_SKIPPED_NO_SIDECAR)
            return
        page_number = wikimedia.extract_page_ordinal_from_commons_title(title)
        _safe_process_one(
            mediaid,
            dpla_id,
            file_page=file_page,
            sdc_payload=payload,
            page_number=page_number,
        )
    else:
        _safe_process_one(mediaid, dpla_id, file_page=file_page)


def _new_maintain_tally(enabled):
    """A zero-initialised per-anchor tally for ``--count-only`` sizing, or None
    when sizing is off. dict.fromkeys keeps a 0 for every bucket so
    :func:`_report_maintain_tally` never KeyErrors on an unseen tier.
    """
    if not enabled:
        return None
    return dict.fromkeys((*_MAINTAIN_TALLY_ANCHORS, "ambiguous"), 0)


def _report_maintain_tally(tally, total):
    """Print the ``--count-only`` per-anchor sizing breakdown."""
    print(f"\nmaintain pre-flight sizing ({total} files in scope):")
    for anchor in _MAINTAIN_TALLY_ANCHORS:
        print(f"  {anchor:>12}: {tally[anchor]}")
    print(f"  {'ambiguous':>12}: {tally['ambiguous']} (multi-hit, not auto-applied)")
    resolved = total - tally["unresolved"]
    print(f"  {'-> resolved':>12}: {resolved}/{total}")


def _emit_maintain_summary(label, elapsed_seconds, workers=1):
    """Maintain mode's terminal summary, mirroring _run_partner_mode's: log the
    ``COUNTS:`` marker (which the status poller reads as the SDC-phase
    completion signal) and post the Slack completion notice with the maintain
    counters (renames included). Called only on normal loop completion — an
    aborted run propagates the exception past this point, so (as in partner
    mode) the shell-level ``notify_pipeline_fail`` handles the failure instead
    and no spurious ``COUNTS:`` is written.

    ``workers`` is the real worker count so the SLOT WAIT (avg/wkr) line divides
    the aggregate worker-seconds correctly; the serial paths keep the default 1.
    """
    logging.info("\n" + str(tracker))
    logging.info(f"{elapsed_seconds} seconds.")
    notify_sdc_complete(
        tracker=tracker,
        partner_label=label,
        elapsed_seconds=elapsed_seconds,
        workers=workers,
        maintain=True,
    )


def process_one(mediaid, dpla_id):
    """Fetch DPLA metadata and sync SDC claims for a single Commons file."""
    # Drop every prior file's cached entity at the file boundary so the
    # cache doesn't leak entities across files (see _entity_cache
    # docstring), and drain the per-file accumulators so the dispatcher
    # only sees this file's fragments. Pre-warm the cache so the ~25
    # add_* / check() calls below share one wbgetentities round-trip.
    _entity_cache.clear()
    _reset_per_file_accumulators()
    get_entity(mediaid)

    # parsed() returns None on lookup failure (was: returned False and the
    # tuple-unpack TypeError was caught here, which also swallowed real
    # parser bugs as "missing ID"). Check the sentinel explicitly.
    parsed_result = parsed(dpla_id, dpla_api)
    if not parsed_result:
        with open("Missing ids.txt", "a") as missing:
            missing.write(dpla_id + "\n")
            print(" -- Missing ID recorded.")
        return

    # Pin P813 to the item's DPLA ingestDate for the whole file's writes.
    # ``parsed()`` stashed the raw doc in ``_legacy_mode_doc_cache``; the
    # legacy-mode cleanup helper pops it after we're done.
    global _current_ingest_date
    _current_ingest_date = ingest_date_from_doc(_legacy_mode_doc_cache[dpla_id])
    (
        url,
        descs,
        dates,
        titles,
        hub,
        local_ids,
        institution,
        rs,
        creators,
        subjects,
        naids,
        access,
        level,
    ) = parsed_result

    try:
        add_rs(mediaid, rs, dpla_id)
    except pywikibot.exceptions.APIError:
        print(" -- No such file on Commons.")
        return
    add_id(mediaid, dpla_id)
    for title in titles:
        add_title(mediaid, title.rstrip(), dpla_id)
    add_collection(mediaid, hub, institution, dpla_id)
    for creator in creators:
        add_creator(mediaid, creator.rstrip(), dpla_id)
    for date in dates:
        add_date(mediaid, date.rstrip(), dpla_id)
    for subject in subjects:
        add_subject(mediaid, subject[0], dpla_id)
        if subject[1]:
            add_subject_entity(mediaid, subject[1], dpla_id)
    for desc in descs:
        add_desc(mediaid, desc.rstrip(), dpla_id)
    add_contributed(mediaid, hub, institution, dpla_id)
    add_source(mediaid, hub, url, dpla_id)
    for local_id in local_ids:
        if len(local_id) < 1501:
            add_local_id(mediaid, local_id, institution, dpla_id)
    for naid in naids:
        add_naid(mediaid, naid, dpla_id)
    add_access(mediaid, access, dpla_id)
    add_level(mediaid, level, dpla_id)

    # Mirror the partner-mode handler at `_run_partner_mode`: ``no-such-entity``
    # is a clean skip, not an error — the MediaInfo entity doesn't exist
    # (file deleted by a Commons curator as a duplicate, or this is a
    # legacy --file/--cat/--list run for a file we never owned). Without
    # this guard the same Commons response that the partner path treats
    # as a skip would crash the legacy entry points.
    try:
        # Run the reconciler builder so it populates the ``removals``
        # accumulator alongside the new-claim and new-ref accumulators
        # built by the check() walk above. Then flush everything in
        # one atomic wbeditentity per file.
        dpla_claims(
            mediaid,
            dpla_id,
            url,
            descs,
            dates,
            titles,
            hub,
            local_ids,
            institution,
            rs,
            creators,
            subjects,
            naids,
            access,
            level,
        )
        _flush_per_file_edits(mediaid, dpla_id)
    except _MissingEntityError:
        logging.info(
            f" -- {mediaid} for {dpla_id}: Commons MediaInfo entity does not"
            " exist; skipping (not an error)."
        )
        tracker.increment(Result.SDC_ORDINALS_SKIPPED_MISSING_ENTITY)
        return


def process_one_from_sdc(
    mediaid, dpla_id, sdc_payload, download_url=None, page_number=None
):
    """Sync SDC for a single Commons file against a precomputed claim list.

    Reads the claim envelope produced by `build_claims_for_doc` and staged
    to S3 as `<partner>/<dpla_id>/sdc.json`, diffs each claim against the
    file's existing SDC, and POSTs adds + removals. This is the PR 4
    cutover from runtime claim construction to pure S3-driven sync — no
    api.dp.la call, no per-add_* dispatch, no Wikidata reconciliation
    here (all of that happened upstream in get-ids-es).

    The diff is the same the legacy `process_one` performs, just driven
    by sdc.json's claim list instead of by an in-code property-by-property
    walk. Reuses the existing `check()` per-property matcher, the shared
    `_post_new_refs`/`_post_new_claims` POST helpers, and the shared
    `_reconcile_existing_claims` removal logic.

    ``download_url`` is the per-ordinal direct file URL (from
    file-list.txt). When supplied, the P7482 (described at) claim gets
    a P2699 (URL) qualifier with that value before being posted —
    different ordinals of the same DPLA item have different download
    URLs, so the qualifier must be materialized per-call here rather
    than baked into the per-item sdc.json. For existing P7482
    statements on Commons that lack P2699, ``_amend_p7482_url_qualifiers``
    POSTs the missing qualifier via wbsetqualifier (idempotent).

    ``page_number`` is the per-ordinal P304 (page-number) value computed
    by :func:`_compute_page_numbers` from upload-result.json's per-file
    extension grouping. Supplied only for files that are part of a
    multi-file extension group on a multipage item; ``None`` for
    single-file ordinals. When supplied, the P760 (DPLA ID) claim
    receives a P304 qualifier with the page number; the legacy-state
    backfill ``_amend_p760_page_qualifier`` handles existing P760
    statements that lack it.
    """
    # Drop every prior file's cached entity at the file boundary so the
    # cache doesn't leak entities across files (see _entity_cache
    # docstring), and drain the per-file accumulators so the dispatcher
    # only sees this file's fragments. Pre-warm the cache so the
    # per-claim check() / amend_* / reconciler calls below share one
    # wbgetentities round-trip.
    _entity_cache.clear()
    _reset_per_file_accumulators()
    get_entity(mediaid)

    # Pin P813 to the ingest date the sdc.json envelope records — same date
    # baked into every P813 snak in the payload's claims, kept as a
    # top-level field so the reference-refresh path doesn't have to peel it
    # out of a claim.
    global _current_ingest_date
    raw_ingest_date = sdc_payload.get("ingest_date")
    if not isinstance(raw_ingest_date, str):
        raise ValueError(
            f"sdc.json for {dpla_id} is missing 'ingest_date' — regenerate "
            "the sidecar with a post-P813-pinning get-ids-* run."
        )
    _current_ingest_date = datetime.date.fromisoformat(raw_ingest_date)

    sdc_claims = sdc_payload.get("claims", [])
    first_check = True
    for source_claim in sdc_claims:
        # Deepcopy before any mutation — `add_ref` stamps `claim["id"]` on
        # the object it's given, and we also append it to `claims["claims"]`
        # for the wbeditentity POST. The same `sdc_payload` is reused across
        # every ordinal of a multi-page item, so without this copy ordinal N
        # would inherit ordinal N-1's per-mediaid claim IDs and references.
        claim = copy.deepcopy(source_claim)
        prop = claim["mainsnak"]["property"]

        # Materialize the per-ordinal P2699 qualifier on the P7482 claim.
        # build_claims_for_doc can't do this — sdc.json is per-DPLA-item
        # and a multi-page item's ordinals have different download URLs.
        if prop == "P7482" and download_url:
            claim.setdefault("qualifiers", {})["P2699"] = [
                {
                    "snaktype": "value",
                    "property": "P2699",
                    "datavalue": {"value": download_url, "type": "string"},
                    "datatype": "url",
                }
            ]

        # Per-ordinal P304 (page-number) qualifier on the P760 (DPLA ID)
        # claim — same per-ordinal rationale as P2699 above. Only stamped
        # for files that are part of a multi-file extension group on a
        # multipage item; ``page_number`` is ``None`` otherwise.
        if prop == "P760" and page_number is not None:
            claim.setdefault("qualifiers", {})["P304"] = [
                {
                    "snaktype": "value",
                    "property": "P304",
                    "datavalue": {"value": str(page_number), "type": "string"},
                }
            ]

        kind = _check_kind_for_claim(claim)
        comparable = _extract_comparable_value(claim)
        if comparable is None:
            # Unknown claim shape — fall through and post unconditionally;
            # `check()` can't compare it against existing state.
            claims["claims"].append(claim)
            continue
        try:
            checkclaim = check(mediaid, (kind, comparable), prop)
        except pywikibot.exceptions.APIError:
            # The first check() call is what tells us whether the file
            # page even exists — subsequent calls hit the cache. If it
            # raises, the page is gone or otherwise unreachable and we
            # shouldn't try to write any SDC to it.
            if first_check:
                print(f" -- No such file on Commons: {mediaid}")
                return
            raise
        first_check = False
        if checkclaim[1]:
            add_ref(checkclaim[1], claim)
        if checkclaim[0] is True:
            pywikibot.output(f" -- Adding [[:d:Property:{prop}]] to {mediaid}.")
            claims["claims"].append(claim)

    # Run the legacy-backfill amend builders and the reconciler so they
    # populate the per-file accumulators (qualifier_amends,
    # qualifier_removals, removals). All four accumulators (those plus
    # the new-claims / new-refs lists populated by the check() walk
    # above) are then flushed in one atomic wbeditentity per file.
    _amend_p7482_url_qualifiers(mediaid, dpla_id, sdc_payload, download_url)
    _amend_p760_page_qualifier(mediaid, dpla_id, sdc_payload, page_number)
    expected = _build_expected_from_sdc(sdc_payload)
    _reconcile_existing_claims(mediaid, dpla_id, expected)
    _reconcile_inferred_from_wikitext_dupes(mediaid)
    _flush_per_file_edits(mediaid, dpla_id)


_NORMALIZE_EDIT_SUMMARY = (
    "Strip {{DPLA metadata}} template parameters now redundant with SDC "
    "(values match what Module:DPLA renders from the structured data)."
)


def _post_sdc_cleanup_for_page(
    file_page,
    dpla_id: str,
    item_metadata: dict,
    provider: dict,
    data_provider: dict,
    *,
    expected_params: dict | None = None,
) -> bool:
    """Post-SDC wikitext cleanup for one Commons file page.

    Dispatches based on the file's current wikitext shape:

    * **Has** ``{{Artwork}}`` / ``{{Information}}`` / ``{{Photograph}}``
      (the legacy DPLA wrappers): run
      :func:`ingest_wikimedia.legacy_artwork.migrate_legacy_file` — walks
      revision history, classifies community contributions vs DPLA-bot
      values, imports community values as SDC statements with the
      ``P887→Q131783016`` + ``P4656→<permalink>`` reference shape, then
      rewrites the wikitext from the legacy form to ``{{DPLA metadata}}``.

    * **Has** ``{{DPLA metadata}}`` (the post-#291 form): run
      :func:`ingest_wikimedia.wikitext_normalize.normalize_page` — strips
      template params whose values are now redundant with the
      DPLA-attributed SDC just written.

    * **Has neither** (rare — a hand-written wikitext or a stub): skip
      silently. The blue box still renders from SDC; nothing to clean
      up in the wikitext.

    The dispatch is order-sensitive: the legacy-template check comes
    first because the migration's revision-history walk is expensive
    (one ``revisions()`` call per file), so we don't want to fire it on
    files that are clearly already on the new template form.

    ``expected_params`` is optional — when not supplied, the dispatcher
    computes it from ``dpla_metadata_params``. Partner mode passes it
    in so a multi-ordinal item doesn't redundantly recompute per
    ordinal.

    Returns ``True`` when the file page was actually saved (legacy
    migration rewrote the wikitext, or normalize_page stripped a
    param); ``False`` when the cleanup was a no-op or skipped.
    Callers use this to track the per-page edit count for the SDC
    summary.
    """
    if not file_page.exists():
        return False

    text = file_page.text or ""

    if legacy_artwork.find_legacy_template(text) is not None:
        # Migrate path. legacy_artwork handles its own provenance walk,
        # SDC import, and wikitext rewrite. Re-fetching expected_params
        # here would be wasted work — migrate_legacy_file recomputes
        # them internally from item_metadata.
        try:
            result = legacy_artwork.migrate_legacy_file(
                file_page=file_page,
                item_metadata=item_metadata,
                provider=provider,
                data_provider=data_provider,
                dpla_id=dpla_id,
                site=site,
            )
        except Exception:
            logging.exception(
                f" -- cleanup: legacy migration of '{file_page.title()}'"
                f" for {dpla_id} failed; skipping."
            )
            return False
        return bool(getattr(result, "wikitext_changed", False))

    # Strip path. Bail before computing ``expected_params`` when the
    # file has no ``{{DPLA metadata}}`` template either — a hand-written
    # stub, or a non-DPLA upload that happens to share a Commons file
    # title. ``dpla_metadata_params`` does real work (resolves the
    # rights URI, builds the hub label, etc.) and would be wasted on
    # a page that ``normalize_page`` is going to no-op anyway.
    if not wikitext_normalize.has_dpla_metadata_template(text):
        return False

    if expected_params is None:
        try:
            expected_params = wikimedia.dpla_metadata_params(
                dpla_id, item_metadata, provider, data_provider
            )
        except Exception:
            logging.exception(
                f" -- cleanup: dpla_metadata_params failed for {dpla_id};"
                " skipping strip."
            )
            return False

    # Defensive guard against the upstream null-pageid bug shape and
    # any future regression: ``normalize_page`` strips wikitext params
    # whose values match ``expected_params``, on the premise that the
    # SDC just written carries the same value so the renderer will
    # supply it. If the file's MediaInfo entity actually has no
    # DPLA-attributed SDC (because the upload-result.json sidecar had
    # a null/zero pageid, or the SDC write was silently dropped for
    # some other reason, or the cleanup is racing a partial sync),
    # the strip would leave the page with no metadata in EITHER
    # representation. Refuse to strip in that state. The per-ordinal
    # SDC writer is the only thing that should ever produce a
    # DPLA-attributed claim; its absence is a reliable signal that
    # this ordinal's SDC sync didn't actually fire.
    mediaid = f"M{file_page.pageid}" if file_page.pageid else None
    if mediaid:
        try:
            entity = _fetch_entity_for_cleanup_guard(mediaid)
        except Exception:
            # API failure during the guard is a hard skip — better
            # than stripping against unknown SDC state.
            logging.exception(
                f" -- cleanup: entity fetch failed for {file_page.title()}"
                f" ({mediaid}); skipping strip out of caution."
            )
            return False
        if not _entity_has_dpla_attributed_claims(entity):
            logging.warning(
                f" -- cleanup: '{file_page.title()}' ({mediaid}) for"
                f" {dpla_id} has no DPLA-attributed SDC; skipping strip"
                " to avoid wiping wikitext params without an SDC"
                " counterpart (upstream sync likely never fired)."
            )
            return False

    try:
        return bool(
            wikitext_normalize.normalize_page(
                file_page, expected_params, _NORMALIZE_EDIT_SUMMARY
            )
        )
    except Exception:
        logging.exception(
            f" -- cleanup: normalize_page on '{file_page.title()}' for"
            f" {dpla_id} failed; skipping."
        )
        return False


def _post_sdc_cleanup_for_item(
    s3, partner: str, dpla_id: str, ordinal_items: list[tuple[str, dict]]
) -> set[str]:
    """Per-item post-SDC cleanup (partner mode).

    Reads ``dpla-map.json`` from S3, resolves provider / data_provider,
    pre-computes the canonical params once for the item, then walks
    every ordinal page through :func:`_post_sdc_cleanup_for_page`.

    Best-effort throughout: any S3 / pywikibot / parse failure is
    logged but never raised. SDC sync has already committed and counted
    before this runs; a cleanup failure must not roll that back or fail
    the partner batch.

    Returns the set of ``ord_str`` values whose file page was actually
    rewritten by cleanup (legacy migration or wikitext strip). The
    partner-mode caller unions this with the set of ordinals whose
    SDC writes landed to derive a per-page edit count without
    double-counting pages that had both kinds of edit.
    """
    from ingest_wikimedia import wikimedia
    from ingest_wikimedia.dpla import DPLA

    edited: set[str] = set()
    try:
        item_raw = s3.get_item_metadata(partner, dpla_id)
    except Exception as e:
        logging.warning(f" -- cleanup: S3 read failed for {dpla_id}: {e!r}; skipping.")
        return edited
    if not item_raw:
        # dpla-map.json missing — get-ids-es never staged it, or the
        # partner-mode item came from a path that doesn't write one.
        # Nothing to compare against; skip silently.
        return edited
    try:
        item_metadata = json.loads(item_raw)
    except json.JSONDecodeError as e:
        logging.warning(
            f" -- cleanup: dpla-map.json parse failed for {dpla_id}: {e}; skipping."
        )
        return edited

    try:
        provider, data_provider = DPLA.get_provider_and_data_provider(
            item_metadata, hubs
        )
    except Exception as e:
        logging.warning(
            f" -- cleanup: provider lookup failed for {dpla_id}: {e!r}; skipping."
        )
        return edited

    try:
        expected_params = wikimedia.dpla_metadata_params(
            dpla_id, item_metadata, provider, data_provider
        )
    except Exception as e:
        logging.warning(
            f" -- cleanup: param compute failed for {dpla_id}: {e!r}; skipping."
        )
        return edited

    for ord_str, data in ordinal_items:
        title = data.get("title")
        if not title or title == "?":
            continue
        try:
            page = pywikibot.FilePage(site, title)
        except Exception:
            logging.exception(
                f" -- cleanup: FilePage construction failed for ordinal"
                f" {ord_str} ({title}) of {dpla_id}; skipping."
            )
            continue
        # Best-effort, per page: the post-SDC cleanup (wikitext strip /
        # legacy migration) loads the page text, which can hit a transient
        # Commons API timeout under concurrency. The item's SDC has already
        # synced and been counted by the time we get here, so a cleanup
        # failure must NOT escape to the worker-task boundary — that would
        # mis-count the item as SDC_ITEMS_SKIPPED_ERROR (double-counting an
        # already-synced item) and skip the PAGES_EDITED tally for its other
        # pages. Skip this page; it re-cleans idempotently on a later run.
        try:
            saved = _post_sdc_cleanup_for_page(
                page,
                dpla_id,
                item_metadata,
                provider,
                data_provider,
                expected_params=expected_params,
            )
        except Exception:
            logging.exception(
                f" -- cleanup: post-SDC cleanup failed for ordinal {ord_str}"
                f" ({title}) of {dpla_id}; skipping (SDC already synced)."
            )
            continue
        if saved:
            edited.add(ord_str)
    return edited


def _post_sdc_cleanup_for_legacy_mode(file_page, dpla_id: str) -> bool:
    """Post-SDC cleanup for the ``--file`` / ``--cat`` / ``--list`` paths.

    Mirrors :func:`_post_sdc_cleanup_for_item` for single-file modes:
    reuses the DPLA item doc ``parsed()`` cached during ``process_one``
    (popped on read from :data:`_legacy_mode_doc_cache`), resolves
    provider / data_provider, then dispatches via
    :func:`_post_sdc_cleanup_for_page`. On a cache miss (e.g. process_one
    early-returned on a missing-ID before the cache populated, or the
    cache was cleared by an earlier cleanup) the doc is fetched fresh
    from S3 / api.dp.la so the cleanup still runs.

    Best-effort: any failure is logged but doesn't raise. Returns
    ``True`` if cleanup actually rewrote the page, else ``False`` —
    used by :func:`_safe_process_one` to track per-page edit counts
    for the SDC summary.
    """
    from ingest_wikimedia.dpla import DPLA

    doc = _legacy_mode_doc_cache.pop(dpla_id, None)
    if doc is None:
        try:
            if _s3_partner is not None:
                doc = _fetch_dpla_doc_from_s3(_s3_client, _s3_partner, dpla_id)
                if doc is None:
                    doc = _fetch_dpla_doc_from_api(dpla_id, dpla_api)
            else:
                doc = _fetch_dpla_doc_from_api(dpla_id, dpla_api)
        except Exception:
            logging.exception(
                f" -- cleanup: DPLA-doc fetch failed for {dpla_id}; skipping."
            )
            return False
    if not doc:
        return False

    try:
        provider, data_provider = DPLA.get_provider_and_data_provider(doc, hubs)
    except Exception as e:
        logging.warning(
            f" -- cleanup: provider lookup failed for {dpla_id}: {e!r}; skipping."
        )
        return False

    return _post_sdc_cleanup_for_page(file_page, dpla_id, doc, provider, data_provider)


def _run_legacy_migration_mode(partner: str, ids_file: str) -> None:
    """Drive the legacy ``{{Artwork}}`` → ``{{DPLA metadata}}`` migration
    for a whole partner.

    Iterates the partner's IDs CSV — same input shape as
    :func:`_run_partner_mode`'s SDC sync path. For each DPLA item:

    1. Read ``dpla-map.json`` (canonical DPLA record) from S3 to drive
       provenance comparison and produce the new template wikitext.
    2. Read ``upload-result.json`` to discover the Commons file titles
       this DPLA item maps to.
    3. For each Commons file:
       * Resolve the ``FilePage`` and walk its revision history.
       * Plan the migration (DPLA-bot vs community provenance).
       * Post any community-import SDC statements with the legacy
         reference shape (P887→Q131783016 + P4656).
       * Rewrite the wikitext, swapping the legacy template for
         ``{{DPLA metadata}}``.

    Per-file exception boundary: any pywikibot APIError, S3 read
    failure, or runtime error on a single Commons file is logged with
    a traceback and counted under
    :class:`Result.LEGACY_SKIPPED_ERROR`. The remaining files for the
    same item, and the partner batch as a whole, keep walking — same
    per-ordinal isolation pattern as :func:`_run_partner_mode`. The
    boundary lives inside :func:`_migrate_one_dpla_item`'s file loop,
    not around the whole item, so one bad ordinal doesn't skip the
    other ordinals' migration.

    Item-level errors (e.g. S3 read failures on the
    ``dpla-map.json`` / ``upload-result.json`` sidecars themselves)
    skip the whole item — same as :func:`_run_partner_mode`. These
    are caught in the outer loop below.
    """
    from botocore.exceptions import ClientError

    from ingest_wikimedia.s3 import S3Client

    # Mirrors _run_partner_mode's start-of-pipeline setup. Operators
    # see the same phase-start / phase-complete Slack messages so
    # wikimedia_upload_status can detect progress identically.
    setup_logging(partner, "legacy-migration", logging.INFO)
    notify_phase_start(partner, "legacy-migration")
    start_time = time.time()
    tracker.reset()

    s3 = S3Client()
    with open(ids_file) as f:
        dpla_ids = [line.strip() for line in f if line.strip()]
    logging.info(
        f"Legacy-migration mode: {partner} — {len(dpla_ids)} items from {ids_file}"
    )

    completed = False
    try:
        for local_count, dpla_id in enumerate(dpla_ids, start=1):
            logging.info(f"DPLA ID: {dpla_id} ({local_count}/{len(dpla_ids)})")
            try:
                _migrate_one_dpla_item(s3, partner, dpla_id)
            except ClientError as e:
                # Item-level sidecar read failure — affects every file
                # for this item identically, so skip the whole item.
                # Per-file errors are caught inside _migrate_one_dpla_item.
                logging.warning(
                    f" -- S3 error during migration of {partner}/{dpla_id}:"
                    f" {e!r}; skipping."
                )
                tracker.increment(Result.LEGACY_SKIPPED_ERROR)
            except Exception:
                # Item-level setup error (DPLA.get_provider_and_data_provider,
                # JSON parse, etc.). Per-file errors are already isolated
                # inside the loop and shouldn't reach here.
                logging.exception(f" -- {dpla_id}: legacy migration failed; skipping.")
                tracker.increment(Result.LEGACY_SKIPPED_ERROR)
        completed = True
    except BaseException as exc:
        logging.exception(
            "Legacy migration aborted with unhandled exception (%s)",
            type(exc).__name__,
        )
        raise
    finally:
        elapsed = time.time() - start_time
        if completed:
            logging.info("\n" + str(tracker))
            logging.info(f"{elapsed} seconds.")
            # No dedicated Slack notification helper yet — reuse the SDC
            # phase's `notify_sdc_complete` is wrong semantically (this
            # isn't an SDC sync), so just log to the local file. A
            # later phase can add a `notify_legacy_complete` if this
            # mode becomes routine.
        else:
            logging.warning(
                "Legacy migration aborted before completion; suppressing"
                " terminal COUNTS dump."
            )


def _migrate_one_dpla_item(s3, partner: str, dpla_id: str) -> None:
    """Inner handler for :func:`_run_legacy_migration_mode` — sidecar
    loading, provider resolution, and the per-file migration loop.

    The per-file exception boundary lives inside the loop here (not in
    the outer caller) so one bad Commons file inside a multi-ordinal
    item doesn't skip its siblings — same per-ordinal isolation
    pattern :func:`_run_partner_mode` uses. Item-level setup errors
    (missing sidecars, provider lookup failures) propagate up to the
    outer loop, which classifies them under the same tracker bucket
    but skips the whole item rather than individual files.
    """
    from ingest_wikimedia.dpla import DPLA
    from ingest_wikimedia.legacy_artwork import migrate_legacy_file

    item_raw = s3.get_item_metadata(partner, dpla_id)
    if not item_raw:
        logging.info(f" -- {dpla_id}: no dpla-map.json on S3; skipping.")
        tracker.increment(Result.LEGACY_SKIPPED_NOT_LEGACY)
        return
    item_metadata = json.loads(item_raw)

    upload_raw = s3.get_upload_result(partner, dpla_id)
    if not upload_raw:
        logging.info(f" -- {dpla_id}: no upload-result.json on S3; skipping.")
        tracker.increment(Result.LEGACY_SKIPPED_NOT_LEGACY)
        return
    upload_result = json.loads(upload_raw)

    provider, data_provider = DPLA.get_provider_and_data_provider(item_metadata, hubs)
    ordinals = upload_result.get("ordinals", {})
    if not isinstance(ordinals, dict):
        logging.warning(
            f" -- {dpla_id}: upload-result.json has non-mapping ordinals; skipping."
        )
        tracker.increment(Result.LEGACY_SKIPPED_ERROR)
        return

    eligible_titles = [
        data.get("title")
        for data in ordinals.values()
        if isinstance(data, dict)
        and data.get("status") in ("UPLOADED", "SKIPPED")
        and data.get("title")
    ]
    if not eligible_titles:
        tracker.increment(Result.LEGACY_SKIPPED_NOT_LEGACY)
        return

    for title in eligible_titles:
        # Per-file exception boundary: a transient pywikibot APIError on
        # one ordinal must not abort the remaining ordinals for this item.
        # Matches the per-ordinal pattern in _run_partner_mode. The
        # MissingEntity case is folded into NOT_LEGACY rather than ERROR
        # because the file is just gone, not a real failure.
        try:
            page = pywikibot.FilePage(site, title)
            if not page.exists():
                tracker.increment(Result.LEGACY_SKIPPED_NOT_LEGACY)
                continue
            result = migrate_legacy_file(
                file_page=page,
                item_metadata=item_metadata,
                provider=provider,
                data_provider=data_provider,
                dpla_id=dpla_id,
                site=site,
            )
        except _MissingEntityError:
            logging.info(
                f" -- '{title}' for {dpla_id}: MediaInfo entity missing; skipping."
            )
            tracker.increment(Result.LEGACY_SKIPPED_NOT_LEGACY)
            continue
        except Exception:
            logging.exception(
                f" -- '{title}' for {dpla_id}: legacy migration failed; skipping."
            )
            tracker.increment(Result.LEGACY_SKIPPED_ERROR)
            continue

        if result.skipped_reason == "no-legacy-template":
            tracker.increment(Result.LEGACY_SKIPPED_NOT_LEGACY)
        elif result.skipped_reason == "already-migrated":
            tracker.increment(Result.LEGACY_SKIPPED_ALREADY)
        else:
            tracker.increment(Result.LEGACY_MIGRATED)
            if result.imports_posted:
                tracker.increment(
                    Result.LEGACY_IMPORTS_POSTED, amount=result.imports_posted
                )
            logging.info(
                f" -- Migrated '{title}': {result.imports_posted} community"
                f" import(s), wikitext_changed={result.wikitext_changed}"
            )


def _run_pool(tasks, *, workers, initializer, initargs, task_fn):
    """Shared spawn-Pool scaffolding for the parallel sync paths.

    Routes worker log records to the parent's handlers (the open ``-sdc.log``)
    through a ``multiprocessing.Queue`` + ``QueueListener``, runs ``task_fn``
    over ``tasks`` with ``imap_unordered``, and merges each task's returned
    tracker delta into the parent ``tracker``. ``initargs`` are forwarded to
    ``initializer`` AFTER the log queue — every worker initializer takes
    ``log_queue`` as its first parameter.

    Uses ``spawn`` start_method explicitly so workers don't inherit the
    parent's pywikibot session sockets — fork-then-use of a live session has
    been a source of half-broken connections in similar bot setups.
    """
    import logging.handlers
    import multiprocessing

    ctx = multiprocessing.get_context("spawn")
    log_queue = ctx.Manager().Queue(-1)
    listener = logging.handlers.QueueListener(
        log_queue, *logging.getLogger().handlers, respect_handler_level=True
    )
    listener.start()
    try:
        with ctx.Pool(
            processes=workers,
            initializer=initializer,
            initargs=(log_queue, *initargs),
        ) as pool:
            for delta in pool.imap_unordered(task_fn, tasks):
                if delta:
                    tracker.merge(delta)
    finally:
        listener.stop()


def _run_partner_mode_parallel(partner, dpla_ids, workers):
    """Dispatch partner-mode SDC sync across ``workers`` processes.

    Each worker process runs :func:`_process_one_partner_item` against one DPLA
    item at a time via :func:`_run_pool`; the parent aggregates per-task counter
    deltas into the module-level tracker.

    Item-level safety: every ordinal of every item has a unique MediaInfo M-id,
    so two workers handling different items never write to the same Commons
    entity. The pywikibot ``Site`` is re-initialized per worker (see
    :func:`_init_partner_worker`), isolating HTTP sessions and CSRF tokens.

    ``initargs`` pickles the parent's already-fetched mapping tables (``hubs`` /
    ``rights`` / ``subject_ids``) across to each worker so the cleanup path
    (``DPLA.get_provider_and_data_provider`` with ``hubs``) doesn't NameError
    under spawn, and all workers + parent agree on one snapshot.
    """
    tasks = [
        (partner, dpla_id, idx, len(dpla_ids))
        for idx, dpla_id in enumerate(dpla_ids, start=1)
    ]
    _run_pool(
        tasks,
        workers=workers,
        initializer=_init_partner_worker,
        initargs=(
            hubs,
            rights,
            subject_ids,
            _normalize_wikitext_enabled,
            _workers_budget,
        ),
        task_fn=_worker_partner_task,
    )


def _init_partner_worker(
    log_queue,
    hubs_data,
    rights_data,
    subject_ids_data,
    normalize_wikitext_enabled,
    workers_budget,
):
    """Per-worker setup for the ``--workers > 1`` partner-mode Pool.

    Each multiprocessing worker process re-imports this module (with
    spawn start_method) and runs this initializer once. Four jobs:

    1. **Fresh pywikibot session per worker.** Each worker gets its
       own ``site``/login pair so HTTP sessions, CSRF tokens, and the
       maxlag-retry config are isolated. (Even with fork, inheriting
       the parent's already-opened sockets is a recipe for half-broken
       connections; we always re-login here.)

    2. **Inject the mapping tables the parent already fetched.**
       ``hubs``, ``rights``, and ``subject_ids`` are bound at parent-
       process ``_initialize()`` time from ingestion3 + the local
       ``rights.json``. With spawn start_method the worker re-imports
       this module fresh — those module globals are declared (lines
       44-46) but not bound, so first access from the cleanup path
       (``_post_sdc_cleanup_for_item`` → ``DPLA.get_provider_and_data_provider``
       at line 3744) would NameError. The parent pickles its already-
       fetched copies into ``initargs`` and the worker binds them on
       init, so cleanup works in parallel mode and all workers see
       identical data (no per-worker re-fetch drift between the parent
       and ingestion3).

    3. **Route log records to the parent.** Replace the root logger's
       handlers with a single ``QueueHandler`` that pushes records to
       the shared multiprocessing queue. The parent runs a
       ``QueueListener`` that drains records to the per-partner SDC
       log file. Keeps log lines atomic and ordered by completion
       time across workers — much safer than letting N processes
       open the same file descriptor.

    4. **Apply the parent's normalize-wikitext flag.** ``_workers``
       and ``_normalize_wikitext_enabled`` are also module globals
       the parent's ``_initialize()`` flips from ``args``; the
       worker's freshly-imported defaults (workers=1, normalize=True)
       happen to match production today, but we set
       ``_normalize_wikitext_enabled`` explicitly here so a future
       parent invocation with ``--no-normalize-wikitext`` doesn't
       silently re-enable the strip in workers.

    5. **Build the box-wide worker-slot budget.** ``workers_budget``
       is the parent's ``--workers-budget`` value; each worker
       constructs its own :class:`WorkerSlotBudget` pointed at the
       shared on-disk slot dir, so every worker across every session
       contends over the same flock'd slot files. ``budget <= 0``
       yields a disabled (no-op) budget.
    """
    import logging.handlers

    import pywikibot

    pywikibot.config.max_retries = _PYWIKIBOT_MAX_RETRIES
    pywikibot.config.retry_wait = _PYWIKIBOT_RETRY_WAIT
    pywikibot.config.retry_max = _PYWIKIBOT_RETRY_MAX

    global site, hubs, rights, subject_ids, _normalize_wikitext_enabled
    global _worker_slot_budget
    site = pywikibot.Site("commons", "commons")
    site.login()
    hubs = hubs_data
    rights = rights_data
    subject_ids = subject_ids_data
    _normalize_wikitext_enabled = bool(normalize_wikitext_enabled)
    _worker_slot_budget = WorkerSlotBudget(workers_budget)

    qh = logging.handlers.QueueHandler(log_queue)
    root = logging.getLogger()
    root.handlers = [qh]
    root.setLevel(logging.INFO)


def _worker_partner_task(args):
    """Pool worker entrypoint — snapshot the worker's tracker, run
    one item end-to-end, return the per-task counter delta.

    Pool workers are reused across many tasks; snapshotting before
    each task and returning the diff gives the parent only what THIS
    item contributed, so the merge into the parent's tracker is
    correct regardless of how many tasks each worker ends up handling.

    Top-level exception boundary mirrors the per-ordinal try/except
    inside ``_process_one_partner_item``: a routine SDC-write failure
    is already caught and counted as ``SDC_ORDINALS_SKIPPED_ERROR``
    in there; reaching this handler means something outside the loop
    raised. Catch and log so a single bad item doesn't kill the whole
    pool batch.
    """
    partner, dpla_id, idx, total = args
    prior = tracker.snapshot()
    wait_before = _worker_slot_budget.total_wait_seconds
    try:
        # Workers create their own S3Client lazily on first use; the
        # parent's instance can't cross the process boundary cleanly
        # (boto3 sessions hold sockets / credentials state).
        s3 = _get_partner_s3_client()
        # Check out one box-wide slot for the duration of this item.
        # The budget caps how many items sync concurrently across all
        # sessions (= a loose, item-granular proxy for concurrent write
        # streams — see worker_slots.py for why per-item, not per-write).
        # When the budget is disabled (workers_budget <= 0) this is a
        # no-op. Acquiring once around the whole item — through the fast
        # S3 reads and all the per-ordinal writes — keeps the acquire in
        # one place; pywikibot's per-worker maxlag backoff remains the
        # real per-write safety net.
        with _worker_slot_budget.acquire():
            _process_one_partner_item(s3, partner, dpla_id, idx, total)
    except CsrfRecoveryFailed:
        # Session-level fatal — propagate to _run_partner_mode_parallel's
        # future-collection loop so main() ends the run rather than
        # every worker looping the same auth failure. Mirrors uploader's
        # CSRF abort contract (PR #350).
        raise
    except Exception:
        logging.exception(
            "Worker task crashed processing %s (idx %s/%s) in partner %s",
            dpla_id,
            idx,
            total,
            partner,
        )
        # Count items that crash before the inner per-ordinal handler runs
        # (e.g. an acquire()/setup fault); otherwise they drop silently from
        # the tally while the per-ordinal handler owns the routine failures.
        tracker.increment(Result.SDC_ITEMS_SKIPPED_ERROR)
    # Fold this task's slot-wait into the tracker so the parent aggregates
    # contention across all workers (worker-seconds). Diff the *floored*
    # cumulative wait rather than rounding the per-task delta: whole-second
    # boundary crossings telescope to int(total), so sustained sub-second
    # waits aren't each rounded away to 0.
    wait_delta = int(_worker_slot_budget.total_wait_seconds) - int(wait_before)
    if wait_delta:
        tracker.increment(Result.SDC_SLOT_WAIT_SECONDS, wait_delta)
    return tracker.diff(prior)


def _get_partner_s3_client():
    """Lazy per-worker S3Client. Cached on the module-level
    ``_s3_client`` so subsequent tasks in the same worker reuse the
    same boto3 session."""
    global _s3_client
    if _s3_client is None:
        from ingest_wikimedia.s3 import S3Client

        _s3_client = S3Client()
    return _s3_client


def _process_one_partner_item(s3, partner, dpla_id, idx, total):
    """Process one DPLA item end-to-end in partner mode — read S3
    sidecars, drive per-ordinal SDC sync against Commons, run the
    post-SDC cleanup.

    Extracted from :func:`_run_partner_mode`'s for-loop body so the
    body can be dispatched to worker processes by a
    ``multiprocessing.Pool`` when ``--workers > 1``. Uses module-
    level state (``tracker``, ``site``, accumulators,
    ``_entity_cache``) — at workers=1 these are the parent's; under
    a Pool each worker process has its own copy. Returns ``None`` on
    every path; failures are tracked via the module-level Tracker,
    not raised.
    """
    from botocore.exceptions import ClientError

    # Item-start marker — `wikimedia_upload_status._sdc_progress`
    # greps for this to surface SDC progress.
    logging.info(f"DPLA ID: {dpla_id} ({idx}/{total})")

    # S3Client.get_item_file returns None on 404/NoSuchKey but
    # re-raises any other ClientError. Catch those per-item so one
    # transient S3 failure doesn't abort the whole partner batch.
    try:
        sdc_raw = s3.get_sdc_json(partner, dpla_id)
    except ClientError as e:
        logging.warning(
            f" -- S3 error reading sdc.json for {partner}/{dpla_id}: {e!r}; skipping."
        )
        tracker.increment(Result.SDC_ITEMS_SKIPPED_NO_SIDECAR)
        return
    if sdc_raw is None:
        logging.info(" -- No sdc.json on S3; skipping.")
        tracker.increment(Result.SDC_ITEMS_SKIPPED_NO_SIDECAR)
        return
    try:
        sdc_payload = json.loads(sdc_raw)
    except json.JSONDecodeError as e:
        logging.warning(f" -- sdc.json failed to parse: {e}; skipping.")
        tracker.increment(Result.SDC_ITEMS_SKIPPED_MAPPING)
        return

    try:
        upload_raw = s3.get_upload_result(partner, dpla_id)
    except ClientError as e:
        logging.warning(
            f" -- S3 error reading upload-result.json for {partner}/{dpla_id}:"
            f" {e!r}; skipping."
        )
        tracker.increment(Result.SDC_ITEMS_SKIPPED_NO_SIDECAR)
        return
    if upload_raw is None:
        logging.info(" -- No upload-result.json on S3; skipping.")
        tracker.increment(Result.SDC_ITEMS_SKIPPED_NO_SIDECAR)
        return
    try:
        upload_result = json.loads(upload_raw)
    except json.JSONDecodeError as e:
        logging.warning(f" -- upload-result.json failed to parse: {e}; skipping.")
        tracker.increment(Result.SDC_ITEMS_SKIPPED_MAPPING)
        return

    # file-list.txt maps ordinal-1 (zero-indexed) → download URL.
    # Used to populate the per-ordinal P2699 qualifier on the
    # P7482 (described at) claim — every file gets one, but the
    # URL differs per ordinal so it can't live in the per-item
    # sdc.json. A missing or empty file-list.txt is non-fatal:
    # P2699 simply isn't materialized for those ordinals (the
    # rest of the SDC pass still runs).
    try:
        file_list = s3.get_file_list(partner, dpla_id)
    except ClientError as e:
        logging.warning(
            f" -- S3 error reading file-list.txt for {partner}/{dpla_id}: {e!r};"
            " continuing without P2699 qualifiers."
        )
        file_list = []

    ordinals = upload_result.get("ordinals", {})
    # Guard the type of `ordinals` before iteration — if the JSON
    # sidecar is corrupt or its schema drifts (null, list, scalar),
    # `ordinals.items()` would raise out of the whole loop and abort
    # the partner batch. Same handling as any other mapping error.
    if not isinstance(ordinals, dict):
        logging.warning(
            f" -- upload-result.json has non-mapping `ordinals` for {dpla_id}; skipping."
        )
        tracker.increment(Result.SDC_ITEMS_SKIPPED_MAPPING)
        return
    eligible: dict[str, dict] = {}
    unresolved_ords: list[str] = []
    for ord_str, data in ordinals.items():
        if not isinstance(data, dict):
            continue
        if data.get("status") in ("UPLOADED", "SKIPPED"):
            eligible[ord_str] = data
        else:
            # NOT_PRESENT / INELIGIBLE / FAILED — the upload phase
            # couldn't confirm this ordinal in *this* run, but the
            # file may exist on Commons from a prior run. Defer to
            # the post-loop Commons discovery pass below.
            unresolved_ords.append(ord_str)

    # Data-side phases (SDC sync, template migration) operate on
    # the Commons file, which is independent of whether the
    # current run successfully refreshed the S3 binary. When the
    # upload phase left ordinals as NOT_PRESENT/INELIGIBLE/
    # FAILED, search Commons by DPLA-ID for any existing files
    # and graft them onto ``eligible``. Lets a transient binary-
    # side failure (broken upstream URL, S3 hiccup) NOT block
    # data-side maintenance of files that are already on Commons.
    #
    # Lazy: the search only fires if at least one ordinal needs
    # rescuing — healthy items where everything UPLOADED/SKIPPED
    # this run pay zero extra API cost.
    if unresolved_ords:
        discovered = _find_existing_commons_files_by_dpla_id(dpla_id)
        rescued = 0
        for ord_str in unresolved_ords:
            found = discovered.get(ord_str)
            if not found:
                continue
            # Preserve the original status string for traceability
            # (Slack summaries / logs can still distinguish "this
            # run didn't upload" from "this run uploaded fresh"),
            # but inject the discovered title+pageid so the
            # per-ordinal sync path treats the file as syncable.
            eligible[ord_str] = {
                **ordinals[ord_str],
                "title": found["title"],
                "pageid": found["pageid"],
                "discovered_via_dpla_id": True,
            }
            rescued += 1
        if rescued:
            logging.info(
                f" -- Recovered {rescued} ordinal(s) via Commons "
                f"intitle:{dpla_id} discovery (upload phase reported "
                "non-eligible status but file exists on Commons)."
            )

    if not eligible:
        logging.info(" -- No SDC-eligible ordinals; skipping.")
        tracker.increment(Result.SDC_ITEMS_SKIPPED_MAPPING)
        return

    # Per-ordinal sync tracking. ``synced_ord_strs`` records
    # which ordinals' ``process_one_from_sdc`` returned without
    # raising — i.e. the ordinals the cleanup pass *should*
    # touch. Filtering on this set when calling
    # ``_post_sdc_cleanup_for_item`` keeps cleanup from
    # running against ordinals that were skipped earlier in
    # this same item (null pageid, missing-entity skip, etc.) —
    # the defensive entity-guard inside ``_post_sdc_cleanup_for_page``
    # would refuse the strip anyway, but routing past it
    # avoids the wasted entity fetch and keeps the contract
    # explicit. ``synced_this_item`` derives from the set
    # below; classification is unchanged.
    synced_ord_strs: set[str] = set()
    # Per-item set of ord_strs whose Commons file page actually
    # received an edit this run — populated from the SDC-write
    # snapshot below (claim/ref/removal writes) and unioned with
    # the set returned by post-SDC cleanup. Drives
    # ``SDC_PAGES_EDITED``, so operators can read the real batch
    # size off the Slack summary instead of inferring it from
    # ``ITEMS SYNCED`` (which collapses 1-file and 1,000-file
    # items into the same count).
    pages_edited: set[str] = set()
    # Tracks whether any ordinal hit the per-ordinal exception
    # path (runtime failure) so an item with all-failed ordinals
    # is classified under SDC_ITEMS_SKIPPED_ERROR rather than
    # SDC_ITEMS_SKIPPED_MAPPING — they mean different things and
    # operators read the Slack summary to distinguish bad data
    # from bad network/API.
    had_ordinal_error = False
    # int(ord_str) on a malformed ordinal key (e.g. "abc" instead
    # of "3") would otherwise raise out of the whole loop and
    # abort the partner batch. Skip the item, log, and account
    # for it as a mapping skip — same handling as malformed JSON.
    try:
        ordinal_items = sorted(eligible.items(), key=lambda kv: int(kv[0]))
    except (TypeError, ValueError):
        # Surface the offending key so operators can trace the
        # data-quality issue back to upload-result.json. The sort
        # key raises on the first int() that fails, but Python's
        # sorted() doesn't expose which one — so we re-scan the
        # keys here purely to find the culprit for logging.
        bad_keys = [repr(k) for k in eligible if not str(k).lstrip("-").isdigit()]
        logging.warning(
            f" -- upload-result ordinals malformed for {dpla_id}"
            f" (non-integer key(s): {', '.join(bad_keys) or '<unknown>'});"
            " skipping."
        )
        tracker.increment(Result.SDC_ITEMS_SKIPPED_MAPPING)
        return

    # Compute per-ordinal P304 (page-number) values for any
    # multi-file extension group. Single-file groups (one JPG,
    # one PDF, etc.) are absent from the map — `page_numbers.get(
    # ord_str)` returns None for them and process_one_from_sdc
    # skips the qualifier. JPGs and PDFs are numbered as
    # independent series per the user-confirmed rule.
    page_numbers = _compute_page_numbers(ordinal_items)

    for ord_str, data in ordinal_items:
        pageid = data.get("pageid")
        title = data.get("title", "?")
        # `if not pageid` rather than `is None` — a recorded
        # pageid of 0 is just as malformed as a missing one
        # (no Commons MediaInfo entity has ID `M0`) and would
        # otherwise propagate downstream as a confusing
        # pywikibot APIError on the bogus mediaid. The
        # uploader has historically written ``pageid: 0`` (and
        # since 2026-06 ``pageid: null``) for successful new
        # uploads when pywikibot's FilePage cache wasn't
        # invalidated post-upload.
        #
        # When ``title`` is present (which it almost always is —
        # the uploader writes both fields), fall back to a
        # Commons API lookup keyed on the title to recover the
        # real pageid. Lets a re-run of ``sdc-sync`` self-heal
        # past the upstream sidecar defect instead of silently
        # repeating the same skip. ``_resolve_pageid_from_title``
        # returns ``None`` on any failure (page deleted, API
        # error) — in that case the original skip path runs.
        if not pageid and title and title != "?":
            resolved = _resolve_pageid_from_title(title)
            if resolved:
                logging.info(
                    f" -- Ordinal {ord_str}: upload-result pageid"
                    f" was {data.get('pageid')!r}; resolved to"
                    f" {resolved} via Commons title lookup."
                )
                pageid = resolved
        if not pageid:
            logging.warning(
                f" -- Ordinal {ord_str}: missing/zero pageid"
                f" ({data.get('pageid')!r}) for '{title}' and"
                " title→pageid fallback failed; skipping."
            )
            tracker.increment(Result.SDC_ORDINALS_SKIPPED_MISSING_PAGEID)
            # Treat as an ordinal error for item-level bucket
            # classification — an item where every ordinal had
            # null pageid should not silently fall into the
            # MAPPING bucket; it's a real failure that needs
            # the upstream uploader fix.
            had_ordinal_error = True
            continue
        mediaid = f"M{pageid}"
        logging.info(f" -- Ordinal {ord_str}: {mediaid} ({title})")

        # Snapshot write counters so we can detect whether this
        # ordinal's sync actually changed anything on Commons.
        writes_before = _sdc_writes_total()
        # Per-ordinal exception boundary. Without this, any
        # transient pywikibot APIError (rate limit, maxlag,
        # transient 503), network timeout, or surprise
        # KeyError/AssertionError deep in the property-builder
        # propagates up through both nested loops and aborts
        # the entire partner batch — losing thousands of items'
        # worth of work because of one bad page. Other failure
        # modes in this function (S3 ClientError, JSON parse,
        # malformed ordinals) already skip-and-continue; this
        # makes the actual SDC write follow the same pattern.
        # `logging.exception` writes the full traceback into
        # the SDC log file so notify_pipeline_fail's
        # `_summarize_log` can surface it in Slack.
        # Look up this ordinal's download URL from file-list.txt
        # (zero-indexed; ord_str is "1"-indexed). Used to stamp the
        # per-ordinal P2699 qualifier on the P7482 statement.
        # Explicit range guard — Python's negative indexing would
        # silently grab the LAST entry for ord_str == "0", planting
        # the wrong URL on a real Commons claim via wbsetqualifier.
        try:
            ord_num = int(ord_str)
        except ValueError:
            download_url = None
        else:
            if 1 <= ord_num <= len(file_list):
                download_url = file_list[ord_num - 1] or None
            else:
                download_url = None
        try:
            process_one_from_sdc(
                mediaid,
                dpla_id,
                sdc_payload,
                download_url=download_url,
                page_number=page_numbers.get(ord_str),
            )
        except _MissingEntityError:
            # Commons says the MediaInfo entity at this M-id doesn't
            # exist. Almost always means the file page was deleted
            # (often by a Commons curator as a duplicate) between
            # upload and SDC sync, OR this is an SDC-only run for a
            # file that wasn't uploaded through our pipeline in this
            # run. Either way it's outside the SDC phase's remit —
            # not an error, just a clean skip. Tracked separately
            # from real errors so operators can tell them apart.
            logging.info(
                f" -- Ordinal {ord_str} ({mediaid}) for {dpla_id}:"
                " Commons MediaInfo entity does not exist; skipping"
                " ordinal (not an error)."
            )
            tracker.increment(Result.SDC_ORDINALS_SKIPPED_MISSING_ENTITY)
            continue
        except CsrfRecoveryFailed:
            # Session-level fatal — propagate past the per-ordinal
            # catch so the whole run aborts rather than looping the
            # same unrecoverable auth error against every remaining
            # ordinal. Mirrors the uploader's CSRF abort contract
            # (PR #350) and the Toledo 2026-06-25 lesson.
            raise
        except Exception:
            logging.exception(
                f" -- Ordinal {ord_str} ({mediaid}) for {dpla_id}:"
                " SDC sync failed; skipping ordinal."
            )
            tracker.increment(Result.SDC_ORDINALS_SKIPPED_ERROR)
            had_ordinal_error = True
            continue
        writes_after = _sdc_writes_total()

        # If SDC actually changed for this ordinal, force a
        # category re-render on the file page via a null edit.
        # MediaWiki caches categories at render time; SDC writes
        # don't propagate to the page's category list until the
        # wikitext is re-evaluated. Without this the maintenance
        # categories like "Digital Public Library of America
        # files missing required SDC statements" cling on long
        # after the SDC was added. Mirrors the touch pattern in
        # ingest_wikimedia/categories.py's
        # touch_files_for_institution().
        if writes_after > writes_before:
            pages_edited.add(ord_str)
            if title and title != "?":
                try:
                    with_csrf_recovery(
                        site,
                        f"touch File:{title}",
                        pywikibot.FilePage(site, title).touch,
                    )
                    logging.info(f" -- Touched '{title}' (category refresh).")
                except CsrfRecoveryFailed:
                    # Session refresh exhausted — same abort contract as
                    # the uploader: propagate past every per-item catch so
                    # main() ends the run rather than looping the same
                    # unrecoverable error against every remaining ordinal.
                    raise
                except Exception as e:
                    logging.warning(
                        f" -- Failed to touch '{title}' for category refresh: {e!r}"
                    )
        synced_ord_strs.add(ord_str)
    # Item-level bucket classification + cleanup dispatch.
    # ``_classify_item_outcome`` returns the bucket; cleanup
    # runs for any progress-made outcome (full or partial),
    # against ONLY the ordinals that synced — partial-sync
    # items must not retry cleanup on the ordinals that
    # were skipped earlier in this loop (null pageid,
    # missing entity, etc.).
    outcome = _classify_item_outcome(bool(synced_ord_strs), had_ordinal_error)
    tracker.increment(outcome)
    if (
        outcome
        in (
            Result.SDC_ITEMS_SYNCED,
            Result.SDC_ITEMS_PARTIALLY_SYNCED,
        )
        and _normalize_wikitext_enabled
    ):
        synced_items = [(o, d) for o, d in ordinal_items if o in synced_ord_strs]
        pages_edited |= _post_sdc_cleanup_for_item(s3, partner, dpla_id, synced_items)
    if pages_edited:
        tracker.increment(Result.SDC_PAGES_EDITED, len(pages_edited))


def _enumerate_maintain_groups(generator, limit):
    """Walk the ``--cat`` page generator once and bucket files by embedded DPLA
    id for parallel maintain.

    Returns a list of groups, each a list of ``(title, pageid, embedded_id)``
    for files sharing one embedded id. Grouping keeps an item's multi-page set
    on a single worker (see :func:`_run_maintain_parallel`). ``limit`` caps the
    total files enumerated (``--limit``). Embedded ids are resolved here in the
    single-threaded parent (``_resolve_dpla_id`` — title-pattern only for
    service hubs, so effectively no api.dp.la calls).
    """
    groups: dict[str, list] = {}
    count = 0
    for page in generator:
        title = page.title()
        embedded_id = _resolve_dpla_id(title, dpla_api)
        groups.setdefault(embedded_id, []).append((title, page.pageid, embedded_id))
        count += 1
        if limit and count >= limit:
            print(f" -- Reached --limit {limit}, stopping enumeration.")
            break
    return list(groups.values())


def _init_maintain_worker(
    log_queue,
    hubs_data,
    s3_partner,
    normalize_wikitext_enabled,
    workers_budget,
):
    """Per-worker setup for parallel maintain — mirrors
    :func:`_init_partner_worker`: a fresh pywikibot session, the parent's
    ``hubs`` snapshot (anchor-3 QID scope map + post-SDC cleanup provider
    lookup), the ``--from-s3`` partner key and an S3 client for sidecar reads,
    the normalize flag, the box-wide slot budget, and log routing to the parent.

    No ``dpla_api`` is injected: embedded ids are precomputed by the parent and
    carried in each group tuple, and the sync reads claims from the staged
    sidecar — neither path calls ``api.dp.la``.
    """
    import logging.handlers

    import pywikibot

    pywikibot.config.max_retries = _PYWIKIBOT_MAX_RETRIES
    pywikibot.config.retry_wait = _PYWIKIBOT_RETRY_WAIT
    pywikibot.config.retry_max = _PYWIKIBOT_RETRY_MAX

    global site, hubs, _s3_partner, _s3_client
    global _normalize_wikitext_enabled, _worker_slot_budget
    site = pywikibot.Site("commons", "commons")
    site.login()
    hubs = hubs_data
    _s3_partner = s3_partner
    _normalize_wikitext_enabled = bool(normalize_wikitext_enabled)
    _worker_slot_budget = WorkerSlotBudget(workers_budget)
    if s3_partner is not None:
        from ingest_wikimedia.s3 import S3Client

        _s3_client = S3Client()

    qh = logging.handlers.QueueHandler(log_queue)
    root = logging.getLogger()
    root.handlers = [qh]
    root.setLevel(logging.INFO)


def _worker_maintain_group_task(group):
    """Pool worker entrypoint for parallel maintain: process one id-group (all
    files sharing an embedded DPLA id) serially in this worker, returning the
    per-group tracker delta.

    Snapshot/diff gives the parent only what this group contributed (workers
    are reused across groups). One box-wide slot is held for the whole group —
    matching partner mode's per-item slot — and a per-file try/except keeps one
    bad page from dropping the rest of the group.
    """
    prior = tracker.snapshot()
    wait_before = _worker_slot_budget.total_wait_seconds
    try:
        with _worker_slot_budget.acquire():
            for title, pageid, embedded_id in group:
                try:
                    file_page = pywikibot.FilePage(site, title)
                    _maintain_process_file(
                        "M" + str(pageid), embedded_id, file_page, title
                    )
                except Exception:
                    logging.exception("maintain: worker failed on %s", title)
                    tracker.increment(Result.SDC_ITEMS_SKIPPED_ERROR)
    except Exception:
        logging.exception("maintain: worker group setup failed (%d files)", len(group))
        tracker.increment(Result.SDC_ITEMS_SKIPPED_ERROR)
    wait_delta = int(_worker_slot_budget.total_wait_seconds) - int(wait_before)
    if wait_delta:
        tracker.increment(Result.SDC_SLOT_WAIT_SECONDS, wait_delta)
    return tracker.diff(prior)


def _maintain_parallel_enabled(maintain, workers, count_only, from_s3_partner):
    """Whether the ``--cat`` maintain run should fan out to the worker pool.

    Requires --maintain, --workers > 1, NOT --count-only (read-only pre-flight
    sizing stays serial), and --from-s3 (``from_s3_partner``). The --from-s3
    requirement is load-critical: without staged sidecars,
    :func:`_maintain_process_file` falls back to the live :func:`process_one`,
    which needs ``dpla_api`` / ``rights`` / ``subject_ids`` (not injected into
    workers) and would hit ``api.dp.la`` once per file — at N-way concurrency
    that is exactly the load the precomputed-sidecar path exists to avoid. With
    no sidecars the caller drops to the serial path, where that single-file
    fallback runs once, in the parent, with the globals bound.
    """
    return bool(
        maintain and workers > 1 and not count_only and from_s3_partner is not None
    )


def _run_maintain_parallel(groups, workers):
    """Dispatch parallel maintain across ``workers`` processes, one id-group per
    task via :func:`_run_pool`.

    Concurrency safety: the canonical title embeds the DPLA id, so two files can
    only collide on a destination title if they share an id — and grouping puts
    every such file on the same worker, serializing those renames. The only
    residual (two different embedded ids that re-link to the SAME current id — a
    drifted duplicate) degrades safely: the second move hits
    ``ArticleExistsConflictError`` → ``MAINTAIN_RENAME_BLOCKED``, the
    dedup-deferral behavior we want anyway. CommonsDelinker posts are append-only
    and already race-safe.
    """
    _run_pool(
        groups,
        workers=workers,
        initializer=_init_maintain_worker,
        initargs=(
            hubs,
            _s3_partner,
            _normalize_wikitext_enabled,
            _workers_budget,
        ),
        task_fn=_worker_maintain_group_task,
    )


def _run_partner_mode(partner, ids_file):
    """Drive the SDC phase from precomputed S3 sidecars for a whole partner.

    For each DPLA ID in `ids_file`:
      * Read `sdc.json` (staged by get-ids-es). Skip if absent — item
        couldn't be parsed, or get-ids-es hasn't run for this partner yet.
      * Read `upload-result.json` (written by uploader). Skip if absent —
        uploader hasn't processed this item, so we don't know which
        ordinals exist on Commons.
      * For each ordinal whose status is UPLOADED or SKIPPED, derive
        `M<pageid>` and call `process_one_from_sdc(mediaid, dpla_id, sdc)`.

    Items where the metadata isn't yet on S3 are skipped silently and
    will be picked up the next time the full pipeline runs.

    Logs to `{partner}/logs/{timestamp}-{label}-sdc.log` (matching the
    downloader/uploader pattern) so `wikimedia-upload-status` can detect
    progress; final summary posted via `notify_sdc_complete`.
    """

    from ingest_wikimedia.s3 import S3Client

    setup_logging(partner, "sdc", logging.INFO)
    # Post the phase-start notification immediately after setup_logging so
    # the operator sees that the SDC phase has actually begun — matching
    # the get-ids-es / downloader / uploader convention. Without this,
    # the gap between the upload-complete message and the eventual
    # sdc-complete summary can stretch hours on a large hub with no
    # indication that work has moved on.
    notify_phase_start(partner, "sdc-sync")
    start_time = time.time()

    # Reset the module-level tracker so per-partner counts don't carry
    # over across invocations of `_run_partner_mode` within a single
    # process (e.g. tests, future multi-partner runs).
    tracker.reset()

    s3 = S3Client()

    with open(ids_file) as f:
        dpla_ids = [line.strip() for line in f if line.strip()]

    workers = _workers
    logging.info(
        f"Partner mode: {partner} — {len(dpla_ids)} items from {ids_file}"
        f" (workers={workers})"
    )
    completed = False
    try:
        if workers <= 1:
            # Single-process: parent's module-level tracker is mutated
            # in-place; no Pool, no logging queue, no delta merge. Still
            # acquires one box-wide slot per item so a 1-worker session
            # counts against --workers-budget like the parallel path
            # (no-op when the budget is 0, so a plain run is unchanged).
            slot_budget = WorkerSlotBudget(_workers_budget)
            for local_count, dpla_id in enumerate(dpla_ids, start=1):
                with slot_budget.acquire():
                    _process_one_partner_item(
                        s3, partner, dpla_id, local_count, len(dpla_ids)
                    )
            # One process, so its accumulated wait IS the session total.
            slot_wait = int(slot_budget.total_wait_seconds)
            if slot_wait:
                tracker.increment(Result.SDC_SLOT_WAIT_SECONDS, slot_wait)
        else:
            _run_partner_mode_parallel(partner, dpla_ids, workers)
        completed = True
    except BaseException as exc:
        # The per-ordinal try/except above already swallows every routine
        # SDC-write failure. Reaching this outer handler means something
        # truly outside-the-loop went wrong (sidecar enumeration, S3 auth,
        # logger state, etc.). Log the traceback into the SDC log file so
        # notify_pipeline_fail's `_summarize_log` can include it in the
        # Slack failure message — otherwise the traceback only hits stderr
        # (the file handler doesn't capture it) and the operator sees an
        # abort warning with no diagnostic.
        #
        # Widened to BaseException to capture SystemExit and KeyboardInterrupt
        # too.  A cluster of 11 SDC aborts in May 2026 (two within 3 seconds
        # of each other across two unrelated processes) all wrote the abort
        # warning but no traceback — meaning a non-Exception class escaped
        # this handler.  `except Exception` doesn't catch SystemExit /
        # KeyboardInterrupt / GeneratorExit, so those bypassed the original
        # log line entirely.  Widening to BaseException + re-raising
        # preserves the original semantics (the shell-level pipeline still
        # sees a non-zero exit and `notify_pipeline_fail` still fires) while
        # making the next abort self-diagnosing.
        logging.exception(
            "SDC sync aborted with unhandled exception (%s)", type(exc).__name__
        )
        raise
    finally:
        elapsed = time.time() - start_time
        # Emit the terminal "COUNTS:" marker and Slack completion message
        # only on a successful loop completion. The shell-level failure
        # handler (`notify_pipeline_fail`) will surface aborted runs via a
        # separate `❌ pipeline step failed` message, so on the failure
        # path we'd otherwise double-signal — and worse, the status script
        # would treat the SDC phase as done based on the spurious COUNTS:
        # line.
        if completed:
            logging.info("\n" + str(tracker))
            logging.info(f"{elapsed} seconds.")
            notify_sdc_complete(
                tracker=tracker,
                partner_label=partner,
                elapsed_seconds=elapsed,
                workers=workers,
            )
        else:
            logging.warning(
                "SDC sync aborted before completion; suppressing terminal "
                "COUNTS dump and notify_sdc_complete call."
            )


# We can use a PWB generator to programatically make the list of files we are working on based on a set of criteria. Here, we are generating the page titles from a Wikimedia Commons search and categories. For other types of available page generators, see <https://doc.wikimedia.org/pywikibot/master/api_ref/pywikibot.html#module-pywikibot.pagegenerators>. As an additional step, we take the pageid provided by the generator and prepend "M" for the mediaid needed for posting SDC statements.


def _safe_process_one(
    mediaid: str,
    dpla_id: str,
    file_page=None,
    sdc_payload=None,
    page_number=None,
) -> None:
    """Run ``process_one`` with the per-file exception boundary the
    legacy ``--list`` / ``--files`` / ``--cat`` loops need.

    When ``sdc_payload`` is supplied (maintain mode against staged S3
    sidecars — see the ``--cat``/``--file`` loops), the precomputed
    :func:`process_one_from_sdc` path is used instead of the live
    :func:`process_one`: no ``api.dp.la`` call and no runtime claim build,
    and ``page_number`` (parsed from the file title) materialises the P304
    page qualifier. With no payload it runs the live ``process_one`` exactly
    as before.

    Without this, a transient pywikibot APIError on any one file
    aborts the entire loop — and on ``--list``, leaves WORKING-*.txt
    unrenamed so the rest of the manifest is silently abandoned.
    Mirrors ``_run_partner_mode``'s per-ordinal boundary at
    line ~2294.

    Catches ``_MissingEntityError`` separately because ``process_one``
    pre-warms via ``get_entity`` BEFORE its own internal handler at
    line 2015 — so a deleted file's ``_MissingEntityError`` bypasses
    that handler and surfaces here. Categorise it as MISSING_ENTITY
    (clean skip) instead of folding it into the generic ERROR bucket.

    When ``file_page`` is supplied and ``--normalize-wikitext`` is on
    (the default — see ``_build_parser``), runs the post-SDC strip /
    legacy-migrate dispatcher on that page after the SDC sync. Mirrors
    the partner-mode cleanup so every trigger path (hub-level,
    per-institution, per-collection, single-id, ``--file``, ``--cat``,
    ``--list``) ends with the same per-file cleanup.
    """
    # Snapshot SDC write counters so we can tell whether ``process_one``
    # actually committed any change for this file. Mirrors the partner-
    # mode pattern at the per-ordinal level so SDC_PAGES_EDITED is fed
    # consistently across modes.
    writes_before = _sdc_writes_total()
    try:
        try:
            if sdc_payload is not None:
                process_one_from_sdc(
                    mediaid, dpla_id, sdc_payload, page_number=page_number
                )
            else:
                process_one(mediaid, dpla_id)
        except _MissingEntityError:
            logging.info(
                f" -- {mediaid} for {dpla_id}: Commons MediaInfo entity"
                " does not exist; skipping (not an error)."
            )
            tracker.increment(Result.SDC_ORDINALS_SKIPPED_MISSING_ENTITY)
            return
        except CsrfRecoveryFailed:
            # Session-level fatal — abort the whole run rather than
            # looping the same unrecoverable auth error against every
            # remaining file. Mirrors uploader's CSRF abort (PR #350).
            raise
        except Exception:
            logging.exception(
                f" -- {mediaid} for {dpla_id}: SDC sync failed; skipping."
            )
            tracker.increment(Result.SDC_ORDINALS_SKIPPED_ERROR)
            return

        sdc_wrote = _sdc_writes_total() > writes_before
        cleanup_wrote = False
        if _normalize_wikitext_enabled and file_page is not None:
            cleanup_wrote = _post_sdc_cleanup_for_legacy_mode(file_page, dpla_id)
        if sdc_wrote or cleanup_wrote:
            tracker.increment(Result.SDC_PAGES_EDITED)
    finally:
        # Drop any stash ``parsed()`` left in the cache so a process_one
        # error path (or a cleanup skip when ``file_page`` is None /
        # normalize-wikitext disabled) doesn't leak the doc across files.
        # On the happy path the cleanup helper already popped — the
        # ``default=None`` makes this a no-op there.
        _legacy_mode_doc_cache.pop(dpla_id, None)


def main() -> None:
    """Entry point for the `sdc-sync` console script.

    Dispatches to the legacy livecat/list/file/cat modes or the PR-4 partner
    mode based on the parsed CLI flags. Initialization (argparse, Commons
    login, ingestion3 JSON fetch) lives in `_initialize()`.
    """
    _initialize()

    # Maintain runs through the legacy --cat/--file dispatch, which (unlike
    # _run_partner_mode) never set up file logging — so a launched maintain
    # session wrote no `-sdc.log`, leaving the status poller
    # (wikimedia_upload_status.py) and the terminal COUNTS:/DPLA ID: markers
    # with nothing to read. Set up the same "sdc"-phase log here so a maintain
    # run reports through the identical status surface as a partner-mode run.
    if args.maintain:
        setup_logging(_s3_partner or "maintain", "sdc", logging.INFO)
        # Announce the SDC phase (as _run_partner_mode does) so a launched
        # maintain run surfaces in Slack instead of going silent until the
        # final summary. The lite --cat/--file path is the only one that
        # reached SDC work without this. Count-only is a read-only sizing
        # pre-flight, so it stays quiet.
        if not args.count_only:
            notify_phase_start(_s3_partner or "maintain", "sdc-sync")

    count = 0
    start_time = time.time()

    if method == "list":
        ltotal = [i for i in os.listdir(args.lists) if ".txt" in i]
        lists = [i for i in ltotal if "COMPLETE" not in i and "WORKING" not in i]
        percent = 100 * (len(ltotal) - len(lists)) / len(ltotal) if ltotal else 0
        while lists:
            # range(0, len(lists)-1) is exclusive on the upper bound, so the
            # previous expression never selected the last index — fixed.
            x = random.randrange(len(lists))
            working_file = os.path.join(args.lists, "WORKING-" + lists[x])
            print(working_file)
            os.rename(os.path.join(args.lists, lists[x]), working_file)

            files = pagegenerators.TextIOPageGenerator(working_file)

            for file in files:
                count += 1
                print(f"{count}:\n - {args.lists}/{lists[x]} ({percent:.2f}% done)")
                print("\n" + str(file).replace('""', '"'))
                # Re-wrap as a FilePage so the post-SDC cleanup gets
                # the right page-type handle. TextIOPageGenerator yields
                # generic Page objects, but normalize_page / migrate
                # both expect a FilePage (its .latest_file_info etc.).
                file_page = pywikibot.FilePage(site, str(file))
                mediaid = "M" + str(file.pageid)
                dpla_id = _resolve_dpla_id(str(file), dpla_api)
                _safe_process_one(mediaid, dpla_id, file_page=file_page)

            os.rename(working_file, os.path.join(args.lists, "COMPLETE-" + lists[x]))

            ltotal = [i for i in os.listdir(args.lists) if ".txt" in i]
            lists = [i for i in ltotal if "COMPLETE" not in i and "WORKING" not in i]
            percent = 100 * (len(ltotal) - len(lists)) / len(ltotal) if ltotal else 0

            duduped = set()
            try:
                with open("Missing ids.txt", "r") as f:
                    for line in f:
                        duduped.add(line.strip())
                with open("Missing ids.txt", "w") as f:
                    f.write("\n".join(duduped) + "\n")
            except FileNotFoundError:
                # No missing IDs recorded this batch; nothing to dedupe.
                pass

    elif args.files:
        # ``--count-only`` is a maintain-mode pre-flight: tally how each file
        # would re-link without writing anything.
        tally = _new_maintain_tally(args.maintain and args.count_only)
        for title in args.files:
            print("\n" + title)
            page = pywikibot.FilePage(site, title)
            if not page.exists():
                print(f" -- Page not found on Commons: {title}")
                continue
            mediaid = "M" + str(page.pageid)
            embedded_id = _resolve_dpla_id(title, dpla_api)
            count += 1
            print(f"{count}: {mediaid}")
            if args.maintain:
                _maintain_process_file(mediaid, embedded_id, page, title, tally=tally)
            else:
                _safe_process_one(mediaid, embedded_id, file_page=page)
        if tally is not None:
            _report_maintain_tally(tally, count)
        elif args.maintain:
            _emit_maintain_summary(_s3_partner or "maintain", time.time() - start_time)

    elif args.cat:
        category = pywikibot.Category(site, args.cat)
        generator = pagegenerators.CategorizedPageGenerator(
            category, namespaces=[6], recurse=args.recurse
        )
        if _maintain_parallel_enabled(
            args.maintain, _workers, args.count_only, _s3_partner
        ):
            # Parallel maintain: enumerate the category into id-groups and
            # dispatch one group per worker. Grouping keeps an item's files
            # together so title-drift renames (which only collide within one
            # id — the id is in the canonical title) can't race across workers.
            groups = _enumerate_maintain_groups(generator, args.limit)
            total_files = sum(len(g) for g in groups)
            print(
                f"maintain: {total_files} files in {len(groups)} id-group(s); "
                f"{_workers} workers."
            )
            _run_maintain_parallel(groups, _workers)
            _emit_maintain_summary(
                _s3_partner or args.cat, time.time() - start_time, workers=_workers
            )
            return

        tally = _new_maintain_tally(args.maintain and args.count_only)
        for page in generator:
            title = page.title()
            print("\n" + title)
            mediaid = "M" + str(page.pageid)
            embedded_id = _resolve_dpla_id(title, dpla_api)
            count += 1
            print(f"{count}: {mediaid}")
            # CategorizedPageGenerator yields generic Page objects;
            # re-wrap as FilePage so the post-SDC cleanup gets the
            # right type (its normalize / migrate helpers expect a
            # FilePage handle).
            file_page = pywikibot.FilePage(site, title)
            if args.maintain:
                _maintain_process_file(
                    mediaid, embedded_id, file_page, title, tally=tally
                )
            else:
                _safe_process_one(mediaid, embedded_id, file_page=file_page)
            if args.limit and count >= args.limit:
                print(f" -- Reached --limit {args.limit}, stopping.")
                break
        if tally is not None:
            _report_maintain_tally(tally, count)
        elif args.maintain:
            _emit_maintain_summary(_s3_partner or args.cat, time.time() - start_time)

    elif args.partner:
        _ids_file = args.ids_file or os.path.join(args.partner, f"{args.partner}.csv")
        if args.migrate_legacy:
            # Phase 3b: legacy {{Artwork}} → {{DPLA metadata}} migration. A
            # one-time operation per file, separate from the SDC sync that
            # ``_run_partner_mode`` performs. Both walk the same partner IDs
            # CSV; the migration mode is gated behind --migrate-legacy so
            # an operator has to ask for it explicitly.
            _run_legacy_migration_mode(args.partner, _ids_file)
        else:
            # Partner-driven SDC phase (PR 4). Iterates the partner's IDs CSV
            # and syncs each item's precomputed sdc.json against Commons.
            # Designed to be the last step in a
            # `get-ids-es → downloader → uploader → sdc-sync` pipeline chain.
            _run_partner_mode(args.partner, _ids_file)


if __name__ == "__main__":
    main()
