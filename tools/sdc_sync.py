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
from bs4 import BeautifulSoup
from pywikibot import pagegenerators
from ingest_wikimedia.logs import setup_logging
from ingest_wikimedia.slack import notify_phase_start, notify_sdc_complete
from ingest_wikimedia.tracker import Result, Tracker
from ingest_wikimedia.wikimedia import extract_dpla_id_from_commons_title

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
    global _s3_partner, _s3_client

    parser = _build_parser()
    args = parser.parse_args()
    if args.method:
        method = args.method

    with open(os.path.join(_REPO_ROOT, "config.toml"), "rb") as f:
        dpla_api = tomllib.load(f)["dpla_api_key"]

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
    ref_url.setTarget(f"https://dp.la/item/{dpla_id}")
    ref_publisher = pywikibot.Claim(site, "P123", is_reference=True)
    ref_publisher.setTarget(pywikibot.ItemPage(repo, "Q2944483"))
    today = datetime.date.today()
    ref_retrieved = pywikibot.Claim(site, "P813", is_reference=True)
    ref_retrieved.setTarget(
        pywikibot.WbTime(year=today.year, month=today.month, day=today.day)
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


# Adds a missing qualifier to an existing claim via ``wbsetqualifier``.
# Currently only used for P459 (determination method). The POST is
# submitted through pywikibot's ``site.simple_request``, which manages
# the CSRF token and retries on transient errors automatically.


def postqual(claimid, prop, value):
    """Add a qualifier to an existing claim via ``wbsetqualifier``.

    Routed through ``site.simple_request`` so pywikibot handles CSRF token
    refresh, ``maxlag`` / ``Retry-After`` honoring, exponential backoff on
    transient errors, and auto-relogin on ``badtoken`` — all automatically.
    Replaces a hand-rolled refresh-token-and-retry-once pattern that didn't
    cover ``maxlag`` or rate-limit signaling.

    Best-effort: a final failure logs and continues; the partner is not
    aborted for a missing qualifier amendment.
    """
    summary = f"Adding [[:d:Property:{prop}]] to {claimid}."
    try:
        site.simple_request(
            action="wbsetqualifier",
            claim=claimid,
            property=prop,
            snaktype="value",
            value=value,
            bot=True,
            summary=summary,
            token=site.tokens["csrf"],
        ).submit()
        pywikibot.output(summary)
    except pywikibot.exceptions.APIError as e:
        # Log and continue — qualifier amends are best-effort, and pywikibot
        # has already exhausted its built-in retry policy by the time an
        # APIError surfaces here. Surface the Commons error code so it's
        # diagnosable from the log without needing to enable verbose tracing.
        print(
            f" -- Failed to amend qualifier {prop} for {claimid}:"
            f" {e.code} — {getattr(e, 'info', '')}"
        )


# This function performs an initial GET request on the given Wikimedia file to check if the statement we will be adding is already in the page. It returns a boolean, with True if the statement is not found and can be added. "qid" is passed as a tuple with both the value and the data type, so this check can handle the formatting for different data types. If statements are found in the entity with the prop and value, but no qualifiers, we return the statement id instead, so that the qualifier can be added to that statement instead of creating a new one using postqual().


# Per-file cache for wbgetentities, populated at the start of process_one()
# and consulted by check() for all subsequent add_* calls. Avoids ~25 redundant
# round-trips per file. Invalidate when claims change to keep the read-after-write
# semantics correct (process_one batches writes at the end, so a single fetch is
# safe for the duration of one file).
_entity_cache = {}


def get_entity(mediaid):
    """Return the wbgetentities response for mediaid, caching per process_one run."""
    cached = _entity_cache.get(mediaid)
    if cached is not None:
        return cached
    request = site.simple_request(action="wbgetentities", ids=mediaid)
    raw = request.submit()
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
#   * add_date (P571)           — P1932 (stated as)
#   * add_contributed (P9126)   — P3831 (object has role)
#   * add_local_id (P217)       — P195 (collection)
#   * add_source (P7482)        — P973 (described at URL), P137 (operator)
_DPLA_EXTRA_QUALIFIER_PROPS = {
    "P170": {"P2093"},
    "P571": {"P1932"},
    "P9126": {"P3831"},
    "P217": {"P195"},
    "P7482": {"P973", "P137"},
}


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
        for statement in statements:
            if (
                statement["mainsnak"]["datavalue"]["value"] == qid[1]
                and not statement.get("references")
                and _is_safe_to_amend_in_place(statement, prop)
            ):
                ref = statement["id"]
                break
        for statement in statements:
            if statement["mainsnak"]["datavalue"]["value"] == qid[
                1
            ] and not statement.get("qualifiers"):
                return add_det(mediaid, statement["id"]), ref

        if any(
            statement["mainsnak"]["datavalue"]["value"] == qid[1]
            and _is_safe_to_amend_in_place(statement, prop)
            for statement in statements
        ):
            print(
                f" -- There already exists a DPLA-authored statement with a {prop} > {qid[1]} claim for {mediaid}."
            )
            return False, ref

        if any(
            statement["mainsnak"]["datavalue"]["value"] == qid[1]
            for statement in statements
        ):
            print(
                f" -- A foreign {prop} > {qid[1]} statement exists for {mediaid}; adding the DPLA-authored statement alongside."
            )
            return True, ""

        return True, ref
    if qid[0] == "monolingualtext":
        for statement in statements:
            if (
                statement["mainsnak"]["datavalue"]["value"]["text"] == qid[1]
                and not statement.get("references")
                and _is_safe_to_amend_in_place(statement, prop)
            ):
                ref = statement["id"]
                break
        for statement in statements:
            if statement["mainsnak"]["datavalue"]["value"]["text"] == qid[
                1
            ] and not statement.get("qualifiers"):
                return add_det(mediaid, statement["id"]), ref

        if any(
            statement["mainsnak"]["datavalue"]["value"]["text"] == qid[1]
            and _is_safe_to_amend_in_place(statement, prop)
            for statement in statements
        ):
            print(
                f" -- There already exists a DPLA-authored statement with a {prop} > {qid[1]} claim for {mediaid}."
            )
            return False, ref

        if any(
            statement["mainsnak"]["datavalue"]["value"]["text"] == qid[1]
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
    if claimid:
        qid = "Q61848113"
        prop = "P459"
        value = json.dumps(
            {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))}
        )
        postqual(claimid, prop, value)
        # postqual just mutated Commons state for this mediaid (added P459
        # to an existing claim). Drop the cached snapshot so any subsequent
        # check() call for the same mediaid in this run reads the fresh
        # post-write state instead of repeating the qualifier write.
        invalidate_entity(mediaid)


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
    """Return the 'kind' tag (item|string|monolingualtext|somevalue|source)
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


def _extract_comparable_value(claim):
    """Pull the comparable scalar value out of a precomputed sdc.json claim.

    Matches what `_reconcile_existing_claims` extracts from existing
    Commons statements so the two sides line up. Returns:

      * Q-ID string for wikibase-entityid (e.g. "Q19652")
      * the raw string for string-typed claims (P760, P217, etc.)
      * the text body for monolingualtext claims (P1476, P10358)
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
        return datavalue["value"]
    if dtype == "monolingualtext":
        return datavalue["value"]["text"]
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

    Raises :class:`_MissingEntityError` when Commons returns
    ``no-such-entity`` (the entity doesn't exist; not the SDC phase's
    problem — see PR #267). Other ``APIError`` codes propagate as a
    ``RuntimeError`` carrying the code + info, which the per-ordinal
    handler catches and treats as an ``SDC_ORDINALS_SKIPPED_ERROR``.

    ``params`` is the per-action payload — for wbeditentity, the
    serialized ``data``; for wbremoveclaims, the pipe-joined ``claim``
    string. ``bot=True``, the CSRF token, and ``id=mediaid`` are
    injected here so call sites stay focused on the action-specific
    differences.
    """
    try:
        site.simple_request(
            action=action,
            id=mediaid,
            bot=True,
            token=site.tokens["csrf"],
            **params,
        ).submit()
    except pywikibot.exceptions.APIError as e:
        if e.code == "no-such-entity":
            raise _MissingEntityError(mediaid) from e
        raise RuntimeError(
            f"{action} failed for {mediaid} ({dpla_id}):"
            f" {e.code} — {_truncate(getattr(e, 'info', ''))}"
        ) from e


def _post_new_refs(mediaid, dpla_id):
    """POST the accumulated ``refclaims["claims"]`` to ``wbeditentity``.

    Reads the module-global ``refclaims`` (populated by ``add_ref`` during
    the per-claim check loop). No-op when nothing to post.
    """
    if not refclaims["claims"]:
        return
    refs_to_post = len(refclaims["claims"])
    _submit_sdc_write(
        "wbeditentity",
        mediaid,
        dpla_id,
        data=json.dumps(refclaims),
        summary=(
            f"Added structured data references from [[COM:DPLA|DPLA]] item"
            f" '[[dpla:{dpla_id}|{dpla_id}]]'."
            f" [[COM:DPLA/MOD|Leave feedback]]!"
        ),
    )
    print(" --- Saved new refs!")
    tracker.increment(Result.SDC_REFS_ADDED, refs_to_post)


def _post_new_claims(mediaid, dpla_id):
    """POST the accumulated ``claims["claims"]`` to ``wbeditentity``.

    Reads the module-global ``claims`` (populated by add_* helpers during
    the per-claim check loop). No-op when nothing to post.
    """
    if not claims["claims"]:
        return
    claims_to_post = len(claims["claims"])
    _submit_sdc_write(
        "wbeditentity",
        mediaid,
        dpla_id,
        data=json.dumps(claims),
        summary=(
            f"Added structured data claims from [[COM:DPLA|DPLA]] item"
            f" '[[dpla:{dpla_id}|{dpla_id}]]'."
            f" [[COM:DPLA/MOD|Leave feedback]]!"
        ),
    )
    print(" --- Saved new claims!")
    tracker.increment(Result.SDC_CLAIMS_ADDED, claims_to_post)


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
    _reconcile_existing_claims(mediaid, dpla_id, expected)


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
    expected = {
        "P217": local_ids,
        "P760": [dpla_id],
        "P1476": titles,
        "P195": ["Q518155" if hub == "Q518155" else institution],
        "P170": creators,
        "P9126": ["Q2944483", hub, institution],
        "P7482": [url],
        "P4272": subjects,
        "P571": dates,
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


def _reconcile_existing_claims(mediaid, dpla_id, expected):
    """Walk DPLA-referenced claims on Commons, queue removals for any whose
    comparable value isn't in `expected`. POSTs wbremoveclaims if needed.

    Shared by `dpla_claims` (legacy partner-API path) and
    `process_one_from_sdc` (PR 4 partner-mode path). Same removal logic;
    just different sources for `expected`.
    """
    # Fetch the file's current MediaInfo state via pywikibot's
    # wbgetentities, NOT a bare `requests.get(...)` to Special:EntityData.
    # Wikimedia now (per phab T400119) returns HTTP 403 to any
    # `python-requests/X.Y`-style default User-Agent, with body
    # "Please set a user-agent and respect our robot policy". `.json()`
    # on that body raises JSONDecodeError. The previous broad
    # `except Exception:` silently swallowed every such failure and
    # fell back to `{"entities": {mediaid: {"statements": {}}}}`,
    # so the reconciler saw zero DPLA-referenced claims on every file
    # and queued ZERO removals across every partner-mode SDC run.
    # Stale claims (e.g. an older author-name-string formatting that
    # has since been replaced in sdc.json) accumulate forever.
    #
    # Routing through `get_entity` reuses pywikibot's
    # `Site.simple_request`, which sets the correct UA, manages
    # CSRF tokens, honors maxlag/Retry-After, etc. We invalidate
    # the per-process entity cache first so this read sees the
    # post-write state from `_post_new_claims` above. Errors here
    # propagate to the per-ordinal exception boundary in
    # `_run_partner_mode` (logged + counted as
    # SDC_ORDINALS_SKIPPED_ERROR) instead of silently masking a
    # broken reconciler.
    print(f" -- Accessing Commons ID {mediaid}")
    invalidate_entity(mediaid)
    entity = get_entity(mediaid)
    print(f" -- Accessed Commons ID {mediaid}")
    statements = entity.get("statements", {}) or {}
    dpla_claim_list = []
    removals = []
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
                                    # P7482 statement without a P973 qualifier — skip it
                                    pass
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
                                dpla_claim_list.append(
                                    {
                                        stmt["mainsnak"]["property"]: {
                                            "id": stmt["id"],
                                            "value": stmt["mainsnak"]["datavalue"][
                                                "value"
                                            ],
                                        }
                                    }
                                )
                            elif dtype == "monolingualtext":
                                dpla_claim_list.append(
                                    {
                                        stmt["mainsnak"]["property"]: {
                                            "id": stmt["id"],
                                            "value": stmt["mainsnak"]["datavalue"][
                                                "value"
                                            ]["text"],
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
            if prop not in expected:
                removals.append(claim[prop]["id"])
            elif claim[prop]["value"] not in expected[prop]:
                removals.append(claim[prop]["id"])
    if removals:
        _submit_sdc_write(
            "wbremoveclaims",
            mediaid,
            dpla_id,
            claim="|".join(removals),
            summary=(
                f"Changing structured data claims from [[COM:DPLA|DPLA]]"
                f" item '[[dpla:{dpla_id}|{dpla_id}]]'."
                f" [[COM:DPLA/MOD|Leave feedback]]!"
            ),
        )
        print(" --- Saved removals!")
        tracker.increment(Result.SDC_REMOVALS, len(removals))


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
        codes = {
            "10031403": "Q66739888",
            "10031402": "Q24238356",
            "10031399": "Q66739729",
            "10031400": "Q66739849",
            "10031401": "Q66739875",
        }
        levels = {"item": "Q11723795", "itemAv": "Q11723795", "fileUnit": "Q59221146"}
        access = ""
        level = ""
        try:
            xml = BeautifulSoup(dpla["originalRecord"]["stringValue"], "xml")
        except Exception as e:
            # No XML parser available (e.g. lxml missing the xml feature) — skip
            # NARA-specific access/level extraction rather than aborting the file.
            print(f" -- Skipping NARA XML parse for {dpla_id}: {e}")
            xml = None
        if xml is not None:
            try:
                acccess_naid = str(
                    xml.find("accessRestriction").find("status").find("naId").text
                )
                access = codes[acccess_naid]
            except Exception:
                access = ""
            for lvl_key in levels:
                if xml.find(lvl_key):
                    level = levels[lvl_key]
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


def process_one(mediaid, dpla_id):
    """Fetch DPLA metadata and sync SDC claims for a single Commons file."""
    global claims, refclaims

    # Drop any stale cache from a prior file so check() always reads fresh state
    # for this mediaid.
    invalidate_entity(mediaid)
    # Pre-warm the per-file entity cache so the ~25 add_*/check() calls below
    # share a single wbgetentities round-trip.
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

    claims = {"claims": []}
    refclaims = {"claims": []}

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
        _post_new_refs(mediaid, dpla_id)
        _post_new_claims(mediaid, dpla_id)
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
    except _MissingEntityError:
        logging.info(
            f" -- {mediaid} for {dpla_id}: Commons MediaInfo entity does not"
            " exist; skipping (not an error)."
        )
        tracker.increment(Result.SDC_ORDINALS_SKIPPED_MISSING_ENTITY)
        return


def process_one_from_sdc(mediaid, dpla_id, sdc_payload):
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
    """
    global claims, refclaims

    # Drop any stale cache from a prior file so check() always reads fresh
    # state for this mediaid. The cache survives across files within one
    # process invocation, so explicit invalidation is necessary when we
    # move to a new file. Pre-warm so the per-claim check() calls below
    # share one wbgetentities round-trip.
    invalidate_entity(mediaid)
    get_entity(mediaid)

    claims = {"claims": []}
    refclaims = {"claims": []}

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

    _post_new_refs(mediaid, dpla_id)
    _post_new_claims(mediaid, dpla_id)

    expected = _build_expected_from_sdc(sdc_payload)
    _reconcile_existing_claims(mediaid, dpla_id, expected)


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
    from botocore.exceptions import ClientError

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

    logging.info(f"Partner mode: {partner} — {len(dpla_ids)} items from {ids_file}")
    completed = False
    try:
        for local_count, dpla_id in enumerate(dpla_ids, start=1):
            # Item-start marker — `wikimedia_upload_status._sdc_progress`
            # greps for this to surface SDC progress.
            logging.info(f"DPLA ID: {dpla_id} ({local_count}/{len(dpla_ids)})")

            # S3Client.get_item_file returns None on 404/NoSuchKey but
            # re-raises any other ClientError. Catch those per-item so one
            # transient S3 failure doesn't abort the whole partner batch.
            try:
                sdc_raw = s3.get_sdc_json(partner, dpla_id)
            except ClientError as e:
                logging.warning(
                    f" -- S3 error reading sdc.json for {dpla_id}: {e!r}; skipping."
                )
                tracker.increment(Result.SDC_ITEMS_SKIPPED_NO_SIDECAR)
                continue
            if sdc_raw is None:
                logging.info(" -- No sdc.json on S3; skipping.")
                tracker.increment(Result.SDC_ITEMS_SKIPPED_NO_SIDECAR)
                continue
            try:
                sdc_payload = json.loads(sdc_raw)
            except json.JSONDecodeError as e:
                logging.warning(f" -- sdc.json failed to parse: {e}; skipping.")
                tracker.increment(Result.SDC_ITEMS_SKIPPED_MAPPING)
                continue

            try:
                upload_raw = s3.get_upload_result(partner, dpla_id)
            except ClientError as e:
                logging.warning(
                    f" -- S3 error reading upload-result.json for {dpla_id}: {e!r}; skipping."
                )
                tracker.increment(Result.SDC_ITEMS_SKIPPED_NO_SIDECAR)
                continue
            if upload_raw is None:
                logging.info(" -- No upload-result.json on S3; skipping.")
                tracker.increment(Result.SDC_ITEMS_SKIPPED_NO_SIDECAR)
                continue
            try:
                upload_result = json.loads(upload_raw)
            except json.JSONDecodeError as e:
                logging.warning(
                    f" -- upload-result.json failed to parse: {e}; skipping."
                )
                tracker.increment(Result.SDC_ITEMS_SKIPPED_MAPPING)
                continue

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
                continue
            eligible = {
                ord_str: data
                for ord_str, data in ordinals.items()
                if isinstance(data, dict)
                and data.get("status") in ("UPLOADED", "SKIPPED")
            }
            if not eligible:
                logging.info(" -- No SDC-eligible ordinals; skipping.")
                tracker.increment(Result.SDC_ITEMS_SKIPPED_MAPPING)
                continue

            synced_this_item = False
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
                logging.warning(
                    f" -- upload-result ordinals malformed for {dpla_id}; skipping."
                )
                tracker.increment(Result.SDC_ITEMS_SKIPPED_MAPPING)
                continue
            for ord_str, data in ordinal_items:
                pageid = data.get("pageid")
                # `if not pageid` rather than `is None` — a recorded
                # pageid of 0 is just as malformed as a missing one
                # (no Commons MediaInfo entity has ID `M0`) and would
                # otherwise propagate downstream as a confusing
                # pywikibot APIError on the bogus mediaid. The
                # uploader has historically written `pageid: 0` for
                # successful new uploads when pywikibot's FilePage
                # cache wasn't invalidated post-upload; treat that
                # sidecar shape as a mapping skip until the upstream
                # bug is fixed and the existing sidecars are
                # backfilled.
                if not pageid:
                    logging.warning(
                        f" -- Ordinal {ord_str}: missing/zero pageid"
                        f" ({pageid!r}); skipping."
                    )
                    continue
                mediaid = f"M{pageid}"
                title = data.get("title", "?")
                logging.info(f" -- Ordinal {ord_str}: {mediaid} ({title})")

                # Snapshot write counters so we can detect whether this
                # ordinal's sync actually changed anything on Commons.
                writes_before = (
                    tracker.count(Result.SDC_CLAIMS_ADDED)
                    + tracker.count(Result.SDC_REFS_ADDED)
                    + tracker.count(Result.SDC_REMOVALS)
                )
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
                try:
                    process_one_from_sdc(mediaid, dpla_id, sdc_payload)
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
                except Exception:
                    logging.exception(
                        f" -- Ordinal {ord_str} ({mediaid}) for {dpla_id}:"
                        " SDC sync failed; skipping ordinal."
                    )
                    tracker.increment(Result.SDC_ORDINALS_SKIPPED_ERROR)
                    had_ordinal_error = True
                    continue
                writes_after = (
                    tracker.count(Result.SDC_CLAIMS_ADDED)
                    + tracker.count(Result.SDC_REFS_ADDED)
                    + tracker.count(Result.SDC_REMOVALS)
                )

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
                if writes_after > writes_before and title and title != "?":
                    try:
                        pywikibot.FilePage(site, title).touch()
                        logging.info(f" -- Touched '{title}' (category refresh).")
                    except Exception as e:
                        logging.warning(
                            f" -- Failed to touch '{title}' for category refresh: {e!r}"
                        )
                synced_this_item = True
            # Only count an item as synced if at least one ordinal actually
            # made it past the pageid guard. An item whose every eligible
            # ordinal lacked a pageid would otherwise inflate the synced
            # counter and underreport mapping skips.
            if synced_this_item:
                tracker.increment(Result.SDC_ITEMS_SYNCED)
            elif had_ordinal_error:
                # Every eligible ordinal raised at runtime — classify
                # under the error bucket, not MAPPING. (Mixed-result
                # items where some ordinals succeed go to SYNCED; the
                # per-ordinal failures are still visible under
                # SDC_ORDINALS_SKIPPED_ERROR.)
                tracker.increment(Result.SDC_ITEMS_SKIPPED_ERROR)
            else:
                tracker.increment(Result.SDC_ITEMS_SKIPPED_MAPPING)
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
            )
        else:
            logging.warning(
                "SDC sync aborted before completion; suppressing terminal "
                "COUNTS dump and notify_sdc_complete call."
            )


# We can use a PWB generator to programatically make the list of files we are working on based on a set of criteria. Here, we are generating the page titles from a Wikimedia Commons search and categories. For other types of available page generators, see <https://doc.wikimedia.org/pywikibot/master/api_ref/pywikibot.html#module-pywikibot.pagegenerators>. As an additional step, we take the pageid provided by the generator and prepend "M" for the mediaid needed for posting SDC statements.


def main() -> None:
    """Entry point for the `sdc-sync` console script.

    Dispatches to the legacy livecat/list/file/cat modes or the PR-4 partner
    mode based on the parsed CLI flags. Initialization (argparse, Commons
    login, ingestion3 JSON fetch) lives in `_initialize()`.
    """
    _initialize()

    count = 0

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
                mediaid = "M" + str(file.pageid)
                dpla_id = _resolve_dpla_id(str(file), dpla_api)
                process_one(mediaid, dpla_id)

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
        for title in args.files:
            print("\n" + title)
            page = pywikibot.Page(site, title)
            if not page.exists():
                print(f" -- Page not found on Commons: {title}")
                continue
            mediaid = "M" + str(page.pageid)
            dpla_id = _resolve_dpla_id(title, dpla_api)
            count += 1
            print(f"{count}: {mediaid}")
            process_one(mediaid, dpla_id)

    elif args.cat:
        category = pywikibot.Category(site, args.cat)
        generator = pagegenerators.CategorizedPageGenerator(
            category, namespaces=[6], recurse=args.recurse
        )
        for page in generator:
            title = page.title()
            print("\n" + title)
            mediaid = "M" + str(page.pageid)
            dpla_id = _resolve_dpla_id(title, dpla_api)
            count += 1
            print(f"{count}: {mediaid}")
            process_one(mediaid, dpla_id)
            if args.limit and count >= args.limit:
                print(f" -- Reached --limit {args.limit}, stopping.")
                break

    elif args.partner:
        # Partner-driven SDC phase (PR 4). Iterates the partner's IDs CSV and
        # syncs each item's precomputed sdc.json against Commons. Designed to
        # be the last step in a `get-ids-es → downloader → uploader → sdc-sync`
        # pipeline chain.
        _ids_file = args.ids_file or os.path.join(args.partner, f"{args.partner}.csv")
        _run_partner_mode(args.partner, _ids_file)


if __name__ == "__main__":
    main()
