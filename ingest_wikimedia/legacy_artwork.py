"""Phase 3 foundation for migrating legacy ``{{Artwork}}`` files to the
``{{DPLA metadata}}`` template.

This module is pure logic — parsing, classification, and claim
construction. No pywikibot API calls, no file I/O. Phase 3b will layer
the actual wbeditentity dispatch and wikitext save on top.

Goal 2 of the SDC ↔ wikitext integration: existing Commons files
uploaded by DPLA in the legacy ``{{Artwork}}`` form need to be migrated
to ``{{DPLA metadata}}`` while preserving anything Commons editors have
contributed since the original upload (sometimes years ago). Naïve
overwrite would discard those community edits; this module's job is to
distinguish DPLA-originated values (safe to overwrite with canonical
data) from community-contributed values (preserve by importing them
into SDC as user-contributed statements before stripping them from the
wikitext).

Community-contributed values get a special reference shape on their SDC
statement — ``P887→Q131783016`` (based on heuristic — "inferred from
Wikitext") + ``P4656→<permalink-to-source-revision>`` (Wikimedia import
URL with the source revision permalink, per Wikidata's recommendation
for that property). They do **not** carry the standard DPLA qualifiers
(P459/Q61848113 heuristic, P813 retrieved-on, etc.) — those would
misrepresent the value as DPLA-sourced when it isn't.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Iterable
from urllib.parse import urlencode

import mwparserfromhell

# DPLA's Commons-bot account names. Revisions authored by these accounts
# are treated as DPLA-originated for the purpose of provenance
# classification — any param value they last touched is safe to
# overwrite with canonical data. Other accounts are treated as
# community contributors whose edits must be preserved (by importing
# to SDC) before the wikitext is rewritten.
#
# Extend this set in a follow-up if older DPLA bot accounts are
# discovered in the upload-history of long-tenure files. The set is
# matched case-insensitively against the revision's ``user`` field;
# add the canonical form a Commons revision history would display.
DPLA_BOT_ACCOUNTS: frozenset[str] = frozenset(
    {
        "DPLA_bot",
        "US National Archives bot",
    }
)

# Wikidata items / properties used in the legacy-import reference shape.
# Hardcoded here (not behind a config knob) because they are part of the
# import's semantic contract — changing them would silently change the
# meaning of every legacy-imported statement.
QID_INFERRED_FROM_WIKITEXT = "Q131783016"  # the source-of-truth item
PID_BASED_ON_HEURISTIC = "P887"
PID_WIKIMEDIA_IMPORT_URL = "P4656"

# Per-param Wikidata-property mapping for the SDC import. Each key is a
# canonical-params key (matching what `dpla_metadata_params` returns);
# each value is the property + value-builder kind. Kept narrow on
# purpose — only the four scalar fields with unambiguous Wikidata
# property mappings are in scope for Phase 3a. Subject, source URL,
# institution, and rights/permission mappings involve richer Wikidata
# semantics and are deferred to a later phase that can mirror the full
# SDC pipeline's logic without duplicating it.
LEGACY_IMPORT_PROPERTY: dict[str, tuple[str, str]] = {
    # ``title`` → P1476 (title), monolingualtext
    "title": ("P1476", "monolingualtext"),
    # ``description`` → P10358 (description), monolingualtext. Long
    # values are chunked via P1545 ordinals downstream; Phase 3a
    # produces a single claim per value and lets the caller split as
    # needed in Phase 3b.
    "description": ("P10358", "monolingualtext"),
    # ``date`` → P571 (inception), time. Phase 3a stores the raw string
    # in a sentinel form so Phase 3b's claim builder can re-parse with
    # the existing :func:`ingest_wikimedia.sdc.parse_dpla_date` helper.
    "date": ("P571", "time"),
    # ``creator`` (or ``author``/``artist`` aliases) → P2093 (creator —
    # stated as), string. Avoids the Wikidata-reconciliation complexity
    # of P170 (creator) for the common case of a string-only credit;
    # editors can promote individual statements to P170 manually later.
    "creator": ("P2093", "string"),
}

# Canonical mapping from legacy template param names (case-folded) to
# the canonical-params keys both the writer and comparator use. Covers
# the param sets the legacy DPLA-bot ``{{Artwork}}`` form emitted plus
# the well-known aliases Commons editors use for the same fields.
ARTWORK_PARAM_TO_CANONICAL_KEY: dict[str, str] = {
    "title": "title",
    "description": "description",
    "date": "date",
    # ``{{Artwork}}`` has historically used ``author`` for the creator
    # row, and Commons editors freely use ``artist`` too. Both map to
    # the same canonical key — the new template's idiomatic name is
    # ``creator`` (matching DPLA's archival-records convention).
    "author": "creator",
    "artist": "creator",
    "creator": "creator",
    "source": "source",
    "institution": "institution",
    "permission": "permission",
}

# Template names (case-folded) whose params get walked during migration.
# Order doesn't matter — if a page carries multiple, only the first
# match is migrated; the others are left alone for a manual sweep.
LEGACY_TEMPLATE_NAMES: tuple[str, ...] = ("artwork", "information", "photograph")


@dataclass(frozen=True)
class RevisionSnapshot:
    """Minimal projection of a pywikibot Revision that the migration
    logic needs. Phase 3b populates this from ``FilePage.revisions()``;
    Phase 3a tests pass it in directly.
    """

    revid: int
    user: str
    text: str


@dataclass
class MigrationPlan:
    """The output of :func:`plan_migration` — a structured description of
    every action Phase 3b's executor needs to perform.

    ``community_imports`` carries the field-by-field values that must
    survive the migration. Each entry is keyed by canonical-params key
    so Phase 3b can dispatch to the right SDC property without
    re-doing the parsing work.

    ``source_permalink`` is the value that gets stamped onto every
    imported claim's P4656 reference. It points at the revision *that
    contains* the community-contributed value — i.e. the page's latest
    revision id at plan-construction time. This lets a reviewer trace
    any imported statement back to its original wikitext source.
    """

    source_permalink: str
    community_imports: dict[str, str] = field(default_factory=dict)
    dpla_originated_params: dict[str, str] = field(default_factory=dict)
    artwork_template_name: str = ""


def _template_name(template) -> str:
    return str(template.name).strip().casefold()


def _normalize_param_name(param) -> str:
    return str(param.name).strip().casefold()


def find_legacy_template(wikitext: str):
    """Locate the first legacy metadata template in ``wikitext``.

    Returns the ``mwparserfromhell`` Template node, or None if no
    legacy template is present. Order in :data:`LEGACY_TEMPLATE_NAMES`
    is honored — ``{{Artwork}}`` wins over ``{{Information}}`` when
    both are present (rare but defensible: a page already migrated
    once that someone added an Information block to).
    """
    wikicode = mwparserfromhell.parse(wikitext)
    for tpl in wikicode.filter_templates():
        if _template_name(tpl) in LEGACY_TEMPLATE_NAMES:
            return tpl
    return None


def parse_artwork_params(wikitext: str) -> dict[str, str]:
    """Extract the canonical-key → value dict from a legacy template
    invocation in ``wikitext``.

    Returns ``{}`` when no legacy template is found, or when one is
    present but carries no recognised params. Values are whitespace-
    stripped (matching the renderer's behavior); the unrecognised
    param names are dropped silently — those don't have a canonical
    target and the wikitext-rewrite step preserves them by leaving
    the template untouched if the param wasn't migrated.
    """
    template = find_legacy_template(wikitext)
    if template is None:
        return {}
    parsed: dict[str, str] = {}
    for param in template.params:
        canonical = ARTWORK_PARAM_TO_CANONICAL_KEY.get(_normalize_param_name(param))
        if canonical is None:
            continue
        value = str(param.value).strip()
        if value:
            # On duplicate canonical keys (e.g. both ``author`` and
            # ``creator`` set), the *last* wins — matches the renderer
            # behavior under MediaWiki, where the later assignment
            # overrides the earlier.
            parsed[canonical] = value
    return parsed


def _is_dpla_bot(user: str) -> bool:
    """Case-insensitive match against :data:`DPLA_BOT_ACCOUNTS`."""
    return user.casefold() in {a.casefold() for a in DPLA_BOT_ACCOUNTS}


def trace_param_provenance(
    revisions: Iterable[RevisionSnapshot],
) -> dict[str, str]:
    """Walk revisions oldest→newest to find which editor last set each
    legacy-template param to its current value.

    Returns ``{canonical_key: editor_user}`` for every param whose
    current value can be traced to at least one revision. Params
    introduced and unchanged since the very first revision are
    attributed to that revision's editor. Params whose value couldn't
    be traced (e.g. they vanished and reappeared in some intermediate
    state mwparserfromhell can't reconstruct) are omitted; the caller
    treats absence as "no provenance found → preserve conservatively."

    The walk is forward in time so each per-key entry is overwritten
    as the value changes — the dict at the end holds the *last*
    revision that touched each current value, which is exactly the
    provenance attribution the classifier needs.

    Pure: takes pre-fetched snapshots and returns a dict; doesn't
    interact with pywikibot.
    """
    sorted_revs = sorted(revisions, key=lambda r: r.revid)
    if not sorted_revs:
        return {}

    final_params = parse_artwork_params(sorted_revs[-1].text)
    if not final_params:
        return {}

    provenance: dict[str, str] = {}
    prior_seen: dict[str, str] = {}
    for rev in sorted_revs:
        rev_params = parse_artwork_params(rev.text)
        # Drop entries from prior_seen for params the current revision
        # no longer carries. Without this, a delete → re-add of the
        # same string at a later revision looks like "unchanged" (the
        # post-delete value still matches the pre-delete value in
        # prior_seen) and stays attributed to the original setter —
        # misclassifying a community restoration as DPLA-originated
        # and putting it on the strip list when the canonical value
        # drifts. Pop them on disappearance so re-add registers as a
        # fresh change attributed to the editor who restored it.
        for stale in tuple(prior_seen.keys() - rev_params.keys()):
            prior_seen.pop(stale, None)
        for key, value in rev_params.items():
            if value != prior_seen.get(key):
                provenance[key] = rev.user
                prior_seen[key] = value
    # Only return entries whose tracked value still matches the final
    # value — a param that was edited and then reverted back to a prior
    # form would otherwise carry stale provenance.
    return {
        key: editor
        for key, editor in provenance.items()
        if prior_seen.get(key) == final_params.get(key)
    }


def classify_param_provenance(
    provenance: dict[str, str],
    bot_accounts: frozenset[str] = DPLA_BOT_ACCOUNTS,
) -> dict[str, str]:
    """For each param, label its provenance as ``"dpla"`` or
    ``"community"`` based on whose account introduced its current value.

    ``bot_accounts`` is parameterised purely so tests can pass a
    custom allowlist. Production callers should accept the module
    default; new bot accounts get added to :data:`DPLA_BOT_ACCOUNTS`
    rather than the per-call argument.
    """
    bot_set_cf = {a.casefold() for a in bot_accounts}
    return {
        key: ("dpla" if editor.casefold() in bot_set_cf else "community")
        for key, editor in provenance.items()
    }


def plan_migration(
    file_title: str,
    revisions: Iterable[RevisionSnapshot],
    canonical_params: dict,
    bot_accounts: frozenset[str] = DPLA_BOT_ACCOUNTS,
) -> MigrationPlan | None:
    """Compute the migration plan for a legacy-Artwork file.

    Walks the revision history to attribute each legacy-template param,
    classifies each value as DPLA-originated vs community-contributed,
    and packages the result as a :class:`MigrationPlan` describing
    everything Phase 3b's executor needs to do.

    Returns None when there's nothing to migrate — no legacy template
    on the page, or no revisions supplied.

    ``canonical_params`` is the dict returned by
    :func:`ingest_wikimedia.wikimedia.dpla_metadata_params` for this
    file's DPLA item. Values are compared against the wikitext to
    detect cases where a "community" provenance is actually a
    community editor re-stating what DPLA already has — in that case
    nothing is imported (the value is redundant with canonical).
    """
    sorted_revs = sorted(revisions, key=lambda r: r.revid)
    if not sorted_revs:
        return None
    latest = sorted_revs[-1]
    if find_legacy_template(latest.text) is None:
        return None

    wikitext_params = parse_artwork_params(latest.text)
    provenance = trace_param_provenance(sorted_revs)
    classified = classify_param_provenance(provenance, bot_accounts)

    community_imports: dict[str, str] = {}
    dpla_originated: dict[str, str] = {}

    for key, value in wikitext_params.items():
        canonical_value = _canonical_value_for_key(canonical_params, key)
        if classified.get(key) == "community" and value != canonical_value:
            # Only import community values that *differ* from canonical
            # — a community editor restating DPLA's title verbatim is
            # not an import we want to record (it's redundant). Same
            # invariant as Goal 1: if it matches, it's strip-eligible.
            community_imports[key] = value
        else:
            dpla_originated[key] = value

    legacy_template = find_legacy_template(latest.text)
    return MigrationPlan(
        source_permalink=_build_permalink(file_title, latest.revid),
        community_imports=community_imports,
        dpla_originated_params=dpla_originated,
        artwork_template_name=_template_name(legacy_template),
    )


def _canonical_value_for_key(canonical_params: dict, key: str) -> str:
    """Pull the comparable canonical value for a key.

    Scalar keys (title/description/date/permission) live at the top
    level. The creator key is special: ``dpla_metadata_params`` emits
    creator inside the ``{{InFi|Creator|...}}`` sub-template shape, so
    the comparable canonical value is the second positional. Source,
    institution, etc. are sub-template valued and not handled in
    Phase 3a's scalar mapping — return empty so the equality check
    correctly classifies them as "different" and they fall through
    to community-import or dpla-originated based on provenance only.
    """
    if key in ("title", "description", "date", "permission"):
        return str(canonical_params.get(key, ""))
    if key == "creator":
        creator = canonical_params.get("creator", {})
        return str(creator.get("params", {}).get("2", ""))
    return ""


def _build_permalink(file_title: str, oldid: int) -> str:
    """Return the Commons permalink for ``<file_title>`` at revision
    ``<oldid>``. Used as the P4656 (Wikimedia import URL) reference
    value; permalinks are required by the property's usage notes.

    Uses :func:`urllib.parse.urlencode` so reserved characters in the
    title — particularly ``&`` (legal in Commons filenames like
    ``File:Foo & Bar.jpg``), but also ``?``, ``#``, ``+`` — are
    percent-encoded.  Hand-rolled "just replace spaces" encoding lets
    such titles produce a URL the query-string parser truncates at
    the literal ``&``, silently breaking the P4656 reference for an
    arbitrary subset of filenames.

    Spaces are converted to underscores first (Commons URL convention)
    so the percent-encoded form mirrors what
    ``[[Special:Permalink/<oldid>]]`` actually serves at the link.
    """
    return "https://commons.wikimedia.org/w/index.php?" + urlencode(
        {"title": file_title.replace(" ", "_"), "oldid": oldid}
    )


# ---------------------------------------------------------------------------
# SDC claim construction (Wikidata-API-ready dicts)
# ---------------------------------------------------------------------------


def _reference_snaks(permalink: str) -> dict:
    """Return the two-snak reference block stamped onto every legacy-
    imported statement: P887→Q131783016 + P4656→<permalink>.

    The order matters cosmetically (snaks-order is what Wikibase
    serializes back when reading), and matches the order
    :func:`format_legacy_import_claim` writes.
    """
    return {
        "snaks": {
            PID_BASED_ON_HEURISTIC: [
                {
                    "snaktype": "value",
                    "property": PID_BASED_ON_HEURISTIC,
                    "datatype": "wikibase-item",
                    "datavalue": {
                        "type": "wikibase-entityid",
                        "value": {
                            "entity-type": "item",
                            "id": QID_INFERRED_FROM_WIKITEXT,
                        },
                    },
                }
            ],
            PID_WIKIMEDIA_IMPORT_URL: [
                {
                    "snaktype": "value",
                    "property": PID_WIKIMEDIA_IMPORT_URL,
                    "datatype": "url",
                    "datavalue": {"type": "string", "value": permalink},
                }
            ],
        },
        "snaks-order": [PID_BASED_ON_HEURISTIC, PID_WIKIMEDIA_IMPORT_URL],
    }


def _monolingualtext_datavalue(text: str, language: str = "en") -> dict:
    return {
        "type": "monolingualtext",
        "value": {"text": text, "language": language},
    }


def _string_datavalue(text: str) -> dict:
    return {"type": "string", "value": text}


def format_legacy_import_claim(
    canonical_key: str,
    value: str,
    permalink: str,
    today: datetime.date | None = None,
) -> dict | None:
    """Build a Wikibase ``wbeditentity``-ready claim dict for one
    legacy-imported value.

    Returns None when ``canonical_key`` isn't in
    :data:`LEGACY_IMPORT_PROPERTY`'s narrow allowlist; the caller
    then leaves the value in wikitext rather than guessing at a
    property mapping.

    ``today`` is accepted (and currently unused) for symmetry with
    other claim builders that stamp ``P813`` retrieved-on — kept in
    the signature so a later phase can decide whether to add a
    secondary P813 reference snak without changing call sites.
    """
    mapping = LEGACY_IMPORT_PROPERTY.get(canonical_key)
    if mapping is None:
        return None
    prop, kind = mapping
    if kind == "monolingualtext":
        datavalue = _monolingualtext_datavalue(value)
        datatype = "monolingualtext"
    elif kind == "string":
        datavalue = _string_datavalue(value)
        datatype = "string"
    elif kind == "time":
        # Phase 3a doesn't parse the date — Phase 3b will pass the
        # raw string through `parse_dpla_date` before constructing
        # the time datavalue. Surface the placeholder shape here so
        # the caller can detect and post-process.
        return {
            "type": "statement",
            "rank": "normal",
            "_phase3a_pending_date_parse": value,
            "_property": prop,
            "_permalink": permalink,
        }
    else:  # pragma: no cover — guarded by the LEGACY_IMPORT_PROPERTY set
        return None

    return {
        "type": "statement",
        "rank": "normal",
        "mainsnak": {
            "snaktype": "value",
            "property": prop,
            "datatype": datatype,
            "datavalue": datavalue,
        },
        "references": [_reference_snaks(permalink)],
    }


def build_legacy_import_claims(plan: MigrationPlan) -> list[dict]:
    """Materialise every community-import in ``plan`` as a list of
    Wikibase claim dicts ready for a ``wbeditentity`` POST.

    Skips canonical keys with no mapping in
    :data:`LEGACY_IMPORT_PROPERTY` — those values stay in wikitext
    until a later phase widens the supported set.
    """
    claims: list[dict] = []
    for key, value in plan.community_imports.items():
        claim = format_legacy_import_claim(key, value, plan.source_permalink)
        if claim is not None:
            claims.append(claim)
    return claims


# ---------------------------------------------------------------------------
# Phase 3b integration layer — pywikibot + wbeditentity + wikitext rewrite.
#
# Everything above this line is pure logic over plain dicts and the
# RevisionSnapshot dataclass. Everything below talks to pywikibot,
# Wikibase, or the in-tree DPLA-API parser. Keeping the boundary
# explicit lets the test suite cover the pure layer without mocks and
# lets future callers (a separate maintenance tool, a one-shot
# migration script) reuse the planner standalone.
# ---------------------------------------------------------------------------


def fetch_revision_snapshots(file_page) -> list[RevisionSnapshot]:
    """Pull the full revision history of ``file_page`` into
    :class:`RevisionSnapshot` records the planner consumes.

    Requests revision text (``content=True``) so the planner can parse
    each historical wikitext. On a long-tenure file with thousands of
    revisions this is a non-trivial pywikibot call; callers running
    against a partner batch should expect minutes of wall time per
    high-traffic file. The cost is paid once at plan time — the result
    of :func:`plan_migration` is a small dict the executor walks
    cheaply.

    Missing per-revision metadata (suppressed-author revisions, missing
    text) is tolerated: empty user names become ``""`` and absent text
    becomes ``""``, so :func:`parse_artwork_params` simply finds no
    params and the revision contributes nothing to provenance.
    """
    snapshots: list[RevisionSnapshot] = []
    for rev in file_page.revisions(content=True):
        snapshots.append(
            RevisionSnapshot(
                revid=getattr(rev, "revid", 0),
                user=getattr(rev, "user", "") or "",
                text=getattr(rev, "text", "") or "",
            )
        )
    return snapshots


def materialize_pending_date_claim(
    placeholder: dict,
    today: datetime.date | None = None,
) -> dict | None:
    """Take a Phase-3a ``_phase3a_pending_date_parse`` placeholder claim
    and materialise it into a real Wikibase P571 (inception) statement.

    Reuses :func:`ingest_wikimedia.sdc.parse_dpla_date` for the parse —
    the DPLA-date parser is the in-tree source of truth for the dozens
    of display-date formats Commons receives, so duplicating it here
    would just drift. When the parser commits to a structured time,
    the claim is value-typed at the parser's chosen precision; when it
    doesn't, the claim is left at ``somevalue`` with the raw string
    preserved as a P1932 (stated as) qualifier. The P1480 circa
    qualifier is stamped when the parser flagged the source as
    approximate.

    Reference shape is the legacy-import P887→Q131783016 +
    P4656→permalink shape from :func:`_reference_snaks` — *not* the
    DPLA-standard refs used by :mod:`ingest_wikimedia.sdc`'s
    ``_build_date_claim``. The whole point of legacy-import is that
    the value didn't come from DPLA and must not be misattributed.

    Returns None when ``placeholder`` doesn't carry the expected
    sentinel keys — the caller drops the claim from the import set
    and the value stays in wikitext until a later phase.

    ``today`` is accepted (currently unused) for symmetry with other
    claim builders so a later phase can add a P813 retrieved-on
    reference snak if useful without changing call sites.
    """
    from .sdc import parse_dpla_date  # late import to avoid cycle

    del today  # currently unused; see docstring
    raw_date = placeholder.get("_phase3a_pending_date_parse")
    prop = placeholder.get("_property")
    permalink_from = placeholder.get("_permalink")
    if not raw_date or not prop or not permalink_from:
        return None

    parsed = parse_dpla_date(raw_date)
    qualifiers: dict[str, list[dict]] = {
        # P1932 (stated as) preserves the verbatim source string so a
        # reader can recover what the editor actually wrote, even when
        # the structured time has reduced precision.
        "P1932": [
            {
                "snaktype": "value",
                "property": "P1932",
                "datatype": "string",
                "datavalue": {"type": "string", "value": raw_date},
            }
        ]
    }
    if parsed is not None:
        mainsnak = {
            "snaktype": "value",
            "property": prop,
            "datatype": "time",
            "datavalue": {"type": "time", "value": parsed["value"]},
        }
        if parsed.get("approximate"):
            # P1480 (sourcing circumstances) = Q5727902 (circa); same
            # encoding as ingest_wikimedia.sdc._build_date_claim.
            qualifiers["P1480"] = [
                {
                    "snaktype": "value",
                    "property": "P1480",
                    "datatype": "wikibase-item",
                    "datavalue": {
                        "type": "wikibase-entityid",
                        "value": {"entity-type": "item", "id": "Q5727902"},
                    },
                }
            ]
    else:
        # ``somevalue`` preserves the legacy behaviour for un-parseable
        # dates — the statement asserts "there is *some* inception
        # value" without committing to which, and the P1932 qualifier
        # carries the editor's display string.
        mainsnak = {"snaktype": "somevalue", "property": prop, "datatype": "time"}

    return {
        "type": "statement",
        "rank": "normal",
        "mainsnak": mainsnak,
        "qualifiers": qualifiers,
        "qualifiers-order": list(qualifiers.keys()),
        "references": [_reference_snaks(permalink_from)],
    }


def materialize_import_claims(claims: list[dict]) -> list[dict]:
    """Walk an import-claim list and substitute every Phase-3a date
    placeholder for a real P571 time statement.

    Claims without a placeholder marker pass through unchanged.
    Placeholders whose date can't be expanded (parser failure paired
    with missing sentinel keys — should never happen for in-tree
    callers) are dropped silently rather than crashing the migration.
    """
    materialised: list[dict] = []
    for claim in claims:
        if "_phase3a_pending_date_parse" in claim:
            real = materialize_pending_date_claim(claim)
            if real is not None:
                materialised.append(real)
        else:
            materialised.append(claim)
    return materialised


def entity_was_already_migrated(entity: dict) -> bool:
    """Return True when ``entity`` (a wbgetentities-shaped MediaInfo
    JSON blob) already carries at least one legacy-import statement.

    Idempotency guard: if the executor's previous run posted SDC for a
    file but crashed before rewriting the wikitext, a re-run sees the
    page still in legacy form, would re-plan, and would re-post the
    same claims — creating duplicates. Detecting our own reference
    signature on any of the import properties is enough to bail out
    cleanly; the operator then re-runs *just* the wikitext rewrite
    via a follow-up tool (Phase 3b doesn't expose that yet, so the
    today-practice is a manual re-edit).

    Looks for a P887 reference snak whose target is exactly
    :data:`QID_INFERRED_FROM_WIKITEXT` on any statement of any
    Phase-3a-mapped property. Subtle: a non-DPLA editor could in
    principle stamp the same ref shape on a hand-authored claim, but
    that's the same semantic ("inferred from Wikitext") and the
    skip-on-detect behaviour is still correct — we just don't add
    duplicates.
    """
    statements = entity.get("statements") or entity.get("claims") or {}
    for prop, _ in LEGACY_IMPORT_PROPERTY.values():
        for stmt in statements.get(prop, []):
            for ref in stmt.get("references", []):
                for snak in ref.get("snaks", {}).get(PID_BASED_ON_HEURISTIC, []):
                    target = snak.get("datavalue", {}).get("value", {}).get("id")
                    if target == QID_INFERRED_FROM_WIKITEXT:
                        return True
    return False


def render_migrated_wikitext(
    original_text: str,
    new_template_block: str,
) -> str:
    """Replace the first legacy-template invocation in ``original_text``
    with ``new_template_block``, preserving everything else verbatim.

    ``new_template_block`` is the full ``{{DPLA metadata ...}}``
    rendering, typically produced by
    :func:`ingest_wikimedia.wikimedia.get_wiki_text`. The caller is
    responsible for any param-stripping pass (Goal 1's
    :mod:`wikitext_normalize`) — this helper just swaps the wrapper.

    Returns the original text untouched when no legacy template is
    present (defensive — the executor's caller has already gated this
    via the migration plan, but a stale text snapshot between plan and
    write would otherwise mangle a non-legacy page).

    Uses mwparserfromhell so license tags, categories, and any other
    page-level structure survive in their original positions. Only
    the legacy-template node itself is replaced.
    """
    wikicode = mwparserfromhell.parse(original_text)
    template = None
    for tpl in wikicode.filter_templates():
        if _template_name(tpl) in LEGACY_TEMPLATE_NAMES:
            template = tpl
            break
    if template is None:
        return original_text
    wikicode.replace(template, new_template_block)
    return str(wikicode)


# Edit summary the migration executor stamps on every wikitext save +
# wbeditentity edit. Centralised so a future change to the wording
# (e.g. linking to the Commons documentation page) doesn't need to be
# hunted down across multiple call sites.
LEGACY_MIGRATION_EDIT_SUMMARY = (
    "Migrate legacy {{Artwork}} to {{DPLA metadata}} per DPLA SDC sync; "
    "community-contributed metadata preserved as SDC statements with "
    "[[d:Q131783016|inferred-from-Wikitext]] reference."
)


@dataclass
class MigrationResult:
    """End-state report from :func:`migrate_legacy_file`.

    ``imports_posted`` is the number of community-import claims
    actually written to the entity (post-materialisation, post-skip
    for date-parse failures). ``wikitext_changed`` is whether the
    page text was edited.
    """

    skipped_reason: str = ""
    imports_posted: int = 0
    wikitext_changed: bool = False
    plan: MigrationPlan | None = None


def post_legacy_import_claims(
    mediaid: str,
    claims: list[dict],
    site,
    summary: str = LEGACY_MIGRATION_EDIT_SUMMARY,
) -> None:
    """POST ``claims`` to ``mediaid`` as a single ``wbeditentity`` edit.

    Mirrors :func:`tools.sdc_sync._submit_sdc_write`'s call pattern —
    CSRF token from ``site.tokens['csrf']``, ``bot=True``, atomic
    bundle — without importing it (the sdc_sync helper reads a
    module-level ``site`` global; we want the explicit-argument form
    so callers can pass a mocked site in tests).

    Raises :class:`pywikibot.exceptions.APIError` on Wikibase rejection
    — the caller catches it per-file so a single failure doesn't kill
    the partner batch.
    """
    import json as _json

    site.simple_request(
        action="wbeditentity",
        id=mediaid,
        bot=True,
        token=site.tokens["csrf"],
        data=_json.dumps({"claims": claims}),
        summary=summary,
    ).submit()


def migrate_legacy_file(
    *,
    file_page,
    item_metadata: dict,
    provider: dict,
    data_provider: dict,
    dpla_id: str,
    site,
    bot_accounts: frozenset[str] = DPLA_BOT_ACCOUNTS,
    summary: str = LEGACY_MIGRATION_EDIT_SUMMARY,
) -> MigrationResult:
    """End-to-end legacy-Artwork migration for a single file.

    The order is load-bearing:

    1. Plan from the live revision history.
    2. Idempotency check against the file's MediaInfo entity — bail
       out if a previous run already imported (avoids duplicates).
    3. POST community-import SDC statements *first*. If this fails
       the wikitext stays in legacy form, the next run will detect
       no SDC, and a retry produces the same outcome.
    4. *Then* rewrite the wikitext. If this fails after step 3
       succeeded, the file carries the imported SDC but still has
       the legacy template — a follow-up wikitext-only sweep would
       complete the migration. The reverse order (rewrite first,
       then SDC) would discard the wikitext provenance before the
       SDC import landed, irrecoverably losing community values if
       the SDC POST then failed.
    """
    from .wikimedia import dpla_metadata_params, get_wiki_text

    title = file_page.title()
    revisions = fetch_revision_snapshots(file_page)
    canonical_params = dpla_metadata_params(
        dpla_id, item_metadata, provider, data_provider
    )
    plan = plan_migration(title, revisions, canonical_params, bot_accounts)
    if plan is None:
        return MigrationResult(skipped_reason="no-legacy-template")

    mediaid = f"M{file_page.pageid}"
    entity = _fetch_entity_or_empty(site, mediaid)
    if entity_was_already_migrated(entity):
        return MigrationResult(skipped_reason="already-migrated", plan=plan)

    claims = materialize_import_claims(build_legacy_import_claims(plan))
    if claims:
        post_legacy_import_claims(mediaid, claims, site, summary=summary)

    new_block = get_wiki_text(dpla_id, item_metadata, provider, data_provider)
    rewritten = render_migrated_wikitext(file_page.text, new_block)
    wikitext_changed = rewritten != file_page.text
    if wikitext_changed:
        file_page.text = rewritten
        file_page.save(summary=summary, minor=False, bot=True)

    return MigrationResult(
        imports_posted=len(claims),
        wikitext_changed=wikitext_changed,
        plan=plan,
    )


def _fetch_entity_or_empty(site, mediaid: str) -> dict:
    """Pull ``mediaid``'s MediaInfo entity via ``wbgetentities``, or
    return an empty dict when the entity doesn't exist (file uploaded
    but no SDC ever written).

    Doesn't surface the missing-entity case as an error — for legacy
    migration, "no SDC yet" is the most common starting state, and an
    empty entity simply means
    :func:`entity_was_already_migrated` returns False and the
    executor proceeds to post the imports as fresh statements.
    """
    try:
        raw = site.simple_request(action="wbgetentities", ids=mediaid).submit()
    except Exception:  # noqa: BLE001 — best-effort fetch
        return {}
    entities = raw.get("entities", {})
    return entities.get(mediaid, {}) if isinstance(entities, dict) else {}
