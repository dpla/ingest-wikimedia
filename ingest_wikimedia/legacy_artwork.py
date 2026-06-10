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
    value; permalinks are required by the property's usage notes."""
    encoded = file_title.replace(" ", "_")
    return f"https://commons.wikimedia.org/w/index.php?title={encoded}&oldid={oldid}"


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
