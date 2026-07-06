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
import json
import re
from dataclasses import dataclass, field
from typing import Iterable
from urllib.parse import urlencode

import mwparserfromhell

from ingest_wikimedia.csrf import with_csrf_recovery
from ingest_wikimedia.sdc import (
    CASEFOLD_COMPARE_KEYS,
    casefold_for_compare,
    dates_semantically_equal,
    is_wikitext_junk_value,
    parse_date_range,
    parse_dpla_date,
    unescape_wikitext_magic_words,
)

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
    stripped (matching the renderer's behavior) and character-escape
    magic words (``{{!}}`` / ``{{=}}`` / …) are un-escaped to the
    literal characters they render as — a community editor's AWB pass
    that swapped ``|`` for ``{{!}}`` inside a param value is a display
    no-op and must be treated as one by both the provenance walk
    (crediting the AWB edit for a real content change would misclassify
    the value as community-contributed) and by downstream SDC-storage
    paths (a stored value that literally contains ``{{!}}`` is nonsense
    outside a template context). Unrecognised param names are dropped
    silently — those don't have a canonical target and the wikitext-
    rewrite step preserves them by leaving the template untouched if
    the param wasn't migrated.

    Repairs literal-``|`` truncation of named-param values. Legacy
    ``{{Artwork}}`` uploads sometimes wrote pipe-separated subject
    lists directly into ``| description = ... buildings | 633 Evesham
    | Dwellings | ...`` without escaping. ``mwparserfromhell`` treats
    each such ``|`` as a parameter separator and truncates the named
    ``description`` value at the first pipe, spilling the rest into
    anonymous positional params (``1``, ``2``, ``3``, …). The legacy
    ``{{Artwork}}`` / ``{{Information}}`` / ``{{Photograph}}``
    templates take only named params by convention, so any anonymous
    positional at the top level is overflow from an earlier named
    value; we stitch each back onto the preceding named param with a
    literal ``|`` rejoin. Without this repair, a later AWB pass that
    rewrote the raw ``|`` to ``{{!}}`` looks like a content change to
    the provenance walker (Rev N-1 parse ends at first pipe; Rev N
    parse gets the full value), and the description gets misattributed
    as community-contributed on migration.
    """
    template = find_legacy_template(wikitext)
    if template is None:
        return {}
    # Stitch: iterate template params, accumulating overflow positional
    # values into the preceding named param's raw value. Track whether
    # the last named param was RECOGNISED so overflow from an
    # unrecognised param (e.g. ``| Other fields 1 = X | Y | title = …``)
    # is dropped rather than misattributed to the previous recognised
    # named entry earlier in the template.
    stitched: list[list[str]] = []
    last_named_recognised = False
    for param in template.params:
        if not param.showkey:
            if last_named_recognised and stitched:
                stitched[-1][1] += "|" + str(param.value)
            continue
        canonical = ARTWORK_PARAM_TO_CANONICAL_KEY.get(_normalize_param_name(param))
        if canonical is None:
            last_named_recognised = False
            continue
        stitched.append([canonical, str(param.value)])
        last_named_recognised = True

    parsed: dict[str, str] = {}
    for canonical, raw in stitched:
        value = unescape_wikitext_magic_words(raw.strip())
        if not value or is_wikitext_junk_value(value):
            # Empty values and wikitext-extraction junk (a stray ``;`` in
            # a date field, ``--`` in a title, etc. — see
            # :func:`is_wikitext_junk_value`) aren't useful provenance
            # to import. Skip them same as we skip missing params so
            # the migrator doesn't preserve markup errors as SDC.
            continue
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
        if classified.get(key) == "community" and not _value_equivalent_to_canonical(
            key, value, canonical_value
        ):
            # Only import community values that *differ* from canonical
            # — a community editor restating DPLA's title verbatim is
            # not an import we want to record (it's redundant). Same
            # invariant as Goal 1: if it matches, it's strip-eligible.
            # Equivalence is checked semantically for the ``date`` key
            # (via :func:`dates_semantically_equal`) and via casefold +
            # punctuation-trim for the display-string keys — so a
            # community edit that reformatted DPLA's own value is not
            # imported as a spurious community contribution.
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


# Matches a bare ``{{Institution|wikidata=Q...}}`` sub-template value.
# Case-insensitive on the template name and param key so hand-typed
# variants (``institution`` / ``Institution``, ``Wikidata`` / ``wikidata``)
# both parse cleanly to the inner Q-ID.
_INSTITUTION_SUBTEMPLATE_RE = re.compile(
    r"^\s*\{\{\s*Institution\s*\|\s*wikidata\s*=\s*(Q\d+)\s*\}\}\s*$",
    re.IGNORECASE,
)


def _extract_institution_qid(value: str) -> str | None:
    """Return the inner Q-ID from a wikitext ``institution`` param
    value, or ``None`` if the value isn't a bare Q-ID or an
    ``{{Institution|wikidata=Q...}}`` sub-template.

    The legacy ``{{Artwork}}`` shape wraps the Q-ID in a sub-template
    (``| Institution = {{Institution|wikidata=Q59661041}}``); the flat
    ``{{DPLA metadata}}`` shape stores it as a bare Q-ID (``| institution
    = Q59661041``). This helper normalises both so the migration
    planner's equivalence check works regardless of which shape the
    legacy wikitext carried.
    """
    if not value:
        return None
    stripped = value.strip()
    m = _INSTITUTION_SUBTEMPLATE_RE.match(stripped)
    if m:
        return m.group(1)
    if re.fullmatch(r"Q\d+", stripped):
        return stripped
    return None


def _value_equivalent_to_canonical(key: str, value: str, canonical: str) -> bool:
    """Return True when ``value`` (from wikitext) and ``canonical`` (from
    DPLA) are the same fact for the purpose of migration-planning.

    Exact string equality is the primary check. For the display-string
    keys (``title``/``description``/``date``/``permission``/``creator``)
    a casefold + leading/trailing-punctuation-trim comparator widens
    the tolerance so a community edit whose only difference from
    canonical is punctuation or case is not imported as a spurious
    community contribution. For the ``date`` key specifically the
    semantic date-equivalence check runs first — an override like
    ``19 November 1902`` collapses cleanly against ``1902-11-19``.

    For ``institution`` the wikitext value may be a bare Q-ID (flat
    ``{{DPLA metadata}}`` shape) or the legacy sub-template
    ``{{Institution|wikidata=Q...}}`` shape; both extract to a plain
    Q-ID that is byte-compared to the canonical DPLA institution Q-ID.
    This closes the case where a legacy NARA file's ``Institution``
    sub-template holds the (custodial-unit) Q-ID that DPLA now emits as
    ``institution`` in canonical params, but where a wrap difference
    prevents plain equality from firing.

    Keys outside those sets (other sub-template shapes) fall back to
    strict equality.
    """
    if value == canonical:
        return True
    if key == "date" and dates_semantically_equal(value, canonical):
        return True
    if key == "institution":
        wiki_qid = _extract_institution_qid(value)
        canon_qid = _extract_institution_qid(canonical) or canonical.strip()
        if wiki_qid and wiki_qid == canon_qid:
            return True
    if key in CASEFOLD_COMPARE_KEYS:
        folded_value = casefold_for_compare(value)
        folded_canonical = casefold_for_compare(canonical)
        if folded_value and folded_value == folded_canonical:
            return True
    return False


def _canonical_value_for_key(canonical_params: dict, key: str) -> str:
    """Pull the comparable canonical value for a key.

    title/description/date/permission/creator/institution are all scalar
    strings in ``dpla_metadata_params``'s output — institution is a bare
    Q-ID (``data_provider_wiki_q``), the rest come from
    ``extract_strings``.  Other sub-template-valued keys (``source``,
    etc.) are not handled in Phase 3a's scalar mapping — return empty
    so the equality check classifies them as "different" and they fall
    through to community-import or dpla-originated based on provenance
    only.
    """
    if key in ("title", "description", "date", "permission", "creator", "institution"):
        return str(canonical_params.get(key, ""))
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


_QID_HEURISTIC = "Q61848113"
_QID_CIRCA = "Q5727902"
_PID_DPLA_DETERMINATION = "P459"
_PID_SOURCING_CIRCUMSTANCES = "P1480"


def _expand_wikitext_for_date_parse(raw_date: str, site) -> str:
    """If ``raw_date`` contains MediaWiki template markup, expand
    templates server-side and return the rendered text. Otherwise
    pass through unchanged.

    The legacy ``{{Artwork}}`` ``| date = …`` value can legitimately
    carry editor-applied template wrappers — most commonly
    ``{{other date|~|1911}}``, ``{{circa|1911}}``, ``{{date|1911}}``
    etc. Storing the raw markup in the SDC ``P1932`` qualifier (1)
    defeats the dedup check below, because the renderer-style
    "circa 1911" string never compares equal to "{{other date|~|
    1911}}", and (2) renders as literal template text in
    Module:DPLA's yellow-box row (Module:DPLA mitigates this at
    display time but the upstream fix is to not store wikitext in a
    structured-data string in the first place).

    Falls back to the raw value when ``site`` is None, when no
    template markup is detected (fast path), or when the API call
    raises. The HTML residue that MediaWiki sometimes appends to a
    template's plain-text rendering (e.g. the hidden
    ``<div ...>date QS:P,…</div>`` micro-format ``{{other date}}``
    emits) is stripped before return — that scaffolding is for the
    rendered page, not for a normalised value string.
    """
    if not raw_date or "{{" not in raw_date or site is None:
        return raw_date
    try:
        expanded = site.expand_text(raw_date)
    except Exception:
        return raw_date
    # Targeted-strip pass: ONLY remove paired tags whose attributes mark
    # them as hidden microformat scaffolding — ``style="display: none"``
    # / ``aria-hidden="true"`` / a QuickStatements ``qs`` class. Visible
    # text inside ordinary ``<span>``/``<i>``/``<sup>`` wrappers (e.g.
    # ``<span>circa 1911</span>`` from a language- or formatting-
    # focused template) must survive — only the QS / display-none
    # noise the date templates inject for downstream tooling should
    # disappear. Generic tag-strip on the second pass removes the
    # remaining bare tags but preserves their inner text.
    expanded = re.sub(
        r"<(span|div)[^>]*"
        r"(?:style\s*=\s*[\"'][^\"']*display\s*:\s*none[^\"']*[\"']"
        r"|aria-hidden\s*=\s*[\"']true[\"']"
        r"|class\s*=\s*[\"'][^\"']*\bqs\b[^\"']*[\"'])"
        r"[^>]*>.*?</\1>",
        "",
        expanded,
        flags=re.DOTALL | re.IGNORECASE,
    )
    expanded = re.sub(r"<[^>]+/?>", "", expanded)
    return expanded.strip() or raw_date


def _iter_dpla_attributed_statements(entity: dict | None, prop: str):
    """Yield every DPLA-attributed statement on ``prop`` in ``entity``.

    "DPLA-attributed" means the statement carries a
    ``P459 = Q61848113`` (determination method = heuristic) qualifier —
    the same signal Module:DPLA's ``dplaStatements`` uses to scope the
    blue-box render. Both date matchers (structured time and range)
    walk the same set of DPLA-attributed statements; this generator
    keeps that contract in one place so a future change to the
    attribution rule applies to both.
    """
    if not entity:
        return
    statements = entity.get("statements") or entity.get("claims") or {}
    for stmt in statements.get(prop, []):
        for q in (stmt.get("qualifiers") or {}).get(_PID_DPLA_DETERMINATION, []):
            qv = q.get("datavalue", {}).get("value", {}) or {}
            if qv.get("id") == _QID_HEURISTIC:
                yield stmt
                break


def _existing_dpla_date_matches_parsed(
    entity: dict | None, prop: str, parsed: dict
) -> bool:
    """Return True iff ``entity`` already has a DPLA-attributed
    statement on ``prop`` whose Wikibase time value and circa-flag
    match ``parsed``.

    The check deliberately doesn't compare reference shape: the editor-
    edited legacy value is a different source than DPLA's own sync, but
    if the *semantic* value and approximate-flag match, there's no
    user-contributed information to preserve — the editor's edit was
    just a re-formatting of the DPLA value.

    Match contract: ``time`` string and ``precision`` integer must
    be exactly equal, AND ``parsed.approximate`` must agree with
    whether the existing statement carries a
    ``P1480 = Q5727902`` (circa) qualifier. Wikibase normalises the
    time string at write time, so byte equality is the right test —
    same-day with different ``before``/``after`` etc. would represent
    a different uncertainty bound and should not be deduped.
    """
    if not parsed or not parsed.get("value"):
        return False
    parsed_value = parsed["value"]
    if not parsed_value.get("time") or parsed_value.get("precision") is None:
        return False
    parsed_circa = bool(parsed.get("approximate"))
    for stmt in _iter_dpla_attributed_statements(entity, prop):
        ms = stmt.get("mainsnak") or {}
        dv = ms.get("datavalue") or {}
        if dv.get("type") != "time":
            continue
        # Whole-value comparison (time, precision, before, after,
        # timezone, calendarmodel) — divergence in any field implies
        # a semantically different statement and must NOT dedup. Both
        # sides go through ``_wikibase_time`` in ``ingest_wikimedia.sdc``,
        # which pins ``before/after/timezone/calendarmodel`` to constants,
        # so under normal pipeline use the dicts match exactly; a
        # hand-edited statement with explicit uncertainty bounds would
        # represent a different precision claim and is correctly
        # excluded.
        if (dv.get("value") or {}) != parsed_value:
            continue
        existing_circa = False
        for q in (stmt.get("qualifiers") or {}).get(_PID_SOURCING_CIRCUMSTANCES, []):
            qv = q.get("datavalue", {}).get("value", {}) or {}
            if qv.get("id") == _QID_CIRCA:
                existing_circa = True
                break
        if existing_circa == parsed_circa:
            return True
    return False


def _existing_dpla_date_range_matches(
    entity: dict | None, prop: str, range_value: tuple[int, int]
) -> bool:
    """Mirror of :func:`_existing_dpla_date_matches_parsed` for range-
    shaped claims. Returns True when ``entity`` already carries a
    DPLA-attributed statement on ``prop`` whose ``P1932`` stated-as
    qualifier parses to the same ``(start_year, end_year)`` range.

    ``parse_dpla_date`` returns None for any multi-year range, so a
    range value can never land as a structured time and the existing
    parsed-time matcher never fires for it. Without this fallback, the
    legacy migration emits an inferred-from-Wikitext range claim
    parallel to the DPLA one — see Group_Portrait_of_"Indians" Mission
    Grove (M193555788) where ``{{other date|between|1934|1948}}`` was
    preserved alongside DPLA's ``"1934 - 1948"`` as two P571 statements.

    Match contract: both sides go through
    :func:`ingest_wikimedia.sdc.parse_date_range` and must produce the
    same ``(start, end)`` tuple — direction-agnostic, since the helper
    canonicalises ``(min, max)``.
    """
    if not range_value:
        return False
    for stmt in _iter_dpla_attributed_statements(entity, prop):
        # Range claims must be ``somevalue`` mainsnak — a value-typed
        # time can't represent a multi-year range, so any DPLA range
        # statement is encoded as somevalue + P1932 stated-as.
        if (stmt.get("mainsnak") or {}).get("snaktype") != "somevalue":
            continue
        for q in (stmt.get("qualifiers") or {}).get("P1932", []):
            qv = q.get("datavalue") or {}
            if qv.get("type") != "string":
                continue
            if parse_date_range(qv.get("value") or "") == range_value:
                return True
    return False


def materialize_pending_date_claim(
    placeholder: dict,
    today: datetime.date | None = None,
    *,
    site=None,
    existing_entity: dict | None = None,
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

    When ``site`` is provided, any template markup in the raw value
    is expanded via MediaWiki's ``expandtemplates`` API before
    parsing AND before being stored in the P1932 qualifier — see
    :func:`_expand_wikitext_for_date_parse`. The DPLA-date parser
    can't structurally read shapes like ``{{other date|~|1911}}``,
    but DOES recognise the expanded ``circa 1911`` form, so the
    expansion turns the somevalue fallback into a real value-typed
    statement and avoids storing wikitext markup in a structured-
    data string. Falls back to the raw value when no ``site`` is
    passed or expansion fails.

    When ``existing_entity`` is provided and the parser commits to a
    structured value, the function checks whether ``entity`` already
    carries a DPLA-attributed statement on the same property with the
    same time + precision + circa flag — see
    :func:`_existing_dpla_date_matches_parsed`. If so, returns None
    to signal that the legacy import is a no-op duplicate (the
    editor merely re-formatted the DPLA-supplied value as a wikitext
    template; no community-contributed information would be lost by
    dropping the import). Callers that don't provide
    ``existing_entity`` get the pre-existing always-import behaviour.

    ``today`` is accepted (currently unused) for symmetry with other
    claim builders so a later phase can add a P813 retrieved-on
    reference snak if useful without changing call sites.
    """
    del today  # currently unused; see docstring
    raw_date = placeholder.get("_phase3a_pending_date_parse")
    prop = placeholder.get("_property")
    permalink_from = placeholder.get("_permalink")
    if not raw_date or not prop or not permalink_from:
        return None

    # Expand any template markup so the value reflects what the editor
    # actually saw on the page, not the raw wikitext syntax. The
    # expanded form goes into both the parser and the P1932 qualifier;
    # the raw markup is never stored as structured data.
    value_for_parse = _expand_wikitext_for_date_parse(raw_date, site)
    parsed = parse_dpla_date(value_for_parse)

    # Dedup-against-DPLA: if the editor's wikitext value parses to
    # something we already have on the entity as a DPLA-attributed
    # claim, the migration import would be a literal duplicate. Drop
    # it rather than create a parallel statement that the
    # community-imports comparator can never strip cleanly.
    if parsed is not None and _existing_dpla_date_matches_parsed(
        existing_entity, prop, parsed
    ):
        return None
    # Same check for the range case — ``parse_dpla_date`` returns None
    # for any multi-year range, so the structured-time matcher above
    # never fires for shapes like ``{{other date|between|1934|1948}}``.
    # ``parse_date_range`` produces a canonical ``(start, end)`` key
    # that compares equal across "1934 - 1948", "between 1934 and 1948",
    # and the raw-wikitext fallback, so the inferred import dedupes
    # cleanly against a DPLA somevalue+P1932 range claim.
    range_value = parse_date_range(value_for_parse)
    if range_value is not None and _existing_dpla_date_range_matches(
        existing_entity, prop, range_value
    ):
        return None

    qualifiers: dict[str, list[dict]] = {
        # P1932 (stated as) preserves the source string so a reader can
        # recover what the editor actually wrote, even when the
        # structured time has reduced precision. The pre-expansion
        # form is what surfaces here when no expansion ran (no site,
        # no template markup, or expand_text failed).
        "P1932": [
            {
                "snaktype": "value",
                "property": "P1932",
                "datatype": "string",
                "datavalue": {"type": "string", "value": value_for_parse},
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


def materialize_import_claims(
    claims: list[dict],
    *,
    site=None,
    existing_entity: dict | None = None,
) -> list[dict]:
    """Walk an import-claim list and substitute every Phase-3a date
    placeholder for a real P571 time statement.

    Claims without a placeholder marker pass through unchanged.
    Placeholders whose date can't be expanded (parser failure paired
    with missing sentinel keys — should never happen for in-tree
    callers) are dropped silently rather than crashing the migration.

    ``site`` and ``existing_entity`` are forwarded to
    :func:`materialize_pending_date_claim` so the date materialiser
    can (a) expand wikitext templates server-side before parsing and
    (b) drop a community-import claim whose parsed value already
    exists DPLA-attributed on the entity. Both are optional — when
    neither is supplied, the function preserves the historical
    always-import behaviour. The original caller signature is also
    preserved for tests that pass only the claims list.
    """
    materialised: list[dict] = []
    for claim in claims:
        if "_phase3a_pending_date_parse" in claim:
            real = materialize_pending_date_claim(
                claim, site=site, existing_entity=existing_entity
            )
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


def _extract_dpla_metadata_template(block: str) -> str:
    """Pull just the ``{{DPLA metadata ...}}`` template out of
    ``block``. ``block`` is typically the full upload-form output of
    :func:`ingest_wikimedia.wikimedia.get_wiki_text`, which includes a
    leading ``== {{int:filedesc}} ==`` heading and a blank-line
    separator. The migration rewrite only needs the template invocation
    itself — the section heading already exists in the page being
    migrated, and substituting the full block in place of ``{{Artwork}}``
    duplicates the heading (the original above the legacy template
    survives, and the new heading lands inline where the template was).

    Falls back to the original block unchanged when no
    ``{{DPLA metadata}}`` template is found in it — a defensive
    no-op for callers that already pass just the template.
    """
    wikicode = mwparserfromhell.parse(block)
    for tpl in wikicode.filter_templates():
        if _template_name(tpl) == "dpla metadata":
            return str(tpl)
    return block


def render_migrated_wikitext(
    original_text: str,
    new_template_block: str,
) -> str:
    """Replace the first legacy-template invocation in ``original_text``
    with the ``{{DPLA metadata}}`` template invocation extracted from
    ``new_template_block``, preserving everything else verbatim.

    ``new_template_block`` is the full ``{{DPLA metadata ...}}``
    rendering as produced by
    :func:`ingest_wikimedia.wikimedia.get_wiki_text` — including the
    section heading and blank-line separator that's only relevant for
    new uploads. Only the inner template invocation is substituted
    here; the original ``== {{int:filedesc}} ==`` heading above the
    legacy template stays put. The caller is responsible for any
    param-stripping or whitespace canonicalisation pass
    (:mod:`wikitext_normalize`) — this helper just swaps the wrapper.

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
    replacement = _extract_dpla_metadata_template(new_template_block)
    wikicode.replace(template, replacement)
    return str(wikicode)


# Edit summary the migration executor stamps on every wikitext save +
# wbeditentity edit. Centralised so a future change to the wording
# (e.g. linking to the Commons documentation page) doesn't need to be
# hunted down across multiple call sites.
#
# ``LEGACY_MIGRATION_EDIT_SUMMARY`` is the with-community-claims form;
# the base form (no community-preservation clause) is used when every
# wikitext value already matched the DPLA-canonical value, so the
# migration produced zero community-import claims. ``build_migration_summary``
# picks the right form for the actual edit, so the wikitext save's
# summary doesn't promise SDC-preservation behaviour that didn't fire.
LEGACY_MIGRATION_BASE_SUMMARY = (
    "Migrate legacy {{Artwork}} to {{DPLA metadata}} per DPLA SDC sync."
)
LEGACY_MIGRATION_EDIT_SUMMARY = (
    LEGACY_MIGRATION_BASE_SUMMARY
    + " Community-contributed metadata preserved as SDC statements with "
    "[[d:Q131783016|inferred-from-Wikitext]] reference."
)


def build_migration_summary(community_claim_count: int) -> str:
    """Return the edit summary describing what this migration did.

    Pass the number of community-import claims actually posted on
    this file. Zero (DPLA-bot-only history, every wikitext value
    already DPLA-canonical) → :data:`LEGACY_MIGRATION_BASE_SUMMARY`.
    Non-zero → :data:`LEGACY_MIGRATION_EDIT_SUMMARY` with the
    community-preservation clause appended.
    """
    if community_claim_count <= 0:
        return LEGACY_MIGRATION_BASE_SUMMARY
    return LEGACY_MIGRATION_EDIT_SUMMARY


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
    the partner batch. Wrapped in :func:`with_csrf_recovery` so an
    invalidated session (``KeyError: Invalid token 'csrf'``) triggers
    a refresh + retry rather than bubbling up as a per-file failure
    for every remaining item — session-level fatals raise
    :class:`CsrfRecoveryFailed` to abort the run instead.
    """
    with_csrf_recovery(
        site,
        f"wbeditentity {mediaid} (legacy-artwork import)",
        lambda: site.simple_request(
            action="wbeditentity",
            id=mediaid,
            bot=True,
            token=site.tokens["csrf"],
            data=json.dumps({"claims": claims}),
            summary=summary,
        ).submit(),
    )


def migrate_legacy_file(
    *,
    file_page,
    item_metadata: dict,
    provider: dict,
    data_provider: dict,
    dpla_id: str,
    site,
    bot_accounts: frozenset[str] = DPLA_BOT_ACCOUNTS,
    summary: str | None = None,
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

    # Pass the just-fetched MediaInfo entity and the site through so the
    # date materialiser can expand wikitext templates server-side AND
    # skip importing a date that already exists DPLA-attributed on the
    # entity (e.g. an editor reformatted the original ``1911?`` value
    # as ``{{other date|~|1911}}`` — both parse to "circa 1911" so the
    # legacy import would be a literal duplicate of the DPLA claim).
    claims = materialize_import_claims(
        build_legacy_import_claims(plan), site=site, existing_entity=entity
    )
    # Pick the accurate summary for what this migration actually did,
    # unless the caller supplied a custom one. Without this, a
    # DPLA-bot-only history (no community values to import) would still
    # carry the boilerplate "community-contributed metadata preserved"
    # clause on its wikitext save, promising SDC-preservation behaviour
    # that didn't fire on this file.
    if summary is None:
        summary = build_migration_summary(len(claims))
    if claims:
        post_legacy_import_claims(mediaid, claims, site, summary=summary)

    new_block = get_wiki_text(dpla_id, item_metadata, provider, data_provider)
    rewritten = render_migrated_wikitext(file_page.text, new_block)
    # ``new_block`` is the full upload-form template with every param
    # populated from ``item_metadata`` — appropriate for a fresh upload
    # but not for migration, where the SDC just written already carries
    # the DPLA-attributed values. Run the same strip pass the post-SDC
    # cleanup path uses on ``{{DPLA metadata}}`` files so the migrated
    # wikitext ends up in the post-strip steady state in one save,
    # instead of leaving the params populated and depending on a
    # follow-up sdc-sync run to strip them. Community-contributed
    # values whose ``canonical_value`` differs from
    # ``canonical_params`` won't match and are preserved — the strip
    # is value-equality, not blanket removal.
    #
    # Canonical-whitespace pass: ``render_migrated_wikitext`` substitutes
    # only the template node, so any leading whitespace the legacy
    # ``{{Artwork}}`` block carried (the pre-#291 pretty-printed indent
    # of "     {{ Artwork") survives as a Text node before the new
    # ``{{DPLA metadata}}`` template. ``canonicalize`` left-justifies
    # the template and forces the canonical blank line between the
    # section heading and the template, matching the shape
    # ``get_wiki_text`` emits for fresh uploads.
    from .wikitext_normalize import canonicalize, normalize

    rewritten, _stripped = normalize(rewritten, canonical_params)
    rewritten = canonicalize(rewritten)
    wikitext_changed = rewritten != file_page.text
    if wikitext_changed:
        file_page.text = rewritten
        with_csrf_recovery(
            file_page.site,
            f"save {file_page.title()} (legacy-artwork migrate)",
            lambda: file_page.save(summary=summary, minor=False, bot=True),
        )

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
