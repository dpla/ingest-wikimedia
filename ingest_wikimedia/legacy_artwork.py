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
from urllib.parse import urlencode, urlparse

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

# Bot accounts whose edits are treated as DPLA-/import-originated for provenance
# classification — any param value they last touched is safe to overwrite with
# canonical data (never preserved as a "community" contribution). Any OTHER
# account is treated as a community contributor whose edits must be preserved
# (imported to SDC) before the wikitext is rewritten.
#
# Coverage:
#   - "DPLA bot"                 — DPLA's current Commons uploader.
#   - "US National Archives bot" — NARA's own bot. Every pre-2020 NARA upload
#                                  was made by it (not DPLA's bot), so this folds
#                                  the oldest NARA files — the highest-risk,
#                                  most community-curated set — in correctly.
#   - "Flickr upload bot"        — Flickr2Commons-style imports. Any partner
#                                  (NARA especially) that also puts collections
#                                  on Flickr can have a file uploaded by this bot
#                                  that we later rename; its metadata is
#                                  automated import data, not community curation.
#
# Matched case- AND underscore/space-insensitively against the revision ``user``
# field (see :func:`_normalize_account`): Commons displays usernames with spaces
# (e.g. "DPLA bot"), which is the form the API returns, so the space form is
# canonical here and the underscore variant folds to it.
DPLA_BOT_ACCOUNTS: frozenset[str] = frozenset(
    {
        "DPLA bot",
        "US National Archives bot",
        "Flickr upload bot",
    }
)


def _normalize_account(name: str) -> str:
    """Casefold and collapse underscores to spaces so ``DPLA_bot`` and
    ``DPLA bot`` compare equal — MediaWiki treats the two as one username and
    the API returns the space form, so provenance matching must not depend on
    which the code happens to write."""
    return name.casefold().replace("_", " ")


# Wikidata items / properties used in the legacy-import reference shape.
# Hardcoded here (not behind a config knob) because they are part of the
# import's semantic contract — changing them would silently change the
# meaning of every legacy-imported statement.
QID_INFERRED_FROM_WIKITEXT = "Q131783016"  # the source-of-truth item
PID_BASED_ON_HEURISTIC = "P887"
PID_WIKIMEDIA_IMPORT_URL = "P4656"
# Related image (P6802) — a commonsMedia-datatype property pointing at another
# Commons file. Populated from a legacy template's "other versions" param
# ({{other version|<file>}}); Module:DPLA already renders it into the yellow
# box. This is the pipeline's FIRST commonsMedia-typed SDC property: its
# datavalue serialises as {"type": "string", "value": "<bare filename>"} while
# the snak's datatype is "commons-media", and Wikibase validates the value
# against a live Commons file — see _build_related_image_claim and
# materialize_pending_related_image_claim.
PID_RELATED_IMAGE = "P6802"

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
    # ``creator`` (or ``author``/``artist`` aliases). NOTE: this entry no
    # longer drives claim CONSTRUCTION — a free-text community creator is
    # built as a P170 ``somevalue`` + P2093 *qualifier* statement by
    # :func:`format_legacy_import_claim` (via :func:`_build_creator_stated_as_claim`),
    # because on a Commons MediaInfo entity P2093 (author name string) is
    # only ever a qualifier of P170; a top-level P2093 mainsnak is
    # unconventional and Module:DPLA doesn't render it as a creator. The
    # entry is retained solely so :func:`_community_value_unfit_for_sdc`
    # can look up the ``"string"`` kind and route vertical-whitespace
    # creator values to wikitext preservation before claim construction.
    "creator": ("P2093", "string"),
}

# Render-critical {{DPLA metadata}} params and the SDC property that must
# already be present before it is safe to strip the param from the wikitext.
# ``migrate_legacy_file`` posts only community imports (never the canonical
# scalars), so its strip relies on the sdc-sync's prior canonical-SDC write
# having landed; stripping a param whose value has no SDC counterpart blanks
# the field in both representations. ``creator`` renders from P170 (P2093 is
# only its qualifier); ``permission`` (rights) from P6216.
#
# The guard is deliberately PRESENCE-based (is there a statement for this
# property at all?), a backstop for the dominant failure: the canonical write
# not firing, leaving the property entirely absent. It is intentionally looser
# than ``tools/sdc_sync.py``'s provenance-based ``_entity_has_dpla_attributed_claims``
# (which matches the P123=Q_DPLA publisher reference) — presence cannot
# over-preserve the normal flow (canonical SDC present ⟹ property present ⟹
# strip proceeds), whereas a provenance check would entangle with the codebase's
# two distinct DPLA-attribution markers (P123 reference vs P459 heuristic
# qualifier) and risk keeping params whose canonical claims are shaped
# differently. Not accidental inconsistency; a deliberately conservative check.
STRIP_GUARD_SDC_PROPERTY: dict[str, str] = {
    "title": "P1476",
    "creator": "P170",
    "description": "P10358",
    "date": "P571",
    "permission": "P6216",
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
#
# {{NARA-image-full}} is included: its Title/Description/Date/Author/Creator
# params share names with the {{Artwork}} form, so they map through
# ARTWORK_PARAM_TO_CANONICAL_KEY with no NARA-specific parsing. Its many
# NARA-only archival params (ARC, NAID, record group, series, scope & content,
# …) have no canonical/SDC target and are dropped on migration — the same
# treatment any unrecognised param gets on any template — so converting a
# {{NARA-image-full}} page lifts its four core fields to SDC and does not carry
# those archival fields onto the new {{DPLA metadata}} form.
LEGACY_TEMPLATE_NAMES: tuple[str, ...] = (
    "artwork",
    "information",
    "photograph",
    "nara-image-full",
)

# Metadata wrappers the cross-page drift rescue can node-swap for a fresh
# {{DPLA metadata}} block. A superset of LEGACY_TEMPLATE_NAMES: also the
# already-migrated {{DPLA metadata}} form, since a source page can already be on
# the new template. The rescue only needs to *locate* the metadata wrapper so it
# can swap that one node and keep everything else on the page verbatim.
#
# This set governs OUTSIDE-template preservation (which node to swap so
# everything around it survives). It is broader than the set whose
# INSIDE-template params get lifted to SDC by exactly the {{DPLA metadata}}
# entry: a source already on {{DPLA metadata}} carries no legacy params for
# plan_migration to walk, so it is swapped for outside content only. See the
# asymmetry note in ``import_cross_page_community_sdc``.
RESCUE_WRAPPER_NAMES: tuple[str, ...] = (
    *LEGACY_TEMPLATE_NAMES,
    "dpla metadata",
)


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
    survive the migration as SDC statements. Each entry is keyed by
    canonical-params key so Phase 3b can dispatch to the right SDC
    property without re-doing the parsing work.

    ``wikitext_preserved_extras`` carries values whose community
    contribution is structural wikitext (galleries, wikitables,
    bulleted lists, horizontal rules) that Wikibase can't hold — its
    monolingual-text validator rejects vertical whitespace. These
    values stay on the migrated ``{{DPLA metadata}}`` template's own
    parameter instead, so the yellow-box renderer picks them up. The
    dict value is the FULL extension remainder verbatim (with any
    leading newline / HR / heading the user wrote) — preserving
    whatever structural marker they chose without treating any single
    one as a canonical signal.

    ``source_permalink`` is the value that gets stamped onto every
    imported claim's P4656 reference. It points at the revision *that
    contains* the community-contributed value — i.e. the page's latest
    revision id at plan-construction time. This lets a reviewer trace
    any imported statement back to its original wikitext source.
    """

    source_permalink: str
    community_imports: dict[str, str] = field(default_factory=dict)
    dpla_originated_params: dict[str, str] = field(default_factory=dict)
    wikitext_preserved_extras: dict[str, str] = field(default_factory=dict)
    artwork_template_name: str = ""
    # Filenames from the template's "other versions" param ({{other version|X}})
    # to import as P6802 (related image). Unconditional preserve, NOT
    # provenance-gated: DPLA has no canonical related-image value, so it is
    # never "drifted DPLA metadata" — it's an additive Commons relationship kept
    # for every author. Each becomes one commonsMedia SDC statement.
    related_image_imports: list[str] = field(default_factory=list)


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

    Known limitation: only ANONYMOUS positional overflow is stitched.
    An overflow fragment that itself happens to contain ``=`` (e.g.
    ``| description = A | region=north | date = …``) is parsed by
    ``mwparserfromhell`` as a NAMED parameter (``region=north``) and
    the current logic drops it — the literal-pipe form's parse then
    diverges from the corresponding ``{{!}}`` form (which would keep
    the whole ``A | region=north`` as one description value). In
    practice DPLA metadata values rarely contain ``key=value`` shapes
    (typical subject lists are noun phrases like ``Ranch houses`` or
    ``Dwellings``), and none of the migration incidents observed to
    date hit this shape. See
    :func:`test_parse_artwork_params_pipe_overflow_with_equals_fragment_is_dropped`
    for the pinned current behaviour.
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
        param_name = _normalize_param_name(param)
        canonical = ARTWORK_PARAM_TO_CANONICAL_KEY.get(param_name)
        # ``Other fields`` / ``Other fields N`` — the legacy shape
        # for creator (and historically other rows) wrapped in
        # ``{{InFi | Creator | <inner>}}`` bot scaffolding. Unwrap the
        # InFi and route the inner value to its canonical key when
        # the label is one we can migrate. Non-Creator InFi wrappers
        # (Description, Photographer, etc.) fall through as
        # unrecognised — a later phase can widen the set.
        if canonical is None and _OTHER_FIELDS_PARAM_RE.match(param_name):
            inner = _unwrap_infi_creator(str(param.value))
            if inner is None:
                last_named_recognised = False
                continue
            canonical = "creator"
            stitched.append([canonical, inner])
            last_named_recognised = True
            continue
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
        # Creator has multiple wikitext shapes: plain stated-as name,
        # ``{{creator|Wikidata=Q…}}`` with a direct QID,
        # ``{{Creator:Foo}}`` with a Creator: page transclusion, or
        # ``{{NARA-Author|…}}`` legacy-bot scaffolding. Classify and
        # tag with a sentinel; downstream comparator + claim-builder
        # branch on the sentinel. ``None`` from the classifier means
        # strip-only (never preserve) — the value is dropped from the
        # parse output entirely, indistinguishable from an absent
        # param, so no community-import fires.
        if canonical == "creator":
            classified = _parse_creator_shape(value)
            if classified is None:
                continue
            value = classified
        # On duplicate canonical keys (e.g. both ``author`` and
        # ``creator`` set), the *last* wins — matches the renderer
        # behavior under MediaWiki, where the later assignment
        # overrides the earlier.
        parsed[canonical] = value
    return parsed


# {{other version|<file>}} inside an "other versions" param. Commons filenames
# can't contain |, {, or }, so [^{}|]+ safely grabs the (first, positional)
# filename; an optional |extra tail (rare) is consumed but ignored.
_OTHER_VERSION_RE = re.compile(
    r"\{\{\s*other[\s_]*version\s*\|([^{}|]+)(?:\|[^{}]*)?\}\}",
    re.IGNORECASE,
)


def _normalize_commons_filename(name: str) -> str:
    """Canonicalise a Commons filename to MediaWiki's title form: strip a
    ``File:`` prefix, underscores → spaces, collapse whitespace runs, and
    uppercase the first character.

    MediaWiki treats File: titles as first-letter-insensitive and
    space/underscore-equivalent, so ``rel_image.jpg`` and ``Rel image.jpg`` name
    the *same* file. Normalising both the extracted value and the stored P6802
    values makes the dedup and existence checks compare like MediaWiki does (and
    the value we write already matches the canonical form Wikibase stores, so
    re-runs dedup cleanly)."""
    name = name.strip()
    if name.lower().startswith("file:"):
        name = name[len("File:") :]
    name = re.sub(r"[\s_]+", " ", name).strip()
    if name:
        name = name[0].upper() + name[1:]
    return name


def _extract_related_image_files(wikitext: str) -> list[str]:
    """Bare Commons filenames referenced by ``{{other version|<file>}}`` inside
    the legacy template's *other versions* param — the value form P6802
    (commonsMedia) stores (no ``File:`` prefix).

    Deduped, order-preserving. ``[]`` when there is no legacy template, no
    *other versions* param, or no ``{{other version}}`` invocations. Only the
    ``{{other version|…}}`` shape is parsed; galleries and bare ``[[File:…]]``
    links in the param are left alone — out of scope, and they have no single
    unambiguous P6802 target.
    """
    template = find_legacy_template(wikitext)
    if template is None:
        return []
    files: list[str] = []
    for param in template.params:
        if _normalize_param_name(param) not in ("other versions", "other_versions"):
            continue
        for match in _OTHER_VERSION_RE.finditer(str(param.value)):
            name = _normalize_commons_filename(match.group(1))
            if name and name not in files:
                files.append(name)
    return files


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
    bot_set = {_normalize_account(a) for a in bot_accounts}
    return {
        key: ("dpla" if _normalize_account(editor) in bot_set else "community")
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
    wikitext_preserved_extras: dict[str, str] = {}

    for key, value in wikitext_params.items():
        # A DPLA/uploader-generated boilerplate value (a bare {{DPLA}} template,
        # an uploader source link, or a rights-statement string) is provenance
        # already rendered from SDC — keeping it would just duplicate that
        # render. Drop it outright BEFORE the provenance/canonical-equivalence
        # branch, so it lands in NO output bucket (community_imports /
        # wikitext_preserved_extras / dpla_originated_params) regardless of
        # classification. Conservative: community prose alongside it is preserved.
        if _is_dpla_generated_extra(key, value):
            continue
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
            #
            # DPLA-prefixed extension: if the wikitext value is the
            # canonical value with additional structural content
            # appended (gallery, wikitable, HR, list, embedded
            # template — anything that introduces vertical
            # whitespace), split it. The DPLA prefix already lives in
            # DPLA-attributed SDC so no ``community_import`` is
            # emitted for it (that would fail Wikibase's
            # monolingual-text validator anyway). The remainder is
            # preserved verbatim on the migrated template's
            # parameter, where the yellow-box renderer picks it up.
            extras = _split_extension_extras(value, canonical_value)
            if extras is not None:
                # DPLA value + community structural addition: the DPLA prefix
                # keeps its canonical SDC, only the remainder rides the param.
                wikitext_preserved_extras[key] = extras
                dpla_originated[key] = value
                continue
            if _community_value_unfit_for_sdc(key, value):
                # Fully community-authored value that can't be an SDC claim of
                # its type — it carries vertical whitespace, which the
                # monolingualtext/string validators reject (e.g. a
                # multi-paragraph description with an HR, italics, a wikilink).
                # There's no DPLA prefix to peel off, so preserve the WHOLE
                # value on the migrated template's param — where Module:DPLA
                # renders it in the yellow box — instead of failing the whole
                # file's import. Same destination as the extension case above:
                # community content that can't be SDC lands in wikitext.
                wikitext_preserved_extras[key] = value
                continue
            community_imports[key] = value
        else:
            dpla_originated[key] = value

    legacy_template = find_legacy_template(latest.text)
    return MigrationPlan(
        source_permalink=_build_permalink(file_title, latest.revid),
        community_imports=community_imports,
        dpla_originated_params=dpla_originated,
        wikitext_preserved_extras=wikitext_preserved_extras,
        artwork_template_name=_template_name(legacy_template),
        # Unconditional (not provenance-gated): DPLA has no canonical
        # related-image value, so an "other versions" reference is never
        # drifted DPLA metadata — preserve it for every author. See the
        # MigrationPlan field docstring.
        related_image_imports=_extract_related_image_files(latest.text),
    )


# Matches a bare ``{{Institution|wikidata=Q...}}`` sub-template value.
# Case-insensitive on the template name and param key so hand-typed
# variants (``institution`` / ``Institution``, ``Wikidata`` / ``wikidata``)
# both parse cleanly to the inner Q-ID.
# Regexes for the legacy ``Other fields N`` creator shapes. Order-sensitive
# only in that the most specific matcher runs first — NARA-Author's
# name+id shape is distinct enough that it never collides with the
# QID-bearing shapes. See :func:`_parse_creator_shape` for the dispatcher.
_INSTITUTION_SUBTEMPLATE_RE = re.compile(
    r"^\s*\{\{\s*Institution\s*\|\s*wikidata\s*=\s*(Q\d+)\s*\}\}\s*$",
    re.IGNORECASE,
)

# ``Other fields 1 = {{InFi | Creator | <inner> | id=fileinfotpl_aut}}``
# and variants. Case-insensitive on InFi and on the ``Creator`` label
# so hand-typed variants (``infi`` / ``INFI``, ``creator`` / ``Creator``)
# both parse cleanly.
_INFI_CREATOR_LABEL_RE = re.compile(r"^\s*creator\s*$", re.IGNORECASE)

# ``Other fields 1``, ``Other fields 2``, ..., or the bare ``Other fields``.
# Case-folded param names since ``_normalize_param_name`` already lower-cases.
# NB: distinct from ``_NUMBERED_OTHER_FIELDS_RE`` (used by the rescue display-row
# carry) — that one excludes the bare form and adds the modern ``other_fields``;
# don't unify them (see the comment there). This one drives creator extraction.
_OTHER_FIELDS_PARAM_RE = re.compile(r"^other fields(?: \d+)?$")

# ``{{creator|Wikidata=Q56159174}}`` — Commons Creator template with a
# direct Wikidata QID. Case-insensitive on template name; the Wikidata
# param name is conventionally capitalized ``Wikidata`` on Commons but
# accepts any casing under MediaWiki's param-name rules.
_CREATOR_WIKIDATA_RE = re.compile(
    r"^\s*\{\{\s*creator\s*\|\s*[Ww]ikidata\s*=\s*(Q\d+)\s*\}\}\s*$",
    re.IGNORECASE,
)

# ``{{Creator:Theodore E. Peiser}}`` — Commons Creator: namespace page
# transclusion (no Wikidata= param, resolved later via Commons API).
# Case-insensitive on the ``Creator:`` prefix because MediaWiki auto-
# capitalises the namespace on a template transclusion; both
# ``{{Creator:Foo}}`` and ``{{creator:Foo}}`` resolve to the same page.
_CREATOR_PAGE_RE = re.compile(r"^\s*\{\{\s*Creator:([^}|]+?)\s*\}\}\s*$", re.IGNORECASE)

# ``{{NARA-Author|Adams, Ansel, 1902-1984, Photographer|1332556}}`` —
# legacy bot-authored NARA authorship template. Never a genuine
# community contribution; strip without extracting either positional.
_NARA_AUTHOR_RE = re.compile(r"^\s*\{\{\s*NARA-Author\s*\|", re.IGNORECASE)


# Sentinel prefixes marking a creator value that carries structured
# provenance beyond a plain stated-as name string. Downstream code
# (comparator + claim-builder) branches on these; unrelated code paths
# treat them as opaque strings that happen not to match anything.
_CREATOR_QID_PREFIX = "__creator_qid__:"
_CREATOR_PAGE_PREFIX = "__creator_page__:"
# ``{{Unknown|<role>}}`` — an editor's assertion that the creator is unknown
# but of a known role (e.g. photographer). Captured as its own sentinel so the
# claim-builder can emit a P170 ``somevalue`` + P3831 (object has role)
# qualifier instead of dropping it as an unrecognised template.
_CREATOR_UNKNOWN_PREFIX = "__creator_unknown__:"
# The {{Unknown|role}} creator grammar, shared by the anchored whole-value
# matcher (_parse_creator_shape) and the unanchored embedded matcher
# (_split_unknown_from_creator), so the two never drift apart.
_UNKNOWN_CREATOR_BODY = r"\{\{\s*[Uu]nknown\s*(?:\|\s*([^}|]*?)\s*)?\}\}"
_UNKNOWN_CREATOR_RE = re.compile(r"^" + _UNKNOWN_CREATOR_BODY + r"$")
# {{Unknown|<role>}} role name (casefolded) → the Wikidata role item for the
# P3831 (object of statement has role) qualifier. Labels verified on Wikidata.
_UNKNOWN_ROLE_QID = {
    "photographer": "Q33231",
    "artist": "Q483501",
    "author": "Q482980",
    "engraver": "Q329439",
    "painter": "Q1028181",
    "illustrator": "Q644687",
}


def _unwrap_infi_creator(value: str) -> str | None:
    """Return the inner value of ``{{InFi | Creator | <inner> …}}`` if
    ``value`` is exactly that shape (any casing on ``InFi`` / ``Creator``,
    any trailing ``id=`` or other kwargs), else ``None``.

    Legacy uploads wrapped the creator row in ``Other fields N =
    {{InFi|Creator|<value>|id=fileinfotpl_aut}}`` to attach the
    machine-readable ``fileinfotpl_aut`` id. The inner value is what
    a Commons editor actually contributed; the wrapper is bot
    scaffolding that drops out on migration.
    """
    parsed = mwparserfromhell.parse(value)
    templates = parsed.filter_templates(recursive=False)
    if len(templates) != 1:
        return None
    tpl = templates[0]
    if str(tpl.name).strip().casefold() != "infi":
        return None
    label_param = None
    inner_param = None
    for p in tpl.params:
        if not p.showkey:
            if label_param is None:
                label_param = p
            elif inner_param is None:
                inner_param = p
    if label_param is None or inner_param is None:
        return None
    if not _INFI_CREATOR_LABEL_RE.match(str(label_param.value)):
        return None
    return str(inner_param.value).strip()


def _parse_creator_shape(value: str) -> str | None:
    """Classify a raw wikitext creator value and return a comparable
    representation.

    Returns:

    * ``None`` if the value is a strip-only shape that must NOT be
      preserved as a community contribution (``{{NARA-Author|…}}`` —
      bot-authored, never a real community edit).
    * ``"__creator_qid__:Q…"`` sentinel when the value carries an
      explicit Wikidata QID (``{{creator|Wikidata=Q…}}``). The QID
      is stored as-is; the executor's comparator matches it directly
      against DPLA-attributed P170 QIDs.
    * ``"__creator_page__:<title>"`` sentinel when the value is a
      Commons Creator: page transclusion (``{{Creator:Foo}}``). The
      executor's QID resolver will call Commons ``pageprops`` to
      find the linked Wikidata QID and either preserve it as
      P170 QID or fall back to a P170 somevalue + P2093 stated-as
      claim when the Creator: page has no Wikibase link.
    * The unchanged ``value`` string for anything that is *not*
      template-shaped — a plain stated-as name
      (``Peiser, Theodore E``), an already-expanded creator string
      from any source. Passes through to the existing string-based
      comparator + claim-builder path unchanged.
    * ``None`` for any template-shaped value that doesn't match one
      of the recognised shapes above — e.g. an unknown ``{{Foo}}``
      wrapper, or a shape we haven't taught the parser about. Preserves
      the "strip when we can't safely preserve" invariant: submitting
      literal ``{{…}}`` markup as a P2093 stated-as string would be
      both wrong (SDC shouldn't store wikitext) and hard to unwind
      later. Deferring to a later phase that widens the recognised
      set is safer than emitting nonsense claims now.
    """
    stripped = value.strip()
    if not stripped:
        return stripped
    if _NARA_AUTHOR_RE.match(stripped):
        return None
    m = _CREATOR_WIKIDATA_RE.match(stripped)
    if m:
        return _CREATOR_QID_PREFIX + m.group(1)
    m = _CREATOR_PAGE_RE.match(stripped)
    if m:
        return _CREATOR_PAGE_PREFIX + m.group(1).strip()
    m = _UNKNOWN_CREATOR_RE.match(stripped)
    if m:
        # ``{{Unknown}}`` / ``{{Unknown|Photographer}}`` — creator unknown,
        # optional role. Preserve as a P170 somevalue (+ P3831 role) rather
        # than dropping it with the other unrecognised templates below.
        return _CREATOR_UNKNOWN_PREFIX + (m.group(1) or "").strip()
    # A compound value that EMBEDS a recognised creator template alongside
    # other text/templates (e.g. "{{Unknown|author}} {{Creator:B}}") is passed
    # through for format_legacy_import_claim to tokenise + split — do NOT drop
    # it here even though it is {{…}}-bookended.
    if _EMBEDDED_UNKNOWN_RE.search(stripped) or _EMBEDDED_CREATOR_PAGE_RE.search(
        stripped
    ):
        return stripped
    # Any remaining ``{{…}}`` template-shaped value we don't recognise
    # is dropped rather than passed through as a raw string. See the
    # returns-``None`` clause of the docstring for the rationale.
    if stripped.startswith("{{") and stripped.endswith("}}"):
        return None
    return stripped


# Recognised creator sub-templates that can appear EMBEDDED in a longer,
# compound free-text creator credit (as opposed to being the entire value —
# those bare shapes are tagged by _parse_creator_shape). Each is tokenised out
# and emitted as its own claim; the leftover text becomes a P2093 stated-as.
# Unanchored (the bare {{Unknown}} matcher is _UNKNOWN_CREATOR_RE).
_EMBEDDED_UNKNOWN_RE = re.compile(_UNKNOWN_CREATOR_BODY)
_EMBEDDED_CREATOR_PAGE_RE = re.compile(r"\{\{\s*[Cc]reator\s*:\s*([^}|]+?)\s*\}\}")
# A remainder made up only of join punctuation / connector words ("A and B")
# is not a real stated-as name — drop it rather than emit a junk P2093.
_CREATOR_CONNECTOR_RE = re.compile(
    r"^(?:and|und|et|&|,|;|/|·|—|–|-|\s)+$", re.IGNORECASE
)


def _split_embedded_creator_templates(
    value: str,
) -> tuple[list[tuple[str, str]] | None, str]:
    """Tokenise every recognised embedded creator sub-template out of a compound
    free-text creator credit. Returns ``(parts, remainder)`` where ``parts`` is
    an ordered list of ``("unknown", role)`` / ``("page", page_title)`` tuples —
    one per embedded ``{{Unknown|role}}`` / ``{{Creator:Page}}`` — and
    ``remainder`` is the leftover text with all recognised templates removed and
    join punctuation trimmed. Returns ``(None, value)`` when the value embeds no
    recognised creator template.

    Handles multiple templates and both kinds in one value (e.g.
    ``{{Creator:A}} and {{Creator:B}}`` → two page claims, no stated-as;
    ``{{unknown|author}} reprinted by {{Creator:B}}`` → an unknown claim, a page
    claim, and a stated-as remainder), so no literal ``{{…}}`` markup survives
    into P2093."""
    found: list[tuple[int, int, str, str]] = []
    for m in _EMBEDDED_UNKNOWN_RE.finditer(value):
        found.append((m.start(), m.end(), "unknown", (m.group(1) or "").strip()))
    for m in _EMBEDDED_CREATOR_PAGE_RE.finditer(value):
        found.append((m.start(), m.end(), "page", m.group(1).strip()))
    if not found:
        return None, value
    found.sort()
    kept: list[str] = []
    last = 0
    for start, end, _kind, _cap in found:
        if start < last:  # defensive: overlapping matches (not expected)
            last = max(last, end)
            continue
        kept.append(value[last:start])
        last = end
    kept.append(value[last:])
    remainder = "".join(kept).strip(" ;,·—–-\t\n").strip()
    if _CREATOR_CONNECTOR_RE.fullmatch(remainder):
        remainder = ""
    return [(kind, cap) for _s, _e, kind, cap in found], remainder


def _creator_sdc_clean(value: str) -> bool:
    """A community creator value is SDC-clean iff, after splitting out recognised
    embedded creator templates ({{Creator:}}/{{Unknown}}), the leftover stated-as
    text carries NO residual template ({{...}}) or wikilink ([[...]]) markup —
    i.e. it can be stored as a clean SDC string (a P170 QID, or a P170 somevalue
    + P2093 stated-as). Otherwise the value carries rich wikitext that can't be
    an SDC string and belongs on the wikitext creator param instead (where its
    markup renders); :func:`_community_value_unfit_for_sdc` routes it there."""
    parts, remainder = _split_embedded_creator_templates(value)
    check = remainder if parts is not None else value
    return "{{" not in check and "[[" not in check


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


_MULTI_VALUE_DELIMITER = "; "

# Any of these characters would trip Wikibase's monolingual-text
# validator (``wikibase-validator-illegal-string-chars``: "String
# should not start or end with whitespace nor include vertical
# whitespace or tabs"). Used to detect the "extension" pattern where
# a user's wikitext contribution carries structural markup (gallery
# blocks, wikitables, bulleted lists, horizontal rules) after the
# DPLA-authored text — a shape that can't survive as-is in SDC and
# needs a different preservation strategy than the default
# community-import path. See :func:`_split_extension_extras`.
_VERTICAL_WHITESPACE_RE = re.compile(r"[\n\r\t]")


def _split_extension_extras(value: str, canonical: str) -> str | None:
    """When ``value`` is a DPLA-prefixed extension, return the verbatim
    remainder; otherwise return None.

    An "extension" is the shape a Commons editor produces when they
    add structural wikitext (gallery, wikitable, bulleted list, HR,
    embedded template, etc.) to a DPLA-authored value by continuing
    the template parameter past the DPLA text. Since Wikibase's
    monolingual-text validator rejects vertical whitespace, values in
    this shape can't be submitted as SDC monolingualtext claims — but
    they're not community *divergence* either, they're community
    *addition*. The DPLA-authored prefix is still intact and should
    keep its DPLA-attributed SDC representation; only the remainder
    needs to live on the migrated template as a wikitext-only
    contribution.

    Detection is deliberately structure-agnostic: any vertical
    whitespace (``\\n``/``\\r``/``\\t``) marks the boundary between
    the DPLA prefix and the community remainder. HRs, galleries, and
    templates all imply vertical whitespace but so does plain
    multi-line prose — none is treated as a canonical signal. The
    substring up to the first vertical-whitespace character must
    casefold-match the canonical value (same normalisation chain used
    elsewhere in this module: magic-word unescape → strip
    leading/trailing punctuation → collapse whitespace → casefold);
    otherwise it's a scalar divergence (e.g. ``"1949"`` →
    ``"1949-02-01"``) and falls through to the ordinary
    community-import path.

    Returns the verbatim remainder from the boundary character onward
    (including the boundary itself) so the caller can inject it into
    the migrated template's parameter byte-identical to what the user
    wrote. Returns None when the value doesn't fit the extension
    shape or when ``canonical`` is empty (nothing to prefix-match).
    """
    if not value or not canonical:
        return None
    match = _VERTICAL_WHITESPACE_RE.search(value)
    if match is None:
        return None
    boundary = match.start()
    prefix = value[:boundary]
    remainder = value[boundary:]
    folded_prefix = casefold_for_compare(prefix)
    folded_canonical = casefold_for_compare(canonical)
    if not folded_prefix or folded_prefix != folded_canonical:
        return None
    return remainder


_DPLA_MANAGED_TEMPLATE_NAMES = frozenset({"dpla", "dpla metadata"})


def _is_redundant_dpla_source(value: str) -> bool:
    """True when ``value`` is nothing but DPLA-managed source template(s)
    (``{{DPLA|…}}`` / ``{{DPLA metadata|…}}``) plus whitespace.

    Such a value is DPLA provenance already carried by the file's structured
    data — re-adding it to the migrated ``{{DPLA metadata}}`` template only
    duplicates what Module:DPLA already renders from SDC. Any community text or
    markup alongside the template means the value is NOT purely redundant and is
    still preserved."""
    if "dpla" not in value.casefold():
        return False  # cheap guard: a DPLA-managed name always contains "dpla"
    saw_dpla_template = False
    for node in mwparserfromhell.parse(value).nodes:
        if isinstance(node, mwparserfromhell.nodes.Template):
            if _template_name(node) not in _DPLA_MANAGED_TEMPLATE_NAMES:
                return False
            saw_dpla_template = True
        elif isinstance(node, mwparserfromhell.nodes.Text):
            if node.value.strip():
                return False  # community text alongside the template
        else:
            return False  # a wikilink or other node -> not purely a DPLA template
    return saw_dpla_template


# Known DPLA/uploader-generated rights boilerplate — an explicit allowlist,
# expected to grow one partner at a time. Its failure mode is deliberately
# benign: an unmatched variant is preserved (over-kept), never lost. If this
# list grows large, prefer mapping rights to SDC over lengthening it.
_CANNED_PD_BLURB = (
    "Public Domain: This image is in the public domain and may be "
    "used free of charge without permissions or fees."
)


def _is_rights_url(url: str) -> bool:
    """True for a DPLA/NARA rights-statement URL, matched by parsed host (and
    path) rather than substring — so ``https://rightsstatements.org@evil.example/…``
    or a URL merely containing the text elsewhere does NOT match."""
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host == "rightsstatements.org" or host.endswith(".rightsstatements.org"):
        return True
    return (
        host == "archives.gov" or host.endswith(".archives.gov")
    ) and parsed.path.startswith("/social-media/flickr-faqs")


def _only_external_links(value: str) -> bool:
    """True when ``value`` is nothing but external link(s) plus whitespace /
    ``<br>`` — no prose, templates, or wikilinks. A bare link like this is an
    uploader-generated source pointer (it becomes the file's SDC source), not
    community-authored context. Any prose alongside means it is NOT pure."""
    saw = False
    for node in mwparserfromhell.parse(value).nodes:
        if isinstance(node, mwparserfromhell.nodes.ExternalLink):
            saw = True
        elif isinstance(node, mwparserfromhell.nodes.Text):
            if node.value.strip():
                return False
        elif (
            isinstance(node, mwparserfromhell.nodes.Tag)
            and str(node.tag).strip().lower() == "br"
        ):
            continue
        else:
            return False
    return saw


def _is_dpla_generated_extra(key: str, value: str) -> bool:
    """True when an unmapped community param value is DPLA/uploader-generated
    boilerplate already represented in the file's structured data — preserving
    it on the migrated ``{{DPLA metadata}}`` template only duplicates the SDC
    render. Generalises :func:`_is_redundant_dpla_source` (the ``{{DPLA}}``
    source template) to its sibling unmapped keys:

    * any key: the value is only a ``{{DPLA}}``/``{{DPLA metadata}}`` template;
    * ``source`` / ``institution``: the value is only external link(s) — an
      uploader source pointer that the migration writes to SDC anyway;
    * ``permission``: the value is only rights-statement link(s)
      (rightsstatements.org, the NARA Flickr-Commons rights link), or the canned
      DPLA public-domain blurb.

    Conservative: any community prose alongside the boilerplate makes the value
    NOT purely generated, so it is preserved (e.g. a rights template with a
    ``deathyear``, or a source line with an institution note)."""
    if _is_redundant_dpla_source(value):
        return True
    if key in ("source", "institution"):
        return _only_external_links(value)
    if key == "permission":
        if casefold_for_compare(value) == casefold_for_compare(_CANNED_PD_BLURB):
            return True
        if not _only_external_links(value):
            return False
        urls = [
            str(n.url)
            for n in mwparserfromhell.parse(value).nodes
            if isinstance(n, mwparserfromhell.nodes.ExternalLink)
        ]
        return all(_is_rights_url(u) for u in urls)
    return False


def _community_value_unfit_for_sdc(key: str, value: str) -> bool:
    """True when a community value can't be posted as an SDC claim and must be
    preserved on the migrated template's param instead of dropped.

    Cases (any one routes the value to wikitext preservation):

    * **No SDC import mapping at all** (``permission``/``source``/
      ``institution`` — not in :data:`LEGACY_IMPORT_PROPERTY`): no Phase-3a
      claim builder, so a community override would otherwise be silently
      dropped. Preserve it on the template param (Module:DPLA's yellow box).
      (A value that is only a redundant DPLA source template is dropped earlier,
      in :func:`plan_migration`, before it reaches this classifier.)
    * **Vertical whitespace** (``title``/``description`` monolingualtext,
      ``creator`` string): the Wikibase text validators reject newlines/tabs/CR.
      ``date`` (time) is parsed, not text-validated, so never unfit here.
    * **Rich creator wikitext** (``creator``): after splitting recognised
      embedded creator templates, residual ``{{...}}`` / ``[[...]]`` markup
      remains — the value can't be a clean SDC string, so the whole thing rides
      the wikitext creator param (where its links render). See
      :func:`_creator_sdc_clean`.
    * **Residual monolingual markup** (``title``/``description``): template or
      wikilink markup survives :func:`_unwrap_lang_template` — an unrecognised
      language code (``{{nan|…}}``), a multi-parameter wrapper
      (``{{en|First|Second}}``), or a non-language template — so a literal
      ``{{…}}`` would otherwise be stored as the monolingual value. Preserve as
      wikitext instead.
    """
    mapping = LEGACY_IMPORT_PROPERTY.get(key)
    if mapping is None:
        return True
    _prop, kind = mapping
    if kind not in ("monolingualtext", "string"):
        return False
    if _VERTICAL_WHITESPACE_RE.search(value):
        return True
    if key == "creator":
        return not _creator_sdc_clean(value)
    # title/description: unfit if template/wikilink markup survives language
    # unwrap (an unrecognised code, a multi-param wrapper, or a non-language
    # template) — a literal {{...}} must not be stored as the monolingual value.
    text, _lang = _unwrap_lang_template(value)
    return "{{" in text or "[[" in text


def _multi_value_subset_of_canonical(value: str, canonical: str) -> bool:
    """True when ``value`` and ``canonical`` are both ``; ``-joined
    string lists AND every casefolded entry in ``value`` also appears
    in ``canonical``.

    Legacy ``{{Artwork}}`` uploads concatenated multi-valued DPLA
    fields (``sourceResource.description`` is often a list) into a
    single template parameter with ``; `` separators;
    :func:`ingest_wikimedia.wikimedia.extract_strings` — the source of
    ``canonical_params['description']`` — joins with the same
    ``VALUE_JOIN_DELIMITER = "; "`` shape. Byte-comparing the two
    concatenations misses the case where DPLA has drifted between
    upload and migration (adding a value, dropping a value,
    reordering) even though every value the wikitext carries is still
    part of the DPLA-authored set. In that case the wikitext content
    is DPLA-originated, not community-contributed, and no
    ``inferred-from-Wikitext`` import should fire.

    Subset — not equal — because the wikitext-side list at migration
    time reflects DPLA data as of upload; DPLA canonical may have
    added values since. The migrated ``{{DPLA metadata}}`` template
    will render the current (superset) canonical from SDC anyway.
    """
    # Only ``canonical`` needs the delimiter — a single-value wikitext
    # (``| description = A``) can still be a subset of a multi-value
    # canonical (``A; B; C``) when DPLA expanded the field after
    # upload. Gating on both sides would reproduce the exact
    # false-positive this helper exists to catch, just for N=1.
    if _MULTI_VALUE_DELIMITER not in canonical:
        return False
    value_parts = {
        folded
        for folded in (
            casefold_for_compare(p) for p in value.split(_MULTI_VALUE_DELIMITER)
        )
        if folded
    }
    canonical_parts = {
        folded
        for folded in (
            casefold_for_compare(p) for p in canonical.split(_MULTI_VALUE_DELIMITER)
        )
        if folded
    }
    return bool(value_parts) and value_parts.issubset(canonical_parts)


# Complete ISO 639-1 two-letter language codes (the set MediaWiki's per-language
# templates use). A single {{<code>|<text>}} wrapper with one of these codes and
# a sole text parameter is unwrapped to (text, code) by _unwrap_lang_template.
# Anything else — an unrecognised code, multiple parameters, or a non-language
# template — leaves residual markup and is preserved on the wikitext param by
# _community_value_unfit_for_sdc, never stored as a mislabelled 'en' string.
_KNOWN_LANG_CODES = frozenset(
    "aa ab ae af ak am an ar as av ay az ba be bg bh bi bm bn bo br bs ca ce ch "
    "co cr cs cu cv cy da de dv dz ee el en eo es et eu fa ff fi fj fo fr fy ga "
    "gd gl gn gu gv ha he hi ho hr ht hu hy hz ia id ie ig ii ik io is it iu ja "
    "jv ka kg ki kj kk kl km kn ko kr ks ku kv kw ky la lb lg li ln lo lt lu lv "
    "mg mh mi mk ml mn mr ms mt my na nb nd ne ng nl nn no nr nv ny oc oj om or "
    "os pa pi pl ps pt qu rm rn ro ru rw sa sc sd se sg sh si sk sl sm sn so sq "
    "sr ss st su sv sw ta te tg th ti tk tl tn to tr ts tt tw ty ug uk ur uz ve "
    "vi vo wa wo xh yi yo za zh zu".split()
)


def _unwrap_lang_template(value: str) -> tuple[str, str]:
    """If ``value`` is exactly one bare ``{{<lang>|<text>}}`` language template
    with a known ISO code, return ``(text, lang)``; otherwise ``(value,
    "en")``. Lets a community title/description be stored as clean monolingual
    text with the correct language code instead of literal ``{{en|…}}`` markup,
    and reconciled against canonical's plain text."""
    # Fast path: a value with no template markup can't be a bare language
    # wrapper — skip the mwparserfromhell parse (this runs on every
    # title/description value across the batch).
    if "{{" not in value:
        return value, "en"
    parsed = mwparserfromhell.parse(value.strip())
    templates = parsed.filter_templates(recursive=False)
    if len(templates) == 1 and str(parsed).strip() == str(templates[0]).strip():
        tpl = templates[0]
        name = str(tpl.name).strip().casefold()
        if name in _KNOWN_LANG_CODES:
            # Unwrap only a SOLE text parameter (one positional, or one explicit
            # 1=). A multi-parameter template like {{en|First|Second}} is not a
            # simple language wrapper; unwrapping it would silently drop the
            # extra parameters, so leave it untouched.
            params = tpl.params
            if len(params) == 1 and (
                not params[0].showkey or str(params[0].name).strip() == "1"
            ):
                return str(params[0].value).strip(), name
    return value, "en"


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

    A ``; ``-joined multi-value subset check widens equivalence for
    keys whose canonical form is a semicolon-joined list (notably
    ``description``): if every value in the wikitext concatenation
    also appears in DPLA canonical's concatenation after casefold,
    the wikitext content is DPLA-originated and no community import
    fires — DPLA-side drift (an extra value added, one dropped, or a
    reorder) between upload and migration doesn't matter.

    For ``institution`` the wikitext value may be a bare Q-ID (flat
    ``{{DPLA metadata}}`` shape) or the legacy sub-template
    ``{{Institution|wikidata=Q...}}`` shape; both extract to a plain
    Q-ID that is byte-compared to the canonical DPLA institution Q-ID.

    Keys outside those sets (other sub-template shapes) fall back to
    strict equality.
    """
    if key in ("title", "description"):
        # A community title/description wrapped in a bare language template
        # ({{en|…}}) is the same fact as canonical's plain text — compare the
        # unwrapped text so it isn't imported as a spurious community value.
        value = _unwrap_lang_template(value)[0]
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
        if _multi_value_subset_of_canonical(value, canonical):
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


class InvalidWikibaseTextValue(ValueError):
    """Raised when a value can't be safely submitted to Wikibase.

    Client-side pre-flight for values that would trip Wikibase's
    ``wikibase-validator-illegal-string-chars`` — the validator
    rejects vertical whitespace (``\\n``/``\\r``/``\\t``) and
    leading/trailing whitespace on monolingualtext + string values.
    A dedicated exception (as opposed to letting the API raise
    :class:`pywikibot.exceptions.APIError`) gives operators a clear
    "value X for property Y didn't pass local validation" message
    instead of a Wikibase traceback the reader has to decode; the
    migration executor catches it with the same per-file boundary
    that catches API errors, so behavior on the wire is unchanged.
    """


def _validate_wikibase_text(text: str, prop: str, canonical_key: str) -> None:
    """Raise :class:`InvalidWikibaseTextValue` if ``text`` can't be
    safely submitted as a monolingualtext / string claim value.

    The migration pipeline's plan-time extension-detection routes
    values containing vertical whitespace to the wikitext-preserved
    path instead of the SDC-import path — so this check should never
    fire on a healthy code path. It exists as belt-and-suspenders: a
    new user-contribution shape the extraction heuristic hasn't been
    taught yet would previously reach Wikibase and get rejected as a
    generic ``APIError``. Failing loud, distinctly, and *before* the
    wire trip keeps future incidents legible in the log.
    """
    if _VERTICAL_WHITESPACE_RE.search(text):
        raise InvalidWikibaseTextValue(
            f"legacy-artwork import claim for {canonical_key!r} → {prop} "
            f"contains vertical whitespace (newline/tab/CR); Wikibase's "
            f"monolingual-text validator would reject it. Value: {text!r}"
        )
    if text != text.strip():
        raise InvalidWikibaseTextValue(
            f"legacy-artwork import claim for {canonical_key!r} → {prop} "
            f"has leading/trailing whitespace; Wikibase's monolingual-text "
            f"validator would reject it. Value: {text!r}"
        )


def _monolingualtext_datavalue(text: str, language: str = "en") -> dict:
    return {
        "type": "monolingualtext",
        "value": {"text": text, "language": language},
    }


def _string_datavalue(text: str) -> dict:
    return {"type": "string", "value": text}


def _commons_media_datavalue(filename: str) -> dict:
    # commonsMedia serialises as a plain string datavalue — the SAME shape as a
    # string value (hence delegating to _string_datavalue). What marks it a file
    # reference is the snak's ``datatype`` ("commons-media"), not the datavalue
    # ``type``. The value is the BARE filename (no "File:" prefix); Wikibase
    # validates it against a live Commons file at write time.
    return _string_datavalue(filename)


def _build_related_image_claim(filename: str, permalink: str) -> dict:
    """Build a P6802 (related image) commonsMedia statement for ``filename``,
    with the inferred-from-Wikitext reference shape (P887 + P4656)."""
    return {
        "type": "statement",
        "rank": "normal",
        "mainsnak": {
            "snaktype": "value",
            "property": PID_RELATED_IMAGE,
            "datatype": "commons-media",
            "datavalue": _commons_media_datavalue(filename),
        },
        "references": [_reference_snaks(permalink)],
    }


def format_legacy_import_claim(
    canonical_key: str,
    value: str,
    permalink: str,
    today: datetime.date | None = None,
) -> dict | list[dict] | None:
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
    # Creator has multiple wikitext shapes: a plain stated-as name
    # (existing string-datavalue path) OR the sentinel-tagged QID /
    # Creator: page shapes emitted by :func:`_parse_creator_shape`.
    # The QID and page shapes need a P170 mainsnak (not the default
    # P2093 stated-as claim); page-shape values also need a Commons
    # API round-trip to resolve the linked Wikidata QID. Surface
    # both as pending markers so :func:`materialize_import_claims`
    # can do the SDC comparison + resolution in the executor's
    # network-touching context, keeping :func:`plan_migration` pure.
    if canonical_key == "creator":

        def _stated_as(text: str) -> dict:
            _validate_wikibase_text(text, "P2093", canonical_key)
            return _build_creator_stated_as_claim(text, permalink)

        if value.startswith(_CREATOR_QID_PREFIX):
            return {
                "type": "statement",
                "rank": "normal",
                "_phase3a_pending_creator_qid": value[len(_CREATOR_QID_PREFIX) :],
                "_permalink": permalink,
            }
        if value.startswith(_CREATOR_PAGE_PREFIX):
            return _build_pending_creator_page_claim(
                value[len(_CREATOR_PAGE_PREFIX) :], permalink
            )
        if value.startswith(_CREATOR_UNKNOWN_PREFIX):
            # {{Unknown|<role>}} — creator unknown, optional role → P170
            # somevalue + (recognised) P3831 role qualifier.
            return _build_creator_unknown_claim(
                value[len(_CREATOR_UNKNOWN_PREFIX) :], permalink
            )
        # A plain free-text creator credit — a stated-as name ("Jane Doe")
        # or a descriptive line ("illustration photographed circa 1910 by
        # Walter F. Piper") — with no {{Creator:}} page or {{creator|Wikidata=}}
        # QID. Preserve it the conventional Commons way: a P170 (creator)
        # ``somevalue`` statement qualified by P2093 (author name string),
        # carrying the inferred-from-Wikitext reference. On a Commons
        # MediaInfo entity P2093 is only ever a *qualifier* of P170 — a
        # top-level P2093 mainsnak (the old ``LEGACY_IMPORT_PROPERTY``
        # string mapping) is unconventional and Module:DPLA doesn't render
        # it as a creator (so it silently failed to appear as one). This is
        # the same shape the {{Creator:}}-no-QID fallback builds.
        parts, remainder = _split_embedded_creator_templates(value)
        if parts is not None:
            # Compound credit embedding one or more recognised creator templates
            # ({{Unknown|role}}, {{Creator:Page}}): emit a claim per template
            # (unknown → P170 somevalue + P3831 role; page → QID-resolved P170),
            # plus a P2093 stated-as ONLY for the fully-cleaned remainder — so no
            # literal template markup survives into the stated-as string.
            claims: list[dict] = []
            for kind, captured in parts:
                if kind == "unknown":
                    claims.append(_build_creator_unknown_claim(captured, permalink))
                else:  # "page"
                    claims.append(
                        _build_pending_creator_page_claim(captured, permalink)
                    )
            if remainder:
                claims.append(_stated_as(remainder))
            return claims
        return _stated_as(value)

    mapping = LEGACY_IMPORT_PROPERTY.get(canonical_key)
    if mapping is None:
        return None
    prop, kind = mapping
    if kind == "monolingualtext":
        # Unwrap a bare language template ({{en|…}}) so the monolingual value
        # stores clean text with the right language code, not literal markup.
        text, language = _unwrap_lang_template(value)
        _validate_wikibase_text(text, prop, canonical_key)
        datavalue = _monolingualtext_datavalue(text, language)
        datatype = "monolingualtext"
    elif kind == "string":
        _validate_wikibase_text(value, prop, canonical_key)
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

    Also emits one P6802 (related image) placeholder per
    ``plan.related_image_imports`` entry. These are placeholders (not final
    claims) because commonsMedia values are validated against a live Commons
    file, so :func:`materialize_import_claims` confirms existence in the
    executor context before emitting the real claim.
    """
    claims: list[dict] = []
    for key, value in plan.community_imports.items():
        claim = format_legacy_import_claim(key, value, plan.source_permalink)
        if claim is None:
            continue
        if isinstance(claim, list):
            claims.extend(claim)
        else:
            claims.append(claim)
    for filename in plan.related_image_imports:
        claims.append(
            {
                "type": "statement",
                "rank": "normal",
                "_phase3a_pending_related_image": filename,
                "_permalink": plan.source_permalink,
            }
        )
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

    A SUPPRESSED (RevDel) revision hides its author and content together
    (``user is None``); it is tolerated — coerced to ``""`` so
    :func:`parse_artwork_params` finds no params and it contributes nothing
    to provenance. But a revision with a VISIBLE author and unloaded content
    (``text is None`` while ``user`` is set) means the
    ``revisions(content=True)`` fetch came back PARTIAL. That is raised, not
    tolerated: silently coercing it to ``""`` would drop it from
    :func:`trace_param_provenance`'s walk, mis-attributing an unchanged
    DPLA-bot param to whichever later community editor's revision did load —
    emitting a false "community" SDC import and a bogus "added by Wikimedia
    users, not verified by the source institution" notice. The caller skips
    and flags the file; a re-run with a complete fetch migrates it correctly.
    """
    snapshots: list[RevisionSnapshot] = []
    for rev in file_page.revisions(content=True):
        revid = getattr(rev, "revid", 0)
        user = getattr(rev, "user", None)
        text = getattr(rev, "text", None)
        if text is None and user is not None:
            raise RuntimeError(
                f"incomplete revision content for {file_page.title()!r} "
                f"(revid {revid}, user {user!r}): refusing to compute legacy "
                f"provenance from a partial revision history"
            )
        snapshots.append(
            RevisionSnapshot(revid=revid, user=user or "", text=text or "")
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


def _entity_p170_qids(existing_entity: dict | None) -> set[str]:
    """Return the set of Wikidata QIDs already stated in ``existing_entity``'s
    P170 (creator) mainsnaks, DPLA-attributed or otherwise.

    Used by the creator claim materialiser to detect when a community-
    contributed QID (from ``{{Creator:Foo}}`` / ``{{creator|Wikidata=Q…}}``)
    duplicates a QID already on the entity — in which case the community
    import is a no-op and gets dropped rather than emitted as a redundant
    second P170 statement carrying the same fact. Reads only the
    ``wikibase-entityid`` mainsnak shape; somevalue + P2093 stated-as
    claims contribute no QID and are ignored here.
    """
    if not existing_entity:
        return set()
    statements = (
        existing_entity.get("statements") or existing_entity.get("claims") or {}
    )
    qids: set[str] = set()
    for stmt in statements.get("P170", []):
        ms = stmt.get("mainsnak") or {}
        if ms.get("snaktype") != "value":
            continue
        dv = ms.get("datavalue") or {}
        if dv.get("type") != "wikibase-entityid":
            continue
        qid = (dv.get("value") or {}).get("id")
        if qid:
            qids.add(qid)
    return qids


_CREATOR_WIKIDATA_PARAM_RE = re.compile(r"\|\s*[Ww]ikidata\s*=\s*(Q\d+)")


def _resolve_commons_creator_qid(site, page_title: str) -> str | None:
    """Look up the Wikidata QID for a Commons ``Creator:<title>`` page.

    Checks two sources, in order:
      1. the ``wikibase_item`` pageprop — a Wikidata *sitelink* to the Creator
         page, and
      2. the ``{{Creator | Wikidata = Q… }}`` parameter in the page's own
         wikitext — the far more common way Commons Creator pages carry their
         Wikidata id (most are NOT sitelinked, so their ``wikibase_item`` is
         empty; e.g. Creator:Theodore E. Peiser → Q56159174 lives only in the
         template param).

    Returns the QID string (``"Q…"``) when found; ``None`` for missing/orphaned
    pages or any API failure. Falling back to ``None`` is intentional: the
    caller then substitutes a P170 somevalue + P2093 stated-as name claim so
    the community contribution is still preserved as a string. Resolving the
    QID instead lets the caller build a P170 → wikibase-entityid claim, so
    Module:DPLA can render the full ``{{Creator:…}}`` template from SDC.

    Requires ``site`` to be a live pywikibot Site; test callers can pass
    ``None`` to short-circuit to the name-only fallback path.
    """
    if site is None or not page_title:
        return None
    try:
        response = site.simple_request(
            action="query",
            prop="pageprops|revisions",
            rvprop="content",
            rvslots="main",
            titles=f"Creator:{page_title}",
            format="json",
        ).submit()
    except Exception:
        return None
    pages = (response.get("query") or {}).get("pages") or {}
    # ``pages`` is a dict keyed by pageid under formatversion=1 and a list under
    # formatversion=2; revision content likewise lives under the ``*`` key
    # (fv=1) or ``content`` key (fv=2). Accept both so the resolver is immune to
    # the pywikibot/API formatversion in effect.
    for page in pages.values() if isinstance(pages, dict) else pages:
        pp = page.get("pageprops") or {}
        qid = pp.get("wikibase_item")
        if isinstance(qid, str) and qid.startswith("Q"):
            return qid
        for rev in page.get("revisions") or []:
            main = (rev.get("slots") or {}).get("main") or {}
            content = (
                main.get("content")
                or main.get("*")
                or rev.get("content")
                or rev.get("*")
                or ""
            )
            match = _CREATOR_WIKIDATA_PARAM_RE.search(content)
            if match:
                return match.group(1)
    return None


def _build_creator_qid_claim(qid: str, permalink: str) -> dict:
    """Build a P170 (creator) statement whose mainsnak is a
    wikibase-entityid pointing at ``qid``, with the inferred-from-
    Wikitext reference shape."""
    return {
        "type": "statement",
        "rank": "normal",
        "mainsnak": {
            "snaktype": "value",
            "property": "P170",
            "datatype": "wikibase-item",
            "datavalue": {
                "type": "wikibase-entityid",
                "value": {"entity-type": "item", "id": qid},
            },
        },
        "references": [_reference_snaks(permalink)],
    }


def _build_creator_unknown_claim(role: str, permalink: str) -> dict:
    """Build a P170 (creator) ``somevalue`` statement for an
    ``{{Unknown|<role>}}`` community credit — an editor's assertion that the
    creator is unknown but of a known role (e.g. photographer). A recognised
    role (:data:`_UNKNOWN_ROLE_QID`) is recorded as a P3831 (object of
    statement has role) qualifier; an absent or unrecognised role yields a
    bare P170 somevalue. Carries the inferred-from-Wikitext reference.

    Module:DPLA renders this shape as ``{{Unknown|<role>}}`` (e.g. "Unknown
    photographer") as of the 2026-07-11 module update (revid 1246261799): its
    creator renderers surface the P3831 role for a somevalue P170 that has no
    P2093 name-string qualifier. (Before that update the shape rendered blank.)
    """
    claim = {
        "type": "statement",
        "rank": "normal",
        "mainsnak": {
            "snaktype": "somevalue",
            "property": "P170",
            "datatype": "wikibase-item",
        },
        "references": [_reference_snaks(permalink)],
    }
    role_qid = _UNKNOWN_ROLE_QID.get(role.casefold()) if role else None
    if role_qid:
        claim["qualifiers"] = {
            "P3831": [
                {
                    "snaktype": "value",
                    "property": "P3831",
                    "datatype": "wikibase-item",
                    "datavalue": {
                        "type": "wikibase-entityid",
                        "value": {"entity-type": "item", "id": role_qid},
                    },
                }
            ]
        }
        claim["qualifiers-order"] = ["P3831"]
    return claim


def _build_pending_creator_page_claim(page_title: str, permalink: str) -> dict:
    """Phase-3a placeholder for a Commons ``{{Creator:PageName}}`` transclusion.
    :func:`materialize_import_claims` resolves it to a P170 QID (or a P170
    somevalue + P2093 stated-as fallback when the Creator page has no Wikidata
    link). Shared by the bare-``{{Creator:}}`` and compound-creator paths."""
    return {
        "type": "statement",
        "rank": "normal",
        "_phase3a_pending_creator_page": page_title,
        "_permalink": permalink,
    }


def _build_creator_stated_as_claim(name: str, permalink: str) -> dict:
    """Build a P170 somevalue statement qualified by P2093 (stated as)
    ``name``, matching the DPLA-bot's canonical creator shape for
    files where the creator is a plain name string with no Wikidata
    counterpart."""
    return {
        "type": "statement",
        "rank": "normal",
        "mainsnak": {
            "snaktype": "somevalue",
            "property": "P170",
            "datatype": "wikibase-item",
        },
        "qualifiers": {
            "P2093": [
                {
                    "snaktype": "value",
                    "property": "P2093",
                    "datatype": "string",
                    "datavalue": {"type": "string", "value": name},
                }
            ],
        },
        "qualifiers-order": ["P2093"],
        "references": [_reference_snaks(permalink)],
    }


def materialize_pending_creator_claim(
    placeholder: dict,
    *,
    site=None,
    existing_entity: dict | None = None,
) -> dict | None:
    """Convert a Phase-3a creator placeholder into a real P170 statement.

    Two placeholder shapes are supported, matching the sentinels
    :func:`_parse_creator_shape` emits:

    * ``_phase3a_pending_creator_qid``: the QID is already known
      (came from ``{{creator|Wikidata=Q…}}``). Compare against
      :func:`_entity_p170_qids`; return ``None`` to drop the claim
      when the QID is already on the entity, else build a P170 QID
      statement.
    * ``_phase3a_pending_creator_page``: the value is a Commons
      Creator: page title (came from ``{{Creator:Foo}}``). Resolve
      to a Wikidata QID via :func:`_resolve_commons_creator_qid`;
      apply the same duplicate check on success. On resolution
      failure (page missing, orphaned Creator, or API error),
      fall back to a P170 somevalue + P2093 stated-as ``<title>``
      claim so the community contribution is preserved as a stated-as
      string.

    Returns ``None`` for placeholders missing the expected sentinel
    keys (defensive — the caller drops such claims) or for the
    duplicate-QID drop case.
    """
    permalink = placeholder.get("_permalink", "")
    existing_qids = _entity_p170_qids(existing_entity)

    qid = placeholder.get("_phase3a_pending_creator_qid")
    if isinstance(qid, str) and qid:
        if qid in existing_qids:
            return None
        return _build_creator_qid_claim(qid, permalink)

    page_title = placeholder.get("_phase3a_pending_creator_page")
    if isinstance(page_title, str) and page_title:
        resolved = _resolve_commons_creator_qid(site, page_title)
        if resolved is not None:
            if resolved in existing_qids:
                return None
            return _build_creator_qid_claim(resolved, permalink)
        # No Wikidata QID on the Creator: page. Preserve as a stated-as
        # string using the page title as the name — best-effort
        # fallback that mirrors what a plain ``| creator = <name>``
        # param would produce.
        return _build_creator_stated_as_claim(page_title, permalink)

    return None


def _entity_p6802_files(existing_entity: dict | None) -> set[str]:
    """MediaWiki-normalized filenames already stated in ``existing_entity``'s
    P6802 (related image) commonsMedia mainsnaks. Used to drop a related-image
    import the entity already carries, so re-runs don't add duplicate
    statements. Values are normalized (:func:`_normalize_commons_filename`) so a
    stored ``Rel_image.jpg`` dedups against an extracted ``rel image.jpg`` —
    MediaWiki treats them as the same file."""
    if not existing_entity:
        return set()
    statements = (
        existing_entity.get("statements") or existing_entity.get("claims") or {}
    )
    files: set[str] = set()
    for stmt in statements.get(PID_RELATED_IMAGE, []):
        ms = stmt.get("mainsnak") or {}
        if ms.get("snaktype") != "value":
            continue
        value = (ms.get("datavalue") or {}).get("value")
        if isinstance(value, str) and value:
            files.add(_normalize_commons_filename(value))
    return files


def _commons_file_exists(site, filename: str) -> bool:
    """True when ``File:<filename>`` exists on Commons.

    ``site is None`` (test / pure contexts) short-circuits to True — the
    existence gate is a live-Wikibase concern. Any API failure returns False so
    an unconfirmable file is *dropped* rather than risking the commonsMedia
    validation error that would fail the whole atomic ``wbeditentity`` bundle.
    """
    if site is None:
        return True
    try:
        response = site.simple_request(
            action="query",
            titles=f"File:{filename}",
            format="json",
        ).submit()
    except Exception:  # noqa: BLE001 — any lookup failure → treat as absent
        return False
    pages = (response.get("query") or {}).get("pages") or {}
    page = next(iter(pages.values()), None)
    return page is not None and "missing" not in page and "invalid" not in page


def materialize_pending_related_image_claim(
    placeholder: dict,
    *,
    site=None,
    existing_entity: dict | None = None,
) -> dict | None:
    """Convert a Phase-3a related-image placeholder into a real P6802 (related
    image) commonsMedia statement — or drop it (return ``None``) when:

    * a required sentinel key is missing (defensive), or
    * the file is already a P6802 value on the entity (re-run dedup), or
    * the referenced Commons file does not exist / can't be confirmed.

    The last case is the important one: commonsMedia is validated by Wikibase
    against a live file, so emitting a claim for a missing target would fail the
    entire atomic ``wbeditentity`` bundle and block the file's other imports.
    Dropping the one claim keeps the rest; the reference still lives in the
    page's revision history for manual recovery.
    """
    filename = placeholder.get("_phase3a_pending_related_image")
    permalink = placeholder.get("_permalink")
    if not filename or not permalink:
        return None
    if filename in _entity_p6802_files(existing_entity):
        return None
    if not _commons_file_exists(site, filename):
        return None
    return _build_related_image_claim(filename, permalink)


def materialize_import_claims(
    claims: list[dict],
    *,
    site=None,
    existing_entity: dict | None = None,
) -> list[dict]:
    """Walk an import-claim list and substitute every Phase-3a
    placeholder for a real Wikibase statement.

    Handles three placeholder shapes: date placeholders (P571 time
    statements via :func:`materialize_pending_date_claim`), creator
    placeholders (P170 statements via
    :func:`materialize_pending_creator_claim`), and related-image
    placeholders (P6802 commonsMedia statements via
    :func:`materialize_pending_related_image_claim`). Claims without a
    placeholder marker pass through unchanged. Placeholders whose
    materialiser returns ``None`` — duplicate of existing SDC, a
    missing Commons file, or an unparseable value — are dropped from
    the returned list.

    ``site`` and ``existing_entity`` are forwarded to the
    materialisers so they can (a) expand wikitext templates
    server-side, (b) look up Commons Creator-page QIDs, and (c)
    drop community-imports whose value already exists on the entity.
    All optional — when none are supplied, the function preserves
    the historical always-import behaviour. The original caller
    signature is also preserved for tests that pass only the
    claims list.
    """
    materialised: list[dict] = []
    for claim in claims:
        if "_phase3a_pending_date_parse" in claim:
            real = materialize_pending_date_claim(
                claim, site=site, existing_entity=existing_entity
            )
            if real is not None:
                materialised.append(real)
        elif (
            "_phase3a_pending_creator_qid" in claim
            or "_phase3a_pending_creator_page" in claim
        ):
            real = materialize_pending_creator_claim(
                claim, site=site, existing_entity=existing_entity
            )
            if real is not None:
                materialised.append(real)
        elif "_phase3a_pending_related_image" in claim:
            real = materialize_pending_related_image_claim(
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
    :data:`QID_INFERRED_FROM_WIKITEXT` on any statement of a *scalar* import
    property (:data:`LEGACY_IMPORT_PROPERTY`). Subtle: a non-DPLA editor could
    in principle stamp the same ref shape on a hand-authored claim, but that's
    the same semantic ("inferred from Wikitext") and the skip-on-detect
    behaviour is still correct — we just don't add duplicates.

    P6802 (related image) is deliberately EXCLUDED from this global trip-wire.
    It's an always-preserve secondary output with its own value-level dedup in
    :func:`materialize_pending_related_image_claim`; letting a P6802-only prior
    import mark the whole file "migrated" would wrongly block a later rescue
    from picking up a newly-added scalar community value on the source page.
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


def _find_dpla_metadata_node(wikicode):
    """Return the first ``{{DPLA metadata}}`` template node in ``wikicode``,
    or ``None``. (Node form of :func:`_extract_dpla_metadata_template`.)"""
    return next(
        (
            t
            for t in wikicode.filter_templates()
            if _template_name(t) == "dpla metadata"
        ),
        None,
    )


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
    node = _find_dpla_metadata_node(mwparserfromhell.parse(block))
    return str(node) if node is not None else block


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
    text, _swapped = _swap_wrapper_node(
        original_text, new_template_block, LEGACY_TEMPLATE_NAMES
    )
    return text


# Community-added display params on a ``{{DPLA metadata}}`` invocation that the
# fresh ``get_wiki_text`` block never emits (it is built from the fixed
# ``dpla_metadata_params`` set): the modern single ``other_fields`` and the
# legacy numbered ``Other fields N``. Names match exactly what ``Module:DPLA``
# reads — ``args.other_fields`` (underscore) and ``args['other fields ' .. i]``
# / ``['Other fields ' .. i]`` (space) — so casefold but do NOT collapse
# underscores to spaces (the two forms are distinct params to the renderer).
# NB: distinct from ``_OTHER_FIELDS_PARAM_RE`` (creator-extraction, matches the
# bare ``other fields`` too); intentionally not unified.
_NUMBERED_OTHER_FIELDS_RE = re.compile(r"other fields \d+")


def _is_user_extension_param(param) -> bool:
    name = _normalize_param_name(param)
    return (
        name == "other_fields" or _NUMBERED_OTHER_FIELDS_RE.fullmatch(name) is not None
    )


def _carry_user_extension_params(old_template, new_block: str) -> str:
    """Return ``new_block`` with any user-extension params (see
    :func:`_is_user_extension_param`) copied from ``old_template`` when the
    fresh block's ``{{DPLA metadata}}`` lacks them.

    Used only when the node being swapped is itself a ``{{DPLA metadata}}``
    template — the cross-page drift rescue over a source already on the new
    form. Without this, a rescue replaces the whole node with the fresh block
    and drops these community-added display rows. Only these extension params
    are carried: every other param is canonical and is intentionally refreshed
    by the rescue, so a fresh-block value always wins.
    """
    to_carry = [p for p in old_template.params if _is_user_extension_param(p)]
    if not to_carry:
        return new_block
    fresh = mwparserfromhell.parse(new_block)
    target = _find_dpla_metadata_node(fresh)
    if target is None:
        return new_block
    existing = {_normalize_param_name(p) for p in target.params}
    for p in to_carry:
        if _normalize_param_name(p) in existing:
            continue  # fresh block already sets it — canonical value wins
        target.add(str(p.name).strip(), str(p.value).strip())
    return str(fresh)


def _swap_wrapper_node(
    original_text: str,
    new_template_block: str,
    wrapper_names: tuple[str, ...],
) -> tuple[str, bool]:
    """Replace the first template in ``original_text`` whose name is in
    ``wrapper_names`` with the ``{{DPLA metadata}}`` template extracted
    from ``new_template_block``, leaving everything else verbatim.

    Returns ``(text, swapped)``. ``swapped`` is False — and ``text`` is
    ``original_text`` byte-for-byte — when no matching wrapper is present,
    so callers can distinguish "swapped the wrapper" from "nothing to do"
    (the shared preserve-by-default primitive behind both the regular
    migration and the cross-page drift rescue).

    When the matched wrapper is itself a ``{{DPLA metadata}}`` template (the
    rescue path — ``render_migrated_wikitext`` passes only legacy names, so it
    never hits this), community-added display rows (``other_fields`` / numbered
    ``Other fields N``) are carried from the old node into the fresh block,
    which is built from canonical params only and would otherwise drop them.
    """
    wikicode = mwparserfromhell.parse(original_text)
    for tpl in wikicode.filter_templates():
        if _template_name(tpl) in wrapper_names:
            replacement = _extract_dpla_metadata_template(new_template_block)
            if _template_name(tpl) == "dpla metadata":
                replacement = _carry_user_extension_params(tpl, replacement)
            wikicode.replace(tpl, replacement)
            return str(wikicode), True
    return original_text, False


def rescue_wikitext(source_text: str, new_template_block: str) -> str:
    """Destination wikitext for a cross-page drift rescue — preserve by default.

    If the source page carries a recognised metadata wrapper
    (:data:`RESCUE_WRAPPER_NAMES`), node-swap just that wrapper for the fresh
    ``{{DPLA metadata}}`` block and keep everything else on the source verbatim:
    categories, ``{{ImageNote}}`` annotations, and every other community
    template survive with no allowlist to maintain. This is the same mechanism
    the regular migration (:func:`render_migrated_wikitext`) uses — the drift
    paths just never adopted it (the node-swap postdates them).

    Only when the source has *no* recognised wrapper — nothing to swap, so
    preserve-by-default is impossible — fall back to
    :func:`ingest_wikimedia.wikimedia.merge_preserved_wikitext`, the narrow
    license/category/assessment allowlist. That no-wrapper case is the sole
    reason ``merge_preserved_wikitext`` still exists.
    """
    text, swapped = _swap_wrapper_node(
        source_text, new_template_block, RESCUE_WRAPPER_NAMES
    )
    if swapped:
        return text
    from .wikimedia import merge_preserved_wikitext

    return merge_preserved_wikitext(source_text, new_template_block)


def _inject_preserved_extras(text: str, extras: dict[str, str]) -> str:
    """Inject each ``key = value`` extension remainder into the
    ``{{DPLA metadata}}`` template inside ``text``.

    Called after ``normalize``+``canonicalize`` have finished — at
    that point the migrated template has already been stripped of
    every DPLA-canonical param, so a preserved extras value (the
    tail of a DPLA-prefixed extension: gallery, HR, wikitable, etc.)
    lands on an otherwise-clean template. Verbatim insertion, no
    whitespace normalisation — the user's structural markup is what
    it is.

    If the template's rendered form is the empty ``{{DPLA metadata}}``
    the templates library folds it onto a single line; the injection
    below places each ``| key = value`` on its own line for legibility
    matching the ``get_wiki_text`` shape a fresh upload would produce.
    Returns the original text unchanged when no ``{{DPLA metadata}}``
    template can be found (defensive — the caller has already run
    ``render_migrated_wikitext`` which guarantees it).
    """
    if not extras:
        return text
    wikicode = mwparserfromhell.parse(text)
    target = None
    for tpl in wikicode.filter_templates():
        if _template_name(tpl) == "dpla metadata":
            target = tpl
            break
    if target is None:
        return text
    # Add each param with a trailing newline on the value so the
    # closing ``}}`` lands on its own line (mwparserfromhell defaults
    # would jam it onto the last content line otherwise). Verbatim
    # value preservation: don't touch the extras themselves; only the
    # ``| key = `` surround gets shaped for readability.
    for key, value in extras.items():
        formatted_value = value if value.endswith("\n") else value + "\n"
        target.add(
            f" {key} ",
            formatted_value,
            preserve_spacing=False,
        )
    # Force a newline right after the opening ``{{DPLA metadata`` so
    # the first ``| key = value`` starts on its own line, matching
    # the multi-line shape a fresh ``get_wiki_text`` emits. Appending
    # ``"\n"`` to the template's own ``name`` attribute scopes the
    # change to this template node — a page-wide string replace
    # would also rewrite the same literal if it appeared inside a
    # preserved extra (e.g. a nested template inside a user's
    # gallery caption).
    name_str = str(target.name)
    if not name_str.endswith("\n"):
        target.name = name_str + "\n"
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

    # Guard: never strip a render-critical param into a void. This migration
    # posts only community imports (not the canonical scalars), so the strip
    # below relies on the sdc-sync's prior canonical-SDC write having landed.
    # If that write silently failed for this file, ``entity`` (fetched above,
    # before our own import) won't carry the param's SDC property, and
    # stripping it would leave the value in NEITHER the wikitext nor SDC — the
    # same blanking that ``normalize_page``'s cleanup guard prevents on the
    # sibling path. Restrict the strip to params whose SDC property is actually
    # present so an unbacked param stays on the template and still renders.
    # ``entity`` predates our community import, so this is not fooled by the
    # claims we just posted (unlike a bare "has any DPLA SDC" entity check).
    entity_statements = entity.get("statements") or entity.get("claims") or {}
    strippable_params = {
        key: value
        for key, value in canonical_params.items()
        if key not in STRIP_GUARD_SDC_PROPERTY
        or STRIP_GUARD_SDC_PROPERTY[key] in entity_statements
    }
    rewritten, _stripped = normalize(rewritten, strippable_params)
    rewritten = canonicalize(rewritten)
    # Re-inject wikitext-preserved extension remainders (galleries,
    # HRs, wikitables, etc. the user appended past the DPLA-authored
    # text) onto the migrated ``{{DPLA metadata}}`` template. These
    # can't live in SDC — Wikibase's monolingual-text validator
    # rejects vertical whitespace — so the migration preserves them
    # on the template's own parameter, where Module:DPLA's
    # ``userValue`` renderer picks them up into the yellow row. See
    # :func:`_split_extension_extras` for the detection rule.
    if plan.wikitext_preserved_extras:
        rewritten = _inject_preserved_extras(rewritten, plan.wikitext_preserved_extras)
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


def import_cross_page_community_sdc(
    *,
    source_page,
    dest_mediaid: str,
    item_metadata: dict,
    provider: dict,
    data_provider: dict,
    dpla_id: str,
    site,
    bot_accounts: frozenset[str] = DPLA_BOT_ACCOUNTS,
    summary: str | None = None,
) -> int:
    """Rescue community-authored, *in-template* metadata from a source page's
    revision history into the destination file's SDC.

    The cross-page analogue of the SDC half of :func:`migrate_legacy_file`,
    for the case where the source page is about to be destroyed (tagged as a
    duplicate) so its history — the only record of who authored each param —
    won't survive. Node-swapping the wikitext (:func:`rescue_wikitext`)
    preserves everything *outside* the metadata template, but the template's
    own params are dropped in the swap; this recovers the ones a non-bot
    editor set, via the same provenance attribution the regular migration uses
    (:func:`plan_migration`), and writes them to the destination as
    inferred-from-Wikitext SDC.

    Reads the SOURCE page's history but writes ``dest_mediaid`` (the
    DESTINATION's ``M<pageid>`` MediaInfo id — the caller resolves the pageid,
    riding out post-upload indexing lag, so this never targets a bogus "M0").
    Idempotent on the destination entity
    (:func:`entity_was_already_migrated`). Returns the number of
    community-import claims posted — 0 when the source has no legacy template,
    nothing community-authored to rescue, or the destination was already
    migrated.
    """
    from .wikimedia import dpla_metadata_params

    # Cheap pre-check before any network I/O. ``plan_migration`` returns None
    # unless the source's *current* wikitext carries a LEGACY_TEMPLATE_NAMES
    # wrapper, and that decision rests on the latest revision alone — so gate
    # on ``source_page.text`` (a single-revision read, usually already cached)
    # and skip the ``wbgetentities`` round-trip and the full
    # revision-history-with-content walk ``fetch_revision_snapshots`` does.
    #
    # This gate is also where the swap-set/rescue-set asymmetry lives, on
    # purpose: ``rescue_wikitext`` node-swaps any RESCUE_WRAPPER_NAMES wrapper
    # (so *outside*-template content is preserved for every wrapper), but only
    # the LEGACY_TEMPLATE_NAMES wrappers have their *inside*-template params
    # lifted to SDC here. The one swap-but-not-rescued wrapper is the
    # already-migrated {{DPLA metadata}} form: it carries no legacy params for
    # plan_migration to walk, so a source already on it short-circuits with 0 —
    # no worse than the pre-refactor allowlist, which rescued no inside-template
    # params at all. Extending SDC rescue to a new wrapper means adding it to
    # LEGACY_TEMPLATE_NAMES (so find_legacy_template/parse_artwork_params walk
    # it), not merely widening RESCUE_WRAPPER_NAMES.
    if find_legacy_template(source_page.text) is None:
        return 0

    mediaid = dest_mediaid
    entity = _fetch_entity_or_empty(site, mediaid)
    if entity_was_already_migrated(entity):
        return 0
    canonical_params = dpla_metadata_params(
        dpla_id, item_metadata, provider, data_provider
    )
    plan = plan_migration(
        source_page.title(),
        fetch_revision_snapshots(source_page),
        canonical_params,
        bot_accounts,
    )
    if plan is None or (not plan.community_imports and not plan.related_image_imports):
        return 0
    claims = materialize_import_claims(
        build_legacy_import_claims(plan), site=site, existing_entity=entity
    )
    if not claims:
        return 0
    post_legacy_import_claims(
        mediaid,
        claims,
        site,
        summary=summary or build_migration_summary(len(claims)),
    )
    return len(claims)


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
