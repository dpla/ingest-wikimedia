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
                wikitext_preserved_extras[key] = extras
                dpla_originated[key] = value
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
    # Any remaining ``{{…}}`` template-shaped value we don't recognise
    # is dropped rather than passed through as a raw string. See the
    # returns-``None`` clause of the docstring for the rationale.
    if stripped.startswith("{{") and stripped.endswith("}}"):
        return None
    return stripped


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
        if value.startswith(_CREATOR_QID_PREFIX):
            return {
                "type": "statement",
                "rank": "normal",
                "_phase3a_pending_creator_qid": value[len(_CREATOR_QID_PREFIX) :],
                "_permalink": permalink,
            }
        if value.startswith(_CREATOR_PAGE_PREFIX):
            return {
                "type": "statement",
                "rank": "normal",
                "_phase3a_pending_creator_page": value[len(_CREATOR_PAGE_PREFIX) :],
                "_permalink": permalink,
            }

    mapping = LEGACY_IMPORT_PROPERTY.get(canonical_key)
    if mapping is None:
        return None
    prop, kind = mapping
    if kind == "monolingualtext":
        _validate_wikibase_text(value, prop, canonical_key)
        datavalue = _monolingualtext_datavalue(value)
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


def _resolve_commons_creator_qid(site, page_title: str) -> str | None:
    """Look up the Wikidata QID linked from a Commons ``Creator:<title>``
    page via the ``prop=pageprops`` API.

    Returns the QID string (``"Q…"``) when the Creator page exists and
    has a ``wikibase_item`` sitelink; ``None`` for missing pages,
    orphaned Creator pages, or any API failure. Falling back to
    ``None`` is intentional: the caller substitutes a P170 somevalue
    + P2093 stated-as name claim so the community contribution is
    preserved as a stated-as string even when the QID resolution
    can't succeed — same outcome as a plain ``| creator = <name>``
    parameter would produce.

    Requires ``site`` to be a live pywikibot Site; test callers can
    pass ``None`` to short-circuit to the name-only fallback path.
    """
    if site is None or not page_title:
        return None
    try:
        response = site.simple_request(
            action="query",
            prop="pageprops",
            titles=f"Creator:{page_title}",
            format="json",
        ).submit()
    except Exception:
        return None
    pages = (response.get("query") or {}).get("pages") or {}
    for page in pages.values():
        pp = page.get("pageprops") or {}
        qid = pp.get("wikibase_item")
        if isinstance(qid, str) and qid.startswith("Q"):
            return qid
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


def materialize_import_claims(
    claims: list[dict],
    *,
    site=None,
    existing_entity: dict | None = None,
) -> list[dict]:
    """Walk an import-claim list and substitute every Phase-3a
    placeholder for a real Wikibase statement.

    Handles two placeholder shapes: date placeholders (P571 time
    statements via :func:`materialize_pending_date_claim`) and
    creator placeholders (P170 statements via
    :func:`materialize_pending_creator_claim`). Claims without a
    placeholder marker pass through unchanged. Placeholders whose
    materialiser returns ``None`` — duplicate of existing SDC, or
    an unparseable value — are dropped from the returned list.

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
    text, _swapped = _swap_wrapper_node(
        original_text, new_template_block, LEGACY_TEMPLATE_NAMES
    )
    return text


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
    """
    wikicode = mwparserfromhell.parse(original_text)
    for tpl in wikicode.filter_templates():
        if _template_name(tpl) in wrapper_names:
            wikicode.replace(tpl, _extract_dpla_metadata_template(new_template_block))
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

    rewritten, _stripped = normalize(rewritten, canonical_params)
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
    dest_page,
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

    Reads the SOURCE page's history but writes the DESTINATION's MediaInfo
    entity. Idempotent on the destination entity
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

    mediaid = f"M{dest_page.pageid}"
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
    if plan is None or not plan.community_imports:
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
