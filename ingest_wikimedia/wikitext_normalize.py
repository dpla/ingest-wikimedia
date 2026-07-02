"""Strip redundant parameters from `{{DPLA metadata}}` template invocations.

Phase 1 of the SDC ↔ wikitext integration design. Runs as a follow-up
edit after `tools.sdc_sync.process_one_from_sdc` writes structured data:
once the SDC-backed render produces the same display the hardcoded
wikitext params produce, those params are redundant. Removing them
makes the wikitext canonical SDC-driven and stops the two representations
from drifting on later API edits.

Scope here is **Goal 1 only** — files that the DPLA uploader wrote in
the current `{{DPLA metadata}}` form. Legacy `{{Artwork}}` migration
(Goal 2) and richer language-template unwrapping (Goal 3 beyond the
"preserve any wrapped value" rule below) come in later phases.

Anchor for "what the wikitext should match" is :func:`dpla_metadata_params`
from ``ingest_wikimedia.wikimedia`` — the same helper ``get_wiki_text``
uses to write the wikitext at upload time. Reading both sides from one
function keeps Goal 1's "exact match" detection drift-free across future
template edits.
"""

from __future__ import annotations

import logging
import re

import mwparserfromhell

from ingest_wikimedia.sdc import (
    CASEFOLD_COMPARE_KEYS,
    casefold_for_compare,
    dates_semantically_equal,
)

# Pattern that identifies the family of Commons single-language wrapper
# templates ({{en|...}}, {{es|...}}, {{de|...}}, {{pt-br|...}}, …).
# Used solely to *recognise* a wrapper so we can decide whether stripping
# the param around it is safe — a non-matching wrapper (e.g. a language
# code not in the item's allowlist) is still detected as "this is a
# wrapper, don't compare" and the value survives the strip.
_LANG_CODE_RE = re.compile(r"^[a-z]{2,3}(?:-[a-z0-9]+)*$")

# Template name (case-folded) for the Commons {{LangSwitch|en=...|es=...}}
# multilingual selector. Always preserved — the file has been explicitly
# multilingualised by an editor, and stripping any branch would discard
# their contribution. The wider _value_matches loop already treats any
# template-wrapped value with named params as a mismatch (because
# _language_wrapper_code returns None for it), but spelling LangSwitch
# out keeps the intent explicit and makes the test for it grep-able.
_LANGSWITCH_NAME = "langswitch"


def _template_name(template) -> str:
    """Return a stripped, case-folded template name.

    mwparserfromhell preserves the exact surface text in ``template.name``
    (whitespace, capitalisation, namespace fragment). MediaWiki normalises
    templates to underscore-collapsed, first-letter-upper for matching,
    but for our purposes case-insensitive whitespace-stripped is enough —
    we're matching against a fixed allowlist of names we ourselves wrote.
    """
    return str(template.name).strip().casefold()


def _matches_template_name(template, expected: str) -> bool:
    return _template_name(template) == expected.casefold()


def _language_wrapper_code(template) -> str | None:
    """Return the language code if ``template`` is a single-language
    wrapper of the ``{{xx|texto}}`` shape, else None.

    A wrapper is the "one positional arg, name is a language code" shape.
    Anything with named args, multiple positionals, or a non-language-code
    name returns None — those are normal sub-templates or LangSwitch.

    Returning the *code* rather than a bool lets callers decide what to
    do with each language: Phase 1 only unwraps codes in
    ``_LANGUAGES_SAFE_TO_UNWRAP`` (English), but the wider "this is a
    wrapper, don't compare" check applies to any code.
    """
    name = _template_name(template)
    if not _LANG_CODE_RE.match(name):
        return None
    positionals = [p for p in template.params if p.showkey is False]
    named = [p for p in template.params if p.showkey is True]
    if len(positionals) != 1 or named:
        return None
    return name


def _canonical_value(value: str) -> str:
    """Strip whitespace mwparserfromhell preserves around param values.

    Wikitext template params let the editor write `| key = value ` with
    arbitrary leading/trailing whitespace that the rendered output
    discards. Equality comparison must match the renderer's behavior, not
    the source-text byte sequence.
    """
    return value.strip()


def _value_matches(
    wikitext_value: str,
    expected: str,
    languages: frozenset[str] | set[str],
    *,
    param_name: str | None = None,
) -> bool:
    """Compare a wikitext value to its canonical-DPLA expectation.

    Direct equality on whitespace-normalized values handles every scalar
    param the uploader writes — including ``permission``, whose canonical
    value is a wrapping ``{{PD-USGov}}``-style template invocation that
    the editor would copy verbatim.

    As a fallback for editor-added language wrappers around the canonical
    value, a wikitext value of the form ``{{<code>|...}}`` is unwrapped
    before re-comparing — but only if ``<code>`` is in ``languages``,
    the per-item allowlist of safe-to-unwrap codes (always includes
    ``en``, plus any ISO 639-1 codes derived from the DPLA record's
    ``sourceResource.language`` field). For a non-English DPLA item
    whose record declares Spanish, ``{{es|<canonical Spanish>}}`` is
    safely unwrappable because the wrapper is purely a language-tag
    annotation over a value the uploader already wrote in Spanish; for
    an English item, ``{{es|A Title}}`` survives even when the inner
    text byte-matches the canonical English (the ``es`` tag is editor-
    contributed translation metadata, not a redundant wrapper).

    Two tolerance widenings run AFTER the byte-exact and language-
    wrapper paths fail:

      * For ``param_name == "date"`` the two values are compared via
        :func:`ingest_wikimedia.sdc.dates_semantically_equal` — so an
        override like ``| date = 19 November 1902`` collapses cleanly
        against a canonical ``1902-11-19``.
      * For every scalar key, a casefold-and-trim comparator
        (:func:`ingest_wikimedia.sdc.casefold_for_compare`) folds both
        sides through the same normaliser used by the SDC dedup
        comparator, so a description differing only by a trailing
        period, wrapping brackets, or an editor's case change strips
        cleanly. Codepoint identifiers (``hub``, ``institution``,
        ``dpla_id``, ``url``, ``local_id``) are excluded from the
        casefold path — those are opaque tokens where a case change
        genuinely represents a different value.

    Other template-wrapped values (``{{LangSwitch|...}}``, an Information
    sub-template, a citation) are conservatively a mismatch — they
    represent deliberate editor structure that the strip must preserve.
    """
    if _canonical_value(wikitext_value) == _canonical_value(expected):
        return True

    parsed = mwparserfromhell.parse(wikitext_value)
    templates = parsed.filter_templates(recursive=False)
    if len(templates) == 1:
        tpl = templates[0]
        # LangSwitch always preserves — defensive guard against any future
        # change to _language_wrapper_code accidentally treating it as a
        # wrapper. (Today it's already not a wrapper because the name
        # doesn't match _LANG_CODE_RE, but spelling it out is cheap and
        # makes the intent reviewable.)
        if _template_name(tpl) == _LANGSWITCH_NAME:
            return False
        lang = _language_wrapper_code(tpl)
        if lang is not None and lang in languages:
            inner = str(tpl.get(1).value)
            if _canonical_value(inner) == _canonical_value(expected):
                return True
            # Fall through to the tolerance-widening checks below on the
            # unwrapped inner value — a bracketed / trailing-period /
            # cased variant inside ``{{en|…}}`` still deserves to dedup.
            wikitext_value = inner

    if param_name == "date" and dates_semantically_equal(
        _canonical_value(wikitext_value), _canonical_value(expected)
    ):
        return True

    if param_name in CASEFOLD_COMPARE_KEYS:
        folded_wiki = casefold_for_compare(_canonical_value(wikitext_value))
        folded_expected = casefold_for_compare(_canonical_value(expected))
        if folded_wiki and folded_wiki == folded_expected:
            return True

    return False


# Top-level scalar params on a flat-shape `{{DPLA metadata}}` invocation.
# Every one is a string compared via `_value_matches`. The post-flat-shape
# rewrite collapsed the previous `source` and `institution` sub-template
# rows into flat scalars (hub / institution Q-ID / url / dpla_id /
# local_id) so the comparator's dispatch is uniform across all rows.
_SCALAR_PARAMS = (
    "title",
    "description",
    "date",
    "permission",
    "creator",
    "hub",
    "institution",
    "url",
    "dpla_id",
    "local_id",
)

# Legacy param shapes that an older upload (pre-flat-shape) may carry.
# Their canonical expectation comes from the same flat scalars in
# `expected_params`; the comparator handles them in a fall-through path
# at the bottom of `normalize` so the strip behavior is symmetric with
# new-shape files. Each tuple is (wikitext-param-name, sub-template-name,
# canonical-key-mapping) where the mapping says which inner sub-template
# arg corresponds to which canonical-params key.
_LEGACY_SOURCE_PARAM = "source"
_LEGACY_INSTITUTION_PARAM = "Institution"
_LEGACY_CREATOR_PARAM = "Other fields 1"


def _find_dpla_metadata_template(wikicode):
    """Return the first ``{{DPLA metadata}}`` template node, or None."""
    for tpl in wikicode.filter_templates():
        if _matches_template_name(tpl, "dpla metadata"):
            return tpl
    return None


def has_dpla_metadata_template(wikitext: str) -> bool:
    """Public test for ``{{DPLA metadata}}`` presence in a wikitext string.

    Lets ``tools/sdc_sync.py``'s post-SDC cleanup dispatcher decide
    between the strip path (this module) and the migrate path
    (:mod:`ingest_wikimedia.legacy_artwork`) without paying the cost
    of a full parse + strip pass when the file isn't on the new
    template form. Pure — no API calls."""
    return _find_dpla_metadata_template(mwparserfromhell.parse(wikitext)) is not None


def _normalize_param_name(param) -> str:
    return str(param.name).strip().casefold()


def _find_param(template, expected_name: str):
    """Return the param node whose case-folded name matches, or None.

    Commons editors capitalize template param names inconsistently
    (``| Institution`` vs ``| institution``, ``| Other fields 1`` vs
    ``| other fields 1``). Case-fold both sides so the comparator
    matches the renderer's behavior — Module:DPLA treats them as the
    same param regardless of source casing.
    """
    target = expected_name.casefold()
    for param in template.params:
        if _normalize_param_name(param) == target:
            return param
    return None


def normalize(wikitext: str, expected_params: dict) -> tuple[str, list[str]]:
    """Strip parameters from a ``{{DPLA metadata}}`` template that exactly
    match the canonical values supplied in ``expected_params``.

    ``expected_params`` is the dict returned by
    :func:`ingest_wikimedia.wikimedia.dpla_metadata_params`. The function
    is purely textual — no API calls, no SDC reads — because Phase 1
    operates on the contract "if SDC was just written from these
    expected values, and the wikitext still carries them verbatim, the
    wikitext is redundant."

    Returns a ``(new_wikitext, stripped)`` tuple where ``stripped`` is
    the list of parameter names that were removed (empty when nothing
    changed). The caller decides whether to save based on whether
    ``stripped`` is non-empty; the returned wikitext is always a valid
    serialization regardless.

    Conservative on every edge case: missing template, multiple
    templates, wrapped values with templates we don't recognise, params
    we don't know how to compare — all leave the wikitext untouched on
    that param. The cost of an unstripped match is a redundant param
    that the next pass will catch; the cost of an incorrect strip is
    data loss.
    """
    wikicode = mwparserfromhell.parse(wikitext)
    template = _find_dpla_metadata_template(wikicode)
    if template is None:
        return wikitext, []

    stripped: list[str] = []
    # Per-item unwrap allowlist. Items with no DPLA-supplied language
    # entries still get English (the helper always seeds ``en``); items
    # whose record declares Spanish/French/etc. get those codes added so
    # an editor-wrapped Spanish title on a Spanish DPLA item is treated
    # as redundant rather than as an editor contribution.
    languages = expected_params.get("languages", frozenset({"en"}))

    # Flat-shape strip pass: every canonical param is a string scalar.
    # ``creator`` is conditional — when DPLA has no creator value, the
    # uploader doesn't emit the row, so an existing ``creator =`` row
    # is an editor contribution we must preserve.
    for param_name in _SCALAR_PARAMS:
        expected_value = expected_params.get(param_name, "")
        if param_name == "creator" and not expected_value:
            continue
        param = _find_param(template, param_name)
        if param is None:
            continue
        if _value_matches(
            str(param.value), expected_value, languages, param_name=param_name
        ):
            template.remove(param, keep_field=False)
            stripped.append(param_name)

    # Legacy-shape strip pass. Pre-flat-shape uploads have
    # ``source = {{DPLA|...}}`` / ``Institution = {{Institution|...}}``
    # / ``Other fields 1 = {{InFi|Creator|...}}`` rows that carry the
    # same canonical values in nested sub-template form. The
    # comparator recognises each shape and strips when its inner
    # values match the flat-canonical equivalents — same redundancy
    # contract as the flat shape, just expressed via the legacy
    # sub-template encoding.
    _strip_legacy_source(template, expected_params, stripped)
    _strip_legacy_institution(template, expected_params, stripped)
    _strip_legacy_creator(template, expected_params, stripped)

    return str(wikicode), stripped


def _strip_legacy_source(template, expected_params: dict, stripped: list[str]) -> None:
    """Strip a legacy ``source = {{DPLA|<inst>|hub=...|url=...|dpla_id=...|local_id=...}}``
    row when every inner param matches the flat-canonical equivalent.

    The comparator compares the *parsed* sub-template params against
    flat-canonical: hub→hub, positional-1→institution, url→url,
    dpla_id→dpla_id, local_id→local_id. Extra args on the wikitext
    side disqualify the strip (an editor may have added a param we
    don't know to match)."""
    param = _find_param(template, _LEGACY_SOURCE_PARAM)
    if param is None:
        return
    parsed = _parse_inner_template(str(param.value), "dpla")
    if parsed is None:
        return
    expected = {
        "1": expected_params.get("institution", ""),
        "hub": expected_params.get("hub", ""),
        "url": expected_params.get("url", ""),
        "dpla_id": expected_params.get("dpla_id", ""),
        "local_id": expected_params.get("local_id", ""),
    }
    if set(parsed) != set(expected):
        return
    if all(parsed[k] == _canonical_value(v) for k, v in expected.items()):
        template.remove(param, keep_field=False)
        stripped.append(_LEGACY_SOURCE_PARAM)


def _strip_legacy_institution(
    template, expected_params: dict, stripped: list[str]
) -> None:
    """Strip a legacy ``Institution = {{Institution|wikidata=Q...}}`` row
    when the inner Q-ID matches ``expected_params["institution"]``."""
    param = _find_param(template, _LEGACY_INSTITUTION_PARAM)
    if param is None:
        return
    parsed = _parse_inner_template(str(param.value), "institution")
    if parsed is None:
        return
    if set(parsed) != {"wikidata"}:
        return
    if parsed["wikidata"] == _canonical_value(expected_params.get("institution", "")):
        template.remove(param, keep_field=False)
        stripped.append(_LEGACY_INSTITUTION_PARAM)


def _strip_legacy_creator(template, expected_params: dict, stripped: list[str]) -> None:
    """Strip a legacy ``Other fields 1 = {{InFi|Creator|<value>|id=fileinfotpl_aut}}``
    row when its second positional arg matches ``expected_params["creator"]``.
    Bails out when the expected creator is empty — same conservative
    rule as the flat-shape creator strip."""
    creator_expected = expected_params.get("creator", "")
    if not creator_expected:
        return
    param = _find_param(template, _LEGACY_CREATOR_PARAM)
    if param is None:
        return
    parsed = _parse_inner_template(str(param.value), "infi")
    if parsed is None:
        return
    # Match the exact-keys check the source/institution legacy strips
    # do. Any extra arg an editor added (e.g. an inline ``|note=``)
    # disqualifies the strip — preserving the editor's contribution.
    if set(parsed) != {"1", "2", "id"}:
        return
    if parsed.get("1") != "Creator":
        return
    if parsed.get("id") != "fileinfotpl_aut":
        return
    if parsed.get("2") != _canonical_value(creator_expected):
        return
    template.remove(param, keep_field=False)
    stripped.append(_LEGACY_CREATOR_PARAM)


def _parse_inner_template(value: str, expected_name: str) -> dict | None:
    """Parse a single-template wikitext value into a flat
    ``{param_name: stripped_value}`` dict, or None when the value
    doesn't contain exactly one template with the given case-folded
    name.

    Positional args use string keys ``"1"``, ``"2"``, ... (matching
    mwparserfromhell's convention). All values are whitespace-
    stripped — the comparator wants to match the renderer's behavior,
    not the source bytes."""
    parsed = mwparserfromhell.parse(value)
    templates = parsed.filter_templates(recursive=False)
    if len(templates) != 1:
        return None
    tpl = templates[0]
    if not _matches_template_name(tpl, expected_name):
        return None
    return {str(p.name).strip(): _canonical_value(str(p.value)) for p in tpl.params}


def canonicalize(wikitext: str) -> str:
    """Enforce the canonical whitespace shape on a ``{{DPLA metadata}}``
    page.

    The canonical shape is what :func:`ingest_wikimedia.wikimedia.get_wiki_text`
    emits for a fresh upload:

    .. code-block:: text

        == {{int:filedesc}} ==

        {{DPLA metadata
        | title = ...
        ...
        }}

    Specifically: left-justified template with no leading whitespace
    on any line, exactly one blank line between the section heading
    and the template, no space between the opening braces and the
    template name, params one per line on ``| key = value``, closing
    ``}}`` on its own line — or, when every param has been stripped,
    the template collapses to single-line ``{{DPLA metadata}}``.

    Pure: takes a wikitext string, returns a wikitext string. The
    surrounding page-level metadata (license tags, categories,
    assessment blocks, the section heading itself) is left untouched.
    Files that don't contain a ``{{DPLA metadata}}`` template are
    returned unchanged.
    """
    wikicode = mwparserfromhell.parse(wikitext)
    template = _find_dpla_metadata_template(wikicode)
    if template is None:
        return wikitext

    # Collect params, dropping leading/trailing whitespace from both
    # name and value so the canonical form matches the renderer's
    # whitespace-tolerant parse. Values may legitimately contain
    # internal whitespace (multi-line descriptions, etc.) — only
    # outer whitespace is trimmed.
    params: list[tuple[str, str]] = []
    for p in template.params:
        params.append((str(p.name).strip(), str(p.value).strip()))

    if not params:
        canonical_template = "{{DPLA metadata}}"
    else:
        lines = ["{{DPLA metadata"]
        for k, v in params:
            lines.append(f"| {k} = {v}")
        lines.append("}}")
        canonical_template = "\n".join(lines)

    wikicode.replace(template, canonical_template)
    text = str(wikicode)

    # Ensure exactly one blank line between the section heading and
    # the template. The default ``get_wiki_text`` output emits this
    # blank line; older pages may have a different separator (no
    # blank line, indentation, multiple blank lines), which the
    # regex normalises.
    text = re.sub(
        r"(==\s*\{\{int:filedesc\}\}\s*==)\s*\n\s*(\{\{DPLA metadata)",
        r"\1\n\n\2",
        text,
    )

    return text


def normalize_page(file_page, expected_params: dict, edit_summary: str) -> bool:
    """Strip redundant params + canonicalize whitespace on a pywikibot
    ``FilePage``; save if any change resulted.

    Returns True when a save was performed, False when the page is a
    redirect or the wikitext is already canonical and has no
    redundant params. All exceptions propagate to the caller, which
    is expected to wrap this in a per-file try/except so a normalize
    failure doesn't abort the SDC-sync batch.

    Saves on any change — including whitespace-only canonicalisation
    — so files where the params don't strip but the template was
    written with non-canonical indentation (a hand-edit, a legacy
    pre-#297 upload) get cleaned up on the same pass. The previous
    behaviour gated the save on ``stripped`` being non-empty, which
    left those pages permanently mis-formatted.

    Redirect guard: pywikibot reads a redirect page's ``.text`` as
    the redirect target's wikitext, but ``.save()`` writes to the
    redirect page itself — saving would replace the ``#REDIRECT``
    line with the target's content and break the redirect.
    """
    if file_page.isRedirectPage():
        return False
    original = file_page.text or ""
    stripped_text, stripped = normalize(original, expected_params)
    canonical_text = canonicalize(stripped_text)
    if canonical_text == original:
        return False
    file_page.text = canonical_text
    if stripped:
        logging.info(
            f" -- {file_page.title()}: stripping redundant DPLA-metadata params: "
            f"{', '.join(stripped)}"
        )
    else:
        logging.info(
            f" -- {file_page.title()}: canonicalising DPLA-metadata template"
            " whitespace (no redundant params to strip)."
        )
    file_page.save(summary=edit_summary, minor=True, bot=True)
    return True
