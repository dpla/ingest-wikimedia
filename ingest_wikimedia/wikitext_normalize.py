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

# ISO 639-1 / -2 / -3 codes the Commons {{xx|...}} language-wrapper convention
# accepts. We don't need an authoritative list — we just need to recognise
# "this looks like a language wrapper" so we never strip a wrapped value.
# A template whose name matches this pattern AND that has exactly one
# positional argument is treated as a language-tagged value; any positional
# count beyond that is left alone (likely a normal sub-template).
_LANG_CODE_RE = re.compile(r"^[a-z]{2,3}(?:-[a-z0-9]+)*$")


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


def _is_language_wrapper(template) -> bool:
    """Detect a single-language wrapper template like ``{{es|texto}}``.

    Returns True only for the "one positional arg, name is a language
    code" shape. Anything with named args, multiple positionals, or a
    non-language-code name is left alone.
    """
    name = _template_name(template)
    if not _LANG_CODE_RE.match(name):
        return False
    positionals = [p for p in template.params if p.showkey is False]
    named = [p for p in template.params if p.showkey is True]
    return len(positionals) == 1 and not named


def _canonical_value(value: str) -> str:
    """Strip whitespace mwparserfromhell preserves around param values.

    Wikitext template params let the editor write `| key = value ` with
    arbitrary leading/trailing whitespace that the rendered output
    discards. Equality comparison must match the renderer's behavior, not
    the source-text byte sequence.
    """
    return value.strip()


def _value_matches(wikitext_value: str, expected: str) -> bool:
    """Compare a wikitext value to its canonical-DPLA expectation.

    Direct equality on whitespace-normalized values handles every scalar
    param the uploader writes — including ``permission``, whose canonical
    value is a wrapping ``{{PD-USGov}}``-style template invocation that
    the editor would copy verbatim.

    As a fallback for editor-added language tags, a wikitext value of the
    shape ``{{<lang>|...}}`` is unwrapped before re-comparing — so the
    canonical English ``"Foo"`` still matches ``{{en|Foo}}``.  Any other
    template-wrapped value (``{{LangSwitch|...}}``, an Information
    sub-template, a citation) is conservatively a mismatch — those
    represent deliberate editor structure that the strip must preserve.
    """
    if _canonical_value(wikitext_value) == _canonical_value(expected):
        return True

    parsed = mwparserfromhell.parse(wikitext_value)
    templates = parsed.filter_templates(recursive=False)
    if len(templates) == 1 and _is_language_wrapper(templates[0]):
        inner = str(templates[0].get(1).value)
        return _canonical_value(inner) == _canonical_value(expected)

    return False


def _subtemplate_matches(wikitext_value: str, expected_subtemplate: dict) -> bool:
    """Compare a sub-template param (e.g. ``| source = {{DPLA|...}}``) to
    the canonical expectation.

    ``expected_subtemplate`` is the shape produced by
    :func:`ingest_wikimedia.wikimedia.dpla_metadata_params` — ``name`` is
    the inner template's name, ``params`` is its argument dict where
    positional args are keyed by string indices (``"1"``, ``"2"``).

    Returns True only when the wikitext value contains exactly one
    template, that template's name matches, and every expected param is
    present with the same value. Extra params on the wikitext side count
    as a mismatch — we never strip when the editor added something we
    don't know to match.
    """
    parsed = mwparserfromhell.parse(wikitext_value)
    templates = parsed.filter_templates(recursive=False)
    if len(templates) != 1:
        return False
    tpl = templates[0]
    if not _matches_template_name(tpl, expected_subtemplate["name"]):
        return False
    expected_params = expected_subtemplate["params"]
    wikitext_params = {
        str(p.name).strip(): _canonical_value(str(p.value)) for p in tpl.params
    }
    if set(wikitext_params) != set(expected_params):
        return False
    return all(
        wikitext_params[k] == _canonical_value(v) for k, v in expected_params.items()
    )


# Top-level params expected on a `{{DPLA metadata}}` invocation written by
# the DPLA uploader, mapped to their kind (scalar vs. sub-template) so we
# can dispatch to the right comparator. Names mirror what `get_wiki_text`
# emits and what `dpla_metadata_params` returns.
_SCALAR_PARAMS = ("title", "description", "date", "permission")
_SUBTEMPLATE_PARAMS = ("source", "institution")
# The uploader writes the creator on a numbered "Other fields" row
# (``Other fields 1 = {{InFi|Creator|...}}``). Module:DPLA accepts both
# that form and a top-level ``creator =`` param, but only the former is
# what the current uploader emits, so that's what we look for here.
_CREATOR_PARAM_NAME = "other fields 1"


def _find_dpla_metadata_template(wikicode):
    """Return the first ``{{DPLA metadata}}`` template node, or None."""
    for tpl in wikicode.filter_templates():
        if _matches_template_name(tpl, "dpla metadata"):
            return tpl
    return None


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

    for param_name in _SCALAR_PARAMS:
        param = _find_param(template, param_name)
        if param is None:
            continue
        if _value_matches(str(param.value), expected_params[param_name]):
            template.remove(param, keep_field=False)
            stripped.append(param_name)

    for param_name in _SUBTEMPLATE_PARAMS:
        param = _find_param(template, param_name)
        if param is None:
            continue
        if _subtemplate_matches(str(param.value), expected_params[param_name]):
            template.remove(param, keep_field=False)
            stripped.append(param_name)

    # Creator lives on `Other fields 1` as `{{InFi|Creator|<value>|id=...}}`.
    # Only compare/strip when the expected creator is non-empty — when DPLA
    # has no creator value, the uploader doesn't emit the row at all, so
    # there's nothing to compare to.
    creator_expected = expected_params["creator"]
    creator_param = _find_param(template, _CREATOR_PARAM_NAME)
    if (
        creator_param is not None
        and creator_expected["params"]["2"]
        and _subtemplate_matches(str(creator_param.value), creator_expected)
    ):
        template.remove(creator_param, keep_field=False)
        stripped.append(_CREATOR_PARAM_NAME)

    return str(wikicode), stripped


def normalize_page(file_page, expected_params: dict, edit_summary: str) -> bool:
    """Apply :func:`normalize` to a pywikibot ``FilePage`` and save if changed.

    Returns True when a save was performed, False when the page is a
    redirect, already canonical, or otherwise out of scope (no params
    to strip). All exceptions propagate to the caller, which is expected
    to wrap this in a per-file try/except so a normalize failure doesn't
    abort the SDC-sync batch.

    Redirect guard: pywikibot reads a redirect page's ``.text`` as the
    redirect target's wikitext, but ``.save()`` writes to the redirect
    page itself — saving would replace the ``#REDIRECT`` line with the
    target's content and break the redirect. Skip redirects entirely;
    the SDC sync targeted the target page anyway via its M-id.
    """
    if file_page.isRedirectPage():
        return False
    original = file_page.text or ""
    new_text, stripped = normalize(original, expected_params)
    if not stripped:
        return False
    file_page.text = new_text
    logging.info(
        f" -- {file_page.title()}: stripping redundant DPLA-metadata params: "
        f"{', '.join(stripped)}"
    )
    file_page.save(summary=edit_summary, minor=True, bot=True)
    return True
