"""Tests for the post-SDC wikitext normalizer (Phase 1 / Goal 1).

The normalizer's job is to strip ``{{DPLA metadata}}`` template parameters
whose values exactly match the SDC just written — leaving the wikitext
rendered from SDC by ``Module:DPLA`` rather than from hardcoded text.

Two layers under test:

* ``dpla_metadata_params`` (in ``ingest_wikimedia.wikimedia``) — the
  canonical-params helper that both ``get_wiki_text`` and the normalizer
  consult, so they can never drift.
* ``normalize`` (in ``ingest_wikimedia.wikitext_normalize``) — the
  string-in, string-out comparator that decides which params to strip.
"""

from __future__ import annotations

from ingest_wikimedia.wikimedia import dpla_metadata_params, get_wiki_text
from ingest_wikimedia.wikitext_normalize import canonicalize, normalize


def _minimal_item():
    return {
        "rights": "http://creativecommons.org/publicdomain/zero/1.0/",
        "isShownAt": "https://example.org/item/123",
        "sourceResource": {
            "creator": ["A Creator"],
            "title": ["A Title"],
            "description": ["A description"],
            "date": [{"displayDate": "1900"}],
            "identifier": ["local-123"],
        },
    }


_PROVIDER = {"Wikidata": "Q1"}
_DATA_PROVIDER = {"Wikidata": "Q2"}


# ---------------------------------------------------------------------------
# dpla_metadata_params — anchors the two-flow drift problem
# ---------------------------------------------------------------------------


def test_dpla_metadata_params_has_every_expected_top_level_key():
    """Goal 1's comparator dispatches by top-level key, so the helper's
    output shape is the contract we test most explicitly."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    assert set(params) == {
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
        "languages",
    }


def test_dpla_metadata_params_scalar_values_match_extract_strings():
    """The scalar-typed params (title/description/date) come straight
    from the source-resource lists via ``extract_strings``."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    assert params["title"] == "A Title"
    assert params["description"] == "A description"
    assert params["date"] == "1900"


def test_dpla_metadata_params_permission_is_empty_for_unmapped_rights():
    """When ``edm:rights`` is an unknown URI, ``get_permissions_template``
    returns the empty string and the canonical ``permission`` value
    must be empty too — never the malformed ``{{}}`` that wrapping an
    empty string would produce. Same goes for an unmapped
    RIGHTS_STATEMENTS URL, where ``get_permissions`` would otherwise
    yield ``{{ | Qxxx}}``.

    A blank permission means ``get_wiki_text`` renders a bare
    ``| permission =`` row, which is graceful degradation; a literal
    ``{{}}`` would have rendered as a broken template invocation.
    """
    item = _minimal_item()
    item["rights"] = "https://example.org/unmapped-rights-uri"
    params = dpla_metadata_params("abc123", item, _PROVIDER, _DATA_PROVIDER)
    assert params["permission"] == ""
    rendered = get_wiki_text("abc123", item, _PROVIDER, _DATA_PROVIDER)
    assert "{{}}" not in rendered


def test_dpla_metadata_params_permission_is_template_wrapped():
    """``permission`` is special: the rendered wikitext value is itself
    a ``{{<template>}}`` invocation, so the canonical value must include
    those braces verbatim for the comparator to match against what the
    wikitext stores."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    # CC-Zero rights → cc-zero permission template.
    assert params["permission"] == "{{cc-zero}}"


def test_dpla_metadata_params_flat_source_fields():
    """Source fields are flat scalars on the canonical-params dict —
    no more nested ``source = {{DPLA|...}}`` sub-template. Each value
    mirrors the wikitext key the uploader emits."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    assert params["institution"] == "Q2"
    assert params["hub"] == "Q1"
    assert params["dpla_id"] == "abc123"
    assert params["local_id"] == "local-123"
    assert params["url"] == "https://example.org/item/123"


def test_dpla_metadata_params_creator_is_flat_string():
    """``creator`` is a plain string on the canonical-params dict —
    no ``{{InFi|Creator|...}}`` sub-template shape. Module:DPLA reads
    the flat value directly."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    assert params["creator"] == "A Creator"


def test_dpla_metadata_params_drives_get_wiki_text_unchanged():
    """Regression guard against drift: the two-flow problem is exactly
    that ``get_wiki_text`` and the normalizer's expected-params source
    must not diverge. Pin the rendered output's key fragments to confirm
    refactoring the helper didn't silently change the rendered template."""
    rendered = get_wiki_text("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    # Every canonical param value appears in the rendered wikitext.
    for expected in ("A Title", "A description", "1900", "{{cc-zero}}", "Q2", "Q1"):
        assert expected in rendered
    # Flat shape: no ``{{DPLA|...}}`` sub-template inside the
    # template params, no ``{{Institution|wikidata=...}}`` sub-template
    # inside the params, no ``{{InFi|Creator|...}}`` sub-template inside
    # the params. The wikitext that wraps ``{{DPLA metadata}}`` itself
    # is still that template; we're only checking the param values.
    assert "source = {{" not in rendered
    assert "Institution = {{" not in rendered
    assert "Other fields 1 = {{" not in rendered


# ---------------------------------------------------------------------------
# normalize — the strip-redundant-params pass
# ---------------------------------------------------------------------------


def _build_full_wikitext(params: dict) -> str:
    """Produce a flat-shape `{{DPLA metadata}}` wikitext block carrying
    every param at its canonical value — i.e. the worst-case "fresh
    DPLA-bot upload with nothing user-touched" case where every param
    is strippable. Matches what ``get_wiki_text`` actually emits."""
    return (
        "== {{int:filedesc}} ==\n"
        "{{DPLA metadata\n"
        f"| creator = {params['creator']}\n"
        f"| title = {params['title']}\n"
        f"| description = {params['description']}\n"
        f"| date = {params['date']}\n"
        f"| permission = {params['permission']}\n"
        f"| hub = {params['hub']}\n"
        f"| institution = {params['institution']}\n"
        f"| url = {params['url']}\n"
        f"| dpla_id = {params['dpla_id']}\n"
        f"| local_id = {params['local_id']}\n"
        "}}\n"
    )


def _build_legacy_wikitext(params: dict) -> str:
    """Produce a *legacy-shape* ``{{DPLA metadata}}`` wikitext block —
    the pre-flat-shape form an existing Commons page may carry. Tests
    the dual-path strip behaviour: every legacy-shape param should
    strip identically to its flat-shape equivalent when the canonical
    values match."""
    return (
        "== {{int:filedesc}} ==\n"
        "{{DPLA metadata\n"
        f"| Other fields 1 = {{{{InFi|Creator|{params['creator']}|id=fileinfotpl_aut}}}}\n"
        f"| title = {params['title']}\n"
        f"| description = {params['description']}\n"
        f"| date = {params['date']}\n"
        f"| permission = {params['permission']}\n"
        f"| source = {{{{DPLA|{params['institution']}|hub={params['hub']}"
        f"|url={params['url']}|dpla_id={params['dpla_id']}"
        f"|local_id={params['local_id']}}}}}\n"
        f"| Institution = {{{{Institution|wikidata={params['institution']}}}}}\n"
        "}}\n"
    )


def test_normalize_strips_every_param_on_a_pristine_dpla_bot_upload():
    """The all-match case on flat shape: every param is canonical,
    every param goes."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    wikitext = _build_full_wikitext(params)
    new_text, stripped = normalize(wikitext, params)
    assert set(stripped) == {
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
    }
    # Every value is gone from the new wikitext.
    for absent in ("A Title", "A description", "1900", "Q1", "Q2", "abc123"):
        assert absent not in new_text


def test_normalize_strips_every_param_on_a_legacy_shape_upload():
    """The all-match case on legacy shape: a file with the pre-flat
    ``source = {{DPLA|...}}`` / ``Institution = {{Institution|...}}``
    / ``Other fields 1 = {{InFi|Creator|...}}`` rows still strips
    each row when its inner values match the flat-canonical
    equivalents. Same redundancy contract, different encoding."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    wikitext = _build_legacy_wikitext(params)
    new_text, stripped = normalize(wikitext, params)
    # The legacy rows strip under their legacy keys; the flat rows
    # absent in legacy-shape wikitext stay absent from `stripped`.
    assert "source" in stripped
    assert "Institution" in stripped
    assert "Other fields 1" in stripped
    assert "title" in stripped
    assert "description" in stripped
    assert "date" in stripped
    assert "permission" in stripped
    # Inner sub-template values are gone — the legacy strip removed
    # the full row, not just the canonical text inside.
    assert "{{DPLA|" not in new_text
    assert "{{Institution|" not in new_text
    assert "{{InFi|" not in new_text


def test_normalize_preserves_param_with_edited_value():
    """The community-edited case: title was changed by an editor — that
    param must survive, while everything else canonical strips."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    wikitext = _build_full_wikitext(params).replace(
        "| title = A Title", "| title = Editor's Better Title"
    )
    new_text, stripped = normalize(wikitext, params)
    assert "title" not in stripped
    assert "Editor's Better Title" in new_text
    # Other params still strip — preservation is param-by-param.
    assert "description" in stripped


def test_normalize_preserves_language_wrapped_when_inner_doesnt_match():
    """``{{es|<spanish>}}`` is a deliberate community-translated value
    and must never be stripped, even if structurally we could match it."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    wikitext = _build_full_wikitext(params).replace(
        "| description = A description",
        "| description = {{es|Una descripción}}",
    )
    new_text, stripped = normalize(wikitext, params)
    assert "description" not in stripped
    assert "{{es|Una descripción}}" in new_text


def test_normalize_preserves_non_english_wrapper_even_when_inner_matches():
    """A non-English wrapper like ``{{es|A Title}}`` where the inner text
    happens to byte-match the canonical English value still must survive
    the strip. The language tag is an editor contribution recording that
    the value is also a valid Spanish rendering; losing it discards that
    metadata.

    The fixture's DPLA record declares no ``sourceResource.language``,
    so only ``en`` is in the unwrap allowlist — any other language code,
    even when the inner happens to match, gets preserved."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    # The canonical title is "A Title". Wrap it in {{es|...}} as if a
    # Spanish-speaking editor confirmed the English value reads naturally
    # in Spanish too.
    wikitext = _build_full_wikitext(params).replace(
        "| title = A Title",
        "| title = {{es|A Title}}",
    )
    new_text, stripped = normalize(wikitext, params)
    assert "title" not in stripped
    assert "{{es|A Title}}" in new_text


def test_normalize_unwraps_language_tagged_canonical_english():
    """An editor who wrapped the canonical English string in ``{{en|...}}``
    didn't change the value — unwrap and treat as a match so the
    redundancy still gets stripped."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    wikitext = _build_full_wikitext(params).replace(
        "| description = A description",
        "| description = {{en|A description}}",
    )
    new_text, stripped = normalize(wikitext, params)
    assert "description" in stripped
    assert "{{en|A description}}" not in new_text


def test_normalize_preserves_url_with_edited_value():
    """If a flat-shape ``url`` was edited away from the canonical
    value, that row must survive. Other flat rows still strip — the
    per-row preservation is independent.

    Test fixture uses a non-URL-shaped edit token so CodeQL's
    ``py/incomplete-url-substring-sanitization`` rule doesn't pattern-
    match the ``substring in url`` shape as a security check."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    wikitext = _build_full_wikitext(params).replace(
        "| url = https://example.org/item/123",
        "| url = https://example.org/item/EDITED-BY-A-HUMAN-456",
    )
    new_text, stripped = normalize(wikitext, params)
    assert "url" not in stripped
    assert "EDITED-BY-A-HUMAN-456" in new_text
    # title still strips — preservation is row-by-row.
    assert "title" in stripped


def test_normalize_preserves_legacy_source_with_edited_subparam():
    """The dual-path strip on the legacy ``source = {{DPLA|...}}``
    row still respects per-inner-arg matching: if any sub-template
    arg is edited away from canonical, the whole row survives."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    wikitext = _build_legacy_wikitext(params).replace(
        "url=https://example.org/item/123",
        "url=https://example.org/item/EDITED-BY-A-HUMAN-456",
    )
    new_text, stripped = normalize(wikitext, params)
    assert "source" not in stripped
    assert "EDITED-BY-A-HUMAN-456" in new_text


def test_normalize_preserves_legacy_source_with_extra_param():
    """An editor-added sub-template arg the bot doesn't know about
    counts as a mismatch — strip would lose the editor's addition."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    wikitext = _build_legacy_wikitext(params).replace(
        "|local_id=local-123}}",
        "|local_id=local-123|note=editor added}}",
    )
    new_text, stripped = normalize(wikitext, params)
    assert "source" not in stripped
    assert "note=editor added" in new_text


def test_normalize_no_dpla_metadata_template_is_a_noop():
    """A page without a ``{{DPLA metadata}}`` invocation is not in scope
    (Goal 1 only applies to fresh DPLA-bot uploads). Returns input
    untouched."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    wikitext = "Some other content with no template\n"
    new_text, stripped = normalize(wikitext, params)
    assert stripped == []
    assert new_text == wikitext


def test_normalize_skips_flat_creator_row_when_dpla_has_no_creator():
    """When DPLA has no creator, the canonical creator value is empty —
    so an existing flat ``creator =`` row (added by an editor) is
    treated as a community contribution and preserved."""
    item = _minimal_item()
    item["sourceResource"]["creator"] = []  # no canonical creator
    params = dpla_metadata_params("abc123", item, _PROVIDER, _DATA_PROVIDER)
    wikitext = (
        "{{DPLA metadata\n| creator = Editor-added creator\n| title = A Title\n}}"
    )
    _, stripped = normalize(wikitext, params)
    assert "creator" not in stripped


def test_normalize_skips_legacy_creator_row_when_dpla_has_no_creator():
    """Same preservation rule for the legacy
    ``Other fields 1 = {{InFi|Creator|...}}`` row: empty canonical
    creator means any existing row is community-contributed."""
    item = _minimal_item()
    item["sourceResource"]["creator"] = []
    params = dpla_metadata_params("abc123", item, _PROVIDER, _DATA_PROVIDER)
    wikitext = (
        "{{DPLA metadata\n"
        "| Other fields 1 = {{InFi|Creator|Editor-added creator|id=fileinfotpl_aut}}\n"
        "| title = A Title\n"
        "}}"
    )
    _, stripped = normalize(wikitext, params)
    assert "Other fields 1" not in stripped


def test_normalize_preserves_legacy_creator_with_extra_param():
    """Symmetry with the legacy-source and legacy-institution strips:
    an editor-added arg inside the ``{{InFi|...}}`` sub-template (e.g.
    ``|note=editor added``) disqualifies the strip even when the
    Creator name and id args still match canonical. Stripping would
    silently lose the editor contribution."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    wikitext = (
        "{{DPLA metadata\n"
        "| Other fields 1 = {{InFi|Creator|A Creator|id=fileinfotpl_aut|note=editor added}}\n"
        "| title = A Title\n"
        "}}"
    )
    new_text, stripped = normalize(wikitext, params)
    assert "Other fields 1" not in stripped
    assert "note=editor added" in new_text
    # Row-by-row independence: the canonical title row still strips —
    # an extra-param violation on one row doesn't suppress the strip
    # on a different, clean row. Mirrors the assertion in
    # ``test_normalize_preserves_url_with_edited_value``.
    assert "title" in stripped


def test_normalize_returns_original_text_unchanged_when_nothing_strips():
    """No-op preserves the input verbatim — important for the caller's
    "don't save if nothing changed" guard."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    # Different DPLA ID in the wikitext → source sub-template won't match,
    # title differs, etc. — but the test point is the byte equality.
    wikitext = "{{DPLA metadata|title=Something else}}"
    new_text, stripped = normalize(wikitext, params)
    assert stripped == []
    assert new_text == wikitext


# ---------------------------------------------------------------------------
# Phase 2: per-item language unwrapping (Goal 3)
# ---------------------------------------------------------------------------


def _item_with_languages(*names):
    """An ``_minimal_item()`` but with ``sourceResource.language`` set to
    the supplied DPLA-style language entries (English-name only — the
    iso639_3 field is intentionally omitted, mirroring the real-world
    case where its value is unreliable per hub)."""
    item = _minimal_item()
    item["sourceResource"]["language"] = [{"name": n} for n in names]
    return item


def test_dpla_metadata_params_default_languages_is_en_only():
    """An item with no language field still gets ``en`` in the allowlist —
    canonical wikitext is English by convention."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    assert params["languages"] == {"en"}


def test_dpla_metadata_params_extracts_declared_iso_codes():
    """Each ``sourceResource.language[].name`` recognised by the map
    contributes its ISO 639-1 code to the per-item allowlist."""
    params = dpla_metadata_params(
        "abc123",
        _item_with_languages("Spanish", "French"),
        _PROVIDER,
        _DATA_PROVIDER,
    )
    assert params["languages"] == {"en", "es", "fr"}


def test_dpla_metadata_params_ignores_unmapped_language_names():
    """A language name not in the ISO map is silently dropped — its
    wrappers stay strip-ineligible, the safe default. ``en`` is still
    present from the seed."""
    params = dpla_metadata_params(
        "abc123",
        _item_with_languages("Klingon", "Spanish"),
        _PROVIDER,
        _DATA_PROVIDER,
    )
    assert params["languages"] == {"en", "es"}


def test_dpla_metadata_params_language_field_as_single_dict():
    """Defensive: some legacy DPLA mappers emit a single-dict
    ``language`` field rather than a list. The helper still extracts
    the code without an isinstance error."""
    item = _minimal_item()
    item["sourceResource"]["language"] = {"name": "Spanish"}
    params = dpla_metadata_params("abc123", item, _PROVIDER, _DATA_PROVIDER)
    assert params["languages"] == {"en", "es"}


def test_normalize_unwraps_dpla_declared_language():
    """For an item whose DPLA record declares Spanish,
    ``{{es|<canonical>}}`` around the canonical value is redundant
    metadata (Module:DPLA can render the value in Spanish from SDC) and
    is safely stripped."""
    item = _item_with_languages("Spanish")
    # Make the canonical title look Spanish so the wrapper-inner has a
    # plausible reason to be there; the strip works on equality, not on
    # any actual language detection.
    item["sourceResource"]["title"] = ["Una Imagen Histórica"]
    params = dpla_metadata_params("abc123", item, _PROVIDER, _DATA_PROVIDER)
    wikitext = _build_full_wikitext(params).replace(
        "| title = Una Imagen Histórica",
        "| title = {{es|Una Imagen Histórica}}",
    )
    _, stripped = normalize(wikitext, params)
    assert "title" in stripped


def test_normalize_does_not_unwrap_undeclared_language_even_when_inner_matches():
    """For an item whose DPLA record declares English only (or no
    language at all), ``{{es|<canonical-english>}}`` survives the strip
    even when the inner byte-matches the canonical value — the ``es``
    tag is editor-contributed translation metadata, not redundant."""
    item = _item_with_languages("English")
    params = dpla_metadata_params("abc123", item, _PROVIDER, _DATA_PROVIDER)
    assert "es" not in params["languages"]
    wikitext = _build_full_wikitext(params).replace(
        "| title = A Title",
        "| title = {{es|A Title}}",
    )
    _, stripped = normalize(wikitext, params)
    assert "title" not in stripped


def test_normalize_preserves_langswitch_even_with_single_named_param():
    """``{{LangSwitch|en=Foo}}`` is multilingual selector syntax. Always
    preserved, regardless of what ``en=...`` happens to contain.
    Defensive guard against any future change to
    ``_language_wrapper_code`` mistakenly treating LangSwitch as a wrapper."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    wikitext = _build_full_wikitext(params).replace(
        "| description = A description",
        "| description = {{LangSwitch|en=A description|es=Una descripción}}",
    )
    new_text, stripped = normalize(wikitext, params)
    assert "description" not in stripped
    assert "{{LangSwitch|en=A description|es=Una descripción}}" in new_text


# ---------------------------------------------------------------------------
# canonicalize — whitespace shape enforcement
# ---------------------------------------------------------------------------


def test_canonicalize_collapses_fully_stripped_template_to_single_line():
    """When every param has been stripped from the ``{{DPLA metadata}}``
    template, the canonical form is a single-line ``{{DPLA metadata}}``
    invocation — not the multi-line skeleton mwparserfromhell leaves
    behind after :func:`normalize`'s ``template.remove`` calls."""
    indented_after_strip = "== {{int:filedesc}} ==\n     {{ DPLA metadata\n        }}\n"
    result = canonicalize(indented_after_strip)
    assert "{{DPLA metadata}}" in result
    # The non-canonical residue is gone.
    assert "{{ DPLA metadata" not in result
    assert "        }}" not in result


def test_canonicalize_inserts_blank_line_between_heading_and_template():
    """The canonical shape has exactly one blank line between
    ``== {{int:filedesc}} ==`` and ``{{DPLA metadata``. Files written
    by the pre-#298 ``get_wiki_text`` (no blank line) get normalised."""
    no_blank_line = "== {{int:filedesc}} ==\n{{DPLA metadata\n| title = Foo\n}}"
    result = canonicalize(no_blank_line)
    assert "== {{int:filedesc}} ==\n\n{{DPLA metadata" in result


def test_canonicalize_left_justifies_indented_template():
    """A historical upload that emitted the template indented (the
    pre-#297 form, e.g. ``     {{ DPLA metadata``) gets left-justified
    in canonical form, no leading whitespace on the ``{{`` or on any
    ``| key = value`` line."""
    indented = (
        "== {{int:filedesc}} ==\n"
        "     {{ DPLA metadata\n"
        "        | title = Foo\n"
        "        | dpla_id = abc123\n"
        "     }}\n"
    )
    result = canonicalize(indented)
    for line in result.splitlines():
        # Lines that aren't the section header line shouldn't have
        # leading whitespace either.
        if line.startswith(" ") or line.startswith("\t"):
            raise AssertionError(f"line has leading whitespace: {line!r}")
    assert "{{DPLA metadata\n| title = Foo\n| dpla_id = abc123\n}}" in result


def test_canonicalize_drops_space_between_opening_braces_and_name():
    """``{{ DPLA metadata`` (with space) gets collapsed to
    ``{{DPLA metadata``. mwparserfromhell preserves the source-text
    whitespace inside template-name slots; the canonicalize pass
    overrides it with the no-space form."""
    spaced = "{{ DPLA metadata\n| title = Foo\n}}"
    result = canonicalize(spaced)
    assert "{{DPLA metadata" in result
    assert "{{ DPLA metadata" not in result


def test_canonicalize_no_op_on_already_canonical_text():
    """The canonical shape is idempotent — running it on text that's
    already canonical returns byte-identical output. Necessary for
    the ``normalize_page`` "save only on change" guard not to fire
    spuriously on files already in good shape."""
    canonical = (
        "== {{int:filedesc}} ==\n"
        "\n"
        "{{DPLA metadata\n"
        "| title = Foo\n"
        "| dpla_id = abc123\n"
        "}}"
    )
    assert canonicalize(canonical) == canonical


def test_canonicalize_returns_input_unchanged_when_no_dpla_metadata_template():
    """Files without ``{{DPLA metadata}}`` (legacy ``{{Artwork}}``,
    hand-written stubs, etc.) are out of scope for whitespace
    canonicalisation here — that's the legacy-migrate dispatcher's
    job upstream."""
    untouched = "Just some prose.\n{{Artwork|title=Foo}}\n[[Category:Foo]]"
    assert canonicalize(untouched) == untouched


# ---------------------------------------------------------------------------
# Tolerance widenings on `_value_matches` (this PR).
# * `date` key gets semantic-equivalence via parse_dpla_date.
# * display-string keys get casefold + leading/trailing-punctuation
#   trim as a byte-equality fallback.
# Identifier keys (hub, dpla_id, url, local_id, institution) do NOT
# get the casefold path — a case change there is a distinct value.
# ---------------------------------------------------------------------------


def _item_with_date(display_date: str, dpla_id: str = "abc") -> dict:
    return {
        "rights": "http://creativecommons.org/publicdomain/zero/1.0/",
        "isShownAt": "https://example.org/item/123",
        "sourceResource": {
            "creator": ["A Creator"],
            "title": ["A Title"],
            "description": ["A description"],
            "date": [{"displayDate": display_date}],
            "identifier": [dpla_id],
        },
    }


def test_normalize_strips_semantically_equal_date_override():
    """M105419621 repro: canonical ``1902-11-19`` DPLA-supplied date;
    editor added ``| date = 19 November 1902`` inside ``{{DPLA
    metadata}}``. The two parse to the same time+precision, so the
    override strips cleanly. Extends the existing byte-equality strip
    contract."""
    item = _item_with_date("1902-11-19")
    expected = dpla_metadata_params("abc", item, _PROVIDER, _DATA_PROVIDER)
    wikitext = (
        "== {{int:filedesc}} ==\n\n"
        "{{DPLA metadata\n| hub = Q72380652\n| date = 19 November 1902\n}}\n"
    )
    normalized, stripped = normalize(wikitext, expected)
    assert "date" in stripped, (
        f"expected `date` override to strip; stripped={stripped}. "
        f"If missing, either parse_dpla_date doesn't recognise "
        f"`19 November 1902` or _value_matches skipped the semantic-"
        f"equivalence path."
    )
    assert "19 November 1902" not in normalized


def test_normalize_preserves_date_override_that_disagrees_semantically():
    """A community edit that changes the date to a different year is
    real information — must NOT strip even under the widened
    tolerance. Semantic equivalence is strict about precision+time."""
    item = _item_with_date("1902-11-19")
    expected = dpla_metadata_params("abc", item, _PROVIDER, _DATA_PROVIDER)
    wikitext = "{{DPLA metadata\n| hub = Q72380652\n| date = 15 November 1903\n}}\n"
    _, stripped = normalize(wikitext, expected)
    assert "date" not in stripped


def test_normalize_strips_description_with_trailing_period():
    """M114630785 shape: canonical description ends with a period;
    editor's copy in wikitext drops the trailing period. Casefold-
    with-punctuation-trim strips the override cleanly."""
    item = _item_with_date("1902")
    item["sourceResource"]["description"] = [
        "A.D. Abbott, Hancock N.H. L.M. Stearns Collection."
    ]
    expected = dpla_metadata_params("abc", item, _PROVIDER, _DATA_PROVIDER)
    wikitext = (
        "{{DPLA metadata\n"
        "| description = A.D. Abbott, Hancock N.H. L.M. Stearns Collection\n"
        "}}\n"
    )
    _, stripped = normalize(wikitext, expected)
    assert "description" in stripped


def test_normalize_strips_title_with_wrapping_brackets():
    """M100761231 shape: canonical title is bracketed (supplied-title
    convention); editor's copy drops the brackets. Casefold-with-
    punctuation-trim on both sides folds to the same key and the
    override strips."""
    item = _item_with_date("1902")
    item["sourceResource"]["title"] = ["[Promissory note for Thomas Love]"]
    expected = dpla_metadata_params("abc", item, _PROVIDER, _DATA_PROVIDER)
    wikitext = "{{DPLA metadata\n| title = Promissory note for Thomas Love\n}}\n"
    _, stripped = normalize(wikitext, expected)
    assert "title" in stripped


def test_normalize_strips_title_that_only_differs_in_case():
    """Casefold widening: an editor who retyped the title in
    all-uppercase still gets stripped."""
    item = _item_with_date("1902")
    item["sourceResource"]["title"] = ["An Old Map"]
    expected = dpla_metadata_params("abc", item, _PROVIDER, _DATA_PROVIDER)
    wikitext = "{{DPLA metadata\n| title = AN OLD MAP\n}}\n"
    _, stripped = normalize(wikitext, expected)
    assert "title" in stripped


def test_normalize_preserves_url_with_case_change():
    """Identifier keys (url / dpla_id / hub / institution / local_id)
    are opaque tokens — case differences are real. The tolerance
    widening deliberately excludes them so an editor's URL variant
    survives instead of being silently overwritten."""
    item = _item_with_date("1902", dpla_id="abcdef1234567890")
    expected = dpla_metadata_params("abc", item, _PROVIDER, _DATA_PROVIDER)
    original_url = expected.get("url", "https://example.org/item/123")
    variant = original_url.upper()  # case flip
    if variant == original_url:
        # Some URL forms are already all-lower; guarantee a case delta.
        variant = original_url + "/EXTRA"
    wikitext = f"{{{{DPLA metadata\n| url = {variant}\n}}}}\n"
    _, stripped = normalize(wikitext, expected)
    assert "url" not in stripped, (
        f"URL is an identifier — case change must NOT strip; got "
        f"stripped={stripped}. If this asserts, the casefold widening "
        f"was accidentally applied to opaque tokens."
    )


def test_normalize_strips_description_with_case_and_trailing_punctuation():
    """Combined tolerance: an editor's copy of the description in
    different case AND missing the trailing period folds to the same
    normaliser key and strips."""
    item = _item_with_date("1902")
    item["sourceResource"]["description"] = ["Some Description Text."]
    expected = dpla_metadata_params("abc", item, _PROVIDER, _DATA_PROVIDER)
    wikitext = "{{DPLA metadata\n| description = SOME DESCRIPTION TEXT\n}}\n"
    _, stripped = normalize(wikitext, expected)
    assert "description" in stripped


def test_normalize_preserves_non_language_template_override():
    """Regression guard (CR flagged on PR #351): a non-language template
    like ``{{Cite|A description}}`` folds to ``cite|a description`` under
    ``casefold_for_compare`` (leading/trailing braces are punctuation
    the trim removes). Without a template-shape guard in the casefold
    fallback, that folded key can accidentally match a bare canonical
    scalar and strip the editor's citation template as if redundant.

    The strip must NOT fire — an editor's citation is deliberate
    structure, distinct from the plain-text value DPLA would supply."""
    item = _item_with_date("1902")
    item["sourceResource"]["description"] = ["A description"]
    expected = dpla_metadata_params("abc", item, _PROVIDER, _DATA_PROVIDER)
    wikitext = "{{DPLA metadata\n| description = {{Cite|A description}}\n}}\n"
    _, stripped = normalize(wikitext, expected)
    assert "description" not in stripped, (
        f"expected non-language template {{Cite|...}} to be preserved; "
        f"stripped={stripped}. Regression: the casefold fallback is "
        f"accepting template-wrapped values whose stripped inner text "
        f"folds to the canonical scalar."
    )


def test_normalize_preserves_multi_template_override():
    """Related: a wikitext value carrying multiple templates (e.g.
    ``{{en|foo}}{{ja|bar}}``) is complex editor structure. Only the
    single-language-wrapper unwrap path is allowed; anything else
    returns False before the casefold fallback runs."""
    item = _item_with_date("1902")
    item["sourceResource"]["title"] = ["Two languages"]
    expected = dpla_metadata_params("abc", item, _PROVIDER, _DATA_PROVIDER)
    wikitext = (
        "{{DPLA metadata\n| title = {{en|Two languages}}{{ja|Some other text}}\n}}\n"
    )
    _, stripped = normalize(wikitext, expected)
    assert "title" not in stripped
