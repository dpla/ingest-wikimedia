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
from ingest_wikimedia.wikitext_normalize import normalize


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
        "source",
        "institution",
    }


def test_dpla_metadata_params_scalar_values_match_extract_strings():
    """The scalar-typed params (title/description/date) come straight
    from the source-resource lists via ``extract_strings``."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    assert params["title"] == "A Title"
    assert params["description"] == "A description"
    assert params["date"] == "1900"


def test_dpla_metadata_params_permission_is_template_wrapped():
    """``permission`` is special: the rendered wikitext value is itself
    a ``{{<template>}}`` invocation, so the canonical value must include
    those braces verbatim for the comparator to match against what the
    wikitext stores."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    # CC-Zero rights → cc-zero permission template.
    assert params["permission"] == "{{cc-zero}}"


def test_dpla_metadata_params_source_subtemplate_carries_positional_and_named():
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    source = params["source"]
    assert source["name"] == "DPLA"
    # Positional 1 is the data-provider Q-ID, mirroring the wikitext form
    # `{{DPLA|<data_provider_qid>|hub=...|url=...|dpla_id=...|local_id=...}}`.
    assert source["params"]["1"] == "Q2"
    assert source["params"]["hub"] == "Q1"
    assert source["params"]["dpla_id"] == "abc123"
    assert source["params"]["local_id"] == "local-123"
    assert source["params"]["url"] == "https://example.org/item/123"


def test_dpla_metadata_params_creator_subtemplate_uses_infi_shape():
    """``creator`` is emitted as ``{{InFi|Creator|<value>|id=fileinfotpl_aut}}``
    on the wikitext side, so the canonical-params shape must mirror that
    positional+named layout."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    creator = params["creator"]
    assert creator["name"] == "InFi"
    assert creator["params"]["1"] == "Creator"
    assert creator["params"]["2"] == "A Creator"
    assert creator["params"]["id"] == "fileinfotpl_aut"


def test_dpla_metadata_params_drives_get_wiki_text_unchanged():
    """Regression guard against drift: the two-flow problem is exactly
    that ``get_wiki_text`` and the normalizer's expected-params source
    must not diverge. Pin the rendered output's key fragments to confirm
    refactoring the helper didn't silently change the rendered template."""
    rendered = get_wiki_text("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    # Every canonical param value appears in the rendered wikitext.
    for expected in ("A Title", "A description", "1900", "{{cc-zero}}", "Q2", "Q1"):
        assert expected in rendered


# ---------------------------------------------------------------------------
# normalize — the strip-redundant-params pass
# ---------------------------------------------------------------------------


def _build_full_wikitext(params: dict) -> str:
    """Produce a `{{DPLA metadata}}` wikitext block carrying every param
    at its canonical value — i.e. the worst-case "fresh DPLA-bot upload
    with nothing user-touched" case where every param is strippable."""
    creator = params["creator"]["params"]["2"]
    source_p = params["source"]["params"]
    inst_p = params["institution"]["params"]
    return (
        "== {{int:filedesc}} ==\n"
        "{{DPLA metadata\n"
        f"| Other fields 1 = {{{{InFi|Creator|{creator}|id=fileinfotpl_aut}}}}\n"
        f"| title = {params['title']}\n"
        f"| description = {params['description']}\n"
        f"| date = {params['date']}\n"
        f"| permission = {params['permission']}\n"
        f"| source = {{{{DPLA|{source_p['1']}|hub={source_p['hub']}"
        f"|url={source_p['url']}|dpla_id={source_p['dpla_id']}"
        f"|local_id={source_p['local_id']}}}}}\n"
        f"| Institution = {{{{Institution|wikidata={inst_p['wikidata']}}}}}\n"
        "}}\n"
    )


def test_normalize_strips_every_param_on_a_pristine_dpla_bot_upload():
    """The all-match case: every param is canonical, every param goes."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    wikitext = _build_full_wikitext(params)
    new_text, stripped = normalize(wikitext, params)
    assert set(stripped) == {
        "title",
        "description",
        "date",
        "permission",
        "source",
        "institution",
        "other fields 1",
    }
    # Every value is gone from the new wikitext.
    for absent in ("A Title", "A description", "1900", "Q1", "Q2", "abc123"):
        assert absent not in new_text


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

    Phase 1 explicitly only unwraps ``{{en|...}}`` — every other language
    code, even when the inner happens to match, gets preserved."""
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


def test_normalize_preserves_source_with_edited_subparam():
    """If the source sub-template's ``url`` was edited away from the
    canonical value, the whole source param must survive — partial
    matches don't count.

    Test fixture uses a non-URL-shaped edit token so CodeQL's
    ``py/incomplete-url-substring-sanitization`` rule doesn't pattern-
    match the ``substring in url`` shape as a security check.
    """
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    wikitext = _build_full_wikitext(params).replace(
        "url=https://example.org/item/123",
        "url=https://example.org/item/EDITED-BY-A-HUMAN-456",
    )
    new_text, stripped = normalize(wikitext, params)
    assert "source" not in stripped
    assert "EDITED-BY-A-HUMAN-456" in new_text


def test_normalize_preserves_source_with_extra_param():
    """An editor-added sub-template arg the bot doesn't know about
    counts as a mismatch — strip would lose the editor's addition."""
    params = dpla_metadata_params("abc123", _minimal_item(), _PROVIDER, _DATA_PROVIDER)
    wikitext = _build_full_wikitext(params).replace(
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


def test_normalize_skips_creator_row_when_dpla_has_no_creator():
    """When DPLA has no creator, the canonical creator value is empty —
    so an existing ``Other fields 1`` row (added by an editor) is
    treated as a community contribution and preserved."""
    item = _minimal_item()
    item["sourceResource"]["creator"] = []  # no canonical creator
    params = dpla_metadata_params("abc123", item, _PROVIDER, _DATA_PROVIDER)
    wikitext = (
        "{{DPLA metadata\n"
        "| Other fields 1 = {{InFi|Creator|Editor-added creator|id=fileinfotpl_aut}}\n"
        "| title = A Title\n"
        "}}"
    )
    _, stripped = normalize(wikitext, params)
    assert "other fields 1" not in stripped


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
