"""Tests for the Phase 3a legacy-Artwork migration foundation.

Covers the pure-logic layer: parsing legacy templates, walking
revision provenance, classifying DPLA-bot vs community edits, and
constructing SDC import claims with the P887/P4656 reference shape.

Phase 3b's pywikibot integration (calling wbeditentity, saving
wikitext) is out of scope here.
"""

from __future__ import annotations

import pytest

from ingest_wikimedia.legacy_artwork import (
    ARTWORK_PARAM_TO_CANONICAL_KEY,
    DPLA_BOT_ACCOUNTS,
    PID_BASED_ON_HEURISTIC,
    PID_WIKIMEDIA_IMPORT_URL,
    QID_INFERRED_FROM_WIKITEXT,
    MigrationPlan,
    RevisionSnapshot,
    build_legacy_import_claims,
    classify_param_provenance,
    find_legacy_template,
    format_legacy_import_claim,
    parse_artwork_params,
    plan_migration,
    trace_param_provenance,
)


# ---------------------------------------------------------------------------
# find_legacy_template / parse_artwork_params
# ---------------------------------------------------------------------------


def test_find_legacy_template_returns_artwork_node():
    wikitext = (
        "== {{int:filedesc}} ==\n"
        "{{Artwork|title=A Title|author=A Creator}}\n"
        "{{PD-USGov}}"
    )
    tpl = find_legacy_template(wikitext)
    assert tpl is not None
    assert str(tpl.name).strip().casefold() == "artwork"


def test_find_legacy_template_falls_back_to_information():
    """When no Artwork is present, the helper still finds an
    ``{{Information}}`` block (the other shape DPLA bots emitted in the
    past)."""
    wikitext = "{{Information|description=foo|date=1900}}"
    tpl = find_legacy_template(wikitext)
    assert tpl is not None
    assert str(tpl.name).strip().casefold() == "information"


def test_find_legacy_template_returns_none_on_dpla_metadata_only():
    """A page already on the new template form is not in scope for the
    legacy migrator."""
    wikitext = "{{DPLA metadata|title=Already migrated}}"
    assert find_legacy_template(wikitext) is None


def test_parse_artwork_params_extracts_known_params():
    wikitext = (
        "{{Artwork\n"
        "| title = A Title\n"
        "| description = A description\n"
        "| date = 1900\n"
        "| author = An Author\n"
        "}}"
    )
    params = parse_artwork_params(wikitext)
    assert params == {
        "title": "A Title",
        "description": "A description",
        "date": "1900",
        "creator": "An Author",
    }


def test_parse_artwork_params_aliases_author_artist_creator_to_creator():
    """``{{Artwork}}`` editors use any of these three names. All three
    map to the canonical ``creator`` key."""
    for label in ("author", "artist", "creator"):
        params = parse_artwork_params(f"{{{{Artwork|{label}=Smith}}}}")
        assert params == {"creator": "Smith"}


def test_parse_artwork_params_drops_unknown_params():
    """A param name without a canonical-key mapping is silently
    dropped — the migrator leaves it in the wikitext untouched."""
    wikitext = "{{Artwork|title=A|other versions=See foo.jpg|something=else}}"
    params = parse_artwork_params(wikitext)
    assert "title" in params
    assert "other versions" not in params
    assert "something" not in params


def test_parse_artwork_params_returns_empty_when_no_template():
    assert parse_artwork_params("Just some text") == {}


def test_parse_artwork_params_drops_empty_values():
    """Editors sometimes leave a param key with no value
    (``| date = ``). That isn't useful provenance to import — drop."""
    params = parse_artwork_params("{{Artwork|title=A|date=}}")
    assert params == {"title": "A"}


# ---------------------------------------------------------------------------
# trace_param_provenance — revision-history walker
# ---------------------------------------------------------------------------


def _make_revs(*entries: tuple[int, str, str]) -> list[RevisionSnapshot]:
    """Helper: build a list of RevisionSnapshot from (revid, user, text)
    tuples in chronological order."""
    return [RevisionSnapshot(revid=r, user=u, text=t) for r, u, t in entries]


def test_trace_provenance_attributes_each_param_to_last_setting_revision():
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|title=Original|date=1900}}"),
        (2, "Editor1", "{{Artwork|title=Original|date=1900|description=A note}}"),
        (3, "Editor2", "{{Artwork|title=Better|date=1900|description=A note}}"),
    )
    provenance = trace_param_provenance(revs)
    assert provenance == {
        "title": "Editor2",  # last touched the title
        "date": "DPLA_bot",  # never touched after initial set
        "description": "Editor1",  # added then unchanged
    }


def test_trace_provenance_returns_empty_for_no_revisions():
    assert trace_param_provenance([]) == {}


def test_trace_provenance_returns_empty_when_latest_has_no_artwork():
    """If the latest revision dropped the Artwork template, there's
    nothing to attribute. (Caller will skip the file.)"""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|title=Original}}"),
        (2, "Cleanup", "Some text without any template"),
    )
    assert trace_param_provenance(revs) == {}


def test_trace_provenance_drops_reverted_intermediate_values():
    """A param that was edited and then reverted back to the original
    DPLA value still attributes to the DPLA bot — not to the editor
    who briefly changed it. The end-state attribution is what matters."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|title=Original}}"),
        (2, "Editor1", "{{Artwork|title=Changed}}"),
        (3, "Editor2", "{{Artwork|title=Original}}"),
    )
    provenance = trace_param_provenance(revs)
    # Editor2's revert puts the title back to "Original" — but it was
    # Editor2 who set the current value, not DPLA_bot. The walker
    # records the *most recent* setter of the current value.
    assert provenance == {"title": "Editor2"}


def test_trace_provenance_delete_then_readd_attributes_to_restorer():
    """Regression: a param that DPLA_bot originally set, then a
    community editor deleted, then a different community editor
    re-added with the same string value, must attribute to the
    *re-adder* — not stay attributed to DPLA_bot.

    Pre-fix, ``prior_seen`` was never cleared on deletion, so the
    re-add looked like "value unchanged" and the provenance stayed
    on DPLA_bot. A subsequent canonical-DPLA value change would have
    then classified the re-added value as DPLA-originated and
    stripped it — losing the community restoration.
    """
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|title=Original}}"),
        (2, "Deleter", "{{Artwork|}}"),  # title param deleted entirely
        (3, "Restorer", "{{Artwork|title=Original}}"),  # community re-add
    )
    provenance = trace_param_provenance(revs)
    assert provenance == {"title": "Restorer"}


def test_plan_migration_imports_restored_community_value_when_canonical_differs():
    """End-to-end of the delete-readd fix: when the canonical DPLA
    title has since changed, the re-added community value is now an
    actual community contribution that must be imported, not
    discarded as DPLA-originated."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|title=Original}}"),
        (2, "Deleter", "{{Artwork|}}"),
        (3, "Restorer", "{{Artwork|title=Original}}"),
    )
    # Canonical title has since been updated by DPLA — so "Original"
    # is now a community-only value.
    plan = plan_migration(
        "File:Foo.jpg",
        revs,
        _canonical_params(title="Updated Canonical Title"),
    )
    assert plan is not None
    assert plan.community_imports == {"title": "Original"}


def test_trace_provenance_sorts_by_revid_not_input_order():
    """Out-of-order input still produces a chronologically correct
    walk — defensive against any caller that doesn't pre-sort."""
    revs = _make_revs(
        (3, "Editor2", "{{Artwork|title=Final}}"),
        (1, "DPLA_bot", "{{Artwork|title=Original}}"),
        (2, "Editor1", "{{Artwork|title=Intermediate}}"),
    )
    provenance = trace_param_provenance(revs)
    assert provenance == {"title": "Editor2"}


# ---------------------------------------------------------------------------
# classify_param_provenance
# ---------------------------------------------------------------------------


def test_classify_uses_dpla_bot_allowlist():
    provenance = {
        "title": "DPLA_bot",
        "description": "Editor1",
        "creator": "US National Archives bot",
    }
    classified = classify_param_provenance(provenance)
    assert classified == {
        "title": "dpla",
        "description": "community",
        "creator": "dpla",  # US National Archives bot is on the allowlist
    }


def test_classify_is_case_insensitive_against_allowlist():
    provenance = {"title": "dpla_BOT"}
    assert classify_param_provenance(provenance) == {"title": "dpla"}


def test_classify_respects_explicit_bot_accounts_argument():
    """Tests can pass a custom allowlist for hypothetical-future or
    deprecated-historical bot accounts without mutating module state."""
    provenance = {"title": "AncientBot"}
    classified = classify_param_provenance(
        provenance, bot_accounts=frozenset({"AncientBot"})
    )
    assert classified == {"title": "dpla"}


def test_dpla_bot_allowlist_contains_known_accounts():
    """Pin the known accounts so a typo'd rename can't silently lose
    classification coverage for the long-tenure files those bots
    uploaded."""
    assert "DPLA_bot" in DPLA_BOT_ACCOUNTS
    assert "US National Archives bot" in DPLA_BOT_ACCOUNTS


# ---------------------------------------------------------------------------
# plan_migration
# ---------------------------------------------------------------------------


def _canonical_params(**overrides) -> dict:
    """Minimal canonical-params dict matching what
    ``dpla_metadata_params`` produces for our test fixtures."""
    base = {
        "title": "A Title",
        "description": "A description",
        "date": "1900",
        "permission": "{{Cc-zero}}",
        "creator": {
            "name": "InFi",
            "params": {"1": "Creator", "2": "A Creator", "id": "fileinfotpl_aut"},
        },
        "source": {"name": "DPLA", "params": {}},
        "institution": {"name": "Institution", "params": {}},
        "languages": frozenset({"en"}),
    }
    base.update(overrides)
    return base


def test_plan_migration_returns_none_with_no_revisions():
    assert plan_migration("File:Foo.jpg", [], _canonical_params()) is None


def test_plan_migration_returns_none_with_no_artwork_in_latest():
    revs = _make_revs((1, "Cleanup", "Just text"))
    assert plan_migration("File:Foo.jpg", revs, _canonical_params()) is None


def test_plan_migration_classifies_bot_value_as_dpla_originated():
    """When DPLA_bot set every param and no one else has touched the
    file, every param is dpla-originated — no community imports."""
    revs = _make_revs(
        (
            1,
            "DPLA_bot",
            "{{Artwork|title=A Title|description=A description|date=1900}}",
        )
    )
    plan = plan_migration("File:Foo.jpg", revs, _canonical_params())
    assert plan is not None
    assert plan.community_imports == {}
    assert plan.dpla_originated_params == {
        "title": "A Title",
        "description": "A description",
        "date": "1900",
    }


def test_plan_migration_imports_community_value_that_differs_from_canonical():
    """An editor changed the title to something different from DPLA's
    canonical title. That value must go into ``community_imports``
    (and Phase 3b will record it as an SDC statement with P887/P4656)."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|title=A Title}}"),
        (2, "Editor1", "{{Artwork|title=A Better Title}}"),
    )
    plan = plan_migration("File:Foo.jpg", revs, _canonical_params())
    assert plan is not None
    assert plan.community_imports == {"title": "A Better Title"}
    assert "title" not in plan.dpla_originated_params


def test_plan_migration_skips_community_value_that_matches_canonical():
    """An editor restated DPLA's value verbatim (maybe by re-saving
    after a typo fix-and-revert). That's redundant — no import,
    treated as dpla-originated for the migration purposes."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|title=A Title}}"),
        (2, "Editor1", "{{Artwork|title=A Title}}"),
    )
    plan = plan_migration("File:Foo.jpg", revs, _canonical_params())
    assert plan is not None
    assert plan.community_imports == {}
    assert plan.dpla_originated_params["title"] == "A Title"


def test_plan_migration_records_source_permalink():
    """The permalink stamped onto every imported claim's P4656 ref
    points at the page's latest revision id at plan time, so a
    reviewer can trace any imported statement back to its wikitext
    source. Spaces become underscores (Commons URL convention) and
    the ``:`` namespace separator is percent-encoded (``%3A``) per
    ``urllib.parse.urlencode`` semantics — that's the same form the
    Wikipedia/Commons API responds with, so it's a recognisable URL."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|title=A Title}}"),
        (42, "Editor1", "{{Artwork|title=A Better Title}}"),
    )
    plan = plan_migration("File:Test File.jpg", revs, _canonical_params())
    assert plan is not None
    assert (
        plan.source_permalink
        == "https://commons.wikimedia.org/w/index.php?title=File%3ATest_File.jpg&oldid=42"
    )


def test_plan_migration_permalink_encodes_ampersand_in_filename():
    """Regression: filenames containing ``&`` are legal on Commons
    (``File:Foo & Bar.jpg``). Hand-rolled spaces-only encoding put
    the raw ``&`` into the URL, causing the query-string parser to
    split there — ``title=File:Foo_`` would have been all the
    permalink resolved to. ``urlencode`` percent-encodes it as
    ``%26`` so the title round-trips through query parsing."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|title=A Title}}"),
        (42, "Editor1", "{{Artwork|title=A Better Title}}"),
    )
    plan = plan_migration("File:Foo & Bar.jpg", revs, _canonical_params())
    assert plan is not None
    assert "%26" in plan.source_permalink  # the ``&`` is encoded
    # Sanity-check end-to-end query parsing matches what we expect.
    from urllib.parse import parse_qs, urlsplit

    parsed_qs = parse_qs(urlsplit(plan.source_permalink).query)
    assert parsed_qs["title"] == ["File:Foo_&_Bar.jpg"]
    assert parsed_qs["oldid"] == ["42"]


# ---------------------------------------------------------------------------
# format_legacy_import_claim / build_legacy_import_claims
# ---------------------------------------------------------------------------


def test_format_claim_for_title_emits_monolingualtext_with_p887_p4656_refs():
    claim = format_legacy_import_claim(
        "title",
        "A Better Title",
        "https://commons.wikimedia.org/w/index.php?title=Foo.jpg&oldid=42",
    )
    assert claim is not None
    assert claim["type"] == "statement"
    assert claim["rank"] == "normal"
    assert claim["mainsnak"]["property"] == "P1476"
    assert claim["mainsnak"]["datatype"] == "monolingualtext"
    assert claim["mainsnak"]["datavalue"] == {
        "type": "monolingualtext",
        "value": {"text": "A Better Title", "language": "en"},
    }
    # Reference block has exactly the two snaks (P887 + P4656), no
    # standard DPLA refs (P459/P973/P813) — the import isn't
    # DPLA-sourced and must not be misrepresented as such.
    refs = claim["references"]
    assert len(refs) == 1
    assert set(refs[0]["snaks"]) == {PID_BASED_ON_HEURISTIC, PID_WIKIMEDIA_IMPORT_URL}
    p887_snak = refs[0]["snaks"][PID_BASED_ON_HEURISTIC][0]
    assert p887_snak["datavalue"]["value"]["id"] == QID_INFERRED_FROM_WIKITEXT
    p4656_snak = refs[0]["snaks"][PID_WIKIMEDIA_IMPORT_URL][0]
    assert p4656_snak["datatype"] == "url"
    assert "oldid=42" in p4656_snak["datavalue"]["value"]


def test_format_claim_for_description_uses_p10358():
    claim = format_legacy_import_claim(
        "description", "A desc", "https://example/permalink"
    )
    assert claim is not None
    assert claim["mainsnak"]["property"] == "P10358"
    assert claim["mainsnak"]["datatype"] == "monolingualtext"


def test_format_claim_for_creator_uses_p2093_string():
    """``P2093`` (creator — stated as) is the string form, used when
    we have no Wikidata item match. Editors can later promote to
    P170 manually."""
    claim = format_legacy_import_claim(
        "creator", "Jane Doe", "https://example/permalink"
    )
    assert claim is not None
    assert claim["mainsnak"]["property"] == "P2093"
    assert claim["mainsnak"]["datatype"] == "string"


def test_format_claim_for_date_emits_phase3a_placeholder():
    """Phase 3a doesn't parse dates — Phase 3b will pass them through
    the existing ``parse_dpla_date`` helper. The placeholder lets a
    caller detect "this needs post-processing" without crashing."""
    claim = format_legacy_import_claim("date", "1900", "https://example/permalink")
    assert claim is not None
    assert claim.get("_phase3a_pending_date_parse") == "1900"
    assert claim["_property"] == "P571"


def test_format_claim_for_unmapped_key_returns_none():
    """Source / institution / permission are out of scope for Phase 3a's
    narrow scalar mapping — caller treats None as "leave in wikitext."""
    assert format_legacy_import_claim("source", "anything", "https://x") is None
    assert format_legacy_import_claim("institution", "anything", "https://x") is None
    assert format_legacy_import_claim("permission", "anything", "https://x") is None


def test_build_legacy_import_claims_iterates_community_imports():
    """End-to-end: a plan with two community imports produces two
    claim dicts, skipping any keys outside the import mapping."""
    plan = MigrationPlan(
        source_permalink="https://example/permalink",
        community_imports={
            "title": "Community Title",
            "creator": "Community Creator",
            "source": "should be skipped",
        },
    )
    claims = build_legacy_import_claims(plan)
    assert len(claims) == 2
    props = {c["mainsnak"]["property"] for c in claims}
    assert props == {"P1476", "P2093"}


# ---------------------------------------------------------------------------
# Cross-cutting: ARTWORK_PARAM_TO_CANONICAL_KEY sanity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "alias, canonical",
    [
        ("title", "title"),
        ("Title", "title"),
        ("AUTHOR", "creator"),
        ("artist", "creator"),
        ("creator", "creator"),
        ("description", "description"),
        ("date", "date"),
    ],
)
def test_artwork_param_aliases_normalize_via_casefolded_lookup(alias, canonical):
    """The mapping is consulted via case-folded keys — confirm every
    advertised alias resolves to its canonical key regardless of
    source casing."""
    assert ARTWORK_PARAM_TO_CANONICAL_KEY.get(alias.casefold()) == canonical


# ---------------------------------------------------------------------------
# Phase 3b: integration layer (date materialisation, idempotency,
# wikitext-rewrite, end-to-end executor)
# ---------------------------------------------------------------------------


from unittest.mock import MagicMock  # noqa: E402

from ingest_wikimedia.legacy_artwork import (  # noqa: E402
    LEGACY_MIGRATION_EDIT_SUMMARY,
    MigrationResult,
    entity_was_already_migrated,
    fetch_revision_snapshots,
    materialize_import_claims,
    materialize_pending_date_claim,
    migrate_legacy_file,
    post_legacy_import_claims,
    render_migrated_wikitext,
)


# --- materialize_pending_date_claim ---------------------------------------


def test_materialize_pending_date_claim_parses_year_to_p571_value_typed():
    """The placeholder expands to a value-typed P571 statement with the
    structured wikibase time datavalue and the original string
    preserved as a P1932 (stated as) qualifier."""
    placeholder = {
        "_phase3a_pending_date_parse": "1900",
        "_property": "P571",
        "_permalink": "https://example/permalink",
        "type": "statement",
    }
    claim = materialize_pending_date_claim(placeholder)
    assert claim is not None
    assert claim["mainsnak"]["snaktype"] == "value"
    assert claim["mainsnak"]["property"] == "P571"
    assert claim["mainsnak"]["datatype"] == "time"
    assert claim["mainsnak"]["datavalue"]["type"] == "time"
    # P1932 preserves the verbatim source string.
    assert claim["qualifiers"]["P1932"][0]["datavalue"]["value"] == "1900"
    # Reference uses P887/P4656, NOT DPLA-standard refs.
    refs = claim["references"]
    assert len(refs) == 1
    assert set(refs[0]["snaks"]) == {"P887", "P4656"}


def test_materialize_pending_date_claim_circa_stamps_p1480():
    """Approximate dates ("ca. 1900") get the P1480 (sourcing
    circumstances → Q5727902 circa) qualifier alongside P1932."""
    placeholder = {
        "_phase3a_pending_date_parse": "ca. 1900",
        "_property": "P571",
        "_permalink": "https://example/permalink",
    }
    claim = materialize_pending_date_claim(placeholder)
    assert claim is not None
    assert "P1480" in claim["qualifiers"]
    assert claim["qualifiers"]["P1480"][0]["datavalue"]["value"]["id"] == "Q5727902"


def test_materialize_pending_date_claim_unparseable_falls_back_to_somevalue():
    """When parse_dpla_date can't commit, the mainsnak goes ``somevalue``
    so the statement still asserts an inception exists, with the
    original string preserved on P1932."""
    placeholder = {
        "_phase3a_pending_date_parse": "sometime in the 1800s maybe",
        "_property": "P571",
        "_permalink": "https://example/permalink",
    }
    claim = materialize_pending_date_claim(placeholder)
    assert claim is not None
    assert claim["mainsnak"]["snaktype"] == "somevalue"
    assert "datavalue" not in claim["mainsnak"]
    assert claim["qualifiers"]["P1932"][0]["datavalue"]["value"] == (
        "sometime in the 1800s maybe"
    )


def test_materialize_import_claims_passes_through_non_date_claims():
    """A title claim doesn't carry the placeholder marker — pass through."""
    title_claim = {"type": "statement", "mainsnak": {"property": "P1476"}}
    out = materialize_import_claims([title_claim])
    assert out == [title_claim]


def test_materialize_import_claims_swaps_only_date_placeholder():
    """Mixed list — one pass-through, one materialised. Order preserved."""
    title_claim = {"type": "statement", "mainsnak": {"property": "P1476"}}
    date_placeholder = {
        "type": "statement",
        "_phase3a_pending_date_parse": "1900",
        "_property": "P571",
        "_permalink": "https://example/permalink",
    }
    out = materialize_import_claims([title_claim, date_placeholder])
    assert len(out) == 2
    assert out[0] == title_claim
    assert out[1]["mainsnak"]["property"] == "P571"
    assert "_phase3a_pending_date_parse" not in out[1]


# --- entity_was_already_migrated ------------------------------------------


def _entity_with_legacy_import(prop: str) -> dict:
    return {
        "statements": {
            prop: [
                {
                    "mainsnak": {"property": prop},
                    "references": [
                        {
                            "snaks": {
                                "P887": [
                                    {
                                        "datavalue": {
                                            "value": {
                                                "entity-type": "item",
                                                "id": "Q131783016",
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    ],
                }
            ]
        }
    }


def test_entity_was_already_migrated_detects_p887_q131783016_ref():
    entity = _entity_with_legacy_import("P1476")
    assert entity_was_already_migrated(entity) is True


def test_entity_was_already_migrated_false_for_empty_entity():
    assert entity_was_already_migrated({}) is False
    assert entity_was_already_migrated({"statements": {}}) is False


def test_entity_was_already_migrated_false_for_unrelated_p887_ref():
    """A P887 ref pointing at a DIFFERENT Wikidata item doesn't count —
    only the Q131783016 signature triggers the bail-out."""
    entity = _entity_with_legacy_import("P1476")
    entity["statements"]["P1476"][0]["references"][0]["snaks"]["P887"][0]["datavalue"][
        "value"
    ]["id"] = "Q99999999"
    assert entity_was_already_migrated(entity) is False


def test_entity_was_already_migrated_accepts_claims_key_legacy_shape():
    """Some pywikibot helpers return MediaInfo entities with statements
    under ``claims`` (the older key) rather than ``statements``. Read
    either."""
    entity = _entity_with_legacy_import("P1476")
    entity["claims"] = entity.pop("statements")
    assert entity_was_already_migrated(entity) is True


# --- render_migrated_wikitext ---------------------------------------------


def test_render_migrated_wikitext_swaps_artwork_for_dpla_metadata():
    original = (
        "== {{int:filedesc}} ==\n{{Artwork|title=Old}}\n{{PD-USGov}}\n[[Category:Foo]]"
    )
    new_block = "{{DPLA metadata|title=New}}"
    result = render_migrated_wikitext(original, new_block)
    assert "{{Artwork" not in result
    assert "{{DPLA metadata|title=New}}" in result
    # Page-level metadata survives.
    assert "{{PD-USGov}}" in result
    assert "[[Category:Foo]]" in result


def test_render_migrated_wikitext_noop_when_no_legacy_template():
    """Defensive: a page already migrated (or never legacy) is left
    byte-identical so the caller's wikitext-changed comparison
    behaves correctly."""
    original = "{{DPLA metadata|title=Already there}}\n"
    result = render_migrated_wikitext(original, "{{DPLA metadata|title=Other}}")
    assert result == original


def test_render_migrated_wikitext_swaps_information_template_too():
    original = "{{Information|description=foo}}"
    result = render_migrated_wikitext(original, "{{DPLA metadata|description=bar}}")
    assert "{{Information" not in result
    assert "{{DPLA metadata|description=bar}}" in result


def test_render_migrated_wikitext_does_not_duplicate_section_heading():
    """Regression for the bug where ``new_template_block`` carrying a
    leading ``== {{int:filedesc}} ==`` heading (as produced by
    :func:`get_wiki_text`) was substituted verbatim in place of the
    legacy ``{{Artwork}}`` template, ending up with two consecutive
    headings on the page (the original above + the new one inline).

    The fix extracts just the ``{{DPLA metadata ...}}`` template from
    ``new_template_block`` before substituting, so the section heading
    that was already in the page above the legacy template stays put
    and isn't duplicated below it.
    """
    original = (
        "== {{int:filedesc}} ==\n"
        "     {{ Artwork\n"
        "        | title = Old\n"
        "        | source = {{DPLA|Q1|hub=Q2|dpla_id=abc|local_id=loc|url=http://x}}\n"
        "     }}\n"
        "{{PD-USGov}}\n"
        "[[Category:Foo]]"
    )
    # Real ``get_wiki_text`` output — full upload form including heading.
    new_block = (
        "== {{int:filedesc}} ==\n\n{{DPLA metadata\n| title = New\n| dpla_id = abc\n}}"
    )
    result = render_migrated_wikitext(original, new_block)
    # Only one section heading in the result.
    assert result.count("== {{int:filedesc}} ==") == 1, (
        f"section heading duplicated:\n{result}"
    )
    # The new template invocation landed where the Artwork block was.
    assert "{{DPLA metadata" in result
    assert "{{Artwork" not in result
    # Page-level content survives.
    assert "{{PD-USGov}}" in result
    assert "[[Category:Foo]]" in result


def test_render_migrated_wikitext_accepts_template_only_block_for_back_compat():
    """The previous test asserts the new caller contract — but the
    extraction step must also be a no-op on the old caller contract
    (a bare ``{{DPLA metadata|...}}`` invocation with no heading
    wrapper). Locks in the back-compat path so an existing in-process
    caller that pre-strips the heading still works."""
    original = "== {{int:filedesc}} ==\n{{Artwork|title=Old}}\n[[Category:Foo]]"
    new_block = "{{DPLA metadata|title=New}}"
    result = render_migrated_wikitext(original, new_block)
    assert result.count("== {{int:filedesc}} ==") == 1
    assert "{{DPLA metadata|title=New}}" in result
    assert "{{Artwork" not in result


# --- post_legacy_import_claims --------------------------------------------


def test_post_legacy_import_claims_submits_atomic_wbeditentity():
    """One ``wbeditentity`` POST with all claims bundled. The request
    action, id, csrf token, and JSON-serialised ``data`` payload are
    the contract."""
    import json as _json

    site = MagicMock()
    site.tokens = {"csrf": "CSRFTOKEN"}
    request = MagicMock()
    site.simple_request.return_value = request

    claims = [{"type": "statement", "mainsnak": {"property": "P1476"}}]
    post_legacy_import_claims("M42", claims, site)

    site.simple_request.assert_called_once()
    kwargs = site.simple_request.call_args.kwargs
    assert kwargs["action"] == "wbeditentity"
    assert kwargs["id"] == "M42"
    assert kwargs["bot"] is True
    assert kwargs["token"] == "CSRFTOKEN"
    assert _json.loads(kwargs["data"]) == {"claims": claims}
    request.submit.assert_called_once()


# --- fetch_revision_snapshots ---------------------------------------------


def test_fetch_revision_snapshots_projects_revid_user_text():
    """The pywikibot Revision object's three relevant fields are
    projected into the dataclass; anything else is dropped. Also pins
    that content=True is the call shape (without it pywikibot doesn't
    populate .text)."""

    class _Rev:
        def __init__(self, revid, user, text):
            self.revid, self.user, self.text = revid, user, text

    file_page = MagicMock()
    file_page.revisions.return_value = [
        _Rev(1, "DPLA_bot", "{{Artwork|title=A}}"),
        _Rev(2, "Editor1", "{{Artwork|title=B}}"),
    ]
    snapshots = fetch_revision_snapshots(file_page)
    assert len(snapshots) == 2
    assert snapshots[0].revid == 1 and snapshots[0].user == "DPLA_bot"
    assert snapshots[1].text == "{{Artwork|title=B}}"
    file_page.revisions.assert_called_once_with(content=True)


def test_fetch_revision_snapshots_tolerates_missing_user_and_text():
    """Suppressed-author revisions can have ``user=None`` and missing
    text. Coerce both to ``""`` so the planner just sees no params
    for that revision."""

    class _Rev:
        revid = 7
        user = None
        text = None

    file_page = MagicMock()
    file_page.revisions.return_value = [_Rev()]
    snapshots = fetch_revision_snapshots(file_page)
    assert len(snapshots) == 1
    assert snapshots[0].user == "" and snapshots[0].text == ""


# --- migrate_legacy_file end-to-end ---------------------------------------


def _mock_file_page(title: str, text: str, revisions: list):
    page = MagicMock()
    page.title.return_value = title
    page.text = text
    page.pageid = 42
    page.revisions.return_value = revisions
    return page


def _site_with_empty_entity():
    site = MagicMock()
    site.tokens = {"csrf": "CSRFTOKEN"}
    site.simple_request.return_value.submit.return_value = {"entities": {"M42": {}}}
    return site


def _item_md(title="A Title"):
    return (
        {
            "rights": "http://creativecommons.org/publicdomain/zero/1.0/",
            "isShownAt": "https://example.org/item/123",
            "sourceResource": {
                "title": [title],
                "description": ["A description"],
                "date": [{"displayDate": "1900"}],
                "creator": ["A Creator"],
                "identifier": ["local-1"],
            },
        },
        {"Wikidata": "Q1"},
        {"Wikidata": "Q2"},
    )


def test_migrate_legacy_file_skips_when_no_legacy_template():
    """A page already on the new template form returns the no-legacy
    skip reason without POSTing anything to Wikibase."""

    class _Rev:
        revid, user, text = 1, "DPLA_bot", "{{DPLA metadata|title=A Title}}"

    page = _mock_file_page("File:Foo.jpg", _Rev.text, [_Rev()])
    item, provider, dp = _item_md()
    site = _site_with_empty_entity()
    result = migrate_legacy_file(
        file_page=page,
        item_metadata=item,
        provider=provider,
        data_provider=dp,
        dpla_id="abc",
        site=site,
    )
    assert isinstance(result, MigrationResult)
    assert result.skipped_reason == "no-legacy-template"
    # No wbeditentity POST anywhere in the call history.
    assert all(
        c.kwargs.get("action") != "wbeditentity"
        for c in site.simple_request.call_args_list
    )


def test_migrate_legacy_file_skips_when_already_migrated():
    """The idempotency check spots an existing P887/Q131783016 ref on
    a P1476 statement and bails out before POSTing duplicates."""

    class _Rev:
        revid, user, text = 1, "DPLA_bot", "{{Artwork|title=A Title}}"

    page = _mock_file_page("File:Foo.jpg", _Rev.text, [_Rev()])
    item, provider, dp = _item_md()
    site = MagicMock()
    site.tokens = {"csrf": "CSRFTOKEN"}
    site.simple_request.return_value.submit.return_value = {
        "entities": {"M42": _entity_with_legacy_import("P1476")}
    }
    result = migrate_legacy_file(
        file_page=page,
        item_metadata=item,
        provider=provider,
        data_provider=dp,
        dpla_id="abc",
        site=site,
    )
    assert result.skipped_reason == "already-migrated"
    page.save.assert_not_called()


def test_migrate_legacy_file_dpla_only_history_writes_no_imports():
    """No community history → no imports, but the wikitext still gets
    rewritten to the new template form so the canonical state is
    consistent."""

    class _Rev:
        revid, user, text = 1, "DPLA_bot", "{{Artwork|title=A Title}}"

    page = _mock_file_page("File:Foo.jpg", _Rev.text, [_Rev()])
    item, provider, dp = _item_md()
    site = _site_with_empty_entity()
    result = migrate_legacy_file(
        file_page=page,
        item_metadata=item,
        provider=provider,
        data_provider=dp,
        dpla_id="abc",
        site=site,
    )
    assert result.imports_posted == 0
    assert result.wikitext_changed is True
    page.save.assert_called_once()
    save_summary = page.save.call_args.kwargs.get("summary", "")
    assert "Migrate legacy" in save_summary


def test_migrate_legacy_file_imports_community_value_and_rewrites_wikitext():
    """End-to-end happy path: DPLA_bot wrote the original, a community
    editor changed the title, we import that community value and
    rewrite the wikitext."""

    class _Rev1:
        revid, user, text = 1, "DPLA_bot", "{{Artwork|title=A Title}}"

    class _Rev2:
        revid, user, text = 2, "EditorOne", "{{Artwork|title=A Better Title}}"

    page = _mock_file_page("File:Foo.jpg", _Rev2.text, [_Rev1(), _Rev2()])
    item, provider, dp = _item_md()
    site = _site_with_empty_entity()
    result = migrate_legacy_file(
        file_page=page,
        item_metadata=item,
        provider=provider,
        data_provider=dp,
        dpla_id="abc",
        site=site,
    )
    assert result.imports_posted == 1
    assert result.wikitext_changed is True

    import json as _json

    wbeditentity_calls = [
        c
        for c in site.simple_request.call_args_list
        if c.kwargs.get("action") == "wbeditentity"
    ]
    assert len(wbeditentity_calls) == 1
    posted = _json.loads(wbeditentity_calls[0].kwargs["data"])
    assert len(posted["claims"]) == 1
    assert posted["claims"][0]["mainsnak"]["property"] == "P1476"
    assert (
        posted["claims"][0]["mainsnak"]["datavalue"]["value"]["text"]
        == "A Better Title"
    )


def test_legacy_migration_edit_summary_mentions_q131783016():
    """The edit summary should mention the inferred-from-Wikitext
    Wikidata item so reviewers can trace the migration's intent."""
    assert "Q131783016" in LEGACY_MIGRATION_EDIT_SUMMARY


def test_migrate_legacy_file_emits_canonical_whitespace():
    """End-to-end regression for the live bug observed on
    https://commons.wikimedia.org/wiki/File:Possibly_Carlisle_Indian_School_football_team_-_DPLA_-_0037596b6b4904655f0f949db0a1ab8b_(page_2).tiff
    where ``migrate_legacy_file`` produced wikitext with:

    1. A duplicated ``== {{int:filedesc}} ==`` heading (the new
       upload-form block was substituted verbatim in place of the
       legacy template, on top of the existing heading); AND
    2. The duplicated heading indented (the leading whitespace before
       the original ``{{ Artwork`` block survived as a Text node).

    The migration must produce the same canonical shape
    :func:`get_wiki_text` emits for new uploads: section heading
    left-justified at column 0, exactly one blank line, then the
    template left-justified at column 0.
    """

    class _Rev1:
        revid, user = 1, "DPLA_bot"
        text = (
            "== {{int:filedesc}} ==\n"
            "     {{ Artwork\n"
            "        | title = Possibly Carlisle Indian School football team\n"
            "        | source = {{ DPLA | Q59661040 | hub = Q518155 |"
            " url = http://x | dpla_id = abc | local_id = 123 }}\n"
            "     }}\n"
        )

    page = _mock_file_page("File:Foo.jpg", _Rev1.text, [_Rev1()])
    item, provider, dp = _item_md()
    site = _site_with_empty_entity()
    migrate_legacy_file(
        file_page=page,
        item_metadata=item,
        provider=provider,
        data_provider=dp,
        dpla_id="abc",
        site=site,
    )
    saved_text = page.text
    # Exactly one section heading.
    assert saved_text.count("== {{int:filedesc}} ==") == 1, (
        f"section heading duplicated:\n{saved_text}"
    )
    # Heading left-justified — no leading indent on the heading line.
    assert saved_text.startswith("== {{int:filedesc}} =="), (
        f"heading not at column 0:\n{saved_text!r}"
    )
    # Template left-justified — no leading whitespace before the
    # opening braces or before any param line.
    for line in saved_text.splitlines():
        if line.startswith("{{DPLA metadata") or line.startswith("| ") or line == "}}":
            continue
        assert not (line.startswith(" ") or line.startswith("\t")), (
            f"line carries leading whitespace: {line!r}"
        )
    # Canonical blank line between the heading and the template.
    assert "== {{int:filedesc}} ==\n\n{{DPLA metadata" in saved_text, (
        f"missing canonical blank-line separator:\n{saved_text!r}"
    )
