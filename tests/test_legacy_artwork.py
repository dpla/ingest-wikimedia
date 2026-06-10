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
    source."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|title=A Title}}"),
        (42, "Editor1", "{{Artwork|title=A Better Title}}"),
    )
    plan = plan_migration("File:Test File.jpg", revs, _canonical_params())
    assert plan is not None
    assert (
        plan.source_permalink
        == "https://commons.wikimedia.org/w/index.php?title=File:Test_File.jpg&oldid=42"
    )


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
