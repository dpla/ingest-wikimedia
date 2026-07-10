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


def test_parse_artwork_params_drops_wikitext_junk_values():
    """A 1-2-character punctuation-only value (``| date = ;``,
    ``| title = --``) is a wikitext-extraction artifact — parser or
    editor error, not real metadata. Extraction drops it the same way
    it drops empty values, so the migrator doesn't preserve markup
    junk as an SDC statement.

    Motivating example: File:A_Toledo_Symphony_Orchestra_---_why%3F_
    -_DPLA_-_640c3941bd35b12a39cb3820e9f778b2_(page_6).jpg — legacy
    template carried ``| date = ;`` and the 2026-06-25 migrator
    dutifully wrote a P571 = somevalue statement with the ``;`` as
    its P1932 stated-as qualifier.
    """
    wikitext = (
        "{{Artwork\n"
        "| title = --\n"
        "| description = A real description here\n"
        "| date = ; \n"
        "| creator = .\n"
        "}}"
    )
    params = parse_artwork_params(wikitext)
    assert params == {"description": "A real description here"}


def test_parse_artwork_params_keeps_single_alnum_values():
    """A single-letter title (film title ``A``) or single-digit value
    passes through — the junk filter targets only punctuation-only
    shorts."""
    wikitext = "{{Artwork|title=A|date=1}}"
    assert parse_artwork_params(wikitext) == {"title": "A", "date": "1"}


def test_parse_artwork_params_stitches_literal_pipe_truncation():
    """A legacy upload that writes pipe-separated subject terms directly
    into a named value (``| description = A | B | C``) gets truncated
    by ``mwparserfromhell`` — the ``|`` reads as a parameter separator
    and ``B`` / ``C`` land as anonymous positional args. Left
    unrepaired, a later AWB pass rewriting ``|`` → ``{{!}}`` looks like
    a content change (short truncated value → full value), causing the
    provenance walker to misattribute description authorship to the
    AWB editor and the migration to preserve DPLA-authored text as a
    spurious ``inferred-from-Wikitext`` SDC claim.

    Regression: Block_Card_633_Evesham_Avenue-DPLA-
    ccb2717b29309f0ef0e58a8221e75019 — 2020 DPLA_bot upload with
    literal ``|`` in description; 2020-06-04 JarektBot AWB pass
    swapped for ``{{!}}``; the pre-fix migration misattributed the
    description to JarektBot and wrote a P10358 statement preserving
    the AWB form of the wikitext.
    """
    literal_pipe_form = (
        "{{Artwork\n"
        "| title = A Title\n"
        "| description = terms: houses | 633 Evesham | Dwellings | Norwood\n"
        "| date = 1937\n"
        "}}"
    )
    magic_word_form = (
        "{{Artwork\n"
        "| title = A Title\n"
        "| description = terms: houses {{!}} 633 Evesham {{!}} Dwellings {{!}} Norwood\n"
        "| date = 1937\n"
        "}}"
    )
    a = parse_artwork_params(literal_pipe_form)
    b = parse_artwork_params(magic_word_form)
    # Both revs parse to the same description — the AWB rewrite is
    # display-invariant and must produce a display-invariant parse too.
    assert a == b
    assert a["description"] == "terms: houses | 633 Evesham | Dwellings | Norwood"
    assert a["date"] == "1937"


def test_parse_artwork_params_pipe_overflow_from_unrecognised_param_dropped():
    """Overflow positional args from an unrecognised named param
    (``| Other fields 1 = X | Y | title = …``) must NOT be misattributed
    to a previous recognised named entry. ``Y`` is overflow from
    ``Other fields 1``; dropping it is correct because we can't route
    it to any canonical target."""
    wikitext = (
        "{{Artwork\n"
        "| title = T\n"
        "| Other fields 1 = X | Y | Z\n"
        "| description = A real description\n"
        "| date = 1937\n"
        "}}"
    )
    p = parse_artwork_params(wikitext)
    assert p == {
        "title": "T",
        "description": "A real description",
        "date": "1937",
    }


def test_parse_artwork_params_stitching_ignores_pipes_inside_nested_templates():
    """A nested ``{{Institution|wikidata=Q…}}`` inside a param value
    keeps its own ``|`` scoped to the nested template — the outer
    Artwork param value is one whole string, not truncated. The
    stitching pass must not double-count or damage this case."""
    wikitext = (
        "{{Artwork\n"
        "| title = T\n"
        "| institution = {{Institution|wikidata=Q7814140}}\n"
        "| date = 1937\n"
        "}}"
    )
    p = parse_artwork_params(wikitext)
    assert p == {
        "title": "T",
        "institution": "{{Institution|wikidata=Q7814140}}",
        "date": "1937",
    }


def test_parse_artwork_params_pipe_overflow_with_equals_fragment_is_dropped():
    """Known limitation of the stitching heuristic: an overflow fragment
    that contains ``=`` (e.g. ``| description = A | region=north | …``)
    is parsed by ``mwparserfromhell`` as a NAMED parameter
    (``region=north``) rather than as an anonymous positional. Only
    anonymous positional overflow is stitched, so the ``region=north``
    fragment is dropped and the literal-pipe form's parse diverges
    from the corresponding ``{{!}}`` form for the same source text.

    Pinned here as a regression test — the shape is rare in real DPLA
    metadata (typical subject lists don't contain ``key=value``) and
    handling it would require a wider allowlist-based heuristic. If
    we ever fix this, the test flips to assert successful stitching.
    """
    literal_pipe_form = "{{Artwork\n| description = A | region=north\n| date = 1937\n}}"
    p = parse_artwork_params(literal_pipe_form)
    # Current behavior: ``region=north`` is dropped because mwparserfromhell
    # parses it as a named param and stitching only rejoins positional
    # overflow. Description ends at the truncation point.
    assert p == {"description": "A", "date": "1937"}
    # For contrast, the {{!}} form keeps the whole value:
    magic_word_form = (
        "{{Artwork\n| description = A {{!}} region=north\n| date = 1937\n}}"
    )
    q = parse_artwork_params(magic_word_form)
    assert q == {"description": "A | region=north", "date": "1937"}


def test_parse_artwork_params_unescapes_magic_words_in_values():
    """A community AWB pass sometimes rewrites literal ``|`` inside a
    template param to the ``{{!}}`` magic word (parser expands it back
    to ``|`` at render time — display-invariant). Extraction must
    un-escape so:

    (a) the value stored downstream as an SDC statement is the literal
        text a reader sees, not the magic-word form that has no meaning
        outside a template context, and
    (b) the migration provenance walker doesn't credit the AWB pass for
        a content change that didn't actually change display.

    Motivating example: File:Block_Card_6_E._Bancroft_Street_-_DPLA_-
    _307d98570261183ed48eb3b1880fce14.jpg — 2020-06-04 AWB pass
    replaced ``|`` with ``{{!}}`` in the description; the July 2026
    legacy-Artwork migrator then stored the escaped form as a P10358
    SDC statement, permanently diverging from DPLA canonical.
    """
    wikitext = (
        "{{Artwork\n"
        "| title = A {{=}} B\n"
        "| description = terms include: buildings {{!}} Italianate "
        "{{!}} one story\n"
        "}}"
    )
    params = parse_artwork_params(wikitext)
    assert params == {
        "title": "A = B",
        "description": ("terms include: buildings | Italianate | one story"),
    }


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
    classification coverage for the long-tenure files those bots uploaded.
    Space form is canonical — it's what the Commons API returns in ``user``."""
    assert "DPLA bot" in DPLA_BOT_ACCOUNTS
    assert "US National Archives bot" in DPLA_BOT_ACCOUNTS
    assert "Flickr upload bot" in DPLA_BOT_ACCOUNTS


def test_classify_matches_underscore_and_space_username_forms():
    """Commons returns usernames with spaces ("DPLA bot"); the allowlist stores
    the space form. Matching must be underscore/space-insensitive so DPLA-bot
    edits are never misclassified as community — which, for a *drifted* value,
    would resurrect stale DPLA metadata as a fake community contribution."""
    classified = classify_param_provenance(
        {"a": "DPLA bot", "b": "DPLA_bot", "c": "dpla_BOT"}
    )
    assert classified == {"a": "dpla", "b": "dpla", "c": "dpla"}


def test_classify_flickr_upload_bot_is_dpla():
    """Flickr2Commons imports are automated import data, not community
    curation — a file the Flickr bot uploaded and we later renamed must not
    have its (drifted) fields resurrected as community contributions."""
    assert classify_param_provenance({"title": "Flickr upload bot"}) == {
        "title": "dpla"
    }


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
        # creator is a plain string in dpla_metadata_params (extract_strings),
        # NOT an {{InFi|Creator|...}} dict — see _canonical_value_for_key.
        "creator": "A Creator",
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


def test_plan_migration_recognizes_nara_image_full():
    """{{NARA-image-full}} is a recognised migration wrapper: its core params
    map by NAME through ARTWORK_PARAM_TO_CANONICAL_KEY (no NARA-specific
    parsing), so a community-edited Title is imported, while its NARA-only
    archival params (ARC / Record group / …) have no canonical target and are
    simply dropped — not imported, not errored."""
    revs = _make_revs(
        (
            1,
            "US National Archives bot",
            "{{NARA-image-full|Title=A Title|Date=1900|ARC=12345|Record group=RG 26}}",
        ),
        (
            2,
            "CommunityEditor",
            "{{NARA-image-full|Title=A Better Title|Date=1900"
            "|ARC=12345|Record group=RG 26}}",
        ),
    )
    plan = plan_migration("File:Foo.jpg", revs, _canonical_params())
    assert plan is not None
    # Community-edited core field rescued via the name-based mapping.
    assert plan.community_imports == {"title": "A Better Title"}
    # NARA-only archival params never became canonical keys.
    assert "ARC" not in plan.dpla_originated_params
    assert "record group" not in plan.dpla_originated_params
    # Date matched canonical and was bot-set → dpla-originated (not imported).
    assert plan.dpla_originated_params.get("date") == "1900"


def test_plan_migration_does_not_resurrect_drifted_bot_value():
    """Core drift-safety property: a value the *bot* last set that now DIFFERS
    from canonical is drifted DPLA metadata (DPLA-caused) — NOT a community
    edit — so it must stay dpla-originated (replaced by canonical), never
    imported as a community contribution. Uses NARA's own bot (its pre-2020
    uploads) with the space-form username the Commons API returns."""
    revs = _make_revs(
        (
            1,
            "US National Archives bot",
            "{{Artwork|title=Old Drifted Title|date=1900}}",
        ),
    )
    plan = plan_migration("File:Foo.jpg", revs, _canonical_params(title="A Title"))
    assert plan is not None
    # Title differs from canonical ("A Title") but was bot-authored → NOT rescued.
    assert plan.community_imports == {}
    assert plan.dpla_originated_params["title"] == "Old Drifted Title"


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


def test_plan_migration_imports_creator_param_that_differs_from_canonical():
    """Regression: canonical 'creator' is a plain string (extract_strings),
    not an {{InFi|Creator|...}} dict. A file whose Artwork template carries
    a community creator that differs from canonical must be imported — and
    must not raise ``AttributeError: 'str' object has no attribute 'get'``
    (the old _canonical_value_for_key called .get() on the string)."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|creator=A Creator}}"),
        (2, "Editor1", "{{Artwork|creator=Someone Else}}"),
    )
    plan = plan_migration("File:Foo.jpg", revs, _canonical_params())
    assert plan is not None
    assert plan.community_imports == {"creator": "Someone Else"}
    assert "creator" not in plan.dpla_originated_params


def test_plan_migration_skips_creator_param_matching_canonical():
    """A community editor restating DPLA's creator verbatim is redundant —
    canonical creator (a string) compares equal, so no import."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|creator=A Creator}}"),
        (2, "Editor1", "{{Artwork|creator=A Creator}}"),
    )
    plan = plan_migration("File:Foo.jpg", revs, _canonical_params())
    assert plan is not None
    assert plan.community_imports == {}
    assert plan.dpla_originated_params.get("creator") == "A Creator"


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
    LEGACY_MIGRATION_BASE_SUMMARY,
    LEGACY_MIGRATION_EDIT_SUMMARY,
    MigrationResult,
    _build_related_image_claim,
    _extract_related_image_files,
    _normalize_commons_filename,
    build_migration_summary,
    entity_was_already_migrated,
    fetch_revision_snapshots,
    import_cross_page_community_sdc,
    materialize_import_claims,
    materialize_pending_date_claim,
    materialize_pending_related_image_claim,
    migrate_legacy_file,
    post_legacy_import_claims,
    render_migrated_wikitext,
    rescue_wikitext,
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


class _FakeSite:
    """Minimal ``site`` stub providing ``expand_text``."""

    def __init__(self, expansions):
        self._expansions = expansions

    def expand_text(self, text):
        return self._expansions.get(text, text)


def _dpla_p571_statement(time_str, precision, *, circa=False):
    """Build a wbgetentities-shaped DPLA-attributed P571 statement.
    The ``value`` dict matches the canonical shape
    ``ingest_wikimedia.sdc._wikibase_time`` emits — DPLA's own
    pipeline always writes that exact shape, so the dedup
    whole-value comparison sees identical dicts under normal use."""
    quals = {
        "P459": [
            {
                "snaktype": "value",
                "property": "P459",
                "datavalue": {
                    "type": "wikibase-entityid",
                    "value": {"id": "Q61848113"},
                },
            }
        ]
    }
    if circa:
        quals["P1480"] = [
            {
                "snaktype": "value",
                "property": "P1480",
                "datavalue": {
                    "type": "wikibase-entityid",
                    "value": {"id": "Q5727902"},
                },
            }
        ]
    return {
        "mainsnak": {
            "snaktype": "value",
            "property": "P571",
            "datavalue": {
                "type": "time",
                "value": {
                    "time": time_str,
                    "precision": precision,
                    "before": 0,
                    "after": 0,
                    "timezone": 0,
                    "calendarmodel": "http://www.wikidata.org/entity/Q1985727",
                },
            },
        },
        "qualifiers": quals,
    }


def test_materialize_pending_date_claim_expands_other_date_template():
    """``{{other date|~|1911}}`` expands to ``circa 1911`` which the
    DPLA-date parser recognises — value-typed P571 + P1480 circa, and
    the P1932 qualifier stores the expanded text, NOT the raw markup."""
    placeholder = {
        "_phase3a_pending_date_parse": "{{other date|~|1911}}",
        "_property": "P571",
        "_permalink": "https://example/permalink",
    }
    site = _FakeSite({"{{other date|~|1911}}": "circa 1911"})
    claim = materialize_pending_date_claim(placeholder, site=site)
    assert claim is not None
    assert claim["mainsnak"]["snaktype"] == "value"
    assert claim["mainsnak"]["datavalue"]["value"]["time"] == "+1911-01-01T00:00:00Z"
    assert claim["qualifiers"]["P1480"][0]["datavalue"]["value"]["id"] == "Q5727902"
    # P1932 stores the expanded text, not the raw template markup.
    assert claim["qualifiers"]["P1932"][0]["datavalue"]["value"] == "circa 1911"


def test_materialize_pending_date_claim_strips_hidden_html_from_expansion():
    """The hidden QuickStatements micro-format ``{{other date}}`` appends
    (a ``<div style="display: none;">...</div>``) is scaffolding for the
    rendered page; it must NOT survive into the P1932 string."""
    placeholder = {
        "_phase3a_pending_date_parse": "{{other date|~|1911}}",
        "_property": "P571",
        "_permalink": "https://example/permalink",
    }
    site = _FakeSite(
        {
            "{{other date|~|1911}}": (
                'circa 1911<div style="display: none;">'
                "date QS:P,+1911-00-00T00:00:00Z/9,P1480,Q5727902</div>"
            )
        }
    )
    claim = materialize_pending_date_claim(placeholder, site=site)
    assert claim is not None
    assert claim["qualifiers"]["P1932"][0]["datavalue"]["value"] == "circa 1911"


def test_materialize_pending_date_claim_skips_when_dpla_already_has_match():
    """When the parsed editor-value matches an existing DPLA-attributed
    P571 (same time + precision + circa flag), the import is a literal
    duplicate of DPLA's own claim — return None so the caller drops it."""
    placeholder = {
        "_phase3a_pending_date_parse": "{{other date|~|1911}}",
        "_property": "P571",
        "_permalink": "https://example/permalink",
    }
    site = _FakeSite({"{{other date|~|1911}}": "circa 1911"})
    existing = {
        "statements": {
            "P571": [_dpla_p571_statement("+1911-01-01T00:00:00Z", 9, circa=True)]
        }
    }
    claim = materialize_pending_date_claim(
        placeholder, site=site, existing_entity=existing
    )
    assert claim is None


def test_materialize_pending_date_claim_imports_when_dpla_value_differs():
    """When the parsed editor-value parses to a DIFFERENT date than
    DPLA's existing P571, the import is real community-contributed
    information and must NOT be dropped."""
    placeholder = {
        "_phase3a_pending_date_parse": "1925",
        "_property": "P571",
        "_permalink": "https://example/permalink",
    }
    existing = {
        "statements": {
            "P571": [_dpla_p571_statement("+1911-01-01T00:00:00Z", 9, circa=True)]
        }
    }
    claim = materialize_pending_date_claim(placeholder, existing_entity=existing)
    assert claim is not None
    assert claim["mainsnak"]["datavalue"]["value"]["time"] == "+1925-01-01T00:00:00Z"


def test_materialize_pending_date_claim_imports_when_circa_flag_differs():
    """Same year, different circa flag — semantically distinct
    (editor asserts "approximately 1911" while DPLA asserts "exactly
    1911" or vice versa); do NOT dedup."""
    placeholder = {
        "_phase3a_pending_date_parse": "ca. 1911",
        "_property": "P571",
        "_permalink": "https://example/permalink",
    }
    existing = {
        "statements": {
            "P571": [_dpla_p571_statement("+1911-01-01T00:00:00Z", 9, circa=False)]
        }
    }
    claim = materialize_pending_date_claim(placeholder, existing_entity=existing)
    assert claim is not None
    assert "P1480" in claim["qualifiers"]


def test_materialize_pending_date_claim_ignores_non_dpla_attributed_existing():
    """A non-DPLA-attributed (no P459=Q61848113) existing P571 must NOT
    trigger the dedup — that statement represents community-contributed
    information of unknown provenance, NOT a value to dedup against."""
    placeholder = {
        "_phase3a_pending_date_parse": "1911",
        "_property": "P571",
        "_permalink": "https://example/permalink",
    }
    # Build a P571 with the same value but NO P459 qualifier.
    naked = _dpla_p571_statement("+1911-01-01T00:00:00Z", 9, circa=False)
    naked["qualifiers"].pop("P459", None)
    existing = {"statements": {"P571": [naked]}}
    claim = materialize_pending_date_claim(placeholder, existing_entity=existing)
    assert claim is not None


def _dpla_p571_range_statement(p1932_value, *, circa=False):
    """Build a wbgetentities-shaped DPLA-attributed P571 *range* claim —
    ``somevalue`` mainsnak with a P1932 stated-as qualifier carrying
    the range string. Mirrors what
    :func:`ingest_wikimedia.sdc._build_date_claim` emits when
    ``parse_dpla_date`` returns None (the universal range path)."""
    quals = {
        "P459": [
            {
                "snaktype": "value",
                "property": "P459",
                "datavalue": {
                    "type": "wikibase-entityid",
                    "value": {"id": "Q61848113"},
                },
            }
        ],
        "P1932": [
            {
                "snaktype": "value",
                "property": "P1932",
                "datavalue": {"type": "string", "value": p1932_value},
            }
        ],
    }
    if circa:
        quals["P1480"] = [
            {
                "snaktype": "value",
                "property": "P1480",
                "datavalue": {
                    "type": "wikibase-entityid",
                    "value": {"id": "Q5727902"},
                },
            }
        ]
    return {
        "mainsnak": {
            "snaktype": "somevalue",
            "property": "P571",
            "datatype": "time",
        },
        "qualifiers": quals,
    }


def test_materialize_pending_date_claim_skips_when_dpla_already_has_matching_range():
    """The original bug — ``{{other date|between|1934|1948}}`` expanded
    to ``between 1934 and 1948`` is semantically identical to DPLA's
    ``1934 - 1948``. Both produce the same ``(1934, 1948)`` canonical
    range key via :func:`ingest_wikimedia.sdc.parse_date_range`, so the
    materialiser must drop the inferred-from-Wikitext import.

    Mirrors M193555788 (Group_Portrait_of_"Indians" Mission Grove)
    where this dedup failure caused two parallel P571 statements before
    the range-matcher landed."""
    placeholder = {
        "_phase3a_pending_date_parse": "{{other date|between|1934|1948}}",
        "_property": "P571",
        "_permalink": "https://example/permalink",
    }
    site = _FakeSite({"{{other date|between|1934|1948}}": "between 1934 and 1948"})
    existing = {"statements": {"P571": [_dpla_p571_range_statement("1934 - 1948")]}}
    claim = materialize_pending_date_claim(
        placeholder, site=site, existing_entity=existing
    )
    assert claim is None


def test_materialize_pending_date_claim_skips_range_match_across_format_variants():
    """Range equivalence is direction- and separator-agnostic. Verify
    every reasonable cross-pairing of editor-side and DPLA-side
    formattings dedups, so we don't ship per-format dedup gaps."""
    site = _FakeSite({})  # no expansion needed; values already in canonical text
    cross_pairs = [
        ("1934-1948", "between 1934 and 1948"),
        ("between 1934 and 1948", "1934 - 1948"),
        ("1934–1948", "1934/1948"),
        ("1948 - 1934", "1934 - 1948"),  # reversed order canonicalises
    ]
    for editor_value, dpla_value in cross_pairs:
        placeholder = {
            "_phase3a_pending_date_parse": editor_value,
            "_property": "P571",
            "_permalink": "https://example/permalink",
        }
        existing = {"statements": {"P571": [_dpla_p571_range_statement(dpla_value)]}}
        claim = materialize_pending_date_claim(
            placeholder, site=site, existing_entity=existing
        )
        assert claim is None, (
            f"editor={editor_value!r} vs dpla={dpla_value!r} should dedup"
        )


def test_materialize_pending_date_claim_imports_when_range_differs():
    """A range with different endpoints is real community-contributed
    information — must NOT dedup. The materialiser produces a
    ``somevalue + P1932`` claim with the editor's range string."""
    placeholder = {
        "_phase3a_pending_date_parse": "1950 - 1960",
        "_property": "P571",
        "_permalink": "https://example/permalink",
    }
    existing = {"statements": {"P571": [_dpla_p571_range_statement("1934 - 1948")]}}
    claim = materialize_pending_date_claim(placeholder, existing_entity=existing)
    assert claim is not None
    assert claim["mainsnak"]["snaktype"] == "somevalue"
    assert claim["qualifiers"]["P1932"][0]["datavalue"]["value"] == "1950 - 1960"


def test_materialize_pending_date_claim_range_match_requires_dpla_attribution():
    """A non-DPLA-attributed (no P459 = Q61848113) existing range claim
    must NOT trigger the range dedup — same safety bar as the
    structured-time matcher. Mirror of
    ``test_materialize_pending_date_claim_ignores_non_dpla_attributed_existing``
    for the range path."""
    placeholder = {
        "_phase3a_pending_date_parse": "1934 - 1948",
        "_property": "P571",
        "_permalink": "https://example/permalink",
    }
    naked = _dpla_p571_range_statement("1934 - 1948")
    naked["qualifiers"].pop("P459", None)
    existing = {"statements": {"P571": [naked]}}
    claim = materialize_pending_date_claim(placeholder, existing_entity=existing)
    assert claim is not None


def test_materialize_import_claims_forwards_existing_entity_for_dedup():
    """The list-level wrapper has to forward ``existing_entity`` so the
    dedup behaviour applies in the migration pipeline, not only in the
    per-claim helper. Verified by passing a dedup-triggering entity
    and asserting the date placeholder gets dropped from the output."""
    title_claim = {"type": "statement", "mainsnak": {"property": "P1476"}}
    date_placeholder = {
        "type": "statement",
        "_phase3a_pending_date_parse": "1911",
        "_property": "P571",
        "_permalink": "https://example/permalink",
    }
    existing = {
        "statements": {
            "P571": [_dpla_p571_statement("+1911-01-01T00:00:00Z", 9, circa=False)]
        }
    }
    out = materialize_import_claims(
        [title_claim, date_placeholder], existing_entity=existing
    )
    assert out == [title_claim]  # date dropped, title preserved


def test_materialize_import_claims_forwards_site_for_expansion():
    """The list-level wrapper has to forward ``site`` so the template-
    expansion behaviour applies in the migration pipeline. Verified by
    passing a wikitext-template date placeholder and asserting the
    expanded value appears in the materialised claim's P1932."""
    date_placeholder = {
        "type": "statement",
        "_phase3a_pending_date_parse": "{{other date|~|1911}}",
        "_property": "P571",
        "_permalink": "https://example/permalink",
    }
    site = _FakeSite({"{{other date|~|1911}}": "circa 1911"})
    out = materialize_import_claims([date_placeholder], site=site)
    assert len(out) == 1
    assert out[0]["qualifiers"]["P1932"][0]["datavalue"]["value"] == "circa 1911"


def test_materialize_pending_date_claim_preserves_visible_inner_text():
    """The hidden-microformat strip must not eat visible text inside
    plain ``<span>``/``<i>`` wrappers (e.g. ``<span>circa 1911</span>``
    from a formatting-focused template). Only display:none / aria-
    hidden / QS-class scaffolding should disappear."""
    placeholder = {
        "_phase3a_pending_date_parse": "{{some formatting template|1911}}",
        "_property": "P571",
        "_permalink": "https://example/permalink",
    }
    site = _FakeSite({"{{some formatting template|1911}}": "<span>circa 1911</span>"})
    claim = materialize_pending_date_claim(placeholder, site=site)
    assert claim is not None
    # Visible inner text survived; outer <span> tag was stripped.
    assert claim["qualifiers"]["P1932"][0]["datavalue"]["value"] == "circa 1911"


def test_materialize_pending_date_claim_skips_when_existing_value_full_match():
    """Full whole-value match required: same time / precision /
    before / after / timezone / calendarmodel. Any divergence in
    those bound fields disqualifies the dedup."""
    placeholder = {
        "_phase3a_pending_date_parse": "1911",
        "_property": "P571",
        "_permalink": "https://example/permalink",
    }
    # Build an existing statement whose ``before`` bound differs from
    # the parser's canonical 0. Should NOT dedup.
    diverging = _dpla_p571_statement("+1911-01-01T00:00:00Z", 9, circa=False)
    diverging["mainsnak"]["datavalue"]["value"]["before"] = 5
    existing = {"statements": {"P571": [diverging]}}
    claim = materialize_pending_date_claim(placeholder, existing_entity=existing)
    assert claim is not None  # different uncertainty bound, import preserved


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


def test_render_migrated_wikitext_preserves_image_notes():
    """The regular migration is a node-swap, so {{ImageNote}} annotation
    blocks outside the legacy template survive verbatim. The #397 title-drift
    fix defers to this path, so it must not drop community image notes."""
    note = (
        "{{ImageNote|id=1|x=10|y=20|w=30|h=40|dimx=100|dimy=200|style=2}}\n"
        "a detail worth noting\n"
        "{{ImageNoteEnd|id=1}}"
    )
    original = (
        "== {{int:filedesc}} ==\n"
        "{{Artwork|title=Old}}\n"
        f"{note}\n"
        "[[Category:Foo]]"
    )
    result = render_migrated_wikitext(original, "{{DPLA metadata|title=New}}")
    assert "{{Artwork" not in result
    assert "{{DPLA metadata|title=New}}" in result
    assert note in result
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


# --- rescue_wikitext (cross-page, preserve-by-default) --------------------


def test_rescue_wikitext_node_swaps_and_preserves_everything_outside():
    """Preserve-by-default: node-swap the source's wrapper for the fresh
    {{DPLA metadata}} block and keep ALL other content verbatim — including
    image-note annotations and arbitrary community templates that no
    allowlist ever enumerated."""
    note = (
        "{{ImageNote|id=1|x=10|y=20|w=30|h=40|dimx=100|dimy=200|style=2}}\n"
        "a community annotation\n"
        "{{ImageNoteEnd|id=1}}"
    )
    source = (
        "== {{int:filedesc}} ==\n"
        "{{Artwork|title=Old|creator={{Creator:Jane Doe}}}}\n"
        f"{note}\n"
        "{{SomeCommunityTemplate|x=1}}\n"
        "[[Category:Community curated]]"
    )
    result = rescue_wikitext(source, "{{DPLA metadata|title=New}}")
    assert "{{Artwork" not in result
    assert "{{DPLA metadata|title=New}}" in result
    assert note in result  # image notes preserved for free
    assert "{{SomeCommunityTemplate|x=1}}" in result  # arbitrary template preserved
    assert "[[Category:Community curated]]" in result


@pytest.mark.parametrize(
    "wrapper",
    [
        "{{Artwork|title=Old}}",
        "{{Information|title=Old}}",
        "{{Photograph|title=Old}}",
        "{{DPLA metadata|title=Old}}",
        "{{NARA-image-full|title=Old}}",
    ],
)
def test_rescue_wikitext_recognizes_all_wrappers(wrapper):
    """Every wrapper the cross-page rescue can meet — the legacy forms, the
    already-migrated {{DPLA metadata}}, and NARA's {{NARA-image-full}} — is
    node-swapped for the fresh block, carrying a trailing category through."""
    source = f"{wrapper}\n[[Category:Keep me]]"
    result = rescue_wikitext(source, "{{DPLA metadata|title=New}}")
    assert "{{DPLA metadata|title=New}}" in result
    assert "[[Category:Keep me]]" in result
    assert "title=Old" not in result  # old wrapper replaced, not appended


def test_rescue_wikitext_falls_back_to_allowlist_when_no_wrapper():
    """A source with no recognised metadata wrapper can't be node-swapped, so
    rescue_wikitext falls back to the narrow merge_preserved_wikitext allowlist
    — license/category kept, an unrecognised community template dropped (the
    inherent limit of the no-wrapper case, not a regression)."""
    source = (
        "{{PD-USGov}}\n{{UnknownCommunityThing|x=1}}\n[[Category:Kept by allowlist]]"
    )
    result = rescue_wikitext(source, "{{DPLA metadata|title=New}}")
    assert "{{DPLA metadata|title=New}}" in result
    assert "{{PD-USGov}}" in result
    assert "[[Category:Kept by allowlist]]" in result
    assert "{{UnknownCommunityThing" not in result


def test_rescue_wikitext_no_duplicate_heading_with_full_form_block():
    """Regression (lesson from #299): the live callers pass get_wiki_text's
    FULL upload form (heading + blank line + template), not a bare template.
    rescue_wikitext must substitute template-only, so a source that already
    carries a heading doesn't end up with two."""
    source = "== {{int:filedesc}} ==\n{{Artwork|title=Old}}\n[[Category:Keep]]"
    full_form_block = "== {{int:filedesc}} ==\n\n{{DPLA metadata\n| title = New\n}}"
    result = rescue_wikitext(source, full_form_block)
    assert result.count("== {{int:filedesc}} ==") == 1
    assert "{{DPLA metadata" in result
    assert "{{Artwork" not in result
    assert "[[Category:Keep]]" in result


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


def test_migrate_legacy_file_converts_nara_image_full_and_drops_archival_params():
    """A {{NARA-image-full}} page migrates like any legacy template: its
    community-edited core field imports to SDC and the wrapper is node-swapped
    for {{DPLA metadata}}. Its NARA-only archival params are not carried onto
    the new form — accepted loss, they have no canonical/SDC home."""

    class _Rev1:
        revid = 1
        user = "US National Archives bot"
        text = "{{NARA-image-full|Title=A Title|ARC=12345|Record group=RG 26}}"

    class _Rev2:
        revid = 2
        user = "CommunityEditor"
        text = "{{NARA-image-full|Title=A Better Title|ARC=12345|Record group=RG 26}}"

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
    saved = page.text
    assert "{{NARA-image-full" not in saved
    assert "{{DPLA metadata" in saved
    # Archival params dropped on the swap (no canonical/SDC home).
    assert "ARC" not in saved
    assert "RG 26" not in saved


# --- import_cross_page_community_sdc (cross-page inside-template rescue) ---


def test_import_cross_page_community_sdc_imports_from_source_history():
    """Plans over the SOURCE page's history and posts the community-authored
    value to the DESTINATION entity — the cross-page analogue of the SDC half
    of migrate_legacy_file."""

    class _Rev1:
        revid, user, text = 1, "DPLA_bot", "{{Artwork|title=A Title}}"

    class _Rev2:
        revid, user, text = 2, "EditorOne", "{{Artwork|title=A Better Title}}"

    source = _mock_file_page("File:Old.jpg", _Rev2.text, [_Rev1(), _Rev2()])
    item, provider, dp = _item_md()
    site = _site_with_empty_entity()
    site.simple_request.return_value.submit.return_value = {"entities": {"M99": {}}}

    n = import_cross_page_community_sdc(
        source_page=source,
        dest_mediaid="M99",
        item_metadata=item,
        provider=provider,
        data_provider=dp,
        dpla_id="abc",
        site=site,
    )
    assert n == 1

    import json as _json

    wbeditentity = [
        c
        for c in site.simple_request.call_args_list
        if c.kwargs.get("action") == "wbeditentity"
    ]
    assert len(wbeditentity) == 1
    # Written to the DESTINATION entity (M99), not the source page.
    assert wbeditentity[0].kwargs["id"] == "M99"
    posted = _json.loads(wbeditentity[0].kwargs["data"])
    assert (
        posted["claims"][0]["mainsnak"]["datavalue"]["value"]["text"]
        == "A Better Title"
    )


def test_import_cross_page_community_sdc_idempotent_on_dest_entity():
    """If the destination entity already carries a legacy-import ref, bail
    out with 0 and post nothing — a re-run must not duplicate claims."""

    class _Rev2:
        revid, user, text = 2, "EditorOne", "{{Artwork|title=A Better Title}}"

    source = _mock_file_page("File:Old.jpg", _Rev2.text, [_Rev2()])
    item, provider, dp = _item_md()
    site = MagicMock()
    site.tokens = {"csrf": "CSRFTOKEN"}
    site.simple_request.return_value.submit.return_value = {
        "entities": {"M42": _entity_with_legacy_import("P1476")}
    }

    n = import_cross_page_community_sdc(
        source_page=source,
        dest_mediaid="M42",
        item_metadata=item,
        provider=provider,
        data_provider=dp,
        dpla_id="abc",
        site=site,
    )
    assert n == 0
    assert all(
        c.kwargs.get("action") != "wbeditentity"
        for c in site.simple_request.call_args_list
    )


def test_import_cross_page_community_sdc_nothing_to_rescue_on_bot_only_history():
    """A DPLA-bot-only source history has no community-authored value to
    import, so nothing is posted and the count is 0."""

    class _Rev:
        revid, user, text = 1, "DPLA_bot", "{{Artwork|title=A Title}}"

    source = _mock_file_page("File:Old.jpg", _Rev.text, [_Rev()])
    item, provider, dp = _item_md()
    site = _site_with_empty_entity()

    n = import_cross_page_community_sdc(
        source_page=source,
        dest_mediaid="M42",
        item_metadata=item,
        provider=provider,
        data_provider=dp,
        dpla_id="abc",
        site=site,
    )
    assert n == 0
    assert all(
        c.kwargs.get("action") != "wbeditentity"
        for c in site.simple_request.call_args_list
    )


# --- related image (P6802, commonsMedia datatype) -------------------------


def test_extract_related_image_files_parses_other_version_templates():
    wt = (
        "{{Artwork|title=T|Other versions="
        "{{other version|File:First related.jpg}}"
        "{{Other Version|Second related.jpg}}}}"
    )
    # File: prefix stripped; case-insensitive template name; order preserved.
    assert _extract_related_image_files(wt) == [
        "First related.jpg",
        "Second related.jpg",
    ]


def test_extract_related_image_files_empty_when_absent():
    assert _extract_related_image_files("{{Artwork|title=T}}") == []
    assert _extract_related_image_files("no template here") == []


def test_normalize_commons_filename_canonicalises_title_form():
    # underscore→space, first-letter upper, whitespace collapse, File: strip.
    assert _normalize_commons_filename("rel_image.jpg") == "Rel image.jpg"
    assert _normalize_commons_filename("File:rel image.jpg") == "Rel image.jpg"
    assert _normalize_commons_filename("  a   b.jpg ") == "A b.jpg"


def test_extract_related_image_files_normalizes_and_dedups_surface_forms():
    # rel image.jpg and Rel_image.jpg name the SAME Commons file → one entry,
    # in MediaWiki's canonical form.
    wt = (
        "{{Artwork|Other versions="
        "{{other version|rel image.jpg}}{{other version|Rel_image.jpg}}}}"
    )
    assert _extract_related_image_files(wt) == ["Rel image.jpg"]


def test_build_related_image_claim_serialises_commons_media_correctly():
    """The first commonsMedia-typed property in the pipeline — verify the
    exact Wikibase shape: string datavalue, 'commons-media' snak datatype,
    bare filename, standard inferred-from-Wikitext reference."""
    claim = _build_related_image_claim(
        "A file.jpg", "https://commons.wikimedia.org/w/index.php?title=X&oldid=1"
    )
    ms = claim["mainsnak"]
    assert ms["property"] == "P6802"
    assert ms["datatype"] == "commons-media"
    assert ms["datavalue"] == {"type": "string", "value": "A file.jpg"}
    assert ms["snaktype"] == "value"
    ref = claim["references"][0]["snaks"]
    assert ref["P887"][0]["datavalue"]["value"]["id"] == "Q131783016"


def test_plan_migration_extracts_related_images_unconditionally():
    """Related images are preserved regardless of provenance — a bot-only
    history (no community edits) still yields the P6802 import, because DPLA
    has no canonical related-image value to drift from."""
    revs = _make_revs(
        (
            1,
            "DPLA_bot",
            "{{Artwork|title=A Title|Other versions={{other version|Rel.jpg}}}}",
        ),
    )
    plan = plan_migration("File:Foo.jpg", revs, _canonical_params())
    assert plan is not None
    assert plan.community_imports == {}  # bot-only → nothing provenance-gated
    assert plan.related_image_imports == ["Rel.jpg"]  # ...but kept anyway


def test_build_legacy_import_claims_emits_related_image_placeholder():
    plan = MigrationPlan(source_permalink="P", related_image_imports=["Rel.jpg"])
    claims = build_legacy_import_claims(plan)
    assert len(claims) == 1
    assert claims[0]["_phase3a_pending_related_image"] == "Rel.jpg"
    assert claims[0]["_permalink"] == "P"


def test_materialize_related_image_builds_when_site_none():
    claim = materialize_pending_related_image_claim(
        {"_phase3a_pending_related_image": "Rel.jpg", "_permalink": "P"}
    )
    assert claim is not None
    assert claim["mainsnak"]["datatype"] == "commons-media"
    assert claim["mainsnak"]["datavalue"]["value"] == "Rel.jpg"


def test_materialize_related_image_drops_missing_file():
    """A {{other version}} pointing at a non-existent file is dropped, so it
    can't fail Wikibase's commonsMedia validation and poison the whole atomic
    claim bundle."""
    site = MagicMock()
    site.simple_request.return_value.submit.return_value = {
        "query": {"pages": {"-1": {"missing": ""}}}
    }
    claim = materialize_pending_related_image_claim(
        {"_phase3a_pending_related_image": "Gone.jpg", "_permalink": "P"}, site=site
    )
    assert claim is None


def test_materialize_related_image_dedups_existing_p6802():
    entity = {
        "statements": {
            "P6802": [
                {"mainsnak": {"snaktype": "value", "datavalue": {"value": "Rel.jpg"}}}
            ]
        }
    }
    claim = materialize_pending_related_image_claim(
        {"_phase3a_pending_related_image": "Rel.jpg", "_permalink": "P"},
        site=None,
        existing_entity=entity,
    )
    assert claim is None


def test_materialize_related_image_dedups_across_title_surface_forms():
    """CR nitpick: a stored P6802 written as 'Rel_image.jpg' must dedup against
    an extracted 'Rel image.jpg' — MediaWiki treats them as the same file."""
    entity = {
        "statements": {
            "P6802": [
                {
                    "mainsnak": {
                        "snaktype": "value",
                        "datavalue": {"value": "Rel_image.jpg"},
                    }
                }
            ]
        }
    }
    claim = materialize_pending_related_image_claim(
        {"_phase3a_pending_related_image": "Rel image.jpg", "_permalink": "P"},
        site=None,
        existing_entity=entity,
    )
    assert claim is None


def test_entity_was_already_migrated_ignores_p6802_only():
    """P6802 (related image) is deliberately NOT part of the global migrated
    trip-wire: a file whose only prior import was a related image must stay
    eligible so a later rescue can pick up a newly-added scalar community
    value. (P6802 duplicate-prevention lives in the materializer instead.)"""
    assert entity_was_already_migrated(_entity_with_legacy_import("P6802")) is False
    # A scalar import property still trips it.
    assert entity_was_already_migrated(_entity_with_legacy_import("P1476")) is True


def test_migrate_legacy_file_imports_related_image_as_commons_media():
    """End-to-end: an {{Artwork}} with Other versions={{other version|X}}
    posts a P6802 commonsMedia claim carrying the bare filename."""

    class _Rev:
        revid = 1
        user = "DPLA_bot"
        text = "{{Artwork|title=A Title|Other versions={{other version|Rel file.jpg}}}}"

    page = _mock_file_page("File:Foo.jpg", _Rev.text, [_Rev()])
    item, provider, dp = _item_md()
    site = MagicMock()
    site.tokens = {"csrf": "CSRFTOKEN"}
    # One response satisfies both the wbgetentities fetch (entities key) and
    # the commonsMedia existence query (query.pages, no "missing").
    site.simple_request.return_value.submit.return_value = {
        "entities": {"M42": {}},
        "query": {"pages": {"1": {"pageid": 1, "title": "File:Rel file.jpg"}}},
    }
    result = migrate_legacy_file(
        file_page=page,
        item_metadata=item,
        provider=provider,
        data_provider=dp,
        dpla_id="abc",
        site=site,
    )
    assert (
        result.imports_posted == 1
    )  # only the related image (title matches canonical)

    import json as _json

    wbedit = [
        c
        for c in site.simple_request.call_args_list
        if c.kwargs.get("action") == "wbeditentity"
    ]
    assert len(wbedit) == 1
    posted = _json.loads(wbedit[0].kwargs["data"])["claims"]
    p6802 = [c for c in posted if c["mainsnak"]["property"] == "P6802"]
    assert len(p6802) == 1
    assert p6802[0]["mainsnak"]["datatype"] == "commons-media"
    assert p6802[0]["mainsnak"]["datavalue"] == {
        "type": "string",
        "value": "Rel file.jpg",
    }


def test_import_cross_page_rescues_related_image_with_no_scalar_community():
    """The cross-page guard must not skip a source whose only rescuable
    content is a related image (no scalar community_imports)."""

    class _Rev:
        revid = 1
        user = "DPLA_bot"
        text = "{{Artwork|title=A Title|Other versions={{other version|Rel.jpg}}}}"

    source = _mock_file_page("File:Old.jpg", _Rev.text, [_Rev()])
    item, provider, dp = _item_md()
    site = MagicMock()
    site.tokens = {"csrf": "CSRFTOKEN"}
    site.simple_request.return_value.submit.return_value = {
        "entities": {"M42": {}},
        "query": {"pages": {"1": {"pageid": 1}}},
    }
    n = import_cross_page_community_sdc(
        source_page=source,
        dest_mediaid="M42",
        item_metadata=item,
        provider=provider,
        data_provider=dp,
        dpla_id="abc",
        site=site,
    )
    assert n == 1  # the related image, despite no scalar community imports


def test_legacy_migration_edit_summary_mentions_q131783016():
    """The edit summary should mention the inferred-from-Wikitext
    Wikidata item so reviewers can trace the migration's intent."""
    assert "Q131783016" in LEGACY_MIGRATION_EDIT_SUMMARY


def test_build_migration_summary_omits_community_clause_when_zero_claims():
    """A DPLA-bot-only history where every wikitext value matched
    the DPLA canonical value posts zero community-import claims —
    the summary must not promise SDC-preservation behaviour that
    didn't fire. Pre-fix, every migration carried the boilerplate
    "community-contributed metadata preserved..." clause regardless
    of whether any community values were actually imported."""
    summary = build_migration_summary(0)
    assert summary == LEGACY_MIGRATION_BASE_SUMMARY
    assert "Q131783016" not in summary
    assert "community-contributed" not in summary
    assert "preserved" not in summary


def test_build_migration_summary_includes_community_clause_when_claims_posted():
    """A history with at least one community-edited wikitext value
    posts that value as an SDC statement under the community-import
    reference shape, and the summary documents that."""
    summary = build_migration_summary(1)
    assert "Q131783016" in summary
    assert "preserved" in summary


def test_migrate_legacy_file_uses_honest_summary_on_dpla_only_history():
    """End-to-end: when no community values were detected on a file
    (DPLA-bot-only revision history, every param matches canonical),
    the wikitext save's edit summary uses the base form without the
    community-preservation clause. Regression for the live case where
    every Valuation Section / Smithsonian negative file carried the
    misleading boilerplate."""

    class _Rev1:
        revid, user = 1, "DPLA_bot"
        text = (
            "== {{int:filedesc}} ==\n"
            "{{ Artwork\n"
            "| title = A Title\n"
            "| source = {{ DPLA | Q1 | hub = Q2 |"
            " url = https://example.org/item/123 |"
            " dpla_id = abc | local_id = local-1 }}\n"
            "| Institution = {{ Institution | wikidata = Q1 }}\n"
            "}}\n"
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
    save_summary = page.save.call_args.kwargs["summary"]
    assert save_summary == LEGACY_MIGRATION_BASE_SUMMARY
    assert "Q131783016" not in save_summary


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


def test_migrate_legacy_file_strips_params_matching_sdc_in_one_edit():
    """Regression for the live bug observed on
    https://commons.wikimedia.org/wiki/File:Indian_portrait,_bust_-_DPLA_-_1c4a5c6601e7daec71e04483a5c304a7.gif
    where migration produced a fully-populated ``{{DPLA metadata}}``
    template with every param verbatim (creator, title, date,
    permission, hub, institution, url, dpla_id, local_id) — the
    pre-strip steady state, not the post-strip steady state.

    For a DPLA-bot-only revision history (no community contributions),
    every wikitext param value matches the canonical ``item_metadata``,
    so the strip pass must remove all of them — the migrated file
    should end at the post-strip steady state ``{{DPLA metadata}}``
    in the same edit as the migrate, not two edits later after a
    follow-up sdc-sync pass.
    """

    class _Rev1:
        revid, user = 1, "DPLA_bot"
        text = (
            "== {{int:filedesc}} ==\n"
            "     {{ Artwork\n"
            "        | Other fields 1 = {{ InFi | Creator |"
            " A Creator | id=fileinfotpl_aut}}\n"
            "        | title = A Title\n"
            "        | description = A description\n"
            "        | date = 1900\n"
            "        | permission = {{NoC-US | Q1}}\n"
            "        | source = {{ DPLA | Q1 | hub = Q2 |"
            " url = https://example.org/item/123 |"
            " dpla_id = abc | local_id = local-1 }}\n"
            "        | Institution = {{ Institution | wikidata = Q1 }}\n"
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
    saved = page.text
    # No DPLA-attributed params should remain in the wikitext. Each
    # one is canonically present in SDC after the migration, so the
    # display will render from there.
    for key in (
        "title",
        "description",
        "creator",
        "date",
        "permission",
        "hub",
        "institution",
        "url",
        "dpla_id",
        "local_id",
    ):
        assert f"| {key}" not in saved, (
            f"strip missed param {key!r}; full text:\n{saved}"
        )
    # The template itself is still present — collapsed to single line.
    assert "{{DPLA metadata}}" in saved
    # And only one save was issued.
    assert page.save.call_count == 1


# ---------------------------------------------------------------------------
# plan_migration — community-vs-canonical equivalence widening (this PR).
# A community edit that reformatted DPLA's own value (case, punctuation,
# or date format) is NOT a community contribution and must not be
# imported as an inferred-from-Wikitext SDC claim. The equivalence
# function :func:`_value_equivalent_to_canonical` widens the previous
# byte-equality check to cover these editor-reformat cases.
# ---------------------------------------------------------------------------


def test_plan_migration_skips_semantically_equal_date():
    """Editor reformatted DPLA's `1900` as `January 1, 1900` (or
    similar). Same year+month+day at same precision — no import."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|date=1900-06-15}}"),
        (2, "Editor1", "{{Artwork|date=15 June 1900}}"),
    )
    plan = plan_migration("File:Foo.jpg", revs, _canonical_params(date="1900-06-15"))
    assert plan is not None
    assert plan.community_imports == {}, (
        f"expected `15 June 1900` == `1900-06-15` semantically; "
        f"got community_imports={plan.community_imports}"
    )
    assert plan.dpla_originated_params.get("date") == "15 June 1900"


def test_plan_migration_skips_case_only_title_change():
    """Editor retyped title in uppercase — no factual change, no
    import. Casefold widening applies to display-string keys."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|title=A Title}}"),
        (2, "Editor1", "{{Artwork|title=A TITLE}}"),
    )
    plan = plan_migration("File:Foo.jpg", revs, _canonical_params())
    assert plan is not None
    assert plan.community_imports == {}


def test_plan_migration_skips_trailing_period_description_change():
    """Editor stripped a trailing period from DPLA's description —
    still the same fact, no import."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|description=A description.}}"),
        (2, "Editor1", "{{Artwork|description=A description}}"),
    )
    plan = plan_migration(
        "File:Foo.jpg", revs, _canonical_params(description="A description.")
    )
    assert plan is not None
    assert plan.community_imports == {}


def test_plan_migration_still_imports_substantively_different_date():
    """Widening must not lose real differences — an editor supplied a
    different date, we import."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|date=1900}}"),
        (2, "Editor1", "{{Artwork|date=1950}}"),
    )
    plan = plan_migration("File:Foo.jpg", revs, _canonical_params(date="1900"))
    assert plan is not None
    assert plan.community_imports == {"date": "1950"}


def test_plan_migration_still_imports_substantively_different_title():
    """Guard against overshoot — a title that differs by more than
    case/punctuation still imports."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|title=A Title}}"),
        (2, "Editor1", "{{Artwork|title=A Completely Different Title}}"),
    )
    plan = plan_migration("File:Foo.jpg", revs, _canonical_params())
    assert plan is not None
    assert plan.community_imports == {"title": "A Completely Different Title"}


def test_plan_migration_skips_description_that_is_multi_value_subset_of_canonical():
    """A community editor reduces a multi-valued DPLA description
    (removes one entry) — the remaining values are all still DPLA-
    authored, so the wikitext content is a subset of DPLA canonical
    and no ``inferred-from-Wikitext`` import should fire. Subset
    check widens equivalence to catch this case; without it, the
    community edit gets preserved forever as a spurious P10358.

    Regression: File:%22Principles_of_Causality%22_essay_by_Sarah_..._-_DPLA_-_b3f489f90ebb903b961500c0cf71edfc
    — DPLA has 6 description values, wikitext concatenation had 5;
    the pre-fix migration wrote an inferred-from-Wikitext P10358
    with the 5-value concatenation preserved as a bogus community
    contribution.

    Test forces the community classification with a real editor
    revision (removing one value) — without a non-bot revision the
    provenance walker attributes the value to DPLA_bot and skips the
    equivalence check entirely.
    """
    canonical_description = "; ".join(
        [
            "Eight page essay with markings made by teacher in pencil.",
            "Date supplied by cataloger.",
            "Sallie M. Field",
            "Phillips Academy Archives received the collection.",
            "From The Trustees of Phillips Academy.",
            "This date is inferred.",
        ]
    )
    # Rev 1 matches full DPLA (6 values). Editor removed one value.
    initial_wikitext_description = canonical_description
    edited_wikitext_description = "; ".join(
        [
            "Eight page essay with markings made by teacher in pencil.",
            "Date supplied by cataloger.",
            "Sallie M. Field",
            "Phillips Academy Archives received the collection.",
            "From The Trustees of Phillips Academy.",
        ]
    )
    revs = _make_revs(
        (
            1,
            "DPLA_bot",
            f"{{{{Artwork|description={initial_wikitext_description}}}}}",
        ),
        (
            2,
            "Editor1",
            f"{{{{Artwork|description={edited_wikitext_description}}}}}",
        ),
    )
    plan = plan_migration(
        "File:Foo.jpg",
        revs,
        _canonical_params(description=canonical_description),
    )
    assert plan is not None
    # Editor last touched description → classified community → equivalence
    # check runs. Subset check widens to say the 5-value wikitext ⊆
    # 6-value canonical, so no community_import.
    assert plan.community_imports == {}, (
        f"community edit whose values are all in DPLA canonical should "
        f"not be an import; got community_imports={plan.community_imports}"
    )


def test_plan_migration_subset_handles_single_value_wikitext_vs_multi_canonical():
    """N=1 boundary of the subset check: wikitext has a single value
    (no ``; `` delimiter) but canonical is multi-value. Still a
    subset — the wikitext value appears in the canonical set — and
    must not classify as community. Pre-fix, the guard required the
    delimiter on both sides, short-circuiting this case to False and
    forcing an inferred-from-Wikitext import for what is really a
    single-value-at-upload-then-DPLA-expanded record."""
    canonical_description = "; ".join(["only value", "later addition"])
    # Rev 1 has two values; Rev 2 (community editor) reduces to one.
    # Reduction changes the parsed value, so provenance walker attributes
    # description to Editor1 → classified as community → subset check
    # runs on the (now single-value) wikitext against the multi-value
    # canonical.
    revs = _make_revs(
        (
            1,
            "DPLA_bot",
            "{{Artwork|description=only value; later addition}}",
        ),
        (2, "Editor1", "{{Artwork|description=only value}}"),
    )
    plan = plan_migration(
        "File:Foo.jpg",
        revs,
        _canonical_params(description=canonical_description),
    )
    assert plan is not None
    # Wikitext value = "only value" (no delim). Canonical = "only value;
    # later addition". Subset check: {"only value"} ⊆ {"only value",
    # "later addition"} → True → not community_import.
    assert plan.community_imports == {}, (
        f"single-value wikitext that's a subset of multi-value canonical "
        f"should not be an import; got community_imports={plan.community_imports}"
    )


def test_plan_migration_still_imports_description_with_extra_community_value():
    """Guard against overshoot — a community editor appended a value
    NOT in DPLA canonical to the description concatenation. Subset
    check correctly says the wikitext isn't a subset of canonical, so
    the community edit is preserved as an inferred-from-Wikitext SDC
    statement rather than silently dropped as an equivalent."""
    canonical_description = "; ".join(["DPLA one.", "DPLA two."])
    wikitext_description = "; ".join(
        ["DPLA one.", "DPLA two.", "Community-added extra value."]
    )
    original_description = "; ".join(["DPLA one.", "DPLA two."])
    revs = _make_revs(
        (
            1,
            "DPLA_bot",
            f"{{{{Artwork|description={original_description}}}}}",
        ),
        (
            2,
            "Editor1",
            f"{{{{Artwork|description={wikitext_description}}}}}",
        ),
    )
    plan = plan_migration(
        "File:Foo.jpg",
        revs,
        _canonical_params(description=canonical_description),
    )
    assert plan is not None
    assert "description" in plan.community_imports
    assert "Community-added extra value" in plan.community_imports["description"]


# ---------------------------------------------------------------------------
# plan_migration — institution Q-ID equivalence (this commit).
# For NARA files in particular, the legacy `{{Artwork}}` template wrote
# the data-provider as a nested `{{Institution|wikidata=Q...}}` sub-
# template, while `dpla_metadata_params` now emits it as a bare Q-ID.
# A byte-wise inequality shouldn't lead to a spurious inferred-from-
# Wikitext import when both sides carry the same Q-ID.
# ---------------------------------------------------------------------------


def test_plan_migration_skips_institution_subtemplate_matching_canonical_qid():
    """Legacy `{{Institution|wikidata=Q59661041}}` sub-template value in
    a community-authored revision must NOT import as inferred-from-
    Wikitext when the canonical DPLA `institution` param is the same
    Q-ID. Motivating example: NARA custodial-unit files whose legacy
    `Institution =` field held the same custodial-unit Q-ID that DPLA
    now writes as canonical."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|institution=Q59661041}}"),
        (2, "Editor1", "{{Artwork|institution={{Institution|wikidata=Q59661041}}}}"),
    )
    plan = plan_migration(
        "File:Foo.jpg", revs, _canonical_params(institution="Q59661041")
    )
    assert plan is not None
    assert plan.community_imports == {}, (
        f"expected `{{Institution|wikidata=Q59661041}}` to be recognised "
        f"as equivalent to canonical `Q59661041`; got "
        f"community_imports={plan.community_imports}"
    )


def test_plan_migration_still_imports_institution_that_differs():
    """Q-ID mismatch is real — the community pointed the file at a
    different institution and that override must be preserved."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|institution=Q59661041}}"),
        (2, "Editor1", "{{Artwork|institution={{Institution|wikidata=Q77777777}}}}"),
    )
    plan = plan_migration(
        "File:Foo.jpg", revs, _canonical_params(institution="Q59661041")
    )
    assert plan is not None
    assert "institution" in plan.community_imports


def test_plan_migration_extract_institution_qid_handles_flat_bare_qid():
    """The flat `{{DPLA metadata|institution=Q123}}` shape stores the
    Q-ID bare — the extractor recognises both shapes so migration off
    an already-partially-modernised page still equates cleanly."""
    from ingest_wikimedia.legacy_artwork import _extract_institution_qid

    assert _extract_institution_qid("Q59661041") == "Q59661041"
    assert _extract_institution_qid("{{Institution|wikidata=Q59661041}}") == "Q59661041"
    # Case-insensitive on the template name + key.
    assert _extract_institution_qid("{{institution|Wikidata=Q123}}") == "Q123"
    # Whitespace-tolerant.
    assert _extract_institution_qid("  {{ Institution | wikidata = Q123 }} ") == "Q123"
    # Non-matching shapes return None (so the caller can fall through
    # to the byte-equality / casefold branches without wrongly claiming
    # a Q-ID equivalence).
    assert _extract_institution_qid("") is None
    assert _extract_institution_qid("Not a Q-ID") is None
    assert _extract_institution_qid("Q") is None  # no digits
    assert _extract_institution_qid("Q59661041x") is None  # trailing junk


def test_plan_migration_preserves_bracketed_date_override():
    """Regression guard (CR flagged on PR #351): a community editor's
    ``date = [1902]`` (archival supplied-date convention) carries an
    approximate-flag semantic distinct from the bare canonical ``1902``.
    Must be preserved as a community import, not classified as
    dpla-originated on a casefold false-match.
    """
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|date=1902}}"),
        (2, "Editor1", "{{Artwork|date=[1902]}}"),
    )
    plan = plan_migration("File:Foo.jpg", revs, _canonical_params(date="1902"))
    assert plan is not None
    assert plan.community_imports == {"date": "[1902]"}, (
        f"expected `[1902]` to be preserved as a community import; "
        f"got community_imports={plan.community_imports}"
    )


def test_plan_migration_preserves_question_marked_date_override():
    """Same shape as above but with the ``1902?`` uncertain-marker."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|date=1902}}"),
        (2, "Editor1", "{{Artwork|date=1902?}}"),
    )
    plan = plan_migration("File:Foo.jpg", revs, _canonical_params(date="1902"))
    assert plan is not None
    assert plan.community_imports == {"date": "1902?"}


# ---------------------------------------------------------------------------
# plan_migration — DPLA-prefixed extension (this commit).
# A community editor extends a DPLA-authored template parameter by
# appending structural wikitext (gallery, HR, wikitable, list, embedded
# template) past the DPLA-authored text. Since Wikibase's monolingual-
# text validator rejects vertical whitespace, the extras can't be
# submitted as an SDC monolingualtext claim — but the DPLA prefix is
# still intact and shouldn't be treated as community divergence.
# The extras go to ``wikitext_preserved_extras`` for injection into
# the migrated ``{{DPLA metadata}}`` template's own parameter; no SDC
# import fires for that key.
# ---------------------------------------------------------------------------


_DPLA_SENTENCE = (
    'Transcribed from photograph: "Portraits. Group. Faculty of Central '
    'School. Opening day, May 7, 1883."'
)


def test_plan_migration_gallery_extension_preserves_extras_not_import():
    """The mockup case: user appends ``----`` + ``<gallery>`` after the
    DPLA sentence. The DPLA prefix stays DPLA-attributed (no
    community-import for description), and the ``----\\n<gallery>``
    block lands verbatim in ``wikitext_preserved_extras`` so the
    migration executor can inject it onto the migrated template."""
    extras = (
        "\n----\n<gallery>\nFile:one.jpg|caption 1\nFile:two.jpg|caption 2\n</gallery>"
    )
    extended = _DPLA_SENTENCE + extras
    revs = _make_revs(
        (1, "DPLA_bot", f"{{{{Artwork|description={_DPLA_SENTENCE}}}}}"),
        (2, "Editor1", f"{{{{Artwork|description={extended}}}}}"),
    )
    plan = plan_migration(
        "File:Foo.jpg", revs, _canonical_params(description=_DPLA_SENTENCE)
    )
    assert plan is not None
    assert "description" not in plan.community_imports, (
        "extension case must not emit an inferred-from-Wikitext claim; "
        "Wikibase would reject the vertical whitespace"
    )
    assert plan.wikitext_preserved_extras.get("description") == extras


def test_plan_migration_hr_only_extension_preserves_extras():
    """No gallery, just an HR + extra prose. The rule is structural-
    marker-agnostic — presence of a vertical whitespace boundary plus a
    matching DPLA prefix is all that's required."""
    extras = "\n----\nAdditional context added by Commons editor."
    extended = _DPLA_SENTENCE + extras
    revs = _make_revs(
        (1, "DPLA_bot", f"{{{{Artwork|description={_DPLA_SENTENCE}}}}}"),
        (2, "Editor1", f"{{{{Artwork|description={extended}}}}}"),
    )
    plan = plan_migration(
        "File:Foo.jpg", revs, _canonical_params(description=_DPLA_SENTENCE)
    )
    assert plan is not None
    assert "description" not in plan.community_imports
    assert plan.wikitext_preserved_extras.get("description") == extras


def test_plan_migration_bulleted_list_extension_preserves_extras():
    """A user-added bulleted list past the DPLA sentence — the shape
    Ohio Cleveland Public Library used to link to related Wikipedia
    entries (the class the log survey identified as the biggest
    silent-skip source, 64 events / 3 unique DPLA IDs)."""
    extras = (
        "\n* [https://en.wikipedia.org/wiki/John_Doe John Doe]"
        "\n* [https://en.wikipedia.org/wiki/Jane_Roe Jane Roe]"
    )
    extended = _DPLA_SENTENCE + extras
    revs = _make_revs(
        (1, "DPLA_bot", f"{{{{Artwork|description={_DPLA_SENTENCE}}}}}"),
        (2, "Editor1", f"{{{{Artwork|description={extended}}}}}"),
    )
    plan = plan_migration(
        "File:Foo.jpg", revs, _canonical_params(description=_DPLA_SENTENCE)
    )
    assert plan is not None
    assert "description" not in plan.community_imports
    assert plan.wikitext_preserved_extras.get("description") == extras


def test_plan_migration_scalar_edit_stays_on_divergence_path():
    """No vertical whitespace = not extension. A single-line reformat
    like ``1949`` → ``1949-02-01`` is a genuine value replacement, not
    an extension, and belongs on the ordinary community-import path so
    the community's more-specific value gets an inferred-from-Wikitext
    SDC claim."""
    revs = _make_revs(
        (1, "DPLA_bot", "{{Artwork|date=1949}}"),
        (2, "Editor1", "{{Artwork|date=1949-02-01}}"),
    )
    plan = plan_migration("File:Foo.jpg", revs, _canonical_params(date="1949"))
    assert plan is not None
    assert plan.community_imports == {"date": "1949-02-01"}
    assert "date" not in plan.wikitext_preserved_extras


def test_plan_migration_extension_prefix_must_match_canonical():
    """A wikitext value that CONTAINS vertical whitespace but whose
    prefix doesn't match canonical is not extension — it's a full
    replacement that happens to have multiple lines. Falls through to
    the ordinary community-import path (which will hit the pre-flight
    validator and raise ``InvalidWikibaseTextValue``, catching the
    unhandled shape loudly instead of silently mis-classifying it as
    an extension)."""
    revs = _make_revs(
        (1, "DPLA_bot", f"{{{{Artwork|description={_DPLA_SENTENCE}}}}}"),
        (
            2,
            "Editor1",
            "{{Artwork|description=Totally different first line.\n"
            "Followed by a second line.}}",
        ),
    )
    plan = plan_migration(
        "File:Foo.jpg", revs, _canonical_params(description=_DPLA_SENTENCE)
    )
    assert plan is not None
    assert "description" in plan.community_imports
    assert "description" not in plan.wikitext_preserved_extras


def test_format_legacy_import_claim_rejects_vertical_whitespace():
    """Pre-flight validator refuses to build a monolingualtext claim
    whose value contains vertical whitespace. This is belt-and-suspenders
    — plan_migration should route such values to
    ``wikitext_preserved_extras`` instead — but if a shape slips past
    the extraction heuristic, the failure surfaces here with a clear
    "value X for property Y failed local validation" message rather
    than as a Wikibase APIError traceback."""
    from ingest_wikimedia.legacy_artwork import InvalidWikibaseTextValue

    with pytest.raises(InvalidWikibaseTextValue) as exc:
        format_legacy_import_claim(
            "description",
            "line one\nline two",
            "https://commons.wikimedia.org/w/index.php?title=X&oldid=1",
        )
    assert "description" in str(exc.value)
    assert "P10358" in str(exc.value)


def test_format_legacy_import_claim_rejects_leading_trailing_whitespace():
    """Wikibase also rejects leading/trailing whitespace on monolingual
    values (same validator, same rule). Same client-side pre-flight
    behaviour."""
    from ingest_wikimedia.legacy_artwork import InvalidWikibaseTextValue

    with pytest.raises(InvalidWikibaseTextValue):
        format_legacy_import_claim(
            "description",
            "   trailing space around a value   ",
            "https://commons.wikimedia.org/w/index.php?title=X&oldid=1",
        )


def test_format_legacy_import_claim_accepts_clean_multiline_free_value():
    """Sanity — a clean scalar description still passes and produces a
    normal monolingualtext claim."""
    claim = format_legacy_import_claim(
        "description",
        "Clean single-line description.",
        "https://commons.wikimedia.org/w/index.php?title=X&oldid=1",
    )
    assert claim is not None
    assert claim["mainsnak"]["datavalue"]["value"]["text"] == (
        "Clean single-line description."
    )


def test_inject_preserved_extras_matches_fresh_upload_layout():
    """Layout regression: injected extras must land in the same
    multi-line ``{{DPLA metadata\\n| key = value\\n}}`` shape that a
    fresh upload emits, not jammed onto the opening ``{{`` line. A
    migrated file should read the same as a freshly-uploaded one so
    operators reviewing diffs don't have to distinguish shapes."""
    from ingest_wikimedia.legacy_artwork import _inject_preserved_extras

    empty = "== {{int:filedesc}} ==\n\n{{DPLA metadata}}\n\n[[Category:Example]]\n"
    extras = {"description": ("\n----\n<gallery>\nFile:one.jpg|front\n</gallery>")}
    injected = _inject_preserved_extras(empty, extras)
    assert "{{DPLA metadata\n| description =" in injected, (
        f"expected first param on its own line; got: {injected!r}"
    )
    assert injected.rstrip().endswith("}}\n\n[[Category:Example]]".rstrip()) or (
        "</gallery>\n}}" in injected
    ), f"expected closing }} on its own line; got: {injected!r}"
    # Category preservation — the injection must not disturb what comes
    # after the template.
    assert "[[Category:Example]]" in injected


def test_inject_preserved_extras_scoped_to_template_node_not_page_wide():
    """Regression: newline-insertion must target the ``{{DPLA metadata}}``
    template node only, not the whole serialised page. If a preserved
    extra ever contains the literal ``{{DPLA metadata|`` (e.g. a nested
    template inside a user's gallery caption, or an embedded example),
    a page-wide string replace would rewrite that literal too and
    corrupt the user's contribution."""
    from ingest_wikimedia.legacy_artwork import _inject_preserved_extras

    poisoned = (
        "\nSee also the wrapper docs: <code>{{DPLA metadata|title=example}}</code>"
    )
    injected = _inject_preserved_extras("{{DPLA metadata}}", {"description": poisoned})
    assert poisoned in injected, (
        "extras value must survive verbatim — the newline insertion must "
        f"not touch matches inside the value. Got: {injected!r}"
    )
    assert "{{DPLA metadata|title=example}}" in injected, (
        "inline ``{{DPLA metadata|title=example}}`` inside the description "
        f"extras was rewritten. Got: {injected!r}"
    )


def test_inject_preserved_extras_preserves_value_verbatim():
    """The extras value goes in byte-identical (aside from a trailing
    newline the layout helper appends when absent). No stripping of
    ``----``, ``<br />``, blank lines, headings, or any specific
    marker — the user's structural markup is what it is."""
    from ingest_wikimedia.legacy_artwork import _inject_preserved_extras

    empty = "{{DPLA metadata}}"
    weird = "\n\n<br />\n\n== Header ==\n{| \n| Cell \n|}"
    injected = _inject_preserved_extras(empty, {"description": weird})
    assert weird in injected, (
        f"extras value must be preserved byte-identical; got: {injected!r}"
    )


# ---------------------------------------------------------------------------
# Creator community-contribution extraction (this commit).
# Legacy ``Other fields N = {{InFi|Creator|<inner>}}`` uploads carried
# community-contributed creator values in a wrapper that the previous
# extractor didn't recognise. This commit adds:
#   * InFi-wrapper unwrap in parse_artwork_params
#   * Inner-shape dispatch for {{Creator:Foo}}, {{creator|Wikidata=Q…}},
#     {{NARA-Author|<name>|<id>}}, and plain strings
#   * Materialise-time QID resolution for Creator: pages via Commons
#     pageprops
#   * P170-QID and P170-somevalue+P2093-stated-as claim shapes,
#     replacing the previous P2093 mainsnak that Module:DPLA didn't
#     read as a creator statement
# ---------------------------------------------------------------------------


def test_parse_artwork_params_unwraps_infi_creator_with_creator_page():
    """The ``Other fields 1 = {{InFi|Creator|{{Creator:Foo}}}}`` wrapper
    must be unwrapped and its inner ``{{Creator:Foo}}`` routed to the
    canonical ``creator`` key with a page-title sentinel so the
    executor can resolve to a Wikidata QID via Commons pageprops."""
    from ingest_wikimedia.legacy_artwork import (
        parse_artwork_params,
        _CREATOR_PAGE_PREFIX,
    )

    wt = (
        "{{Artwork\n"
        "| Other fields 1 = {{ InFi | Creator | "
        "{{Creator:Theodore E. Peiser}} | id=fileinfotpl_aut }}\n"
        "| title = A title\n"
        "}}"
    )
    params = parse_artwork_params(wt)
    assert params.get("creator") == (_CREATOR_PAGE_PREFIX + "Theodore E. Peiser"), (
        f"got {params!r}"
    )


def test_parse_artwork_params_unwraps_infi_creator_with_wikidata_template():
    """``{{creator|Wikidata=Q…}}`` inside the InFi wrapper is
    dispatched with a QID sentinel so the executor doesn't need to
    resolve anything — the community already supplied the QID."""
    from ingest_wikimedia.legacy_artwork import (
        parse_artwork_params,
        _CREATOR_QID_PREFIX,
    )

    wt = (
        "{{Artwork\n"
        "| Other fields 1 = {{ InFi | Creator | "
        "{{creator|Wikidata=Q56159174}} | id=fileinfotpl_aut }}\n"
        "}}"
    )
    params = parse_artwork_params(wt)
    assert params.get("creator") == _CREATOR_QID_PREFIX + "Q56159174"


def test_parse_artwork_params_strips_nara_author_completely():
    """``{{NARA-Author|<name>|<id>}}`` is bot-authored legacy
    scaffolding, never a real community edit. It must be stripped
    entirely — no community-import claim fires — so any drift between
    its captured value and current DPLA SDC doesn't become permanently
    attributed as an inferred-from-Wikitext contribution. Verified by
    the creator key being absent from the parse output entirely,
    matching the behaviour of an omitted param."""
    from ingest_wikimedia.legacy_artwork import parse_artwork_params

    wt = (
        "{{Artwork\n"
        "| Other fields 1 = {{ InFi | Creator | "
        "{{NARA-Author|Adams, Ansel, 1902-1984, Photographer|1332556}} "
        "| id=fileinfotpl_aut }}\n"
        "| title = A title\n"
        "}}"
    )
    params = parse_artwork_params(wt)
    assert "creator" not in params, (
        f"NARA-Author must be stripped without producing a creator "
        f"entry; got {params!r}"
    )


def test_parse_artwork_params_plain_string_creator_in_infi_wrapper():
    """A plain stated-as name inside the InFi wrapper still routes to
    ``creator`` as a plain string — same path as the flat ``|creator=``
    shape."""
    from ingest_wikimedia.legacy_artwork import parse_artwork_params

    wt = (
        "{{Artwork\n"
        "| Other fields 1 = {{ InFi | Creator | Peiser, Theodore E "
        "| id=fileinfotpl_aut }}\n"
        "}}"
    )
    params = parse_artwork_params(wt)
    assert params.get("creator") == "Peiser, Theodore E"


def test_parse_artwork_params_creator_wikidata_template_in_flat_param():
    """``{{creator|Wikidata=Q…}}`` in the flat ``| creator = …``
    param (post-migration Commons-editor shape) is also dispatched
    with a QID sentinel — same downstream handling as the wrapped
    legacy form."""
    from ingest_wikimedia.legacy_artwork import (
        parse_artwork_params,
        _CREATOR_QID_PREFIX,
    )

    wt = "{{Artwork\n| creator = {{creator|Wikidata=Q56159174}}\n}}"
    params = parse_artwork_params(wt)
    assert params.get("creator") == _CREATOR_QID_PREFIX + "Q56159174"


def test_materialize_creator_qid_placeholder_drops_when_qid_already_on_entity():
    """``{{creator|Wikidata=Q…}}`` whose QID already matches a P170
    QID on the entity is a no-op community import — drop the claim
    from the materialised list rather than posting a redundant
    second P170 statement."""
    from ingest_wikimedia.legacy_artwork import materialize_import_claims

    placeholder = {
        "type": "statement",
        "rank": "normal",
        "_phase3a_pending_creator_qid": "Q56159174",
        "_permalink": "https://commons.wikimedia.org/w/index.php?oldid=1",
    }
    entity = {
        "statements": {
            "P170": [
                {
                    "mainsnak": {
                        "snaktype": "value",
                        "property": "P170",
                        "datavalue": {
                            "type": "wikibase-entityid",
                            "value": {"entity-type": "item", "id": "Q56159174"},
                        },
                    }
                }
            ]
        }
    }
    materialised = materialize_import_claims([placeholder], existing_entity=entity)
    assert materialised == [], (
        "creator-QID placeholder whose QID is already on the entity's "
        f"P170 must not produce a claim; got {materialised!r}"
    )


def test_materialize_creator_qid_placeholder_emits_when_qid_differs():
    """When the community QID isn't already on the entity, emit a
    proper P170 mainsnak QID statement with the inferred-from-
    Wikitext reference shape — NOT the previous P2093 mainsnak that
    Module:DPLA doesn't render as a creator row."""
    from ingest_wikimedia.legacy_artwork import materialize_import_claims

    placeholder = {
        "type": "statement",
        "rank": "normal",
        "_phase3a_pending_creator_qid": "Q56159174",
        "_permalink": "https://commons.wikimedia.org/w/index.php?oldid=1",
    }
    # Entity has only a somevalue P170 (DPLA's stated-as name shape),
    # no QID — the community QID is genuinely new information.
    entity = {
        "statements": {
            "P170": [
                {
                    "mainsnak": {
                        "snaktype": "somevalue",
                        "property": "P170",
                    }
                }
            ]
        }
    }
    materialised = materialize_import_claims([placeholder], existing_entity=entity)
    assert len(materialised) == 1
    claim = materialised[0]
    assert claim["mainsnak"]["property"] == "P170"
    assert claim["mainsnak"]["snaktype"] == "value"
    dv = claim["mainsnak"]["datavalue"]
    assert dv["type"] == "wikibase-entityid"
    assert dv["value"]["id"] == "Q56159174"
    # Inferred-from-Wikitext reference shape must be intact.
    refs = claim["references"][0]["snaks"]
    assert refs["P887"][0]["datavalue"]["value"]["id"] == "Q131783016"


def test_materialize_creator_page_placeholder_falls_back_to_stated_as_when_no_qid():
    """A ``{{Creator:Foo}}`` transclusion whose Creator: page has no
    linked Wikidata item (orphaned page, or the page doesn't exist)
    falls back to a P170 somevalue + P2093 stated-as claim using the
    page title as the name. Preserves the community contribution as
    a stated-as string rather than dropping it silently.

    The resolver is stubbed to return None so the fallback path fires
    without requiring a live Commons API call."""
    from ingest_wikimedia.legacy_artwork import materialize_import_claims

    placeholder = {
        "type": "statement",
        "rank": "normal",
        "_phase3a_pending_creator_page": "Orphaned Person",
        "_permalink": "https://commons.wikimedia.org/w/index.php?oldid=1",
    }
    # ``site=None`` short-circuits ``_resolve_commons_creator_qid`` to
    # None, exercising the stated-as fallback without a network call.
    materialised = materialize_import_claims([placeholder], site=None)
    assert len(materialised) == 1
    claim = materialised[0]
    assert claim["mainsnak"]["property"] == "P170"
    assert claim["mainsnak"]["snaktype"] == "somevalue"
    stated_as = claim["qualifiers"]["P2093"][0]["datavalue"]["value"]
    assert stated_as == "Orphaned Person", (
        f"expected stated-as to fall back to the page title; got {stated_as!r}"
    )
    refs = claim["references"][0]["snaks"]
    assert refs["P887"][0]["datavalue"]["value"]["id"] == "Q131783016"


def test_plan_migration_extracts_infi_creator_with_wikidata_qid():
    """Integration: a legacy Artwork upload with a community
    contribution wrapped in ``Other fields 1 = {{InFi|Creator|
    {{creator|Wikidata=Q…}}}}`` must produce a plan whose
    community_imports carries the QID sentinel — which then
    materialises into a P170 QID claim at execute time."""
    from ingest_wikimedia.legacy_artwork import (
        plan_migration,
        _CREATOR_QID_PREFIX,
    )

    revs = _make_revs(
        (
            1,
            "DPLA_bot",
            "{{Artwork\n| Other fields 1 = {{ InFi | Creator | "
            "Peiser, Theodore E | id=fileinfotpl_aut }}\n}}",
        ),
        (
            2,
            "Community_editor",
            "{{Artwork\n| Other fields 1 = {{ InFi | Creator | "
            "{{creator|Wikidata=Q56159174}} | id=fileinfotpl_aut }}\n}}",
        ),
    )
    plan = plan_migration(
        "File:Foo.jpg",
        revs,
        _canonical_params(creator="Peiser, Theodore E"),
    )
    assert plan is not None
    assert plan.community_imports.get("creator") == (_CREATOR_QID_PREFIX + "Q56159174")


def test_parse_creator_shape_matches_creator_page_case_insensitively():
    """MediaWiki auto-capitalises the ``Creator:`` namespace on
    template transclusions, so ``{{creator:Foo}}`` and ``{{Creator:Foo}}``
    both resolve to the same page. The parser matches both."""
    from ingest_wikimedia.legacy_artwork import (
        _parse_creator_shape,
        _CREATOR_PAGE_PREFIX,
    )

    assert _parse_creator_shape("{{creator:Theodore E. Peiser}}") == (
        _CREATOR_PAGE_PREFIX + "Theodore E. Peiser"
    )
    assert _parse_creator_shape("{{CREATOR:Theodore E. Peiser}}") == (
        _CREATOR_PAGE_PREFIX + "Theodore E. Peiser"
    )


def test_parse_creator_shape_strips_unknown_template_shapes():
    """Any template-shaped value that doesn't match one of the
    recognised creator shapes is dropped rather than passed through
    as a literal string. Submitting raw ``{{…}}`` markup as a P2093
    stated-as would be both wrong (SDC shouldn't store wikitext) and
    a maintenance burden to reverse later. Deferring to a later phase
    that widens the recognised set is safer than emitting nonsense
    claims now."""
    from ingest_wikimedia.legacy_artwork import _parse_creator_shape

    # An unknown creator-authoring template shape.
    assert _parse_creator_shape("{{some-unknown-creator-template|Foo Bar}}") is None
    # A partially-typed Creator: shape that misses ``_CREATOR_PAGE_RE``
    # (contains pipe → treated as a template with args rather than a
    # bare page transclusion). Better to strip than to emit
    # ``P2093="{{Creator:Foo|extra}}"``.
    assert _parse_creator_shape("{{Creator:Foo|extra}}") is None


def test_parse_creator_shape_passes_through_plain_names():
    """Regression: plain-string creator names still pass through
    unchanged. The unknown-template guard only fires on ``{{…}}``-
    wrapped values."""
    from ingest_wikimedia.legacy_artwork import _parse_creator_shape

    assert _parse_creator_shape("Peiser, Theodore E") == "Peiser, Theodore E"
    # A name that happens to contain balanced braces mid-string (not a
    # template) must still pass through — the guard checks bounds, not
    # substring occurrence.
    assert _parse_creator_shape("Smith, John (author)") == "Smith, John (author)"
