"""Tests for ``ingest_wikimedia/sdc.py`` helpers.

Focused initially on ``parse_nara_access_level``: the helper that
replaced a ``BeautifulSoup(..., "xml")`` call whose silent fallback
(when lxml was missing on the runtime host) caused the SDC reconciler
to strip valid P7228 / P6224 claims off thousands of NARA Commons
files on 2026-06-07. The stdlib ElementTree implementation removes
the runtime dependency and surfaces parse failures loudly.
"""

import xml.etree.ElementTree as ET

import pytest

from ingest_wikimedia.sdc import (
    NARA_ACCESS_CODES,
    NARA_LEVELS,
    Q_NARA_FILE_UNIT,
    Q_NARA_ITEM,
    _build_date_claim,
    _build_source_claim,
    parse_dpla_date,
    parse_nara_access_level,
)

# Sample NARA originalRecord XML — namespaced, structure matching the
# real NARA RDX records the bot processes. Production values come from
# ``doc["originalRecord"]["stringValue"]`` on a DPLA API response.
NARA_XML_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<{root_tag} xmlns="http://description.das.nara.gov/">
  {access_block}
  <coverageDates>
    <coverageEndDate>
      <year>1945</year>
    </coverageEndDate>
  </coverageDates>
</{root_tag}>"""

VALID_ACCESS_BLOCK = """<accessRestriction>
    <status>
      <naId>10031403</naId>
      <termName>Unrestricted</termName>
    </status>
  </accessRestriction>"""


def _nara_xml(root_tag="item", access_block=VALID_ACCESS_BLOCK):
    return NARA_XML_TEMPLATE.format(root_tag=root_tag, access_block=access_block)


def test_valid_nara_item_extracts_access_and_level():
    """The canonical happy path the production WWII-era propaganda image
    (DPLA id f1f5bf26…) takes: <item> root with an accessRestriction →
    P7228 = Q66739888 (unrestricted), P6224 = Q11723795 (item)."""
    access, level = parse_nara_access_level(_nara_xml())
    assert access == "Q66739888"
    assert level == Q_NARA_ITEM


def test_itemAv_root_maps_to_item_level():
    """itemAv and item both map to Q_NARA_ITEM by design (NARA_LEVELS)."""
    access, level = parse_nara_access_level(_nara_xml(root_tag="itemAv"))
    assert level == Q_NARA_ITEM


def test_fileUnit_root_maps_to_file_unit_level():
    access, level = parse_nara_access_level(_nara_xml(root_tag="fileUnit"))
    assert level == Q_NARA_FILE_UNIT


def test_unknown_root_tag_yields_empty_level_but_extracts_access():
    """A NARA record whose root tag isn't in NARA_LEVELS returns
    ``""`` for level (legitimate empty, distinct from a parse
    failure) but still extracts access from the descendant
    accessRestriction block — the two fields are independent."""
    access, level = parse_nara_access_level(_nara_xml(root_tag="series"))
    assert access == "Q66739888"
    assert level == ""


def test_descendant_level_tag_does_not_overwrite_root_classification():
    """Regression guard against the prior 'last match wins' behavior:
    an <item> root containing a stray <fileUnit> descendant must stay
    classified as Q_NARA_ITEM, not get reclassified as fileUnit by
    descendant-iteration order. The BS4 code had this defect; the
    rewrite pins level to the root tag explicitly."""
    block = (
        "<accessRestriction><status><naId>10031403</naId></status>"
        "</accessRestriction><fileUnit/>"
    )
    _, level = parse_nara_access_level(_nara_xml(access_block=block))
    assert level == Q_NARA_ITEM


def test_first_accessRestriction_with_naid_is_selected():
    """If a NARA record has multiple accessRestriction blocks (rare but
    seen in upstream data), the XPath returns the first complete
    a/s/naId chain in document order. Lock the contract so a future
    rewrite doesn't silently flip it."""
    block = (
        "<accessRestriction><status></status></accessRestriction>"
        "<accessRestriction><status><naId>10031403</naId></status></accessRestriction>"
    )
    access, _ = parse_nara_access_level(_nara_xml(access_block=block))
    assert access == "Q66739888"


def test_missing_access_restriction_yields_empty_access():
    """No accessRestriction element → access = "" (legitimate empty)."""
    access, _ = parse_nara_access_level(_nara_xml(access_block=""))
    assert access == ""


def test_missing_naid_inside_status_yields_empty_access():
    """accessRestriction present but with no <naId> → access = "" (no raise)."""
    access, _ = parse_nara_access_level(
        _nara_xml(
            access_block="<accessRestriction><status></status></accessRestriction>"
        )
    )
    assert access == ""


def test_unknown_access_naid_yields_empty_access():
    """A naId that isn't in NARA_ACCESS_CODES (e.g. NARA adding a new
    code) → access = "" rather than a crash. The bot should add the
    code mapping when this happens, but in the meantime the missing
    P7228 claim must not be confused with a parse failure."""
    block = (
        "<accessRestriction><status><naId>99999999</naId></status></accessRestriction>"
    )
    access, _ = parse_nara_access_level(_nara_xml(access_block=block))
    assert access == ""


def test_naid_whitespace_is_stripped():
    """NARA-supplied naId text may have leading/trailing whitespace from
    pretty-printed XML — the canonical codes table has bare digits."""
    block = "<accessRestriction><status><naId>  10031403  </naId></status></accessRestriction>"
    access, _ = parse_nara_access_level(_nara_xml(access_block=block))
    assert access == "Q66739888"


def test_no_xmlns_still_parses():
    """The {*} wildcard in the find() expressions must match elements
    with no xmlns too (defensive: NARA might one day strip the
    namespace, and the production code shouldn't silently start
    returning empty)."""
    no_ns = (
        "<item><accessRestriction><status><naId>10031403</naId></status>"
        "</accessRestriction></item>"
    )
    access, level = parse_nara_access_level(no_ns)
    assert access == "Q66739888"
    assert level == Q_NARA_ITEM


def test_malformed_xml_raises_parse_error():
    """The whole point of the rewrite: a parse failure MUST propagate,
    not be silently swallowed into ``("","")``. Catching it and
    defaulting is what let the bot strip valid P7228/P6224 claims
    off thousands of files on 2026-06-07 — the silent-failure
    pattern this signature explicitly rules out."""
    with pytest.raises(ET.ParseError):
        parse_nara_access_level("<item><not closed>")


def test_empty_string_raises_parse_error():
    """An empty originalRecord stringValue is a real upstream data
    problem (NARA item that lacks the expected XML payload). Surface
    it; do not silently treat as empty access/level."""
    with pytest.raises(ET.ParseError):
        parse_nara_access_level("")


def test_all_documented_access_codes_round_trip():
    """Every code in NARA_ACCESS_CODES must round-trip through the
    helper. Regression guard against the table or the XPath drifting
    out of sync."""
    for naid, expected_qid in NARA_ACCESS_CODES.items():
        block = (
            f"<accessRestriction><status><naId>{naid}</naId>"
            "</status></accessRestriction>"
        )
        access, _ = parse_nara_access_level(_nara_xml(access_block=block))
        assert access == expected_qid, (
            f"naId {naid} expected {expected_qid}, got {access}"
        )


def test_all_documented_levels_round_trip():
    """Every key in NARA_LEVELS must be recognised as a root tag."""
    for lvl_key, expected_qid in NARA_LEVELS.items():
        _, level = parse_nara_access_level(_nara_xml(root_tag=lvl_key))
        assert level == expected_qid, (
            f"root {lvl_key} expected {expected_qid}, got {level}"
        )


# ---------------------------------------------------------------------------
# parse_dpla_date — opportunistic structured time extraction from DPLA's
# free-text displayDate strings. A None return is a feature: the claim
# builder falls back to somevalue + P1932 stated-as, preserving the
# original DPLA prose for the wiki template to render verbatim.
# ---------------------------------------------------------------------------


def test_parse_year_only():
    """The most common DPLA date shape — no decorators, so not approximate."""
    parsed = parse_dpla_date("1945")
    assert parsed["value"] == {
        "time": "+1945-01-01T00:00:00Z",
        "precision": 9,
        "before": 0,
        "after": 0,
        "timezone": 0,
        "calendarmodel": "http://www.wikidata.org/entity/Q1985727",
    }
    assert parsed["approximate"] is False


def test_parse_year_month():
    parsed = parse_dpla_date("1945-06")
    assert parsed["value"]["time"] == "+1945-06-01T00:00:00Z"
    assert parsed["value"]["precision"] == 10
    assert parsed["approximate"] is False


def test_parse_full_iso_date():
    parsed = parse_dpla_date("1945-06-07")
    assert parsed["value"]["time"] == "+1945-06-07T00:00:00Z"
    assert parsed["value"]["precision"] == 11
    assert parsed["approximate"] is False


def test_parse_decade():
    """``1940s`` → precision 8, time pinned to decade-start. Decade
    precision is structural (it says "this happened in the 1940s"), NOT
    "approximate" in the P1480 sense — so ``approximate`` stays False."""
    parsed = parse_dpla_date("1940s")
    assert parsed["value"]["time"] == "+1940-01-01T00:00:00Z"
    assert parsed["value"]["precision"] == 8
    assert parsed["approximate"] is False


def test_parse_decade_rejects_non_decade_year():
    """``1945s`` is almost certainly a typo for ``1945``. Refuse to
    silently coerce it to decade precision."""
    assert parse_dpla_date("1945s") is None


def test_parse_circa_marks_approximate():
    """``circa`` (and equivalents) carry uncertainty into the structured
    claim via the ``approximate`` flag — the caller stamps
    ``P1480 = Q5727902`` on the resulting claim."""
    parsed = parse_dpla_date("circa 1945")
    assert parsed["value"]["time"] == "+1945-01-01T00:00:00Z"
    assert parsed["value"]["precision"] == 9
    assert parsed["approximate"] is True


def test_parse_strips_c_ca_approximately_tilde_marks_approximate():
    """All circa-equivalent prefixes set the approximate flag."""
    for src in ("c. 1945", "ca. 1945", "approximately 1945", "approx. 1945", "~1945"):
        parsed = parse_dpla_date(src)
        assert parsed["value"]["time"] == "+1945-01-01T00:00:00Z", src
        assert parsed["approximate"] is True, (
            f"{src!r} should mark the date approximate"
        )


def test_parse_brackets_and_question_mark_mark_approximate():
    """Archival convention: bracketed dates (``[1945]``) and
    trailing-question-mark dates (``1945?``) are both inexact —
    treat them as approximate. Nested decorators collapse
    iteratively."""
    for src in ("[1945]", "[1945?]", "1945?"):
        parsed = parse_dpla_date(src)
        assert parsed["value"]["time"] == "+1945-01-01T00:00:00Z", src
        assert parsed["approximate"] is True, src


def test_parse_rejects_range():
    """``1945-1950`` is a range, not a single date. Wikibase has no
    canonical single-time representation; fall back to somevalue+P1932
    so the original string is preserved."""
    assert parse_dpla_date("1945-1950") is None


def test_parse_rejects_free_prose():
    """The single biggest motivation for the somevalue fallback — DPLA
    carries a long tail of un-coercable date strings that must still
    surface in the template."""
    assert parse_dpla_date("During the Gilded Age") is None
    assert parse_dpla_date("could not be determined") is None
    assert parse_dpla_date("unknown") is None


def test_parse_rejects_year_zero():
    """Proleptic Gregorian has no year zero; defensive guard."""
    assert parse_dpla_date("0") is None
    assert parse_dpla_date("0000") is None


def test_parse_rejects_bc_year():
    """BC years are out of the year-only regex's scope — fall back so
    the original string survives. Could be added later if a hub
    needs it."""
    assert parse_dpla_date("-500") is None
    assert parse_dpla_date("500 BC") is None


def test_parse_rejects_invalid_iso_date():
    """``datetime.date`` validation catches Feb 30 / month > 12; the
    less-precise regexes don't accept this shape either, so the
    result is None — not a silent fallback to a coarser precision."""
    assert parse_dpla_date("2024-02-30") is None
    assert parse_dpla_date("2024-13-01") is None


def test_parse_rejects_empty_and_whitespace():
    assert parse_dpla_date("") is None
    assert parse_dpla_date("   ") is None
    assert parse_dpla_date(None) is None


def test_parse_falls_back_when_residue_has_unrecognised_text():
    """Conservative-fallback contract: any text the parser can't map to
    either a recognised precision (year/month/day/decade) OR a
    recognised qualifier (the circa-mappable decorators) MUST cause
    the whole parse to fall back to ``None``. Otherwise we'd silently
    strip meaningful text (era markers, month names, parens, "before",
    free prose adjacent to a year) and emit a structured date that
    doesn't actually represent the source.

    The builder turns ``None`` into a ``somevalue + P1932`` claim with
    the verbatim DPLA string preserved — so the template still
    renders the original prose. Zero false-precision risk."""
    cases = [
        # Era markers — not stripped, not recognised in any regex.
        "1945 AD",
        "AD 1945",
        "1945 BCE",
        # Range-y / multi-date prose.
        "1945 or 1946",
        "1945 and 1946",
        "before 1945",
        "after 1945",
        "between 1945 and 1950",
        # Season markers — not stripped. Month names ARE now handled
        # separately (see test_parse_month_name_forms) so they are
        # correctly excluded from the fall-back list.
        "Spring 1945",
        "summer 1945",
        # Parenthesised forms — parens not in the decorator list.
        "(1945)",
        "1945 (uncertain)",
        "1945 (approximate)",
        # Recognised decorator + UNRECOGNISED residue.
        "circa unknown",
        "ca. summer 1945",
        "approximately 1945-1950",
        "[1945 to 1950]",
        # Numeric residue that ISN'T a date.
        "1945.5",
        "v.1945",
        "1945abc",
        # Decade with trailing non-decade text.
        "1940s and 1950s",
    ]
    for src in cases:
        assert parse_dpla_date(src) is None, (
            f"{src!r} should fall back to somevalue, not produce a structured date"
        )


def test_build_date_claim_emits_value_typed_when_parseable():
    """Parser succeeds, no decorators → value-typed claim, NO P1480
    qualifier (the date is definite). P1932 always carries the
    original DPLA string."""
    import datetime as _dt

    claim = _build_date_claim("1945", "abc123", _dt.date(2026, 6, 7))
    assert claim["mainsnak"]["snaktype"] == "value"
    assert claim["mainsnak"]["datavalue"]["type"] == "time"
    assert claim["mainsnak"]["datavalue"]["value"]["precision"] == 9
    assert claim["qualifiers"]["P1932"][0]["datavalue"]["value"] == "1945"
    assert "P1480" not in claim["qualifiers"]


def test_build_date_claim_stamps_p1480_for_circa():
    """Parser flagged the date as approximate → claim gets a P1480
    qualifier with value Q5727902 (circa), per Wikidata Help:Dates
    convention for inexact dates. The structured time value AND the
    P1480 marker together represent "around 1945" in a Wikidata-shaped
    way; P1932 still carries the verbatim source string."""
    import datetime as _dt

    claim = _build_date_claim("circa 1945", "abc123", _dt.date(2026, 6, 7))
    assert claim["mainsnak"]["snaktype"] == "value"
    assert claim["mainsnak"]["datavalue"]["value"]["precision"] == 9
    # P1932 preserves the verbatim source decoration.
    assert claim["qualifiers"]["P1932"][0]["datavalue"]["value"] == "circa 1945"
    # P1480 = Q5727902 (circa). _item_value uses numeric-id encoding.
    p1480 = claim["qualifiers"]["P1480"][0]
    assert p1480["property"] == "P1480"
    assert p1480["datavalue"]["type"] == "wikibase-entityid"
    assert p1480["datavalue"]["value"]["numeric-id"] == 5727902
    assert p1480["datavalue"]["value"]["entity-type"] == "item"


def test_build_date_claim_stamps_p1480_for_bracketed_and_question_mark():
    """Brackets and trailing ? both indicate inexact dates → P1480."""
    import datetime as _dt

    for src in ("[1945]", "1945?", "[1945?]"):
        claim = _build_date_claim(src, "abc123", _dt.date(2026, 6, 7))
        assert "P1480" in claim["qualifiers"], (
            f"{src!r} should produce a P1480 qualifier"
        )
        assert (
            claim["qualifiers"]["P1480"][0]["datavalue"]["value"]["numeric-id"]
            == 5727902
        ), src


def test_build_date_claim_no_p1480_for_decade():
    """Decade precision is structural, not "approximate" in the P1480
    sense — ``1940s`` stays without P1480."""
    import datetime as _dt

    claim = _build_date_claim("1940s", "abc123", _dt.date(2026, 6, 7))
    assert claim["mainsnak"]["datavalue"]["value"]["precision"] == 8
    assert "P1480" not in claim["qualifiers"]


def test_build_date_claim_falls_back_to_somevalue_when_unparseable():
    """somevalue + P1932 fallback preserves DPLA's exact prose."""
    import datetime as _dt

    claim = _build_date_claim("During the Gilded Age", "abc123", _dt.date(2026, 6, 7))
    assert claim["mainsnak"]["snaktype"] == "somevalue"
    assert "datavalue" not in claim["mainsnak"]
    assert (
        claim["qualifiers"]["P1932"][0]["datavalue"]["value"] == "During the Gilded Age"
    )


def test_build_date_claim_returns_none_for_empty_input():
    """Matches the pre-existing contract so the caller's
    ``if c is not None`` loop short-circuits cleanly."""
    import datetime as _dt

    assert _build_date_claim("", "abc123", _dt.date(2026, 6, 7)) is None
    assert _build_date_claim("   ", "abc123", _dt.date(2026, 6, 7)) is None


# ---------------------------------------------------------------------------
# parse_dpla_date — natural-language month/year and slash-form
# extensions. Motivating example: M174777532 (indiana partner) has a
# DPLA-attributed P571 stored as ``somevalue + P1932="June 1959"``, whose
# comparable key would not match the inferred-from-Wikitext value-typed
# time claim for the same date until parse_dpla_date learned month
# names.
# ---------------------------------------------------------------------------


def test_parse_month_name_year_forms():
    """``Month YYYY`` variants → month precision (10)."""
    for src in (
        "June 1959",
        "june 1959",
        "Jun 1959",
        "JUN 1959",
        "Sept 1912",
        "sept. 1912",
        "September 1912",
        "May 1900",
    ):
        r = parse_dpla_date(src)
        assert r is not None, f"{src!r} should now parse to a structured month"
        v = r["value"]
        assert v["precision"] == 10, f"{src!r} → wrong precision {v['precision']}"
        assert not r["approximate"]
        # Time string always +YYYY-MM-01T00:00:00Z
        assert v["time"].endswith("-01T00:00:00Z"), f"{src!r} time={v['time']}"


def test_parse_month_day_year_forms():
    """``Month D, YYYY`` and ``Month D YYYY`` → day precision (11)."""
    for src in (
        "November 19, 1902",
        "November 19 1902",
        "Nov 19, 1902",
        "nov. 19 1902",
        "March 3, 1900",
    ):
        r = parse_dpla_date(src)
        assert r is not None, f"{src!r} should now parse to a structured day"
        v = r["value"]
        assert v["precision"] == 11, f"{src!r} → wrong precision"
        assert not r["approximate"]


def test_parse_day_month_year_forms():
    """``D Month YYYY`` (British / scholarly) → day precision (11).
    Motivating example: M105419621's ``| date = 19 November 1902``
    override must parse to the same time as DPLA's ``1902-11-19``."""
    r = parse_dpla_date("19 November 1902")
    assert r is not None
    assert r["value"]["precision"] == 11
    assert r["value"]["time"] == "+1902-11-19T00:00:00Z"


def test_parse_us_slash_date_form():
    """``MM/DD/YYYY`` → day precision. Common in US-hub-supplied dates."""
    r = parse_dpla_date("11/19/1902")
    assert r is not None
    assert r["value"]["precision"] == 11
    assert r["value"]["time"] == "+1902-11-19T00:00:00Z"


def test_parse_month_name_forms_reject_invalid_calendar_dates():
    """``February 30, 1902`` etc. must NOT silently parse — the
    calendar check runs on every day-precision path so a phantom
    date can't sneak through."""
    for src in ("February 30, 1902", "13/40/1900", "November 31 1902"):
        assert parse_dpla_date(src) is None, f"{src!r} should not parse"


def test_parse_month_name_forms_reject_year_zero():
    """Year zero is invalid in the proleptic Gregorian calendar; the
    month-name paths honour the same guard as the existing regexes."""
    for src in ("June 0", "November 19, 0", "19 November 0"):
        assert parse_dpla_date(src) is None


def test_parse_month_name_forms_reject_adjacent_text():
    """A recognised month + year must not silently absorb trailing
    prose. Regex is fully anchored so the parser refuses anything
    outside the exact shape."""
    for src in (
        "January 1945 or 1946",
        "Jan 1945 AD",
        "circa Jan 1945 (uncertain)",
        "November 19, 1902 (approximately)",
    ):
        assert parse_dpla_date(src) is None, f"{src!r} should NOT parse"


def test_parse_month_name_forms_carry_approximate_flag():
    """Circa / bracket / question-mark decorators around a
    month-name date correctly propagate the approximate flag."""
    r = parse_dpla_date("circa June 1959")
    assert r is not None and r["approximate"] is True
    r = parse_dpla_date("[19 November 1902]")
    assert r is not None and r["approximate"] is True


# ---------------------------------------------------------------------------
# casefold_for_compare — comparator-only text normaliser. Motivating
# examples: M114630785 (trailing "." on DPLA-side description) and
# M100761231 (wrapping "[…]" brackets on DPLA-side title).
# ---------------------------------------------------------------------------


def test_casefold_for_compare_trims_leading_and_trailing_punctuation():
    """Wrapping brackets / parens / quotes on either side collapse."""
    from ingest_wikimedia.sdc import casefold_for_compare

    # Trailing period (M114630785 case).
    assert casefold_for_compare(
        "A.D. Abbott, Hancock N.H. L.M. Stearns Collection."
    ) == casefold_for_compare("A.D. Abbott, Hancock N.H. L.M. Stearns Collection")
    # Wrapping brackets (M100761231 case).
    assert casefold_for_compare(
        "[Promissory note for Thomas Love]"
    ) == casefold_for_compare("Promissory note for Thomas Love")
    # Curly-quote wrappers and paren wrappers.
    assert casefold_for_compare("“A title”") == casefold_for_compare("A title")
    assert casefold_for_compare("(1902)") == casefold_for_compare("1902")


def test_casefold_for_compare_folds_case_but_not_internal_punctuation():
    """Case-fold turns ``ABC`` == ``abc``, but internal punctuation is
    preserved — so ``"A.D. Abbott"`` and ``"A D Abbott"`` do NOT
    collapse. Rationale: the reconciler is one-way (removes inferred
    claims only), but conservative internal-punctuation handling still
    protects the rare case where internal punctuation is meaningful
    (e.g. a stringy inventory number)."""
    from ingest_wikimedia.sdc import casefold_for_compare

    assert casefold_for_compare("ABC def") == casefold_for_compare("abc DEF")
    # Internal punctuation NOT collapsed.
    assert casefold_for_compare("A.D. Abbott") != casefold_for_compare("A D Abbott")


def test_casefold_for_compare_collapses_internal_whitespace():
    """Runs of whitespace fold to a single space — an editor's
    accidental double-space or newline in a wikitext value doesn't
    survive as a mismatch."""
    from ingest_wikimedia.sdc import casefold_for_compare

    assert casefold_for_compare("A  B  C") == casefold_for_compare("A B C")
    assert casefold_for_compare("A\tB\nC") == casefold_for_compare("A B C")


def test_casefold_for_compare_handles_falsy_input():
    """Non-string / empty input returns an empty string — callers that
    compare two folded keys treat that as unequal-to-any-real-value
    (an empty key from a malformed claim should never accidentally
    dedup against another empty)."""
    from ingest_wikimedia.sdc import casefold_for_compare

    assert casefold_for_compare("") == ""
    assert casefold_for_compare(None) == ""  # type: ignore[arg-type]
    assert casefold_for_compare("   .,  ") == ""


def test_casefold_for_compare_unescapes_wikitext_magic_words():
    """A community AWB pass sometimes rewrites literal ``|`` inside a
    template param to the ``{{!}}`` magic word (the parser expands it
    back to ``|`` at render time). If the escaped form leaks into
    stored SDC and DPLA's canonical carries the literal, byte- and
    casefold-comparison fail on a display-invariant transform. The
    comparator must un-escape.

    Motivating example: File:Block_Card_6_E._Bancroft_Street_-_DPLA_-
    _307d98570261183ed48eb3b1880fce14.jpg — description migrated to
    P10358 with literal ``{{!}}`` between terms, but DPLA canonical
    description carries ``|``.
    """
    from ingest_wikimedia.sdc import casefold_for_compare

    dpla = (
        "Descriptive terms related to this photograph include: "
        "commercial buildings | Italianate | one story"
    )
    community = (
        "Descriptive terms related to this photograph include: "
        "commercial buildings {{!}} Italianate {{!}} one story"
    )
    assert casefold_for_compare(dpla) == casefold_for_compare(community)

    # ``{{=}}`` and the bracket variants land the same way.
    assert casefold_for_compare("a = b") == casefold_for_compare("a {{=}} b")
    assert casefold_for_compare("{{x}}") == casefold_for_compare("{{((}}x{{))}}")


def test_unescape_wikitext_magic_words_leaves_regular_text_alone():
    """The helper is pure substitution of the documented magic words —
    ordinary text (including stray braces that aren't part of a magic-
    word) passes through unchanged."""
    from ingest_wikimedia.sdc import unescape_wikitext_magic_words

    assert unescape_wikitext_magic_words("plain text") == "plain text"
    assert unescape_wikitext_magic_words("{{cite}}") == "{{cite}}"
    assert unescape_wikitext_magic_words("") == ""
    assert unescape_wikitext_magic_words(None) is None  # type: ignore[arg-type]


def test_is_wikitext_junk_value_flags_short_punctuation_only():
    """1-2 characters that are all punctuation/whitespace read as a
    wikitext-extraction artifact rather than metadata worth keeping."""
    from ingest_wikimedia.sdc import is_wikitext_junk_value

    assert is_wikitext_junk_value(";")
    assert is_wikitext_junk_value(" ; ")
    assert is_wikitext_junk_value("--")
    assert is_wikitext_junk_value(".")
    assert is_wikitext_junk_value(",,")
    assert is_wikitext_junk_value("()")


def test_is_wikitext_junk_value_lets_content_through():
    """Anything with a letter or digit — or 3+ characters — is
    considered potentially-legitimate and passes through unfiltered."""
    from ingest_wikimedia.sdc import is_wikitext_junk_value

    # Single letters / digits are legitimate values (title "A", etc.).
    assert not is_wikitext_junk_value("A")
    assert not is_wikitext_junk_value("1")
    assert not is_wikitext_junk_value("1.")
    # 3+ punctuation is likely a stylistic choice / ellipsis proxy —
    # not caught by this narrow filter.
    assert not is_wikitext_junk_value("---")
    assert not is_wikitext_junk_value("...")
    assert not is_wikitext_junk_value("1937")
    assert not is_wikitext_junk_value("A Title")


def test_is_wikitext_junk_value_handles_falsy_input():
    from ingest_wikimedia.sdc import is_wikitext_junk_value

    assert not is_wikitext_junk_value("")
    assert not is_wikitext_junk_value("   ")
    assert not is_wikitext_junk_value(None)  # type: ignore[arg-type]


def test_unescape_wikitext_magic_words_covers_documented_forms():
    """All the character-escape magic words documented on
    https://www.mediawiki.org/wiki/Help:Magic_words expand to the
    literal char they render as."""
    from ingest_wikimedia.sdc import unescape_wikitext_magic_words

    assert unescape_wikitext_magic_words("a{{!}}b") == "a|b"
    assert unescape_wikitext_magic_words("a{{=}}b") == "a=b"
    assert unescape_wikitext_magic_words("{{((}}x{{))}}") == "{{x}}"
    # {{!(}} and {{)!}} are the community table-start / table-end
    # escapes used to keep a nested wikitable's `{|` / `|}` tokens
    # from being consumed by the outer template's argument parser.
    assert unescape_wikitext_magic_words("{{!(}}row{{)!}}") == "{|row|}"


def test_unescape_wikitext_magic_words_is_single_pass():
    """``{{((}}))}}`` is the community idiom for a literal ``{{}}``:
    the ``{{((}}`` token expands to ``{{`` and the trailing ``))}}``
    stays literal because it's not a magic-word token on its own —
    ``{{))}}`` (with the leading ``{{``) is, but the tail alone is
    not. A sequential-replace implementation would first swap
    ``{{((}}`` for ``{{`` and then rescan its own output, matching a
    spurious ``{{))}}`` and collapsing the whole thing to ``}}``.
    Single-pass regex substitution matches each source token exactly
    once against the original string.
    """
    from ingest_wikimedia.sdc import unescape_wikitext_magic_words

    assert unescape_wikitext_magic_words("{{((}}))}}") == "{{))}}"
    # Adjacent magic words unescape independently — sequential replace
    # would still get this right, but the case pins the intent.
    assert unescape_wikitext_magic_words("{{!}}{{!}}") == "||"


# ---------------------------------------------------------------------------
# dates_semantically_equal — semantic date equivalence for
# ``{{DPLA metadata}}`` ``date =`` overrides that reformat DPLA's own
# supplied date. Motivating example: M105419621 with
# ``| date = 19 November 1902`` alongside canonical ``1902-11-19``.
# ---------------------------------------------------------------------------


def test_dates_semantically_equal_matches_across_formats():
    """Cross-format equivalence: month-name day form, US slash form,
    and canonical ISO all fold to the same time+precision."""
    from ingest_wikimedia.sdc import dates_semantically_equal

    assert dates_semantically_equal("19 November 1902", "1902-11-19")
    assert dates_semantically_equal("November 19, 1902", "1902-11-19")
    assert dates_semantically_equal("11/19/1902", "1902-11-19")
    assert dates_semantically_equal("June 1959", "1959-06")


def test_dates_semantically_equal_rejects_different_precisions():
    """Same year but different precisions must NOT match — a P571
    claim at year precision and one at day precision are different
    facts even when the day one falls inside the year."""
    from ingest_wikimedia.sdc import dates_semantically_equal

    assert not dates_semantically_equal("1902", "1902-11-19")
    assert not dates_semantically_equal("1902-11", "1902-11-19")


def test_dates_semantically_equal_respects_circa_flag():
    """A circa-decorated date and a plain date at the same time are
    NOT semantically equal — the circa flag drives the P1480
    qualifier and represents a distinct uncertainty semantic."""
    from ingest_wikimedia.sdc import dates_semantically_equal

    assert not dates_semantically_equal("circa 1902-11-19", "1902-11-19")
    assert dates_semantically_equal("circa 1902-11-19", "circa November 19, 1902")


def test_dates_semantically_equal_returns_false_on_unparseable():
    """Anything the parser can't structure returns False — never a
    false positive from two None-returns colliding."""
    from ingest_wikimedia.sdc import dates_semantically_equal

    assert not dates_semantically_equal("some date", "another date")
    assert not dates_semantically_equal("", "1902-11-19")
    assert not dates_semantically_equal("1902-11-19", "")


# ---------------------------------------------------------------------------
# _build_source_claim — P7482 (described at) with its qualifier bundle.
# P6108 (IIIF manifest URL) is the new per-item qualifier in this PR;
# P2699 is per-ordinal and gets materialized in sdc-sync, NOT here.
# ---------------------------------------------------------------------------


def test_build_source_claim_omits_p6108_when_no_iiif_manifest():
    """Items without iiifManifest in the source doc → no P6108 qualifier.
    Default behavior — preserves the pre-PR shape for the long tail of
    non-IIIF partners."""
    import datetime as _dt

    claim = _build_source_claim(
        "Q12345", "https://example.org/item/1", "abc", _dt.date(2026, 6, 7)
    )
    quals = claim["qualifiers"]
    assert "P973" in quals  # described-at URL still there
    assert "P137" in quals  # operator still there
    assert "P6108" not in quals


def test_build_source_claim_stamps_p6108_when_iiif_manifest_provided():
    """When the source carries a IIIF manifest URL, build it into the
    P7482 statement as a P6108 qualifier (per-item). sdc.json now carries
    the value; sdc-sync handles backfill of existing P7482 statements
    via ``_amend_p7482_url_qualifiers``."""
    import datetime as _dt

    manifest_url = "https://example.org/iiif/abc/manifest.json"
    claim = _build_source_claim(
        "Q12345",
        "https://example.org/item/1",
        "abc",
        _dt.date(2026, 6, 7),
        iiif_manifest_url=manifest_url,
    )
    p6108 = claim["qualifiers"]["P6108"][0]
    assert p6108["property"] == "P6108"
    assert p6108["datavalue"]["value"] == manifest_url
    assert p6108["datavalue"]["type"] == "string"
    assert p6108["datatype"] == "url"


def test_build_claims_for_doc_rejects_junky_iiif_manifest_values():
    """``doc["iiifManifest"]`` arrives from upstream DPLA records that
    occasionally carry junk — literal string "null", whitespace, schemes
    we don't recognise. Stamping any of those as P6108 on Commons would
    require manual cleanup. Pin the defense: only stamp when the value
    is a real http(s) URL after stripping."""
    from ingest_wikimedia.sdc import build_claims_for_doc

    def _doc(iiif_value):
        return {
            "id": "abc1234567890",
            "ingestDate": "2026-06-07T00:00:00Z",
            "provider": {"name": "Digital Commonwealth"},
            "dataProvider": {"name": "Boston Public Library"},
            "sourceResource": {
                "title": ["A title"],
                "date": [{"displayDate": "1945"}],
            },
            "isShownAt": "https://example.org/item/abc",
            "rights": "http://rightsstatements.org/vocab/InC/1.0/",
            "iiifManifest": iiif_value,
        }

    hubs = {
        "Digital Commonwealth": {
            "Wikidata": "Q1",
            "institutions": {"Boston Public Library": {"Wikidata": "Q2"}},
        }
    }

    for junk in (None, "", "   ", "null", "ftp://example.org/x", "not a url"):
        out = build_claims_for_doc(_doc(junk), "abc1234567890", hubs, {}, {}, {})
        p7482 = next(c for c in out["claims"] if c["mainsnak"]["property"] == "P7482")
        assert "P6108" not in p7482["qualifiers"], (
            f"{junk!r} should not produce a P6108 qualifier"
        )

    # Valid http/https URLs DO get stamped, with surrounding whitespace
    # tolerated.
    for valid in (
        "https://example.org/iiif/manifest.json",
        "  https://example.org/iiif/manifest.json  ",
        "http://example.org/iiif/manifest.json",
    ):
        out = build_claims_for_doc(_doc(valid), "abc1234567890", hubs, {}, {}, {})
        p7482 = next(c for c in out["claims"] if c["mainsnak"]["property"] == "P7482")
        assert "P6108" in p7482["qualifiers"], (
            f"{valid!r} should produce a P6108 qualifier"
        )
        # Stamped value is the stripped form.
        assert p7482["qualifiers"]["P6108"][0]["datavalue"]["value"] == valid.strip()


def test_build_claims_for_doc_tolerates_missing_rights_field():
    """In maintain mode + ``--skip-media-filter``, the ES query no longer
    enforces ``rightsCategory == "Unlimited Re-Use"``, so docs with no
    ``rights`` field reach SDC pre-compute. Pre-fix, ``parse_dpla_doc``
    did ``doc["rights"]`` unguarded and raised ``KeyError`` on the first
    such doc — aborting the entire id-generation pass for one maintain
    target (concretely: Duke maintain via Internet Archive / Digital
    Library of Georgia hubs, both of which have de-opted institutions
    whose records still flow through the maintain scope).

    The fix defaults missing ``rights`` to an empty string;
    ``normalize_rights_uri("")`` is a no-op and the
    ``rights.get("")`` lookup in ``_build_rights_claims`` returns
    None, so no rights claim is emitted (instead of crashing).
    """
    from ingest_wikimedia.sdc import build_claims_for_doc

    doc = {
        "id": "abc1234567890",
        "ingestDate": "2026-06-29T00:00:00Z",
        "provider": {"name": "Internet Archive"},
        "dataProvider": {"name": "Duke University Libraries"},
        "sourceResource": {
            "title": ["A book"],
            "date": [{"displayDate": "1945"}],
        },
        "isShownAt": "https://example.org/item/abc",
        # No "rights" key.
    }
    hubs = {
        "Internet Archive": {
            "Wikidata": "Q461",
            "institutions": {"Duke University Libraries": {"Wikidata": "Q5312898"}},
        }
    }

    out = build_claims_for_doc(doc, "abc1234567890", hubs, {}, {}, {})
    # No crash. No P275/P6426 rights claims since the input had no rights.
    rights_props = {"P275", "P6426", "P6216"}
    rights_claims = [
        c for c in out["claims"] if c["mainsnak"]["property"] in rights_props
    ]
    assert rights_claims == [], (
        f"missing rights must produce no rights claims; got {rights_claims}"
    )


def test_build_rights_claims_pdm_emits_bare_public_domain_status():
    """CC's Public Domain Mark is a rights-statement declaration, not a
    copyright license — emitting it as a P275 (copyright license) claim
    (as rights.json would naively have us do) produces SDC that
    Module:License's branch table can't reconcile. Assert the shape we
    write instead: a single P6216=Q19652 (public domain) statement,
    DPLA-authored via ``formattedclaim``'s default reference triple + P459
    qualifier. No P275, no jurisdiction qualifier, no reason-specific
    determination method — PDM doesn't warrant asserting a particular
    reason (>95 years old, government work, etc.). Module:DPLA reads this
    shape and emits ``{{PD-US}}`` directly."""
    import datetime as _dt

    from ingest_wikimedia.sdc import (
        PD_MARK_URI_CANONICAL,
        _build_rights_claims,
        load_rights_json,
    )

    claims = _build_rights_claims(
        PD_MARK_URI_CANONICAL,
        load_rights_json(),
        "abc1234567890",
        _dt.date(2026, 7, 10),
    )

    props = [c["mainsnak"]["property"] for c in claims]
    assert props == ["P6216"], (
        f"PDM shape must be a single P6216 statement, got {props}"
    )
    p6216 = claims[0]
    assert p6216["mainsnak"]["datavalue"]["value"]["numeric-id"] == 19652

    # Only the default P459=Q61848113 (heuristic) qualifier is stamped by
    # ``formattedclaim`` — no P1001, no reason-specific determination method.
    quals = p6216["qualifiers"]
    assert list(quals.keys()) == ["P459"], (
        f"PDM P6216 must carry only the default P459 qualifier; got {list(quals.keys())}"
    )
    assert quals["P459"][0]["datavalue"]["value"]["numeric-id"] == 61848113

    # DPLA-provenance reference triple present.
    assert any(
        any(
            snak.get("datavalue", {}).get("value", {}).get("numeric-id") == 2944483
            for snak in ref.get("snaks", {}).get("P123", [])
        )
        for ref in p6216.get("references", [])
    ), "PDM shape must carry the DPLA-publisher reference marker"


def test_build_rights_claims_pdm_uri_variants_all_route_through_pdm_branch():
    """DPLA emits the PDM URI in http/https and with/without trailing
    slash variants. ``normalize_rights_uri`` canonicalises all of them
    to the same key, and the PDM branch must fire regardless of which
    variant the source record supplies. Failing on any of these would
    leak the old rights.json-driven P275=Q7257361 shape into a subset
    of PDM ingests."""
    import datetime as _dt

    from ingest_wikimedia.sdc import _build_rights_claims, load_rights_json

    rights = load_rights_json()
    for variant in (
        "http://creativecommons.org/publicdomain/mark/1.0",
        "http://creativecommons.org/publicdomain/mark/1.0/",
        "https://creativecommons.org/publicdomain/mark/1.0",
        "https://creativecommons.org/publicdomain/mark/1.0/",
    ):
        claims = _build_rights_claims(
            variant, rights, "abc1234567890", _dt.date(2026, 7, 10)
        )
        props = [c["mainsnak"]["property"] for c in claims]
        assert props == ["P6216"], (
            f"variant {variant!r} produced {props} instead of a single P6216"
        )


def test_build_source_claim_never_stamps_p2699():
    """``P2699`` is per-ordinal — different ordinals of the same DPLA
    item have different download URLs, so the qualifier can't be baked
    into sdc.json. It gets injected by ``process_one_from_sdc`` at sync
    time. Pin the contract that ``_build_source_claim`` does NOT emit
    it, even speculatively."""
    import datetime as _dt

    claim = _build_source_claim(
        "Q12345",
        "https://example.org/item/1",
        "abc",
        _dt.date(2026, 6, 7),
        iiif_manifest_url="https://example.org/iiif/abc/manifest.json",
    )
    assert "P2699" not in claim["qualifiers"]


# --------------------------------------------------------------------------
# Long-value chunking — _normalize_string_value, _chunk_value,
# _next_series_letter, _chunk_and_emit_claims.
#
# Wikibase normalizes string and monolingualtext values on save
# (lib/includes/StringNormalizer.php): control char runs become a single
# space, leading/trailing Unicode whitespace is stripped, monolingualtext
# is NFC-normalized. We pre-apply the same transforms locally so chunk
# boundary char counts match what Wikibase will store and chunk-by-chunk
# matching stays stable across syncs.
# --------------------------------------------------------------------------


def test_normalize_string_value_strips_leading_trailing_whitespace():
    from ingest_wikimedia.sdc import _normalize_string_value

    assert _normalize_string_value("  hello  ") == "hello"
    assert _normalize_string_value("\thello\t") == "hello"
    assert _normalize_string_value(" hello ") == "hello"


def test_normalize_string_value_collapses_control_char_runs_to_single_space():
    """Wikibase's preg_replace('/\\p{Cc}+/u', ' ', ...) — any run of
    control chars becomes a single ASCII space. Internal regular
    spaces are NOT collapsed."""
    from ingest_wikimedia.sdc import _normalize_string_value

    assert _normalize_string_value("Line one.\nLine two.") == "Line one. Line two."
    assert _normalize_string_value("a\n\n\nb") == "a b"
    assert _normalize_string_value("a\t\r\n\tb") == "a b"
    assert _normalize_string_value("a    b") == "a    b"


def test_normalize_string_value_empty_input():
    from ingest_wikimedia.sdc import _normalize_string_value

    assert _normalize_string_value("") == ""
    assert _normalize_string_value("   ") == ""
    assert _normalize_string_value("\n\t\r") == ""


def test_normalize_string_value_idempotent():
    """Applying normalization twice yields the same result as once."""
    from ingest_wikimedia.sdc import _normalize_string_value

    samples = ["  hello  ", "a\nb", "café", "Line\n\nbreak", "\thello\t"]
    for s in samples:
        once = _normalize_string_value(s)
        twice = _normalize_string_value(once)
        assert once == twice, f"non-idempotent on {s!r}: {once!r} → {twice!r}"


def test_normalize_string_value_nfc_for_monolingualtext():
    """cleanupToNFC is wired only for monolingualtext on Wikibase."""
    from ingest_wikimedia.sdc import _normalize_string_value

    decomposed = "é"  # e + COMBINING ACUTE ACCENT (NFD)
    precomposed = "é"  # NFC
    assert _normalize_string_value(decomposed, is_monolingualtext=True) == precomposed
    assert _normalize_string_value(decomposed, is_monolingualtext=False) == decomposed


def test_chunk_value_under_limit_returns_single_chunk():
    from ingest_wikimedia.sdc import _chunk_value

    assert _chunk_value("short") == ["short"]
    text = "x" * 1500
    assert _chunk_value(text) == [text]


def test_chunk_value_over_limit_splits_at_non_whitespace_boundary():
    """Boundary search walks backward to find a position where both
    adjacent characters are non-whitespace, so Wikibase's
    leading/trailing-whitespace strip can't eat bytes at the boundary."""
    from ingest_wikimedia.sdc import _chunk_value

    text = "abcdefghij klmnopqrst"
    chunks = _chunk_value(text, limit=12)
    assert len(chunks) == 2
    assert not chunks[0].endswith(" ")
    assert not chunks[1].startswith(" ")
    assert "".join(chunks) == text


def test_chunk_value_pathological_whitespace_run_falls_back_to_exact_limit(caplog):
    """A whitespace run longer than the chunk window leaves no valid
    non-whitespace boundary in reach. Chunk at exactly limit + warn."""
    import logging

    from ingest_wikimedia.sdc import _chunk_value

    text = "a" + (" " * 20) + "b"
    with caplog.at_level(logging.WARNING, logger="ingest_wikimedia.sdc"):
        chunks = _chunk_value(text, limit=10)
    assert len(chunks) >= 2
    assert any("whitespace run" in rec.message for rec in caplog.records)
    assert "".join(chunks) == text


def test_chunk_value_multiple_chunks():
    from ingest_wikimedia.sdc import _chunk_value

    text = "x" * 3500
    chunks = _chunk_value(text, limit=1000)
    assert len(chunks) == 4
    assert sum(len(c) for c in chunks) == 3500
    for chunk in chunks:
        assert len(chunk) <= 1000


def test_next_series_letter_advances_per_key():
    from ingest_wikimedia.sdc import _next_series_letter

    letters = {}
    assert _next_series_letter(letters, "P1476", "en") == "A"
    assert _next_series_letter(letters, "P1476", "en") == "B"
    assert _next_series_letter(letters, "P10358", "en") == "A"
    assert _next_series_letter(letters, "P1476", "en") == "C"
    assert _next_series_letter(letters, "P1476", "fr") == "A"


def test_chunk_and_emit_claims_short_value_emits_one_claim_no_p1545():
    """Single-chunk values keep the pre-chunking shape — no P1545."""
    import datetime as _dt

    from ingest_wikimedia.sdc import _chunk_and_emit_claims

    letters = {}
    claims = _chunk_and_emit_claims(
        "P1476",
        "Short title",
        "monolingualtext",
        "abc",
        _dt.date(2026, 1, 1),
        letters,
        language="en",
    )
    assert len(claims) == 1
    assert "P1545" not in claims[0]["qualifiers"]
    assert letters == {}


def test_chunk_and_emit_claims_long_value_emits_multiple_with_p1545():
    """Long values split into multiple claims, each with P1545="A1",
    "A2", etc. Series letter advances for the next long value on the
    same (prop, language)."""
    import datetime as _dt

    from ingest_wikimedia.sdc import _chunk_and_emit_claims

    letters = {}
    long_text = "x" * 1500 + " " + "y" * 800
    claims = _chunk_and_emit_claims(
        "P10358",
        long_text,
        "monolingualtext",
        "abc",
        _dt.date(2026, 1, 1),
        letters,
        language="en",
    )
    assert len(claims) == 2
    assert claims[0]["qualifiers"]["P1545"][0]["datavalue"]["value"] == "A1"
    assert claims[1]["qualifiers"]["P1545"][0]["datavalue"]["value"] == "A2"
    assert letters == {("P10358", "en"): "A"}

    second_long = "z" * 1500 + " " + "w" * 800
    more = _chunk_and_emit_claims(
        "P10358",
        second_long,
        "monolingualtext",
        "abc",
        _dt.date(2026, 1, 1),
        letters,
        language="en",
    )
    assert [c["qualifiers"]["P1545"][0]["datavalue"]["value"] for c in more] == [
        "B1",
        "B2",
    ]


def test_chunk_and_emit_claims_empty_after_normalization_emits_nothing():
    import datetime as _dt

    from ingest_wikimedia.sdc import _chunk_and_emit_claims

    assert (
        _chunk_and_emit_claims(
            "P1476",
            "   ",
            "monolingualtext",
            "abc",
            _dt.date(2026, 1, 1),
            {},
            language="en",
        )
        == []
    )
    assert (
        _chunk_and_emit_claims("P217", "", "string", "abc", _dt.date(2026, 1, 1), {})
        == []
    )


def test_chunk_and_emit_claims_per_language_series_independent():
    """A long English description and a long French description on the
    same property each start a fresh A series — series-letter key is
    (prop, language)."""
    import datetime as _dt

    from ingest_wikimedia.sdc import _chunk_and_emit_claims

    letters = {}
    en = _chunk_and_emit_claims(
        "P10358",
        "x" * 2500,
        "monolingualtext",
        "abc",
        _dt.date(2026, 1, 1),
        letters,
        language="en",
    )
    fr = _chunk_and_emit_claims(
        "P10358",
        "y" * 2500,
        "monolingualtext",
        "abc",
        _dt.date(2026, 1, 1),
        letters,
        language="fr",
    )
    assert en[0]["qualifiers"]["P1545"][0]["datavalue"]["value"].startswith("A")
    assert fr[0]["qualifiers"]["P1545"][0]["datavalue"]["value"].startswith("A")


def test_chunk_and_emit_claims_normalizes_before_chunking():
    """Embedded newlines collapse to single spaces (per Wikibase's
    server-side normalization) BEFORE the chunk boundary search runs.
    Without this, our chunk char counts would diverge from Wikibase's
    stored byte length and matching would drift on every sync."""
    import datetime as _dt

    from ingest_wikimedia.sdc import _chunk_and_emit_claims

    # 1500 'x' + newline + 1500 'y' → after normalization it's
    # 1500 'x' + single space + 1500 'y' = 3001 chars, chunked.
    text = "x" * 1500 + "\n" + "y" * 1500
    claims = _chunk_and_emit_claims(
        "P10358",
        text,
        "monolingualtext",
        "abc",
        _dt.date(2026, 1, 1),
        {},
        language="en",
    )
    # Concatenated values reassemble to the normalized form, not the raw.
    rebuilt = "".join(c["mainsnak"]["datavalue"]["value"]["text"] for c in claims)
    assert rebuilt == "x" * 1500 + " " + "y" * 1500


def test_normalize_string_value_strips_bom_and_bidi_marks():
    """Wikibase's trimBadChars equivalent strips BOM (U+FEFF), bidi marks
    (U+200E/U+200F), and non-characters (U+FFFE/U+FFFF) — invisible
    codepoints that DPLA source CSV/JSON occasionally carries from
    pasted-text round-trips. Without local stripping, the next sync
    would see drift on every such value (Wikibase stores N-1 bytes,
    we emit N) and re-write every chunk indefinitely."""
    from ingest_wikimedia.sdc import _normalize_string_value

    # BOM at start, mid, and end — all stripped.
    assert _normalize_string_value("﻿Hello") == "Hello"
    assert _normalize_string_value("Hello﻿") == "Hello"
    assert _normalize_string_value("He﻿llo") == "Hello"
    # LRM (U+200E) and RLM (U+200F) — both stripped.
    assert _normalize_string_value("Hello‎") == "Hello"
    assert _normalize_string_value("a‏b") == "ab"
    # Non-characters U+FFFE / U+FFFF — both stripped.
    assert _normalize_string_value("Hello￾") == "Hello"
    assert _normalize_string_value("a￿b") == "ab"


def test_advance_series_letter_basic_increment():
    from ingest_wikimedia.sdc import _advance_series_letter

    assert _advance_series_letter("A") == "B"
    assert _advance_series_letter("B") == "C"
    assert _advance_series_letter("Y") == "Z"


def test_advance_series_letter_carry_past_z():
    """The bug this guards against: ``chr(ord('Z') + 1) == '['`` —
    silently advancing into non-alpha codepoints would corrupt Lua-side
    reassembly. Excel-style carry to AA is the documented behavior."""
    from ingest_wikimedia.sdc import _advance_series_letter

    assert _advance_series_letter("Z") == "AA"
    assert _advance_series_letter("AA") == "AB"
    assert _advance_series_letter("AZ") == "BA"
    assert _advance_series_letter("BZ") == "CA"
    assert _advance_series_letter("ZZ") == "AAA"
    assert _advance_series_letter("AAZ") == "ABA"


def test_next_series_letter_advances_past_z_without_corruption():
    """A doc with 27+ long values on one (prop, lang) advances past Z
    safely. Verify the first 28 letters in sequence."""
    from ingest_wikimedia.sdc import _next_series_letter

    letters = {}
    seen = [_next_series_letter(letters, "P10358", "en") for _ in range(28)]
    # First 26 letters A through Z.
    assert seen[:26] == [chr(ord("A") + i) for i in range(26)]
    # 27th is AA (not "[").
    assert seen[26] == "AA"
    assert seen[27] == "AB"
    # The persisted state matches the last value returned.
    assert letters[("P10358", "en")] == "AB"


# ---------------------------------------------------------------------------
# parse_date_range — year-range equivalence helper used purely for dedup
# between range-shaped somevalue+P1932 claims. parse_dpla_date deliberately
# returns None for any range, so this is the only path to recognise two
# different formattings of the same range as the same fact.
# ---------------------------------------------------------------------------


def test_parse_date_range_dash_variants():
    """All separator variants between two 4-digit years yield the same
    (start, end) tuple."""
    from ingest_wikimedia.sdc import parse_date_range

    assert parse_date_range("1934 - 1948") == (1934, 1948)
    assert parse_date_range("1934-1948") == (1934, 1948)
    assert parse_date_range("1934–1948") == (1934, 1948)  # en-dash
    assert parse_date_range("1934—1948") == (1934, 1948)  # em-dash
    assert parse_date_range("1934/1948") == (1934, 1948)


def test_parse_date_range_between():
    """``between X and Y`` is the prose form produced by ``{{other date|
    between|X|Y}}`` after wikitext expansion. Case-insensitive."""
    from ingest_wikimedia.sdc import parse_date_range

    assert parse_date_range("between 1934 and 1948") == (1934, 1948)
    assert parse_date_range("Between 1934 and 1948") == (1934, 1948)
    assert parse_date_range("BETWEEN 1934 AND 1948") == (1934, 1948)


def test_parse_date_range_from_to():
    """``X to Y`` (and ``from X to Y``) covers a less common DPLA shape."""
    from ingest_wikimedia.sdc import parse_date_range

    assert parse_date_range("1934 to 1948") == (1934, 1948)
    assert parse_date_range("from 1934 to 1948") == (1934, 1948)


def test_parse_date_range_other_date_template_raw_wikitext():
    """When ``_expand_wikitext_for_date_parse`` couldn't reach the API
    (or the value was stored before the expand-then-store fix), the
    P1932 qualifier carries literal ``{{other date|between|X|Y}}``
    markup. The range parser still recognises it so the reconciler can
    dedup these legacy-encoded statements without a second API round
    trip to expand them."""
    from ingest_wikimedia.sdc import parse_date_range

    assert parse_date_range("{{other date|between|1934|1948}}") == (1934, 1948)
    # Tolerate optional whitespace and the ``other_date`` underscored
    # alias MediaWiki accepts for template names.
    assert parse_date_range("{{ other date | between | 1934 | 1948 }}") == (
        1934,
        1948,
    )
    assert parse_date_range("{{other_date|between|1934|1948}}") == (1934, 1948)


def test_parse_date_range_canonicalises_min_max():
    """Reversed-order inputs canonicalise to ``(min, max)`` so two
    range claims that disagree on direction (e.g. ``"1948 - 1934"`` and
    ``"1934 - 1948"``) still dedup. Wikibase has no semantic for
    direction on a P1932 qualifier; both encode the same span."""
    from ingest_wikimedia.sdc import parse_date_range

    assert parse_date_range("1948 - 1934") == (1934, 1948)
    assert parse_date_range("between 1948 and 1934") == (1934, 1948)


def test_parse_date_range_rejects_year_zero():
    """Year 0 is undefined in proleptic Gregorian — same rule as
    ``parse_dpla_date``. Refuse to produce a comparable that would
    collide with a year-1 claim."""
    from ingest_wikimedia.sdc import parse_date_range

    assert parse_date_range("0 - 1948") is None
    assert parse_date_range("1934 - 0") is None


def test_parse_date_range_rejects_single_year_and_year_month():
    """Single years and ISO year-month strings must NOT register as
    ranges — those are the domain of ``parse_dpla_date``. The dash
    pattern requires 3-4 digits on BOTH sides, which excludes the
    ``YYYY-MM`` shape (month digits are 1-2)."""
    from ingest_wikimedia.sdc import parse_date_range

    assert parse_date_range("1945") is None
    assert parse_date_range("1945-06") is None
    assert parse_date_range("1945-06-07") is None


def test_parse_date_range_rejects_free_prose():
    """Strings that contain a range-like substring but extra prose
    around it (e.g. ``"around 1934-1948 era"``) must fall back to
    ``None``. The regexes are anchored to ``^…$`` so any non-range
    residue refuses the match — mirrors ``parse_dpla_date``'s
    conservative-fallback contract."""
    from ingest_wikimedia.sdc import parse_date_range

    assert parse_date_range("around 1934-1948 era") is None
    assert parse_date_range("Civil War 1861-1865") is None
    assert parse_date_range("") is None


def test_parse_dpla_date_still_returns_none_for_ranges():
    """Regression guard — parse_date_range is a *separate* helper, NOT a
    fallback path inside parse_dpla_date. ``_build_date_claim`` must
    keep its somevalue-fallback behaviour for ranges so the raw DPLA
    string is preserved in P1932 instead of being silently coerced to a
    single year."""
    assert parse_dpla_date("1934 - 1948") is None
    assert parse_dpla_date("between 1934 and 1948") is None


# ---------------------------------------------------------------------------
# parse_other_date_template — convert raw {{other date|MODIFIER|...}} wikitext
# (legacy inferred-from-Wikitext claims written before the expand-then-store
# fix) into the display string parse_dpla_date understands, so the reconciler
# comparable matches the DPLA-sourced equivalent. Conservative: only the
# modifiers DPLA can represent; None otherwise.
# ---------------------------------------------------------------------------


def test_parse_other_date_template_circa_family():
    """The reported case (M180313608): every circa-marker spelling maps to
    'circa <date>', which parse_dpla_date turns into year-precision +
    approximate — matching the DPLA value-typed time + P1480 circa."""
    from ingest_wikimedia.sdc import parse_other_date_template

    for raw in (
        "{{other date|~|1911}}",
        "{{other date|circa|1911}}",
        "{{other date|c|1911}}",
        "{{other date|c.|1911}}",
        "{{other date|ca|1911}}",
        "{{other date|ca.|1911}}",
    ):
        assert parse_other_date_template(raw) == "circa 1911", raw


def test_parse_other_date_template_uncertain_and_decade():
    from ingest_wikimedia.sdc import parse_other_date_template

    assert parse_other_date_template("{{other date|?|1945}}") == "1945?"
    assert parse_other_date_template("{{other date|s|1910}}") == "1910s"
    assert parse_other_date_template("{{other date|decade|1910}}") == "1910s"


def test_parse_other_date_template_tolerates_whitespace_and_underscore_alias():
    from ingest_wikimedia.sdc import parse_other_date_template

    assert parse_other_date_template("{{ other date | ~ | 1911 }}") == "circa 1911"
    assert parse_other_date_template("{{other_date|~|1911}}") == "circa 1911"


def test_parse_other_date_template_preserves_month_precision():
    """The circa wrapper is stripped to the inner date verbatim, so a
    YYYY-MM inner value keeps month precision once parse_dpla_date runs."""
    from ingest_wikimedia.sdc import parse_other_date_template

    assert parse_other_date_template("{{other date|c.|1911-06}}") == "circa 1911-06"


def test_parse_other_date_template_returns_none_for_unsupported_modifiers():
    """between/before/after/century/season have no parse_dpla_date
    equivalent — return None so the caller keeps the raw string (which
    only matches byte-identical text). Critical: never widen a dedup into
    a wrong removal by guessing at a modifier DPLA can't represent.
    ``between`` is handled separately by parse_date_range, so None here
    is correct too."""
    from ingest_wikimedia.sdc import parse_other_date_template

    for raw in (
        "{{other date|between|1934|1948}}",
        "{{other date|before|1911}}",
        "{{other date|after|1911}}",
        "{{other date|century|19}}",
        "{{other date|spring|1911}}",
    ):
        assert parse_other_date_template(raw) is None, raw


def test_parse_other_date_template_returns_none_for_non_template():
    from ingest_wikimedia.sdc import parse_other_date_template

    assert parse_other_date_template("circa 1911") is None
    assert parse_other_date_template("1911") is None
    assert parse_other_date_template("") is None
    assert parse_other_date_template("{{some other template|x}}") is None
    # Template present but no date argument → None (nothing to convert).
    assert parse_other_date_template("{{other date|~}}") is None


def _p9126_roles(out):
    """(mainsnak Q-ID, P3831 role Q-ID) pairs from _build_contributed_claims."""

    def _qid(v):
        return "Q" + str(v["numeric-id"])

    return [
        (
            _qid(c["mainsnak"]["datavalue"]["value"]),
            _qid(c["qualifiers"]["P3831"][0]["datavalue"]["value"]),
        )
        for c in out
    ]


def test_build_contributed_claims_content_hub_shape():
    """Pins the content-hub P9126 role shape (see _build_contributed_claims
    for the model). Exact Q-IDs are asserted because they must match what is
    already on Commons — a value change forces a remove+re-add on re-sync."""
    import datetime

    from ingest_wikimedia.sdc import (
        CONTENT_HUB_QIDS,
        Q_DPLA,
        Q_NARA,
        Q_ROLE_AGGREGATOR,
        Q_ROLE_CONTRIBUTING,
        Q_ROLE_REPOSITORY,
        Q_SMITHSONIAN,
        _build_contributed_claims,
    )

    assert {Q_NARA, Q_SMITHSONIAN} <= CONTENT_HUB_QIDS
    date = datetime.date(2026, 6, 16)
    for hub in (Q_NARA, Q_SMITHSONIAN):
        roles = _p9126_roles(_build_contributed_claims(hub, "Q999", "abc", date))
        assert roles == [
            (Q_DPLA, Q_ROLE_AGGREGATOR),
            (hub, Q_ROLE_REPOSITORY),
            ("Q999", Q_ROLE_CONTRIBUTING),
        ]


def test_build_contributed_claims_service_hub_shape():
    """Service hubs are aggregating intermediaries (aggregator role, like
    DPLA); their institution is a distinct organization (repository) that
    also lands in P195."""
    import datetime

    from ingest_wikimedia.sdc import (
        Q_DPLA,
        Q_ROLE_AGGREGATOR,
        Q_ROLE_REPOSITORY,
        _build_contributed_claims,
    )

    roles = _p9126_roles(
        _build_contributed_claims("Q12345", "Q67890", "abc", datetime.date(2026, 6, 16))
    )
    assert roles == [
        (Q_DPLA, Q_ROLE_AGGREGATOR),
        ("Q12345", Q_ROLE_AGGREGATOR),
        ("Q67890", Q_ROLE_REPOSITORY),
    ]


def test_ingest_date_from_doc_parses_iso_timestamp():
    """The DPLA API emits ``ingestDate`` as ISO 8601. Strip the time part
    and return a ``datetime.date``."""
    import datetime

    from ingest_wikimedia.sdc import ingest_date_from_doc

    doc = {"ingestDate": "2026-06-23T15:50:29.874Z"}
    assert ingest_date_from_doc(doc) == datetime.date(2026, 6, 23)


def test_ingest_date_from_doc_raises_when_missing():
    """A DPLA record without ingestDate is corrupted / an ES bug. Raise
    loudly so the caller can skip the item — do NOT synthesize a date."""
    from ingest_wikimedia.sdc import ingest_date_from_doc

    for bad in (
        None,
        {},
        {"ingestDate": None},
        {"ingestDate": ""},
        {"ingestDate": 12345},
    ):
        try:
            ingest_date_from_doc(bad)
        except ValueError:
            continue
        else:
            raise AssertionError(f"expected ValueError for {bad!r}")


def test_ingest_date_from_doc_raises_on_malformed_timestamp():
    """A non-ISO ``ingestDate`` value is a data-integrity signal, not a
    condition to paper over."""
    from ingest_wikimedia.sdc import ingest_date_from_doc

    try:
        ingest_date_from_doc({"ingestDate": "not-a-date-value"})
    except ValueError:
        return
    raise AssertionError("expected ValueError for malformed ingestDate")


def test_build_claims_for_doc_pins_p813_to_ingest_date():
    """Every P813 (retrieved on) reference across the returned claims is
    pinned to the doc's ``ingestDate``, not the current date. Two calls
    against the same doc must produce identical P813 values regardless of
    when they run — this is the whole point of ingest-date pinning."""
    from ingest_wikimedia.sdc import build_claims_for_doc

    doc = {
        "id": "abc1234567890",
        "ingestDate": "2026-06-23T15:50:29.874Z",
        "provider": {"name": "Digital Commonwealth"},
        "dataProvider": {"name": "Boston Public Library"},
        "sourceResource": {
            "title": ["A title"],
            "date": [{"displayDate": "1945"}],
        },
        "isShownAt": "https://example.org/item/abc",
        "rights": "http://rightsstatements.org/vocab/InC/1.0/",
    }
    hubs = {
        "Digital Commonwealth": {
            "Wikidata": "Q1",
            "institutions": {"Boston Public Library": {"Wikidata": "Q2"}},
        }
    }
    out_a = build_claims_for_doc(doc, "abc1234567890", hubs, {}, {}, {})
    out_b = build_claims_for_doc(doc, "abc1234567890", hubs, {}, {}, {})

    assert out_a["ingest_date"] == "2026-06-23"
    assert out_b["ingest_date"] == "2026-06-23"

    def _p813_times(payload):
        times = []
        for claim in payload["claims"]:
            for ref in claim.get("references") or []:
                for snak in (ref.get("snaks") or {}).get("P813") or []:
                    times.append(snak["datavalue"]["value"]["time"])
        return times

    times = _p813_times(out_a)
    assert times, "every claim carries a P813 reference"
    assert set(times) == {"+2026-06-23T00:00:00Z"}, (
        f"P813 must be pinned to ingestDate; got {set(times)}"
    )
    assert _p813_times(out_a) == _p813_times(out_b), (
        "identical doc must produce byte-identical P813 stamps"
    )
