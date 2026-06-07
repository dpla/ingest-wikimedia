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
