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
