"""Tests for sdc_sync's existing-statement reconciliation logic.

Focused on the bug fix that motivated the helper: foreign statements
(carrying qualifiers DPLA didn't author) must never be amended via
wbeditentity-with-id, because Wikibase replaces the entire claim's
qualifiers + references with what we send.

The full check() flow is hard to unit-test without standing up a fake
get_entity() return — these tests mock that at module scope and cover
the four observable outcomes:

  * existing matching statement that is DPLA-shaped → don't add duplicate
  * existing matching statement that is foreign → add ours alongside,
    don't capture its id for ref-stamping (the bug case)
  * existing matching statement with no qualifiers at all → stamp our
    P459 qualifier onto it
  * no matching statement → add new
"""

from unittest.mock import patch

import pytest


def _qual_p459(qid):
    """Build a `qualifiers.P459` snak list with a single entity value."""
    return [
        {
            "snaktype": "value",
            "property": "P459",
            "datavalue": {
                "type": "wikibase-entityid",
                "value": {
                    "entity-type": "item",
                    "numeric-id": int(qid.replace("Q", "")),
                    "id": qid,
                },
            },
        }
    ]


def _qual_other(prop, qid):
    """Build a non-P459 entity-valued qualifier snak list."""
    return [
        {
            "snaktype": "value",
            "property": prop,
            "datavalue": {
                "type": "wikibase-entityid",
                "value": {
                    "entity-type": "item",
                    "numeric-id": int(qid.replace("Q", "")),
                    "id": qid,
                },
            },
        }
    ]


def _item_statement(stmt_id, value_qid, qualifiers=None, references=None):
    """Construct a Commons MediaInfo statement dict for tests."""
    stmt = {
        "id": stmt_id,
        "mainsnak": {
            "property": "P6216",
            "snaktype": "value",
            "datavalue": {
                "type": "wikibase-entityid",
                "value": {
                    "entity-type": "item",
                    "numeric-id": int(value_qid.replace("Q", "")),
                    "id": value_qid,
                },
            },
        },
    }
    if qualifiers is not None:
        stmt["qualifiers"] = qualifiers
    if references is not None:
        stmt["references"] = references
    return stmt


def test_is_dpla_shaped_recognises_p459_marker():
    """_is_dpla_shaped is the sole gate that distinguishes our writes from
    foreign ones. Confirm it fires on Q61848113 (our marker) and only that."""
    from tools.sdc_sync import _is_dpla_shaped

    assert _is_dpla_shaped(
        _item_statement("Z$1", "Q19652", qualifiers={"P459": _qual_p459("Q61848113")})
    )
    # Foreign P459 value (e.g. "work of US federal government") must not be
    # confused for ours — that's exactly the case that caused the bug.
    assert not _is_dpla_shaped(
        _item_statement("Z$2", "Q19652", qualifiers={"P459": _qual_p459("Q60671452")})
    )
    # No P459 qualifier at all.
    assert not _is_dpla_shaped(
        _item_statement(
            "Z$3", "Q19652", qualifiers={"P1001": _qual_other("P1001", "Q30")}
        )
    )
    # No qualifiers at all.
    assert not _is_dpla_shaped(_item_statement("Z$4", "Q19652"))


def test_check_foreign_match_adds_alongside_not_overwrite():
    """Reproduces the production bug. Foreign P6216=Q19652 statement with
    P1001+P459 qualifiers must NOT get its id captured for ref-stamping
    (which would clobber its qualifiers via wbeditentity-with-id).

    Expected: check returns (True, "") meaning 'add a new DPLA-authored
    statement alongside'. The foreign statement is left untouched."""
    from tools import sdc_sync

    foreign_stmt = _item_statement(
        "M999$abc",
        "Q19652",
        qualifiers={
            "P1001": _qual_other("P1001", "Q30"),
            "P459": _qual_p459("Q60671452"),
        },
    )
    fake_entity = {
        "pageid": 999,
        "statements": {"P6216": [foreign_stmt]},
    }
    with patch.object(sdc_sync, "get_entity", return_value=fake_entity):
        result = sdc_sync.check("M999", ("item", "Q19652"), "P6216")
    # First element True → add new statement. Second element "" → no
    # ref-stamp candidate; in particular, NOT the foreign statement's id.
    assert result == (True, "")


def test_check_dpla_shaped_match_doesnt_duplicate():
    """When the existing matching statement is DPLA-shaped, we've already
    written this claim. Don't add a duplicate. Capture its id for
    ref-stamping if it lacks references — safe because we own it."""
    from tools import sdc_sync

    dpla_stmt = _item_statement(
        "M999$ours",
        "Q19652",
        qualifiers={"P459": _qual_p459("Q61848113")},
    )
    fake_entity = {
        "pageid": 999,
        "statements": {"P6216": [dpla_stmt]},
    }
    with patch.object(sdc_sync, "get_entity", return_value=fake_entity):
        result = sdc_sync.check("M999", ("item", "Q19652"), "P6216")
    # add_new=False (already there), ref="M999$ours" (safe to amend — ours).
    assert result == (False, "M999$ours")


def test_check_no_qualifiers_match_stamps_p459():
    """An existing statement with matching value and NO qualifiers is the
    legitimate "stamp our P459 qualifier" case — preserved from the
    pre-fix behavior. wbsetqualifier is non-destructive."""
    from tools import sdc_sync

    empty_stmt = _item_statement("M999$empty", "Q19652")
    fake_entity = {"pageid": 999, "statements": {"P6216": [empty_stmt]}}
    with (
        patch.object(sdc_sync, "get_entity", return_value=fake_entity),
        # The production add_det returns None implicitly; mirror that on the
        # mock so the (None, ref) tuple comparison below is exact.
        patch.object(sdc_sync, "add_det", return_value=None) as mock_add_det,
    ):
        result = sdc_sync.check("M999", ("item", "Q19652"), "P6216")
    # add_det was called with the matching statement id (stamps the P459
    # qualifier via wbsetqualifier).
    mock_add_det.assert_called_once_with("M999", "M999$empty")
    # First element is add_det's return (None) — the caller's `if checkclaim[0] is True`
    # is False, so no new statement gets queued. Second element is the
    # captured ref ("M999$empty" — also DPLA-safe because no qualifiers).
    assert result == (None, "M999$empty")


def test_check_no_matching_statement_adds_new():
    """No existing P6216 statement at all → add new (True), no ref to amend."""
    from tools import sdc_sync

    fake_entity = {"pageid": 999, "statements": {}}
    with patch.object(sdc_sync, "get_entity", return_value=fake_entity):
        result = sdc_sync.check("M999", ("item", "Q19652"), "P6216")
    assert result == (True, "")


@pytest.mark.parametrize(
    "kind, value, mainsnak_value",
    [
        ("string", "abc-123", "abc-123"),
        ("monolingualtext", "Hello world", {"text": "Hello world", "language": "en"}),
    ],
)
def test_check_foreign_match_other_value_types(kind, value, mainsnak_value):
    """Same foreign-match-don't-overwrite behavior for string and
    monolingualtext mainsnaks (P760, P1476 etc.)."""
    from tools import sdc_sync

    foreign_stmt = {
        "id": "M999$str",
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"type": kind, "value": mainsnak_value},
        },
        "qualifiers": {"P1001": _qual_other("P1001", "Q30")},
    }
    fake_entity = {"pageid": 999, "statements": {"P760": [foreign_stmt]}}
    with patch.object(sdc_sync, "get_entity", return_value=fake_entity):
        result = sdc_sync.check("M999", (kind, value), "P760")
    assert result == (True, "")
