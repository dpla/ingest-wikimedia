"""Tests for sdc_sync's existing-statement reconciliation logic.

These tests focus on the amend-in-place gate that decides whether an
existing matching statement on Commons is safe for the bot to amend
via `wbeditentity`-with-id (a wholesale replace of qualifiers +
references) or must be left alone with the DPLA-authored claim added
as a separate statement.

The gate is `_is_safe_to_amend_in_place`: amend only when every
existing qualifier property is one DPLA writes for that property
(P459 always, plus per-property extras from
`_DPLA_EXTRA_QUALIFIER_PROPS`) AND every existing reference carries
the DPLA publisher marker (P123=Q2944483 via `_is_dpla_reference`).

A claim that contains any user-authored qualifier or reference is
NOT safe — the wbeditentity round-trip would erase that data.
"""

from unittest.mock import MagicMock, patch

import pytest


def _qual_entity(prop, qid):
    """Build a single wikibase-entityid qualifier snak under `prop`."""
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


def _qual_string(prop, value):
    """Build a string-valued qualifier snak (e.g. P973 URL, P2093 name)."""
    return [
        {
            "snaktype": "value",
            "property": prop,
            "datavalue": {"type": "string", "value": value},
        }
    ]


def _dpla_p459():
    return _qual_entity("P459", "Q61848113")


def _dpla_reference(dpla_id="abcdef"):
    """Build a DPLA-authored reference snak set (P854 URL, P123 publisher,
    P813 retrieved)."""
    return {
        "snaks": {
            "P854": [
                {
                    "snaktype": "value",
                    "property": "P854",
                    "datavalue": {
                        "type": "string",
                        "value": f"https://dp.la/item/{dpla_id}",
                    },
                }
            ],
            "P123": _qual_entity("P123", "Q2944483"),
            "P813": [
                {
                    "snaktype": "value",
                    "property": "P813",
                    "datavalue": {
                        "type": "time",
                        "value": {
                            "time": "+2026-05-27T00:00:00Z",
                            "timezone": 0,
                            "before": 0,
                            "after": 0,
                            "precision": 11,
                            "calendarmodel": "http://www.wikidata.org/entity/Q1985727",
                        },
                    },
                }
            ],
        }
    }


def _foreign_reference():
    """A reference snak set without the DPLA publisher marker — e.g. a
    user-added reference pointing to some other source."""
    return {
        "snaks": {
            "P854": [
                {
                    "snaktype": "value",
                    "property": "P854",
                    "datavalue": {
                        "type": "string",
                        "value": "https://example.org/citation",
                    },
                }
            ],
        }
    }


def _item_statement(stmt_id, value_qid, qualifiers=None, references=None, prop="P6216"):
    """Construct a Commons MediaInfo statement dict for tests."""
    stmt = {
        "id": stmt_id,
        "mainsnak": {
            "property": prop,
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


# ---------------------------------------------------------------------------
# _is_dpla_reference
# ---------------------------------------------------------------------------


def test_is_dpla_reference_recognises_p123_publisher_marker():
    """A reference is DPLA-authored iff its P123 snak resolves to Q2944483."""
    from tools.sdc_sync import _is_dpla_reference

    assert _is_dpla_reference(_dpla_reference())
    # Foreign reference (no P123).
    assert not _is_dpla_reference(_foreign_reference())
    # Reference with P123 but pointing at a different publisher.
    assert not _is_dpla_reference({"snaks": {"P123": _qual_entity("P123", "Q9999999")}})
    assert not _is_dpla_reference(None)
    assert not _is_dpla_reference({})


# ---------------------------------------------------------------------------
# _is_safe_to_amend_in_place
# ---------------------------------------------------------------------------


def test_safe_to_amend_truly_bare_statement():
    """No qualifiers, no references → safe (vacuously)."""
    from tools.sdc_sync import _is_safe_to_amend_in_place

    assert _is_safe_to_amend_in_place(_item_statement("Z$1", "Q19652"), "P6216")


def test_safe_to_amend_dpla_only_qualifier():
    """Only P459=Q61848113 → safe (it's DPLA's universal marker)."""
    from tools.sdc_sync import _is_safe_to_amend_in_place

    assert _is_safe_to_amend_in_place(
        _item_statement("Z$2", "Q19652", qualifiers={"P459": _dpla_p459()}), "P6216"
    )


def test_safe_to_amend_dpla_only_reference_no_qualifier():
    """Has DPLA reference, no qualifier → safe. This is the case the bot
    should be able to amend by adding the missing P459 qualifier."""
    from tools.sdc_sync import _is_safe_to_amend_in_place

    assert _is_safe_to_amend_in_place(
        _item_statement("Z$3", "Q19652", references=[_dpla_reference()]), "P6216"
    )


def test_safe_to_amend_dpla_qualifier_no_reference():
    """Has DPLA's P459 qualifier, no reference → safe. The bot should be
    able to amend by adding the missing DPLA reference."""
    from tools.sdc_sync import _is_safe_to_amend_in_place

    assert _is_safe_to_amend_in_place(
        _item_statement("Z$4", "Q19652", qualifiers={"P459": _dpla_p459()}), "P6216"
    )


def test_unsafe_when_user_authored_qualifier_alongside_dpla():
    """The residual bug case: claim has DPLA's P459 AND a user-added
    qualifier (e.g. P1001=Q30 added by a community editor after our
    write). The looser `_is_dpla_shaped` predecessor returned True here;
    `_is_safe_to_amend_in_place` correctly returns False."""
    from tools.sdc_sync import _is_safe_to_amend_in_place

    assert not _is_safe_to_amend_in_place(
        _item_statement(
            "Z$5",
            "Q19652",
            qualifiers={
                "P459": _dpla_p459(),
                "P1001": _qual_entity("P1001", "Q30"),
            },
        ),
        "P6216",
    )


def test_unsafe_when_foreign_reference_present():
    """Claim with a non-DPLA reference (someone else cited a source) →
    unsafe, even if all qualifiers are DPLA's. The round-trip would
    erase that foreign reference."""
    from tools.sdc_sync import _is_safe_to_amend_in_place

    assert not _is_safe_to_amend_in_place(
        _item_statement(
            "Z$6",
            "Q19652",
            qualifiers={"P459": _dpla_p459()},
            references=[_foreign_reference()],
        ),
        "P6216",
    )


def test_safe_when_per_property_extra_qualifier_is_recognised():
    """P7482 (source-of-file) statements legitimately carry P973 and P137
    qualifiers in addition to P459 — these are DPLA-authored per
    _DPLA_EXTRA_QUALIFIER_PROPS. The same qualifier set on a DIFFERENT
    property (P6216) is foreign."""
    from tools.sdc_sync import _is_safe_to_amend_in_place

    stmt_with_p973 = _item_statement(
        "Z$7",
        "Q74228490",
        qualifiers={
            "P459": _dpla_p459(),
            "P973": _qual_string("P973", "https://example.gov/item/1"),
            "P137": _qual_entity("P137", "Q123"),
        },
        prop="P7482",
    )
    # Safe under P7482 — P973 and P137 are DPLA-authored qualifiers
    # for source-of-file statements.
    assert _is_safe_to_amend_in_place(stmt_with_p973, "P7482")
    # Unsafe under P6216 — P973/P137 are not DPLA-authored for
    # copyright-status statements.
    assert not _is_safe_to_amend_in_place(stmt_with_p973, "P6216")


# ---------------------------------------------------------------------------
# check() — end-to-end behaviour through the tightened gate
# ---------------------------------------------------------------------------


def test_check_foreign_qualifier_match_adds_alongside():
    """Production bug case: P6216=Q19652 claim with P1001+P459 qualifiers
    that we didn't author. The bot must NOT capture this claim's id —
    that would clobber P1001 via wbeditentity-with-id. Instead add the
    DPLA-authored claim alongside as a separate statement."""
    from tools import sdc_sync

    foreign_stmt = _item_statement(
        "M999$abc",
        "Q19652",
        qualifiers={
            "P1001": _qual_entity("P1001", "Q30"),
            "P459": _qual_entity("P459", "Q60671452"),
        },
    )
    fake_entity = {"pageid": 999, "statements": {"P6216": [foreign_stmt]}}
    with patch.object(sdc_sync, "get_entity", return_value=fake_entity):
        result = sdc_sync.check("M999", ("item", "Q19652"), "P6216")
    assert result == (True, "")


def test_check_mixed_dpla_and_foreign_qualifier_treated_as_foreign():
    """A claim with BOTH DPLA's P459=Q61848113 AND a user-added qualifier
    (e.g. P1001=Q30 added by a community editor later) is no longer
    safe to amend — the prior `_is_dpla_shaped` gate would have
    misclassified this as DPLA-shaped. Expected: add new alongside.

    This is the residual-bug case the tightened gate now handles."""
    from tools import sdc_sync

    mixed_stmt = _item_statement(
        "M999$mixed",
        "Q19652",
        qualifiers={
            "P459": _dpla_p459(),
            "P1001": _qual_entity("P1001", "Q30"),
        },
    )
    fake_entity = {"pageid": 999, "statements": {"P6216": [mixed_stmt]}}
    with patch.object(sdc_sync, "get_entity", return_value=fake_entity):
        result = sdc_sync.check("M999", ("item", "Q19652"), "P6216")
    assert result == (True, "")


def test_check_dpla_only_match_no_reference_captures_ref():
    """Pure DPLA-shaped claim (P459=Q61848113 only) without a reference
    is the partial-DPLA-write case: we should capture its id for
    ref-stamping, not duplicate the claim."""
    from tools import sdc_sync

    dpla_stmt = _item_statement(
        "M999$ours", "Q19652", qualifiers={"P459": _dpla_p459()}
    )
    fake_entity = {"pageid": 999, "statements": {"P6216": [dpla_stmt]}}
    with patch.object(sdc_sync, "get_entity", return_value=fake_entity):
        result = sdc_sync.check("M999", ("item", "Q19652"), "P6216")
    assert result == (False, "M999$ours")


def test_check_no_qualifier_match_stamps_p459_via_add_det():
    """An existing matching statement with no qualifiers triggers
    branch 2's add_det call (wbsetqualifier — non-destructive)."""
    from tools import sdc_sync

    empty_stmt = _item_statement("M999$empty", "Q19652")
    fake_entity = {"pageid": 999, "statements": {"P6216": [empty_stmt]}}
    with (
        patch.object(sdc_sync, "get_entity", return_value=fake_entity),
        patch.object(sdc_sync, "add_det", return_value=None) as mock_add_det,
    ):
        result = sdc_sync.check("M999", ("item", "Q19652"), "P6216")
    mock_add_det.assert_called_once_with("M999", "M999$empty")
    assert result == (None, "M999$empty")


def test_check_no_matching_statement_adds_new():
    """No existing P6216 statement at all → add new, no ref to amend."""
    from tools import sdc_sync

    fake_entity = {"pageid": 999, "statements": {}}
    with patch.object(sdc_sync, "get_entity", return_value=fake_entity):
        result = sdc_sync.check("M999", ("item", "Q19652"), "P6216")
    assert result == (True, "")


def test_check_dpla_only_with_dpla_reference_no_action():
    """A claim that already has DPLA's P459 qualifier AND a DPLA reference
    is fully covered — don't duplicate, don't re-amend."""
    from tools import sdc_sync

    fully_done = _item_statement(
        "M999$done",
        "Q19652",
        qualifiers={"P459": _dpla_p459()},
        references=[_dpla_reference()],
    )
    fake_entity = {"pageid": 999, "statements": {"P6216": [fully_done]}}
    with patch.object(sdc_sync, "get_entity", return_value=fake_entity):
        result = sdc_sync.check("M999", ("item", "Q19652"), "P6216")
    # First element False → don't add new. Second element "" → no
    # ref-stamp needed (statement already has its reference).
    assert result == (False, "")


def test_check_foreign_reference_match_adds_alongside():
    """Even with DPLA-style qualifiers, a foreign reference on the
    matching statement makes it unsafe to amend (the round-trip would
    replace the foreign reference with ours). Add alongside."""
    from tools import sdc_sync

    foreign_ref_stmt = _item_statement(
        "M999$foreignref",
        "Q19652",
        qualifiers={"P459": _dpla_p459()},
        references=[_foreign_reference()],
    )
    fake_entity = {"pageid": 999, "statements": {"P6216": [foreign_ref_stmt]}}
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
    """Same add-alongside behavior for string and monolingualtext
    mainsnak types (P760, P1476, P10358, etc.)."""
    from tools import sdc_sync

    foreign_stmt = {
        "id": "M999$str",
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"type": kind, "value": mainsnak_value},
        },
        "qualifiers": {"P1001": _qual_entity("P1001", "Q30")},
    }
    fake_entity = {"pageid": 999, "statements": {"P760": [foreign_stmt]}}
    with patch.object(sdc_sync, "get_entity", return_value=fake_entity):
        result = sdc_sync.check("M999", (kind, value), "P760")
    assert result == (True, "")


# --------------------------------------------------------------------------
# _run_partner_mode — SystemExit / KeyboardInterrupt diagnostic capture
#
# 11 SDC aborts were observed across 3 days in May 2026 with the warning
# "SDC sync aborted before completion" but NO traceback or exception
# class logged.  The old `except Exception:` only catches Exception-class
# exceptions; SystemExit / KeyboardInterrupt / GeneratorExit (all
# subclasses of BaseException, not Exception) bypassed it entirely.
# Widened to `except BaseException` so the next abort logs a traceback +
# the exception type name, making future occurrences self-diagnosing.
# --------------------------------------------------------------------------


def test_run_partner_mode_logs_traceback_and_type_on_systemexit(
    tmp_path, caplog, monkeypatch
):
    """A SystemExit raised during partner-mode iteration must:
      1. Be caught by the outer handler (not propagate silently)
      2. Get logged with `logging.exception(...)` so the SDC log file
         captures the traceback + exception class name
      3. Be re-raised so the shell pipeline still sees a non-zero exit
         and `notify_pipeline_fail` still fires.
    The 11-abort cluster all WROTE the abort warning (so finally ran)
    but produced no diagnostic, which only makes sense if the exception
    class wasn't `Exception` — exactly what this widened catch fixes.
    """
    import logging as _logging

    from tools import sdc_sync

    ids_file = tmp_path / "ids.txt"
    ids_file.write_text("abcdef01abcdef01abcdef01abcdef01\n")

    fake_s3_client = MagicMock(name="S3Client_instance")
    fake_s3_client.get_sdc_json.side_effect = SystemExit("simulated pywikibot exit")

    notify_complete_calls = []

    with (
        patch.object(sdc_sync, "setup_logging"),
        patch.object(sdc_sync, "notify_phase_start"),
        patch.object(
            sdc_sync,
            "notify_sdc_complete",
            side_effect=lambda **kw: notify_complete_calls.append(kw),
        ),
        # _run_partner_mode imports S3Client inside the function body
        # (`from ingest_wikimedia.s3 import S3Client`).  Patch the
        # constructor in the source module so the inner import picks
        # up the mock.
        patch("ingest_wikimedia.s3.S3Client", return_value=fake_s3_client),
        caplog.at_level(_logging.WARNING, logger="root"),
    ):
        with pytest.raises(SystemExit):
            sdc_sync._run_partner_mode("nara", str(ids_file))

    # The completion notification must NOT have fired — the abort path
    # explicitly suppresses it (otherwise the status reporter would see
    # a `COUNTS:` marker and treat the aborted run as done).
    assert notify_complete_calls == [], (
        f"notify_sdc_complete must not be called on abort; got: {notify_complete_calls!r}"
    )

    # The diagnostic log must include the exception type name so future
    # aborts are self-identifying (was it SystemExit?  KeyboardInterrupt?).
    messages = " | ".join(r.getMessage() for r in caplog.records)
    assert "SDC sync aborted with unhandled exception" in messages
    assert "SystemExit" in messages, (
        f"exception type name must be in the log message for diagnosis; "
        f"got: {messages!r}"
    )

    # The finally-block warning must still fire (the message that the
    # 11 May 2026 aborts were the ONLY log output for).  This is the
    # signal `wikimedia_upload_status` reads as "not complete".
    assert "SDC sync aborted before completion" in messages


def test_run_partner_mode_logs_traceback_on_keyboardinterrupt(
    tmp_path, caplog, monkeypatch
):
    """Same contract for KeyboardInterrupt — also a BaseException
    subclass not caught by `except Exception`.  Default Python signal
    handler raises KeyboardInterrupt on SIGINT, so this covers the
    "process group received SIGINT" hypothesis."""
    import logging as _logging

    from tools import sdc_sync

    ids_file = tmp_path / "ids.txt"
    ids_file.write_text("abcdef01abcdef01abcdef01abcdef01\n")

    fake_s3_client = MagicMock(name="S3Client_instance")
    fake_s3_client.get_sdc_json.side_effect = KeyboardInterrupt()

    with (
        patch.object(sdc_sync, "setup_logging"),
        patch.object(sdc_sync, "notify_phase_start"),
        patch.object(sdc_sync, "notify_sdc_complete"),
        patch("ingest_wikimedia.s3.S3Client", return_value=fake_s3_client),
        caplog.at_level(_logging.WARNING, logger="root"),
    ):
        with pytest.raises(KeyboardInterrupt):
            sdc_sync._run_partner_mode("nara", str(ids_file))

    messages = " | ".join(r.getMessage() for r in caplog.records)
    assert "KeyboardInterrupt" in messages


# --------------------------------------------------------------------------
# Tests for SDC POST helpers raising RuntimeError instead of sys.exit().
#
# Background: the original sys.exit() calls in _post_new_refs /
# _post_new_claims / _reconcile_existing_claims aborted the entire
# partner batch for a single failed Commons write. SystemExit is a
# BaseException, so the per-ordinal `except Exception:` in
# process_one_from_sdc could not catch it. Replacing with
# `raise RuntimeError(...)`:
#   1. Makes the failure self-documenting (mediaid + dpla_id + the
#      response body / underlying exception type).
#   2. Routes through the per-ordinal handler, so the partner batch
#      logs the traceback and continues with the next ordinal instead
#      of losing thousands of items' worth of work.
# --------------------------------------------------------------------------


def test_truncate_helper_returns_under_limit_unchanged_and_truncates_long():
    """Sanity check for the _truncate helper used in the new error messages."""
    from tools import sdc_sync

    assert sdc_sync._truncate("short", limit=100) == "short"
    truncated = sdc_sync._truncate("x" * 1000, limit=10)
    assert truncated.startswith("x" * 10)
    assert "truncated" in truncated
    assert "1000" in truncated  # original length is preserved in the suffix
    assert sdc_sync._truncate(None) == ""


def test_post_new_refs_raises_runtime_error_on_non_success_save(monkeypatch):
    """A non-success wbeditentity refs response must raise RuntimeError,
    not call sys.exit(). The error message must include the mediaid and
    DPLA id so the failure is self-identifying in the SDC log."""
    from tools import sdc_sync

    response = MagicMock()
    response.text = '{"error": {"code": "badtoken", "info": "Invalid token"}}'

    # Pre-populate the module-level refclaims so the helper actually posts.
    monkeypatch.setitem(sdc_sync.refclaims, "claims", [{"some": "ref"}])
    monkeypatch.setattr(sdc_sync, "token", "stub-token", raising=False)
    monkeypatch.setattr(sdc_sync.http, "fetch", lambda *a, **kw: response)

    with pytest.raises(RuntimeError) as excinfo:
        sdc_sync._post_new_refs("M12345", "abcdef01abcdef01abcdef01abcdef01")

    msg = str(excinfo.value)
    assert "M12345" in msg
    assert "abcdef01abcdef01abcdef01abcdef01" in msg
    assert "non-success" in msg


def test_post_new_claims_raises_runtime_error_on_non_success_save(monkeypatch):
    """Same contract for the claims POST helper."""
    from tools import sdc_sync

    response = MagicMock()
    response.text = '{"error": {"code": "ratelimited"}}'

    monkeypatch.setitem(sdc_sync.claims, "claims", [{"some": "claim"}])
    monkeypatch.setattr(sdc_sync, "token", "stub-token", raising=False)
    monkeypatch.setattr(sdc_sync.http, "fetch", lambda *a, **kw: response)

    with pytest.raises(RuntimeError) as excinfo:
        sdc_sync._post_new_claims("M67890", "fedcba98fedcba98fedcba98fedcba98")

    msg = str(excinfo.value)
    assert "M67890" in msg
    assert "fedcba98fedcba98fedcba98fedcba98" in msg
    assert "non-success" in msg


def test_post_new_refs_runtime_error_caught_by_per_ordinal_handler(monkeypatch):
    """The RuntimeError raised from inside _post_new_refs must be a
    plain Exception subclass (not BaseException), so the per-ordinal
    `except Exception:` in process_one_from_sdc catches it and the
    partner batch can continue with the next ordinal.

    This is the key behavioural difference vs the old sys.exit() — old
    code raised SystemExit (BaseException, not caught by the per-ordinal
    handler), so a single failed write tanked the entire partner.
    """
    from tools import sdc_sync

    response = MagicMock()
    response.text = '{"error": {"code": "badtoken"}}'
    monkeypatch.setitem(sdc_sync.refclaims, "claims", [{"some": "ref"}])
    monkeypatch.setattr(sdc_sync, "token", "stub-token", raising=False)
    monkeypatch.setattr(sdc_sync.http, "fetch", lambda *a, **kw: response)

    skipped = False
    try:
        sdc_sync._post_new_refs("M12345", "abcdef01abcdef01abcdef01abcdef01")
    except Exception:
        # This is the same `except Exception:` clause that wraps each
        # ordinal in process_one_from_sdc — catching it here proves the
        # per-ordinal handler will skip rather than abort the partner.
        skipped = True

    assert skipped, (
        "RuntimeError from _post_new_refs must be an Exception subclass so the"
        " per-ordinal handler in process_one_from_sdc catches it (was previously"
        " SystemExit, which bypassed that handler and aborted the whole partner)"
    )
