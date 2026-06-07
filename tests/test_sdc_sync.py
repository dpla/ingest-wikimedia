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

from ingest_wikimedia.tracker import Result


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


def _install_module_globals(
    monkeypatch,
    sdc_sync,
    *,
    refclaims_payload=None,
    claims_payload=None,
    submit_side_effect=None,
    submit_return=None,
):
    """Inject the module-level globals the SDC POST helpers expect AND
    mock the pywikibot path the helpers now write through.

    The POST helpers no longer hit raw ``http.fetch`` — they call
    ``site.simple_request(action=..., ...).submit()``, which is
    pywikibot's high-level write entry point. To exercise the helpers
    without an actual network round-trip we mock the ``site`` module
    global so ``simple_request`` returns a request object whose
    ``submit()`` either returns ``submit_return`` (success) or raises
    ``submit_side_effect`` (the pywikibot ``APIError`` we're modelling).

    ``refclaims`` / ``claims`` are not initialised at import time —
    they're declared ``global`` and populated inside ``process_one`` /
    ``process_one_from_sdc`` before each item, so tests that call the
    POST helpers directly install them here with ``raising=False``.
    """
    monkeypatch.setattr(
        sdc_sync,
        "refclaims",
        {"claims": refclaims_payload if refclaims_payload is not None else []},
        raising=False,
    )
    monkeypatch.setattr(
        sdc_sync,
        "claims",
        {"claims": claims_payload if claims_payload is not None else []},
        raising=False,
    )

    request_mock = MagicMock()
    if submit_side_effect is not None:
        request_mock.submit.side_effect = submit_side_effect
    else:
        request_mock.submit.return_value = (
            submit_return if submit_return is not None else {"success": 1}
        )

    fake_site = MagicMock()
    fake_site.simple_request.return_value = request_mock
    # Pywikibot's lazy CSRF cache surfaces as ``site.tokens["csrf"]``.
    # The migration accesses it; mock it to a deterministic value.
    fake_site.tokens = {"csrf": "stub-csrf-token"}
    monkeypatch.setattr(sdc_sync, "site", fake_site, raising=False)
    return fake_site


def _api_error(code, info=""):
    """Construct a ``pywikibot.exceptions.APIError`` for tests.

    APIError's constructor signature is ``(code, info, **kwargs)`` and
    callers across pywikibot pass extra context (servedby, etc.) as
    kwargs. We only need code + info for assertions.
    """
    import pywikibot.exceptions

    return pywikibot.exceptions.APIError(code=code, info=info)


def test_post_new_refs_raises_runtime_error_on_apierror(monkeypatch):
    """Pywikibot's ``APIError`` for any code OTHER than ``no-such-entity``
    must be re-raised as ``RuntimeError`` carrying mediaid + dpla_id +
    Commons error code so the per-ordinal handler logs a useful traceback.
    """
    from tools import sdc_sync

    _install_module_globals(
        monkeypatch,
        sdc_sync,
        refclaims_payload=[{"some": "ref"}],
        submit_side_effect=_api_error("badtoken", info="Invalid token"),
    )

    with pytest.raises(RuntimeError) as excinfo:
        sdc_sync._post_new_refs("M12345", "abcdef01abcdef01abcdef01abcdef01")

    msg = str(excinfo.value)
    assert "M12345" in msg
    assert "abcdef01abcdef01abcdef01abcdef01" in msg
    # The Commons error code must appear in the RuntimeError message so
    # the per-ordinal ``logging.exception`` captures it without needing
    # to unwrap the ``__cause__`` chain.
    assert "badtoken" in msg, f"expected 'badtoken' in {msg!r}"


def test_post_new_claims_raises_runtime_error_on_apierror(monkeypatch):
    """Same contract for the claims POST helper — non-no-such-entity
    APIErrors come out as RuntimeError with the code in the message."""
    from tools import sdc_sync

    _install_module_globals(
        monkeypatch,
        sdc_sync,
        claims_payload=[{"some": "claim"}],
        submit_side_effect=_api_error("abusefilter-disallowed", info="Rule X tripped."),
    )

    with pytest.raises(RuntimeError) as excinfo:
        sdc_sync._post_new_claims("M67890", "fedcba98fedcba98fedcba98fedcba98")

    msg = str(excinfo.value)
    assert "M67890" in msg
    assert "fedcba98fedcba98fedcba98fedcba98" in msg
    assert "abusefilter-disallowed" in msg, f"expected error code in {msg!r}"


def test_post_new_refs_runtime_error_caught_by_per_ordinal_handler(monkeypatch):
    """The RuntimeError raised from inside ``_post_new_refs`` must be a
    plain ``Exception`` subclass (not ``BaseException``), so the
    ``except Exception:`` clause in ``process_one_from_sdc`` catches it
    and the partner batch can continue with the next ordinal.

    Regression guard against the pre-PR-#263 ``sys.exit()`` behaviour,
    which raised ``SystemExit`` (a ``BaseException``) and bypassed the
    per-ordinal handler — one failed write tanked the entire partner.
    """
    from tools import sdc_sync

    _install_module_globals(
        monkeypatch,
        sdc_sync,
        refclaims_payload=[{"some": "ref"}],
        submit_side_effect=_api_error("badtoken"),
    )

    skipped = False
    try:
        sdc_sync._post_new_refs("M12345", "abcdef01abcdef01abcdef01abcdef01")
    except Exception:
        # Same ``except Exception:`` clause that wraps each ordinal in
        # ``process_one_from_sdc`` — catching it here proves the
        # per-ordinal handler will skip rather than abort the partner.
        skipped = True

    assert skipped, (
        "RuntimeError from _post_new_refs must be an Exception subclass so"
        " the per-ordinal handler in process_one_from_sdc catches it"
    )


def test_post_new_refs_success_path_increments_counter(monkeypatch):
    """The happy path: ``simple_request().submit()`` returns success →
    the SDC_REFS_ADDED counter increments by the number of refs posted."""
    from tools import sdc_sync

    _install_module_globals(
        monkeypatch,
        sdc_sync,
        refclaims_payload=[{"ref": "a"}, {"ref": "b"}, {"ref": "c"}],
        submit_return={"success": 1, "entity": {}},
    )

    before = sdc_sync.tracker.count(Result.SDC_REFS_ADDED)
    sdc_sync._post_new_refs("M12345", "abcdef01abcdef01abcdef01abcdef01")
    after = sdc_sync.tracker.count(Result.SDC_REFS_ADDED)
    assert after == before + 3, (
        f"SDC_REFS_ADDED should bump by 3; before={before}, after={after}"
    )


def test_post_new_claims_success_path_increments_counter(monkeypatch):
    """Same happy-path contract for claims."""
    from tools import sdc_sync

    _install_module_globals(
        monkeypatch,
        sdc_sync,
        claims_payload=[{"c": "a"}, {"c": "b"}],
        submit_return={"success": 1, "entity": {}},
    )

    before = sdc_sync.tracker.count(Result.SDC_CLAIMS_ADDED)
    sdc_sync._post_new_claims("M12345", "abcdef01abcdef01abcdef01abcdef01")
    after = sdc_sync.tracker.count(Result.SDC_CLAIMS_ADDED)
    assert after == before + 2


def test_post_new_refs_uses_pywikibot_simple_request_with_csrf_token(monkeypatch):
    """Verify the helper actually routes through pywikibot's
    ``simple_request`` and passes the CSRF token from ``site.tokens``
    (rather than a stale module-global ``token``, which the migration
    deleted).
    """
    from tools import sdc_sync

    fake_site = _install_module_globals(
        monkeypatch,
        sdc_sync,
        refclaims_payload=[{"some": "ref"}],
        submit_return={"success": 1},
    )

    sdc_sync._post_new_refs("M12345", "abcdef01abcdef01abcdef01abcdef01")

    assert fake_site.simple_request.call_count == 1
    kwargs = fake_site.simple_request.call_args.kwargs
    assert kwargs["action"] == "wbeditentity"
    assert kwargs["id"] == "M12345"
    assert kwargs["bot"] is True
    assert kwargs["token"] == "stub-csrf-token"
    # The encoded claims payload should be the JSON of refclaims.
    assert "ref" in kwargs["data"]


# --------------------------------------------------------------------------
# `no-such-entity` is treated as a clean skip, NOT a failure.
#
# When pywikibot raises ``APIError(code="no-such-entity")``, the
# MediaInfo entity for the staged M-id doesn't exist — most commonly
# because the file page was deleted by a Commons curator as a duplicate,
# or because this is an SDC-only run for a file that wasn't uploaded
# through our pipeline. Neither is the SDC phase's fault, and re-running
# wouldn't help — the entity isn't coming back via retry. The phase
# converts this specific APIError code into ``_MissingEntityError``,
# which the per-ordinal handler logs at INFO (not ERROR) and counts
# under ``SDC_ORDINALS_SKIPPED_MISSING_ENTITY`` (not ``..._ERROR``).
# --------------------------------------------------------------------------


def test_post_new_claims_no_such_entity_raises_missing_entity_error(monkeypatch):
    """Claims POST helper must raise ``_MissingEntityError`` (not
    ``RuntimeError``) when pywikibot reports ``no-such-entity``."""
    from tools import sdc_sync

    _install_module_globals(
        monkeypatch,
        sdc_sync,
        claims_payload=[{"some": "claim"}],
        submit_side_effect=_api_error("no-such-entity", info="⧼no-such-entity⧽"),
    )

    with pytest.raises(sdc_sync._MissingEntityError) as excinfo:
        sdc_sync._post_new_claims("M12345", "abcdef01abcdef01abcdef01abcdef01")

    assert "M12345" in str(excinfo.value)


def test_post_new_refs_no_such_entity_raises_missing_entity_error(monkeypatch):
    """Same contract for the refs POST helper."""
    from tools import sdc_sync

    _install_module_globals(
        monkeypatch,
        sdc_sync,
        refclaims_payload=[{"some": "ref"}],
        submit_side_effect=_api_error("no-such-entity"),
    )

    with pytest.raises(sdc_sync._MissingEntityError) as excinfo:
        sdc_sync._post_new_refs("M67890", "fedcba98fedcba98fedcba98fedcba98")

    assert "M67890" in str(excinfo.value)


def test_submit_sdc_write_translates_no_such_entity_for_wbremoveclaims(monkeypatch):
    """The shared write helper translates ``APIError(no-such-entity)``
    for ANY action — proving the wbremoveclaims path
    (``_reconcile_existing_claims`` removals block) inherits the same
    clean-skip contract as the wbeditentity path, since they both go
    through ``_submit_sdc_write``.
    """
    from tools import sdc_sync

    fake_site = _install_module_globals(
        monkeypatch,
        sdc_sync,
        submit_side_effect=_api_error("no-such-entity"),
    )

    with pytest.raises(sdc_sync._MissingEntityError) as excinfo:
        sdc_sync._submit_sdc_write(
            "wbremoveclaims",
            "M12345",
            "abcdef01abcdef01abcdef01abcdef01",
            claim="M12345$id-1|M12345$id-2",
        )

    assert "M12345" in str(excinfo.value)
    assert fake_site.simple_request.call_args.kwargs["action"] == "wbremoveclaims"


def test_submit_sdc_write_runtime_error_message_names_the_action(monkeypatch):
    """The RuntimeError message includes the action name so the
    per-ordinal log distinguishes ``wbeditentity failed`` from
    ``wbremoveclaims failed`` without needing to inspect the traceback.
    """
    from tools import sdc_sync

    _install_module_globals(
        monkeypatch,
        sdc_sync,
        submit_side_effect=_api_error("permissiondenied", info="Denied."),
    )

    with pytest.raises(RuntimeError) as excinfo:
        sdc_sync._submit_sdc_write(
            "wbremoveclaims",
            "M99999",
            "deadbeefdeadbeefdeadbeefdeadbeef",
            claim="M99999$x",
        )

    msg = str(excinfo.value)
    assert "wbremoveclaims" in msg
    assert "M99999" in msg
    assert "permissiondenied" in msg


def test_missing_entity_error_is_not_a_runtime_error():
    """The per-ordinal handler distinguishes ``_MissingEntityError`` from
    generic ``RuntimeError`` / ``Exception``. Keep the class hierarchy
    explicit: it must be an ``Exception`` (so the per-ordinal handler
    catches it at all) but NOT a ``RuntimeError`` subclass, so the
    per-ordinal handler's separate ``except _MissingEntityError:`` arm
    (logged at INFO + counted under SKIPPED_MISSING_ENTITY) reliably
    fires before the broader ``except Exception:`` arm (logged at ERROR
    + counted under SKIPPED_ERROR).
    """
    from tools import sdc_sync

    assert issubclass(sdc_sync._MissingEntityError, Exception)
    assert not issubclass(sdc_sync._MissingEntityError, RuntimeError)


def test_legacy_process_one_treats_missing_entity_as_clean_skip(monkeypatch):
    """``process_one()`` is the legacy entry point used by ``--file`` /
    ``--cat`` / ``--list`` runs (separate from partner mode's
    ``process_one_from_sdc``). It must apply the same
    ``_MissingEntityError`` → skip contract — otherwise a Commons
    ``no-such-entity`` response would abort the legacy run, exactly the
    cascade the PR's partner-mode handler avoids.
    """
    from tools import sdc_sync

    # parsed() returns a real DPLA-shaped tuple so process_one proceeds
    # past the "missing id" early return.
    monkeypatch.setattr(
        sdc_sync,
        "parsed",
        lambda dpla_id, dpla_api: (
            "http://example/url",  # url
            ["desc"],  # descs
            ["2020"],  # dates
            ["title"],  # titles
            "nara",  # hub
            ["local-id-1"],  # local_ids
            "National Archives",  # institution
            "http://creativecommons.org/publicdomain/mark/1.0/",  # rs
            ["creator"],  # creators
            [("subject", None)],  # subjects
            ["12345"],  # naids
            "access",  # access
            "level",  # level
        ),
    )
    monkeypatch.setattr(sdc_sync, "dpla_api", "stub-api-key", raising=False)
    monkeypatch.setattr(sdc_sync, "invalidate_entity", lambda *_a, **_k: None)
    monkeypatch.setattr(sdc_sync, "get_entity", lambda *_a, **_k: {})
    # Skip every add_* helper — they call check() which hits the real
    # Commons API for entity reads. Replace them with no-ops.
    for name in [
        "add_rs",
        "add_id",
        "add_title",
        "add_collection",
        "add_creator",
        "add_date",
        "add_subject",
        "add_subject_entity",
        "add_desc",
        "add_contributed",
        "add_source",
        "add_local_id",
        "add_naid",
        "add_access",
        "add_level",
    ]:
        monkeypatch.setattr(sdc_sync, name, lambda *_a, **_k: None)
    # `dpla_claims` calls `_reconcile_existing_claims` — stub it too.
    monkeypatch.setattr(sdc_sync, "dpla_claims", lambda *_a, **_k: None)
    # The actual POST helper raises the missing-entity error.
    monkeypatch.setattr(
        sdc_sync,
        "_post_new_refs",
        lambda *_a, **_k: (_ for _ in ()).throw(sdc_sync._MissingEntityError("M12345")),
    )
    monkeypatch.setattr(sdc_sync, "_post_new_claims", lambda *_a, **_k: None)

    counter_before = sdc_sync.tracker.count(Result.SDC_ORDINALS_SKIPPED_MISSING_ENTITY)

    # Should NOT raise — process_one must catch _MissingEntityError and
    # return cleanly.
    sdc_sync.process_one("M12345", "abcdef01abcdef01abcdef01abcdef01")

    counter_after = sdc_sync.tracker.count(Result.SDC_ORDINALS_SKIPPED_MISSING_ENTITY)
    assert counter_after == counter_before + 1, (
        "process_one must increment SDC_ORDINALS_SKIPPED_MISSING_ENTITY on"
        f" the no-such-entity skip path; before={counter_before}, after={counter_after}"
    )


# --------------------------------------------------------------------------
# formattedclaim now produces its dict via pywikibot.Claim + Claim.toJSON().
# The function-boundary contract is unchanged — it still returns a
# wbeditentity wire-format dict that the add_* callers append directly
# to claims["claims"] — but the body is now type-safe (pywikibot.Claim
# validates value/property combinations at build time) and idiomatic
# (uses pywikibot.WbMonolingualText, pywikibot.WbTime, pywikibot.ItemPage
# instead of hand-built dicts).
#
# The order keys pywikibot's toJSON emits (qualifiers-order, snaks-order)
# are stripped so the existing inline-qualifier mutation pattern in
# add_date / add_subject_entity / etc. (claim["qualifiers"][prop] = ...)
# stays correct without each callsite having to also update the order
# list.
# --------------------------------------------------------------------------


def test_set_claim_target_dispatches_each_value_type(monkeypatch):
    """Each of the 4 value_types our 17 add_* helpers use must dispatch
    to the right ``setTarget`` argument shape.

    Verifies the value-type → pywikibot type translation matches the
    contract the previous hand-built dict expressed inline:

    - ``wikibase-entityid`` → ``ItemPage(repo, "Q...")``
    - ``string``            → raw string
    - ``monolingualtext``   → ``WbMonolingualText(text, language)``

    ``pywikibot.ItemPage`` validates that its ``site`` arg is a real
    ``DataSite`` (raises ``TypeError`` on a ``MagicMock``), so it's
    monkeypatched here to a stub that returns a labelled sentinel —
    the assertion is on the dispatch, not on ``ItemPage`` internals.
    """
    from tools import sdc_sync
    from unittest.mock import MagicMock
    import pywikibot

    fake_claim = MagicMock()
    fake_repo = MagicMock()

    # wikibase-entityid — patch ItemPage so the real constructor doesn't
    # try to validate the MagicMock repo. ``patch.object`` gives us a
    # default ``MagicMock`` whose ``return_value`` we identity-check
    # against the value ``setTarget`` receives — more conventional than
    # a ``side_effect`` lambda returning a sentinel tuple.
    with patch.object(pywikibot, "ItemPage") as mock_item_page:
        sdc_sync._set_claim_target(
            fake_claim,
            fake_repo,
            {"entity-type": "item", "numeric-id": 19652},
            "wikibase-entityid",
        )
    mock_item_page.assert_called_once_with(fake_repo, "Q19652")
    assert fake_claim.setTarget.call_args.args[0] is mock_item_page.return_value

    fake_claim.reset_mock()

    # string — no pywikibot wrapping; raw string passed straight through.
    sdc_sync._set_claim_target(fake_claim, fake_repo, "abc123", "string")
    assert fake_claim.setTarget.call_args.args[0] == "abc123"

    fake_claim.reset_mock()

    # monolingualtext — WbMonolingualText is a plain data class with no
    # site validation, so it doesn't need monkeypatching.
    sdc_sync._set_claim_target(
        fake_claim,
        fake_repo,
        {"text": "Hello", "language": "en"},
        "monolingualtext",
    )
    target = fake_claim.setTarget.call_args.args[0]
    assert isinstance(target, pywikibot.WbMonolingualText)
    assert target.text == "Hello"
    assert target.language == "en"


def test_set_claim_target_rejects_unknown_value_type():
    """The translator helper must raise on a value_type it doesn't know,
    so a future caller can't silently produce malformed wire data.
    ``"time"`` is intentionally rejected here because the only time
    callsite (``add_date``) passes ``"somevalue"``, which is handled by
    ``formattedclaim`` before reaching this translator.
    """
    from tools import sdc_sync
    from unittest.mock import MagicMock

    claim = MagicMock()
    repo = MagicMock()
    with pytest.raises(ValueError, match="unsupported value_type"):
        sdc_sync._set_claim_target(claim, repo, "x", "globe-coordinate")
    with pytest.raises(ValueError, match="unsupported value_type"):
        sdc_sync._set_claim_target(claim, repo, {}, "time")


# Regression coverage for the Special:EntityData UA-403 silent-failure bug
# in `_reconcile_existing_claims` (phab T400119).


def _stmt(stmt_id, prop, snaktype, value, qualifiers=None, references=None):
    """Build a Commons statement dict in the wbgetentities shape."""
    if snaktype == "somevalue":
        mainsnak = {"snaktype": "somevalue", "property": prop}
    elif prop == "P760":
        mainsnak = {
            "snaktype": "value",
            "property": prop,
            "datavalue": {"value": value, "type": "string"},
        }
    else:
        mainsnak = {
            "snaktype": "value",
            "property": prop,
            "datavalue": {
                "value": {
                    "entity-type": "item",
                    "numeric-id": int(value.replace("Q", "")),
                    "id": value,
                },
                "type": "wikibase-entityid",
            },
        }
    s = {"id": stmt_id, "mainsnak": mainsnak, "type": "statement", "rank": "normal"}
    if qualifiers is not None:
        s["qualifiers"] = qualifiers
    if references is not None:
        s["references"] = references
    return s


def test_reconciler_queues_removal_for_stale_p170_string():
    """Stale DPLA-referenced P170 qualifier (value not in `expected`) is queued for wbremoveclaims."""
    from tools import sdc_sync

    stale_stmt = _stmt(
        stmt_id="M999$STALE",
        prop="P170",
        snaktype="somevalue",
        value=None,
        qualifiers={
            "P459": _dpla_p459(),
            "P2093": _qual_string("P2093", "U.S. Senate. 3/4/1789"),
        },
        references=[_dpla_reference()],
    )
    entity = {"pageid": 999, "statements": {"P170": [stale_stmt]}}
    expected = {"P170": ["U.S. Senate. (03/04/1789)"]}

    submit_calls = []

    def fake_submit(action, mediaid, dpla_id, **params):
        submit_calls.append((action, mediaid, params))

    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
        patch.object(sdc_sync, "_submit_sdc_write", side_effect=fake_submit),
    ):
        sdc_sync._reconcile_existing_claims(
            "M999", "abc1234567890abcdef1234567890abcd", expected
        )

    assert len(submit_calls) == 1
    action, mediaid, params = submit_calls[0]
    assert action == "wbremoveclaims"
    assert mediaid == "M999"
    assert params["claim"] == "M999$STALE", (
        f"reconciler should queue the stale claim for removal; got {params['claim']!r}"
    )


def test_reconciler_keeps_claim_when_value_matches_expected():
    """Healthy DPLA-referenced claim whose value is in `expected` is left alone."""
    from tools import sdc_sync

    good_stmt = _stmt(
        stmt_id="M999$KEEP",
        prop="P170",
        snaktype="somevalue",
        value=None,
        qualifiers={
            "P459": _dpla_p459(),
            "P2093": _qual_string("P2093", "U.S. Senate. (03/04/1789)"),
        },
        references=[_dpla_reference()],
    )
    entity = {"pageid": 999, "statements": {"P170": [good_stmt]}}
    expected = {"P170": ["U.S. Senate. (03/04/1789)"]}

    submit_calls = []

    def fake_submit(*args, **kwargs):
        submit_calls.append((args, kwargs))

    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
        patch.object(sdc_sync, "_submit_sdc_write", side_effect=fake_submit),
    ):
        sdc_sync._reconcile_existing_claims(
            "M999", "abc1234567890abcdef1234567890abcd", expected
        )

    assert submit_calls == [], (
        f"reconciler should not queue removal for a healthy claim; got {submit_calls!r}"
    )


def test_reconciler_ignores_foreign_claim_without_dpla_reference():
    """Foreign (non-DPLA-referenced) claim is never queued for removal, even when its value isn't in `expected`."""
    from tools import sdc_sync

    foreign_stmt = _stmt(
        stmt_id="M999$FOREIGN",
        prop="P170",
        snaktype="somevalue",
        value=None,
        qualifiers={"P2093": _qual_string("P2093", "Some User's Author Name")},
        references=[_foreign_reference()],
    )
    entity = {"pageid": 999, "statements": {"P170": [foreign_stmt]}}
    expected = {"P170": ["U.S. Senate. (03/04/1789)"]}

    submit_calls = []

    def fake_submit(*args, **kwargs):
        submit_calls.append((args, kwargs))

    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
        patch.object(sdc_sync, "_submit_sdc_write", side_effect=fake_submit),
    ):
        sdc_sync._reconcile_existing_claims(
            "M999", "abc1234567890abcdef1234567890abcd", expected
        )

    assert submit_calls == [], (
        "reconciler must not touch foreign (non-DPLA-referenced) claims"
    )


def test_reconciler_propagates_get_entity_error():
    """Entity-fetch errors propagate (no silent fallback to an empty entity)."""
    from tools import sdc_sync

    with (
        patch.object(
            sdc_sync,
            "get_entity",
            side_effect=RuntimeError("simulated wbgetentities failure"),
        ),
        patch.object(sdc_sync, "invalidate_entity"),
        pytest.raises(RuntimeError, match="simulated wbgetentities failure"),
    ):
        sdc_sync._reconcile_existing_claims(
            "M999", "abc1234567890abcdef1234567890abcd", {"P170": ["x"]}
        )


# ---------------------------------------------------------------------------
# _raise_if_missing_entity / get_entity
#
# Reads are now expected to translate ``no-such-entity`` to
# ``_MissingEntityError`` so the partner-mode boundary categorises deleted
# files as MISSING_ENTITY rather than generic ERROR.
# ---------------------------------------------------------------------------


def test_raise_if_missing_entity_translates_no_such_entity():
    """APIError(code='no-such-entity') becomes _MissingEntityError."""
    from tools import sdc_sync

    err = _api_error("no-such-entity", info="No entity")
    with pytest.raises(sdc_sync._MissingEntityError):
        sdc_sync._raise_if_missing_entity(err, "M999")


def test_raise_if_missing_entity_silent_on_other_apierror():
    """Other APIError codes pass through so the caller can wrap/re-raise."""
    from tools import sdc_sync

    # No raise → returns None implicitly.
    assert sdc_sync._raise_if_missing_entity(_api_error("maxlag"), "M999") is None


def test_get_entity_translates_no_such_entity(monkeypatch):
    """When wbgetentities returns no-such-entity for a deleted Commons file,
    get_entity must raise _MissingEntityError so the partner-mode boundary
    increments SDC_ORDINALS_SKIPPED_MISSING_ENTITY (not _ERROR)."""
    from tools import sdc_sync

    fake_request = MagicMock()
    fake_request.submit.side_effect = _api_error("no-such-entity")
    fake_site = MagicMock()
    fake_site.simple_request.return_value = fake_request
    monkeypatch.setattr(sdc_sync, "site", fake_site, raising=False)
    monkeypatch.setattr(sdc_sync, "_entity_cache", {}, raising=False)

    with pytest.raises(sdc_sync._MissingEntityError):
        sdc_sync.get_entity("M999")


def test_get_entity_propagates_other_apierrors(monkeypatch):
    """Non-missing APIErrors (maxlag, badtoken, etc.) propagate so the
    per-ordinal boundary catches them as generic errors."""
    import pywikibot.exceptions

    from tools import sdc_sync

    fake_request = MagicMock()
    fake_request.submit.side_effect = _api_error("maxlag", info="DB lag")
    fake_site = MagicMock()
    fake_site.simple_request.return_value = fake_request
    monkeypatch.setattr(sdc_sync, "site", fake_site, raising=False)
    monkeypatch.setattr(sdc_sync, "_entity_cache", {}, raising=False)

    with pytest.raises(pywikibot.exceptions.APIError, match="maxlag"):
        sdc_sync.get_entity("M999")


# ---------------------------------------------------------------------------
# _safe_process_one — per-file exception boundary for the legacy
# --list / --files / --cat loops, mirroring _run_partner_mode's
# per-ordinal boundary. Unit-tested directly here; integration with main()
# is exercised by hand against the three call sites.
# ---------------------------------------------------------------------------


def test_safe_process_one_passes_through_on_success(monkeypatch):
    """No exception → no tracker increment."""
    from tools import sdc_sync

    fake_tracker = MagicMock()
    monkeypatch.setattr(sdc_sync, "tracker", fake_tracker)
    monkeypatch.setattr(sdc_sync, "process_one", lambda *a: None)

    sdc_sync._safe_process_one("M1", "dpla-1")

    fake_tracker.increment.assert_not_called()


def test_safe_process_one_categorises_missing_entity(monkeypatch):
    """_MissingEntityError → SDC_ORDINALS_SKIPPED_MISSING_ENTITY, not the
    generic ERROR bucket. Without this branch, the new get_entity
    translation (which can raise _MissingEntityError from process_one's
    pre-warm at line 1950, OUTSIDE its own try/except at line 2015)
    would be miscounted as a generic error."""
    from tools import sdc_sync

    fake_tracker = MagicMock()
    monkeypatch.setattr(sdc_sync, "tracker", fake_tracker)

    def raise_missing(*a):
        raise sdc_sync._MissingEntityError("M1")

    monkeypatch.setattr(sdc_sync, "process_one", raise_missing)

    sdc_sync._safe_process_one("M1", "dpla-1")

    fake_tracker.increment.assert_called_once_with(
        Result.SDC_ORDINALS_SKIPPED_MISSING_ENTITY
    )


def test_safe_process_one_categorises_generic_error(monkeypatch):
    """Any other Exception → SDC_ORDINALS_SKIPPED_ERROR + traceback logged
    (so the loop continues to the next file)."""
    from tools import sdc_sync

    fake_tracker = MagicMock()
    monkeypatch.setattr(sdc_sync, "tracker", fake_tracker)

    def raise_other(*a):
        raise RuntimeError("simulated APIError")

    monkeypatch.setattr(sdc_sync, "process_one", raise_other)

    # Should NOT propagate; loops upstream rely on this swallowing-and-continuing.
    sdc_sync._safe_process_one("M1", "dpla-1")

    fake_tracker.increment.assert_called_once_with(Result.SDC_ORDINALS_SKIPPED_ERROR)


# ---------------------------------------------------------------------------
# pywikibot retry budget — bounded by _initialize()
# ---------------------------------------------------------------------------


def test_initialize_pins_pywikibot_retry_budget(monkeypatch, tmp_path):
    """A hung Commons endpoint must not stall a single API call for the
    ~30-minute pywikibot default. _initialize() pins max_retries +
    retry_wait + retry_max to the module's _PYWIKIBOT_* constants so
    the worst-case stall is bounded (~9 min)."""
    import pywikibot

    from tools import sdc_sync

    # monkeypatch.setattr registers teardown BEFORE the mutation, so a
    # mid-test failure can't leak polluted config to other tests.
    monkeypatch.setattr(pywikibot.config, "max_retries", 15)
    monkeypatch.setattr(pywikibot.config, "retry_wait", 5)
    monkeypatch.setattr(pywikibot.config, "retry_max", 120)

    # _initialize() reads config.toml + rights.json from _REPO_ROOT; stub both.
    (tmp_path / "config.toml").write_text('dpla_api_key = "stub"\n')
    (tmp_path / "rights.json").write_text("{}")
    monkeypatch.setattr(sdc_sync, "_REPO_ROOT", str(tmp_path))
    monkeypatch.setattr(
        sdc_sync,
        "_build_parser",
        lambda: MagicMock(
            parse_args=lambda: MagicMock(method=None, from_s3=None),
        ),
    )
    monkeypatch.setattr(pywikibot, "Site", MagicMock)
    monkeypatch.setattr(
        sdc_sync.requests, "get", lambda *a, **k: MagicMock(json=lambda: {})
    )

    sdc_sync._initialize()

    # Pin equality, not <=, so any future loosening of the constants is
    # a deliberate edit reviewers see — not an invisible drift.
    assert pywikibot.config.max_retries == sdc_sync._PYWIKIBOT_MAX_RETRIES
    assert pywikibot.config.retry_wait == sdc_sync._PYWIKIBOT_RETRY_WAIT
    assert pywikibot.config.retry_max == sdc_sync._PYWIKIBOT_RETRY_MAX


# ---------------------------------------------------------------------------
# Opportunistic date parsing — time-typed P571 claims and the migration
# from old somevalue+P1932 statements. The reconciler/check side has to
# treat the canonical (time, precision) key as DIFFERENT from any P1932
# string so a re-sync after the parser ships removes the old somevalue
# claim and adds the new value-typed one in a single cycle.
# ---------------------------------------------------------------------------


def _time_value(time_str, precision):
    """Build a Wikibase time-datavalue ``value`` dict for tests."""
    return {
        "time": time_str,
        "precision": precision,
        "before": 0,
        "after": 0,
        "timezone": 0,
        "calendarmodel": "http://www.wikidata.org/entity/Q1985727",
    }


def _value_typed_p571_claim(stmt_id, time_str, precision, stated_as):
    """Construct a value-typed P571 claim (the new shape) for tests."""
    return {
        "id": stmt_id,
        "type": "statement",
        "rank": "normal",
        "mainsnak": {
            "snaktype": "value",
            "property": "P571",
            "datavalue": {"value": _time_value(time_str, precision), "type": "time"},
        },
        "qualifiers": {
            "P459": _dpla_p459(),
            "P1932": _qual_string("P1932", stated_as),
        },
        "references": [_dpla_reference()],
    }


def _somevalue_p571_claim(stmt_id, stated_as):
    """Construct an old-shape P571 claim (somevalue + P1932) for tests."""
    return _stmt(
        stmt_id=stmt_id,
        prop="P571",
        snaktype="somevalue",
        value=None,
        qualifiers={
            "P459": _dpla_p459(),
            "P1932": _qual_string("P1932", stated_as),
        },
        references=[_dpla_reference()],
    )


def test_time_comparable_uses_only_time_and_precision():
    """``timezone``, ``before``, ``after``, ``calendarmodel`` are constants
    in DPLA's writes; including them in the comparable key would
    silently churn statements on every re-sync if any drift crept in."""
    from tools import sdc_sync

    v = _time_value("+1945-01-01T00:00:00Z", 9)
    assert sdc_sync._time_comparable(v) == "+1945-01-01T00:00:00Z|P9"


def test_time_comparable_distinguishes_precision():
    """Same time, different precision → different comparable. ``1945``
    (year) and ``1945-01-01`` (day) must NOT collapse to the same key —
    they're different facts about the same item."""
    from tools import sdc_sync

    year = sdc_sync._time_comparable(_time_value("+1945-01-01T00:00:00Z", 9))
    day = sdc_sync._time_comparable(_time_value("+1945-01-01T00:00:00Z", 11))
    assert year != day


def test_extract_comparable_value_handles_value_typed_time():
    """``_extract_comparable_value`` must produce the same canonical key
    for sdc.json-side claims as the reconciler extracts from Commons
    statements; otherwise expected/observed never line up and the
    reconciler removes valid claims (or fails to remove stale ones)."""
    from tools import sdc_sync

    claim = _value_typed_p571_claim("Z$1", "+1945-01-01T00:00:00Z", 9, "1945")
    assert sdc_sync._extract_comparable_value(claim) == "+1945-01-01T00:00:00Z|P9"


def test_extract_comparable_value_still_handles_somevalue_p571():
    """Backward-compatibility: existing somevalue+P1932 claims (still on
    Commons until the migration completes) continue to compare by the
    P1932 stated-as string."""
    from tools import sdc_sync

    claim = _somevalue_p571_claim("Z$2", "During the Gilded Age")
    assert sdc_sync._extract_comparable_value(claim) == "During the Gilded Age"


def test_reconciler_migrates_old_somevalue_to_new_value_typed(monkeypatch):
    """End-to-end idempotency check the user called out explicitly:

      * Commons currently holds the OLD somevalue+P1932="1945" claim
        (DPLA-authored from a prior healthy run).
      * The new sdc.json holds a value-typed P571 with the same
        underlying date.
      * The reconciler MUST queue the old somevalue claim for removal
        (its comparable string "1945" is not in expected[P571], which
        now carries the canonical time key).
      * The matching ADD side (``_post_new_claims``) handles inserting
        the new value-typed claim; the reconciler's job is purely to
        clean up the stale half.

    One reconcile cycle migrates the file cleanly without leaving a
    duplicate date statement on Commons."""
    from tools import sdc_sync

    # sdc.json's expected map (built from a value-typed P571 claim).
    new_claim = _value_typed_p571_claim(
        "WILL_BE_FRESH$1", "+1945-01-01T00:00:00Z", 9, "1945"
    )
    expected = sdc_sync._build_expected_from_sdc({"claims": [new_claim]})
    assert expected == {"P571": ["+1945-01-01T00:00:00Z|P9"]}

    # Commons-side state: the OLD somevalue+P1932="1945" claim.
    stale_old = _somevalue_p571_claim("M999$OLD", "1945")
    entity = {"pageid": 999, "statements": {"P571": [stale_old]}}

    submit_calls = []

    def fake_submit(action, mediaid, dpla_id, **params):
        submit_calls.append((action, params.get("claim")))

    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
        patch.object(sdc_sync, "_submit_sdc_write", side_effect=fake_submit),
    ):
        sdc_sync._reconcile_existing_claims(
            "M999", "abc1234567890abcdef1234567890abcd", expected
        )

    assert submit_calls == [("wbremoveclaims", "M999$OLD")], submit_calls


def test_reconciler_leaves_value_typed_claim_alone_when_unchanged(monkeypatch):
    """No-op after migration: a value-typed P571 on Commons matches the
    same canonical key in expected, so the reconciler doesn't queue any
    removal."""
    from tools import sdc_sync

    new_claim = _value_typed_p571_claim(
        "WILL_BE_FRESH$1", "+1945-01-01T00:00:00Z", 9, "1945"
    )
    expected = sdc_sync._build_expected_from_sdc({"claims": [new_claim]})

    on_commons = _value_typed_p571_claim(
        "M999$LIVE", "+1945-01-01T00:00:00Z", 9, "1945"
    )
    entity = {"pageid": 999, "statements": {"P571": [on_commons]}}

    submit_calls = []

    def fake_submit(*a, **k):
        submit_calls.append((a, k))

    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
        patch.object(sdc_sync, "_submit_sdc_write", side_effect=fake_submit),
    ):
        sdc_sync._reconcile_existing_claims(
            "M999", "abc1234567890abcdef1234567890abcd", expected
        )

    assert submit_calls == []


def test_reconciler_removes_value_typed_claim_when_dpla_dropped_the_date(monkeypatch):
    """When DPLA drops a date (sdc.json no longer carries that P571 at
    all), the reconciler must clean up the prior value-typed claim too
    — same contract as for somevalue claims. Regression guard against
    the dtype=time branch in the value-side extraction loop being
    forgotten."""
    from tools import sdc_sync

    # New sdc.json has NO P571 claim at all.
    expected = sdc_sync._build_expected_from_sdc({"claims": []})
    assert expected == {}

    stale_live = _value_typed_p571_claim(
        "M999$STALE", "+1945-01-01T00:00:00Z", 9, "1945"
    )
    entity = {"pageid": 999, "statements": {"P571": [stale_live]}}

    submit_calls = []

    def fake_submit(action, mediaid, dpla_id, **params):
        submit_calls.append((action, params.get("claim")))

    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
        patch.object(sdc_sync, "_submit_sdc_write", side_effect=fake_submit),
    ):
        sdc_sync._reconcile_existing_claims(
            "M999", "abc1234567890abcdef1234567890abcd", expected
        )

    assert submit_calls == [("wbremoveclaims", "M999$STALE")], submit_calls


def test_check_time_kind_recognises_existing_value_typed_match():
    """When the new sdc.json's value-typed P571 already exists on
    Commons with the same canonical key AND is DPLA-authored,
    ``check()`` returns ``(False, ref)`` — i.e. "don't re-add". The
    no-op behavior after migration relies on this."""
    from tools import sdc_sync

    existing = _value_typed_p571_claim("M999$LIVE", "+1945-01-01T00:00:00Z", 9, "1945")
    entity = {"pageid": 999, "statements": {"P571": [existing]}}

    with patch.object(sdc_sync, "get_entity", return_value=entity):
        result = sdc_sync.check("M999", ("time", "+1945-01-01T00:00:00Z|P9"), "P571")

    assert result == (False, "")


def test_check_time_kind_does_not_match_old_somevalue_claim():
    """The migration relies on this: the new value-typed claim is added
    even when an OLD somevalue+P1932 claim exists for the same date
    (so ``_post_new_claims`` runs first to insert the new shape;
    ``_reconcile_existing_claims`` then removes the old)."""
    from tools import sdc_sync

    old_somevalue = _somevalue_p571_claim("M999$OLD", "1945")
    entity = {"pageid": 999, "statements": {"P571": [old_somevalue]}}

    with patch.object(sdc_sync, "get_entity", return_value=entity):
        result = sdc_sync.check("M999", ("time", "+1945-01-01T00:00:00Z|P9"), "P571")

    # First element True = "go ahead and add the new claim".
    assert result[0] is True


def test_reconciler_queues_removal_for_malformed_commons_time_value():
    """Defensive: a community editor (or partial import) could leave a P571
    statement with snaktype=value, type=time, but a value dict missing
    ``time`` or ``precision``. Without the guard, ``_time_comparable``
    raises KeyError, the for-loop unwinds, and the WHOLE reconciler
    crashes for this file (and — in a fresh process — silently leaves
    the remaining files in the batch unreconciled).

    With the guard, the malformed statement is queued for removal
    (matching the somevalue-missing-qualifier branch's behavior), the
    loop continues, and the rest of the reconciler runs."""
    from tools import sdc_sync

    expected = {"P571": ["+1945-01-01T00:00:00Z|P9"]}
    malformed_stmt = {
        "id": "M999$BORK",
        "type": "statement",
        "rank": "normal",
        "mainsnak": {
            "snaktype": "value",
            "property": "P571",
            # datavalue type is "time" but the inner value is missing "precision"
            "datavalue": {"value": {"time": "+1945-01-01T00:00:00Z"}, "type": "time"},
        },
        "qualifiers": {"P459": _dpla_p459()},
        "references": [_dpla_reference()],
    }
    entity = {"pageid": 999, "statements": {"P571": [malformed_stmt]}}

    submit_calls = []

    def fake_submit(action, mediaid, dpla_id, **params):
        submit_calls.append((action, params.get("claim")))

    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
        patch.object(sdc_sync, "_submit_sdc_write", side_effect=fake_submit),
    ):
        sdc_sync._reconcile_existing_claims(
            "M999", "abc1234567890abcdef1234567890abcd", expected
        )

    assert submit_calls == [("wbremoveclaims", "M999$BORK")], submit_calls


def test_check_time_kind_does_not_crash_on_malformed_commons_value():
    """A Commons statement with snaktype=value and dtype=time but a
    malformed value dict (missing ``precision``) must not crash
    ``check()``. The KeyError would otherwise propagate past
    ``process_one_from_sdc``'s APIError-only handler, abort the entire
    ordinal, and prevent ``_reconcile_existing_claims`` from cleaning up
    the malformed statement.

    Expected behavior: ``check()`` treats the malformed statement as
    non-matching, returns True (add the new claim), and the reconciler's
    own defensive guard later queues the malformed statement for
    removal."""
    from tools import sdc_sync

    malformed_stmt = {
        "id": "M999$BORK",
        "type": "statement",
        "rank": "normal",
        "mainsnak": {
            "snaktype": "value",
            "property": "P571",
            # datavalue type is "time" but inner value is missing "precision"
            "datavalue": {"value": {"time": "+1945-01-01T00:00:00Z"}, "type": "time"},
        },
        "qualifiers": {"P459": _dpla_p459()},
        "references": [_dpla_reference()],
    }
    entity = {"pageid": 999, "statements": {"P571": [malformed_stmt]}}

    with patch.object(sdc_sync, "get_entity", return_value=entity):
        result = sdc_sync.check("M999", ("time", "+1945-01-01T00:00:00Z|P9"), "P571")

    assert result[0] is True


def test_extract_comparable_value_returns_none_for_malformed_time():
    """Parallel guard on the sdc.json side. If _wikibase_time ever
    regresses or a hand-crafted fixture omits a field, build_expected_from_sdc
    must skip the broken claim rather than crash the whole expected-map
    construction."""
    from tools import sdc_sync

    broken_claim = {
        "type": "statement",
        "rank": "normal",
        "mainsnak": {
            "snaktype": "value",
            "property": "P571",
            "datavalue": {"value": {"time": "+1945-01-01T00:00:00Z"}, "type": "time"},
        },
        "qualifiers": {"P459": _dpla_p459()},
        "references": [_dpla_reference()],
    }
    assert sdc_sync._extract_comparable_value(broken_claim) is None


# ---------------------------------------------------------------------------
# Circa-bit propagation in the comparable key (CodeRabbit Major finding):
# without including P1480 presence in the comparable, a DPLA source-string
# change "1945" ↔ "circa 1945" doesn't trigger the reconciler to add or
# remove the P1480 qualifier on the existing Commons claim. The
# _time_claim_comparable wrapper adds a "|circa" suffix when the claim
# carries P1480 = Q5727902 so the two shapes have distinct identities.
# ---------------------------------------------------------------------------


def _value_typed_p571_with_circa(stmt_id, time_str, precision, stated_as):
    """Build a value-typed P571 claim WITH the P1480 = Q5727902 (circa)
    qualifier — the shape the new builder emits for approximate dates."""
    claim = _value_typed_p571_claim(stmt_id, time_str, precision, stated_as)
    claim["qualifiers"]["P1480"] = _qual_entity("P1480", "Q5727902")
    return claim


def test_circa_bit_distinguishes_otherwise_identical_claims():
    """A circa-qualified and a definite claim with the SAME underlying
    time + precision must produce DIFFERENT comparable keys, so the
    reconciler can detect the migration in either direction."""
    from tools import sdc_sync

    plain = _value_typed_p571_claim("M$plain", "+1945-01-01T00:00:00Z", 9, "1945")
    circa = _value_typed_p571_with_circa(
        "M$circa", "+1945-01-01T00:00:00Z", 9, "circa 1945"
    )

    plain_key = sdc_sync._time_claim_comparable(plain)
    circa_key = sdc_sync._time_claim_comparable(circa)

    assert plain_key == "+1945-01-01T00:00:00Z|P9"
    assert circa_key == "+1945-01-01T00:00:00Z|P9|circa"
    assert plain_key != circa_key


def test_reconciler_migrates_plain_to_circa_when_dpla_adds_decorator(monkeypatch):
    """DPLA changes ``"1945"`` → ``"circa 1945"`` on a previously-migrated
    item. The new sdc.json's expected key carries ``|circa``; the
    existing Commons claim's comparable doesn't. Reconciler queues the
    old (no-P1480) for removal so the new (with-P1480) can take its
    place. Without the circa-bit in the comparable, both shapes
    matched and the migration silently stalled — the bug CodeRabbit
    flagged."""
    from tools import sdc_sync

    new_claim = _value_typed_p571_with_circa(
        "M$NEW", "+1945-01-01T00:00:00Z", 9, "circa 1945"
    )
    expected = sdc_sync._build_expected_from_sdc({"claims": [new_claim]})
    assert expected == {"P571": ["+1945-01-01T00:00:00Z|P9|circa"]}

    on_commons = _value_typed_p571_claim(
        "M999$EXACT", "+1945-01-01T00:00:00Z", 9, "1945"
    )
    entity = {"pageid": 999, "statements": {"P571": [on_commons]}}

    submit_calls = []

    def fake_submit(action, mediaid, dpla_id, **params):
        submit_calls.append((action, params.get("claim")))

    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
        patch.object(sdc_sync, "_submit_sdc_write", side_effect=fake_submit),
    ):
        sdc_sync._reconcile_existing_claims(
            "M999", "abc1234567890abcdef1234567890abcd", expected
        )

    assert submit_calls == [("wbremoveclaims", "M999$EXACT")], submit_calls


def test_check_time_kind_treats_circa_and_exact_as_distinct():
    """``check()``'s time branch uses the circa-bit too. An exact claim
    on Commons does NOT match a circa target — so the new circa
    claim gets added, and the exact one gets reconciled away."""
    from tools import sdc_sync

    exact_on_commons = _value_typed_p571_claim(
        "M999$EXACT", "+1945-01-01T00:00:00Z", 9, "1945"
    )
    entity = {"pageid": 999, "statements": {"P571": [exact_on_commons]}}

    with patch.object(sdc_sync, "get_entity", return_value=entity):
        result = sdc_sync.check(
            "M999", ("time", "+1945-01-01T00:00:00Z|P9|circa"), "P571"
        )

    # True = "add the new claim alongside" — the exact claim doesn't
    # collide with the circa claim now.
    assert result[0] is True


# ---------------------------------------------------------------------------
# Legacy `_build_expected_from_parsed` must protect BOTH shapes for P571
# (CodeRabbit Major finding): otherwise a legacy --file/--cat/--list
# rerun against a file that partner mode has migrated to value-typed
# time would see the migrated claim's canonical comparable as
# "unexpected" and queue it for removal.
# ---------------------------------------------------------------------------


def test_build_expected_from_parsed_includes_both_shapes_for_p571():
    """The legacy expected map's P571 entry must include the raw
    display-date string (for unmigrated somevalue+P1932 claims) AND
    the canonical time-comparable key for the parseable subset (for
    migrated value-typed claims), so a rerun after partner-mode
    migration doesn't strip the migrated claim."""
    from tools import sdc_sync

    expected = sdc_sync._build_expected_from_parsed(
        dpla_id="abcdef",
        url="http://example.com/x",
        descs=[],
        dates=["1945", "circa 1929-10", "During the Gilded Age"],
        titles=[],
        hub="Q1",
        local_ids=[],
        institution="Q2",
        rs="",
        creators=[],
        subjects=[],
        naids=[],
        access="",
        level="",
    )
    p571 = expected["P571"]
    # Raw strings preserved for old-shape somevalue+P1932 protection.
    assert "1945" in p571
    assert "circa 1929-10" in p571
    assert "During the Gilded Age" in p571
    # Canonical time keys added for the parseable subset.
    assert "+1945-01-01T00:00:00Z|P9" in p571
    assert "+1929-10-01T00:00:00Z|P10|circa" in p571
    # Unparseable date contributes only the raw string (no canonical key).
    canonical_keys = [v for v in p571 if v.startswith("+")]
    # 2 parseables → exactly 2 canonical entries.
    assert len(canonical_keys) == 2
