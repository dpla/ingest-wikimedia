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
    # The combined per-file dispatcher raises the missing-entity error
    # (the equivalent of the old _post_new_* POST helpers' failure
    # surface, now consolidated into a single wbeditentity per file).
    monkeypatch.setattr(
        sdc_sync,
        "_flush_per_file_edits",
        lambda *_a, **_k: (_ for _ in ()).throw(sdc_sync._MissingEntityError("M12345")),
    )

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
    """Stale DPLA-referenced P170 qualifier (value not in ``expected``)
    is pushed onto the module-level ``removals`` accumulator. The
    dispatcher flushes it via the combined wbeditentity payload's
    ``{"id": ..., "remove": ""}`` claim entries."""
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

    sdc_sync._reset_per_file_accumulators()
    with patch.object(sdc_sync, "get_entity", return_value=entity):
        sdc_sync._reconcile_existing_claims(
            "M999", "abc1234567890abcdef1234567890abcd", expected
        )

    assert sdc_sync.removals == ["M999$STALE"]
    sdc_sync._reset_per_file_accumulators()


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
    fake_tracker.count.return_value = 0
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


def test_safe_process_one_runs_post_sdc_cleanup_when_file_page_supplied(monkeypatch):
    """The legacy ``--file`` / ``--cat`` / ``--list`` paths pass a
    ``file_page`` so the same post-SDC cleanup (strip-or-migrate) the
    partner mode does also runs here. Locks in the wire from
    ``_safe_process_one`` → ``_post_sdc_cleanup_for_legacy_mode`` so a
    refactor of either function can't silently drop the cleanup hook
    on the legacy paths."""
    from tools import sdc_sync

    fake_tracker = MagicMock()
    fake_tracker.count.return_value = 0
    monkeypatch.setattr(sdc_sync, "tracker", fake_tracker)
    monkeypatch.setattr(sdc_sync, "process_one", lambda *a: None)
    monkeypatch.setattr(sdc_sync, "_normalize_wikitext_enabled", True)

    cleanup_calls = []

    def _record(page, dpla_id):
        cleanup_calls.append((page, dpla_id))
        return False

    monkeypatch.setattr(sdc_sync, "_post_sdc_cleanup_for_legacy_mode", _record)

    fake_page = MagicMock(name="FilePage")
    sdc_sync._safe_process_one("M1", "dpla-1", file_page=fake_page)

    assert cleanup_calls == [(fake_page, "dpla-1")]


def test_safe_process_one_skips_cleanup_when_no_file_page(monkeypatch):
    """Backwards-compat: callers that didn't update to pass file_page
    still work (no cleanup attempted; no AttributeError on the
    ``None`` page handle)."""
    from tools import sdc_sync

    fake_tracker = MagicMock()
    fake_tracker.count.return_value = 0
    monkeypatch.setattr(sdc_sync, "tracker", fake_tracker)
    monkeypatch.setattr(sdc_sync, "process_one", lambda *a: None)
    monkeypatch.setattr(sdc_sync, "_normalize_wikitext_enabled", True)

    called = []
    monkeypatch.setattr(
        sdc_sync,
        "_post_sdc_cleanup_for_legacy_mode",
        lambda *a: called.append(a) or False,
    )

    sdc_sync._safe_process_one("M1", "dpla-1")  # no file_page

    assert called == []


def test_safe_process_one_skips_cleanup_when_disabled(monkeypatch):
    """The ``--no-normalize-wikitext`` opt-out short-circuits the
    cleanup even when ``file_page`` is supplied. Diagnostic runs that
    explicitly disable the strip / migrate must not surprise the
    operator with post-SDC edits."""
    from tools import sdc_sync

    fake_tracker = MagicMock()
    fake_tracker.count.return_value = 0
    monkeypatch.setattr(sdc_sync, "tracker", fake_tracker)
    monkeypatch.setattr(sdc_sync, "process_one", lambda *a: None)
    monkeypatch.setattr(sdc_sync, "_normalize_wikitext_enabled", False)

    called = []
    monkeypatch.setattr(
        sdc_sync,
        "_post_sdc_cleanup_for_legacy_mode",
        lambda *a: called.append(a) or False,
    )

    sdc_sync._safe_process_one("M1", "dpla-1", file_page=MagicMock())

    assert called == []


def test_safe_process_one_increments_pages_edited_when_sdc_wrote(monkeypatch):
    """A successful ``process_one`` that committed SDC changes must
    bump ``SDC_PAGES_EDITED`` once. Detected via the write-counter
    snapshot delta (claims/refs/removals after > before)."""
    from tools import sdc_sync

    counts = {
        Result.SDC_CLAIMS_ADDED: 0,
        Result.SDC_REFS_ADDED: 0,
        Result.SDC_REMOVALS: 0,
    }
    fake_tracker = MagicMock()
    fake_tracker.count.side_effect = lambda r: counts.get(r, 0)
    monkeypatch.setattr(sdc_sync, "tracker", fake_tracker)

    def write_some_claims(*_a):
        # Simulate process_one's effect: it bumped CLAIMS_ADDED via the
        # SDC dispatcher.
        counts[Result.SDC_CLAIMS_ADDED] += 5

    monkeypatch.setattr(sdc_sync, "process_one", write_some_claims)
    monkeypatch.setattr(sdc_sync, "_normalize_wikitext_enabled", False)

    sdc_sync._safe_process_one("M1", "dpla-1")

    fake_tracker.increment.assert_called_once_with(Result.SDC_PAGES_EDITED)


def test_safe_process_one_increments_pages_edited_when_cleanup_wrote(monkeypatch):
    """Even if SDC made no changes (file already in sync), a wikitext
    cleanup that actually saved still counts the page as edited.
    Cleanup-only edits are rare but they're real bytes-on-the-wire."""
    from tools import sdc_sync

    fake_tracker = MagicMock()
    fake_tracker.count.return_value = 0  # SDC wrote nothing
    monkeypatch.setattr(sdc_sync, "tracker", fake_tracker)
    monkeypatch.setattr(sdc_sync, "process_one", lambda *a: None)
    monkeypatch.setattr(sdc_sync, "_normalize_wikitext_enabled", True)
    # Cleanup helper claims it wrote — drives the cleanup_wrote branch.
    monkeypatch.setattr(sdc_sync, "_post_sdc_cleanup_for_legacy_mode", lambda *_a: True)

    sdc_sync._safe_process_one("M1", "dpla-1", file_page=MagicMock())

    fake_tracker.increment.assert_called_once_with(Result.SDC_PAGES_EDITED)


def test_safe_process_one_counts_page_once_when_both_paths_wrote(monkeypatch):
    """A file that received both an SDC write AND a wikitext cleanup
    must count as ONE edited page, not two. Otherwise the operator-
    facing PAGES EDITED total inflates beyond the real file count."""
    from tools import sdc_sync

    counts = {
        Result.SDC_CLAIMS_ADDED: 0,
        Result.SDC_REFS_ADDED: 0,
        Result.SDC_REMOVALS: 0,
    }
    fake_tracker = MagicMock()
    fake_tracker.count.side_effect = lambda r: counts.get(r, 0)
    monkeypatch.setattr(sdc_sync, "tracker", fake_tracker)
    monkeypatch.setattr(
        sdc_sync,
        "process_one",
        lambda *_: counts.__setitem__(
            Result.SDC_CLAIMS_ADDED, counts[Result.SDC_CLAIMS_ADDED] + 1
        ),
    )
    monkeypatch.setattr(sdc_sync, "_normalize_wikitext_enabled", True)
    monkeypatch.setattr(sdc_sync, "_post_sdc_cleanup_for_legacy_mode", lambda *_a: True)

    sdc_sync._safe_process_one("M1", "dpla-1", file_page=MagicMock())

    page_increments = [
        c
        for c in fake_tracker.increment.call_args_list
        if c.args == (Result.SDC_PAGES_EDITED,)
    ]
    assert len(page_increments) == 1


def test_safe_process_one_does_not_increment_pages_edited_when_nothing_wrote(
    monkeypatch,
):
    """The no-op case: process_one ran cleanly but committed no edits
    (file already in sync), and cleanup didn't either. Nothing got
    written to Commons → PAGES EDITED must stay flat."""
    from tools import sdc_sync

    fake_tracker = MagicMock()
    fake_tracker.count.return_value = 0
    monkeypatch.setattr(sdc_sync, "tracker", fake_tracker)
    monkeypatch.setattr(sdc_sync, "process_one", lambda *a: None)
    monkeypatch.setattr(sdc_sync, "_normalize_wikitext_enabled", True)
    monkeypatch.setattr(
        sdc_sync, "_post_sdc_cleanup_for_legacy_mode", lambda *_a: False
    )

    sdc_sync._safe_process_one("M1", "dpla-1", file_page=MagicMock())

    page_increments = [
        c
        for c in fake_tracker.increment.call_args_list
        if c.args == (Result.SDC_PAGES_EDITED,)
    ]
    assert page_increments == []


def test_safe_process_one_skips_cleanup_on_missing_entity(monkeypatch):
    """A file whose MediaInfo entity is gone (deleted by a Commons
    curator, etc.) raises ``_MissingEntityError`` from ``process_one``.
    The cleanup must NOT run on that page — saving wikitext to a
    deleted page would either fail loudly or undelete it. Mirrors the
    early-return on the error path."""
    from tools import sdc_sync

    monkeypatch.setattr(sdc_sync, "tracker", MagicMock())
    monkeypatch.setattr(sdc_sync, "_normalize_wikitext_enabled", True)

    def raise_missing(*a):
        raise sdc_sync._MissingEntityError("M1")

    monkeypatch.setattr(sdc_sync, "process_one", raise_missing)

    called = []
    monkeypatch.setattr(
        sdc_sync,
        "_post_sdc_cleanup_for_legacy_mode",
        lambda *a: called.append(a),
    )

    sdc_sync._safe_process_one("M1", "dpla-1", file_page=MagicMock())

    assert called == []


def test_safe_process_one_clears_doc_cache_on_missing_entity(monkeypatch):
    """If ``process_one`` populates the doc cache via ``parsed()`` and
    then raises ``_MissingEntityError`` before cleanup runs, the
    ``finally`` in ``_safe_process_one`` must drop the cached entry —
    otherwise the doc leaks across files in long ``--list`` runs."""
    from tools import sdc_sync

    monkeypatch.setattr(sdc_sync, "tracker", MagicMock())
    monkeypatch.setattr(sdc_sync, "_normalize_wikitext_enabled", True)

    def populate_then_raise(*_a):
        sdc_sync._legacy_mode_doc_cache["dpla-1"] = {"some": "doc"}
        raise sdc_sync._MissingEntityError("M1")

    monkeypatch.setattr(sdc_sync, "process_one", populate_then_raise)
    monkeypatch.setattr(sdc_sync, "_post_sdc_cleanup_for_legacy_mode", lambda *a: None)
    sdc_sync._legacy_mode_doc_cache.clear()

    sdc_sync._safe_process_one("M1", "dpla-1", file_page=MagicMock())

    assert "dpla-1" not in sdc_sync._legacy_mode_doc_cache


def test_post_sdc_cleanup_for_legacy_mode_reuses_cached_doc(monkeypatch):
    """The doc ``parsed()`` cached during ``process_one`` gets popped
    and reused in the cleanup helper — no second S3 / api.dp.la fetch.
    Locks in the optimisation so a refactor can't silently regress to
    re-fetching the same doc."""
    from ingest_wikimedia.dpla import DPLA
    from tools import sdc_sync

    cached_doc = {"id": "dpla-1", "sourceResource": {}, "provider": {"name": "p"}}
    sdc_sync._legacy_mode_doc_cache.clear()
    sdc_sync._legacy_mode_doc_cache["dpla-1"] = cached_doc

    s3_fetches = []
    api_fetches = []
    monkeypatch.setattr(
        sdc_sync,
        "_fetch_dpla_doc_from_s3",
        lambda *a, **kw: s3_fetches.append(a) or None,
    )
    monkeypatch.setattr(
        sdc_sync,
        "_fetch_dpla_doc_from_api",
        lambda *a, **kw: api_fetches.append(a) or None,
    )

    dispatch_calls = []

    def fake_dispatch(file_page, dpla_id, doc, provider, data_provider):
        dispatch_calls.append((dpla_id, doc, provider, data_provider))

    monkeypatch.setattr(sdc_sync, "_post_sdc_cleanup_for_page", fake_dispatch)
    monkeypatch.setattr(
        DPLA,
        "get_provider_and_data_provider",
        staticmethod(lambda doc, hubs: ({"name": "p"}, {"name": "d"})),
    )
    monkeypatch.setattr(sdc_sync, "hubs", {}, raising=False)

    sdc_sync._post_sdc_cleanup_for_legacy_mode(MagicMock(name="FilePage"), "dpla-1")

    assert s3_fetches == []
    assert api_fetches == []
    assert len(dispatch_calls) == 1
    assert dispatch_calls[0][1] is cached_doc
    # Pop-on-read drains the cache so a second cleanup call on the
    # same dpla_id (re-entry) would fall through to the fetch path.
    assert "dpla-1" not in sdc_sync._legacy_mode_doc_cache


# ---------------------------------------------------------------------------
# _post_sdc_cleanup_for_page — strip-or-migrate dispatcher
# ---------------------------------------------------------------------------


def _make_canonical_item():
    """Minimal DPLA item dict the canonical-params helper accepts."""
    return {
        "rights": "http://creativecommons.org/publicdomain/zero/1.0/",
        "isShownAt": "https://example.org/item/1",
        "sourceResource": {
            "title": ["A Title"],
            "description": ["A description"],
            "date": [{"displayDate": "1900"}],
            "creator": ["A Creator"],
            "identifier": ["local-1"],
        },
    }


def test_post_sdc_cleanup_dispatches_to_migrate_on_legacy_template(monkeypatch):
    """A file whose current wikitext carries ``{{Artwork}}`` (the
    legacy DPLA-bot wrapper) routes to the
    ``legacy_artwork.migrate_legacy_file`` migration path, not the
    strip path. Locks in the dispatch order — the migrate check has
    to come first because the strip is a no-op on legacy-shaped
    files, but the migrate is the actual cleanup."""
    from ingest_wikimedia import legacy_artwork, wikitext_normalize
    from tools import sdc_sync

    # _post_sdc_cleanup_for_page passes the module-level ``site``
    # global to migrate_legacy_file. ``_initialize()`` populates it in
    # production but tests don't run init — set a stub here.
    monkeypatch.setattr(sdc_sync, "site", MagicMock(name="Site"), raising=False)

    fake_page = MagicMock(name="FilePage")
    fake_page.exists.return_value = True
    fake_page.text = "{{Artwork|title=A Title}}\n[[Category:Foo]]"

    migrate_calls = []
    monkeypatch.setattr(
        legacy_artwork,
        "migrate_legacy_file",
        lambda **kw: migrate_calls.append(kw),
    )
    strip_calls = []
    monkeypatch.setattr(
        wikitext_normalize,
        "normalize_page",
        lambda *a, **kw: strip_calls.append((a, kw)),
    )

    sdc_sync._post_sdc_cleanup_for_page(
        fake_page,
        "dpla-1",
        _make_canonical_item(),
        {"Wikidata": "Q1"},
        {"Wikidata": "Q2"},
    )

    assert len(migrate_calls) == 1
    assert strip_calls == []


def test_post_sdc_cleanup_dispatches_to_strip_on_new_template(monkeypatch):
    """A file already on ``{{DPLA metadata}}`` routes to the strip
    path. The migrate path's revision-history walk is expensive and
    would be wasted work on a file that's already on the new shape."""
    from ingest_wikimedia import legacy_artwork, wikitext_normalize
    from tools import sdc_sync

    fake_page = MagicMock(name="FilePage")
    fake_page.exists.return_value = True
    fake_page.pageid = 42
    fake_page.text = "{{DPLA metadata\n| title = A Title\n| dpla_id = dpla-1\n}}"

    migrate_calls = []
    monkeypatch.setattr(
        legacy_artwork,
        "migrate_legacy_file",
        lambda **kw: migrate_calls.append(kw),
    )
    strip_calls = []
    monkeypatch.setattr(
        wikitext_normalize,
        "normalize_page",
        lambda *a, **kw: strip_calls.append((a, kw)),
    )
    # The cleanup guard fetches the entity to verify it carries
    # DPLA-attributed SDC before stripping. Stub a passing entity so
    # this test exercises the existing dispatch behaviour; the guard's
    # refusal-on-empty-entity case has its own dedicated regression.
    monkeypatch.setattr(
        sdc_sync,
        "_fetch_entity_for_cleanup_guard",
        lambda mid: {
            "statements": {
                "P1476": [
                    {
                        "mainsnak": {"snaktype": "value"},
                        "qualifiers": {
                            "P459": [
                                {
                                    "snaktype": "value",
                                    "datavalue": {
                                        "value": {"id": "Q61848113"},
                                        "type": "wikibase-entityid",
                                    },
                                }
                            ]
                        },
                    }
                ]
            }
        },
    )

    sdc_sync._post_sdc_cleanup_for_page(
        fake_page,
        "dpla-1",
        _make_canonical_item(),
        {"Wikidata": "Q1"},
        {"Wikidata": "Q2"},
    )

    assert migrate_calls == []
    assert len(strip_calls) == 1


def test_post_sdc_cleanup_skips_when_neither_template_present(monkeypatch):
    """A file with neither template (hand-written wikitext, a stub,
    a file that was never DPLA-uploaded) gets a quiet no-op: the
    dispatcher uses ``has_dpla_metadata_template`` to short-circuit
    before computing ``expected_params`` or calling ``normalize_page``,
    saving the parse + the rights-URI / hub-label resolution work
    that ``dpla_metadata_params`` would do."""
    from ingest_wikimedia import legacy_artwork, wikimedia, wikitext_normalize
    from tools import sdc_sync

    fake_page = MagicMock(name="FilePage")
    fake_page.exists.return_value = True
    fake_page.text = "Just some prose, no template."

    migrate_calls = []
    monkeypatch.setattr(
        legacy_artwork,
        "migrate_legacy_file",
        lambda **kw: migrate_calls.append(kw),
    )
    strip_calls = []
    monkeypatch.setattr(
        wikitext_normalize,
        "normalize_page",
        lambda *a, **kw: strip_calls.append((a, kw)),
    )
    params_calls = []
    monkeypatch.setattr(
        wikimedia,
        "dpla_metadata_params",
        lambda *a, **kw: params_calls.append((a, kw)) or {},
    )

    sdc_sync._post_sdc_cleanup_for_page(
        fake_page,
        "dpla-1",
        _make_canonical_item(),
        {"Wikidata": "Q1"},
        {"Wikidata": "Q2"},
    )

    assert migrate_calls == []
    assert strip_calls == []
    assert params_calls == []


def test_post_sdc_cleanup_skips_when_page_missing(monkeypatch):
    """A page that doesn't exist on Commons (race, deletion) gets no
    cleanup attempt at all."""
    from ingest_wikimedia import legacy_artwork, wikitext_normalize
    from tools import sdc_sync

    fake_page = MagicMock(name="FilePage")
    fake_page.exists.return_value = False

    migrate_calls = []
    strip_calls = []
    monkeypatch.setattr(
        legacy_artwork,
        "migrate_legacy_file",
        lambda **kw: migrate_calls.append(kw),
    )
    monkeypatch.setattr(
        wikitext_normalize,
        "normalize_page",
        lambda *a, **kw: strip_calls.append((a, kw)),
    )

    sdc_sync._post_sdc_cleanup_for_page(
        fake_page,
        "dpla-1",
        _make_canonical_item(),
        {"Wikidata": "Q1"},
        {"Wikidata": "Q2"},
    )

    assert migrate_calls == []
    assert strip_calls == []


def test_post_sdc_cleanup_for_page_isolates_migrate_failure(monkeypatch):
    """A migration that raises is logged and swallowed — the post-SDC
    cleanup is best-effort. The outer SDC sync result has already
    committed; a stripper-side failure must not surface as a SDC sync
    error."""
    from ingest_wikimedia import legacy_artwork
    from tools import sdc_sync

    monkeypatch.setattr(sdc_sync, "site", MagicMock(name="Site"), raising=False)

    fake_page = MagicMock(name="FilePage")
    fake_page.exists.return_value = True
    fake_page.text = "{{Artwork|title=A Title}}"

    def raise_migrate(**kw):
        raise RuntimeError("simulated migration failure")

    monkeypatch.setattr(legacy_artwork, "migrate_legacy_file", raise_migrate)

    # Should NOT propagate.
    sdc_sync._post_sdc_cleanup_for_page(
        fake_page,
        "dpla-1",
        _make_canonical_item(),
        {"Wikidata": "Q1"},
        {"Wikidata": "Q2"},
    )


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

    sdc_sync._reset_per_file_accumulators()
    with patch.object(sdc_sync, "get_entity", return_value=entity):
        sdc_sync._reconcile_existing_claims(
            "M999", "abc1234567890abcdef1234567890abcd", expected
        )

    assert sdc_sync.removals == ["M999$OLD"]
    sdc_sync._reset_per_file_accumulators()


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

    sdc_sync._reset_per_file_accumulators()
    with patch.object(sdc_sync, "get_entity", return_value=entity):
        sdc_sync._reconcile_existing_claims(
            "M999", "abc1234567890abcdef1234567890abcd", expected
        )

    assert sdc_sync.removals == ["M999$STALE"]
    sdc_sync._reset_per_file_accumulators()


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

    sdc_sync._reset_per_file_accumulators()
    with patch.object(sdc_sync, "get_entity", return_value=entity):
        sdc_sync._reconcile_existing_claims(
            "M999", "abc1234567890abcdef1234567890abcd", expected
        )

    assert sdc_sync.removals == ["M999$BORK"]
    sdc_sync._reset_per_file_accumulators()


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

    sdc_sync._reset_per_file_accumulators()
    with patch.object(sdc_sync, "get_entity", return_value=entity):
        sdc_sync._reconcile_existing_claims(
            "M999", "abc1234567890abcdef1234567890abcd", expected
        )

    assert sdc_sync.removals == ["M999$EXACT"]
    sdc_sync._reset_per_file_accumulators()


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


def test_build_expected_from_parsed_includes_both_shapes_for_p571(monkeypatch):
    """The legacy expected map's P571 entry must include the raw
    display-date string (for unmigrated somevalue+P1932 claims) AND
    the canonical time-comparable key for the parseable subset (for
    migrated value-typed claims), so a rerun after partner-mode
    migration doesn't strip the migrated claim."""
    from tools import sdc_sync

    # `_build_expected_from_parsed` reads the module-global ``rights``
    # (populated by ``_initialize()`` in production). Pin it explicitly
    # so this test is self-contained — passes when run in isolation,
    # under reordering, or under pytest-randomly.
    monkeypatch.setattr(sdc_sync, "rights", {}, raising=False)

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


def test_reconciler_protected_props_skips_removal_for_chunkable_claims():
    """The legacy ``dpla_claims()`` path passes
    ``protected_props=CHUNKABLE_PROPS`` to ``_reconcile_existing_claims``
    so partner-mode-written chunked claims (P1545="A1", "A2", ...) on
    a Commons file aren't queued for removal during a legacy
    ``--file`` / ``--cat`` / ``--list`` rerun. The legacy expected-
    builder doesn't model chunk shape; without protection the
    reconciler would see chunked Commons claims as unexpected and
    delete them.
    """
    from tools import sdc_sync

    # A chunked P10358 statement on Commons — DPLA-authored, P1545="A1".
    chunked_a1 = {
        "id": "M999$chunkA1",
        "mainsnak": {
            "property": "P10358",
            "snaktype": "value",
            "datavalue": {
                "type": "monolingualtext",
                "value": {"text": "chunk one text", "language": "en"},
            },
        },
        "qualifiers": {"P459": _dpla_p459(), "P1545": _qual_string("P1545", "A1")},
        "references": [_dpla_reference("abcdef")],
    }
    entity = {"pageid": 999, "statements": {"P10358": [chunked_a1]}}

    # Legacy expected models the unchunked full text — won't match the
    # ("chunk one text", "A1") tuple the reconciler extracts.
    expected = {"P10358": ["the full unchunked description text"]}

    submit_calls = []
    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
        patch.object(
            sdc_sync,
            "_submit_sdc_write",
            side_effect=lambda action, *a, **kw: submit_calls.append((action, a, kw)),
        ),
    ):
        sdc_sync._reconcile_existing_claims(
            "M999",
            "abcdef",
            expected,
            protected_props=frozenset({"P10358"}),
        )

    # Nothing got queued for removal because P10358 is protected.
    assert submit_calls == []


def test_reconciler_without_protection_would_remove_unrecognized_chunked_claim():
    """Sanity check that the previous test would fail without
    protection — the protection mechanism is doing real work, not just
    a no-op covering for some other guard."""
    from tools import sdc_sync

    chunked_a1 = {
        "id": "M999$chunkA1",
        "mainsnak": {
            "property": "P10358",
            "snaktype": "value",
            "datavalue": {
                "type": "monolingualtext",
                "value": {"text": "chunk one text", "language": "en"},
            },
        },
        "qualifiers": {"P459": _dpla_p459(), "P1545": _qual_string("P1545", "A1")},
        "references": [_dpla_reference("abcdef")],
    }
    entity = {"pageid": 999, "statements": {"P10358": [chunked_a1]}}
    expected = {"P10358": ["the full unchunked description text"]}

    sdc_sync._reset_per_file_accumulators()
    with patch.object(sdc_sync, "get_entity", return_value=entity):
        # No protected_props — the chunked statement gets queued for removal
        # because ("chunk one text", "A1") isn't in expected["P10358"].
        sdc_sync._reconcile_existing_claims("M999", "abcdef", expected)

    assert sdc_sync.removals == ["M999$chunkA1"]
    sdc_sync._reset_per_file_accumulators()


# ---------------------------------------------------------------------------
# P2699 / P6108 qualifiers on P7482 (PR description in
# tools/sdc_sync.py::_amend_p7482_url_qualifiers).
#
# Test surface:
#   * per-ordinal P2699 injection in process_one_from_sdc
#   * idempotent amend of existing DPLA-authored P7482 statements
#     (missing → POST wbsetqualifier; already present → no-op)
#   * eligibility gate: don't amend foreign-shaped P7482 statements
#   * reconciler treats P2699/P6108 as DPLA-owned (doesn't queue removal)
# ---------------------------------------------------------------------------


def _p7482_stmt(stmt_id, p973_value, extra_qualifiers=None):
    """A DPLA-authored P7482 statement with the standard P973/P137/P459
    qualifier set. ``extra_qualifiers`` is a dict merged in on top
    (used by tests that need to set P2699 / P6108 to pre-existing
    values)."""
    qualifiers = {
        "P459": _dpla_p459(),
        "P973": _qual_string("P973", p973_value),
        "P137": _qual_entity("P137", "Q12345"),
    }
    if extra_qualifiers:
        qualifiers.update(extra_qualifiers)
    return {
        "id": stmt_id,
        "type": "statement",
        "rank": "normal",
        "mainsnak": {
            "snaktype": "value",
            "property": "P7482",
            "datavalue": {
                "value": {
                    "entity-type": "item",
                    "numeric-id": 74228490,
                    "id": "Q74228490",
                },
                "type": "wikibase-entityid",
            },
        },
        "qualifiers": qualifiers,
        "references": [_dpla_reference()],
    }


def _sdc_payload_with_p7482(catalog_url, iiif_manifest_url=None):
    """Build a sdc.json shape with a single P7482 claim — what
    ``_amend_p7482_url_qualifiers`` reads to learn what should be there."""
    p7482 = {
        "type": "statement",
        "rank": "normal",
        "mainsnak": {
            "snaktype": "value",
            "property": "P7482",
            "datavalue": {
                "value": {"entity-type": "item", "numeric-id": 74228490},
                "type": "wikibase-entityid",
            },
        },
        "qualifiers": {
            "P459": _dpla_p459(),
            "P973": _qual_string("P973", catalog_url),
            "P137": _qual_entity("P137", "Q12345"),
        },
        "references": [_dpla_reference()],
    }
    if iiif_manifest_url is not None:
        p7482["qualifiers"]["P6108"] = _qual_string("P6108", iiif_manifest_url)
    return {"claims": [p7482]}


def test_amend_p7482_adds_missing_p2699_and_p6108(monkeypatch):
    """Existing DPLA-authored P7482 with only P973/P137/P459 — the
    common case for every file uploaded before this code shipped. The
    builder pushes a (claimid, prop, snak) tuple onto the module-level
    ``qualifier_amends`` accumulator for each missing qualifier; the
    per-file dispatcher flushes them in the combined wbeditentity."""
    from tools import sdc_sync

    catalog_url = "https://example.org/item/abc"
    download_url = "https://example.org/files/abc-page1.jpg"
    iiif_url = "https://example.org/iiif/abc/manifest.json"

    legacy_stmt = _p7482_stmt("M999$P7482", catalog_url)
    entity = {"pageid": 999, "statements": {"P7482": [legacy_stmt]}}
    payload = _sdc_payload_with_p7482(catalog_url, iiif_manifest_url=iiif_url)

    sdc_sync._reset_per_file_accumulators()
    with patch.object(sdc_sync, "get_entity", return_value=entity):
        sdc_sync._amend_p7482_url_qualifiers(
            "M999", "abcdef", payload, download_url=download_url
        )

    by_prop = {prop: (cid, snak) for (cid, prop, snak) in sdc_sync.qualifier_amends}
    assert "P2699" in by_prop, sdc_sync.qualifier_amends
    assert "P6108" in by_prop, sdc_sync.qualifier_amends
    assert by_prop["P2699"][0] == "M999$P7482"
    assert by_prop["P6108"][0] == "M999$P7482"
    assert by_prop["P2699"][1]["datavalue"]["value"] == download_url
    assert by_prop["P6108"][1]["datavalue"]["value"] == iiif_url
    sdc_sync._reset_per_file_accumulators()


def test_amend_p7482_noop_when_qualifiers_already_match(monkeypatch):
    """Already-present qualifiers with the matching values → zero
    wbsetqualifier calls. Re-running the same partner against an
    already-migrated file is silent."""
    from tools import sdc_sync

    catalog_url = "https://example.org/item/abc"
    download_url = "https://example.org/files/abc-page1.jpg"
    iiif_url = "https://example.org/iiif/abc/manifest.json"

    already_migrated = _p7482_stmt(
        "M999$P7482",
        catalog_url,
        extra_qualifiers={
            "P2699": _qual_string("P2699", download_url),
            "P6108": _qual_string("P6108", iiif_url),
        },
    )
    entity = {"pageid": 999, "statements": {"P7482": [already_migrated]}}
    payload = _sdc_payload_with_p7482(catalog_url, iiif_manifest_url=iiif_url)

    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
    ):
        sdc_sync._amend_p7482_url_qualifiers(
            "M999", "abcdef", payload, download_url=download_url
        )


def test_amend_p7482_skips_when_no_existing_statement(monkeypatch):
    """When Commons has no P7482 yet (newly uploaded file), the standard
    ``_post_new_claims`` path is responsible for adding it with all the
    qualifiers already in place. ``_amend_p7482_url_qualifiers`` should
    not POST anything — no claim id to target."""
    from tools import sdc_sync

    entity = {"pageid": 999, "statements": {}}  # no P7482
    payload = _sdc_payload_with_p7482("https://example.org/item/abc")

    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
    ):
        sdc_sync._amend_p7482_url_qualifiers(
            "M999",
            "abcdef",
            payload,
            download_url="https://example.org/files/abc.jpg",
        )


def test_amend_p7482_does_not_touch_foreign_statements(monkeypatch):
    """A P7482 statement carrying user-added qualifiers (or a non-DPLA
    publisher reference) must be left alone — the
    ``_is_safe_to_amend_in_place`` gate keeps community-edited statements
    out of the amend path. Re-running shouldn't drag in our P2699/P6108
    next to someone else's data."""
    from tools import sdc_sync

    catalog_url = "https://example.org/item/abc"
    foreign_stmt = _p7482_stmt(
        "M999$FOREIGN",
        catalog_url,
        extra_qualifiers={
            # P1001 isn't in _DPLA_EXTRA_QUALIFIER_PROPS for P7482 →
            # _is_safe_to_amend_in_place returns False.
            "P1001": _qual_entity("P1001", "Q30"),
        },
    )
    entity = {"pageid": 999, "statements": {"P7482": [foreign_stmt]}}
    payload = _sdc_payload_with_p7482(catalog_url)

    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
    ):
        sdc_sync._amend_p7482_url_qualifiers(
            "M999", "abcdef", payload, download_url="https://example.org/files/abc.jpg"
        )


def test_dpla_extra_qualifier_props_p7482_includes_new_url_quals():
    """``_DPLA_EXTRA_QUALIFIER_PROPS["P7482"]`` must include both P2699
    and P6108 so the reconciler's amend-safety check counts them as
    DPLA-owned. Without this, a P7482 statement carrying our own
    P2699/P6108 would look 'foreign' to ``_is_safe_to_amend_in_place``
    and the reconciler would refuse to clean it up if DPLA's catalog
    URL ever changed."""
    from tools import sdc_sync

    allowed = sdc_sync._DPLA_EXTRA_QUALIFIER_PROPS["P7482"]
    assert "P2699" in allowed
    assert "P6108" in allowed
    # Keep the existing qualifiers in the set too — regression guard
    # against an accidental overwrite.
    assert "P973" in allowed
    assert "P137" in allowed


# --------------------------------------------------------------------------
# #40 — P304 (page-number) per-ordinal qualifier on P760 (DPLA ID).
#
# Multipage items have multiple Commons files. Files of the same extension
# get numbered within their format series (3 JPGs → 1/2/3, 2 PDFs → 1/2).
# A file that's the only one of its extension on the item gets no P304.
# --------------------------------------------------------------------------


def test_file_extension_lowercases_and_handles_dotless_titles():
    from tools import sdc_sync

    assert sdc_sync._file_extension("File:Foo - 1.jpg") == "jpg"
    assert sdc_sync._file_extension("File:Foo.PDF") == "pdf"
    assert sdc_sync._file_extension("File:Foo - 1 - bar.JPG") == "jpg"
    assert sdc_sync._file_extension("noext") == ""
    assert sdc_sync._file_extension("") == ""
    assert sdc_sync._file_extension(None) == ""


def test_compute_page_numbers_single_ordinal_returns_empty():
    """One file → no P304 (not multipage)."""
    from tools import sdc_sync

    ordinals = [("1", {"title": "File:Solo.jpg"})]
    assert sdc_sync._compute_page_numbers(ordinals) == {}


def test_compute_page_numbers_multiple_jpgs_single_pdf_numbers_only_jpgs():
    """3 JPGs + 1 PDF: only the JPGs get P304 (the PDF is alone in its
    extension series). User-confirmed: 'we are only numbering within
    each ordinal series — JPGs and PDFs are numbered separately'."""
    from tools import sdc_sync

    ordinals = [
        ("1", {"title": "File:A.jpg"}),
        ("2", {"title": "File:B.pdf"}),
        ("3", {"title": "File:C.jpg"}),
        ("4", {"title": "File:D.jpg"}),
    ]
    result = sdc_sync._compute_page_numbers(ordinals)
    # JPGs (ordinals 1, 3, 4) get 1, 2, 3 in the order encountered.
    assert result == {"1": 1, "3": 2, "4": 3}
    # The single PDF gets no entry.
    assert "2" not in result


def test_compute_page_numbers_multiple_extensions_each_numbered_separately():
    """3 JPGs + 2 PDFs: JPGs get 1/2/3, PDFs get 1/2."""
    from tools import sdc_sync

    ordinals = [
        ("1", {"title": "File:A.jpg"}),
        ("2", {"title": "File:B.pdf"}),
        ("3", {"title": "File:C.jpg"}),
        ("4", {"title": "File:D.pdf"}),
        ("5", {"title": "File:E.jpg"}),
    ]
    result = sdc_sync._compute_page_numbers(ordinals)
    assert result == {"1": 1, "3": 2, "5": 3, "2": 1, "4": 2}


def test_compute_page_numbers_mixed_singletons_returns_empty():
    """1 JPG + 1 PDF: neither file is part of a multipage series within
    its own format. No P304 anywhere."""
    from tools import sdc_sync

    ordinals = [
        ("1", {"title": "File:A.jpg"}),
        ("2", {"title": "File:B.pdf"}),
    ]
    assert sdc_sync._compute_page_numbers(ordinals) == {}


def test_amend_p760_page_qualifier_noop_when_page_number_is_none_and_no_existing_p304():
    """``page_number=None`` (singleton in extension group) + no
    existing P304 → no writes. Note the helper STILL reads the entity
    to confirm there's no stale P304 to clean up; this is intentional
    (the cleanup pass is the whole reason None doesn't early-return)."""
    from tools import sdc_sync

    no_p304_stmt = {
        "id": "M999$p760id",
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"type": "string", "value": "abcdef"},
        },
        "qualifiers": {"P459": _dpla_p459()},
        "references": [_dpla_reference("abcdef")],
    }
    entity = {"pageid": 999, "statements": {"P760": [no_p304_stmt]}}

    submit_calls = []
    invalidate_calls = []
    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(
            sdc_sync,
            "invalidate_entity",
            side_effect=lambda *a: invalidate_calls.append(a),
        ),
        patch.object(
            sdc_sync,
            "_submit_sdc_write",
            side_effect=lambda action, *a, **kw: submit_calls.append((action, a, kw)),
        ),
    ):
        sdc_sync._amend_p760_page_qualifier(
            "M999", "abcdef", {"claims": []}, page_number=None
        )
    assert submit_calls == []
    # No writes happened → no terminal invalidate.
    assert invalidate_calls == []


def test_amend_p760_page_qualifier_removes_stale_p304_when_page_number_is_none():
    """File that USED to be in a multi-file extension group (had
    P304="2") is now a singleton — ``page_number=None``. The builder
    must push the stale snak hash onto ``qualifier_removals``; the
    per-file dispatcher will exclude it from the wholesale-replace
    qualifier set in the combined wbeditentity. Otherwise the file
    keeps incorrect page metadata indefinitely since the reconciler
    doesn't diff P304."""
    from tools import sdc_sync

    existing_p760 = {
        "id": "M999$p760id",
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"type": "string", "value": "abcdef"},
        },
        "qualifiers": {
            "P459": _dpla_p459(),
            "P304": [
                {
                    "snaktype": "value",
                    "property": "P304",
                    "datavalue": {"type": "string", "value": "2"},
                    "hash": "obsolete-hash",
                }
            ],
        },
        "references": [_dpla_reference("abcdef")],
    }
    entity = {"pageid": 999, "statements": {"P760": [existing_p760]}}

    sdc_sync._reset_per_file_accumulators()
    with patch.object(sdc_sync, "get_entity", return_value=entity):
        sdc_sync._amend_p760_page_qualifier(
            "M999", "abcdef", {"claims": []}, page_number=None
        )

    assert sdc_sync.qualifier_removals == [("M999$p760id", "obsolete-hash")]
    assert sdc_sync.qualifier_amends == []
    sdc_sync._reset_per_file_accumulators()


def test_amend_p760_page_qualifier_stamps_missing_p304_via_accumulator():
    """Existing DPLA-authored P760 with no P304 → the builder pushes a
    new-P304 snak onto ``qualifier_amends`` for the dispatcher to flush
    in the combined wbeditentity."""
    from tools import sdc_sync

    existing_p760 = {
        "id": "M999$p760id",
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"type": "string", "value": "abcdef"},
        },
        "qualifiers": {"P459": _dpla_p459()},
        "references": [_dpla_reference("abcdef")],
    }
    entity = {"pageid": 999, "statements": {"P760": [existing_p760]}}

    sdc_sync._reset_per_file_accumulators()
    with patch.object(sdc_sync, "get_entity", return_value=entity):
        sdc_sync._amend_p760_page_qualifier(
            "M999", "abcdef", {"claims": []}, page_number=2
        )

    assert len(sdc_sync.qualifier_amends) == 1
    claimid, prop, snak = sdc_sync.qualifier_amends[0]
    assert claimid == "M999$p760id"
    assert prop == "P304"
    assert snak["datavalue"]["value"] == "2"
    assert sdc_sync.qualifier_removals == []
    sdc_sync._reset_per_file_accumulators()


def test_amend_p760_page_qualifier_idempotent_when_p304_already_matches():
    """Existing DPLA-authored P760 already has the correct P304 value →
    no POST. Re-running the same sync must be a no-op."""
    from tools import sdc_sync

    existing_p760 = {
        "id": "M999$p760id",
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"type": "string", "value": "abcdef"},
        },
        "qualifiers": {
            "P459": _dpla_p459(),
            "P304": _qual_string("P304", "2"),
        },
        "references": [_dpla_reference("abcdef")],
    }
    entity = {"pageid": 999, "statements": {"P760": [existing_p760]}}

    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
    ):
        sdc_sync._amend_p760_page_qualifier(
            "M999", "abcdef", {"claims": []}, page_number=2
        )


def test_amend_p760_page_qualifier_skips_when_no_dpla_authored_p760():
    """Only foreign / unsafe P760 statements on Commons → nothing to
    amend; helper exits quietly."""
    from tools import sdc_sync

    foreign_p760 = {
        "id": "M999$foreign",
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"type": "string", "value": "abcdef"},
        },
        "qualifiers": {"P1001": _qual_string("P1001", "user-added")},
        "references": [_foreign_reference()],
    }
    entity = {"pageid": 999, "statements": {"P760": [foreign_p760]}}

    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
    ):
        sdc_sync._amend_p760_page_qualifier(
            "M999", "abcdef", {"claims": []}, page_number=1
        )


def test_amend_p760_page_qualifier_removes_stale_value_when_page_changes():
    """Renumber scenario: existing DPLA-authored P760 has P304="3"
    (from a prior sync), but the recomputed page_number is now 2 (a
    sibling ordinal got deleted, shifting this file's position). The
    builder must push both the stale snak-hash removal AND the new
    snak addition onto the accumulators; the dispatcher folds them
    together into a single wholesale-replace qualifier set on the
    combined wbeditentity."""
    from tools import sdc_sync

    existing_p760 = {
        "id": "M999$p760id",
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"type": "string", "value": "abcdef"},
        },
        "qualifiers": {
            "P459": _dpla_p459(),
            "P304": [
                {
                    "snaktype": "value",
                    "property": "P304",
                    "datavalue": {"type": "string", "value": "3"},
                    "hash": "stale-snak-hash-3",
                }
            ],
        },
        "references": [_dpla_reference("abcdef")],
    }
    entity = {"pageid": 999, "statements": {"P760": [existing_p760]}}

    sdc_sync._reset_per_file_accumulators()
    with patch.object(sdc_sync, "get_entity", return_value=entity):
        sdc_sync._amend_p760_page_qualifier(
            "M999", "abcdef", {"claims": []}, page_number=2
        )

    assert sdc_sync.qualifier_removals == [("M999$p760id", "stale-snak-hash-3")]
    assert len(sdc_sync.qualifier_amends) == 1
    claimid, prop, snak = sdc_sync.qualifier_amends[0]
    assert claimid == "M999$p760id"
    assert prop == "P304"
    assert snak["datavalue"]["value"] == "2"
    sdc_sync._reset_per_file_accumulators()


def test_amend_p760_page_qualifier_removes_stale_when_expected_also_present():
    """Mixed case: existing P304 has both the expected value AND a stale
    one (e.g. a prior renumber wasn't cleaned up). Builder must push
    only the stale snak-hash removal onto ``qualifier_removals`` and
    NOT add the expected value (it's already present)."""
    from tools import sdc_sync

    existing_p760 = {
        "id": "M999$p760id",
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"type": "string", "value": "abcdef"},
        },
        "qualifiers": {
            "P459": _dpla_p459(),
            "P304": [
                {
                    "snaktype": "value",
                    "property": "P304",
                    "datavalue": {"type": "string", "value": "2"},
                    "hash": "hash-2",
                },
                {
                    "snaktype": "value",
                    "property": "P304",
                    "datavalue": {"type": "string", "value": "3"},
                    "hash": "stale-hash-3",
                },
            ],
        },
        "references": [_dpla_reference("abcdef")],
    }
    entity = {"pageid": 999, "statements": {"P760": [existing_p760]}}

    sdc_sync._reset_per_file_accumulators()
    with patch.object(sdc_sync, "get_entity", return_value=entity):
        sdc_sync._amend_p760_page_qualifier(
            "M999", "abcdef", {"claims": []}, page_number=2
        )

    # Only the stale snak got queued for removal; the expected value
    # was already present, so no addition pushed.
    assert sdc_sync.qualifier_removals == [("M999$p760id", "stale-hash-3")]
    assert sdc_sync.qualifier_amends == []
    sdc_sync._reset_per_file_accumulators()


def test_dpla_extra_qualifier_props_p760_includes_p304_and_p1545():
    """``_DPLA_EXTRA_QUALIFIER_PROPS["P760"]`` must include P304 (per-
    ordinal page number) and P1545 (chunk series ordinal) so
    ``_is_safe_to_amend_in_place`` counts them as DPLA-owned."""
    from tools import sdc_sync

    allowed = sdc_sync._DPLA_EXTRA_QUALIFIER_PROPS["P760"]
    assert "P304" in allowed
    assert "P1545" in allowed


# --------------------------------------------------------------------------
# #39 — chunked-claim idempotency: _extract_p1545_value, check(),
# _extract_comparable_value tuple shape.
# --------------------------------------------------------------------------


def test_extract_p1545_value_returns_none_when_absent():
    from tools import sdc_sync

    stmt = {"qualifiers": {"P459": _dpla_p459()}}
    assert sdc_sync._extract_p1545_value(stmt) is None
    # Also handles missing qualifiers key.
    assert sdc_sync._extract_p1545_value({}) is None


def test_extract_p1545_value_returns_string_when_present():
    from tools import sdc_sync

    stmt = {"qualifiers": {"P1545": _qual_string("P1545", "A2")}}
    assert sdc_sync._extract_p1545_value(stmt) == "A2"


def test_extract_p1545_value_skips_malformed_snaks():
    """A P1545 qualifier missing 'datavalue' or whose snaktype isn't
    'value' shouldn't crash — return None and move on."""
    from tools import sdc_sync

    stmt = {
        "qualifiers": {
            "P1545": [
                {"snaktype": "novalue", "property": "P1545"},
                # Then a well-formed one — still returns the well-formed value.
                {
                    "snaktype": "value",
                    "property": "P1545",
                    "datavalue": {"type": "string", "value": "B1"},
                },
            ]
        }
    }
    assert sdc_sync._extract_p1545_value(stmt) == "B1"


def test_extract_comparable_value_returns_tuple_for_string():
    """String/monolingualtext comparable values are (value, p1545)
    tuples so chunk-by-chunk matching can distinguish chunks of the
    same logical value."""
    from tools import sdc_sync

    unchunked = {
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"type": "string", "value": "abcdef"},
        }
    }
    assert sdc_sync._extract_comparable_value(unchunked) == ("abcdef", None)

    chunked = {
        "mainsnak": {
            "property": "P217",
            "snaktype": "value",
            "datavalue": {"type": "string", "value": "chunk-1-text"},
        },
        "qualifiers": {"P1545": _qual_string("P1545", "A1")},
    }
    assert sdc_sync._extract_comparable_value(chunked) == ("chunk-1-text", "A1")


def test_extract_comparable_value_returns_tuple_for_monolingualtext():
    from tools import sdc_sync

    claim = {
        "mainsnak": {
            "property": "P1476",
            "snaktype": "value",
            "datavalue": {
                "type": "monolingualtext",
                "value": {"text": "Hello", "language": "en"},
            },
        },
        "qualifiers": {"P1545": _qual_string("P1545", "A2")},
    }
    assert sdc_sync._extract_comparable_value(claim) == ("Hello", "A2")


def test_check_chunked_claim_matches_only_same_p1545():
    """sdc.json claim with P1545="A1" must NOT match an existing Commons
    statement with P1545="A2" — different chunks of the same logical
    value are independent."""
    from tools import sdc_sync

    chunked_a1 = {
        "id": "M999$chunkA1",
        "mainsnak": {
            "property": "P1476",
            "snaktype": "value",
            "datavalue": {
                "type": "monolingualtext",
                "value": {"text": "shared chunk text", "language": "en"},
            },
        },
        "qualifiers": {"P459": _dpla_p459(), "P1545": _qual_string("P1545", "A1")},
        "references": [_dpla_reference("abcdef")],
    }
    entity = {"pageid": 999, "statements": {"P1476": [chunked_a1]}}

    with patch.object(sdc_sync, "get_entity", return_value=entity):
        # Looking for A1 — should find the existing A1 (no add).
        result_a1 = sdc_sync.check(
            "M999", ("monolingualtext", ("shared chunk text", "A1")), "P1476"
        )
        # Looking for A2 — same text but different chunk; should add new.
        result_a2 = sdc_sync.check(
            "M999", ("monolingualtext", ("shared chunk text", "A2")), "P1476"
        )

    assert result_a1[0] is False  # don't duplicate the matching A1
    assert result_a2[0] is True  # A2 is a distinct chunk — add it


def test_check_chunked_claim_does_not_match_unchunked_legacy_statement():
    """A pre-chunking Commons statement (no P1545) is NOT the same logical
    claim as a new chunked statement, even with matching text. The
    reconciler will remove the legacy one; check() must let the new one
    through."""
    from tools import sdc_sync

    legacy_stmt = {
        "id": "M999$legacy",
        "mainsnak": {
            "property": "P1476",
            "snaktype": "value",
            "datavalue": {
                "type": "monolingualtext",
                "value": {"text": "value", "language": "en"},
            },
        },
        "qualifiers": {"P459": _dpla_p459()},
        "references": [_dpla_reference("abcdef")],
    }
    entity = {"pageid": 999, "statements": {"P1476": [legacy_stmt]}}

    with patch.object(sdc_sync, "get_entity", return_value=entity):
        result = sdc_sync.check("M999", ("monolingualtext", ("value", "A1")), "P1476")

    # The chunked sdc.json claim must be added — it's not equivalent to
    # the pre-chunking statement.
    assert result[0] is True


def test_check_legacy_string_qid_still_accepted():
    """Backwards compat: legacy callers (dpla_claims path) still pass
    plain-string qids without P1545. check() must accept that shape
    and treat it as p1545=None."""
    from tools import sdc_sync

    legacy_stmt = {
        "id": "M999$legacy",
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"type": "string", "value": "abc"},
        },
        "qualifiers": {"P459": _dpla_p459()},
        "references": [_dpla_reference("abcdef")],
    }
    entity = {"pageid": 999, "statements": {"P760": [legacy_stmt]}}

    with patch.object(sdc_sync, "get_entity", return_value=entity):
        # qid[1] is a raw string (not tuple) — legacy shape.
        result = sdc_sync.check("M999", ("string", "abc"), "P760")

    # Existing matches with no P1545, qid implicitly p1545=None → no
    # duplicate add.
    assert result[0] is False


def test_dpla_extra_qualifier_props_chunkable_have_p1545():
    """Every chunkable property must include P1545 in its allowed
    qualifier set; otherwise ``_is_safe_to_amend_in_place`` would treat
    DPLA's own chunked claims as foreign. The dict is populated from
    ``CHUNKABLE_PROPS`` so adding a new chunkable property in sdc.py
    automatically widens this allowed-qualifier set."""
    from ingest_wikimedia.sdc import CHUNKABLE_PROPS
    from tools import sdc_sync

    for prop in CHUNKABLE_PROPS:
        allowed = sdc_sync._DPLA_EXTRA_QUALIFIER_PROPS[prop]
        assert "P1545" in allowed, f"{prop} allowed set missing P1545: {allowed}"


def test_chunked_claim_roundtrip_extract_comparable_value_preserves_chunk_identity():
    """End-to-end: build a long monolingualtext value via
    ``build_claims_for_doc``, then extract comparable tuples for each
    resulting chunk claim via ``_extract_comparable_value``. The
    tuples must be distinct per chunk (text or P1545 differs), and
    the P1545 series ordinal must follow A1/A2/A3 in order. Without
    this end-to-end coverage, the sdc.py and sdc_sync.py halves could
    drift on the chunk-key contract."""
    import datetime as _dt

    from ingest_wikimedia.sdc import build_claims_for_doc
    from tools import sdc_sync

    long_desc = "a" * 1500 + " " + "b" * 1500 + " " + "c" * 800
    doc = {
        "id": "abc1234567890",
        "provider": {"name": "Digital Commonwealth"},
        "dataProvider": {"name": "Boston Public Library"},
        "sourceResource": {
            "title": ["A title"],
            "description": [long_desc],
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
    out = build_claims_for_doc(
        doc, "abc1234567890", hubs, {}, {}, {}, _dt.date(2026, 6, 1)
    )
    assert out is not None, "doc should be parseable; check fixture shape"
    desc_claims = [c for c in out["claims"] if c["mainsnak"]["property"] == "P10358"]
    assert len(desc_claims) == 3, (
        f"expected 3 chunks for {len(long_desc)}-char description, "
        f"got {len(desc_claims)}"
    )
    comparable_tuples = [sdc_sync._extract_comparable_value(c) for c in desc_claims]
    # All distinct.
    assert len(set(comparable_tuples)) == 3
    # P1545 ordinals follow A1/A2/A3.
    assert [t[1] for t in comparable_tuples] == ["A1", "A2", "A3"]


def test_amend_p760_page_qualifier_does_not_re_invalidate_when_p304_matches():
    """When the existing P760 already has the expected P304, the helper
    must skip both postqual AND the terminal invalidate_entity — at
    millions of items, the spared cache drop is real. Mirrors the
    conditional-invalidate pattern in ``_amend_p7482_url_qualifiers``."""
    from tools import sdc_sync

    existing_p760 = {
        "id": "M999$p760id",
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"type": "string", "value": "abcdef"},
        },
        "qualifiers": {
            "P459": _dpla_p459(),
            "P304": _qual_string("P304", "2"),
        },
        "references": [_dpla_reference("abcdef")],
    }
    entity = {"pageid": 999, "statements": {"P760": [existing_p760]}}

    invalidate_calls = []
    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(
            sdc_sync,
            "invalidate_entity",
            side_effect=lambda *a: invalidate_calls.append(a),
        ),
    ):
        sdc_sync._amend_p760_page_qualifier(
            "M999", "abcdef", {"claims": []}, page_number=2
        )

    assert invalidate_calls == []


def test_qualifier_values_returns_empty_when_no_qualifiers():
    """Module-level helper safety — passes the smoke-test cases that
    callers used to inline in each amend helper."""
    from tools import sdc_sync

    assert sdc_sync._qualifier_values({}, "P304") == []
    assert sdc_sync._qualifier_values({"qualifiers": {}}, "P304") == []
    # Skip malformed snaks rather than crash.
    stmt = {
        "qualifiers": {
            "P304": [
                {"snaktype": "novalue", "property": "P304"},
                {
                    "snaktype": "value",
                    "property": "P304",
                    "datavalue": {"type": "string", "value": "2"},
                },
            ]
        }
    }
    assert sdc_sync._qualifier_values(stmt, "P304") == ["2"]
    assert sdc_sync._first_qualifier_value(stmt, "P304") == "2"
    assert sdc_sync._first_qualifier_value(stmt, "P999") is None


# ---------------------------------------------------------------------------
# Entity-cache leak fix.
#
# Pre-fix, ``_entity_cache`` accumulated one cached MediaInfo entity per file
# processed across a session's lifetime — only ``invalidate_entity(mediaid)``
# (per-write, single-key pop) ever evicted entries. A 25-hour NARA run
# reached ~2.6 GB RSS as the cache grew to ~200k entities. Both
# ``process_one`` and ``process_one_from_sdc`` now ``_entity_cache.clear()``
# at the file-boundary entry so cache locality stays per-file but doesn't
# leak across files.
# ---------------------------------------------------------------------------


def _patch_process_one_from_sdc_dependencies(monkeypatch, sdc_sync, current_mediaid):
    """Stub every network/IO surface ``process_one_from_sdc`` touches so the
    test can drive it without contacting Commons."""
    fake_request = MagicMock()
    fake_request.submit.return_value = {
        "entities": {
            current_mediaid: {
                "pageid": int(current_mediaid.lstrip("M")),
                "statements": {},
            }
        }
    }
    fake_site = MagicMock()
    fake_site.simple_request.return_value = fake_request
    monkeypatch.setattr(sdc_sync, "site", fake_site, raising=False)
    monkeypatch.setattr(sdc_sync, "_amend_p7482_url_qualifiers", lambda *a, **kw: None)
    monkeypatch.setattr(sdc_sync, "_amend_p760_page_qualifier", lambda *a, **kw: None)
    monkeypatch.setattr(sdc_sync, "_reconcile_existing_claims", lambda *a, **kw: None)
    monkeypatch.setattr(sdc_sync, "_flush_per_file_edits", lambda *a, **kw: None)


def test_process_one_from_sdc_clears_entity_cache_at_file_boundary(monkeypatch):
    """Regression guard for the cache-leak fix. The module-level
    ``_entity_cache`` must be flushed at every file-boundary entry so a
    long-running session's RSS stays flat regardless of file count."""
    from tools import sdc_sync

    sdc_sync._entity_cache.update(
        {
            "M111": {"pageid": 111, "statements": {}},
            "M222": {"pageid": 222, "statements": {}},
            "M333": {"pageid": 333, "statements": {}},
            "M444": {"pageid": 444, "statements": {}},
            "M555": {"pageid": 555, "statements": {}},
        }
    )
    assert len(sdc_sync._entity_cache) == 5

    _patch_process_one_from_sdc_dependencies(monkeypatch, sdc_sync, "M999")

    sdc_sync.process_one_from_sdc("M999", "abcdef", {"claims": []})

    # Prior files' entities are gone. The current mediaid may or may not
    # be in the cache afterward (post-write cleanup invalidates it), but
    # in no case do prior files persist.
    for prior in ("M111", "M222", "M333", "M444", "M555"):
        assert prior not in sdc_sync._entity_cache, (
            f"prior file {prior} leaked across the file boundary"
        )
    # Defensive cleanup so this test doesn't leak state into the next.
    sdc_sync._entity_cache.clear()


def test_entity_cache_does_not_grow_across_many_files(monkeypatch):
    """Direct memory-leak regression. Run the file-boundary entry point
    against many synthetic files in sequence; cache size must stay
    bounded (≤ 1 entry per call), never accumulating."""
    from tools import sdc_sync

    sdc_sync._entity_cache.clear()
    sizes = []
    for i in range(100):
        mediaid = f"M{1000 + i}"
        _patch_process_one_from_sdc_dependencies(monkeypatch, sdc_sync, mediaid)
        sdc_sync.process_one_from_sdc(mediaid, f"dpla{i:06d}", {"claims": []})
        sizes.append(len(sdc_sync._entity_cache))

    # Pre-fix, sizes would be [1, 2, 3, ..., 100] (linear growth).
    # Post-fix, every entry must be ≤ 1.
    assert max(sizes) <= 1, (
        f"_entity_cache grew across files (max size {max(sizes)}); "
        f"cache locality must be per-file"
    )
    sdc_sync._entity_cache.clear()


def _patch_process_one_dependencies(monkeypatch, sdc_sync, current_mediaid, tmp_path):
    """Stub the legacy ``process_one`` entry point's network/IO so its
    file-boundary clear can be tested in isolation. ``parsed`` returning
    a falsy value causes the function to return after writing
    ``Missing ids.txt`` — chdir into ``tmp_path`` so that write happens
    in a throwaway directory rather than polluting the working tree."""
    fake_request = MagicMock()
    fake_request.submit.return_value = {
        "entities": {
            current_mediaid: {
                "pageid": int(current_mediaid.lstrip("M")),
                "statements": {},
            }
        }
    }
    fake_site = MagicMock()
    fake_site.simple_request.return_value = fake_request
    monkeypatch.setattr(sdc_sync, "site", fake_site, raising=False)
    monkeypatch.setattr(sdc_sync, "parsed", lambda *a, **kw: None)
    monkeypatch.chdir(tmp_path)


def test_process_one_clears_entity_cache_at_file_boundary(monkeypatch, tmp_path):
    """Symmetric regression guard for the legacy ``process_one`` entry
    point. Both file-boundary entry points (``process_one`` and
    ``process_one_from_sdc``) must flush the module-level cache; locking
    down only the partner-mode entry would silently leave the legacy
    path leaking on ``--file`` / ``--cat`` / ``--list`` reruns."""
    from tools import sdc_sync

    sdc_sync._entity_cache.update(
        {
            "M111": {"pageid": 111, "statements": {}},
            "M222": {"pageid": 222, "statements": {}},
            "M333": {"pageid": 333, "statements": {}},
        }
    )
    assert len(sdc_sync._entity_cache) == 3

    _patch_process_one_dependencies(monkeypatch, sdc_sync, "M999", tmp_path)

    sdc_sync.process_one("M999", "abcdef")

    for prior in ("M111", "M222", "M333"):
        assert prior not in sdc_sync._entity_cache, (
            f"prior file {prior} leaked across the legacy-path file boundary"
        )
    sdc_sync._entity_cache.clear()


def test_process_one_cache_does_not_grow_across_many_files(monkeypatch, tmp_path):
    """Direct memory-leak regression for the legacy path. Parallel to
    ``test_entity_cache_does_not_grow_across_many_files`` but exercising
    ``process_one`` rather than ``process_one_from_sdc``."""
    from tools import sdc_sync

    sdc_sync._entity_cache.clear()
    sizes = []
    for i in range(100):
        mediaid = f"M{1000 + i}"
        _patch_process_one_dependencies(monkeypatch, sdc_sync, mediaid, tmp_path)
        sdc_sync.process_one(mediaid, f"dpla{i:06d}")
        sizes.append(len(sdc_sync._entity_cache))

    assert max(sizes) <= 1, (
        f"_entity_cache grew across files via legacy path (max size "
        f"{max(sizes)}); cache locality must be per-file"
    )
    sdc_sync._entity_cache.clear()


# ---------------------------------------------------------------------------
# Single-edit dispatcher: every per-file accumulator drains into one
# wbeditentity POST per file. The atomic-edit guarantee from #38.
# ---------------------------------------------------------------------------


def test_submit_per_item_edit_no_op_when_all_fragment_lists_empty():
    """Empty accumulators → no POST. Files with nothing to change don't
    generate spurious revisions."""
    from tools import sdc_sync

    submit_calls = []
    with patch.object(
        sdc_sync,
        "_submit_sdc_write",
        side_effect=lambda *a, **kw: submit_calls.append((a, kw)),
    ):
        sdc_sync._submit_per_item_edit("M999", "abcdef", summary="summary")
    assert submit_calls == []


def test_submit_per_item_edit_bundles_all_fragments_into_one_post():
    """Every fragment kind — new claims, reference updates, qualifier
    updates, removals — gets bundled into a single ``wbeditentity``
    POST with the combined claims list in ``data.claims``."""
    import json

    from tools import sdc_sync

    new_claim = {
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"value": "abc", "type": "string"},
        },
        "type": "statement",
        "rank": "normal",
    }
    # Reference- and qualifier-update fragments carry ``type: "statement"``
    # — wbeditentity rejects the bundle without it. The dispatcher
    # backstops this by stamping the field on any fragment that's
    # missing it, so callers may pass fragments with or without it
    # interchangeably; the assertion below checks both fragments end up
    # with it in the POSTed payload regardless of which side set it.
    ref_update = {
        "id": "M999$existing",
        "type": "statement",
        "references": [{"snaks": {}}],
    }
    qual_update = {"id": "M999$amend", "qualifiers": {"P459": []}}
    removal = {"id": "M999$gone", "remove": ""}

    submit_calls = []
    with patch.object(
        sdc_sync,
        "_submit_sdc_write",
        side_effect=lambda action, mediaid, dpla_id, **kw: submit_calls.append(
            (action, mediaid, dpla_id, kw)
        ),
    ):
        sdc_sync._submit_per_item_edit(
            "M999",
            "abcdef",
            summary="combined edit",
            new_claims=[new_claim],
            reference_updates=[ref_update],
            qualifier_updates=[qual_update],
            removals=[removal],
        )

    assert len(submit_calls) == 1
    action, mediaid, dpla_id, kw = submit_calls[0]
    assert action == "wbeditentity"
    assert mediaid == "M999"
    payload = json.loads(kw["data"])
    # ``qual_update`` was passed in without ``type``; the dispatcher
    # should have stamped it on. Assert against the dispatcher-stamped
    # shape so this test enforces the invariant rather than enshrining
    # the broken pre-fix one.
    expected_qual_update = {**qual_update, "type": "statement"}
    assert payload["claims"] == [new_claim, ref_update, expected_qual_update, removal]


def test_submit_per_item_edit_increments_qualifier_counter():
    """Qualifier-only fragments still represent a real ``wbeditentity``
    write on a Commons file, so the dispatcher must bump
    ``SDC_QUALIFIER_UPDATES``. That counter is in ``_SDC_WRITE_COUNTERS``,
    so ``_sdc_writes_total()`` picks up the change and
    ``SDC_PAGES_EDITED`` counts the page — the rare case where every
    DPLA claim on the file is already on today's P813 (so the
    opportunistic refresh adds no reference fragments) and the only
    write left in the bundle is the qualifier amend."""
    from tools import sdc_sync

    qual_update = {"id": "M999$amend", "qualifiers": {"P459": []}}

    fake_tracker = MagicMock()
    with (
        patch.object(sdc_sync, "tracker", fake_tracker),
        patch.object(sdc_sync, "_submit_sdc_write", side_effect=lambda *a, **kw: None),
    ):
        sdc_sync._submit_per_item_edit(
            "M999",
            "abcdef",
            summary="qualifier-only edit",
            qualifier_updates=[qual_update],
        )

    fake_tracker.increment.assert_called_once_with(Result.SDC_QUALIFIER_UPDATES, 1)


def test_sdc_writes_total_includes_qualifier_updates():
    """The write-delta source used by the partner-mode loop and
    ``_safe_process_one`` to detect "did this ordinal write anything?"
    must include qualifier updates — otherwise the rare qualifier-only
    commit slips past ``SDC_PAGES_EDITED``."""
    from tools import sdc_sync

    counts = {r: 0 for r in sdc_sync._SDC_WRITE_COUNTERS}
    fake_tracker = MagicMock()
    fake_tracker.count.side_effect = lambda r: counts.get(r, 0)
    with patch.object(sdc_sync, "tracker", fake_tracker):
        before = sdc_sync._sdc_writes_total()
        counts[Result.SDC_QUALIFIER_UPDATES] = 3
        after = sdc_sync._sdc_writes_total()
    assert after - before == 3


def test_p813_refresh_skips_when_no_other_edits():
    """No fragments in any accumulator → no POST, including no P813
    refresh. The refresh only piggybacks on edits we're already making."""
    from tools import sdc_sync

    entity = {
        "pageid": 999,
        "statements": {
            "P760": [
                {
                    "id": "M999$existing",
                    "mainsnak": {
                        "property": "P760",
                        "snaktype": "value",
                        "datavalue": {"value": "abcdef", "type": "string"},
                    },
                    "qualifiers": {"P459": _dpla_p459()},
                    "references": [_dpla_reference("abcdef")],
                }
            ]
        },
    }

    sdc_sync._reset_per_file_accumulators()
    submit_calls = []
    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
        patch.object(
            sdc_sync,
            "_submit_sdc_write",
            side_effect=lambda *a, **kw: submit_calls.append((a, kw)),
        ),
    ):
        sdc_sync._flush_per_file_edits("M999", "abcdef")

    assert submit_calls == []
    sdc_sync._reset_per_file_accumulators()


def _stale_p813_ref(dpla_id):
    """DPLA-shape reference with an old P813 date so the refresh path
    will detect it as stale."""
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
                            "time": "+2024-01-01T00:00:00Z",
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


def test_p813_refresh_added_when_other_edits_exist():
    """When any other edit is being made on the file, the dispatcher
    refreshes P813 on all DPLA-authored claims whose P813 isn't already
    today's date."""
    import datetime as _dt
    import json

    from tools import sdc_sync

    old_claim = {
        "id": "M999$old",
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"value": "abcdef", "type": "string"},
        },
        "qualifiers": {"P459": _dpla_p459()},
        "references": [_stale_p813_ref("abcdef")],
    }
    entity = {"pageid": 999, "statements": {"P760": [old_claim]}}

    sdc_sync._reset_per_file_accumulators()
    # Queue ONE unrelated removal so the dispatcher decides to edit.
    sdc_sync.removals.append("M999$some-other-claim")

    submit_calls = []
    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
        patch.object(
            sdc_sync,
            "_submit_sdc_write",
            side_effect=lambda *a, **kw: submit_calls.append((a, kw)),
        ),
    ):
        sdc_sync._flush_per_file_edits("M999", "abcdef")

    payload = json.loads(submit_calls[0][1]["data"])
    today_iso = "+" + _dt.date.today().isoformat() + "T00:00:00Z"
    refresh = [
        c for c in payload["claims"] if c.get("id") == "M999$old" and "references" in c
    ]
    assert len(refresh) == 1
    refreshed_p813 = refresh[0]["references"][0]["snaks"]["P813"][0]["datavalue"][
        "value"
    ]["time"]
    assert refreshed_p813 == today_iso
    sdc_sync._reset_per_file_accumulators()


def test_p813_refresh_skips_claims_already_dated_today():
    """A DPLA reference whose P813 is already today is not re-emitted."""
    import datetime as _dt
    import json

    from tools import sdc_sync

    today_iso = "+" + _dt.date.today().isoformat() + "T00:00:00Z"
    fresh_ref = {
        "snaks": {
            "P854": [
                {
                    "snaktype": "value",
                    "property": "P854",
                    "datavalue": {
                        "type": "string",
                        "value": "https://dp.la/item/abcdef",
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
                            "time": today_iso,
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
    fresh_claim = {
        "id": "M999$fresh",
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"value": "abcdef", "type": "string"},
        },
        "qualifiers": {"P459": _dpla_p459()},
        "references": [fresh_ref],
    }
    entity = {"pageid": 999, "statements": {"P760": [fresh_claim]}}

    sdc_sync._reset_per_file_accumulators()
    sdc_sync.removals.append("M999$some-other-claim")

    submit_calls = []
    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
        patch.object(
            sdc_sync,
            "_submit_sdc_write",
            side_effect=lambda *a, **kw: submit_calls.append((a, kw)),
        ),
    ):
        sdc_sync._flush_per_file_edits("M999", "abcdef")

    payload = json.loads(submit_calls[0][1]["data"])
    fresh_refresh = [c for c in payload["claims"] if c.get("id") == "M999$fresh"]
    assert fresh_refresh == []
    sdc_sync._reset_per_file_accumulators()


def test_p813_refresh_preserves_user_added_references():
    """Files with a mix of DPLA-authored and user-added references get
    ONLY the DPLA reference refreshed; user references are preserved
    verbatim in the rewritten references list."""
    import datetime as _dt
    import json

    from tools import sdc_sync

    user_ref = {
        "snaks": {
            "P854": [
                {
                    "snaktype": "value",
                    "property": "P854",
                    "datavalue": {
                        "type": "string",
                        "value": "https://example.com/citation",
                    },
                }
            ],
        }
    }
    claim = {
        "id": "M999$mixed",
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"value": "abcdef", "type": "string"},
        },
        "qualifiers": {"P459": _dpla_p459()},
        "references": [user_ref, _stale_p813_ref("abcdef")],
    }
    entity = {"pageid": 999, "statements": {"P760": [claim]}}

    sdc_sync._reset_per_file_accumulators()
    sdc_sync.removals.append("M999$some-other-claim")

    submit_calls = []
    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
        patch.object(
            sdc_sync,
            "_submit_sdc_write",
            side_effect=lambda *a, **kw: submit_calls.append((a, kw)),
        ),
    ):
        sdc_sync._flush_per_file_edits("M999", "abcdef")

    payload = json.loads(submit_calls[0][1]["data"])
    refresh = next(
        c
        for c in payload["claims"]
        if c.get("id") == "M999$mixed" and "references" in c
    )
    refs = refresh["references"]
    assert refs[0] == user_ref
    today_iso = "+" + _dt.date.today().isoformat() + "T00:00:00Z"
    assert refs[1]["snaks"]["P813"][0]["datavalue"]["value"]["time"] == today_iso
    sdc_sync._reset_per_file_accumulators()


def test_flush_emits_type_statement_on_every_non_removal_fragment():
    """Regression: wbeditentity rejects the entire bundle with
    ``invalid-claim: Type is missing`` unless every non-removal claim
    entry carries ``type: "statement"``. The dispatcher's atomicity then
    drops every other edit too — so a missing ``type`` on one fragment
    silently blocks unrelated P2699 backfills, new-claim adds, and
    removals on the same file.

    Exercise the realistic mix: one queued qualifier-amend (drives a
    qualifier-update fragment), one queued removal (drives a removal
    fragment), and a DPLA-authored claim with a stale P813 reference
    (drives a P813 refresh fragment). Assert every non-removal claim
    in the POSTed payload carries ``type: "statement"``.
    """
    import json

    from tools import sdc_sync

    stale_claim = {
        "id": "M999$stale",
        "mainsnak": {
            "property": "P760",
            "snaktype": "value",
            "datavalue": {"value": "abcdef", "type": "string"},
        },
        "qualifiers": {"P459": _dpla_p459()},
        "references": [_stale_p813_ref("abcdef")],
    }
    amend_target = {
        "id": "M999$amend",
        "rank": "preferred",
        "mainsnak": {
            "property": "P7482",
            "snaktype": "value",
            "datavalue": {
                "type": "wikibase-entityid",
                "value": {"entity-type": "item", "id": "Q74228490"},
            },
        },
        "qualifiers": {"P459": _dpla_p459()},
        "references": [_dpla_reference("abcdef")],
    }
    entity = {
        "pageid": 999,
        "statements": {
            "P760": [stale_claim],
            "P7482": [amend_target],
        },
    }

    sdc_sync._reset_per_file_accumulators()
    sdc_sync.qualifier_amends.append(
        ("M999$amend", "P2699", sdc_sync._url_snak("P2699", "https://example.com/foo"))
    )
    sdc_sync.removals.append("M999$gone")

    submit_calls = []
    with (
        patch.object(sdc_sync, "get_entity", return_value=entity),
        patch.object(sdc_sync, "invalidate_entity"),
        patch.object(
            sdc_sync,
            "_submit_sdc_write",
            side_effect=lambda *a, **kw: submit_calls.append((a, kw)),
        ),
    ):
        sdc_sync._flush_per_file_edits("M999", "abcdef")

    assert len(submit_calls) == 1, "dispatcher should POST exactly once"
    payload = json.loads(submit_calls[0][1]["data"])
    sent_claims = payload["claims"]
    by_id = {c.get("id"): c for c in sent_claims}
    # Sanity: every code path should be exercised by this fixture so
    # the assertions below actually cover all three fragment kinds.
    assert "M999$gone" in by_id, "removal fragment should be present"
    assert "M999$amend" in by_id, "qualifier-update fragment should be present"
    assert "M999$stale" in by_id, "P813 refresh fragment should be present"
    for claim in sent_claims:
        if claim.get("remove") == "":
            continue  # removal entries are exempt from the type/mainsnak rule
        assert claim.get("type") == "statement", (
            f"non-removal fragment missing type:statement — wbeditentity "
            f"would reject the bundle: {claim!r}"
        )
        # wbeditentity treats every non-removal claim as a
        # wholesale-replace; partial fragments ``{id, qualifiers}`` /
        # ``{id, references}`` get rejected with
        # ``invalid-claim: Attribute "mainsnak" is missing`` and the
        # atomic bundle (every OTHER edit in this file's per-file
        # batch) is dropped with it.
        assert "mainsnak" in claim, (
            f"non-removal fragment missing mainsnak — wbeditentity "
            f"would reject the bundle: {claim!r}"
        )
    # Sibling-field preservation: wbeditentity wholesale-replaces every
    # field provided, so a fragment that drops the sibling field (e.g.
    # qualifier-update without ``references``) would silently erase
    # those references on the live statement. Catch both the rejection
    # path AND the silent-data-loss side by asserting each fragment
    # kind preserves the fields it isn't supposed to modify.
    assert by_id["M999$amend"]["references"] == amend_target["references"], (
        "qualifier-update fragment must preserve existing references"
    )
    assert by_id["M999$amend"]["rank"] == "preferred", (
        "qualifier-update fragment must preserve existing rank"
    )
    assert by_id["M999$stale"]["qualifiers"] == stale_claim["qualifiers"], (
        "P813 refresh fragment must preserve existing qualifiers"
    )
    sdc_sync._reset_per_file_accumulators()


# ---------------------------------------------------------------------------
# pageid recovery + cleanup-guard: null-pageid live bug repro
# (https://commons.wikimedia.org/wiki/File:Southern_Railway_Company,_Valuation_Section_22_-_DPLA_-_e314839e2ca3906b29bcbecc3d615740_(page_1).tiff)
# ---------------------------------------------------------------------------


def test_resolve_pageid_from_title_returns_pid_on_existing_page(monkeypatch):
    """The fallback for upload-result.json sidecars with null/0
    pageid: query Commons for the page id given the title."""
    from tools import sdc_sync

    fake_site = MagicMock()
    fake_site.simple_request.return_value.submit.return_value = {
        "query": {"pages": {"193644002": {"pageid": 193644002, "title": "File:X.tiff"}}}
    }
    monkeypatch.setattr(sdc_sync, "site", fake_site, raising=False)

    assert sdc_sync._resolve_pageid_from_title("File:X.tiff") == 193644002


def test_resolve_pageid_from_title_returns_none_on_missing_page(monkeypatch):
    """Page genuinely doesn't exist on Commons (upload silently
    failed): no pageid to recover, caller drops into the existing
    skip-with-counter path."""
    from tools import sdc_sync

    fake_site = MagicMock()
    fake_site.simple_request.return_value.submit.return_value = {
        "query": {"pages": {"-1": {"missing": "", "title": "File:Nope.tiff"}}}
    }
    monkeypatch.setattr(sdc_sync, "site", fake_site, raising=False)

    assert sdc_sync._resolve_pageid_from_title("File:Nope.tiff") is None


def test_resolve_pageid_from_title_returns_none_on_api_error(monkeypatch):
    """API failure during the fallback shouldn't crash the partner
    batch — just signal "couldn't recover" so the call site falls
    into its existing skip path."""
    from tools import sdc_sync

    fake_site = MagicMock()
    fake_site.simple_request.return_value.submit.side_effect = RuntimeError("boom")
    monkeypatch.setattr(sdc_sync, "site", fake_site, raising=False)

    assert sdc_sync._resolve_pageid_from_title("File:X.tiff") is None


def test_resolve_pageid_from_title_returns_none_on_empty_title(monkeypatch):
    """Bare empty/None title shouldn't even hit the API."""
    from tools import sdc_sync

    fake_site = MagicMock()
    monkeypatch.setattr(sdc_sync, "site", fake_site, raising=False)

    assert sdc_sync._resolve_pageid_from_title("") is None
    assert sdc_sync._resolve_pageid_from_title(None) is None  # type: ignore[arg-type]
    assert fake_site.simple_request.call_count == 0


def test_resolve_pageid_from_title_prepends_file_namespace(monkeypatch):
    """Live-bug regression on the post-#302 deploy: ``upload-result.json``
    stores titles WITHOUT the ``File:`` namespace prefix — that's the
    form pywikibot's ``FilePage.title(with_ns=False)`` returns and what
    the uploader serialises. A bare ``titles=<bare title>`` query
    against the Commons API resolves to the *main* namespace, where
    DPLA media files don't live; the response reports ``missing`` and
    the fallback returned ``None`` even on files that actually exist
    (e.g. the Southern Railway file the PR was supposed to self-heal).

    Fix: the fallback prepends ``File:`` when absent so the lookup
    lands in the correct namespace."""
    from tools import sdc_sync

    captured_titles = []

    def stub_submit():
        # Mirror MediaWiki's response shape for a File:-namespace hit.
        return {
            "query": {
                "pages": {
                    "193644002": {
                        "pageid": 193644002,
                        "ns": 6,
                        "title": "File:X.tiff",
                    }
                }
            }
        }

    def stub_simple_request(**kwargs):
        captured_titles.append(kwargs.get("titles"))
        m = MagicMock()
        m.submit.side_effect = stub_submit
        return m

    fake_site = MagicMock()
    fake_site.simple_request.side_effect = stub_simple_request
    monkeypatch.setattr(sdc_sync, "site", fake_site, raising=False)

    # Bare title — what upload-result.json actually stores.
    assert sdc_sync._resolve_pageid_from_title("X.tiff") == 193644002
    # Already-prefixed title — pass through unchanged.
    assert sdc_sync._resolve_pageid_from_title("File:X.tiff") == 193644002

    assert captured_titles == ["File:X.tiff", "File:X.tiff"], (
        "both bare and File:-prefixed inputs must query File:X.tiff"
    )


def test_entity_has_dpla_attributed_claims_finds_p459_q61848113():
    """The cleanup guard's notion of "has DPLA SDC" matches
    Module:DPLA's ``isDplaDetermined`` filter: at least one statement
    with P459 = Q61848113 qualifier."""
    from tools import sdc_sync

    entity = {
        "statements": {
            "P195": [
                {
                    "mainsnak": {"snaktype": "value"},
                    "qualifiers": {
                        "P459": [
                            {
                                "snaktype": "value",
                                "datavalue": {
                                    "value": {"id": "Q61848113"},
                                    "type": "wikibase-entityid",
                                },
                            }
                        ]
                    },
                }
            ]
        }
    }
    assert sdc_sync._entity_has_dpla_attributed_claims(entity) is True


def test_entity_has_dpla_attributed_claims_false_on_empty_entity():
    """An empty entity (no-such-entity post-fetch, or zero claims) is
    the bug shape the guard exists to catch — return False so the
    cleanup refuses to strip wikitext."""
    from tools import sdc_sync

    assert sdc_sync._entity_has_dpla_attributed_claims({}) is False
    assert sdc_sync._entity_has_dpla_attributed_claims({"statements": {}}) is False


def test_entity_has_dpla_attributed_claims_false_on_unrelated_p459_value():
    """Other heuristic determination Q-IDs (e.g. P459 = Q131783016
    for inferred-from-Wikitext community-import claims) don't count
    as DPLA-attributed."""
    from tools import sdc_sync

    entity = {
        "statements": {
            "P1476": [
                {
                    "mainsnak": {"snaktype": "value"},
                    "qualifiers": {
                        "P459": [
                            {
                                "snaktype": "value",
                                "datavalue": {
                                    "value": {"id": "Q131783016"},
                                    "type": "wikibase-entityid",
                                },
                            }
                        ]
                    },
                }
            ]
        }
    }
    assert sdc_sync._entity_has_dpla_attributed_claims(entity) is False


def test_post_sdc_cleanup_for_page_refuses_strip_when_entity_has_no_dpla_sdc(
    monkeypatch,
):
    """Live-bug regression
    (https://commons.wikimedia.org/wiki/File:Southern_Railway_Company,_Valuation_Section_22_-_DPLA_-_e314839e2ca3906b29bcbecc3d615740_(page_1).tiff):
    when the per-ordinal SDC write was skipped (upstream null pageid)
    but the post-SDC cleanup ran anyway, the strip removed every
    wikitext param matching the DPLA canonical value, leaving the
    page with no metadata in either representation. The defensive
    guard fetches the entity and refuses to strip when it has zero
    DPLA-attributed claims."""
    from ingest_wikimedia import wikimedia, wikitext_normalize
    from tools import sdc_sync

    fake_page = MagicMock(name="FilePage")
    fake_page.exists.return_value = True
    fake_page.pageid = 193644002
    fake_page.title.return_value = "File:Southern Railway.tiff"
    fake_page.text = (
        "{{DPLA metadata\n"
        "| title = Southern Railway Company, Valuation Section 22\n"
        "| dpla_id = e314839e2ca3906b29bcbecc3d615740\n"
        "}}"
    )

    monkeypatch.setattr(
        wikimedia,
        "dpla_metadata_params",
        lambda *a, **kw: {"title": "Southern Railway Company, Valuation Section 22"},
    )
    # Entity exists but is empty — no DPLA-attributed claims.
    monkeypatch.setattr(
        sdc_sync, "_fetch_entity_for_cleanup_guard", lambda mid: {"statements": {}}
    )

    strip_calls = []
    monkeypatch.setattr(
        wikitext_normalize,
        "normalize_page",
        lambda *a, **kw: strip_calls.append((a, kw)),
    )

    sdc_sync._post_sdc_cleanup_for_page(
        fake_page,
        "e314839e2ca3906b29bcbecc3d615740",
        {
            "sourceResource": {
                "title": ["Southern Railway Company, Valuation Section 22"]
            }
        },
        {"Wikidata": "Q518155"},
        {"Wikidata": "Q59661038"},
    )

    assert strip_calls == [], (
        "strip must not run against an entity with no DPLA-attributed SDC"
    )


def test_post_sdc_cleanup_for_page_strips_when_entity_has_dpla_sdc(monkeypatch):
    """Happy path: the entity DOES carry DPLA-attributed SDC, the
    guard passes, and the strip runs as before. Locks the guard in
    as a *defensive* layer that only activates on the bug shape."""
    from ingest_wikimedia import wikimedia, wikitext_normalize
    from tools import sdc_sync

    fake_page = MagicMock(name="FilePage")
    fake_page.exists.return_value = True
    fake_page.pageid = 12345
    fake_page.title.return_value = "File:Healthy.tiff"
    fake_page.text = "{{DPLA metadata\n| title = X\n}}"

    monkeypatch.setattr(
        wikimedia, "dpla_metadata_params", lambda *a, **kw: {"title": "X"}
    )
    monkeypatch.setattr(
        sdc_sync,
        "_fetch_entity_for_cleanup_guard",
        lambda mid: {
            "statements": {
                "P1476": [
                    {
                        "mainsnak": {"snaktype": "value"},
                        "qualifiers": {
                            "P459": [
                                {
                                    "snaktype": "value",
                                    "datavalue": {
                                        "value": {"id": "Q61848113"},
                                        "type": "wikibase-entityid",
                                    },
                                }
                            ]
                        },
                    }
                ]
            }
        },
    )

    strip_calls = []
    monkeypatch.setattr(
        wikitext_normalize,
        "normalize_page",
        lambda *a, **kw: strip_calls.append((a, kw)),
    )

    sdc_sync._post_sdc_cleanup_for_page(
        fake_page,
        "dpla-id",
        {"sourceResource": {"title": ["X"]}},
        {"Wikidata": "Q1"},
        {"Wikidata": "Q2"},
    )

    assert len(strip_calls) == 1


# ---------------------------------------------------------------------------
# Item-level outcome classification: partial-sync items distinct from
# fully-synced. Per CR review on PR #302 — mixed-result items (one ordinal
# synced, sibling ordinal hit MISSING_PAGEID / SKIPPED_ERROR) shouldn't be
# silently counted as fully ``SDC_ITEMS_SYNCED``.
# ---------------------------------------------------------------------------


def test_classify_item_outcome_full_sync():
    """All ordinals synced cleanly → full ``SDC_ITEMS_SYNCED``."""
    from tools.sdc_sync import _classify_item_outcome

    assert (
        _classify_item_outcome(synced_this_item=True, had_ordinal_error=False)
        == Result.SDC_ITEMS_SYNCED
    )


def test_classify_item_outcome_partial_sync():
    """Live-bug regression: at least one ordinal synced AND at
    least one sibling ordinal hit the error / null-pageid skip
    path → ``SDC_ITEMS_PARTIALLY_SYNCED``, not the full-sync
    bucket. Dashboards keying on full-sync as "items healthy"
    correctly excluded."""
    from tools.sdc_sync import _classify_item_outcome

    assert (
        _classify_item_outcome(synced_this_item=True, had_ordinal_error=True)
        == Result.SDC_ITEMS_PARTIALLY_SYNCED
    )


def test_classify_item_outcome_all_errored():
    """No ordinals synced but at least one raised → error bucket,
    not the no-progress mapping bucket."""
    from tools.sdc_sync import _classify_item_outcome

    assert (
        _classify_item_outcome(synced_this_item=False, had_ordinal_error=True)
        == Result.SDC_ITEMS_SKIPPED_ERROR
    )


def test_classify_item_outcome_no_progress():
    """No ordinals synced AND no ordinals errored — every eligible
    ordinal silently no-op'd somewhere. Mapping skip."""
    from tools.sdc_sync import _classify_item_outcome

    assert (
        _classify_item_outcome(synced_this_item=False, had_ordinal_error=False)
        == Result.SDC_ITEMS_SKIPPED_MAPPING
    )


# ---------------------------------------------------------------------------
# _reconcile_inferred_from_wikitext_dupes — idempotency cleanup. Removes
# inferred-from-Wikitext claims (P887 → Q131783016 reference) whose
# comparable value equals a DPLA-attributed claim on the same property.
# Covers the M193555788 (Mission Grove) case where the legacy migrator
# preserved {{other date|between|1934|1948}} alongside DPLA's
# "1934 - 1948" because the structured-time matcher couldn't see ranges.
# ---------------------------------------------------------------------------


def _inferred_from_wikitext_reference(
    permalink="https://commons.wikimedia.org/w/index.php?oldid=1227892018",
):
    """Reference snak set the legacy migration importer stamps on
    every claim it adds: P887 → Q131783016 ("inferred from Wikitext")
    plus P4656 → permalink. Recognised by the dedup cleanup as "this
    is one of ours, safe to remove if equivalent to DPLA's"."""
    return {
        "snaks": {
            "P887": _qual_entity("P887", "Q131783016"),
            "P4656": [
                {
                    "snaktype": "value",
                    "property": "P4656",
                    "datavalue": {"type": "string", "value": permalink},
                }
            ],
        }
    }


def _p571_somevalue_with_p1932(stmt_id, p1932_value, references=None, p459=True):
    """Build a P571 somevalue+P1932 statement — the canonical shape for
    a range-shaped claim, used for both the DPLA-attributed and the
    inferred-from-Wikitext sides of the dedup test."""
    quals = {
        "P1932": _qual_string("P1932", p1932_value),
    }
    if p459:
        quals["P459"] = _dpla_p459()
    return _stmt(
        stmt_id=stmt_id,
        prop="P571",
        snaktype="somevalue",
        value=None,
        qualifiers=quals,
        references=references or [],
    )


def test_inferred_dupe_cleanup_removes_equivalent_range_claim():
    """Mission Grove repro — DPLA's P571 somevalue+P1932="1934 - 1948"
    and an inferred-from-Wikitext P571 somevalue+P1932="{{other date|
    between|1934|1948}}" should dedup to one claim; the inferred
    statement gets queued for removal."""
    from tools import sdc_sync

    dpla_claim = _p571_somevalue_with_p1932(
        "M999$DPLA",
        "1934 - 1948",
        references=[_dpla_reference()],
    )
    inferred_claim = _p571_somevalue_with_p1932(
        "M999$INFERRED",
        "{{other date|between|1934|1948}}",
        references=[_inferred_from_wikitext_reference()],
        p459=False,  # inferred claims don't carry the DPLA-determination qualifier
    )
    entity = {
        "pageid": 999,
        "statements": {"P571": [dpla_claim, inferred_claim]},
    }

    sdc_sync._reset_per_file_accumulators()
    with patch.object(sdc_sync, "get_entity", return_value=entity):
        sdc_sync._reconcile_inferred_from_wikitext_dupes("M999")
    assert sdc_sync.removals == ["M999$INFERRED"]
    sdc_sync._reset_per_file_accumulators()


def test_inferred_dupe_cleanup_preserves_distinct_range_claim():
    """Inferred range that does NOT match DPLA's range is real
    community-contributed information — never queue for removal."""
    from tools import sdc_sync

    dpla_claim = _p571_somevalue_with_p1932(
        "M999$DPLA",
        "1934 - 1948",
        references=[_dpla_reference()],
    )
    inferred_claim = _p571_somevalue_with_p1932(
        "M999$DISTINCT",
        "1950 - 1960",
        references=[_inferred_from_wikitext_reference()],
        p459=False,
    )
    entity = {
        "pageid": 999,
        "statements": {"P571": [dpla_claim, inferred_claim]},
    }

    sdc_sync._reset_per_file_accumulators()
    with patch.object(sdc_sync, "get_entity", return_value=entity):
        sdc_sync._reconcile_inferred_from_wikitext_dupes("M999")
    assert sdc_sync.removals == []
    sdc_sync._reset_per_file_accumulators()


def test_inferred_dupe_cleanup_ignores_foreign_community_claim():
    """Safety bar: a community claim that happens to encode the same
    range as DPLA but lacks the P887 → Q131783016 inferred-from-
    Wikitext fingerprint is third-party content and must NOT be touched.
    Same string equivalence; only the reference shape distinguishes them."""
    from tools import sdc_sync

    dpla_claim = _p571_somevalue_with_p1932(
        "M999$DPLA",
        "1934 - 1948",
        references=[_dpla_reference()],
    )
    foreign_claim = _p571_somevalue_with_p1932(
        "M999$FOREIGN",
        "between 1934 and 1948",
        references=[_foreign_reference()],
        p459=False,
    )
    entity = {
        "pageid": 999,
        "statements": {"P571": [dpla_claim, foreign_claim]},
    }

    sdc_sync._reset_per_file_accumulators()
    with patch.object(sdc_sync, "get_entity", return_value=entity):
        sdc_sync._reconcile_inferred_from_wikitext_dupes("M999")
    assert sdc_sync.removals == []
    sdc_sync._reset_per_file_accumulators()


def test_inferred_dupe_cleanup_removes_equivalent_single_date_claim():
    """Idempotency for non-range claims: if a single-date inferred-
    from-Wikitext claim parses to the same canonical time as a DPLA
    value-typed time claim, remove the inferred one. Covers the case
    where DPLA drifts into emitting the structured time for a value
    the importer had previously preserved as somevalue+P1932="1945"."""
    from tools import sdc_sync

    # Hand-built value-typed time statement — _stmt's generic builder
    # doesn't cover the time mainsnak shape (no other reconciler test
    # needed it).
    dpla_claim = {
        "id": "M999$DPLA",
        "type": "statement",
        "rank": "normal",
        "mainsnak": {
            "snaktype": "value",
            "property": "P571",
            "datatype": "time",
            "datavalue": {
                "type": "time",
                "value": {
                    "time": "+1945-01-01T00:00:00Z",
                    "precision": 9,
                    "before": 0,
                    "after": 0,
                    "timezone": 0,
                    "calendarmodel": "http://www.wikidata.org/entity/Q1985727",
                },
            },
        },
        "qualifiers": {"P459": _dpla_p459()},
        "references": [_dpla_reference()],
    }
    inferred_claim = _p571_somevalue_with_p1932(
        "M999$INFERRED",
        "1945",
        references=[_inferred_from_wikitext_reference()],
        p459=False,
    )
    entity = {
        "pageid": 999,
        "statements": {"P571": [dpla_claim, inferred_claim]},
    }

    sdc_sync._reset_per_file_accumulators()
    with patch.object(sdc_sync, "get_entity", return_value=entity):
        sdc_sync._reconcile_inferred_from_wikitext_dupes("M999")
    assert sdc_sync.removals == ["M999$INFERRED"]
    sdc_sync._reset_per_file_accumulators()


def test_inferred_dupe_cleanup_skips_property_with_no_dpla_claim():
    """When no DPLA-attributed claim exists on a property, even a
    matching inferred-from-Wikitext claim must stay — there's no
    duplication to clean up."""
    from tools import sdc_sync

    inferred_claim = _p571_somevalue_with_p1932(
        "M999$ALONE",
        "1934 - 1948",
        references=[_inferred_from_wikitext_reference()],
        p459=False,
    )
    entity = {
        "pageid": 999,
        "statements": {"P571": [inferred_claim]},
    }

    sdc_sync._reset_per_file_accumulators()
    with patch.object(sdc_sync, "get_entity", return_value=entity):
        sdc_sync._reconcile_inferred_from_wikitext_dupes("M999")
    assert sdc_sync.removals == []
    sdc_sync._reset_per_file_accumulators()


# ---------------------------------------------------------------------------
# _find_existing_commons_files_by_dpla_id — Commons-side discovery primitive
# that decouples SDC eligibility from the upload phase's per-ordinal status.
# Lets a file already on Commons get its SDC re-synced when the current
# run's binary path failed (e.g. NARA upstream URLs went stale).
# ---------------------------------------------------------------------------


def _search_hit(pageid: int, title: str) -> dict:
    """Build a CirrusSearch result hit in the shape Commons returns."""
    return {"pageid": pageid, "ns": 6, "title": title}


def test_find_existing_commons_files_returns_ordinal_map(monkeypatch):
    """The happy path: search returns N hits, helper parses each
    title's ``(page N)`` suffix into an ord_str and pairs it with the
    pageid. Titles are returned without the ``File:`` prefix to match
    what upload-result.json carries."""
    from tools import sdc_sync

    dpla_id = "1cac77c12985e6c4a35039031ea49c07"
    hits = [
        _search_hit(101521797, f"File:Foo - DPLA - {dpla_id} (page 1).jpg"),
        _search_hit(101521981, f"File:Foo - DPLA - {dpla_id} (page 19).jpg"),
        _search_hit(101522004, f"File:Foo - DPLA - {dpla_id} (page 20).jpg"),
    ]
    fake_site = MagicMock()
    fake_site.simple_request.return_value.submit.return_value = {
        "query": {"search": hits}
    }
    monkeypatch.setattr(sdc_sync, "site", fake_site, raising=False)

    found = sdc_sync._find_existing_commons_files_by_dpla_id(dpla_id)
    assert found == {
        "1": {"title": f"Foo - DPLA - {dpla_id} (page 1).jpg", "pageid": 101521797},
        "19": {"title": f"Foo - DPLA - {dpla_id} (page 19).jpg", "pageid": 101521981},
        "20": {"title": f"Foo - DPLA - {dpla_id} (page 20).jpg", "pageid": 101522004},
    }


def test_find_existing_commons_files_single_file_item(monkeypatch):
    """Single-file DPLA items don't carry the ``(page N)`` suffix —
    helper maps them to ord_str ``"1"`` since the uploader's single-
    ordinal items always serialize as ordinal 1 in upload-result.json."""
    from tools import sdc_sync

    dpla_id = "deadbeef00000000000000000000beef"
    hits = [_search_hit(12345, f"File:A single file - DPLA - {dpla_id}.jpg")]
    fake_site = MagicMock()
    fake_site.simple_request.return_value.submit.return_value = {
        "query": {"search": hits}
    }
    monkeypatch.setattr(sdc_sync, "site", fake_site, raising=False)

    found = sdc_sync._find_existing_commons_files_by_dpla_id(dpla_id)
    assert found == {
        "1": {"title": f"A single file - DPLA - {dpla_id}.jpg", "pageid": 12345}
    }


def test_find_existing_commons_files_returns_empty_on_no_hits(monkeypatch):
    """File never made it to Commons (or was deleted) — empty result
    set. Caller's existing skip path runs unchanged."""
    from tools import sdc_sync

    fake_site = MagicMock()
    fake_site.simple_request.return_value.submit.return_value = {
        "query": {"search": []}
    }
    monkeypatch.setattr(sdc_sync, "site", fake_site, raising=False)

    assert sdc_sync._find_existing_commons_files_by_dpla_id("a" * 32) == {}


def test_find_existing_commons_files_returns_empty_on_api_error(monkeypatch):
    """Transient Commons API failure during discovery must not abort
    the partner batch — return ``{}`` and the caller's skip path runs."""
    from tools import sdc_sync

    fake_site = MagicMock()
    fake_site.simple_request.return_value.submit.side_effect = RuntimeError("boom")
    monkeypatch.setattr(sdc_sync, "site", fake_site, raising=False)

    assert sdc_sync._find_existing_commons_files_by_dpla_id("a" * 32) == {}


def test_find_existing_commons_files_returns_empty_on_blank_dpla_id(monkeypatch):
    """Defense-in-depth — never issue an unbounded ``intitle:""``
    search just because the DPLA ID was empty."""
    from tools import sdc_sync

    fake_site = MagicMock()
    monkeypatch.setattr(sdc_sync, "site", fake_site, raising=False)

    assert sdc_sync._find_existing_commons_files_by_dpla_id("") == {}
    assert sdc_sync._find_existing_commons_files_by_dpla_id(None) == {}  # type: ignore[arg-type]
    assert fake_site.simple_request.call_count == 0


def test_find_existing_commons_files_skips_non_canonical_titles(monkeypatch):
    """A search hit whose title contains the DPLA ID but doesn't match
    the canonical ``- DPLA - <id> (page N).<ext>`` pattern (someone
    uploaded a file with that ID embedded mid-title, or with a non-
    file extension) is skipped rather than guessing an ordinal."""
    from tools import sdc_sync

    dpla_id = "1cac77c12985e6c4a35039031ea49c07"
    hits = [
        # Canonical — keep.
        _search_hit(1, f"File:Foo - DPLA - {dpla_id} (page 1).jpg"),
        # ID is in title but not in the canonical position — skip.
        _search_hit(2, f"File:Notes on {dpla_id} - other text.txt"),
    ]
    fake_site = MagicMock()
    fake_site.simple_request.return_value.submit.return_value = {
        "query": {"search": hits}
    }
    monkeypatch.setattr(sdc_sync, "site", fake_site, raising=False)

    found = sdc_sync._find_existing_commons_files_by_dpla_id(dpla_id)
    assert list(found.keys()) == ["1"]
