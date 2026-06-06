"""CLI-level tests for tools/get_ids_es.py.

Focused on the new multi-``--institution`` + ``--collection`` argument
validation contract added when launch.py grew QID-combine support. The
ES query construction and S3 staging paths are exercised end-to-end by
the actual partner runs — only the validation surface that can silently
ship a wrong CLI is unit-tested here.
"""

from __future__ import annotations

from unittest.mock import patch

from click.testing import CliRunner


def _invoke(*args: str):
    """Invoke get-ids-es's Click app with monkey-patched DPLA.check_partner
    so we don't need a config.toml. Stops further execution after
    validation by patching everything that follows."""
    from tools import get_ids_es

    with (
        patch.object(get_ids_es.DPLA, "check_partner", return_value=None),
        patch.object(get_ids_es, "notify_phase_start"),
        patch.object(get_ids_es, "PARTNER_HUBS", {"bpl": "Digital Commonwealth"}),
        patch.object(
            get_ids_es,
            "fetch_institutions_v2",
            # ``load_eligible_dp_names`` only counts an institution as
            # eligible when BOTH the hub and the institution carry a
            # ``Wikidata`` field — that's how the production data
            # excludes records that haven't been fully onboarded.
            # Stubbed values are arbitrary QIDs; they exist solely to
            # pass the eligibility gate so the tests can exercise the
            # CLI-validation contract we actually care about.
            return_value={
                "Digital Commonwealth": {
                    "Wikidata": "Q12345",
                    "institutions": {
                        "Boston Public Library": {
                            "upload": True,
                            "Wikidata": "Q1001",
                        },
                        "Boston City Archives": {
                            "upload": True,
                            "Wikidata": "Q1002",
                        },
                    },
                }
            },
        ),
        # Short-circuit AFTER validation by raising at the next external call.
        patch.object(
            get_ids_es,
            "fetch_subjects_json",
            side_effect=RuntimeError("stop-after-validation"),
        ),
        patch.object(get_ids_es, "load_rights_json", return_value={}),
    ):
        runner = CliRunner()
        return runner.invoke(get_ids_es.main, list(args), catch_exceptions=False)


def test_collection_requires_exactly_one_institution_zero():
    """``--collection`` with no ``--institution`` must fail validation
    (collection scoping is per-institution)."""
    result = _invoke(
        "bpl",
        "--collection",
        "Some Collection",
    )
    assert result.exit_code == 1, result.output
    assert "exactly one --institution" in result.output


def test_collection_requires_exactly_one_institution_many():
    """``--collection`` with multiple ``--institution`` values must fail
    validation — combining a collection scope across institutions is
    not meaningful."""
    result = _invoke(
        "bpl",
        "--institution",
        "Boston Public Library",
        "--institution",
        "Boston City Archives",
        "--collection",
        "Some Collection",
    )
    assert result.exit_code == 1, result.output
    assert "exactly one --institution" in result.output


def test_multiple_institutions_pass_validation_and_combine():
    """Multiple ``--institution`` values without ``--collection`` must
    pass validation and reach the ID-generation path (where our stubbed
    ``fetch_subjects_json`` raises a known sentinel — proving the run
    got past validation rather than rejecting the inputs upfront)."""
    result = _invoke(
        "bpl",
        "--institution",
        "Boston Public Library",
        "--institution",
        "Boston City Archives",
    )
    assert isinstance(result.exception, RuntimeError)
    assert "stop-after-validation" in str(result.exception)


def test_unknown_institution_rejected_with_clear_message():
    """An institution name that isn't in the hub's eligible set must be
    rejected explicitly, naming the offender — this is the safety net
    that prevents the launch script from silently asking ES for an
    institution that doesn't exist."""
    result = _invoke(
        "bpl",
        "--institution",
        "Boston Public Library",
        "--institution",
        "Nonexistent Institution",
    )
    assert result.exit_code == 1, result.output
    assert "not upload-eligible" in result.output
    assert "Nonexistent Institution" in result.output
