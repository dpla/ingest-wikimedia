"""Tests for tools/resolve_dpla_ids.py — the single-item resolver called by
the launch script's ``/wikimedia-upload <dpla-id>`` and
``/wikimedia-upload maintain <dpla-id>`` paths.

The bug being pinned: pre-fix, ``resolve_dpla_ids`` unconditionally routed
through ``is_item_upload_eligible`` — which requires ``upload=True`` on the
institution — so a single-ID maintain launch against a de-opted institution
(e.g. Internet Archive) died with the misleading "missing Wikidata ID or
upload flag" INELIGIBLE message even when both Wikidata IDs were present.
"""

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from tools import resolve_dpla_ids


def _es_response(source: dict) -> MagicMock:
    """Fake requests.Response yielding a one-hit ES result for `source`."""
    resp = MagicMock()
    resp.json.return_value = {"hits": {"hits": [{"_source": source}]}}
    resp.raise_for_status.return_value = None
    return resp


def _make_source(dpla_id: str = "abc") -> dict:
    """Minimal ES `_source` covering the checks in `_process_one` before the
    eligibility gate: banlist, rights, media, hub resolution."""
    return {
        "id": dpla_id,
        "rightsCategory": "Unlimited Re-Use",
        "mediaMaster": ["https://example.org/foo.jpg"],
        "isShownAt": "https://example.org/item/abc",
        "provider": {"name": "Internet Archive"},
        "dataProvider": {"name": "Internet Archive"},
    }


def _invoke(*args: str) -> object:
    """Run resolve_dpla_ids.main with the given CLI args + mocked I/O."""
    with (
        patch.object(
            resolve_dpla_ids, "post_es", return_value=_es_response(_make_source())
        ),
        patch.object(resolve_dpla_ids, "check_es_response", lambda _: None),
        patch.object(resolve_dpla_ids, "Banlist") as banlist_cls,
        patch.object(resolve_dpla_ids, "S3Client") as s3_cls,
        patch.object(resolve_dpla_ids, "resolve_slug", return_value="ia"),
        patch.object(resolve_dpla_ids, "check_item_eligibility") as check_eligibility,
    ):
        banlist_cls.return_value.is_banned.return_value = False
        s3_cls.return_value.write_item_metadata.return_value = None
        # Set a default return that surfaces the `maintain` arg for
        # inspection — tests below assert on it directly.
        check_eligibility.return_value = (True, "")
        result = CliRunner().invoke(resolve_dpla_ids.main, list(args))
        return result, check_eligibility, s3_cls.return_value


def test_default_call_passes_maintain_false_to_eligibility():
    """The single-DPLA-ID launch path (no --maintain) must gate on the
    upload-eligibility profile — matches the pre-fix behaviour for the
    upload subcommand."""
    result, check_eligibility, _ = _invoke("abc")
    assert result.exit_code == 0, result.output
    check_eligibility.assert_called_once()
    _, kwargs = check_eligibility.call_args
    assert kwargs.get("maintain") is False


def test_maintain_flag_passes_maintain_true_to_eligibility():
    """The core fix: ``--maintain`` on the resolver plumbs through to
    ``check_item_eligibility``. Without this, the launch script's
    maintain-mode dispatch (below) can't reach maintain-eligible items on
    de-opted institutions."""
    result, check_eligibility, _ = _invoke("--maintain", "abc")
    assert result.exit_code == 0, result.output
    check_eligibility.assert_called_once()
    _, kwargs = check_eligibility.call_args
    assert kwargs.get("maintain") is True


def test_ineligible_reason_surfaced_verbatim():
    """Emit the specific reason from ``check_item_eligibility`` — no
    conflation into a single "missing Wikidata ID or upload flag" bucket."""
    # Override the mock inside the with block via a fresh invocation.
    with (
        patch.object(
            resolve_dpla_ids, "post_es", return_value=_es_response(_make_source())
        ),
        patch.object(resolve_dpla_ids, "check_es_response", lambda _: None),
        patch.object(resolve_dpla_ids, "Banlist") as banlist_cls,
        patch.object(resolve_dpla_ids, "S3Client") as s3_cls,
        patch.object(resolve_dpla_ids, "resolve_slug", return_value="ia"),
        patch.object(
            resolve_dpla_ids,
            "check_item_eligibility",
            return_value=(
                False,
                "institution 'Internet Archive' has upload=False in"
                " institutions_v2.json (retry with maintain mode …)",
            ),
        ),
    ):
        banlist_cls.return_value.is_banned.return_value = False
        s3_cls.return_value.write_item_metadata.return_value = None
        result = CliRunner().invoke(resolve_dpla_ids.main, ["abc"])
    assert result.exit_code == 0, result.output
    # INELIGIBLE payload carries the specific reason string.
    assert "abc INELIGIBLE:" in result.output
    assert "upload=False" in result.output
    assert "retry with maintain mode" in result.output
    # Nothing staged when ineligible.
    s3_cls.return_value.write_item_metadata.assert_not_called()


def test_maintain_lets_upload_off_institution_through():
    """End-to-end: under ``--maintain``, an item whose institution has
    ``upload=False`` (but both Wikidata IDs present) resolves as
    ``HUB=<slug>`` instead of INELIGIBLE, and its metadata is staged to S3
    exactly like an eligible upload target. This is the scenario the launch
    script's maintain dispatch relies on."""
    result, _, s3_client = _invoke("--maintain", "abc")
    assert result.exit_code == 0, result.output
    assert "abc HUB=ia" in result.output
    s3_client.write_item_metadata.assert_called_once()
