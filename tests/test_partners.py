"""Tests for ingest_wikimedia/partners.py — focused on the
upload-eligibility filtering in :func:`resolve_wikidata_id`.

The bug being pinned: ``Q131454`` (Library of Congress) appeared in
institutions_v2.json both as a hub (``lc``) and as a contributing
institution under several other hubs (Indiana, Digital Commonwealth,
HathiTrust, BHL, etc.). All of those entries were marked
``upload: false``. Pre-fix, ``resolve_wikidata_id`` returned every
match unconditionally, the launcher saw the hub-level ``(lc, None)``
pair, and the run died with ``Skipped targets: 'lc': not upload-eligible``.
The fix is to filter results by ``upload`` at resolution time so
non-eligible matches never leak to the launcher.
"""

from unittest.mock import patch

from ingest_wikimedia.partners import resolve_wikidata_id


def _mock_institutions(data: dict):
    """Patch ``_get_institutions`` to return ``data`` instead of fetching
    institutions_v2.json over the network. Returns the patch context
    manager for use in ``with`` blocks."""
    return patch("ingest_wikimedia.partners._get_institutions", return_value=data)


def test_resolve_wikidata_id_drops_hub_level_match_when_hub_upload_false():
    """A QID that matches a hub entry whose ``upload`` is ``False`` must
    NOT produce a ``(slug, None)`` result, even if other (institution-
    level) matches exist. Otherwise the launcher's "hub-level scope
    wins" tie-break routes onto a non-eligible hub.

    Mirrors the production Q131454 case: matches the LC hub itself
    (upload=false) AND LC-as-institution under several other hubs.
    """
    data = {
        "Library of Congress": {  # hub key matches the "lc" slug
            "Wikidata": "Q131454",
            "upload": False,
            "institutions": {},
        },
        "Indiana Memory": {
            "Wikidata": "Q83878471",
            "upload": True,
            "institutions": {
                "Library of Congress": {"Wikidata": "Q131454", "upload": False},
            },
        },
    }
    with _mock_institutions(data):
        results = resolve_wikidata_id("Q131454")
    # The Indiana-side match survives because its hub has upload=true
    # (even though the institution entry has upload=false). The LC-hub
    # match is dropped because the hub itself has upload=false.
    assert ("lc", None) not in results
    assert ("indiana", "Library of Congress") in results


def test_resolve_wikidata_id_drops_institution_match_when_neither_upload_true():
    """An institution-level QID match where BOTH the hub and the
    institution have ``upload: false`` must be dropped. Pins the
    eligibility rule against the OKHub / Oklahoma Historical Society
    case the user flagged."""
    data = {
        "OKHub": {
            "Wikidata": "Q123",
            "upload": False,
            "institutions": {
                "Oklahoma Historical Society": {
                    "Wikidata": "Q7082247",
                    "upload": False,
                },
            },
        },
        "The Portal to Texas History": {
            "Wikidata": "Q456",
            "upload": True,
            "institutions": {
                "Oklahoma Historical Society": {
                    "Wikidata": "Q7082247",
                    "upload": False,
                },
            },
        },
    }
    with _mock_institutions(data):
        results = resolve_wikidata_id("Q7082247")
    # OKHub hub.upload=false AND its OHS entry upload=false → dropped.
    assert ("oklahoma", "Oklahoma Historical Society") not in results
    # Texas hub.upload=true makes its OHS entry eligible even though
    # the institution-level upload flag is false (matches
    # is_item_upload_eligible's semantics).
    assert ("texas", "Oklahoma Historical Society") in results


def test_resolve_wikidata_id_keeps_hub_level_match_when_hub_upload_true():
    """Sanity: when the hub itself is upload-eligible, a hub-level QID
    match still yields ``(slug, None)``. Verifies the filter only
    drops the non-eligible cases."""
    data = {
        "National Archives and Records Administration": {
            "Wikidata": "Q518155",
            "upload": True,
            "institutions": {},
        },
    }
    with _mock_institutions(data):
        results = resolve_wikidata_id("Q518155")
    assert results == [("nara", None)]


def test_resolve_wikidata_id_keeps_inst_match_when_institution_upload_true():
    """Institution-level eligibility: hub.upload=false but
    institution.upload=true → keep. Pins the second leg of the
    ``hub OR institution`` rule."""
    data = {
        "Hub With Opt-In Subset": {
            "Wikidata": "Qhub",
            "upload": False,
            "institutions": {
                "Eligible Institution": {"Wikidata": "Qinst", "upload": True},
                "Ineligible Institution": {"Wikidata": "QinstN", "upload": False},
            },
        },
    }
    # Add a slug for the synthetic hub so _SLUG_BY_HUB_NAME resolves it.
    with (
        _mock_institutions(data),
        patch.dict(
            "ingest_wikimedia.partners._SLUG_BY_HUB_NAME",
            {"hub with opt-in subset": "subset-hub"},
        ),
    ):
        eligible = resolve_wikidata_id("Qinst")
        ineligible = resolve_wikidata_id("QinstN")
    assert eligible == [("subset-hub", "Eligible Institution")]
    assert ineligible == []


def test_resolve_wikidata_id_no_matches_returns_empty():
    """Unrecognised QID → empty list (unchanged from pre-fix)."""
    with _mock_institutions(
        {"Some Hub": {"Wikidata": "QX", "upload": True, "institutions": {}}}
    ):
        assert resolve_wikidata_id("Q-does-not-exist") == []
