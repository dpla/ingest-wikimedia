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

import json
from unittest.mock import MagicMock, patch

from ingest_wikimedia import partners
from ingest_wikimedia.partners import (
    commons_has_files_for_qid,
    resolve_commons_category,
    resolve_wikidata_id,
    wikidata_qid_for_target,
)


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


def test_resolve_wikidata_id_same_hub_overlap_picks_up_eligible_institution():
    """Edge case: a QID matches both the hub-level entry AND an
    institution within the same hub. If the hub itself is not
    upload-eligible but the institution is, the institution-level
    match must still be returned. The first version of this fix
    used a ``continue`` on the hub-level match, which skipped the
    institution scan within that hub and dropped this case silently
    (caught by CodeRabbit on PR #326)."""
    data = {
        "Hub Opted Out With One Opted In": {
            "Wikidata": "QSAME",
            "upload": False,
            "institutions": {
                # Same QID as the hub, but opted in at the institution level.
                "Self-listed Institution": {"Wikidata": "QSAME", "upload": True},
            },
        },
    }
    with (
        _mock_institutions(data),
        patch.dict(
            "ingest_wikimedia.partners._SLUG_BY_HUB_NAME",
            {"hub opted out with one opted in": "overlap-hub"},
        ),
    ):
        results = resolve_wikidata_id("QSAME")
    # Hub-level match dropped (hub.upload=false).
    assert ("overlap-hub", None) not in results
    # Institution-level match preserved (institution.upload=true).
    assert results == [("overlap-hub", "Self-listed Institution")]


def test_resolve_wikidata_id_maintain_keeps_de_opted_matches_across_hubs():
    """In maintain mode the upload-eligibility filter is dropped (QID-only
    gate): a de-opted (upload=false) institution still resolves, because
    maintain reconciles files ALREADY on Commons — exactly when it applies. A
    QID present under multiple hubs resolves to one match per hub. Mirrors the
    reported Duke (Q5312898) case, which the default filter wrongly returned
    empty for (→ misleading "not found in institutions_v2.json")."""
    data = {
        "Digital Library of Georgia": {
            "Wikidata": "Qhub-ga",
            "upload": False,
            "institutions": {
                "Duke University. Library": {"Wikidata": "Q5312898", "upload": False},
            },
        },
        "Internet Archive": {
            "Wikidata": "Qhub-ia",
            "upload": False,
            "institutions": {
                "Duke University Libraries": {"Wikidata": "Q5312898", "upload": False},
            },
        },
        "North Carolina Digital Heritage Center": {
            "Wikidata": "Qhub-nc",
            "upload": False,
            "institutions": {
                "Duke University Libraries": {"Wikidata": "Q5312898", "upload": False},
            },
        },
    }
    # Pin the slug map locally so the test exercises only the maintain-mode
    # filter, not the real registry (a slug rename shouldn't break it).
    with (
        _mock_institutions(data),
        patch.dict(
            "ingest_wikimedia.partners._SLUG_BY_HUB_NAME",
            {
                "digital library of georgia": "georgia",
                "internet archive": "ia",
                "north carolina digital heritage center": "digitalnc",
            },
        ),
    ):
        default = resolve_wikidata_id("Q5312898")
        maintained = resolve_wikidata_id("Q5312898", maintain=True)
    # The default (upload-gated) path drops all three — the reported bug.
    assert default == []
    # Maintain keeps every match, one per hub (launcher groups → one session each).
    assert set(maintained) == {
        ("georgia", "Duke University. Library"),
        ("ia", "Duke University Libraries"),
        ("digitalnc", "Duke University Libraries"),
    }


# --- maintain mode: slug/target -> QID -> exact Commons category -------------


def test_wikidata_qid_for_target_hub_and_institution():
    # Keys are hub DISPLAY names (as in institutions_v2.json); the slug maps
    # to the display name via PARTNER_HUBS ("georgia" -> "Digital Library of
    # Georgia").
    data = {
        "Digital Library of Georgia": {
            "Wikidata": "Q5275908",
            "institutions": {"Some Library": {"Wikidata": "Q42"}},
        }
    }
    with _mock_institutions(data):
        assert wikidata_qid_for_target("georgia") == "Q5275908"
        assert wikidata_qid_for_target("georgia", "Some Library") == "Q42"
        assert wikidata_qid_for_target("georgia", "Unknown Inst") is None
        assert wikidata_qid_for_target("not-a-slug") is None


def test_resolve_commons_category_follows_p8464_to_sitelink():
    # Hub item Q5275908 has P8464 -> category item Q999, whose commonswiki
    # sitelink is the real Category page (authoritative; note the "the").
    entities = {
        "Q5275908": {
            "claims": {
                "P8464": [{"mainsnak": {"datavalue": {"value": {"id": "Q999"}}}}]
            }
        },
        "Q999": {
            "sitelinks": {
                "commonswiki": {
                    "title": "Category:Media contributed by the Digital Library of Georgia"
                }
            }
        },
    }
    with (
        patch.object(partners, "_commons_category_cache", {}),
        patch.object(
            partners, "_fetch_wikidata_entity", side_effect=lambda q, t: entities.get(q)
        ),
    ):
        cat = resolve_commons_category("Q5275908")
    assert cat == "Category:Media contributed by the Digital Library of Georgia"


def test_resolve_commons_category_none_when_no_p8464():
    with (
        patch.object(partners, "_commons_category_cache", {}),
        patch.object(partners, "_fetch_wikidata_entity", return_value={"claims": {}}),
    ):
        assert resolve_commons_category("Q123") is None


def test_resolve_commons_category_rejects_non_qid():
    # No network call for a non-QID input.
    with patch.object(partners, "_fetch_wikidata_entity") as fetch:
        assert resolve_commons_category("georgia") is None
        fetch.assert_not_called()


def _fake_commons_search_response(totalhits: int) -> bytes:
    """Shape a Cirrus list=search response so urlopen().read() can hand it
    straight to ``commons_has_files_for_qid``."""
    return json.dumps(
        {"query": {"searchinfo": {"totalhits": totalhits}, "search": []}}
    ).encode()


def test_commons_has_files_for_qid_true_when_p195_hits():
    """One or more files carry P195=<qid> in SDC: maintain has work to do."""
    fake_resp = MagicMock()
    fake_resp.__enter__.return_value.read.return_value = _fake_commons_search_response(
        42
    )
    with (
        patch.object(partners, "_commons_files_for_qid_cache", {}),
        patch.object(partners.urllib.request, "urlopen", return_value=fake_resp),
    ):
        assert commons_has_files_for_qid("Q12345") is True


def test_commons_has_files_for_qid_false_when_no_hits():
    """Zero P195=<qid> hits: case-2 — the launcher should skip the
    target's SDC step gracefully (see
    ``test_maintain_no_category_no_files_skips_target_gracefully``)."""
    fake_resp = MagicMock()
    fake_resp.__enter__.return_value.read.return_value = _fake_commons_search_response(
        0
    )
    with (
        patch.object(partners, "_commons_files_for_qid_cache", {}),
        patch.object(partners.urllib.request, "urlopen", return_value=fake_resp),
    ):
        assert commons_has_files_for_qid("Q12345") is False


def test_commons_has_files_for_qid_fails_open_on_network_error():
    """A transient Commons API outage must NOT silently downgrade a real
    case-1 (files exist) target to case-2 (skipped). Return True on any
    network/parse error so the launcher errs on the side of running the
    SDC step rather than dropping a target."""
    import urllib.error as _ue

    with (
        patch.object(partners, "_commons_files_for_qid_cache", {}),
        patch.object(
            partners.urllib.request,
            "urlopen",
            side_effect=_ue.URLError("boom"),
        ),
    ):
        assert commons_has_files_for_qid("Q12345") is True


def test_commons_has_files_for_qid_rejects_non_qid_without_network():
    """No QID → no network call; just return False."""
    with patch.object(partners.urllib.request, "urlopen") as urlopen:
        assert commons_has_files_for_qid("not-a-qid") is False
        urlopen.assert_not_called()
