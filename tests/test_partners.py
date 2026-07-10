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
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from ingest_wikimedia import partners
from ingest_wikimedia.partners import (
    check_item_eligibility,
    commons_has_files_for_qid,
    is_item_upload_eligible,
    resolve_commons_category,
    resolve_wikidata_id,
    wikidata_qid_for_target,
)


def _mock_institutions(data: dict):
    """Patch ``load_institutions`` to return ``data`` instead of fetching
    institutions_v2.json over the network. Returns the patch context
    manager for use in ``with`` blocks."""
    return patch("ingest_wikimedia.partners.load_institutions", return_value=data)


def _json_urlopen(data: dict):
    """urlopen replacement: a context manager whose read() yields ``data`` as JSON bytes."""
    resp = MagicMock()
    resp.read.return_value = json.dumps(data).encode()
    cm = MagicMock()
    cm.__enter__.return_value = resp
    cm.__exit__.return_value = False
    return cm


def _http_429(retry_after=None):
    hdrs = {"Retry-After": retry_after} if retry_after is not None else None
    return urllib.error.HTTPError("https://x", 429, "Too Many Requests", hdrs, None)


def _queue_urlopen(queue):
    """urlopen side-effect that pops ``queue`` in order, raising Exception items
    (to simulate 429s) and returning the rest — for multi-attempt retry tests."""

    def _urlopen(*args, **kwargs):
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    return _urlopen


@pytest.fixture(autouse=True)
def _isolate_staged_config(monkeypatch):
    """Reset the staged-config cache and clear the WIKIMEDIA_*_FILE env vars
    around every test so cached data or a stray env var can't leak between
    tests."""
    monkeypatch.delenv(partners.INSTITUTIONS_FILE_ENV, raising=False)
    monkeypatch.delenv(partners.SUBJECTS_FILE_ENV, raising=False)
    partners._staged_config_cache.clear()
    yield
    partners._staged_config_cache.clear()


def test_load_institutions_prefers_local_file(tmp_path, monkeypatch):
    data = {"Hub": {"upload": True}}
    f = tmp_path / "institutions_v2.json"
    f.write_text(json.dumps(data))
    monkeypatch.setenv(partners.INSTITUTIONS_FILE_ENV, str(f))
    with patch.object(partners.urllib.request, "urlopen") as urlopen:
        assert partners.load_institutions() == data
        urlopen.assert_not_called()  # local copy → no network fetch


def test_load_institutions_fetches_when_no_local_file():
    data = {"Hub": {"upload": True}}
    with patch.object(
        partners.urllib.request, "urlopen", return_value=_json_urlopen(data)
    ):
        assert partners.load_institutions() == data


def test_load_institutions_empty_local_file_falls_back_to_fetch(tmp_path, monkeypatch):
    fetched = {"Hub": {"upload": True}}
    f = tmp_path / "institutions_v2.json"
    f.write_text("{}")  # valid JSON but empty → treated as unusable, fetch instead
    monkeypatch.setenv(partners.INSTITUTIONS_FILE_ENV, str(f))
    with patch.object(
        partners.urllib.request, "urlopen", return_value=_json_urlopen(fetched)
    ) as urlopen:
        assert partners.load_institutions() == fetched
        urlopen.assert_called_once()


def test_load_institutions_retries_on_429_then_succeeds():
    data = {"Hub": {"upload": True}}
    queue = [_http_429(), _http_429(), _json_urlopen(data)]
    with (
        patch.object(partners.time, "sleep") as sleep,
        patch.object(
            partners.urllib.request, "urlopen", side_effect=_queue_urlopen(queue)
        ),
    ):
        assert partners.load_institutions() == data
        assert sleep.call_count == 2  # two 429s → two backoff sleeps


def test_load_institutions_honors_retry_after_header():
    data = {"Hub": {"upload": True}}
    queue = [_http_429(retry_after="1"), _json_urlopen(data)]
    with (
        patch.object(partners.time, "sleep") as sleep,
        patch.object(
            partners.urllib.request, "urlopen", side_effect=_queue_urlopen(queue)
        ),
    ):
        assert partners.load_institutions() == data
        sleep.assert_called_once_with(1.0)  # honors Retry-After, not default backoff


def test_load_institutions_raises_after_exhausting_429_retries():
    with (
        patch.object(partners.time, "sleep"),
        patch.object(partners.urllib.request, "urlopen", side_effect=_http_429()),
        pytest.raises(urllib.error.HTTPError),
    ):
        partners.load_institutions()


def test_load_subjects_prefers_local_file(tmp_path, monkeypatch):
    """subjects.json rides the same local-first loader: a staged file is read
    from disk with no network fetch (the whole point of WIKIMEDIA_SUBJECTS_FILE)."""
    data = {"Photographs": "Q125191"}
    f = tmp_path / "subjects.json"
    f.write_text(json.dumps(data))
    monkeypatch.setenv(partners.SUBJECTS_FILE_ENV, str(f))
    with patch.object(partners.urllib.request, "urlopen") as urlopen:
        assert partners.load_subjects() == data
        urlopen.assert_not_called()


def test_load_subjects_fetches_when_no_local_file():
    data = {"Photographs": "Q125191"}
    with patch.object(
        partners.urllib.request, "urlopen", return_value=_json_urlopen(data)
    ):
        assert partners.load_subjects() == data


def test_load_institutions_and_subjects_cached_independently(tmp_path, monkeypatch):
    """The shared staged-config cache is keyed by URL, so institutions and
    subjects staged to different files must not collide."""
    insts = {"Hub": {"upload": True}}
    subs = {"Photographs": "Q125191"}
    fi = tmp_path / "institutions_v2.json"
    fi.write_text(json.dumps(insts))
    fs = tmp_path / "subjects.json"
    fs.write_text(json.dumps(subs))
    monkeypatch.setenv(partners.INSTITUTIONS_FILE_ENV, str(fi))
    monkeypatch.setenv(partners.SUBJECTS_FILE_ENV, str(fs))
    with patch.object(partners.urllib.request, "urlopen") as urlopen:
        assert partners.load_institutions() == insts
        assert partners.load_subjects() == subs
        urlopen.assert_not_called()


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


# --- check_item_eligibility --------------------------------------------------
#
# Splits the historical conflated "missing Wikidata ID or upload flag"
# response into four distinct reasons. Pins each branch, both eligibility
# profiles (upload / maintain), and the legacy is_item_upload_eligible
# boolean wrapper.

_ELIG_HUB_DATA_UPLOAD_ON = {
    "Digital Library of Georgia": {
        "Wikidata": "Qhub",
        "upload": True,
        "institutions": {
            "Some University": {"Wikidata": "Qinst", "upload": False},
        },
    },
}

_ELIG_HUB_DATA_UPLOAD_OFF = {
    "Internet Archive": {
        "Wikidata": "Q461",
        "upload": False,
        "institutions": {
            "Internet Archive": {"Wikidata": "Q461", "upload": False},
        },
    },
}


def test_check_item_eligibility_unknown_hub_reason():
    """A canonical slug missing from PARTNER_HUBS gets a distinct 'unknown
    hub' reason, not the same conflated message as the flag/ID branches."""
    with _mock_institutions({}):
        ok, reason = check_item_eligibility("not-a-hub", "Anywhere")
    assert ok is False
    assert "unknown hub" in reason
    assert "'not-a-hub'" in reason


def test_check_item_eligibility_hub_missing_wikidata_reason():
    """Hub-level Wikidata ID missing → specific reason mentioning the hub
    display name and the Commons-category-resolution consequence."""
    data = {
        "Digital Library of Georgia": {
            "Wikidata": "",
            "upload": True,
            "institutions": {
                "Some University": {"Wikidata": "Qinst", "upload": True},
            },
        },
    }
    with _mock_institutions(data):
        ok, reason = check_item_eligibility("georgia", "Some University")
    assert ok is False
    assert "hub" in reason and "Wikidata" in reason
    assert "Digital Library of Georgia" in reason


def test_check_item_eligibility_institution_missing_wikidata_reason():
    """Institution-level Wikidata ID missing → distinct reason from the
    upload-flag case, so an operator can tell why maintain wouldn't
    unblock this item (Commons category can't be resolved either way)."""
    data = {
        "Digital Library of Georgia": {
            "Wikidata": "Qhub",
            "upload": True,
            "institutions": {
                "Some University": {"Wikidata": "", "upload": True},
            },
        },
    }
    with _mock_institutions(data):
        ok, reason = check_item_eligibility("georgia", "Some University")
    assert ok is False
    assert "institution" in reason and "Wikidata" in reason
    assert "'Some University'" in reason


def test_check_item_eligibility_upload_off_reason_suggests_maintain():
    """Both QIDs present but upload=False on hub AND institution → the
    reported Internet Archive case. Reason must name the upload-flag
    failure specifically and steer the operator toward maintain mode
    (the exact scenario maintain is designed for)."""
    with _mock_institutions(_ELIG_HUB_DATA_UPLOAD_OFF):
        ok, reason = check_item_eligibility("ia", "Internet Archive")
    assert ok is False
    assert "upload=False" in reason
    assert "maintain" in reason.lower()


def test_check_item_eligibility_upload_true_passes():
    """The happy path — hub.upload=True → eligible."""
    with _mock_institutions(_ELIG_HUB_DATA_UPLOAD_ON):
        ok, reason = check_item_eligibility("georgia", "Some University")
    assert ok is True
    assert reason == ""


def test_check_item_eligibility_maintain_bypasses_upload_flag():
    """The core bug fix: an institution with QIDs present but upload=False
    is eligible under maintain=True. Same Internet Archive case as the
    upload branch above; only the flag flips the result."""
    with _mock_institutions(_ELIG_HUB_DATA_UPLOAD_OFF):
        ok, reason = check_item_eligibility("ia", "Internet Archive", maintain=True)
    assert ok is True
    assert reason == ""


def test_check_item_eligibility_maintain_still_requires_wikidata_ids():
    """Maintain mode relaxes the upload-flag gate only; missing Wikidata
    IDs still block (Commons categories can't be resolved without them)."""
    data = {
        "Digital Library of Georgia": {
            "Wikidata": "",
            "upload": False,
            "institutions": {
                "Some University": {"Wikidata": "Qinst", "upload": False},
            },
        },
    }
    with _mock_institutions(data):
        ok, reason = check_item_eligibility("georgia", "Some University", maintain=True)
    assert ok is False
    assert "Wikidata" in reason


def test_is_item_upload_eligible_wraps_check_item_eligibility():
    """The legacy boolean wrapper stays False under upload=False even
    though the maintain-mode call in the previous test returned True —
    proving the wrapper defaults maintain=False."""
    with _mock_institutions(_ELIG_HUB_DATA_UPLOAD_OFF):
        assert is_item_upload_eligible("ia", "Internet Archive") is False
    with _mock_institutions(_ELIG_HUB_DATA_UPLOAD_ON):
        assert is_item_upload_eligible("georgia", "Some University") is True
