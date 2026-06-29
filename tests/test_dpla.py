from pathlib import Path

import pytest

from unittest.mock import patch, MagicMock

from requests import Session

from ingest_wikimedia.banlist import Banlist, BANLIST_FILE_NAME
from ingest_wikimedia.dpla import (
    EDM_IS_SHOWN_AT,
    MEDIA_MASTER_FIELD_NAME,
    RIGHTS_CATEGORY_FIELD_NAME,
    DPLA,
)
from ingest_wikimedia.iiif import IIIF
from ingest_wikimedia.s3 import S3Client
from ingest_wikimedia.tracker import Tracker


@pytest.fixture
@patch("ingest_wikimedia.tracker.Tracker")
@patch("requests.Session")
@patch("ingest_wikimedia.s3.S3Client")
@patch("ingest_wikimedia.banlist.Banlist")
@patch("ingest_wikimedia.iiif.IIIF")
def dpla(
    tracker: Tracker,
    http_session: Session,
    s3_client: S3Client,
    banlist: Banlist,
    iiif: IIIF,
) -> DPLA:
    banlist.is_banned = lambda dpla_id: False
    iiif.contentdm_iiif_url = MagicMock()
    iiif.contentdm_iiif_url.return_value = "http://example.com/iiif"
    dpla = DPLA("test_api_key", tracker, http_session, s3_client, banlist, iiif)
    dpla.http_session = MagicMock()
    dpla.http_session.head.return_value.status_code = 200
    return dpla


@pytest.fixture
def good_item_metadata():
    return {
        RIGHTS_CATEGORY_FIELD_NAME: "Unlimited Re-Use",
        EDM_IS_SHOWN_AT: "https://example.com",
        MEDIA_MASTER_FIELD_NAME: ["https://example.com/media"],
    }


@pytest.fixture
def good_provider():
    return {"upload": True, "Wikidata": "abcd"}


@pytest.fixture
def good_data_provider():
    return {"upload": True, "Wikidata": "efgh"}


@pytest.fixture
def good_dpla_id():
    return "12345"


def test_check_partner_rejects_unknown_slug(dpla: DPLA):
    """A slug not in PARTNER_HUBS is rejected before any network call."""
    with pytest.raises(ValueError, match="Unrecognized partner"):
        dpla.check_partner("invalid_partner")


def test_check_partner_accepts_upload_eligible_hub(dpla: DPLA):
    """A recognised hub that's marked upload-eligible in institutions_v2.json
    passes the check. Mocked to avoid hitting GitHub from the test suite."""
    with patch("ingest_wikimedia.dpla.is_upload_eligible", return_value=True):
        dpla.check_partner("bpl")


def test_check_partner_rejects_hub_with_no_upload_eligible_institutions(
    dpla: DPLA,
):
    """A recognised hub slug whose institutions_v2.json entry has nothing
    upload-eligible is rejected with an actionable message pointing at the
    JSON file — making it obvious that the fix is a data edit, not a code
    change."""
    with patch("ingest_wikimedia.dpla.is_upload_eligible", return_value=False):
        with pytest.raises(ValueError, match="institutions_v2.json"):
            dpla.check_partner("bpl")


def test_check_partner_maintain_mode_accepts_de_opted_hub(dpla: DPLA):
    """Maintain mode reconciles files ALREADY on Commons for de-opted
    institutions, so a hub with zero ``upload: True`` institutions must
    still pass the precheck when ``maintain=True``. This is the
    digitalnc-class case from the user's Q5312898 Duke Libraries run:
    the launcher correctly resolved the QID to three hubs (one of which,
    NCDH, has no opted-in institutions), and the maintain pipeline
    needs to process each — but pre-fix, ``DPLA.check_partner`` raised
    ``ValueError`` → ``click.BadParameter`` (exit 2) at every CLI
    entry point before maintain mode could rescue.

    The partner-level twin of the bug PR #342 fixed for
    :func:`resolve_wikidata_id`.
    """
    with patch("ingest_wikimedia.dpla.is_upload_eligible", return_value=False):
        # No exception when maintain=True, even though the hub fails
        # the eligibility check.
        dpla.check_partner("bpl", maintain=True)
    # And the eligibility check is still skipped — not even called.
    with patch("ingest_wikimedia.dpla.is_upload_eligible") as mock_eligible:
        dpla.check_partner("bpl", maintain=True)
        mock_eligible.assert_not_called()


def test_check_partner_maintain_mode_still_rejects_unknown_slug(dpla: DPLA):
    """Maintain mode drops ONLY the upload-eligibility check. A slug
    not in ``PARTNER_HUBS`` is still a real bug (typo / unsupported
    hub) and must fail loudly even in maintain mode — otherwise the
    downstream tools would dereference ``PARTNER_HUBS[partner]`` and
    KeyError obscurely."""
    with pytest.raises(ValueError, match="Unrecognized partner"):
        dpla.check_partner("nope-not-a-real-slug", maintain=True)


def test_check_record_partner_valid(dpla: DPLA):
    partner = "bpl"
    item_metadata = {"provider": {"name": "Digital Commonwealth"}}
    assert dpla.check_record_partner(partner, item_metadata)


def test_check_record_partner_invalid(dpla: DPLA):
    partner = "bpl"
    item_metadata = {"provider": {"name": "Some Other Provider"}}
    assert not dpla.check_record_partner(partner, item_metadata)


def test_check_record_partner_missing_provider(dpla: DPLA):
    partner = "bpl"
    item_metadata = {}
    assert not dpla.check_record_partner(partner, item_metadata)


def test_get_item_metadata(dpla: DPLA):
    mock_response = MagicMock()
    mock_response.json.return_value = {"docs": [{"id": "test_id"}]}

    mock_http_session = MagicMock()
    mock_http_session.get.return_value = mock_response
    dpla.http_session = mock_http_session

    result = dpla.get_item_metadata("test_id")
    assert result == {"id": "test_id"}


def test_is_wiki_eligible_yes(
    dpla, good_dpla_id, good_item_metadata, good_provider, good_data_provider
):
    assert dpla.is_wiki_eligible(
        good_dpla_id, good_item_metadata, good_provider, good_data_provider
    )


def test_not_wiki_eligible_provider_wikidata(
    dpla, good_dpla_id, good_item_metadata, good_provider, good_data_provider
):
    del good_provider["Wikidata"]
    assert not dpla.is_wiki_eligible(
        good_dpla_id, good_item_metadata, good_provider, good_data_provider
    )


def test_not_wiki_eligible_dataprovider_wikidata(
    dpla, good_dpla_id, good_item_metadata, good_provider, good_data_provider
):
    del good_data_provider["Wikidata"]
    assert not dpla.is_wiki_eligible(
        good_dpla_id, good_item_metadata, good_provider, good_data_provider
    )


def test_not_wiki_eligible_dpla_id(
    dpla, good_dpla_id, good_item_metadata, good_provider, good_data_provider
):
    dpla.banlist = Banlist()
    banlist_path = Path(__file__).parent.parent / BANLIST_FILE_NAME
    with open(banlist_path, "r") as file:
        banlist = [line.rstrip() for line in file]

    assert not dpla.is_wiki_eligible(
        banlist[0], good_item_metadata, good_provider, good_data_provider
    )


def test_not_wiki_eligible_rights_category(
    dpla, good_dpla_id, good_item_metadata, good_provider, good_data_provider
):
    good_item_metadata["rightsCategory"] = "On fire"
    assert not dpla.is_wiki_eligible(
        good_dpla_id, good_item_metadata, good_provider, good_data_provider
    )


def test_not_wiki_eligible_providers(
    dpla, good_dpla_id, good_item_metadata, good_provider, good_data_provider
):
    good_provider["upload"] = False
    good_data_provider["upload"] = False
    assert not dpla.is_wiki_eligible(
        good_dpla_id, good_item_metadata, good_provider, good_data_provider
    )


def test_not_wiki_eligible_media(
    dpla, good_dpla_id, good_item_metadata, good_provider, good_data_provider
):
    del good_item_metadata[MEDIA_MASTER_FIELD_NAME]
    del good_item_metadata[EDM_IS_SHOWN_AT]
    dpla.iiif.contentdm_iiif_url = MagicMock()
    dpla.iiif.contentdm_iiif_url.return_value = None
    eligible = dpla.is_wiki_eligible(
        good_dpla_id, good_item_metadata, good_provider, good_data_provider
    )
    assert not eligible


def test_get_provider_and_data_provider(dpla):
    item_metadata = {
        "provider": {"name": "test_provider"},
        "dataProvider": {"name": "test_data_provider"},
    }
    providers_json = {"test_provider": {"institutions": {"test_data_provider": {}}}}

    provider, data_provider = dpla.get_provider_and_data_provider(
        item_metadata, providers_json
    )
    assert provider == {"institutions": {"test_data_provider": {}}}
    assert data_provider == {}


def test_get_providers_data(dpla):
    mock_response = MagicMock()
    mock_response.json.return_value = {"provider": "data"}

    mock_http_session = MagicMock()
    mock_http_session.get.return_value = mock_response

    dpla.http_session = mock_http_session

    result = dpla.get_providers_data()
    assert result == {"provider": "data"}


def test_provider_str():
    provider = {"Wikidata": "Q123", "upload": True}
    result = DPLA.provider_str(provider)
    assert result == "Provider: Q123, True"


def test_extract_urls(dpla):
    item_metadata = {"mediaMaster": ["http://example.com/media"]}
    result = dpla.extract_urls("partner", "dpla_id", item_metadata)
    assert result == ["http://example.com/media"]
