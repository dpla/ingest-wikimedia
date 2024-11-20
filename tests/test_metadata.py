import pytest

from unittest.mock import patch, MagicMock
from ingest_wikimedia.metadata import (
    check_partner,
    get_item_metadata,
    is_wiki_eligible,
    get_provider_and_data_provider,
    get_providers_data,
    provider_str,
    extract_urls,
    iiif_v2_urls,
    iiif_v3_urls,
    get_iiif_urls,
    get_iiif_manifest,
    contentdm_iiif_url,
)


def test_check_partner():
    with pytest.raises(Exception, match="Unrecognized partner."):
        check_partner("invalid_partner")

    # Assuming "bpl" is a valid partner
    check_partner("bpl")


@patch("ingest_wikimedia.metadata.get_http_session")
def test_get_item_metadata(mock_get_http_session):
    mock_response = MagicMock()
    mock_response.json.return_value = {"docs": [{"id": "test_id"}]}
    mock_get_http_session.return_value.get.return_value = mock_response

    result = get_item_metadata("test_id", "test_api_key")
    assert result == {"id": "test_id"}


def test_is_wiki_eligible():
    item_metadata = {
        "rightsCategory": "Unlimited Re-Use",
        "isShownAt": "http://example.com",
        "mediaMaster": ["http://example.com/media"],
    }
    provider = {"upload": True}
    data_provider = {"upload": True}

    assert is_wiki_eligible(item_metadata, provider, data_provider)


def test_get_provider_and_data_provider():
    item_metadata = {
        "provider": {"name": "test_provider"},
        "dataProvider": {"name": "test_data_provider"},
    }
    providers_json = {"test_provider": {"institutions": {"test_data_provider": {}}}}

    provider, data_provider = get_provider_and_data_provider(
        item_metadata, providers_json
    )
    assert provider == {"institutions": {"test_data_provider": {}}}
    assert data_provider == {}


@patch("ingest_wikimedia.metadata.get_http_session")
def test_get_providers_data(mock_get_http_session):
    mock_response = MagicMock()
    mock_response.json.return_value = {"provider": "data"}
    mock_get_http_session.return_value.get.return_value = mock_response

    result = get_providers_data()
    assert result == {"provider": "data"}


def test_provider_str():
    provider = {"Wikidata": "Q123", "upload": True}
    result = provider_str(provider)
    assert result == "Provider: Q123, True"


def test_extract_urls():
    item_metadata = {"mediaMaster": ["http://example.com/media"]}
    result = extract_urls("partner", "dpla_id", item_metadata)
    assert result == ["http://example.com/media"]


def test_iiif_v2_urls():
    iiif = {
        "sequences": [
            {
                "canvases": [
                    {"images": [{"resource": {"@id": "http://example.com/image"}}]}
                ]
            }
        ]
    }
    result = iiif_v2_urls(iiif)
    assert result == ["http://example.com/image"]


def test_iiif_v3_urls():
    iiif = {
        "items": [
            {"items": [{"items": [{"body": {"id": "http://example.com/image"}}]}]}
        ]
    }
    result = iiif_v3_urls(iiif)
    assert result == ["http://example.com/image/full/full/0/default.jpg"]


def test_get_iiif_urls():
    iiif_v2 = {"@context": "http://iiif.io/api/presentation/2/context.json"}
    iiif_v3 = {"@context": "http://iiif.io/api/presentation/3/context.json"}
    iiif_not = {"@context": "https://realultimatepower.net/"}

    with patch("ingest_wikimedia.metadata.iiif_v2_urls", return_value=["v2_url"]):
        assert get_iiif_urls(iiif_v2) == ["v2_url"]

    with patch("ingest_wikimedia.metadata.iiif_v3_urls", return_value=["v3_url"]):
        assert get_iiif_urls(iiif_v3) == ["v3_url"]

    with pytest.raises(Exception, match="Unimplemented IIIF version"):
        get_iiif_urls(iiif_not)


@patch("ingest_wikimedia.metadata.get_http_session")
def test_get_iiif_manifest(mock_get_http_session):
    mock_response = MagicMock()
    mock_response.json.return_value = {"manifest": "data"}
    mock_get_http_session.return_value.get.return_value = mock_response

    result = get_iiif_manifest("http://example.com/manifest")
    assert result == {"manifest": "data"}


def test_contentdm_iiif_url():
    is_shown_at = "http://www.ohiomemory.org/cdm/ref/collection/p16007coll33/id/126923"
    expected_url = (
        "http://www.ohiomemory.org/iiif/info/p16007coll33/126923/manifest.json"
    )
    assert contentdm_iiif_url(is_shown_at) == expected_url
