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
    check_record_partner,
    maximize_iiif_url,
)


def test_check_partner():
    with pytest.raises(Exception, match="Unrecognized partner."):
        check_partner("invalid_partner")

    # Assuming "bpl" is a valid partner
    check_partner("bpl")


def test_check_record_partner_valid():
    partner = "bpl"
    item_metadata = {"provider": {"name": "Digital Commonwealth"}}
    assert check_record_partner(partner, item_metadata)


def test_check_record_partner_invalid():
    partner = "bpl"
    item_metadata = {"provider": {"name": "Some Other Provider"}}
    assert not check_record_partner(partner, item_metadata)


def test_check_record_partner_missing_provider():
    partner = "bpl"
    item_metadata = {}
    assert not check_record_partner(partner, item_metadata)


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
                    {
                        "images": [
                            {
                                "resource": {
                                    "service": {"@id": "http://server/iiif/identifier"}
                                }
                            }
                        ]
                    }
                ]
            }
        ]
    }
    result = iiif_v2_urls(iiif)
    assert result == ["http://server/iiif/identifier/full/max/0/default.jpg"]


def test_iiif_v3_urls():
    iiif = {
        "items": [
            {
                "items": [
                    {
                        "items": [
                            {
                                "body": {
                                    "id": "https://iiif.oregondigital.org/iiif/f0%2Fdf%2F72%2Fhj%2F15%2Ft-jp2.jp2/full/640,/0/default.jpg"
                                }
                            }
                        ]
                    }
                ]
            }
        ]
    }
    result = iiif_v3_urls(iiif)
    assert result == [
        "https://iiif.oregondigital.org/iiif/f0%2Fdf%2F72%2Fhj%2F15%2Ft-jp2.jp2/full/max/0/default.jpg"
    ]


def test_get_iiif_urls():
    iiif_v2 = {"@context": "http://iiif.io/api/presentation/2/context.json"}
    iiif_v3 = {"@context": "http://iiif.io/api/presentation/3/context.json"}
    iiif_v2_list = {
        "@context": ["foo", "http://iiif.io/api/presentation/2/context.json", "bar"]
    }
    iiif_v3_list = {
        "@context": ["baz", "http://iiif.io/api/presentation/3/context.json", "buz"]
    }
    iiif_not = {"@context": "https://realultimatepower.net/"}

    with patch("ingest_wikimedia.metadata.iiif_v2_urls", return_value=["v2_url"]):
        assert get_iiif_urls(iiif_v2) == ["v2_url"]
        assert get_iiif_urls(iiif_v2_list) == ["v2_url"]

    with patch("ingest_wikimedia.metadata.iiif_v3_urls", return_value=["v3_url"]):
        assert get_iiif_urls(iiif_v3) == ["v3_url"]
        assert get_iiif_urls(iiif_v3_list) == ["v3_url"]

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


def test_bpl_iiif_imageapi_url():
    url = "https://iiif.digitalcommonwealth.org/iiif/2/commonwealth:c534kh14z"
    expected_url = "https://iiif.digitalcommonwealth.org/iiif/2/commonwealth:c534kh14z/full/max/0/default.jpg"
    assert maximize_iiif_url(url) == expected_url


def test_colorado_imageapi_url():
    url = "https://cudl.colorado.edu/luna/servlet/iiif/UCBOULDERCB1~17~17~33595~102636"
    expected_url = "https://cudl.colorado.edu/luna/servlet/iiif/UCBOULDERCB1~17~17~33595~102636/full/max/0/default.jpg"
    assert maximize_iiif_url(url) == expected_url


def test_texas_imageapi_url():
    url = "https://texashistory.unt.edu/iiif/ark:/67531/metapth540971/m1/1"
    expected_url = "https://texashistory.unt.edu/iiif/ark:/67531/metapth540971/m1/1/full/max/0/default.jpg"
    assert maximize_iiif_url(url) == expected_url
