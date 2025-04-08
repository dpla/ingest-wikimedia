import pytest

from unittest.mock import patch, MagicMock

from requests import Session

from ingest_wikimedia.iiif import IIIF, IIIF_V3_FULL_RES_JPG_SUFFIX
from ingest_wikimedia.tracker import Tracker


@pytest.fixture
@patch("ingest_wikimedia.tracker.Tracker")
@patch("requests.Session")
def iiif(tracker: Tracker, http_session: Session) -> IIIF:
    return IIIF(tracker, http_session)


def test_iiif_v2_urls(iiif: IIIF):
    iiif_record = {
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
    result = iiif.iiif_v2_urls(iiif_record)
    assert result == ["http://server/iiif/identifier/full/max/0/default.jpg"]


def test_iiif_v2_multiple_sequences(iiif: IIIF):
    iiif_record = {
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
            },
            {},
        ]
    }
    result = iiif.iiif_v2_urls(iiif_record)
    assert result == ["http://server/iiif/identifier/full/max/0/default.jpg"]


def test_iiif_v3_urls(iiif: IIIF):
    iiif_record = {
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
    result = iiif.iiif_v3_urls(iiif_record)
    assert result == [
        "https://iiif.oregondigital.org/iiif/f0%2Fdf%2F72%2Fhj%2F15%2Ft-jp2.jp2/full/max/0/default.jpg"
    ]


def test_get_iiif_urls(iiif: IIIF):
    iiif_v2 = {"@context": "http://iiif.io/api/presentation/2/context.json"}
    iiif_v3 = {"@context": "http://iiif.io/api/presentation/3/context.json"}
    iiif_v2_list = {
        "@context": ["foo", "http://iiif.io/api/presentation/2/context.json", "bar"]
    }
    iiif_v3_list = {
        "@context": ["baz", "http://iiif.io/api/presentation/3/context.json", "buz"]
    }
    iiif_not = {"@context": "https://realultimatepower.net/"}

    iiif.iiif_v2_urls = MagicMock()
    iiif.iiif_v2_urls.return_value = ["v2_url"]
    iiif.iiif_v3_urls = MagicMock()
    iiif.iiif_v3_urls.return_value = ["v3_url"]

    assert iiif.get_iiif_urls(iiif_v2) == ["v2_url"]
    assert iiif.get_iiif_urls(iiif_v2_list) == ["v2_url"]

    assert iiif.get_iiif_urls(iiif_v3) == ["v3_url"]
    assert iiif.get_iiif_urls(iiif_v3_list) == ["v3_url"]

    with pytest.raises(Exception, match="Unimplemented IIIF version"):
        iiif.get_iiif_urls(iiif_not)


def test_get_iiif_manifest(iiif: IIIF):
    mock_response = MagicMock()
    mock_response.json.return_value = {"manifest": "data"}
    iiif.http_session = MagicMock()
    iiif.http_session.get.return_value = mock_response
    result = iiif.get_iiif_manifest("http://example.com/manifest")
    assert result == {"manifest": "data"}


def test_contentdm_iiif_url(iiif: IIIF):
    is_shown_at = "http://www.ohiomemory.org/cdm/ref/collection/p16007coll33/id/126923"
    expected_url = (
        "http://www.ohiomemory.org/iiif/info/p16007coll33/126923/manifest.json"
    )
    assert iiif.contentdm_iiif_url(is_shown_at) == expected_url


def test_bpl_iiif_imageapi_url(iiif: IIIF):
    url = "https://iiif.digitalcommonwealth.org/iiif/2/commonwealth:c534kh14z"
    expected_url = "https://iiif.digitalcommonwealth.org/iiif/2/commonwealth:c534kh14z/full/max/0/default.jpg"
    assert iiif.maximize_iiif_url(url, IIIF_V3_FULL_RES_JPG_SUFFIX) == expected_url


def test_colorado_imageapi_url(iiif: IIIF):
    url = "https://cudl.colorado.edu/luna/servlet/iiif/UCBOULDERCB1~17~17~33595~102636"
    expected_url = "https://cudl.colorado.edu/luna/servlet/iiif/UCBOULDERCB1~17~17~33595~102636/full/max/0/default.jpg"
    assert iiif.maximize_iiif_url(url, IIIF_V3_FULL_RES_JPG_SUFFIX) == expected_url


def test_texas_imageapi_url(iiif: IIIF):
    url = "https://texashistory.unt.edu/iiif/ark:/67531/metapth540971/m1/1"
    expected_url = "https://texashistory.unt.edu/iiif/ark:/67531/metapth540971/m1/1/full/max/0/default.jpg"
    assert iiif.maximize_iiif_url(url, IIIF_V3_FULL_RES_JPG_SUFFIX) == expected_url
