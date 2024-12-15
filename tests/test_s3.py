from unittest.mock import patch, MagicMock

from botocore.exceptions import ClientError

from ingest_wikimedia.s3 import (
    get_s3,
    get_item_s3_path,
    get_media_s3_path,
    s3_file_exists,
    write_item_metadata,
    write_file_list,
    write_iiif_manifest,
    write_item_file,
    get_item_file,
    get_item_metadata,
    get_file_list,
)
from ingest_wikimedia.common import CHECKSUM


@patch("ingest_wikimedia.s3.boto3.resource")
def test_get_s3(mock_boto3_resource):
    mock_s3 = MagicMock()
    mock_boto3_resource.return_value = mock_s3

    s3 = get_s3()
    assert s3 == mock_s3
    assert mock_boto3_resource.called


def test_get_item_s3_path():
    path = get_item_s3_path("abcd1234", "file.txt", "partner")
    expected_path = "partner/images/a/b/c/d/abcd1234/file.txt"
    assert path == expected_path


def test_get_media_s3_path():
    path = get_media_s3_path("abcd1234", 1, "partner")
    expected_path = "partner/images/a/b/c/d/abcd1234/1_abcd1234"
    assert path == expected_path


@patch("ingest_wikimedia.s3.get_s3")
def test_s3_file_exists(mock_get_s3):
    mock_s3 = MagicMock()
    mock_get_s3.return_value = mock_s3
    mock_s3.Object.return_value.load.return_value = None

    assert s3_file_exists("path/to/file")
    mock_s3.Object.return_value.load.side_effect = ClientError(
        {"Error": {"Code": "404"}}, "load"
    )
    assert not s3_file_exists("path/to/file")


@patch("ingest_wikimedia.s3.write_item_file")
def test_write_item_metadata(mock_write_item_file):
    write_item_metadata("partner", "abcd1234", "metadata")
    mock_write_item_file.assert_called_once_with(
        "partner", "abcd1234", "metadata", "dpla-map.json", "text/plain"
    )


@patch("ingest_wikimedia.s3.get_item_file")
def test_read_item_metadata(mock_get_item_file):
    get_item_metadata("partner", "abcd1234")
    mock_get_item_file.assert_called_once_with("partner", "abcd1234", "dpla-map.json")


@patch("ingest_wikimedia.s3.write_item_file")
def test_write_file_list(mock_write_item_file):
    write_file_list("partner", "abcd1234", ["url1", "url2"])
    mock_write_item_file.assert_called_once_with(
        "partner", "abcd1234", "url1\nurl2", "file-list.txt", "text/plain"
    )


@patch("ingest_wikimedia.s3.get_item_file")
def test_read_file_list(mock_get_item_file):
    get_file_list("partner", "abcd1234")
    mock_get_item_file.assert_called_once_with("partner", "abcd1234", "file-list.txt")


@patch("ingest_wikimedia.s3.write_item_file")
def test_write_iiif_manifest(mock_write_item_file):
    write_iiif_manifest("partner", "abcd1234", "manifest")
    mock_write_item_file.assert_called_once_with(
        "partner", "abcd1234", "manifest", "iiif.json", "application/json"
    )


@patch("ingest_wikimedia.s3.get_s3")
@patch("ingest_wikimedia.s3.get_bytes_hash")
def test_write_item_file(mock_get_bytes_hash, mock_get_s3):
    mock_s3 = MagicMock()
    mock_get_s3.return_value = mock_s3
    mock_get_bytes_hash.return_value = "fakehash"

    write_item_file("partner", "abcd1234", "data", "file.txt", "text/plain")
    mock_s3.Object.return_value.put.assert_called_once_with(
        ContentType="text/plain", Metadata={CHECKSUM: "fakehash"}, Body="data"
    )


@patch("ingest_wikimedia.s3.get_s3")
def test_read_item_file(mock_get_s3):
    mock_s3 = MagicMock()
    mock_get_s3.return_value = mock_s3
    get_item_file("partner", "abcd1234", "file.txt")
    mock_s3.Object.return_value.get.assert_called_once()
