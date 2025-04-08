from unittest.mock import patch, Mock

import pytest
from botocore.exceptions import ClientError

from ingest_wikimedia.common import CHECKSUM
from ingest_wikimedia.s3 import S3Client


@pytest.fixture
def s3_client():
    s3 = Mock()
    s3_client = S3Client()
    s3_client.s3 = s3
    return s3_client


@patch("ingest_wikimedia.s3.boto3.resource")
def test_get_s3(mock_boto3_resource):
    mock_s3 = Mock()
    mock_boto3_resource.return_value = mock_s3
    s3_client = S3Client()
    s3 = s3_client.get_s3()
    assert s3 == mock_s3
    assert mock_boto3_resource.called


def test_get_item_s3_path(s3_client: S3Client):
    path = s3_client.get_item_s3_path("abcd1234", "file.txt", "partner")
    expected_path = "partner/images/a/b/c/d/abcd1234/file.txt"
    assert path == expected_path


def test_get_media_s3_path(s3_client: S3Client):
    path = s3_client.get_media_s3_path("abcd1234", 1, "partner")
    expected_path = "partner/images/a/b/c/d/abcd1234/1_abcd1234"
    assert path == expected_path


def test_s3_file_exists(s3_client: S3Client):
    s3_client.s3.Object = Mock()
    assert s3_client.s3_file_exists("path/to/file")

    s3_client.s3.Object = Mock(
        side_effect=ClientError({"Error": {"Code": "404"}}, "load")
    )
    result = s3_client.s3_file_exists("path/to/file")
    assert not result


def test_write_item_metadata(s3_client: S3Client):
    s3_client.write_item_file = Mock()
    s3_client.write_item_metadata("partner", "abcd1234", "metadata")
    s3_client.write_item_file.assert_called_once_with(
        "partner", "abcd1234", "metadata", "dpla-map.json", "application/json"
    )


def test_read_item_metadata(s3_client: S3Client):
    s3_client.get_item_file = Mock()
    s3_client.get_item_metadata("partner", "abcd1234")
    s3_client.get_item_file.assert_called_once_with(
        "partner", "abcd1234", "dpla-map.json"
    )


def test_write_file_list(s3_client: S3Client):
    s3_client.write_item_file = Mock()
    s3_client.write_file_list("partner", "abcd1234", ["url1", "url2"])
    s3_client.write_item_file.assert_called_once_with(
        "partner", "abcd1234", "url1\nurl2", "file-list.txt", "text/plain"
    )


def test_read_file_list(s3_client: S3Client):
    s3_client.get_item_file = Mock()
    s3_client.get_file_list("partner", "abcd1234")
    s3_client.get_item_file.assert_called_once_with(
        "partner", "abcd1234", "file-list.txt"
    )


def test_write_iiif_manifest(s3_client: S3Client):
    s3_client.write_item_file = Mock()
    s3_client.write_iiif_manifest("partner", "abcd1234", "manifest")
    s3_client.write_item_file.assert_called_once_with(
        "partner", "abcd1234", "manifest", "iiif.json", "application/json"
    )


@patch("ingest_wikimedia.s3.LocalFS.get_bytes_hash")
def test_write_item_file(mock_get_bytes_hash, s3_client: S3Client):
    mock_get_bytes_hash.return_value = "fakehash"
    mock_s3 = Mock()
    s3_client.s3 = mock_s3
    obj = Mock()
    put = Mock()
    obj.put = put
    mock_s3.Object.return_value = obj
    s3_client.write_item_file("partner", "abcd1234", "data", "file.txt", "text/plain")
    put.assert_called_once_with(
        ContentType="text/plain", Metadata={CHECKSUM: "fakehash"}, Body="data"
    )


def test_read_item_file(s3_client: S3Client):
    mock_data = Mock()
    mock_data.read.return_value.decode.return_value = "data"
    mock_s3 = Mock()
    mock_s3.Object.return_value.get.return_value = {"Body": mock_data}
    s3_client.s3 = mock_s3

    result = s3_client.get_item_file("partner", "abcd1234", "file.txt")
    assert result == "data"
