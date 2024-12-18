import os
import hashlib
from unittest.mock import patch
import pytest
from ingest_wikimedia.local import (
    setup_temp_dir,
    cleanup_temp_dir,
    get_temp_file,
    clean_up_tmp_file,
    get_file_hash,
    get_bytes_hash,
    get_content_type,
)


@pytest.fixture(autouse=True)
def setup_and_teardown_temp_dir():
    setup_temp_dir()
    yield
    cleanup_temp_dir()


def test_get_and_cleanup_temp_file():
    temp_file = get_temp_file()
    assert os.path.exists(temp_file.name)
    temp_file.close()
    clean_up_tmp_file(temp_file)
    assert not os.path.exists(temp_file.name)


def test_get_file_hash(tmp_path):
    test_file = tmp_path / "test_file.txt"
    test_file.write_text("test content")
    expected_hash = hashlib.sha1(test_file.read_bytes()).hexdigest()
    assert get_file_hash(str(test_file)) == expected_hash


def test_get_bytes_hash():
    data = "test content"
    expected_hash = hashlib.sha1(data.encode("utf-8")).hexdigest()
    assert get_bytes_hash(data) == expected_hash


SPACER_GIF = (
    b"GIF89a\\x01\\x00\\x01\\x00\\x80\\x00\\x00\\xff\\xff\\xff\\xff\\xff\\xff!\\xf9"
    b"\\x04\\x01\\x00\\x00\\x01\\x00,\\x00\\x00\\x00\\x00\\x01\\x00\\x01\\x00\\x00"
    b"\\x02\\x02L\\x01\\x00;"
)


def test_get_content_type(tmp_path):
    test_file = tmp_path / "test_file.txt"
    test_file.write_bytes(SPACER_GIF)
    assert get_content_type(str(test_file)) == "image/gif"


def test_clean_up_tmp_file_none_does_not_raise():
    clean_up_tmp_file(None)  # Should not raise any errors


def test_get_file_hash_file_not_found():
    with pytest.raises(FileNotFoundError):
        get_file_hash("non_existent_file.txt")


def test_get_file_hash_empty_file(tmp_path):
    test_file = tmp_path / "empty_file.txt"
    test_file.write_text("")
    expected_hash = hashlib.sha1(b"").hexdigest()
    assert get_file_hash(str(test_file)) == expected_hash


@patch("ingest_wikimedia.local.magic.from_file")
def test_get_content_type_magic_called(mock_magic_from_file, tmp_path):
    mock_magic_from_file.return_value = "application/octet-stream"
    test_file = tmp_path / "test.bin"
    test_file.write_bytes(b"some data")
    content_type = get_content_type(str(test_file))
    assert content_type == "application/octet-stream"


def test_get_content_type_file_not_found():
    with pytest.raises(FileNotFoundError):
        get_content_type("non_existent_file.txt")


def test_get_bytes_hash_empty_string():
    assert get_bytes_hash("") == "da39a3ee5e6b4b0d3255bfef95601890afd80709"
