from io import StringIO
from ingest_wikimedia.common import load_ids, null_safe, get_list, get_str, get_dict


def test_load_ids():
    ids_file = StringIO("id1\nid2\nid3")
    expected_ids = ["id1", "id2", "id3"]
    assert load_ids(ids_file) == expected_ids


def test_null_safe():
    data = {"key1": "value1", "key2": 2}
    assert null_safe(data, "key1", "") == "value1"
    assert null_safe(data, "key2", 0) == 2
    assert null_safe(data, "key3", "default") == "default"
    assert null_safe(None, "key1", "default") == "default"  # pyright: ignore [reportArgumentType] NOSONAR
    assert (
        null_safe(data, "key1", 0) == 0
    )  # Type mismatch, should return identity_element


def test_get_list():
    data = {"key1": [1, 2, 3], "key2": "not a list"}
    assert get_list(data, "key1") == [1, 2, 3]
    assert get_list(data, "key2") == []
    assert get_list(data, "key3") == []


def test_get_str():
    data = {"key1": "value1", "key2": 2}
    assert get_str(data, "key1") == "value1"
    assert get_str(data, "key2") == ""
    assert get_str(data, "key3") == ""


def test_get_dict():
    data = {"key1": {"subkey": "subvalue"}, "key2": "not a dict"}
    assert get_dict(data, "key1") == {"subkey": "subvalue"}
    assert get_dict(data, "key2") == {}
    assert get_dict(data, "key3") == {}
