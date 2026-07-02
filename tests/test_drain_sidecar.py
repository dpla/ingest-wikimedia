"""Tests for the deferred-drain sidecar (``ingest_wikimedia.drain_sidecar``)."""

from __future__ import annotations

import json
import os

import pytest

from ingest_wikimedia import drain_sidecar


@pytest.fixture(autouse=True)
def chdir_tmp(tmp_path, monkeypatch):
    """Every test runs in an isolated tmp directory so the sidecar
    path (``<partner>/deferred-drain.json``) doesn't collide with any
    real partner tree or between tests."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_read_sidecar_missing_returns_empty_list():
    assert drain_sidecar.read_sidecar("nara") == []


def test_write_then_read_roundtrip():
    drain_sidecar.write_sidecar("nara", ["abc", "def"])
    assert drain_sidecar.read_sidecar("nara") == ["abc", "def"]


def test_write_empty_deletes_the_file():
    drain_sidecar.write_sidecar("nara", ["abc"])
    assert drain_sidecar.sidecar_path("nara").exists()
    drain_sidecar.write_sidecar("nara", [])
    assert not drain_sidecar.sidecar_path("nara").exists()
    # Missing == empty for reader.
    assert drain_sidecar.read_sidecar("nara") == []


def test_write_dedupes_within_the_input():
    drain_sidecar.write_sidecar("nara", ["abc", "def", "abc", "ghi", "def"])
    assert drain_sidecar.read_sidecar("nara") == ["abc", "def", "ghi"]


def test_write_is_atomic(tmp_path):
    """The temp-file+rename shape shouldn't leave a truncated file
    observable at the final path. Sanity check by seeing that a
    partially-written file at the temp name (from a hypothetical
    crash) doesn't affect a subsequent successful write."""
    partner_dir = tmp_path / "nara"
    partner_dir.mkdir()
    # Simulate crash residue: a stray tempfile that was never renamed.
    (partner_dir / ".deferred-drain-crashed.tmp").write_text("{ broken")
    drain_sidecar.write_sidecar("nara", ["abc"])
    assert drain_sidecar.read_sidecar("nara") == ["abc"]


def test_merge_sidecar_appends_new_ids_preserving_order():
    drain_sidecar.write_sidecar("nara", ["a", "b"])
    combined = drain_sidecar.merge_sidecar("nara", ["c", "b", "d"])
    # Existing preserved in original order; new entries appended in input
    # order; dupes suppressed.
    assert combined == ["a", "b", "c", "d"]
    assert drain_sidecar.read_sidecar("nara") == ["a", "b", "c", "d"]


def test_merge_sidecar_creates_when_missing():
    combined = drain_sidecar.merge_sidecar("nara", ["a", "b"])
    assert combined == ["a", "b"]
    assert drain_sidecar.read_sidecar("nara") == ["a", "b"]


def test_remove_from_sidecar_drops_only_the_given_ids():
    drain_sidecar.write_sidecar("nara", ["a", "b", "c", "d"])
    remaining = drain_sidecar.remove_from_sidecar("nara", ["b", "d"])
    assert remaining == ["a", "c"]
    assert drain_sidecar.read_sidecar("nara") == ["a", "c"]


def test_remove_from_sidecar_ignores_absent_ids():
    drain_sidecar.write_sidecar("nara", ["a"])
    remaining = drain_sidecar.remove_from_sidecar("nara", ["never-queued", "a"])
    assert remaining == []


def test_remove_from_sidecar_empty_remainder_deletes_the_file():
    """Removing the last IDs leaves the unambiguous empty state
    (missing file), same as ``write_sidecar([])``."""
    drain_sidecar.write_sidecar("nara", ["a", "b"])
    remaining = drain_sidecar.remove_from_sidecar("nara", ["a", "b"])
    assert remaining == []
    assert not drain_sidecar.sidecar_path("nara").exists()


def test_remove_from_sidecar_on_missing_file_is_a_noop():
    assert drain_sidecar.remove_from_sidecar("nara", ["a"]) == []
    assert not drain_sidecar.sidecar_path("nara").exists()


def test_read_tolerates_unparseable_file():
    """A file present but corrupted (mid-write crash, hand-edit,
    disk error) is treated the same as missing — the drain loop
    should stay resilient rather than crash on a bad sidecar."""
    path = drain_sidecar.sidecar_path("nara")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{ this is not json")
    assert drain_sidecar.read_sidecar("nara") == []


def test_read_tolerates_wrong_shape():
    """A JSON file whose payload doesn't have the expected shape
    returns [] — same as unparseable."""
    path = drain_sidecar.sidecar_path("nara")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"partner": "nara", "wrong_key": ["a"]}))
    assert drain_sidecar.read_sidecar("nara") == []


def test_sidecar_payload_is_stable_shape():
    """Pin the on-disk format so a future refactor can't silently
    rename ``deferred_dpla_ids`` and cause every existing sidecar in
    production to be treated as empty on read."""
    drain_sidecar.write_sidecar("nara", ["abc"])
    with open(drain_sidecar.sidecar_path("nara")) as f:
        data = json.load(f)
    assert data == {"partner": "nara", "deferred_dpla_ids": ["abc"]}


def test_no_stray_tempfiles_after_successful_writes(tmp_path):
    """Belt: repeat writes shouldn't accumulate ``.deferred-drain-*``
    tempfiles in the partner dir."""
    for i in range(5):
        drain_sidecar.write_sidecar("nara", [f"id-{i}"])
    tempfiles = list((tmp_path / "nara").glob(".deferred-drain-*"))
    assert tempfiles == [], f"stray tempfiles: {tempfiles}"
    # Sanity: the intended file exists.
    assert drain_sidecar.sidecar_path("nara").exists()


def test_failed_write_cleans_up_its_tempfile(tmp_path, monkeypatch):
    """A write that dies before the atomic rename (e.g. disk full
    mid-``json.dump``) must remove its own ``.deferred-drain-*.tmp``
    orphan and leave the previous sidecar contents untouched."""
    drain_sidecar.write_sidecar("nara", ["keep-me"])

    def boom(*args, **kwargs):
        raise OSError("disk full")

    with monkeypatch.context() as m:
        m.setattr(drain_sidecar.json, "dump", boom)
        with pytest.raises(OSError, match="disk full"):
            drain_sidecar.write_sidecar("nara", ["new-id"])

    tempfiles = list((tmp_path / "nara").glob(".deferred-drain-*"))
    assert tempfiles == [], f"orphaned tempfiles: {tempfiles}"
    assert drain_sidecar.read_sidecar("nara") == ["keep-me"]


def test_read_ignores_non_string_entries():
    """A hand-edited sidecar with garbage in the list still reads
    cleanly — the drain loop should ignore junk rather than crash."""
    path = drain_sidecar.sidecar_path("nara")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {"partner": "nara", "deferred_dpla_ids": ["good", 42, None, "also-good"]}
        )
    )
    assert drain_sidecar.read_sidecar("nara") == ["good", "also-good"]


def test_sidecar_path_is_partner_scoped():
    """One sidecar per partner. Partner slug lands as the parent dir."""
    p = drain_sidecar.sidecar_path("nara")
    assert p.name == "deferred-drain.json"
    assert p.parent.name == "nara"


def test_write_creates_partner_directory_if_missing():
    """A drain phase that starts before any partner-dir files exist
    still gets a working sidecar."""
    assert not os.path.isdir("nara")
    drain_sidecar.write_sidecar("nara", ["abc"])
    assert os.path.isdir("nara")
    assert drain_sidecar.read_sidecar("nara") == ["abc"]
