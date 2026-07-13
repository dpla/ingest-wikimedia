"""Tests for the await-target-free sidecar
(:mod:`ingest_wikimedia.await_target_free_sidecar`)."""

from __future__ import annotations

import json

import pytest

from ingest_wikimedia import await_target_free_sidecar, drain_sidecar


def _entry(dpla_id: str = "22412cd0", ordinal: int = 1, **overrides) -> dict:
    """Canonical entry factory. Fill defaults; individual fields overridable
    via keyword args."""
    base = {
        "dpla_id": dpla_id,
        "ordinal": ordinal,
        "tagged_title": f"File:X - DPLA - {dpla_id}.jpg",
        "community_title": "File:X.jpg",
        "expected_sha1": "9719e05ab718aac6d400b239792ceeb45a766954",
    }
    base.update(overrides)
    return base


@pytest.fixture(autouse=True)
def override_root(tmp_path, monkeypatch):
    """Isolate each test at a tmp root — same pattern as
    ``test_drain_sidecar``. Both modules resolve paths via
    :data:`drain_sidecar.INGEST_WIKI_ROOT`, so patching there is enough."""
    monkeypatch.setattr(drain_sidecar, "INGEST_WIKI_ROOT", tmp_path)
    return tmp_path


def test_read_missing_returns_empty_list():
    assert await_target_free_sidecar.read_sidecar("nara") == []


def test_write_then_read_roundtrip():
    entries = [_entry("aaa", 1), _entry("bbb", 1)]
    await_target_free_sidecar.write_sidecar("nara", entries)
    assert await_target_free_sidecar.read_sidecar("nara") == entries


def test_write_empty_removes_file():
    await_target_free_sidecar.write_sidecar("nara", [_entry("aaa", 1)])
    assert await_target_free_sidecar.sidecar_path("nara").exists()
    await_target_free_sidecar.write_sidecar("nara", [])
    assert not await_target_free_sidecar.sidecar_path("nara").exists()
    assert await_target_free_sidecar.read_sidecar("nara") == []


def test_write_dedupes_by_dpla_id_and_ordinal():
    entries = [
        _entry("aaa", 1),
        _entry("aaa", 2),  # same dpla_id, different ordinal → distinct
        _entry("aaa", 1),  # dupe → suppressed
        _entry("bbb", 1),
    ]
    await_target_free_sidecar.write_sidecar("nara", entries)
    stored = await_target_free_sidecar.read_sidecar("nara")
    assert [(e["dpla_id"], e["ordinal"]) for e in stored] == [
        ("aaa", 1),
        ("aaa", 2),
        ("bbb", 1),
    ]


def test_read_tolerates_malformed_entries():
    """Corrupt sidecar (missing keys, wrong types) is treated as empty
    for the malformed entries; well-formed entries in the same file
    still surface. Keeps the drain loop resilient rather than crashing
    on a mid-write truncation."""
    path = await_target_free_sidecar.sidecar_path("nara")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "partner": "nara",
                "entries": [
                    _entry("good", 1),
                    "not a dict",
                    {"dpla_id": "missing-titles"},  # required keys absent
                    {**_entry("bad-ordinal", 1), "ordinal": "one"},
                    _entry("also-good", 1),
                ],
            }
        )
    )
    stored = await_target_free_sidecar.read_sidecar("nara")
    assert [e["dpla_id"] for e in stored] == ["good", "also-good"]


def test_read_tolerates_unparseable_file():
    path = await_target_free_sidecar.sidecar_path("nara")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{ this is not json")
    assert await_target_free_sidecar.read_sidecar("nara") == []


def test_has_entry_matches_only_on_dpla_id_and_ordinal():
    await_target_free_sidecar.write_sidecar(
        "nara", [_entry("aaa", 1), _entry("bbb", 2)]
    )
    assert await_target_free_sidecar.has_entry("nara", "aaa", 1)
    assert await_target_free_sidecar.has_entry("nara", "bbb", 2)
    # Wrong ordinal on same DPLA ID → not a match.
    assert not await_target_free_sidecar.has_entry("nara", "aaa", 2)
    assert not await_target_free_sidecar.has_entry("nara", "ccc", 1)


def test_add_entry_dedupes_by_key():
    await_target_free_sidecar.add_entry("nara", _entry("aaa", 1))
    # Second add with same (dpla_id, ordinal) is a no-op (returns existing).
    result = await_target_free_sidecar.add_entry(
        "nara",
        _entry("aaa", 1, tagged_title="File:different tag.jpg"),
    )
    assert len(result) == 1
    # The FIRST add's fields win; the second add is discarded.
    assert result[0]["tagged_title"] == "File:X - DPLA - aaa.jpg"


def test_add_entry_rejects_malformed():
    with pytest.raises(ValueError):
        await_target_free_sidecar.add_entry("nara", {"dpla_id": "no-titles"})


def test_remove_entry_by_key():
    await_target_free_sidecar.write_sidecar(
        "nara", [_entry("aaa", 1), _entry("bbb", 1), _entry("ccc", 1)]
    )
    remaining = await_target_free_sidecar.remove_entry("nara", "bbb", 1)
    assert [e["dpla_id"] for e in remaining] == ["aaa", "ccc"]


def test_remove_entry_absent_key_is_noop():
    await_target_free_sidecar.write_sidecar("nara", [_entry("aaa", 1)])
    remaining = await_target_free_sidecar.remove_entry("nara", "not-there", 1)
    assert [e["dpla_id"] for e in remaining] == ["aaa"]


def test_remove_entry_empty_remainder_deletes_file():
    await_target_free_sidecar.write_sidecar("nara", [_entry("aaa", 1)])
    await_target_free_sidecar.remove_entry("nara", "aaa", 1)
    assert not await_target_free_sidecar.sidecar_path("nara").exists()


def test_sidecar_is_partner_scoped():
    """Two partners' sidecars live in different files and don't
    cross-contaminate — same guarantee as ``drain_sidecar``."""
    await_target_free_sidecar.write_sidecar("nara", [_entry("aaa", 1)])
    await_target_free_sidecar.write_sidecar("texas", [_entry("bbb", 1)])
    assert [e["dpla_id"] for e in await_target_free_sidecar.read_sidecar("nara")] == [
        "aaa"
    ]
    assert [e["dpla_id"] for e in await_target_free_sidecar.read_sidecar("texas")] == [
        "bbb"
    ]


def test_sidecar_path_uses_partner_dir_mapping_for_smithsonian():
    """The ``si`` → ``smithsonian`` mapping in
    :data:`ingest_wikimedia.partners.PARTNER_DIR` is honoured, since
    ``sidecar_path`` reuses ``drain_sidecar.partner_dir_path``. Same
    guarantee as ``drain_sidecar.sidecar_path``."""
    assert await_target_free_sidecar.sidecar_path("si").parent.name == "smithsonian"
