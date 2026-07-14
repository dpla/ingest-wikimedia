"""Tests for the await-target-free set
(:mod:`ingest_wikimedia.await_target_free_sidecar`)."""

from __future__ import annotations

import fcntl
import multiprocessing
import os
from pathlib import Path

import pytest

from ingest_wikimedia import await_target_free_sidecar, drain_sidecar


@pytest.fixture(autouse=True)
def override_root(tmp_path, monkeypatch):
    """Isolate each test at a tmp root — same pattern as
    ``test_drain_sidecar``. Both modules resolve paths via
    :data:`drain_sidecar.INGEST_WIKI_ROOT`, so patching there is enough."""
    monkeypatch.setattr(drain_sidecar, "INGEST_WIKI_ROOT", tmp_path)
    return tmp_path


def test_read_missing_returns_empty_list():
    assert await_target_free_sidecar.read_keys("nara") == []
    assert await_target_free_sidecar.awaiting_dpla_ids("nara") == []


def test_add_then_has_and_read_roundtrip():
    await_target_free_sidecar.add_key("nara", "aaa", 1)
    assert await_target_free_sidecar.has_key("nara", "aaa", 1)
    assert await_target_free_sidecar.read_keys("nara") == ["aaa\t1"]


def test_has_key_matches_on_dpla_id_and_ordinal():
    await_target_free_sidecar.add_key("nara", "aaa", 1)
    await_target_free_sidecar.add_key("nara", "bbb", 2)
    assert await_target_free_sidecar.has_key("nara", "aaa", 1)
    assert await_target_free_sidecar.has_key("nara", "bbb", 2)
    # Same DPLA id, wrong ordinal → not a match.
    assert not await_target_free_sidecar.has_key("nara", "aaa", 2)
    assert not await_target_free_sidecar.has_key("nara", "ccc", 1)


def test_add_key_is_idempotent():
    await_target_free_sidecar.add_key("nara", "aaa", 1)
    await_target_free_sidecar.add_key("nara", "aaa", 1)
    assert await_target_free_sidecar.read_keys("nara") == ["aaa\t1"]


def test_remove_key_drops_only_the_matching_ordinal():
    await_target_free_sidecar.add_key("nara", "aaa", 1)
    await_target_free_sidecar.add_key("nara", "aaa", 2)
    await_target_free_sidecar.remove_key("nara", "aaa", 1)
    assert await_target_free_sidecar.read_keys("nara") == ["aaa\t2"]
    assert await_target_free_sidecar.has_key("nara", "aaa", 2)
    assert not await_target_free_sidecar.has_key("nara", "aaa", 1)


def test_remove_absent_key_is_noop():
    await_target_free_sidecar.add_key("nara", "aaa", 1)
    await_target_free_sidecar.remove_key("nara", "not-there", 9)
    assert await_target_free_sidecar.read_keys("nara") == ["aaa\t1"]


def test_remove_last_key_deletes_the_file():
    await_target_free_sidecar.add_key("nara", "aaa", 1)
    assert await_target_free_sidecar.sidecar_path("nara").exists()
    await_target_free_sidecar.remove_key("nara", "aaa", 1)
    assert not await_target_free_sidecar.sidecar_path("nara").exists()
    assert await_target_free_sidecar.read_keys("nara") == []


def test_awaiting_dpla_ids_dedupes_ordinals_preserving_order():
    await_target_free_sidecar.add_key("nara", "aaa", 1)
    await_target_free_sidecar.add_key("nara", "aaa", 2)  # same item, 2nd ordinal
    await_target_free_sidecar.add_key("nara", "bbb", 1)
    # Unique DPLA ids, first-seen order — this is what the drain re-runs.
    assert await_target_free_sidecar.awaiting_dpla_ids("nara") == ["aaa", "bbb"]


def test_read_tolerates_unparseable_file():
    """A present-but-unparseable file is treated as empty (like
    drain_sidecar): the set is reconstructable from Commons, so a
    mid-write crash mustn't wedge the drain."""
    path = await_target_free_sidecar.sidecar_path("nara")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{ this is not json")
    assert await_target_free_sidecar.read_keys("nara") == []


def test_read_ignores_malformed_keys():
    """Entries without the key separator are skipped, not returned."""
    import json

    path = await_target_free_sidecar.sidecar_path("nara")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"partner": "nara", "awaiting": ["aaa\t1", "no-sep", 42, "bbb\t3"]})
    )
    assert await_target_free_sidecar.read_keys("nara") == ["aaa\t1", "bbb\t3"]


def test_set_is_partner_scoped():
    await_target_free_sidecar.add_key("nara", "aaa", 1)
    await_target_free_sidecar.add_key("texas", "bbb", 1)
    assert await_target_free_sidecar.awaiting_dpla_ids("nara") == ["aaa"]
    assert await_target_free_sidecar.awaiting_dpla_ids("texas") == ["bbb"]


def test_sidecar_path_uses_partner_dir_mapping_for_smithsonian():
    """The ``si`` → ``smithsonian`` mapping in
    :data:`ingest_wikimedia.partners.PARTNER_DIR` is honoured, since
    ``sidecar_path`` reuses ``drain_sidecar.partner_dir_path``."""
    assert await_target_free_sidecar.sidecar_path("si").parent.name == "smithsonian"


def _child_add_key(root: str, dpla_id: str, ordinal: int) -> None:
    """Spawn-safe helper: re-anchor the root in the child, then add one
    key. Runs in a fresh interpreter under
    ``multiprocessing.get_context('spawn')`` so no state is inherited."""
    from ingest_wikimedia import await_target_free_sidecar as sidecar
    from ingest_wikimedia import drain_sidecar as ds

    ds.INGEST_WIKI_ROOT = Path(root)
    sidecar.add_key("nara", dpla_id, ordinal)


def test_add_key_serializes_concurrent_writers_across_processes(tmp_path):
    """The uploader's multiprocessing.Pool fans work out to workers that
    each call add_key concurrently. Without the cross-process flock, the
    read-modify-write races and a worker's key is lost. Eight spawned
    children each add a distinct key — all must survive."""
    ctx = multiprocessing.get_context("spawn")
    workers = [
        ctx.Process(target=_child_add_key, args=(str(tmp_path), f"id-{i:02d}", 1))
        for i in range(8)
    ]
    for w in workers:
        w.start()
    for w in workers:
        w.join(timeout=30)
        assert w.exitcode == 0, f"child worker failed (exit {w.exitcode})"
    ids = sorted(await_target_free_sidecar.awaiting_dpla_ids("nara"))
    assert ids == [f"id-{i:02d}" for i in range(8)], (
        f"expected all 8 concurrent adds to survive; got {ids!r}"
    )


def test_add_key_blocks_while_lock_is_held_and_proceeds_when_freed(
    tmp_path, monkeypatch
):
    """Direct check on the _locked_for_write gate: while a foreign fd
    holds the companion lockfile's flock, add_key blocks; once released,
    it proceeds."""
    import threading

    monkeypatch.setattr(drain_sidecar, "INGEST_WIKI_ROOT", tmp_path)
    sidecar_json = await_target_free_sidecar.sidecar_path("nara")
    sidecar_json.parent.mkdir(parents=True, exist_ok=True)
    lock_path = sidecar_json.with_suffix(sidecar_json.suffix + ".lock")
    fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)
    fcntl.flock(fd, fcntl.LOCK_EX)

    proceeded = threading.Event()

    def worker():
        await_target_free_sidecar.add_key("nara", "aaa", 1)
        proceeded.set()

    t = threading.Thread(target=worker)
    t.start()
    try:
        assert not proceeded.wait(timeout=0.5), (
            "add_key entered its critical section despite the lockfile being held"
        )
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
    assert proceeded.wait(timeout=3), "add_key did not proceed after lock release"
    t.join(timeout=1)
    assert await_target_free_sidecar.read_keys("nara") == ["aaa\t1"]
