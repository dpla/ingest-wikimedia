"""Tests for the banlist, focused on the Quarry remote-fetch layer.

The committed ``dpla-id-banlist.txt`` is the floor; the Quarry feed can only
add IDs on top of it. These tests pin that invariant and the fail-safe
behavior (network error / empty run / garbage never shrinks the banlist).
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from ingest_wikimedia import banlist as banlist_mod
from ingest_wikimedia.banlist import BANLIST_FILE_NAME, Banlist

COMMITTED_IDS = {
    line.rstrip()
    for line in (Path(__file__).parent.parent / BANLIST_FILE_NAME)
    .read_text()
    .splitlines()
    if line.strip()
}

NEW_ID = "a" * 32  # a valid-looking hex ID that is not in the committed file


@pytest.fixture(autouse=True)
def _isolated_cache(tmp_path, monkeypatch):
    """Point the remote cache at an empty tmp file so tests never read or
    write the real cache, and each test starts with a cold (absent) cache."""
    monkeypatch.setattr(banlist_mod, "CACHE_PATH", tmp_path / "cache.txt")


def _json_lines(*ids: str) -> str:
    return "\n".join('{"DPLA_id": "%s", "File": "x", "Reason": "y"}' % i for i in ids)


def _text_response(text: str) -> MagicMock:
    resp = MagicMock()
    resp.text = text
    resp.raise_for_status = MagicMock()
    return resp


def _meta_response(run_id: int = 1, status: str = "complete") -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {"latest_run": {"id": run_id, "status": status}}
    resp.raise_for_status = MagicMock()
    return resp


def _fake_get(*, meta: MagicMock, output: MagicMock):
    """A ``requests.get`` stand-in that dispatches on the URL: the ``meta``
    endpoint returns the meta payload, the ``output`` endpoint the JSON-lines."""

    def _dispatch(url, *args, **kwargs):
        if "/meta" in url:
            return meta
        if "/output/" in url:
            return output
        raise AssertionError(f"unexpected URL {url}")

    return _dispatch


def test_file_only_matches_committed_list():
    """fetch_remote=False loads exactly the committed file — no network."""
    with patch.object(banlist_mod.requests, "get") as get:
        bl = Banlist(fetch_remote=False)
        get.assert_not_called()
    assert bl.dpla_id_banlist == COMMITTED_IDS


def test_remote_ids_union_onto_committed_file():
    """A fresh Quarry ID from a completed run is added on top of the floor."""
    fake = _fake_get(
        meta=_meta_response(status="complete"),
        output=_text_response(_json_lines(NEW_ID)),
    )
    with patch.object(banlist_mod.requests, "get", side_effect=fake):
        bl = Banlist(fetch_remote=True)
    assert bl.is_banned(NEW_ID)
    assert COMMITTED_IDS <= bl.dpla_id_banlist  # floor preserved
    assert bl.dpla_id_banlist == COMMITTED_IDS | {NEW_ID}


def test_running_query_holds_last_good_list_without_reading_output():
    """While a run is executing (status != complete) we must NOT consume its
    output — we hold the committed floor (no cache present here)."""
    fake = _fake_get(
        meta=_meta_response(status="running"),
        # If the output endpoint is hit at all, the test fails loudly.
        output=_text_response("SHOULD NOT BE READ"),
    )
    with patch.object(banlist_mod.requests, "get", side_effect=fake) as get:
        bl = Banlist(fetch_remote=True)
    # meta was read, output was not.
    assert all("/output/" not in c.args[0] for c in get.call_args_list)
    assert bl.dpla_id_banlist == COMMITTED_IDS


def test_wait_for_run_blocks_until_complete_then_reads_output():
    """With wait_for_run=True, an in-progress run is polled until it completes,
    then its output is consumed."""
    metas = [
        _meta_response(run_id=7, status="queued"),
        _meta_response(run_id=7, status="running"),
        _meta_response(run_id=7, status="complete"),
    ]
    output = _text_response(_json_lines(NEW_ID))

    def _dispatch(url, *args, **kwargs):
        if "/meta" in url:
            return metas.pop(0)
        if "/output/" in url:
            return output
        raise AssertionError(f"unexpected URL {url}")

    with (
        patch.object(banlist_mod.requests, "get", side_effect=_dispatch),
        patch.object(banlist_mod.time, "sleep") as sleep,
    ):
        bl = Banlist(fetch_remote=True, wait_for_run=True)

    assert sleep.call_count == 2  # slept once per non-complete poll
    assert not metas  # all three meta polls consumed
    assert bl.dpla_id_banlist == COMMITTED_IDS | {NEW_ID}


def test_wait_for_run_times_out_and_falls_back():
    """If the run never completes within the budget, the launch falls back to
    the committed floor rather than blocking forever."""
    times = iter([0.0, 10.0, banlist_mod.RUN_WAIT_TIMEOUT_SECONDS + 1])
    with (
        patch.object(
            banlist_mod.requests,
            "get",
            side_effect=_fake_get(
                meta=_meta_response(status="running"),
                output=_text_response("SHOULD NOT BE READ"),
            ),
        ),
        patch.object(banlist_mod.time, "sleep"),
        patch.object(banlist_mod.time, "monotonic", side_effect=lambda: next(times)),
    ):
        bl = Banlist(fetch_remote=True, wait_for_run=True)
    assert bl.dpla_id_banlist == COMMITTED_IDS


def test_wait_for_run_ignores_fresh_cache():
    """A launch must observe current run state, not short-circuit on a stale
    cache from before the operator's manual refresh."""
    banlist_mod.CACHE_PATH.write_text("f" * 32 + "\n")  # would be used if cached
    fake = _fake_get(
        meta=_meta_response(status="complete"),
        output=_text_response(_json_lines(NEW_ID)),
    )
    with patch.object(banlist_mod.requests, "get", side_effect=fake) as get:
        bl = Banlist(fetch_remote=True, wait_for_run=True)
    # It went to the network despite a fresh cache being present.
    assert any("/meta" in c.args[0] for c in get.call_args_list)
    assert NEW_ID in bl.dpla_id_banlist
    assert ("f" * 32) not in bl.dpla_id_banlist  # stale cache entry not used


def test_malformed_lines_and_ids_are_dropped():
    """Non-JSON lines, missing keys, and non-hex IDs are ignored; valid ones
    still land."""
    body = "\n".join(
        [
            "not json at all",
            '{"File": "no id key"}',
            '{"DPLA_id": "TOO-SHORT"}',
            '{"DPLA_id": "notvalidhex____________________"}',
            '{"DPLA_id": "%s"}' % NEW_ID,
        ]
    )
    fake = _fake_get(meta=_meta_response(), output=_text_response(body))
    with patch.object(banlist_mod.requests, "get", side_effect=fake):
        bl = Banlist(fetch_remote=True)
    assert bl.dpla_id_banlist == COMMITTED_IDS | {NEW_ID}


def test_network_error_falls_back_to_committed_file():
    """A request exception never shrinks the banlist below the committed floor."""
    with patch.object(
        banlist_mod.requests,
        "get",
        side_effect=requests.ConnectionError("quarry down"),
    ):
        bl = Banlist(fetch_remote=True)
    assert bl.dpla_id_banlist == COMMITTED_IDS


def test_empty_completed_run_is_treated_as_failure():
    """An empty (or HTML error) output from a completed run must not wipe the
    banlist."""
    fake = _fake_get(meta=_meta_response(), output=_text_response(""))
    with patch.object(banlist_mod.requests, "get", side_effect=fake):
        bl = Banlist(fetch_remote=True)
    assert bl.dpla_id_banlist == COMMITTED_IDS


def test_fresh_cache_is_used_without_hitting_the_network():
    """A fresh cache short-circuits the fetch — the single-ID fan-out must not
    hammer Quarry once per process."""
    banlist_mod.CACHE_PATH.write_text(NEW_ID + "\n")
    with patch.object(banlist_mod.requests, "get") as get:
        bl = Banlist(fetch_remote=True)
        get.assert_not_called()
    assert bl.dpla_id_banlist == COMMITTED_IDS | {NEW_ID}


def test_stale_cache_used_when_fetch_fails():
    """If the cache is stale AND the fetch fails, the stale cache is still a
    better floor than nothing (still unioned with the committed file)."""
    cache = banlist_mod.CACHE_PATH
    cache.write_text(NEW_ID + "\n")
    # Force the cache to look stale, then fail the fetch.
    with (
        patch.object(banlist_mod, "_cache_is_fresh", return_value=False),
        patch.object(
            banlist_mod.requests,
            "get",
            side_effect=requests.ConnectionError("down"),
        ),
    ):
        bl = Banlist(fetch_remote=True)
    assert bl.dpla_id_banlist == COMMITTED_IDS | {NEW_ID}


def test_remote_disabled_via_env(monkeypatch):
    """The env kill-switch pins the banlist to the committed file."""
    monkeypatch.setenv("INGEST_WIKIMEDIA_BANLIST_REMOTE", "0")
    with patch.object(banlist_mod.requests, "get") as get:
        bl = Banlist()  # default -> env-controlled
        get.assert_not_called()
    assert bl.dpla_id_banlist == COMMITTED_IDS
