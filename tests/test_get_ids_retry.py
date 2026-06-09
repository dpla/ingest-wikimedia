"""Tests for the SDC log scanner in tools.get_ids_retry.

The upload and download scanners are exercised indirectly by integration
tests on the retry pipeline; this file focuses on parse_sdc_log because
its classification rules are dense (every transient pattern is one
substring match) and the scanner is the only point where structural
errors are deliberately excluded from the retry CSV.
"""

from pathlib import Path

import pytest

from tools.get_ids_retry import parse_sdc_log

MAXLAG_BLOCK = """\
[INFO] 09:11:50:  -- Ordinal 113: M192146077 (something)
[ERROR] 09:16:50:  -- Ordinal 113 (M192146077) for {id}: SDC sync failed; skipping ordinal.
Traceback (most recent call last):
  File "/x/sdc_sync.py", line 3307, in _run_partner_mode
    process_one_from_sdc(...)
  File "/x/sdc_sync.py", line 3011, in process_one_from_sdc
    get_entity(mediaid)
  File "/x/sdc_sync.py", line 496, in get_entity
    raw = site.simple_request(action="wbgetentities", ids=mediaid).submit()
pywikibot.exceptions.MaxlagTimeoutError: Maximum retries attempted due to maxlag without success.
[INFO] 09:21:51:  -- Ordinal 114: M192146082 (something)
"""

READONLY_BLOCK = """\
[ERROR] 09:30:00:  -- Ordinal 22 (M9999) for {id}: SDC sync failed; skipping ordinal.
Traceback (most recent call last):
  ...
pywikibot.exceptions.APIError: readonly: The wiki is currently in read-only mode.
[INFO] 09:30:05:  -- Ordinal 23: M99999 (something)
"""

EDITCONFLICT_BLOCK = """\
[ERROR] 09:40:00:  -- Ordinal 5 (M5555) for {id}: SDC sync failed; skipping ordinal.
Traceback (most recent call last):
  ...
pywikibot.exceptions.EditConflictError: editconflict: An edit conflict occurred.
[INFO] 09:40:05:  -- Ordinal 6: M5556 (something)
"""

CONNRESET_BLOCK = """\
[ERROR] 09:50:00:  -- Ordinal 2 (M2222) for {id}: SDC sync failed; skipping ordinal.
Traceback (most recent call last):
  ...
  File ".../urllib3/connection.py", line 100, in _new_conn
    raise NewConnectionError(...)
requests.exceptions.ConnectionError: HTTPSConnectionPool(host='commons.wikimedia.org', port=443): Max retries exceeded
[INFO] 09:50:05:  -- Ordinal 3: M2223 (something)
"""

INVALID_CLAIM_BLOCK = """\
[ERROR] 10:00:00:  -- Ordinal 8 (M8888) for {id}: SDC sync failed; skipping ordinal.
Traceback (most recent call last):
  ...
  File "/x/sdc_sync.py", line 1914, in _submit_sdc_write
    raise RuntimeError(...) from e
RuntimeError: wbeditentity failed for M8888 (deadbeef...): invalid-claim - Type is missing
[INFO] 10:00:05:  -- Ordinal 9: M8889 (something)
"""

PERMISSION_BLOCK = """\
[ERROR] 10:10:00:  -- Ordinal 11 (M1111) for {id}: SDC sync failed; skipping ordinal.
Traceback (most recent call last):
  ...
pywikibot.exceptions.APIError: permissiondenied: You don't have permission to edit this page.
[INFO] 10:10:05:  -- Ordinal 12: M1112 (something)
"""

KEYERROR_BLOCK = """\
[ERROR] 10:20:00:  -- Ordinal 14 (M1414) for {id}: SDC sync failed; skipping ordinal.
Traceback (most recent call last):
  ...
  File "/x/sdc_sync.py", line 555, in process_one_from_sdc
    foo = claim['nonexistent']
KeyError: 'nonexistent'
[INFO] 10:20:05:  -- Ordinal 15: M1415 (something)
"""


def _write_log(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "20260101-000000-nara+example-sdc.log"
    p.write_text(body, encoding="utf-8")
    return p


@pytest.mark.parametrize(
    "block,name",
    [
        (MAXLAG_BLOCK, "maxlag"),
        (READONLY_BLOCK, "readonly"),
        (EDITCONFLICT_BLOCK, "editconflict"),
        (CONNRESET_BLOCK, "connection-reset"),
    ],
)
def test_transient_errors_are_retryable(tmp_path, block, name):
    """Every pattern in SDC_TRANSIENT_ERRORS classifies the ID as retryable."""
    dpla_id = "a" * 32
    log = _write_log(tmp_path, block.format(id=dpla_id))
    assert parse_sdc_log(log) == {dpla_id}, f"{name} should be retryable"


@pytest.mark.parametrize(
    "block,name",
    [
        (INVALID_CLAIM_BLOCK, "invalid-claim"),
        (PERMISSION_BLOCK, "permissiondenied"),
        (KEYERROR_BLOCK, "KeyError-code-bug"),
    ],
)
def test_structural_errors_excluded(tmp_path, block, name):
    """Structural / permanent failures must NOT land in the retry set."""
    dpla_id = "b" * 32
    log = _write_log(tmp_path, block.format(id=dpla_id))
    assert parse_sdc_log(log) == set(), f"{name} should be excluded"


def test_mixed_log_collects_only_retryable(tmp_path):
    """Single log file with multiple per-ordinal errors of varying types:
    only the transient ones end up in the result."""
    log_body = (
        MAXLAG_BLOCK.format(id="1" * 32)
        + INVALID_CLAIM_BLOCK.format(id="2" * 32)
        + CONNRESET_BLOCK.format(id="3" * 32)
        + PERMISSION_BLOCK.format(id="2" * 32)  # same id as the invalid-claim
        + EDITCONFLICT_BLOCK.format(id="4" * 32)
    )
    log = _write_log(tmp_path, log_body)
    # Note: "2" * 32 has BOTH a structural and a permission error in this
    # synthetic log. Neither matches SDC_TRANSIENT_RE, so the ID stays out.
    assert parse_sdc_log(log) == {"1" * 32, "3" * 32, "4" * 32}


def test_same_id_multiple_ordinals_dedups(tmp_path):
    """An item with N transient-failed ordinals appears once in the result."""
    dpla_id = "c" * 32
    log_body = MAXLAG_BLOCK.format(id=dpla_id) + CONNRESET_BLOCK.format(id=dpla_id)
    log = _write_log(tmp_path, log_body)
    assert parse_sdc_log(log) == {dpla_id}


def test_empty_log_returns_empty_set(tmp_path):
    log = _write_log(tmp_path, "")
    assert parse_sdc_log(log) == set()


def test_log_with_no_errors_returns_empty_set(tmp_path):
    log = _write_log(
        tmp_path,
        "[INFO] 09:00:00: Partner mode: nara — 100 items from ...csv\n"
        "[INFO] 09:00:01:  -- Ordinal 1: M1 (something)\n"
        "[INFO] 09:00:02:  -- Touched 'File:foo.jpg' (category refresh).\n",
    )
    assert parse_sdc_log(log) == set()


def test_traceback_terminates_at_next_error_marker(tmp_path):
    """Back-to-back per-ordinal errors with no [INFO] between them: each
    error's traceback ends when the next [ERROR] marker is reached, so
    the classifier doesn't blur the two error contexts together.
    """
    id_a = "a" * 32
    id_b = "b" * 32
    # First error is structural (KeyError), second is transient (maxlag).
    # If the parser blurred them, both IDs would appear in the result
    # because the maxlag string would taint the first block. Correct
    # behavior: only the maxlag-id is retryable.
    log_body = (
        KEYERROR_BLOCK.format(id=id_a).rstrip().rsplit("\n", 1)[0] + "\n"
        f"[ERROR] 10:25:00:  -- Ordinal 14 (M1414) for {id_b}: SDC sync failed; skipping ordinal.\n"
        "pywikibot.exceptions.MaxlagTimeoutError: Maximum retries attempted.\n"
        "[INFO] 10:25:05:  -- Ordinal 15: M1415 (foo)\n"
    )
    log = _write_log(tmp_path, log_body)
    result = parse_sdc_log(log)
    assert id_b in result
    assert id_a not in result
