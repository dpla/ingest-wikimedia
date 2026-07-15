"""Tests for ingest_wikimedia.sha1_lock — the per-SHA1 cross-process upload lock."""

import fcntl
import os

import pytest

from ingest_wikimedia.sha1_lock import (
    NUM_BUCKETS,
    _bucket,
    _check_owner,
    acquire_sha1_lock,
    release_sha1_lock,
)


def test_bucket_is_deterministic_and_in_range():
    sha1 = "abcdef1234567890" * 2 + "abcdef12"  # 40 hex chars
    assert _bucket(sha1) == _bucket(sha1)
    assert 0 <= _bucket(sha1) < NUM_BUCKETS


def test_bucket_same_sha1_same_bucket_is_the_exclusion_guarantee():
    # The whole point: identical content always maps to the same bucket, so
    # two workers with the same SHA1 always contend on the same lock file.
    s = "0f1e2d3c" + "0" * 32
    assert _bucket(s) == _bucket(s)


def test_bucket_non_hex_input_does_not_raise():
    # A malformed sha1 must lock deterministically rather than blow up the
    # caller's hot path.
    b = _bucket("not-a-hex-digest!")
    assert 0 <= b < NUM_BUCKETS
    assert b == _bucket("not-a-hex-digest!")


def test_acquire_holds_exclusive_then_release_frees(tmp_path):
    sha1 = "deadbeef" + "0" * 32
    lock_dir = str(tmp_path)
    path = os.path.join(lock_dir, f"sha1-{_bucket(sha1):04x}.lock")
    fd = acquire_sha1_lock(sha1, lock_dir)
    try:
        # While held, a non-blocking exclusive flock on the SAME bucket file
        # from an independent fd must fail (the lock is genuinely exclusive
        # across fds / would-be processes).
        probe = os.open(path, os.O_RDWR)
        try:
            with pytest.raises(OSError):
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(probe)
    finally:
        # release_sha1_lock freeing the lock is the behavior under test here.
        release_sha1_lock(fd)

    # After release the same bucket file can be locked again.
    probe2 = os.open(path, os.O_RDWR)
    try:
        fcntl.flock(probe2, fcntl.LOCK_EX | fcntl.LOCK_NB)  # must not raise
        fcntl.flock(probe2, fcntl.LOCK_UN)
    finally:
        os.close(probe2)


def test_different_sha1_different_bucket_do_not_block(tmp_path):
    # Two SHA1s in different buckets can be held simultaneously — parallelism
    # is preserved for distinct content. (Pick two values known to differ.)
    lock_dir = str(tmp_path)
    a = "0" * 39 + "1"  # differ in the LOW bits, which drive the bucket
    b = "0" * 39 + "2"
    assert _bucket(a) != _bucket(b)
    fd_a = acquire_sha1_lock(a, lock_dir)
    try:
        fd_b = acquire_sha1_lock(b, lock_dir)  # must not block on fd_a
        try:
            assert fd_a != fd_b  # both held simultaneously; distinct fds
        finally:
            # Cleanup only (release_sha1_lock is covered by its own tests), so
            # close directly.
            os.close(fd_b)
    finally:
        os.close(fd_a)


def test_release_none_is_noop():
    release_sha1_lock(None)  # must not raise


def test_release_is_idempotent(tmp_path):
    fd = acquire_sha1_lock("a" * 40, str(tmp_path))
    try:
        release_sha1_lock(fd)  # first release closes the fd
    finally:
        release_sha1_lock(fd)  # double release must not raise (already-closed)


def test_foreign_owned_lock_dir_is_refused(tmp_path, monkeypatch):
    # A lock dir owned by a different uid is refused: an attacker who can write
    # there could unlink held lock files and break exclusion. Exercise the
    # guard (_check_owner) directly — it is what acquire_sha1_lock calls before
    # opening any fd, so no descriptor is created on this path.
    real_uid = os.stat(str(tmp_path)).st_uid
    monkeypatch.setattr(os, "getuid", lambda: real_uid + 4242)
    with pytest.raises(RuntimeError, match="refusing to use it"):
        _check_owner(str(tmp_path))
