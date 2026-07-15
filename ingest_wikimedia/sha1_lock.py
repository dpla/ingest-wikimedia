"""Per-SHA1 cross-process lock for the uploader's Commons check-then-act.

The uploader parallelizes one partner run across worker processes (a
``multiprocessing`` Pool), and several partner sessions run concurrently on
the wiki box. When two of those processes hold byte-identical source media for
DIFFERENT DPLA items (a cross-item / cross-institution duplicate), both can
observe :func:`ingest_wikimedia.wikimedia.find_file_by_hash` returning no match
and both proceed to upload the same SHA1.

Commons's own duplicate detection still prevents a second file from being
published — the fresh-upload path commits with ``force_ignore_warnings=False``,
so the loser's commit surfaces the duplicate warning rather than suppressing
it — so this never violated the one-SHA1-one-file invariant. But the loser
wasted an upload attempt, recorded a spurious ``FAILED``, and (for a cross-item
duplicate) only merged its SDC onto the winner on a LATER run. This lock closes
that window: a process takes an exclusive per-SHA1 lock before its
check-then-act and holds it through the upload commit, so a second process with
the same SHA1 waits, re-checks, and (barring replica lag — see below) sees the
winner's file and resolves to a merge / redirect / skip in the SAME pass.

The re-check reads a MediaWiki replica that may not yet reflect the winner's
just-committed upload, so under replication lag the loser can still fall
through to a fresh upload — the lock NARROWS the window rather than fully
closing it. That residual is harmless: the loser's commit uses
``force_ignore_warnings=False``, so Commons' own duplicate-SHA1 detection
rejects it and no second file is published (worst case equals the pre-lock
behavior — a wasted attempt, never a duplicate). The lock is an optimization
layered on that invariant backstop, not a replacement for it.

Design (mirrors :mod:`ingest_wikimedia.worker_slots`'s flock rationale —
crash-safe, inspectable, no cleanup contract):

  * ``fcntl.flock`` on a lock file; the lock releases when the fd is closed OR
    the holder dies, so a crashed / OOM-killed worker frees it immediately with
    no leaked-permit accounting.
  * **Striped**: a FIXED set of :data:`NUM_BUCKETS` lock files keyed by a hash
    of the SHA1 (``sha1 -> bucket``). The same SHA1 always maps to the same
    bucket, so the exclusion the caller needs is exact; two DIFFERENT SHA1s
    only ever contend on a bucket collision (~holders / NUM_BUCKETS — well
    under 1% at the box's ~16-writer concurrency with 4096 buckets). Such a
    collision waits for the holder's check-then-act, which spans its full
    upload commit — bounded, but up to ``UPLOAD_TIMEOUT_SECS`` in the
    pathological hung-Commons case, not necessarily brief. Progress is always
    guaranteed (the holder releases within that bound, or immediately on
    process death via flock auto-release), and a blocked worker keeps its
    ``WorkerSlotBudget`` slot while waiting, so a same-SHA1 / same-bucket
    hotspot transiently serializes slot usage. Striping bounds the lock-file
    count to NUM_BUCKETS
    regardless of how many distinct SHA1s a run uploads; a per-SHA1 file would
    grow unbounded (one per new upload) and bloat the directory, and deleting
    per-SHA1 files after release would reintroduce the classic unlink-vs-lock
    exclusion race (see the same note in ``worker_slots``).
  * Under ``/tmp`` because the locks are inherently ephemeral — a reboot that
    clears ``/tmp`` also kills every holder, so there is nothing to preserve.

Scope: this guards the fresh-upload leg (the lookup-then-upload race). The
collision-resolution legs (merge-onto-canonical / redirect) are additive and
idempotent with ``reconcile=False`` and rely on pywikibot's edit-conflict
retry for concurrent same-canonical writes, so they don't need this lock to
stay correct — only the upload leg could publish wasted work.
"""

from __future__ import annotations

import fcntl
import os
import zlib

# Fixed directory holding the striped lock files. Shared across every uploader
# process on the host (box-wide), so a cross-partner duplicate is serialized
# too. Under /tmp — ephemeral by design.
SHA1_LOCK_DIR = "/tmp/dpla-uploader-sha1-locks"

# Number of striped buckets. Sized so that, at the box's practical write
# concurrency (~16 simultaneous uploaders), the chance a fresh upload's bucket
# is already held by an unrelated SHA1 (~holders / NUM_BUCKETS) stays well
# under 1%. Same-SHA1 exclusion is always exact regardless of this value.
NUM_BUCKETS = 4096

# Owner-only: the lock files carry no data (the flock is the whole signal), so
# group/other access buys nothing and a stricter mode keeps another local
# account from interfering with them on a shared host.
_LOCK_FILE_MODE = 0o600


def _bucket(sha1: str) -> int:
    """Map a SHA1 to its striped bucket in ``[0, NUM_BUCKETS)``.

    A SHA1 is a 40-char hex digest; parsing the whole digest mod NUM_BUCKETS
    distributes uniformly for real (random) inputs. Falls back to ``crc32`` for
    any non-hex input so a malformed value still locks deterministically. The
    fallback deliberately uses ``crc32`` and NOT the builtin ``hash``: ``hash``
    is per-process salted (``PYTHONHASHSEED``), so it would map the same value
    to DIFFERENT buckets in different worker processes and silently break the
    cross-process exclusion this module exists to provide. ``crc32`` is stable
    across processes and Python versions.
    """
    try:
        return int(sha1, 16) % NUM_BUCKETS
    except (ValueError, TypeError):
        return zlib.crc32(str(sha1).encode()) % NUM_BUCKETS


def _check_owner(lock_dir: str) -> None:
    """Refuse a lock dir owned by another uid.

    Mirrors ``worker_slots``: an attacker who can write into our lock dir could
    ``unlink`` a held lock file, forcing a new inode the next time we
    ``os.open`` it and silently breaking the exclusion invariant. On today's
    single-tenant EC2 this is a no-op; the cost is one ``os.stat``.
    """
    st = os.stat(lock_dir)
    if st.st_uid != os.getuid():
        raise RuntimeError(
            f"SHA1 lock dir {lock_dir!r} is owned by uid {st.st_uid}, not the "
            f"current user (uid {os.getuid()}); refusing to use it (an attacker "
            f"with write access could unlink held lock files and break the "
            f"exclusion invariant)."
        )


def acquire_sha1_lock(sha1: str, lock_dir: str = SHA1_LOCK_DIR) -> int:
    """Block until the exclusive lock for ``sha1``'s bucket is held; return fd.

    Close the returned fd (:func:`release_sha1_lock`) to release; the flock also
    releases automatically if this process dies. Blocking is intended: a second
    process with the same SHA1 waits for the holder's check-then-act (including
    its upload commit) to finish, then proceeds and re-checks.
    """
    os.makedirs(lock_dir, exist_ok=True)
    _check_owner(lock_dir)
    path = os.path.join(lock_dir, f"sha1-{_bucket(sha1):04x}.lock")
    fd = os.open(path, os.O_RDWR | os.O_CREAT, _LOCK_FILE_MODE)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
    except BaseException:
        os.close(fd)
        raise
    return fd


def release_sha1_lock(fd: int | None) -> None:
    """Release a lock from :func:`acquire_sha1_lock`. No-op for ``None``.

    Best-effort: closing the fd releases the flock, but an already-closed fd
    must never abort the run (the lock is an optimization, not a correctness
    dependency — Commons' own dedup is the invariant backstop). The tolerated
    double-close only stays safe while the fd integer has NOT been reused by a
    later ``os.open``; callers must release each fd exactly once (the uploader
    does, from a single method-level ``finally``). The tolerance is defensive
    against a stray repeat, not license to release repeatedly.
    """
    if fd is None:
        return
    try:
        os.close(fd)
    except OSError:
        pass
