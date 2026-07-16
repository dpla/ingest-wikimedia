"""DPLA → Wikimedia Commons per-ordinal uploader.

## The upload invariant (correctness criterion for this file)

**For every DPLA item, the SHA1 of the S3-staged source bytes for that
item must live at the Commons title ``get_page_title(dpla_id, …)``
produces.**

That is the entire correctness criterion this file is responsible
for. Every branch of :meth:`Uploader.process_file`,
:meth:`Uploader._resolve_hash_drift`, and the redirect handling below
is chosen because it either enforces this invariant directly or
delegates to a step that will.

## The SHA1-uniqueness constraint (PR C+D)

**No two Commons files may share a SHA1.** The invariant GOAL above is
unchanged, but we no longer satisfy it by uploading a byte-identical
second file. When our S3 source's SHA1 already exists on Commons we
CENTRALIZE that SHA1 to ONE canonical file (the earliest existing
upload), which carries every contributing DPLA item's structured data,
and REDIRECT the other expected titles to it. We never upload a second
copy of bytes already present, and we never emit ``{{Duplicate}}``.

Consequences enforced in this file:

- Once :func:`find_file_by_hash` returns any existing Commons file for
  our SHA1, ``process_file`` NEVER falls through to an upload. It
  resolves to exactly one of: SKIP (already at the intended title),
  MOVE/rename (our SHA1 at a wrong title and the intended title is
  free or a redirect to our own file), MERGE+REDIRECT (the SHA1 match
  is legitimate source duplication — same bytes at multiple positions
  within an item, or across items / institutions), or HAND_FIX (our
  SHA1 is at a wrong title and the intended title is occupied by a
  DIFFERENT file, so the bot cannot safely make the rename).
- Human-authored ``#REDIRECT`` on a Commons title still does not bind
  us on the fresh-upload path: the intended title is where the bytes
  belong for our DPLA ID.

The full statement — including the centralize/redirect/merge model —
lives in ``docs/upload-invariant.md``. **Read that document before
making any change to this file that could affect what SHA1 lands at
what Commons title.**
"""

import concurrent.futures
import datetime
import gc
import json
import logging
import mimetypes
import os
import random
import re
import time
from enum import Enum

import click
import pywikibot
from pywikibot.site import BaseSite

from tqdm import tqdm

from ingest_wikimedia.common import (
    get_list,
    get_dict,
    get_str,
    load_ids,
    CHECKSUM,
)
from ingest_wikimedia.localfs import LocalFS
from ingest_wikimedia.logs import setup_logging
from ingest_wikimedia.slack import notify_phase_start
from ingest_wikimedia.s3 import (
    S3_BUCKET,
    S3Client,
)
from ingest_wikimedia.tools_context import ToolsContext
from ingest_wikimedia.tracker import Result, Tracker
from ingest_wikimedia.sha1_lock import (
    SHA1_LOCK_DIR,
    acquire_sha1_lock,
    release_sha1_lock,
)
from ingest_wikimedia.worker_slots import (
    UPLOADER_PRIORITY_SLOT_DIR,
    UPLOADER_PRIORITY_SLOTS,
    WorkerSlotBudget,
)
from ingest_wikimedia.categories import CategoryEnsurer, touch_institution_files
from ingest_wikimedia.csrf import (
    CsrfRecoveryFailed,
    MAX_CSRF_RECOVERIES,
    bump_session_recovery,
    is_csrf_token_error,
    recover_commons_session,
    reset_session_recoveries,
    session_recoveries_used,
    with_csrf_recovery,
)
from ingest_wikimedia.dpla import (
    SOURCE_RESOURCE_FIELD_NAME,
    DC_TITLE_FIELD_NAME,
    DATA_PROVIDER_FIELD_NAME,
    EDM_AGENT_NAME,
    WIKIDATA_FIELD_NAME,
    DPLA,
)
from ingest_wikimedia import hand_fix_sidecar
from ingest_wikimedia.slack import (
    notify_upload_aborted,
    notify_upload_complete,
)
from ingest_wikimedia.wikimedia import (
    WMC_UPLOAD_CHUNK_SIZE,
    IGNORE_WIKIMEDIA_WARNINGS,
    MIME_UNKNOWN_EXT,
    build_title_drift_move_reason,
    collect_duplicate_source_sha1s,
    compute_sha1_page_numbers,
    prescan_ordinals,
    get_page_title,
    get_wiki_text,
    wikimedia_url,
    find_file_by_hash,
    first_uploader,
    extract_dpla_id_from_commons_title,
    is_same_item_redirect_relic,
    check_content_type,
    is_download_only,
    get_page,
    ERROR_FILEEXISTS,
    ERROR_MIME,
    ERROR_BANNED,
    ERROR_DUPLICATE,
    ERROR_NOCHANGE,
    ERROR_BACKEND_FAIL,
    get_site,
    file_has_inbound_usage,
    post_commonsdelinker_request,
)
from ingest_wikimedia.legacy_artwork import (
    DPLA_BOT_ACCOUNTS,
    _normalize_account,
    rescue_wikitext,
)

MAX_UPLOAD_RETRIES = 3
UPLOAD_RETRY_BASE_DELAY_SECS = 5
# Post-upload pageid-refresh retry budget. Commons accepts large
# (chunked) uploads before its search/categorylinks index has caught
# up; the immediate ``FilePage.exists()`` query then races indexing
# and the resulting ``.pageid`` is missing or 0. A small bounded
# retry closes the window for ~all realistic indexing-lag durations
# without adding latency on the typical small-file fast path.
PAGEID_REFRESH_MAX_ATTEMPTS = 3
PAGEID_REFRESH_BACKOFF_SECS = 4
UPLOAD_RETRY_MAX_DELAY_SECS = 60
# pywikibot's async upload polls Commons indefinitely when the job queue is stuck.
# This cap ensures a single hung upload never freezes the whole session.
UPLOAD_TIMEOUT_SECS = 3600  # 1 hour

# Wikimedia's MediaWiki API rejects single-request upload bodies above roughly
# 100 MB (the exact threshold varies with infrastructure; the practical hard
# limit observed in NARA runs is between 100–200 MB before the gateway closes
# the connection mid-stream).  Pywikibot's chunk_size=0 path puts the entire
# file in one HTTP body, and its internal request-retry loop keeps that body
# alive via exception tracebacks across retries — a single 211 MB upload was
# observed to grow the process to 6.7 GB resident before OOM.  Above this
# size we force chunked upload regardless of any other flag: stash chunks are
# bounded at WMC_UPLOAD_CHUNK_SIZE (20 MB), so peak memory stays well under
# control even on the same internal retry pattern.
LARGE_FILE_DIRECT_UPLOAD_LIMIT_BYTES = 95 * 1024 * 1024  # 95 MB


def is_dup_sha1_sibling_at_expected_title(
    *,
    sha1: str,
    existing_file_title: str,
    duplicate_source_sha1s: set[str] | None,
    expected_item_titles: set[str] | None,
) -> bool:
    """Return True iff the existing Commons file is a true sibling at one of
    this item's own expected current titles.

    ``True`` means this is legitimate WITHIN-ITEM source duplication — the
    same bytes appear at more than one of this item's current asset
    positions. Under the SHA1-uniqueness constraint (PR C+D) we do NOT
    upload a second copy: the caller routes this to
    ``DriftResolution.MERGE_AND_REDIRECT`` — merge this ordinal's page
    number onto the sibling (canonical) file's SDC and leave a #REDIRECT at
    our intended title.

    ``False`` means the existing file is at a *different* title than any of
    this item's current asset positions (typically a legacy upload from a
    previous naming scheme, like a NARA-bot title from 2011).  Even when the
    SHA1 legitimately appears at multiple source positions, leaving the
    legacy title alone produces an orphan duplicate on Commons alongside our
    new ``(page N).ext`` uploads.  The caller should route through normal
    drift handling instead, which migrates the legacy title via Case 3.
    """
    return bool(
        duplicate_source_sha1s
        and sha1 in duplicate_source_sha1s
        and expected_item_titles
        and existing_file_title in expected_item_titles
    )


def select_upload_chunk_size(
    *,
    file_exists: bool,
    force_ignore_warnings: bool,
    file_size_bytes: int,
) -> tuple[int, bool]:
    """Pick the pywikibot ``chunk_size`` for an upload attempt.

    Returns ``(chunk_size, prefers_direct)``.  ``chunk_size`` is ``0`` for
    a direct whole-body POST or :data:`WMC_UPLOAD_CHUNK_SIZE` for the
    chunked stash-commit path.  ``prefers_direct`` reflects whether the
    caller's flags would have chosen direct — useful to pick the matching
    ``ignore_warnings`` value and to log a size-override decision.

    Either ``file_exists`` (target Commons page already has content we
    want to overwrite) or ``force_ignore_warnings`` (hash-drift path has
    accepted a duplicate SHA1) makes direct preferred, because direct
    suppresses MediaWiki warnings the stash-commit path can't.

    Above :data:`LARGE_FILE_DIRECT_UPLOAD_LIMIT_BYTES` the override fires:
    Wikimedia's API gateway rejects single-body uploads at that size, so
    the warning-bypass benefit is moot and we must chunk regardless.  A
    bounded failure (e.g. fileexists-shared-forbidden at commit) is
    strictly better than OOM-killing the whole run.
    """
    prefers_direct = file_exists or force_ignore_warnings
    must_chunk_for_size = file_size_bytes > LARGE_FILE_DIRECT_UPLOAD_LIMIT_BYTES
    if prefers_direct and not must_chunk_for_size:
        return 0, prefers_direct
    return WMC_UPLOAD_CHUNK_SIZE, prefers_direct


# Per-ordinal status strings written to <partner>/<dpla_id>/upload-result.json
# and consumed by the SDC sync phase (PR 4). The SDC phase only attempts
# wbsetclaims for ordinals whose status is UPLOADED or SKIPPED — the other
# three mean no canonical Commons file exists at the expected title for this
# ordinal in this run, so writing structured data would be pointing at the
# wrong page or none at all.
ORDINAL_UPLOADED = "UPLOADED"  # file just uploaded (or drift-moved into place)
# Existing Commons file present at the expected title. Usually its SHA1
# matches the source S3 SHA1 (the ordinary hash-match skip). One
# narrow exception: ``_detect_commons_dedup_skip`` also emits SKIPPED
# when Commons has a real (non-redirect) file whose SHA1 differs from
# ours — the byte-drift class the class docstring calls out. In both
# subcases the Commons file at the expected title is CORRECT (the
# byte-drift file was accepted by Commons on its original upload with
# server-side normalisation), so ``UPLOADED``/``SKIPPED`` remains the
# right signal to the SDC phase: a canonical Commons file exists at
# this ordinal's title, safe to target for wbsetclaims. Audit the
# byte-drift subcase via ``Result.UPLOAD_SKIPPED_COMMONS_DEDUP``.
ORDINAL_SKIPPED = "SKIPPED"
ORDINAL_NOT_PRESENT = "NOT_PRESENT"  # no S3 asset to upload (downloader gap)
ORDINAL_INELIGIBLE = "INELIGIBLE"  # S3 asset present but uploader chose not
# to upload (bad MIME, download-only, unguessable extension, etc.)
ORDINAL_FAILED = "FAILED"  # upload attempted, raised, did not land
# SHA1-uniqueness redesign (PR C+D). Two new terminal ordinal statuses, both
# meaning "no NEW canonical Commons file was uploaded at the intended title in
# this run" — so the SDC sync phase does NOT target the intended title (it
# treats only UPLOADED / SKIPPED as SDC-eligible).
#   HAND_FIX — our SHA1 is at a wrong title and the intended title is occupied
#     by a different file; the rename is blocked, recorded to hand-fix.jsonl.
#   MERGED — source-duplication SHA1 match; this item's SDC was merged onto the
#     canonical file inline and our intended title is now a #REDIRECT to it.
ORDINAL_HAND_FIX = "HAND_FIX"
ORDINAL_MERGED = "MERGED"

# A file is "community" — off-limits to our automated rename / SDC-merge /
# redirect / template-migration — only when BOTH signals say it isn't ours:
# its title lacks the DPLA/NARA naming shape AND its original uploader isn't
# one of our bots (the canonical ``DPLA_BOT_ACCOUNTS`` set). The AND is
# deliberate; see ``Uploader._is_community_file``.
_DPLA_TITLE_MARKERS = (" - DPLA - ", " - NARA - ")


def _has_dpla_shaped_title(title: str) -> bool:
    """True when a Commons title carries the DPLA or NARA naming shape."""
    return any(marker in title for marker in _DPLA_TITLE_MARKERS)


# Matches the MediaWiki redirect directive ``#REDIRECT [[Target]]``, ANCHORED
# to the start of the page (``\A``) where MediaWiki actually recognizes it —
# only leading whitespace may precede it. Anchoring means a stray ``#REDIRECT``
# inside a comment or template lower in the page can never be the match target.
# Allows whitespace before the link and an optional leading ``:`` in it (e.g.
# ``[[:File:Foo.jpg]]``); case-insensitive per MediaWiki's own ``#redirect``
# handling. Used to re-point ONLY the directive of an existing redirect page,
# preserving any ancillary wikitext (categories, templates) that sits below it.
_REDIRECT_LINE_RE = re.compile(r"\A\s*#REDIRECT\s*\[\[[^\]]*\]\]", re.IGNORECASE)


class DriftResolution(str, Enum):
    """The outcomes ``_resolve_hash_drift`` produces, one per
    invariant-restoring next step the caller takes.

    Values are the caller-visible string sentinels; ``str, Enum``
    subclassing means ``DriftResolution.MOVED == "moved"`` still
    holds, so any legacy comparison against the raw string keeps
    working. New comparisons should reference the enum member so a
    typo or rename is a hard error at import time rather than a
    silent no-match at runtime.

    See ``_resolve_hash_drift``'s docstring for the per-outcome story.

    Under the SHA1-uniqueness constraint (PR C+D) NONE of these
    outcomes results in an upload — our SHA1 is already on Commons, so
    a second byte-identical file is forbidden.
    """

    MOVED = "moved"
    # Legitimate source duplication (same bytes at multiple positions within an
    # item, or across items / institutions): merge this item's SDC onto the
    # earliest existing (canonical) file and leave a #REDIRECT at our title.
    MERGE_AND_REDIRECT = "merge_and_redirect"
    # Our SHA1 is at a wrong title and the intended title is occupied by a
    # DIFFERENT file (different SHA1) — the rename that would restore the
    # invariant is blocked and the bot must not pick a winner. Hand off to a
    # human via the hand-fix.jsonl sidecar; no upload.
    HAND_FIX = "hand_fix"
    ALREADY_CORRECT = "already_correct"


class UploadTimeoutError(RuntimeError):
    """Raised when a single file upload exceeds UPLOAD_TIMEOUT_SECS.

    Distinct from RuntimeError so it can escape process_file()'s catch-all
    and break the remaining-files loop in process_item() — no point attempting
    further pages when Commons' job queue is stuck.
    """


class NewFilePageBlocked(RuntimeError):
    """Raised by the no-create fence when an upload would create a File page
    that does not already exist on Commons.

    This is the safety backbone of "maintain" mode (in-place upkeep of
    already-uploaded files for institutions no longer authorized for new
    uploads): every existing maintenance action is idempotent and repairable,
    so the one invariant that must hold absolutely is that maintenance never
    emits a *new* File page. The fence enforces that at the single upload
    call site; this exception is caught per-ordinal in process_item() and
    recorded as ``UPLOAD_SKIPPED_WOULD_CREATE`` — never fatal, never retried.
    """


_TITLE_WHITESPACE_RE = re.compile(r"[\s_]+")


def _canonicalize_commons_title(title: str) -> str:
    """Return ``title`` under a light MediaWiki-title normalisation:
    strip leading/trailing whitespace/underscores and collapse runs
    of whitespace and underscores to a single space.

    MediaWiki treats ``_`` and space as equivalent in page titles
    (``File:X_Y`` and ``File:X Y`` are the same page). Callers doing
    Python-string equality between an uploader-constructed title and
    one returned from Commons must fold both forms — otherwise a
    same-page pair reads as drift and misroutes through
    ``_resolve_hash_drift`` (e.g. a phantom HAND_FIX) instead of the
    ``ALREADY_CORRECT`` skip it deserves.

    Also folds whitespace-run vs single-space differences (the
    ``get_page_title`` truncation-lands-on-whitespace edge case).

    Not exhaustive — Unicode NFC, namespace-first-letter
    capitalisation, and other server-side normalisations still live
    on the API side. Covers exactly the two classes that produce
    false-drift signals in the uploader's Python-side identity check.
    """
    return _TITLE_WHITESPACE_RE.sub(" ", title).strip()


class Uploader:
    """Per-ordinal uploader — the object that satisfies the upload
    invariant one Commons title at a time.

    Invariant contract (same as the module docstring): on any
    :meth:`process_file` success path, the Commons title
    ``get_page_title(dpla_id, …)`` produces holds the SHA1 of the
    item's S3 source bytes. Every code path below is chosen because
    it maintains that invariant. See ``docs/upload-invariant.md`` for
    the full statement, corollaries, anti-patterns, and past incidents.
    """

    def __init__(
        self,
        tracker: Tracker,
        local_fs: LocalFS,
        s3_client: S3Client,
        dpla: DPLA,
        site: BaseSite,
        category_ensurer: CategoryEnsurer | None = None,
        no_create: bool = False,
        sha1_lock_dir: str | None = None,
    ):
        self.tracker = tracker
        self.local_fs = local_fs
        self.s3_client = s3_client
        self.site = site
        self.dpla = dpla
        self.category_ensurer = category_ensurer
        # no_create: maintain-mode fence. When True, _safe_upload refuses to
        # write to a File page that does not already exist, so no run can
        # create a new Commons file. Defaults False (normal upload behaviour).
        self.no_create = no_create
        # sha1_lock_dir: directory for the per-SHA1 cross-process upload lock
        # (see ingest_wikimedia.sha1_lock). None disables it, which is the
        # default used by programmatic / unit-test construction (hermetic, no
        # filesystem writes). BOTH production entry points — the parallel pool
        # workers and the standalone CLI — pass SHA1_LOCK_DIR to enable it: the
        # lock is box-wide, so even a single-worker standalone run needs it to
        # serialize against a different partner's concurrent session.
        self.sha1_lock_dir = sha1_lock_dir

    def _detect_commons_dedup_skip(
        self,
        *,
        page_title: str,
        our_sha1: str,
        dpla_id: str,
        ordinal: int,
    ) -> dict | None:
        """Return an ``ORDINAL_SKIPPED`` result dict when the state at
        ``page_title`` matches the "Commons-dedup byte-drift" pattern;
        return ``None`` when it doesn't and the caller should keep its
        RuntimeError path.

        The pattern (from investigation of 109k historical events):
        Commons has a real file at our expected title whose SHA1
        differs from our S3 SHA1, typically by a single trailing byte.
        Commons then treats every subsequent re-upload as a
        "duplicate of current version" via a warning
        ``IGNORE_WIKIMEDIA_WARNINGS`` doesn't cover — pywikibot returns
        ``None`` from ``Site.upload()`` and the caller reaches here.
        Nothing on Commons needs repair; the file is at the right
        title with valid bytes, we just can't (and don't need to)
        replace them. Reporting these as ``FAILED`` inflates the
        counter and hides real drift-repair gaps in the same bucket.

        The detection is intentionally narrow: only a real
        (non-redirect) file at the exact expected title with a
        readable, different SHA1 qualifies. Any other state — target
        is a redirect, target doesn't exist, SHA1 unreadable — falls
        through to the caller's RuntimeError so genuine
        drift-repair gaps still surface as FAILED (which is
        actionable: rerun after fix, investigate manually, or add
        a new drift-resolution case).

        Increments :attr:`Result.SKIPPED` + the dedicated
        :attr:`Result.UPLOAD_SKIPPED_COMMONS_DEDUP` breakdown so the
        SDC phase still targets the ordinal (its ``UPLOADED`` /
        ``SKIPPED`` gate covers the correct file at the expected
        title regardless of which counter fired) while the
        summary keeps the byte-drift class audit-able.
        """
        try:
            existing = get_page(self.site, page_title)
            existing.exists()  # populate cache
        except Exception as ex:
            logging.warning(
                f"Commons-dedup detection failed for {dpla_id} {ordinal}: "
                f"could not fetch [[File:{page_title}]] ({ex!r}); "
                f"falling through to RuntimeError."
            )
            return None
        if not existing.exists() or existing.isRedirectPage():
            return None
        try:
            existing_sha1 = existing.latest_file_info.sha1
        except Exception:
            return None
        if not existing_sha1 or existing_sha1 == our_sha1:
            return None
        # Persist the pywikibot-normalized title, not the raw
        # constructed ``page_title`` — downstream sidecars / SDC-sync
        # key on the Commons-stored form, so returning the raw form
        # here would break the very equality checks elsewhere in the
        # pipeline that skip results feed into. Same normalization
        # ``_resolve_hash_drift``'s ALREADY_CORRECT branch does at
        # line 693 for the same reason.
        canonical_title = existing.title(with_ns=False)
        logging.info(
            f"Skipping {dpla_id} {ordinal}: Commons-dedup byte-drift — "
            f"target [[File:{canonical_title}]] already holds a real file "
            f"(SHA1 {existing_sha1}) that Commons treats as a "
            f"duplicate of our re-upload (our S3 SHA1 {our_sha1}). "
            f"File on Commons is correct; nothing to repair. See "
            f"``Result.UPLOAD_SKIPPED_COMMONS_DEDUP`` for the counter."
        )
        self.tracker.increment(Result.SKIPPED)
        self.tracker.increment(Result.UPLOAD_SKIPPED_COMMONS_DEDUP)
        return {
            "status": ORDINAL_SKIPPED,
            "title": canonical_title,
            "pageid": existing.pageid,
        }

    # Match Commons's ``fileexists-no-change`` message and extract the
    # namespaced title Commons named. The message pattern is stable:
    # ``The upload is an exact duplicate of the current version of
    # [[:File:<title>]]``. Captures the title without the leading
    # colon so it matches our ``page_title`` shape (``File:<title>``).
    _NOCHANGE_TITLE_RE = re.compile(
        r"exact duplicate of the current version of \[\[:(File:[^\]]+)\]\]"
    )

    def _detect_commons_dedup_from_nochange_error(
        self,
        ex: Exception,
        *,
        page_title: str,
        our_sha1: str,
        dpla_id: str,
        ordinal: int,
    ) -> dict | None:
        """When ``ex`` is Commons's ``fileexists-no-change`` response
        naming our intended target, treat it as a Commons-dedup
        byte-drift SKIP and return an ``ORDINAL_SKIPPED`` result;
        return ``None`` otherwise so the caller keeps the FAILED
        path.

        Semantic contract: Commons's ``fileexists-no-change`` message
        is the authoritative statement that our upload equals what
        Commons already stores at the named title, after any
        server-side normalisation (e.g., trailing ``\\r`` stripped
        from PDFs — verified byte-for-byte for the two files that
        originally motivated this treatment). When the named title
        matches our intended ``page_title``, the upload invariant is
        satisfied at the correct title. Trust Commons.

        Defensive title match: if Commons's message names a DIFFERENT
        title from our intended one (I can't construct that scenario
        from Commons's current message format, but the check is
        cheap), fall through to the FAILED path so we don't
        misclassify a real cross-title drift as a benign skip. Same
        defense-in-depth stance as ``_detect_commons_dedup_skip``'s
        redirect / missing / SHA1-mismatch guards.

        Reuses ``_detect_commons_dedup_skip`` for the actual result
        construction — same counter, same log shape, same pageid
        refresh — so the two upload paths (direct raise vs
        chunked-None-return) produce identical outcomes for the same
        underlying phenomenon.
        """
        if ERROR_NOCHANGE not in str(ex):
            return None
        m = self._NOCHANGE_TITLE_RE.search(str(ex))
        if m is None:
            return None
        commons_named_title = m.group(1)
        # ``page_title`` in this file is passed WITHOUT the ``File:`` prefix
        # elsewhere in ``process_file`` (see the ``get_page_title`` return
        # contract at line 581), so compare against the prefixed form.
        if (
            commons_named_title != f"File:{page_title}"
            and commons_named_title != page_title
        ):
            logging.warning(
                f"Commons ``fileexists-no-change`` for {dpla_id} {ordinal} "
                f"names title {commons_named_title!r} which does not match "
                f"our intended {page_title!r}; treating as FAILED (real "
                f"drift, not the byte-drift class)."
            )
            return None
        # Fetch the target to confirm state + populate the pageid for the
        # sidecar. Even though Commons's message is authoritative for the
        # invariant, downstream sdc-sync keys on the sidecar's pageid, so
        # we still need a live lookup. This is what
        # ``_detect_commons_dedup_skip`` already does — reuse it.
        return self._detect_commons_dedup_skip(
            page_title=page_title,
            our_sha1=our_sha1,
            dpla_id=dpla_id,
            ordinal=ordinal,
        )

    def _safe_upload(self, *, filepage, **kwargs):
        """Sole sanctioned wrapper around ``site.upload`` — every upload in
        this module MUST go through here so the no-create fence cannot be
        bypassed by adding a new call site.

        In no-create (maintain) mode, write only when the target is an existing
        real File page (an overwrite / new version); otherwise raise
        :class:`NewFilePageBlocked`. Two cases are blocked:

          * the title doesn't exist — a net-new upload; and
          * the title is a *redirect* — it holds no file of its own, so
            uploading there would create file content at a title that had
            none. ``FilePage.exists()`` returns True for a redirect (and
            pywikibot transparently follows it when reading properties), so
            the explicit ``isRedirectPage()`` guard is required — see the
            "Pywikibot ... transparently follows redirects" lesson. A genuine
            de-redirect belongs in the explicit move/redirect path, not here.

        Moving and editing existing pages are unaffected — only *creating* a
        new File page is fenced.
        """
        if self.no_create and (not filepage.exists() or filepage.isRedirectPage()):
            raise NewFilePageBlocked(filepage.title())
        return self.site.upload(filepage=filepage, **kwargs)

    def _refresh_pageid_with_retries(self, page_title: str) -> int | None:
        """Resolve the Commons pageid for ``page_title`` post-upload,
        retrying with bounded backoff to ride out the indexing-lag
        race that Commons exhibits on large (chunked) uploads.

        Returns the pageid on success, or ``None`` when the budget is
        exhausted without a real id (page still indexing, page genuinely
        doesn't exist, persistent API failure). Caller records
        ``pageid: None`` in the sidecar; ``sdc-sync``'s title→pageid
        fallback recovers from that state on the next run.

        Extracted from the inline loop in ``process_file`` so it can
        be exercised directly in tests (per CR feedback on PR #302):
        previously the test inlined the loop, which would silently
        pass while production diverged. Single source of truth here.

        See https://commons.wikimedia.org/wiki/File:Southern_Railway_Company,_Valuation_Section_22_-_DPLA_-_e314839e2ca3906b29bcbecc3d615740_(page_1).tiff
        for the live incident this retry exists to handle.
        """
        for attempt in range(1, PAGEID_REFRESH_MAX_ATTEMPTS + 1):
            try:
                fresh_page = get_page(self.site, page_title)
                fresh_page.exists()
                candidate = fresh_page.pageid
            except Exception as refresh_ex:
                if attempt < PAGEID_REFRESH_MAX_ATTEMPTS:
                    logging.warning(
                        f"Uploaded {page_title} but post-upload"
                        f" pageid refresh (attempt {attempt}/"
                        f"{PAGEID_REFRESH_MAX_ATTEMPTS}) raised:"
                        f" {refresh_ex!r}. Retrying after"
                        f" {PAGEID_REFRESH_BACKOFF_SECS}s."
                    )
                    time.sleep(PAGEID_REFRESH_BACKOFF_SECS)
                    continue
                logging.warning(
                    f"Uploaded {page_title} but post-upload"
                    f" pageid refresh raised on final attempt"
                    f" ({attempt}/{PAGEID_REFRESH_MAX_ATTEMPTS}):"
                    f" {refresh_ex!r}. Recording pageid=None;"
                    " sdc-sync's title→pageid fallback will"
                    " recover on next run."
                )
                return None
            if candidate:
                return candidate
            # Refresh returned but the pageid is still falsy
            # (typically 0, the indexing-lag shape). Retry before
            # giving up.
            if attempt < PAGEID_REFRESH_MAX_ATTEMPTS:
                logging.info(
                    f"Uploaded {page_title}: post-upload pageid"
                    f" refresh attempt {attempt}/"
                    f"{PAGEID_REFRESH_MAX_ATTEMPTS} returned"
                    f" {candidate!r}; retrying after"
                    f" {PAGEID_REFRESH_BACKOFF_SECS}s (indexing lag)."
                )
                time.sleep(PAGEID_REFRESH_BACKOFF_SECS)
            else:
                logging.warning(
                    f"Uploaded {page_title} but resolved pageid"
                    f" is still {candidate!r} after"
                    f" {PAGEID_REFRESH_MAX_ATTEMPTS} attempts;"
                    " recording pageid=None, sdc-sync's"
                    " title→pageid fallback will recover."
                )
        return None

    def _track_ordinal_skip(self, skip_kind: Result) -> None:
        """Bump both the aggregate ``Result.SKIPPED`` (which legacy
        Slack summaries and dashboards key on) and the granular
        ``skip_kind`` counter so operators can distinguish "upstream
        gap, no S3 asset" (``UPLOAD_SKIPPED_NOT_PRESENT``) from
        "S3 asset present but uploader chose not to upload"
        (``UPLOAD_SKIPPED_INELIGIBLE``). Previously the four ordinal
        skip sites all incremented ``Result.SKIPPED`` flat, leaving
        the breakdown unrecoverable from metrics."""
        self.tracker.increment(Result.SKIPPED)
        self.tracker.increment(skip_kind)

    def process_file(
        self,
        dpla_id: str,
        title: str,
        item_metadata: dict,
        provider: dict,
        data_provider: dict,
        ordinal: int,
        partner: str,
        page_label: str,
        verbose: bool,
        dry_run: bool,
        duplicate_source_sha1s: set[str] | None = None,
        expected_item_titles: set[str] | None = None,
        canonical_page_numbers: list[int] | None = None,
    ) -> dict:
        """Process one ordinal's source asset and return a per-ordinal result dict.

        **Invariant contract**: on ``UPLOADED`` or ``SKIPPED`` return, the
        Commons title ``get_page_title(dpla_id, …, page=page_label)``
        produces holds the SHA1 of ``s3_object.metadata['sha1']`` — the
        S3-staged source bytes for this DPLA item + ordinal. Every
        branch below is chosen because it maintains that invariant.
        See ``docs/upload-invariant.md``.

        Roadmap of the branches (each explains its invariant story
        at the branch site):

        1. **already-at-intended-title fast path**: SHA1 lookup finds
           the file at exactly the intended title. Invariant already
           satisfied. Return ``SKIPPED``.
        2. **hash-drift resolution** via :meth:`_resolve_hash_drift`:
           SHA1 lookup finds the file at some OTHER title. Dispatch to
           one of ``moved`` (Case 1/3 — move restores invariant),
           ``upload_and_tag`` (Case 2 orphan — upload restores it here,
           tag cleans up the wrong-title relic), ``leave_others_alone``
           (cross-item collision with a live sibling DPLA ID — upload
           restores it here, other title correctly holds ITS DPLA ID's
           SHA1 by corollary 1), or ``already_correct``
           (defense-in-depth normalization match — invariant already
           satisfied).
        3. **redirect handling** if the intended title is a redirect:
           move over the redirect (if target IS our SHA1's home for
           the same DPLA ID + logical page), or overwrite the redirect
           in place (cross-item / relic / no-DPLA-ID redirect — the
           redirect is a stale curatorial judgment per corollary 2
           and doesn't bind our invariant obligation). Upload proceeds
           and lands the S3 bytes at the intended title.
        4. **fresh upload** with the retry-and-drift-tag path when
           there's no existing state to reconcile.

        Return shape (consumed by process_item to assemble upload-result.json):
          {"status": <ORDINAL_*>, "title": str | None,
           "pageid": int | None, "error": str | None}

        The status drives the SDC sync phase: UPLOADED and SKIPPED
        ordinals are eligible for wbsetclaims; NOT_PRESENT, INELIGIBLE,
        HAND_FIX, MERGED, and FAILED ordinals are not (a MERGED ordinal
        already had its SDC merged onto the canonical file inline, and
        its intended title is now a redirect). ``title`` and ``pageid``
        are populated for UPLOADED, SKIPPED, and MERGED (the canonical
        file); everything else has no canonical Commons page to attach
        structured data to.
        """
        temp_file = self.local_fs.get_temp_file()
        # Held fd for the per-SHA1 cross-process upload lock (sha1_lock);
        # None until/unless the fresh-upload double-check acquires it, and
        # released in this method's finally regardless of how we exit.
        sha1_lock_fd = None

        try:
            wiki_markup = get_wiki_text(dpla_id, item_metadata, provider, data_provider)
            s3_path = self.s3_client.get_media_s3_path(dpla_id, ordinal, partner)
            upload_comment = f'Uploading DPLA ID "[[dpla:{dpla_id}|{dpla_id}]]".'
            if not self.s3_client.s3_file_exists(s3_path):
                logging.info(f"{dpla_id} {ordinal} not present.")
                self._track_ordinal_skip(Result.UPLOAD_SKIPPED_NOT_PRESENT)
                return {"status": ORDINAL_NOT_PRESENT}

            s3_object = self.s3_client.get_s3().Object(S3_BUCKET, s3_path)
            file_size = s3_object.content_length

            if file_size == 0:
                logging.info(f"Skipping {dpla_id} {ordinal}: File size is 0.")
                self._track_ordinal_skip(Result.UPLOAD_SKIPPED_NOT_PRESENT)
                return {"status": ORDINAL_NOT_PRESENT}

            sha1 = s3_object.metadata.get(CHECKSUM, "")
            mime = s3_object.content_type
            file_downloaded = False

            if mime in ("application/octet-stream", "binary/octet-stream"):
                s3_object.download_file(temp_file.name)
                file_downloaded = True
                detected = self.local_fs.get_content_type(temp_file.name)
                if detected not in ("application/octet-stream", "binary/octet-stream"):
                    action = "would update S3" if dry_run else "updating S3"
                    logging.info(
                        f"Re-detected {dpla_id} {ordinal}: {mime} -> {detected}; {action}"
                    )
                    if not dry_run:
                        self.s3_client.get_s3().meta.client.copy_object(
                            Bucket=S3_BUCKET,
                            Key=s3_path,
                            ContentType=detected,
                            Metadata=dict(s3_object.metadata),
                            MetadataDirective="REPLACE",
                            CopySource=S3_BUCKET + "/" + s3_path,
                        )
                    mime = detected
                else:
                    action = "would delete from S3" if dry_run else "deleting from S3"
                    logging.warning(
                        f"Skipping {dpla_id} {ordinal}: Unable to detect type beyond "
                        f"octet-stream; {action} so downloader can retry."
                    )
                    if not dry_run:
                        self.s3_client.get_s3().Object(S3_BUCKET, s3_path).delete()
                    self._track_ordinal_skip(Result.UPLOAD_SKIPPED_INELIGIBLE)
                    return {"status": ORDINAL_INELIGIBLE}

            if not check_content_type(mime):
                logging.info(f"Skipping {dpla_id} {ordinal}: Bad content type: {mime}")
                self._track_ordinal_skip(Result.UPLOAD_SKIPPED_INELIGIBLE)
                return {"status": ORDINAL_INELIGIBLE}

            if is_download_only(mime):
                logging.info(
                    f"Skipping {dpla_id} {ordinal}: {mime} staged for conversion, not uploaded."
                )
                self._track_ordinal_skip(Result.UPLOAD_SKIPPED_INELIGIBLE)
                return {"status": ORDINAL_INELIGIBLE}

            ext = mimetypes.guess_extension(mime)

            if not ext or ext == MIME_UNKNOWN_EXT:
                logging.info(
                    f"Skipping {dpla_id} {ordinal}: Unable to guess extension for {mime}"
                )
                self._track_ordinal_skip(Result.UPLOAD_SKIPPED_INELIGIBLE)
                return {"status": ORDINAL_INELIGIBLE}

            page_title = get_page_title(
                item_title=title,
                dpla_identifier=dpla_id,
                suffix=ext,
                page=page_label,
            )

            if verbose:
                logging.info(f"DPLA ID: {dpla_id}")
                logging.info(f"Title: {title}")
                logging.info(f"Page title: {page_title}")
                logging.info(f"Provider: {DPLA.provider_str(provider)}")
                logging.info(f"Data Provider: {DPLA.provider_str(data_provider)}")
                logging.info(f"MIME: {mime}")
                logging.info(f"Extension: {ext}")
                logging.info(f"File size: {file_size}")
                logging.info(f"SHA-1: {sha1}")
                logging.info(f"Upload comment: {upload_comment}")
                logging.info(f"Wikitext: \n {wiki_markup}")

            # Check whether this file's hash already exists on Commons.
            # If it's at the correct title, skip. If it's at a different title,
            # attempt drift correction before uploading.
            existing_file = find_file_by_hash(
                self.site, sha1, preferred_title=page_title
            )
            if existing_file is not None:
                if existing_file.title(with_ns=False) == page_title:
                    # Our bytes are already at the canonical title — invariant
                    # already satisfied, nothing to do.
                    logging.info(
                        f"Skipping {dpla_id} {ordinal}: Already exists on commons."
                    )
                    self.tracker.increment(Result.SKIPPED)
                    return {
                        "status": ORDINAL_SKIPPED,
                        "title": page_title,
                        "pageid": existing_file.pageid,
                    }
                logging.info(
                    f"Hash drift detected for {dpla_id} {ordinal}: "
                    f"SHA1 found at [[File:{existing_file.title(with_ns=False)}]]"
                )

            if not dry_run:
                # SHA1-uniqueness constraint (PR C+D): if our S3 SHA1 already
                # exists anywhere on Commons, we NEVER upload a second copy.
                # Resolve to exactly one non-upload outcome and return — no
                # branch below falls through to the download/upload path.
                # NOTE: this block is NOT gated on ``file_downloaded``. A file
                # already fetched by the octet-stream MIME re-detection path
                # above (file_downloaded=True) must still be collision-resolved
                # or uploaded here — the fresh-upload download below is the only
                # step that skips when the bytes are already on disk.
                #
                # Per-SHA1 cross-process guard (double-checked locking). If the
                # fast-path lookup found nothing, we're headed for a fresh
                # upload — but another worker/session processing byte-identical
                # media for a DIFFERENT item may upload our SHA1 first. Take the
                # per-SHA1 lock (striped flock; see ingest_wikimedia.sha1_lock)
                # and RE-CHECK: the race-loser then sees the winner's file and
                # resolves to a non-upload outcome in THIS pass instead of a
                # duplicate upload Commons rejects (which would cost a spurious
                # FAILED and defer the SDC merge to a later run). Different SHA1s
                # don't contend, so parallelism is preserved. The lock is held
                # through the upload commit and released in this method's
                # finally. Enabled at both prod entry points (the parallel pool
                # workers AND the standalone CLI — a standalone single-worker
                # partner run can still race a different partner's session
                # box-wide); disabled only for the default sha1_lock_dir=None
                # used by programmatic / unit-test construction. Skipped for a
                # falsy sha1 (missing checksum): a lock keyed on empty content
                # gives no per-content exclusion and would funnel every such
                # upload onto one bucket.
                if existing_file is None and self.sha1_lock_dir is not None and sha1:
                    try:
                        sha1_lock_fd = acquire_sha1_lock(sha1, self.sha1_lock_dir)
                    except Exception as lock_ex:  # noqa: BLE001 — best-effort lock
                        # The lock is an optimization, not a correctness
                        # dependency (Commons' force_ignore_warnings=False dedup
                        # is the invariant backstop), so a lock-infrastructure
                        # failure — e.g. a foreign-owned lock dir — must degrade
                        # to a lock-less upload, NOT fail the ordinal. Skip the
                        # re-check and proceed; sha1_lock_fd stays None so the
                        # finally release is a no-op.
                        logging.warning(
                            f"Per-SHA1 upload lock unavailable for {dpla_id} "
                            f"{ordinal} ({lock_ex}); proceeding without it "
                            f"(Commons dedup still guards the one-SHA1 invariant)."
                        )
                    else:
                        existing_file = find_file_by_hash(
                            self.site, sha1, preferred_title=page_title
                        )
                        if existing_file is not None:
                            recheck_title = existing_file.title(with_ns=False)
                            if recheck_title == page_title:
                                # A concurrent worker uploaded our bytes at our
                                # exact intended title — invariant satisfied.
                                logging.info(
                                    f"Skipping {dpla_id} {ordinal}: uploaded "
                                    f"concurrently by another worker (now at the "
                                    f"intended title)."
                                )
                                self.tracker.increment(Result.SKIPPED)
                                return {
                                    "status": ORDINAL_SKIPPED,
                                    "title": page_title,
                                    "pageid": existing_file.pageid,
                                }
                            # Concurrent upload landed at a different title — fall
                            # through to the collision block below.
                            logging.info(
                                f"Concurrent upload detected for {dpla_id} "
                                f"{ordinal}: SHA1 now at [[File:{recheck_title}]];"
                                f" resolving as collision."
                            )
                if existing_file is not None:
                    existing_title = existing_file.title(with_ns=False)
                    # Community files are off-limits to our automation: never
                    # rename, merge onto, redirect, or migrate a file that is
                    # BOTH non-DPLA/NARA-shaped in title AND not one of our
                    # bots' uploads. Hand it to a human (distinct reason) and
                    # stop — checked before any drift resolution so no MOVE /
                    # MERGE_AND_REDIRECT can touch a community file.
                    if self._is_community_file(existing_file):
                        return self._record_community_hand_fix_and_skip(
                            partner=partner,
                            dpla_id=dpla_id,
                            ordinal=ordinal,
                            our_sha1=sha1,
                            intended_title=page_title,
                            community_file=existing_file,
                        )
                    # Within-item source duplication: the same bytes sit at
                    # another of THIS item's current asset positions (its own
                    # sibling ordinal). Centralize on that sibling (canonical)
                    # file — merge this ordinal's page number onto its SDC and
                    # leave a #REDIRECT at our intended title. See
                    # is_dup_sha1_sibling_at_expected_title's docstring for why
                    # a legacy title with the same SHA1 does NOT take this path.
                    if is_dup_sha1_sibling_at_expected_title(
                        sha1=sha1,
                        existing_file_title=existing_title,
                        duplicate_source_sha1s=duplicate_source_sha1s,
                        expected_item_titles=expected_item_titles,
                    ):
                        logging.info(
                            f"Within-item duplicate SHA1 for {dpla_id} {ordinal}: "
                            f"canonical file at [[File:{existing_title}]]; merging "
                            f"SDC and redirecting [[File:{page_title}]] to it."
                        )
                        return self._merge_and_redirect(
                            canonical_file=existing_file,
                            intended_title=page_title,
                            dpla_id=dpla_id,
                            ordinal=ordinal,
                            partner=partner,
                            page_label=page_label,
                            within_item=True,
                            sha1=sha1,
                            canonical_page_numbers=canonical_page_numbers,
                        )

                    drift_action = self._resolve_hash_drift(
                        existing_file=existing_file,
                        page_title=page_title,
                        dpla_id=dpla_id,
                        ordinal=ordinal,
                        expected_item_titles=expected_item_titles,
                    )
                    if drift_action == DriftResolution.MOVED:
                        # Our SHA1 was renamed into the previously-empty (or
                        # redirect-to-self) intended title. The same file page
                        # now lives at page_title; existing_file.pageid is
                        # preserved by MediaWiki across moves.
                        self.tracker.increment(Result.UPLOADED)
                        return {
                            "status": ORDINAL_UPLOADED,
                            "title": page_title,
                            "pageid": existing_file.pageid,
                        }
                    if drift_action == DriftResolution.ALREADY_CORRECT:
                        # Phantom drift — the SHA1-lookup file IS the file at the
                        # intended title under whitespace normalisation. Persist
                        # the pywikibot-normalized title (downstream sidecars /
                        # SDC-sync key on the Commons-stored form, so the raw
                        # double-space form would break their equality checks).
                        canonical_title = existing_file.title(with_ns=False)
                        logging.info(
                            f"Skipping {dpla_id} {ordinal}: Already exists on "
                            f"commons (normalized identity)."
                        )
                        self.tracker.increment(Result.SKIPPED)
                        return {
                            "status": ORDINAL_SKIPPED,
                            "title": canonical_title,
                            "pageid": existing_file.pageid,
                        }
                    if drift_action == DriftResolution.MERGE_AND_REDIRECT:
                        # Source duplication resolved by the drift classifier:
                        # cross-item / cross-institution, OR a within-item
                        # sibling the primary short-circuit missed (its SHA1 was
                        # not in duplicate_source_sha1s). Derive within_item from
                        # the data — canonical at one of THIS item's expected
                        # titles is within-item — so that case still stamps its
                        # P304 page number instead of being mislabeled cross-item.
                        return self._merge_and_redirect(
                            canonical_file=existing_file,
                            intended_title=page_title,
                            dpla_id=dpla_id,
                            ordinal=ordinal,
                            partner=partner,
                            page_label=page_label,
                            within_item=bool(expected_item_titles)
                            and existing_title in expected_item_titles,
                            sha1=sha1,
                            canonical_page_numbers=canonical_page_numbers,
                        )
                    # DriftResolution.HAND_FIX (and any unforeseen sentinel):
                    # our SHA1 is at a wrong title and the intended title is
                    # occupied by a DIFFERENT file (different SHA1) — the bot
                    # cannot safely make the rename. Record for a human and
                    # stop; never upload a duplicate.
                    return self._record_hand_fix_and_skip(
                        partner=partner,
                        dpla_id=dpla_id,
                        ordinal=ordinal,
                        our_sha1=sha1,
                        intended_title=page_title,
                        our_current_file=existing_file,
                    )

                # existing_file is None — our SHA1 is NOT on Commons; this is a
                # genuine fresh upload. Any redirect at the intended title is
                # handled below (a pre-existing redirect does not bind us on the
                # fresh-upload path).
                #
                # Concurrency: reaching here means the find_file_by_hash
                # RE-CHECK under the per-SHA1 lock above (when enabled) still
                # found no match, so we hold the lock through this commit and a
                # concurrent same-SHA1 worker is serialized behind us. As a
                # second line of defense — and the sole guard when the lock is
                # disabled — this commit uses ``force_ignore_warnings=False``, a
                # stash-commit that surfaces rather than suppresses MediaWiki
                # warnings, so any commit that still races hits Commons's own
                # duplicate-SHA1 detection and does not publish a second file.
                force_ignore_warnings = False

                # Skip re-downloading when the octet-stream MIME re-detection
                # path above already fetched the bytes to temp_file
                # (file_downloaded); re-downloading would just refetch the same
                # object.
                if not file_downloaded:
                    with tqdm(
                        total=s3_object.content_length,
                        leave=False,
                        desc="S3 Download",
                        unit="B",
                        unit_scale=1024,
                        unit_divisor=True,
                        delay=2,
                        ncols=100,
                    ) as t:
                        s3_object.download_file(
                            temp_file.name,
                            Callback=lambda bytes_xfer: t.update(bytes_xfer),
                        )

                wiki_file_page = get_page(self.site, page_title)

                # Fresh-upload path (our SHA1 is not on Commons). If the
                # intended title is nonetheless a redirect, route through the
                # redirect-handler: uploading directly onto a redirect page
                # fails with `fileexists-shared-forbidden` (the API treats the
                # upload as creating a duplicate of the redirect's target). The
                # handler picks the right strategy (move, or overwrite-in-place
                # with the appropriate metadata preservation) based on the
                # redirect target, and sets `force_ignore_warnings=True` for the
                # subsequent upload. A pre-existing human/bot redirect does not
                # bind us — the intended title is where our bytes belong.
                if wiki_file_page.isRedirectPage():
                    target_title = wiki_file_page.getRedirectTarget().title(
                        with_ns=False
                    )
                    target_dpla_id = extract_dpla_id_from_commons_title(target_title)
                    is_relic = is_same_item_redirect_relic(
                        wiki_file_page.title(with_ns=False), target_title, dpla_id
                    )
                    # A move only makes sense when the redirect target carries
                    # this item's DPLA ID at the same logical page (title-text
                    # drift). For everything else — same-item different-page
                    # relics, cross-item dedups, or no-DPLA-ID legacy redirects
                    # — we overwrite the redirect in place so the upload lands
                    # at our intended title without touching the target.
                    if target_dpla_id == dpla_id and not is_relic:
                        try:
                            wiki_file_page = self._resolve_redirect_move(
                                wiki_file_page, dpla_id
                            )
                        except pywikibot.exceptions.ArticleExistsConflictError:
                            # Move is blocked because the redirect page itself
                            # has page history or structured data Commons won't
                            # let us overwrite via a move. Fall back to
                            # replacing the redirect text in-place.
                            logging.info(
                                f"Move blocked (ArticleExistsConflictError) for "
                                f"{dpla_id}; falling back to redirect-overwrite"
                            )
                            wiki_file_page, _ = self._resolve_redirect_overwrite(
                                wiki_file_page, dpla_id, wiki_markup
                            )
                            force_ignore_warnings = True
                    else:
                        # Same-item relic: target is a sibling page of this
                        # item — preserve its metadata, since license tags
                        # and categories carry meaning across pages of the
                        # same multi-page item.
                        # Cross-item / no-DPLA-ID: don't pull metadata from
                        # a foreign page; we'd inherit its Image-extracted
                        # link and unrelated categories.
                        preserve = is_relic
                        if is_relic:
                            logging.info(
                                f"Same-item redirect relic for {dpla_id}: "
                                f"[[File:{wiki_file_page.title(with_ns=False)}]] → "
                                f"[[File:{target_title}]]; overwriting without "
                                f"moving sibling page."
                            )
                        else:
                            # Cross-item OR no-DPLA-ID redirect. The
                            # intended title is a redirect either to
                            # another DPLA ID's canonical file, or to
                            # some unrelated Commons file. Either way,
                            # the redirect is a stale curatorial
                            # judgment about a partner-decided fact
                            # (corollary 2 of the upload invariant —
                            # see ``docs/upload-invariant.md``) and
                            # does NOT override our obligation to
                            # place OUR S3 SHA1 at OUR canonical title.
                            # Overwrite it in place, upload our bytes,
                            # invariant satisfied at our title.
                            #
                            # ANTI-PATTERN: do NOT add a "if the
                            # redirect target has our SHA1, skip our
                            # upload and honor the redirect" check
                            # here. That would leave our intended
                            # title as a redirect (i.e., without our
                            # required bytes) — direct invariant
                            # violation. The two-files-same-SHA1
                            # outcome is corollary 1 and is CORRECT.
                            # See the 2026-07-02 Palo Pinto incident.
                            logging.info(
                                f"Cross-item or no-DPLA redirect for {dpla_id}: "
                                f"[[File:{wiki_file_page.title(with_ns=False)}]] → "
                                f"[[File:{target_title}]]; overwriting redirect "
                                f"with fresh wikitext (invariant corollary 2: "
                                f"stale curatorial redirect does not bind our "
                                f"obligation to place S3 SHA1 at DPLA-ID title)."
                            )
                        wiki_file_page, _ = self._resolve_redirect_overwrite(
                            wiki_file_page,
                            dpla_id,
                            wiki_markup,
                            preserve_from_target=preserve,
                        )
                        force_ignore_warnings = True
                        # Intentionally NOT setting drift_old_filename in
                        # either of these paths: the target is either a
                        # valid sibling (same-item relic) or a foreign file
                        # (cross-item) and must not be tagged as a duplicate.

                # Direct vs. chunked upload decision — see
                # select_upload_chunk_size for the contract. The on-disk
                # temp-file size is authoritative (S3 content_length may have
                # been a stale stub before the re-download path fired).
                file_exists = (
                    wiki_file_page.exists() and not wiki_file_page.isRedirectPage()
                )
                temp_file_size = os.path.getsize(temp_file.name)
                chunk_size, prefers_direct = select_upload_chunk_size(
                    file_exists=file_exists,
                    force_ignore_warnings=force_ignore_warnings,
                    file_size_bytes=temp_file_size,
                )
                if prefers_direct and chunk_size != 0:
                    logging.info(
                        f"Forcing chunked upload for {dpla_id} {ordinal}: "
                        f"file size {temp_file_size:,} B exceeds direct-upload "
                        f"limit {LARGE_FILE_DIRECT_UPLOAD_LIMIT_BYTES:,} B "
                        f"(would have used chunk_size=0 for "
                        f"file_exists={file_exists}, "
                        f"force_ignore_warnings={force_ignore_warnings})."
                    )
                # ``ignore_warnings`` on ``site.upload()`` is union-typed: a
                # bool (``True`` = suppress every warning class) or a list
                # of warning codes (suppress only the listed classes).
                # Direct (non-chunked) uploads of files small enough to
                # fit under the warnings threshold are trusted — we
                # accept whatever Commons returns. Chunked uploads go
                # through the larger-file flow where some warning
                # classes are real (size limits, hash drift) — only
                # the vetted ``IGNORE_WIKIMEDIA_WARNINGS`` set is
                # suppressed. Mixed-type literal is intentional; the
                # explicit type annotation documents the union for
                # readers and silences the static-analysis flag.
                #
                # INVARIANT-LOAD-BEARING: the plain fresh-upload path (a
                # genuine new SHA1 at an unoccupied title) is NOT direct —
                # file_exists and force_ignore_warnings are both False, so
                # prefers_direct is False and this yields the LIST, not
                # ``True``. ``IGNORE_WIKIMEDIA_WARNINGS`` deliberately omits
                # ``'duplicate'`` (see its definition), so if a concurrent
                # worker/session races us and publishes our SHA1 first, our
                # commit surfaces the ``duplicate`` warning and Commons
                # rejects it — the one-SHA1 backstop the per-SHA1 lock and
                # this comment's caller rely on. ``True`` is reached only for
                # deliberate overwrites (file_exists byte-drift, or
                # force_ignore_warnings redirect handling), never the race.
                upload_warnings: bool | list[str] = (
                    True if prefers_direct else IGNORE_WIKIMEDIA_WARNINGS
                )

                result = None
                # Avoid the `with executor:` context manager — its __exit__ calls
                # shutdown(wait=True), which would block until pywikibot's stuck
                # polling thread exits on its own, defeating the timeout entirely.
                # Use try/finally to guarantee shutdown(wait=False) on all paths.
                executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                try:
                    # ``attempt`` is managed manually rather than via
                    # ``for attempt in range(...)`` so a CSRF-recovery
                    # continue can re-loop WITHOUT consuming an attempt
                    # slot — the recovered session deserves a fresh try
                    # of the same call. Only real per-ordinal errors
                    # (backend-fail retries, unrecoverable exceptions)
                    # advance ``attempt``.
                    attempt = 1
                    while attempt <= MAX_UPLOAD_RETRIES:
                        future = None
                        try:
                            future = executor.submit(
                                self._safe_upload,
                                filepage=wiki_file_page,
                                source_filename=temp_file.name,
                                comment=upload_comment,
                                text=wiki_markup,
                                ignore_warnings=upload_warnings,
                                asynchronous=True,
                                chunk_size=chunk_size,
                            )
                            try:
                                result = future.result(timeout=UPLOAD_TIMEOUT_SECS)
                            except concurrent.futures.TimeoutError:
                                self.tracker.increment(Result.FAILED)
                                # Note: the worker thread keeps running until
                                # pywikibot's own socket timeout fires (~11 min
                                # max). This is acceptable — UploadTimeoutError
                                # skips the remaining files for this item, so
                                # at most one orphaned thread exists per item.
                                raise UploadTimeoutError(
                                    f"Upload timed out after {UPLOAD_TIMEOUT_SECS // 3600}h "
                                    f"— Commons job queue likely stuck"
                                )
                            # Successful upload against the current session
                            # — reset the shared consecutive-recovery
                            # counter so long-running processes that
                            # legitimately hit N stale-then-refresh events
                            # over their lifetime don't trip the cap. Same
                            # semantics as ``with_csrf_recovery``.
                            reset_session_recoveries()
                            break
                        except Exception as ex:
                            is_backend_fail = ERROR_BACKEND_FAIL in str(ex)
                            is_csrf_error = is_csrf_token_error(ex)
                            if is_csrf_error:
                                # Session was invalidated — every subsequent
                                # upload will hit the same KeyError until
                                # we re-authenticate. Retrying the same call
                                # can't help; refresh the session instead.
                                # Coordinates with the shared csrf.py budget
                                # so a run that ALSO invokes non-uploader
                                # write paths (touch_institution_files, etc.)
                                # can't independently spend the cap — one
                                # process, one counter.
                                if session_recoveries_used() >= MAX_CSRF_RECOVERIES:
                                    raise CsrfRecoveryFailed(
                                        f"Commons session invalidated: CSRF"
                                        f" token still invalid after"
                                        f" {session_recoveries_used()}"
                                        f" recovery attempts; aborting run."
                                    ) from ex
                                logging.warning(
                                    "CSRF token invalidated (attempt"
                                    " %d/%d for %s ordinal %d); refreshing"
                                    " Commons session (recovery %d/%d)",
                                    attempt,
                                    MAX_UPLOAD_RETRIES,
                                    dpla_id,
                                    ordinal,
                                    session_recoveries_used() + 1,
                                    MAX_CSRF_RECOVERIES,
                                )
                                try:
                                    recover_commons_session(self.site)
                                except Exception as recover_ex:
                                    raise CsrfRecoveryFailed(
                                        f"Commons session recovery threw"
                                        f" ({recover_ex!r}); aborting rather"
                                        f" than looping unrecoverable auth"
                                        f" errors."
                                    ) from recover_ex
                                bump_session_recovery()
                                del future
                                gc.collect()
                                # Do NOT advance ``attempt``: the previous call
                                # failed on session state, not on anything
                                # ordinal-specific — the recovered session
                                # gets a fresh attempt slot.
                                continue
                            if is_backend_fail and attempt < MAX_UPLOAD_RETRIES:
                                delay = min(
                                    UPLOAD_RETRY_BASE_DELAY_SECS * (2 ** (attempt - 1)),
                                    UPLOAD_RETRY_MAX_DELAY_SECS,
                                ) + random.uniform(0, 1.0)
                                logging.warning(
                                    f"Transient upload error on attempt {attempt}/"
                                    f"{MAX_UPLOAD_RETRIES}, retrying in "
                                    f"{delay:.1f}s: {ex}"
                                )
                                # Release the failed Future before sleeping —
                                # its retained traceback frame holds pywikibot's
                                # request body buffer (the entire file for
                                # chunk_size=0). Without this, body buffers from
                                # each failed attempt pile up across the retry
                                # loop's sleep, plus pywikibot's inner retry
                                # loop's own accumulated allocations.
                                del future
                                gc.collect()
                                time.sleep(delay)
                                attempt += 1
                                continue
                            else:
                                # Don't increment FAILED here — the ``raise``
                                # propagates to the generic ``except Exception``
                                # at the end of process_file (line ~848), which
                                # owns the single counter increment for every
                                # non-timeout exception path. Incrementing here
                                # too would double-count the same ordinal.
                                raise
                finally:
                    executor.shutdown(wait=False)
                    # Final sweep — clears the last attempt's Future and any
                    # cycle-trapped pywikibot request/response objects so the
                    # next process_file iteration doesn't inherit them.
                    gc.collect()

                if not result:
                    # ``site.upload()`` returned ``None`` — pywikibot's
                    # signal that the upload response carried a warning
                    # class our ``ignore_warnings`` list didn't cover.
                    # Two very different situations reach here, and
                    # they need to be reported very differently:
                    #
                    # 1. **Commons-dedup byte-drift** (the observed
                    #    dominant class — 91k+ of the 109k historical
                    #    events, mostly Ohio PDFs). The target title
                    #    already holds a real file whose SHA1 does not
                    #    match ours (a subtle byte difference; the
                    #    file on Commons is one byte shorter than the
                    #    S3 asset, likely from server-side ingest
                    #    normalisation on the original upload).
                    #    Commons rejects our re-upload as "duplicate
                    #    of current version" via a warning that
                    #    IGNORE_WIKIMEDIA_WARNINGS doesn't include.
                    #    The file is CORRECT on Commons; nothing to
                    #    repair. Reporting as FAILED inflates the
                    #    counter and buries any real drift-repair
                    #    gaps in the same log stream. Skip cleanly
                    #    with a dedicated counter so operators can
                    #    audit the drift class independently.
                    # 2. **True unhandled drift** — target is still a
                    #    redirect, or doesn't exist, or has an
                    #    unexpected shape ``_resolve_hash_drift`` /
                    #    the redirect-handler didn't cover. This IS
                    #    a failure we need to see; keep the
                    #    RuntimeError path so it lands in FAILED
                    #    with a distinct log line (see
                    #    ``handle_upload_exception``'s message
                    #    mapping for the "possible ID drift" case).
                    dedup_skipped = self._detect_commons_dedup_skip(
                        page_title=page_title,
                        our_sha1=sha1,
                        dpla_id=dpla_id,
                        ordinal=ordinal,
                    )
                    if dedup_skipped is not None:
                        return dedup_skipped
                    raise RuntimeError(
                        "File linked to another page (possible ID drift)"
                    )

                logging.info(f"Uploaded to {wikimedia_url(page_title)}")
                self.tracker.increment(Result.UPLOADED)
                self.tracker.increment(Result.BYTES, file_size)
                # `wiki_file_page.pageid` is stale for net-new uploads:
                # the pre-upload existence check populated the cached
                # `_pageid = 0` (definitively, since the file did not
                # exist), and `site.upload()` does not invalidate that
                # cache. Reading it here records `pageid: 0` in
                # upload-result.json even though the file is now on
                # Commons with a real pageid — which downstream
                # sdc-sync resolves to the bogus mediaid "M0" and
                # raises. Construct a fresh FilePage (no cached
                # pageid) and force a populated load via .exists() so
                # the recorded pageid matches the API's assigned
                # value.
                #
                # Fail closed on the sidecar contract: if the refresh
                # throws or still returns a falsy pageid, record
                # `pageid: None` rather than the malformed `0`. The
                # upload itself genuinely succeeded — keep
                # ORDINAL_UPLOADED so future runs see the file as
                # already uploaded (returning ORDINAL_FAILED would
                # trigger a retry against a file that's already on
                # Commons, hitting the "file linked to another page"
                # drift detection). sdc-sync's `if not pageid` guard
                # (added in #252) cleanly skips None as a mapping
                # malformed record without raising.
                # Indexing-lag race: Commons accepts the upload (especially
                # for chunked uploads of files >100 MB) before its
                # search/categorylinks index has caught up. The fresh
                # ``FilePage.exists()`` query lands in that window and
                # returns without a real ``.pageid`` (or returns 0).
                # Live incident:
                # https://commons.wikimedia.org/wiki/File:Southern_Railway_Company,_Valuation_Section_22_-_DPLA_-_e314839e2ca3906b29bcbecc3d615740_(page_1).tiff
                # — a 327 MB TIFF whose first-ordinal pageid lookup
                # raced indexing and produced ``pageid: null`` in the
                # sidecar, cascading into a silent sdc-sync skip and
                # eventually a wiped-wikitext page.
                #
                # Retry the refresh with bounded backoff so the lookup
                # gets a few more shots before Commons has propagated.
                # Typical case (small/medium files): first attempt
                # succeeds, zero added latency. Large-file race case:
                # 1-2 retries usually close the window. Hard failure
                # (page genuinely deleted, API error, network down):
                # falls through to the existing pageid=None branch and
                # sdc-sync's title→pageid fallback picks up the slack
                # on the next run.
                resolved_pageid = self._refresh_pageid_with_retries(page_title)
                return {
                    "status": ORDINAL_UPLOADED,
                    "title": page_title,
                    "pageid": resolved_pageid,
                }

            # dry_run path falls through without uploading — flag as INELIGIBLE
            # for SDC purposes (no real Commons file was placed in this run).
            return {"status": ORDINAL_INELIGIBLE}

        except UploadTimeoutError:
            # TimeoutError already counted at the future.result() timeout site
            # (the only path that reports FAILED before falling through to
            # here). Re-raise so the item-level loop breaks and remaining
            # ordinals aren't attempted — a stuck Commons job queue won't
            # magically un-stick mid-item.
            raise
        except CsrfRecoveryFailed:
            # Session-level fatal — propagates to main() so the whole run
            # aborts. Not a per-ordinal FAILED (the auth state is broken
            # for every subsequent write, not just this ordinal).
            raise
        except Exception as ex:
            # Commons-dedup byte-drift, direct-upload path. Commons's
            # response ``fileexists-no-change: The upload is an exact
            # duplicate of the current version of [[:File:X]]`` is an
            # authoritative statement that our upload equals what
            # Commons already stores at title X, *after* Commons's
            # server-side normalization. When X matches our intended
            # title, the upload invariant is satisfied at the correct
            # title even though our S3 bytes and Commons's bytes may
            # differ pre-normalization (e.g., partner PDFs with a
            # trailing ``\r`` after ``%%EOF`` that Commons strips on
            # ingest — verified for both b2bc51b… and 8ac21ee786…
            # ord 2, byte-for-byte identical in the overlap). Skip
            # cleanly with the dedicated dedup counter so the
            # byte-drift class is audit-able independently of the
            # FAILED bucket, mirroring the None-return branch's
            # ``_detect_commons_dedup_skip`` treatment in the chunked-
            # upload path. See ``docs/upload-invariant.md`` for the
            # invariant amendment that admits Commons-normalization
            # equivalence.
            # ``page_title`` and ``sha1`` are populated after the pre-upload
            # setup but before the upload attempt; an exception raised BEFORE
            # they're computed (S3 read failure, provider lookup, etc.)
            # reaches this except block without them bound. Only run the
            # dedup check when both are in scope — matches the semantic
            # intent (we only care about the specific
            # ``fileexists-no-change`` shape emitted from the upload leg).
            _frame_locals = locals()
            if "page_title" in _frame_locals and "sha1" in _frame_locals:
                dedup_skipped = self._detect_commons_dedup_from_nochange_error(
                    ex,
                    page_title=_frame_locals["page_title"],
                    our_sha1=_frame_locals["sha1"],
                    dpla_id=dpla_id,
                    ordinal=ordinal,
                )
                if dedup_skipped is not None:
                    return dedup_skipped
            # Single source of truth for the per-ordinal FAILED counter on
            # every non-timeout, non-session-fatal exception path — includes
            # pywikibot API errors like a *recoverable* CSRF ``KeyError``
            # that we couldn't recover from within this attempt (post the
            # session-recovery branch in the retry loop above), backend-fail
            # retries exhausted, "file linked to another page" ID drift, and
            # anything else that reaches this catch.
            # Pre-fix, ``handle_upload_exception`` logged the ``Failed:
            # <reason>`` line but never bumped the counter, so a whole
            # class of upload failures were silently absent from
            # ``COUNTS: FAILED`` and the Slack summary (concretely: 22,700+
            # CSRF failures counted as 13 on the NARA Washington DC
            # general-records run).
            self.tracker.increment(Result.FAILED)
            self.handle_upload_exception(ex)
            return {"status": ORDINAL_FAILED, "error": str(ex)}

        finally:
            # Release the per-SHA1 upload lock (no-op if never acquired);
            # closing the fd releases the flock so a waiting same-SHA1 worker
            # can proceed and re-check against our just-committed file.
            release_sha1_lock(sha1_lock_fd)
            self.local_fs.clean_up_tmp_file(temp_file)

    def _resolve_redirect_move(
        self,
        wiki_file_page: pywikibot.FilePage,
        dpla_id: str,
    ) -> pywikibot.FilePage:
        """
        Title-text-drift correction: the redirect at our intended title points
        to the same item's file under a slightly different title. Move the
        target file to the intended title and post a CommonsDelinker request.

        **Invariant maintained**: on return, the S3 SHA1 for this
        ``dpla_id`` lives at ``get_page_title(dpla_id, …)``'s output.
        See ``docs/upload-invariant.md``.

        Caller must verify the redirect target carries the same DPLA ID and
        same logical page (i.e. not a same-item different-page relic, where
        moving would oscillate); see is_same_item_redirect_relic.

        Raises ArticleExistsConflictError if the move is blocked (e.g. the
        redirect page has history/structured data). Caller falls back to
        _resolve_redirect_overwrite in that case.
        """
        redirect_target = wiki_file_page.getRedirectTarget()
        old_filename = redirect_target.title(with_ns=False)
        new_filename = wiki_file_page.title(with_ns=False)
        reason = build_title_drift_move_reason(
            old_filename, new_filename, dpla_id, self.site.user()
        )
        # Gate before the move, while old_filename is still the live file
        # (see post_commonsdelinker_request docstring).
        needs_relink = file_has_inbound_usage(self.site, old_filename)
        logging.info(
            f"Title drift redirect detected — moving "
            f"[[File:{old_filename}]] → [[File:{new_filename}]]"
        )
        with_csrf_recovery(
            self.site,
            f"move {redirect_target.title()} → {wiki_file_page.title()}",
            lambda: redirect_target.move(
                wiki_file_page.title(),
                reason=reason,
                movetalk=False,
                noredirect=False,  # leave a redirect at the old title
            ),
        )
        if needs_relink:
            post_commonsdelinker_request(
                self.site, old_filename, new_filename, check_usage=False
            )
        else:
            logging.info(
                " -- No inbound usage for [[File:%s]]; skipping CommonsDelinker "
                "request (nothing to relink).",
                old_filename,
            )

        # Fresh FilePage for the now-real file page at the intended title
        return get_page(self.site, wiki_file_page.title())

    def _resolve_redirect_overwrite(
        self,
        wiki_file_page: pywikibot.FilePage,
        dpla_id: str,
        wiki_markup: str,
        preserve_from_target: bool = True,
    ) -> tuple[pywikibot.FilePage, str]:
        """
        Replace a redirect at our intended title with wikitext so the
        subsequent upload can land the new S3 file there.

        **Invariant maintained**: after the caller's subsequent upload
        completes, the S3 SHA1 for this ``dpla_id`` lives at
        ``get_page_title(dpla_id, …)``'s output. Corollary 2 of the
        upload invariant: pre-existing Commons redirects do not bind
        us — they are stale curatorial judgments about partner-decided
        facts and are safely overwritten so the invariant lands the
        S3 bytes at the DPLA-ID's canonical title. See
        ``docs/upload-invariant.md``.

        Works for *any* redirect target — same-item different-page relic,
        cross-item dedup (e.g. a 2022 human "redirecting to duplicate file"
        edit), or a no-DPLA-ID legacy title. The S3 sha1 must land at the
        intended title regardless of where the redirect points.

        When `preserve_from_target` is True (default), the redirect target's
        content is carried into the new page by :func:`rescue_wikitext`:
        node-swap the target's metadata wrapper for our fresh
        ``{{DPLA metadata}}`` and keep everything else verbatim (categories,
        image-note annotations, every community template). Callers should pass
        False when the target is a foreign DPLA item, since its categories and
        Image-extracted parent link don't apply to our page.

        Unlike the tag-duplicate path, this does *not* import the target's
        in-template community params to SDC: the redirect target is not
        deleted here (we overwrite our own title in place, pre-upload — there
        is no destination MediaInfo entity yet), so any in-template community
        value keeps living on the still-existing target page. Only the
        outside-template presentation is copied forward.

        Returns (updated_file_page, old_filename).
        """
        redirect_target = wiki_file_page.getRedirectTarget()
        old_filename = redirect_target.title(with_ns=False)
        logging.info(
            f"Replacing redirect at "
            f"[[File:{wiki_file_page.title(with_ns=False)}]] "
            f"(target [[File:{old_filename}]], "
            f"preserve_metadata={preserve_from_target})"
        )
        if preserve_from_target:
            new_text = rescue_wikitext(redirect_target.text or "", wiki_markup)
        else:
            new_text = wiki_markup
        wiki_file_page.text = new_text
        with_csrf_recovery(
            self.site,
            f"save {wiki_file_page.title()} (redirect-overwrite)",
            lambda: wiki_file_page.save(
                summary=(
                    f"Replacing redirect with DPLA metadata for title drift "
                    f"correction (DPLA ID [[dpla:{dpla_id}|{dpla_id}]])"
                ),
                minor=False,
            ),
        )
        wiki_file_page.clear_cache()
        return wiki_file_page, old_filename

    def _move_to_correct_title(
        self,
        existing_file: pywikibot.FilePage,
        intended_page: pywikibot.FilePage,
        dpla_id: str,
        case_label: str,
        post_commonsdelinker: bool = True,
    ) -> None:
        """Move existing_file to intended_page and post a CommonsDelinker request.

        The moved page's *description* is intentionally left untouched — the
        community-preserving template migration is done later by the post-SDC
        ``sdc-sync`` cleanup (see the NOTE in the body). This method only
        restores the title invariant (the S3 SHA1 now lives at the canonical
        title) and relinks inbound usage.

        post_commonsdelinker controls whether we ask CommonsDelinker to
        rewrite external references to actual_filename. Default True (the
        usual title-drift case where actual_filename's redirect will outlive
        this session). Callers pass False when they know actual_filename is
        a sibling slot that another ordinal in this same session will
        overwrite with different content — making the
        rewrite-to-intended_filename request invalid the moment the
        redirect is replaced.
        """
        actual_filename = existing_file.title(with_ns=False)
        intended_filename = intended_page.title(with_ns=False)
        reason = build_title_drift_move_reason(
            actual_filename, intended_filename, dpla_id, self.site.user()
        )
        # Gate before the move, while actual_filename is still the live file
        # (see post_commonsdelinker_request docstring).
        needs_relink = post_commonsdelinker and file_has_inbound_usage(
            self.site, actual_filename
        )
        logging.info(
            f"Title drift ({case_label}): moving "
            f"[[File:{actual_filename}]] → [[File:{intended_filename}]]"
        )
        with_csrf_recovery(
            self.site,
            f"move {existing_file.title()} → {intended_page.title()}",
            lambda: existing_file.move(
                intended_page.title(),
                reason=reason,
                movetalk=False,
                noredirect=False,
            ),
        )
        if needs_relink:
            post_commonsdelinker_request(
                self.site, actual_filename, intended_filename, check_usage=False
            )
        elif not post_commonsdelinker:
            logging.info(
                f"Suppressing CommonsDelinker request "
                f"[[File:{actual_filename}]] → [[File:{intended_filename}]]: "
                f"actual_filename is one of this item's current asset "
                f"positions and will be overwritten with different content "
                f"by a later ordinal in this session."
            )
        else:
            logging.info(
                " -- No inbound usage for [[File:%s]]; skipping CommonsDelinker "
                "request (nothing to relink).",
                actual_filename,
            )

        # NOTE: we deliberately do NOT rewrite the moved page's description
        # here. Title/ID drift is DPLA-caused, so the old wikitext's
        # DPLA-authored fields (id/url/title) legitimately differ from current —
        # but the page may ALSO carry genuine community contributions. Safely
        # distinguishing the two needs the revision-history *provenance* walk in
        # ``ingest_wikimedia.legacy_artwork.migrate_legacy_file``, which the
        # post-SDC ``sdc-sync`` cleanup (``_post_sdc_cleanup_for_page``) runs on
        # this file later in the same pipeline: community edits are imported to
        # SDC and only bot-authored (drifted) fields are replaced with canonical
        # data. A blunt overwrite here previously discarded community metadata
        # (e.g. ``{{Creator:...}}``); the move alone restores the title invariant.

    def _resolve_hash_drift(
        self,
        existing_file: pywikibot.FilePage,
        page_title: str,
        dpla_id: str,
        ordinal: int,
        expected_item_titles: set[str] | None = None,
    ) -> DriftResolution:
        """Classify (and, for the rename cases, perform) the resolution when
        our S3 source's SHA1 already lives on Commons at a different title
        than we intend to write.

        Under the SHA1-uniqueness constraint (PR C+D) our SHA1 is already on
        Commons, so NONE of these outcomes uploads a second copy. See
        ``docs/upload-invariant.md``. Return values name the case + the
        caller's next step:

        - ``MOVED`` — **title_text_drift**: the same content should simply
          live at the intended title, which is empty or a redirect to our
          OWN file. The file has been renamed here (side effect); caller
          records UPLOADED. One file, one SHA1, canonical title.

        - ``MERGE_AND_REDIRECT`` — **source duplication**: our SHA1 is
          another LIVE DPLA ID's canonical content (cross-item /
          cross-institution), or sits at one of THIS item's own current
          asset positions (within-item). The caller merges this item's SDC
          onto that canonical file and leaves a ``#REDIRECT`` at our
          intended title — centralizing the SHA1 to one file.

        - ``HAND_FIX`` — **rename blocked**: our SHA1 is at a wrong title and
          the intended title is occupied by a DIFFERENT file (different
          SHA1), or by a redirect to some third file, or the colliding
          file's DPLA ID could not be verified. The bot must neither upload
          a duplicate nor clobber the occupant, so the caller records the
          case to the hand-fix.jsonl sidecar for a human.

        - ``ALREADY_CORRECT`` — **normalized_identity**: the file the SHA1
          lookup returned IS the file at the intended title under
          whitespace-run normalization (a post-title-truncation artefact of
          ``get_page_title``). No drift to resolve. Caller records SKIPPED.
        """
        actual_filename = existing_file.title(with_ns=False)

        # Defense-in-depth: if pywikibot's normalized title for the file
        # that carries this SHA1 equals the intended title we're about
        # to upload to, there is no drift to resolve — MediaWiki
        # collapsed whatever difference our raw ``page_title`` had
        # (typically a whitespace-run artefact from title truncation,
        # see ``get_page_title``) and the file we found IS the file at
        # the canonical title. Return early rather than fall through to
        # Case 1/2/3, all of which are destructive in this state:
        # Case 3 would attempt a move-to-self; Case 1/2 would attempt an
        # upload + tag-as-duplicate against the same page (in practice
        # Commons rejects the upload with ``fileexists-no-change``, so
        # the tag path was never reached — but the ordinal was recorded
        # as FAILED instead of SKIPPED, inflating counters across 7,550
        # DPLA items in past runs).
        #
        # The upstream ``process_file`` identity check
        # (``existing_file.title(with_ns=False) == page_title``) SHOULD
        # catch this earlier, but that comparison is a raw Python
        # equality on the constructed vs. pywikibot-normalized strings —
        # any character MediaWiki normalises that our constructor
        # doesn't (or vice versa) leaks through. This guard closes the
        # remaining gap without depending on the constructor's
        # correctness for every future normalisation change on either
        # side.
        if _canonicalize_commons_title(actual_filename) == _canonicalize_commons_title(
            page_title
        ):
            logging.info(
                f"Hash drift for {dpla_id} {ordinal}: "
                f"[[File:{actual_filename}]] IS the intended title after "
                f"pywikibot normalisation; nothing to resolve."
            )
            return DriftResolution.ALREADY_CORRECT

        # Cross-item / cross-institution collision: the file at the wrong
        # title carries a DIFFERENT DPLA ID. If that ID is still a LIVE item,
        # our SHA1 is legitimate source duplication across DPLA items — merge
        # our SDC onto their (canonical) file and redirect our title. If the
        # ID no longer resolves (404) the file is an orphan; fall through to
        # the rename/hand-fix logic below to reclaim our title. Any other
        # verification error is conservative → HAND_FIX (never guess).
        #
        # Same-item collision (same DPLA ID, different title) — e.g. the
        # post-PR-#173 case where the page-suffix no longer matches the new
        # naming scheme — falls through to the rename/redirect logic below.
        existing_dpla_id = extract_dpla_id_from_commons_title(actual_filename)

        if existing_dpla_id and existing_dpla_id != dpla_id:
            try:
                other_item = self.dpla.get_item_metadata(existing_dpla_id)
            except Exception as ex:
                # 404 → the colliding ID no longer resolves, so its file is an
                # orphan: fall through and reclaim our title. Any other error
                # (timeout, 5xx, parse) leaves the collision unverified — route
                # to hand-fix rather than act on a transient blip.
                status = getattr(getattr(ex, "response", None), "status_code", None)
                if status == 404:
                    logging.info(
                        f"Hash drift for {dpla_id} {ordinal}: colliding "
                        f"DPLA item {existing_dpla_id} no longer exists "
                        f"(404); treating [[File:{actual_filename}]] as "
                        f"an orphan and reclaiming our title."
                    )
                    other_item = None
                else:
                    logging.warning(
                        f"Hash drift for {dpla_id} {ordinal}: failed to verify "
                        f"colliding DPLA item {existing_dpla_id}: {ex}; "
                        f"routing to hand-fix (cannot safely resolve)."
                    )
                    return DriftResolution.HAND_FIX
            if other_item:
                # Our SHA1 is another LIVE DPLA ID's canonical content. Under
                # the uniqueness constraint we do NOT upload a second file:
                # centralize on their (earliest) file — merge our item's SDC
                # onto it and redirect our intended title to it.
                logging.info(
                    f"Hash drift for {dpla_id} {ordinal}: "
                    f"[[File:{actual_filename}]] is live DPLA item "
                    f"{existing_dpla_id}'s canonical content; merging our SDC "
                    f"onto it and redirecting [[File:{page_title}]] "
                    f"(cross-item source duplication)."
                )
                return DriftResolution.MERGE_AND_REDIRECT

        intended_page = get_page(self.site, page_title)

        # Pre-compute the "actual_filename is a sibling slot" guard once;
        # used by Case 1 and Case 3 below to decide whether the post-move
        # CommonsDelinker request would survive long enough to be valid.
        # When actual_filename is one of this item's expected titles, a
        # later ordinal's iteration will overwrite the redirect at
        # actual_filename with different content. CommonsDelinker would
        # then rewrite external references away from a title that is no
        # longer a redirect — silently showing the wrong file to readers
        # who landed via the rewritten link. Skip the request in that
        # case; the move itself is still useful (it places the file at
        # its new canonical title cheaply), but external link rewrites
        # would be incorrect.
        sibling_slot = bool(
            expected_item_titles and actual_filename in expected_item_titles
        )

        if not intended_page.exists():
            # Case: title_text_drift_empty_intended (Case 3).
            # Nothing at the intended title — simple move restores the
            # invariant (our SHA1 now lives at get_page_title(dpla_id)).
            self._move_to_correct_title(
                existing_file,
                intended_page,
                dpla_id,
                "title_text_drift_empty_intended (Case 3)",
                post_commonsdelinker=not sibling_slot,
            )
            return DriftResolution.MOVED

        if intended_page.isRedirectPage():
            # Intended title is a redirect. If it points at OUR file (a
            # redirect-to-self relic from the same file living at both names),
            # move over it — the rename restores the invariant. If it points
            # elsewhere, renaming would collide with a third file's redirect;
            # the bot cannot safely resolve that — hand-fix.
            redirect_target = intended_page.getRedirectTarget()
            if redirect_target.title(with_ns=False) == actual_filename:
                self._move_to_correct_title(
                    existing_file,
                    intended_page,
                    dpla_id,
                    "title_text_drift_redirect_at_intended",
                    post_commonsdelinker=not sibling_slot,
                )
                return DriftResolution.MOVED
            logging.info(
                f"Hash drift for {dpla_id} {ordinal}: intended title "
                f"[[File:{intended_page.title(with_ns=False)}]] is a redirect to "
                f"[[File:{redirect_target.title(with_ns=False)}]], not the "
                f"location of our SHA1 ([[File:{actual_filename}]]); routing to "
                f"hand-fix."
            )
            return DriftResolution.HAND_FIX

        # Intended title holds a real file whose SHA1 differs from ours
        # (find_file_by_hash returned a DIFFERENT title, so the occupant is
        # not our SHA1).
        #
        # If our SHA1 lives at one of THIS item's own current asset positions,
        # this is within-item source duplication — the same bytes legitimately
        # appear at more than one position. Renaming would strand that sibling
        # ordinal, so instead centralize on it: merge our SDC and redirect our
        # intended title.
        if expected_item_titles and actual_filename in expected_item_titles:
            logging.info(
                f"Hash drift for {dpla_id} {ordinal}: our SHA1 lives at this "
                f"item's own asset position [[File:{actual_filename}]]; merging "
                f"SDC and redirecting [[File:{page_title}]] "
                f"(within-item source duplication)."
            )
            return DriftResolution.MERGE_AND_REDIRECT

        # Our SHA1 is at a wrong title and the intended title is occupied by a
        # DIFFERENT file. The bot must neither upload a duplicate nor clobber
        # the occupant, so it cannot make the rename — hand off to a human.
        logging.info(
            f"Hash drift for {dpla_id} {ordinal}: intended title "
            f"[[File:{intended_page.title(with_ns=False)}]] is occupied by a "
            f"DIFFERENT file and our SHA1 lives at [[File:{actual_filename}]]; "
            f"the rename is blocked — routing to hand-fix."
        )
        return DriftResolution.HAND_FIX

    def _merge_and_redirect(
        self,
        *,
        canonical_file: pywikibot.FilePage,
        intended_title: str,
        dpla_id: str,
        ordinal: int,
        partner: str,
        page_label: str,
        within_item: bool,
        sha1: str,
        canonical_page_numbers: list[int] | None = None,
    ) -> dict:
        """Rule #3 (source duplication): centralize our SHA1 onto the earliest
        existing (canonical) Commons file instead of uploading a second
        byte-identical copy, and redirect our intended title to it.

        (a) Merge THIS item's structured data onto the canonical file's
            MediaInfo entity (add-only; other contributors' statements are
            preserved). Within-item duplication stamps the canonical's COMPLETE
            page set (``canonical_page_numbers`` — every page position this
            SHA1 occupies in the item, computed up front) as P304 qualifiers,
            authoritatively and in one edit; cross-item passes none.
        (b) Leave a ``#REDIRECT`` at our intended title. A redirect carries no
            media, so the one-SHA1-one-file constraint holds.

        Step (a) runs FIRST and is a precondition for (b): a redirect is only
        written — and ``MERGED`` only reported — once the SDC merge lands. If
        the canonical file has no resolvable pageid, or the merge fails, we
        write NO redirect and return a retryable ``ORDINAL_FAILED`` so the whole
        MERGE_AND_REDIRECT re-runs cleanly next pass. This prevents stranding
        the item's data: a redirect hides our title and ``ORDINAL_MERGED``
        excludes the ordinal from the SDC-sync phase, so a redirect left over an
        unmerged item would make its metadata appear nowhere.

        Once the SDC has merged, the outcome is gated on whether the redirect
        was established (``_create_redirect_to_canonical``'s return value):

        - ``created`` / ``already`` → report ``ORDINAL_MERGED``. Our intended
          title now resolves to the canonical file. ``ORDINAL_MERGED`` is
          deliberately NOT in the SDC-sync UPLOADED/SKIPPED eligibility set — the
          SDC was merged inline onto the canonical file. The COUNTS tally is
          separate from that status: a real merge (redirect newly ``created`` OR
          the SDC merge wrote something) bumps ``UPLOAD_MERGED_TO_CANONICAL``; a
          pure no-op re-encounter (redirect ``already`` there AND idempotent SDC
          merge) bumps ``SKIPPED`` instead, matching the UPLOADED-vs-SKIPPED
          convention. The recorded status stays ``ORDINAL_MERGED`` either way.
        - ``blocked_real_file`` → the intended title holds a DIFFERENT real
          file, so the redirect could not be established and our SHA1 is NOT
          discoverable at the intended title. Do NOT report MERGED; record a
          ``rename_blocked`` HAND_FIX instead. The SDC merge already ran onto
          the canonical file (add-only / idempotent), so this is safe and
          re-runnable.
        - ``fenced`` → maintain-mode no-create fence declined to create the
          redirect page. Non-terminal skip (``UPLOAD_SKIPPED_WOULD_CREATE`` +
          INELIGIBLE), not MERGED — a later non-maintain run creates it.
        """
        canonical_title = canonical_file.title(with_ns=False)
        canonical_pageid = canonical_file.pageid
        # Merge the SDC FIRST and only centralize (redirect + terminal MERGED)
        # once it lands. Leaving a redirect while the contributing item's SDC is
        # NOT on the canonical file would strand that data: the redirect hides
        # our title and ORDINAL_MERGED excludes the ordinal from the SDC-sync
        # phase, so the metadata would never appear anywhere. When the merge
        # can't run, return a retryable ORDINAL_FAILED (no redirect written) so
        # a later run re-attempts the whole MERGE_AND_REDIRECT cleanly.
        if not canonical_pageid:
            logging.warning(
                f"Merge for {dpla_id} {ordinal}: canonical file "
                f"[[File:{canonical_title}]] has no resolvable pageid; cannot "
                f"merge SDC. Leaving the ordinal for retry (no redirect written)."
            )
            # Count the retryable failure: these branches return an
            # ORDINAL_FAILED result normally (not via process_file's except
            # block), so the tracker must be bumped here or COUNTS/Slack would
            # underreport merge failures — see the FAILED single-source-of-truth
            # note on process_file's handler.
            self.tracker.increment(Result.FAILED)
            return {
                "status": ORDINAL_FAILED,
                "title": None,
                "pageid": None,
                "error": (
                    f"canonical [[File:{canonical_title}]] has no resolvable "
                    f"pageid; SDC merge could not run"
                ),
            }
        merge_ok, sdc_changed = self._merge_sdc_onto_canonical(
            canonical_mediaid=f"M{canonical_pageid}",
            dpla_id=dpla_id,
            partner=partner,
            page_label=page_label,
            within_item=within_item,
            canonical_page_numbers=canonical_page_numbers,
        )
        if not merge_ok:
            logging.warning(
                f"Merge for {dpla_id} {ordinal}: SDC merge onto "
                f"[[File:{canonical_title}]] failed; leaving the ordinal for "
                f"retry (no redirect written)."
            )
            self.tracker.increment(Result.FAILED)
            return {
                "status": ORDINAL_FAILED,
                "title": None,
                "pageid": None,
                "error": (
                    f"SDC merge onto [[File:{canonical_title}]] failed; "
                    f"ordinal left for retry"
                ),
            }
        redirect_outcome = self._create_redirect_to_canonical(
            intended_title=intended_title,
            canonical_title=canonical_title,
            dpla_id=dpla_id,
            ordinal=ordinal,
        )
        if redirect_outcome == "blocked_real_file":
            # A DIFFERENT real file occupies the intended title — the redirect
            # was NOT established, so we must not claim MERGED. Hand it to a
            # human (rename_blocked). Our SHA1's canonical home is the canonical
            # file; the occupant sits at the intended title.
            return self._record_hand_fix_and_skip(
                partner=partner,
                dpla_id=dpla_id,
                ordinal=ordinal,
                our_sha1=sha1,
                intended_title=intended_title,
                our_current_file=canonical_file,
            )
        if redirect_outcome == "fenced":
            # maintain mode: the redirect page would be net-new and the
            # no-create fence declined it. Report a would-create skip so the
            # SDC phase does not target a page that isn't there.
            self._track_ordinal_skip(Result.UPLOAD_SKIPPED_WOULD_CREATE)
            return {
                "status": ORDINAL_INELIGIBLE,
                "title": None,
                "pageid": None,
                "error": "would create a redirect page (blocked in maintain mode)",
            }
        # redirect_outcome in ("created", "already"): the redirect is established
        # and the intended title resolves to the canonical file. Tally a real
        # MERGE vs a no-op SKIP (see docstring); the recorded status stays
        # ORDINAL_MERGED in BOTH cases — it is a redirect and MUST stay excluded
        # from sdc-sync eligibility (PR #410 / within-item P304 fix).
        if redirect_outcome == "created" or sdc_changed:
            self.tracker.increment(Result.UPLOAD_MERGED_TO_CANONICAL)
        else:
            self.tracker.increment(Result.SKIPPED)
        return {
            "status": ORDINAL_MERGED,
            "title": canonical_title,
            "pageid": canonical_pageid or None,
        }

    def _merge_sdc_onto_canonical(
        self,
        *,
        canonical_mediaid: str,
        dpla_id: str,
        partner: str,
        page_label: str,
        within_item: bool,
        canonical_page_numbers: list[int] | None = None,
    ) -> tuple[bool, bool]:
        """Read this item's staged ``sdc.json`` and merge it onto the canonical
        file's MediaInfo entity via ``sdc_sync.merge_item_onto_canonical``
        (already on ``main`` from PR A).

        Imported lazily: ``tools.sdc_sync`` carries heavy module-level state
        (the ``site`` handle the merge writes through) and is only needed on
        this rare path. We point its module ``site`` at our authenticated
        Commons site for the duration of the call.

        Returns ``(ok, changed)``. ``ok`` is ``True`` when the item's SDC was
        merged onto the canonical file, or when there is genuinely nothing to
        merge (no staged ``sdc.json`` — the item contributes no structured
        data); ``False`` on a hard failure (import / S3 read / JSON parse /
        merge error) so the caller can avoid reporting a terminal ``MERGED``
        for an ordinal whose data never landed, and retry it on a later run
        instead. ``changed`` is ``True`` only when the merge actually wrote to
        Commons (detected via the SDC write-counter delta), so the caller can
        distinguish a real merge from an idempotent no-op re-encounter.
        """
        try:
            from tools import sdc_sync
        except Exception as ex:  # pragma: no cover — defensive import guard
            logging.warning(
                f"Merge for {dpla_id}: could not import sdc_sync ({ex!r}); "
                f"SDC not merged."
            )
            return False, False
        try:
            sdc_raw = self.s3_client.get_sdc_json(partner, dpla_id)
        except Exception as ex:
            logging.warning(
                f"Merge for {dpla_id}: S3 read of sdc.json failed ({ex!r}); "
                f"SDC not merged."
            )
            return False, False
        if not sdc_raw:
            logging.info(f"Merge for {dpla_id}: no staged sdc.json; nothing to merge.")
            return True, False
        try:
            sdc_payload = json.loads(sdc_raw)
        except json.JSONDecodeError as ex:
            logging.warning(
                f"Merge for {dpla_id}: sdc.json failed to parse ({ex}); SDC not merged."
            )
            return False, False
        # Within-item duplication: stamp the canonical's COMPLETE page set —
        # every position this SHA1 occupies in the item, computed up front from
        # the source asset list — as P304 qualifiers, authoritatively and in one
        # edit. Passing only this ordinal's page would make the authoritative
        # amend STRIP the canonical's other pages (the bug this fixes). Fall
        # back to this ordinal's page only if the complete set wasn't supplied;
        # the SDC-sync phase re-stamps the full set from the recorded numbers
        # regardless. Cross-item / cross-institution: no page number.
        if within_item:
            # ``page_label`` is already a page-number string ("" for none); keep
            # it as-is (no int() — _normalize_pages/P304 store strings anyway, so
            # a non-numeric label can never raise here).
            page_numbers = canonical_page_numbers or (
                [page_label] if page_label else None
            )
        else:
            page_numbers = None
        # Snapshot the SDC write counter around the merge so we can tell a real
        # merge (something was written) from an idempotent no-op re-encounter
        # (everything already present → merge_item_onto_canonical writes
        # nothing). merge_item_onto_canonical → process_one_from_sdc bumps
        # these counters on every commit, so a post-minus-pre delta of zero
        # means the merge changed nothing on Commons this run.
        writes_before = sdc_sync._sdc_writes_total()
        try:
            # Pass our authenticated site through the call (merge_item_onto_canonical
            # installs it as sdc_sync's module ``site``) rather than mutating
            # sdc_sync.site from here.
            sdc_sync.merge_item_onto_canonical(
                canonical_mediaid,
                dpla_id,
                sdc_payload,
                page_numbers=page_numbers,
                commons_site=self.site,
            )
            changed = sdc_sync._sdc_writes_total() > writes_before
            logging.info(
                f"Merged SDC for {dpla_id} onto canonical {canonical_mediaid}"
                + (f" (pages {sorted(page_numbers)})" if page_numbers else "")
                + ("" if changed else " (no change)")
            )
            return True, changed
        except CsrfRecoveryFailed:
            raise
        except Exception as ex:
            logging.warning(
                f"Merge for {dpla_id}: merge_item_onto_canonical failed "
                f"({ex!r}); SDC not merged, ordinal left for retry next run."
            )
            return False, False

    def _create_redirect_to_canonical(
        self,
        *,
        intended_title: str,
        canonical_title: str,
        dpla_id: str,
        ordinal: int,
    ) -> str:
        """Leave a ``#REDIRECT [[File:<canonical>]]`` at our intended title.

        Idempotent: an intended title already redirecting to the canonical
        file is a no-op; a redirect pointing elsewhere is re-pointed. Refuses
        to clobber a real (non-redirect) file — that state is left for a human
        (logged) rather than overwriting content. Respects the maintain-mode
        no-create fence: a not-yet-existing intended title is not created in
        maintain mode.

        When re-pointing an EXISTING redirect page, replaces ONLY the redirect
        line (preserving any ancillary wikitext — categories, templates,
        comments — the page carries around it); a brand-new page gets the bare
        ``#REDIRECT`` text.

        Returns the outcome so the caller can decide whether the redirect was
        actually established:

        - ``"created"`` — the redirect was written (new page, or an existing
          redirect re-pointed).
        - ``"already"`` — the intended title already redirects to the canonical
          file; nothing to do.
        - ``"blocked_real_file"`` — a real (non-redirect) file occupies the
          intended title; left for a human, no redirect written.
        - ``"fenced"`` — maintain-mode no-create fence declined to create a
          net-new redirect page.
        """
        page = get_page(self.site, intended_title)
        canonical_redirect = f"#REDIRECT [[File:{canonical_title}]]"
        if page.exists():
            if not page.isRedirectPage():
                logging.warning(
                    f"Redirect for {dpla_id} {ordinal}: intended title "
                    f"[[File:{intended_title}]] holds a real file; refusing to "
                    f"overwrite it with a redirect to [[File:{canonical_title}]] "
                    f"— left for manual review."
                )
                return "blocked_real_file"
            try:
                current_target = page.getRedirectTarget().title(with_ns=False)
            except Exception:
                current_target = None
            if current_target == canonical_title:
                logging.info(
                    f"Redirect for {dpla_id} {ordinal}: "
                    f"[[File:{intended_title}]] already redirects to "
                    f"[[File:{canonical_title}]]; nothing to do."
                )
                return "already"
            # Existing redirect pointing elsewhere: re-point ONLY the redirect
            # line so any ancillary wikitext (categories, templates, comments)
            # survives. Fall back to the bare redirect if no redirect line is
            # found (an odd redirect page whose text we can't parse).
            existing_text = page.text or ""
            new_text, n_subs = _REDIRECT_LINE_RE.subn(
                canonical_redirect, existing_text, count=1
            )
            page.text = new_text if n_subs else canonical_redirect
        elif self.no_create:
            logging.info(
                f"maintain: not creating redirect [[File:{intended_title}]] → "
                f"[[File:{canonical_title}]] for {dpla_id} {ordinal} "
                f"(no-create fence)."
            )
            return "fenced"
        else:
            page.text = canonical_redirect
        with_csrf_recovery(
            self.site,
            f"save {page.title()} (redirect to canonical for SHA1 centralization)",
            lambda: page.save(
                summary=(
                    f"Redirecting to canonical file holding this SHA1 "
                    f"(DPLA ID [[dpla:{dpla_id}|{dpla_id}]])"
                ),
                minor=False,
            ),
        )
        logging.info(
            f"Redirected [[File:{intended_title}]] → [[File:{canonical_title}]] "
            f"(DPLA ID {dpla_id} ordinal {ordinal})."
        )
        return "created"

    def _is_community_file(self, file_page: pywikibot.FilePage) -> bool:
        """A file is 'community' — off-limits to our automated rename, SDC
        merge, redirect, or template migration — only when BOTH signals say it
        isn't ours: (1) its title lacks the DPLA/NARA shape (``- DPLA -`` /
        ``- NARA -``), AND (2) its ORIGINAL uploader isn't one of our bots
        (DPLA bot / US National Archives bot).

        The AND is deliberate, and trusting title-shape alone (the early
        return below) is intentional — NOT a missing provenance check:

        * A bot upload with a malformed title (shape fails but uploader is
          ours) is still ours to fix.
        * A DPLA/NARA-shaped title is treated as ours even when the recorded
          uploader is a real person, for two reasons. (a) Such files
          legitimately exist with a non-bot uploader on record — e.g. test
          pages uploaded from a maintainer's account, or files renamed in from
          a community member before this rule existed — so the shape is the
          more reliable "ours" signal there. (b) More fundamentally, a
          DPLA-shaped title OCCUPIES a title reserved for our content: it is
          blocking the exact name our upload would take, and no community user
          has a legitimate expectation of holding that name. Handing it off
          would strand our own file behind someone else's, so we always act on
          it regardless of uploader.

        Only a file that looks non-ours on BOTH axes is handed to a human.
        When the uploader can't be read, err toward community (hands-off).

        (Considered and rejected on PR #408: making uploader-provenance
        override title shape. Because the bot allowlist is a small fixed set,
        that would flip every DPLA/NARA-shaped file whose oldest uploader isn't
        an exact allowlisted account into community-hand-off, stranding our own
        corpus — the mis-classification this AND-contract exists to prevent.)"""
        if _has_dpla_shaped_title(file_page.title(with_ns=False)):
            return False
        try:
            uploader = first_uploader(file_page)
        except Exception:  # noqa: BLE001 — unreadable history → treat as community
            uploader = None
        if uploader is None:
            return True
        # Compare against the canonical bot allowlist with the same
        # normalization the provenance logic uses (``DPLA_bot`` == ``DPLA bot``).
        bot_accounts = {_normalize_account(a) for a in DPLA_BOT_ACCOUNTS}
        return _normalize_account(uploader) not in bot_accounts

    def _record_community_hand_fix_and_skip(
        self,
        *,
        partner: str,
        dpla_id: str,
        ordinal: int,
        our_sha1: str,
        intended_title: str,
        community_file: pywikibot.FilePage,
    ) -> dict:
        """Our S3 SHA1 matches a COMMUNITY-uploaded file (see
        :meth:`_is_community_file`). We never rename, merge onto, redirect, or
        migrate a community file — record it for human review with the distinct
        ``community_file`` reason and skip (no upload). Best-effort sidecar
        write; the ordinal is counted as HAND_FIX regardless."""
        community_title = community_file.title(with_ns=False)
        try:
            uploader = first_uploader(community_file)
        except Exception:  # noqa: BLE001
            uploader = None
        try:
            community_sha1 = community_file.latest_file_info.sha1
        except Exception:  # noqa: BLE001
            community_sha1 = None
        logging.warning(
            f"HAND-FIX (community) for {dpla_id} {ordinal}: our SHA1 "
            f"({our_sha1}) matches community-uploaded "
            f"[[File:{community_title}]] (uploader {uploader!r}); not "
            f"renaming/merging/migrating a community file — recorded to the "
            f"hand-fix sidecar."
        )
        hand_fix_sidecar.record_hand_fix(
            partner,
            dpla_id=dpla_id,
            ordinal=ordinal,
            our_sha1=our_sha1,
            intended_title=intended_title,
            occupying_title=community_title,
            occupying_sha1=community_sha1,
            reason="community_file",
            community_uploader=uploader,
        )
        self.tracker.increment(Result.UPLOAD_HAND_FIX)
        return {"status": ORDINAL_HAND_FIX, "title": None, "pageid": None}

    def _record_hand_fix_and_skip(
        self,
        *,
        partner: str,
        dpla_id: str,
        ordinal: int,
        our_sha1: str,
        intended_title: str,
        our_current_file: pywikibot.FilePage,
    ) -> dict:
        """Record a rename-blocked (HAND_FIX) ordinal to the per-partner
        ``hand-fix.jsonl`` sidecar and return an ``ORDINAL_HAND_FIX`` result.

        Our S3 SHA1 lives at a wrong Commons title (``our_current_file``) and
        the intended title is occupied by a DIFFERENT file, so the bot can
        neither upload a duplicate nor clobber the occupant. A human resolves
        it. Best-effort: a sidecar write failure never blocks the run (the
        ordinal is counted and the case re-detects on a later run).
        """
        occupying_title: str | None = None
        occupying_sha1: str | None = None
        try:
            occupying = get_page(self.site, intended_title)
            if occupying.exists():
                occupying_title = occupying.title(with_ns=False)
                if not occupying.isRedirectPage():
                    try:
                        occupying_sha1 = occupying.latest_file_info.sha1
                    except Exception:
                        occupying_sha1 = None
        except Exception as ex:
            logging.warning(
                f"Hand-fix for {dpla_id} {ordinal}: could not inspect occupant "
                f"at [[File:{intended_title}]]: {ex!r}"
            )
        our_current_title = our_current_file.title(with_ns=False)
        logging.warning(
            f"HAND-FIX required for {dpla_id} {ordinal}: our SHA1 ({our_sha1}) "
            f"lives at [[File:{our_current_title}]] but intended title "
            f"[[File:{intended_title}]] is occupied by a different file "
            f"([[File:{occupying_title}]] SHA1 {occupying_sha1}); recorded to "
            f"the hand-fix sidecar."
        )
        hand_fix_sidecar.record_hand_fix(
            partner,
            dpla_id=dpla_id,
            ordinal=ordinal,
            our_sha1=our_sha1,
            intended_title=intended_title,
            occupying_title=occupying_title,
            occupying_sha1=occupying_sha1,
            reason="rename_blocked",
            current_title=our_current_title,
        )
        self.tracker.increment(Result.UPLOAD_HAND_FIX)
        return {"status": ORDINAL_HAND_FIX, "title": None, "pageid": None}

    def _persist_upload_result(
        self,
        partner: str,
        dpla_id: str,
        ordinal_results: dict[str, dict],
        dry_run: bool,
    ) -> None:
        """Write the per-item upload-result.json sidecar to S3.

        Called at every non-exception exit path through process_item so the
        sidecar always reflects what this run decided about this item — never
        a stale verdict from a previous run. An empty ordinal_results dict is
        the correct signal for "uploader saw the item but produced nothing
        the SDC phase should write structured data on" (ineligible item,
        missing institution Wikidata, zero-file manifest, etc.).

        Dry-run path is intentionally a no-op: dry runs shouldn't mutate S3
        any more than they mutate Commons.

        Best-effort on the write itself: a failure here is logged but doesn't
        propagate, since the upload work has already succeeded for any
        ordinals that were processed and the SDC phase can re-derive on a
        future uploader run.
        """
        if dry_run:
            return
        upload_result = {
            "run_at": datetime.datetime.now(datetime.timezone.utc).isoformat(
                timespec="seconds"
            ),
            "ordinals": ordinal_results,
        }
        try:
            self.s3_client.write_upload_result(partner, dpla_id, upload_result)
        except Exception as ex:
            logging.warning(
                f"Failed to write upload-result.json for {dpla_id}: {ex}; continuing"
            )

    def process_item(
        self,
        dpla_id: str,
        providers_json: dict,
        partner: str,
        verbose: bool,
        dry_run: bool,
    ):
        try:
            logging.info(f"DPLA ID: {dpla_id}")

            item_metadata_result = self.s3_client.get_item_metadata(partner, dpla_id)
            if not item_metadata_result:
                # Missing dpla-map.json. The item is in this run's IDs CSV but
                # there's no metadata to work from — either get-ids-es never
                # staged it, the object was deleted between phases, or this is
                # a transient S3 hiccup. Persist an empty result so the SDC
                # phase doesn't keep treating a prior run's UPLOADED ordinals
                # as authoritative for an item we now lack metadata for.
                self.tracker.increment(Result.ITEM_NOT_PRESENT)
                self._persist_upload_result(partner, dpla_id, {}, dry_run)
                return

            item_metadata = json.loads(item_metadata_result)

            provider, data_provider = self.dpla.get_provider_and_data_provider(
                item_metadata, providers_json
            )

            if not self.dpla.is_wiki_eligible(
                dpla_id, item_metadata, provider, data_provider
            ):
                logging.info(f"Skipping {dpla_id}: Not eligible.")
                self.tracker.increment(Result.SKIPPED)
                # Persist an empty result so any prior upload-result.json
                # doesn't keep telling the SDC phase the item is still SDC-able
                # after eligibility was revoked.
                self._persist_upload_result(partner, dpla_id, {}, dry_run)
                return

            if self.category_ensurer:
                institution_name = get_str(
                    get_dict(item_metadata, DATA_PROVIDER_FIELD_NAME), EDM_AGENT_NAME
                )
                institution_qid = get_str(data_provider, WIKIDATA_FIELD_NAME)
                hub_institution_qid = get_str(provider, WIKIDATA_FIELD_NAME)
                if institution_qid and hub_institution_qid:
                    self.category_ensurer.ensure(
                        institution_qid, institution_name, hub_institution_qid
                    )
                else:
                    logging.warning(
                        f"Skipping {dpla_id}: "
                        f"missing institution_qid={institution_qid!r} or "
                        f"hub_institution_qid={hub_institution_qid!r}"
                    )
                    self.tracker.increment(Result.SKIPPED)
                    self._persist_upload_result(partner, dpla_id, {}, dry_run)
                    return

            titles = get_list(
                get_dict(item_metadata, SOURCE_RESOURCE_FIELD_NAME),
                DC_TITLE_FIELD_NAME,
            )

            title = titles[0] if titles else ""

            files = self.s3_client.get_file_list(partner, dpla_id)

            # Single S3 pre-scan (one HEAD per ordinal) over the shared helper,
            # so the verifier can reconstruct the same page-label assignments
            # without duplicating the logic. The extension/page-label accounting
            # and the SHA1 snapshot come from the SAME HEAD per object, so they
            # can never diverge — a within-item duplicate that is detected always
            # has its page unioned onto the canonical (see prescan_ordinals).
            ordinal_exts, page_labels, sha1_by_ordinal = prescan_ordinals(
                self.s3_client, dpla_id, partner, len(files)
            )

            # SHA1s that legitimately appear at MORE THAN ONE position in the
            # source asset list. process_file uses this to short-circuit drift
            # correction when its SHA1 is in the set — the existing Commons file
            # at another title is a valid sibling, not a drift artefact, and both
            # positions should remain as separate Commons pages.
            duplicate_source_sha1s = collect_duplicate_source_sha1s(sha1_by_ordinal)

            # The COMPLETE P304 page set for each ordinal's file, from the item's
            # own SHA1 grouping. A within-item duplicate and its canonical share
            # the same SHA1 and therefore the same full page list, so whichever
            # ordinal lands the canonical stamps EVERY page position
            # authoritatively — and each ordinal's list is recorded in
            # upload-result.json so the SDC-sync phase re-stamps from the same
            # source of truth rather than re-deriving it per-ordinal.
            page_numbers_by_ordinal = compute_sha1_page_numbers(
                sha1_by_ordinal, page_labels
            )

            # Build the set of Commons titles this item's current asset list
            # will produce.  _resolve_hash_drift uses this to recognise when
            # an "old title" found via SHA1 lookup is actually one of THIS
            # run's other ordinals (a soon-to-be-overwritten sibling, not a
            # trailing orphan) and skip the duplicate-tagging in that case.
            # Without this, Case 2 produces a cascade of wrong tags whenever
            # source content has shifted within a multi-page item.
            expected_item_titles: set[str] = set()
            for ord_n, ext in ordinal_exts.items():
                if not ext:
                    continue
                expected_item_titles.add(
                    get_page_title(
                        item_title=title,
                        dpla_identifier=dpla_id,
                        suffix=ext,
                        page=page_labels.get(ord_n, ""),
                    )
                )

            # Per-ordinal results collected here are written to
            # <partner>/<dpla_id>/upload-result.json at the end of this method,
            # and read by the SDC sync phase to decide which ordinals are
            # eligible for wbsetclaims. Schema: {ordinal_str: {status, title?,
            # pageid?, error?}}.
            ordinal_results: dict[str, dict] = {}

            for ordinal, _ in enumerate(
                tqdm(
                    files, desc="Uploading Files", leave=False, unit="File", ncols=100
                ),
                start=1,
            ):
                logging.info(f"Page {ordinal}")
                page_label = page_labels.get(ordinal, "")
                # This ordinal's COMPLETE P304 page set (every page its SHA1
                # occupies in the item). Passed to the merge path AND recorded so
                # the SDC-sync phase stamps it authoritatively from this single
                # source of truth (within-item canonical → all its pages; normal
                # file → its one page; singleton → []).
                ordinal_pages = page_numbers_by_ordinal.get(ordinal, [])
                try:
                    result = self.process_file(
                        dpla_id,
                        title,
                        item_metadata,
                        provider,
                        data_provider,
                        ordinal,
                        partner,
                        page_label,
                        verbose,
                        dry_run,
                        duplicate_source_sha1s=duplicate_source_sha1s,
                        expected_item_titles=expected_item_titles,
                        canonical_page_numbers=ordinal_pages,
                    )
                    # process_file always returns a dict, but guard defensively:
                    # a NoneType here would crash the whole item's upload loop.
                    if isinstance(result, dict):
                        result["page_numbers"] = ordinal_pages
                    ordinal_results[str(ordinal)] = result
                except UploadTimeoutError as ex:
                    ordinal_results[str(ordinal)] = {
                        "status": ORDINAL_FAILED,
                        "error": str(ex),
                        # Record on failure paths too so the SDC-sync reader's
                        # "field absent = legacy sidecar" signal stays reliable:
                        # a discovery-rescued failed ordinal then uses this
                        # recorded set instead of the positional fallback.
                        "page_numbers": ordinal_pages,
                    }
                    self.handle_upload_exception(ex)
                    break
                except NewFilePageBlocked as ex:
                    # Maintain-mode fence tripped: this ordinal would have
                    # created a new File page. Record a would-create skip
                    # (never fatal, never retried) and move on — other ordinals
                    # of the same item may legitimately already exist. Status
                    # INELIGIBLE keeps SDC sync from targeting a page that
                    # isn't there.
                    logging.info(
                        f"maintain: blocked net-new upload for {dpla_id} "
                        f"ordinal {ordinal} ({ex})"
                    )
                    self._track_ordinal_skip(Result.UPLOAD_SKIPPED_WOULD_CREATE)
                    ordinal_results[str(ordinal)] = {
                        "status": ORDINAL_INELIGIBLE,
                        "title": None,
                        "pageid": None,
                        "error": "would create a new File page (blocked in maintain mode)",
                        # See the UploadTimeoutError handler: record here too so a
                        # later discovery-rescue of this ordinal reads the complete
                        # set rather than the positional fallback.
                        "page_numbers": ordinal_pages,
                    }
                    continue

            # After the per-asset loop, audit for "trailing-page orphan"
            # Commons files for this item — pages whose ordinal exceeds the
            # current source asset count for that extension. These are
            # invisible to process_file (it only iterates the current asset
            # list). Under the SHA1-uniqueness redesign this is a log-only
            # audit: it no longer emits {{Duplicate}} tags. Wrap separately so
            # a check failure isn't charged as FAILED against the item — the
            # per-asset uploads have already succeeded at this point.
            try:
                _post_item_orphan_check(
                    self.site,
                    self.s3_client,
                    self.tracker,
                    dpla_id,
                    title,
                    partner,
                    ordinal_exts,
                    page_labels,
                    dry_run,
                )
            except CsrfRecoveryFailed:
                raise
            except Exception as ex:
                logging.warning(f"Orphan check failed for {dpla_id}: {ex}; continuing")

            # Persist the per-ordinal results so the SDC sync phase knows which
            # ordinals to attempt structured-data writes on. Fires even when
            # ordinal_results is empty (e.g. zero files in file_list.txt) so a
            # previous run's results don't get treated as the current truth.
            self._persist_upload_result(partner, dpla_id, ordinal_results, dry_run)

        except CsrfRecoveryFailed:
            # Session-level fatal — propagates to main() so the run
            # aborts. Explicit re-raise ordering: the generic catch
            # below is for per-item transient errors and would swallow
            # this into a FAILED bump.
            raise
        except Exception as ex:
            # Intentionally NOT writing upload-result.json on the catch-all
            # exception path. The failure may be transient (S3 hiccup,
            # pywikibot socket reset) and the previous result file — if any —
            # is more likely to still be accurate than a fresh empty one.
            logging.warning(
                f"Caught exception getting item info for {dpla_id}", exc_info=ex
            )
            self.tracker.increment(Result.FAILED)

    @staticmethod
    def handle_upload_exception(ex) -> None:
        error_string = str(ex)
        message = "Unknown"
        error = False

        if ERROR_FILEEXISTS in error_string:
            # A file with this name exists at the Wikimedia Commons.
            message = "File already uploaded"
            error = True
        elif ERROR_MIME in error_string:
            message = "Invalid MIME type"
            error = True
        elif ERROR_BANNED in error_string:
            message = "Banned file type"
            error = True
        elif ERROR_DUPLICATE in error_string:
            # The file is a duplicate of a deleted file or
            # The upload is an exact duplicate of older version(s) of this file
            message = f"File already exists, {error_string}"
        elif ERROR_NOCHANGE in error_string:
            message = f"File exists, no change, {error_string}"
        elif ERROR_BACKEND_FAIL in error_string:
            message = "Wikimedia storage backend error (retries exhausted)"
            error = True
        elif "possible ID drift" in error_string:
            # RuntimeError raised at the ``upload() returned None`` site
            # in process_file. Reached only after
            # ``_detect_commons_dedup_skip`` ruled out the byte-drift
            # class, so what remains is a genuine drift-repair gap: the
            # target is a redirect / missing / has no readable SHA1 /
            # some shape ``_resolve_hash_drift`` and the redirect-
            # handler didn't cover. Log distinctly so this doesn't
            # disappear into "Failed: Unknown" and stays audit-able
            # for future drift-resolution work.
            message = "File linked to another page (unhandled drift shape)"
            error = True

        if error:
            logging.error(f"Failed: {message}", exc_info=ex)
        else:
            logging.warning(f"Failed: {message}", exc_info=ex)


# Module-level worker state populated by _init_upload_worker in each pool
# process. The pool worker task reads these directly rather than
# threading them through per-call args (which multiprocessing would
# have to pickle for every dispatch). Declared at module scope so the
# task function's globals lookup finds them regardless of the spawn
# start method's re-import.
_worker_uploader = None
_worker_slot_budget = None
_worker_providers_json = None
_worker_partner = None
_worker_dry_run = False
_worker_verbose = False


def _init_upload_worker(
    log_queue,
    workers_budget: int,
    partner: str,
    dry_run: bool,
    verbose: bool,
    no_create: bool,
    providers_json: dict,
    fallback_gate,
    priority_holdings,
):
    """Per-worker setup for the ``--workers > 1`` uploader Pool.

    Each spawned worker process re-imports this module fresh and runs
    this initializer once. Same shape as sdc-sync's
    ``_init_partner_worker`` — fresh pywikibot ``Site``/login, cross-
    process log routing via a ``QueueHandler``, and per-worker
    construction of the stateful helpers the item loop needs
    (Uploader, CategoryEnsurer, DupThrottle, ToolsContext).

    The parent passes ``fallback_gate`` (a ``multiprocessing.Semaphore(1)``)
    and ``priority_holdings`` (a ``multiprocessing.Value('i', 0)``) so
    every worker's :class:`WorkerSlotBudget` shares the same two per-
    session objects — enforcing "at most one shared-pool slot per
    session" and "no shared slot when the session already holds any
    priority slot" across the whole worker pool. See
    :class:`ingest_wikimedia.worker_slots.WorkerSlotBudget` for the
    invariants.
    """
    import logging.handlers

    import pywikibot

    pywikibot.config.max_retries = 5
    pywikibot.config.retry_wait = 5
    pywikibot.config.retry_max = 60
    pywikibot.config.socket_timeout = (10, 60)

    root = logging.getLogger()
    root.handlers[:] = [logging.handlers.QueueHandler(log_queue)]
    root.setLevel(logging.INFO)

    global _worker_uploader, _worker_slot_budget
    global _worker_providers_json, _worker_partner, _worker_dry_run, _worker_verbose

    commons_site = get_site()
    category_ensurer = CategoryEnsurer(commons_site, dry_run=dry_run)
    tools_context = ToolsContext.init(partner)
    tools_context.get_local_fs().setup_temp_dir()

    _worker_uploader = Uploader(
        tools_context.get_tracker(),
        tools_context.get_local_fs(),
        tools_context.get_s3_client(),
        tools_context.get_dpla(),
        commons_site,
        category_ensurer,
        no_create=no_create,
        sha1_lock_dir=SHA1_LOCK_DIR,
    )

    if workers_budget > 0:
        shared_budget = WorkerSlotBudget(workers_budget)
        _worker_slot_budget = WorkerSlotBudget(
            UPLOADER_PRIORITY_SLOTS,
            slot_dir=UPLOADER_PRIORITY_SLOT_DIR,
            fallback=shared_budget,
            fallback_gate=fallback_gate,
            priority_holdings=priority_holdings,
        )
    else:
        _worker_slot_budget = WorkerSlotBudget(0)

    _worker_providers_json = providers_json
    _worker_partner = partner
    _worker_dry_run = dry_run
    _worker_verbose = verbose


def _worker_warmup(_ignored):
    """Confirm this worker's ``_init_upload_worker`` finished. Returns
    True on success; raises whatever the initializer already raised
    (re-raised from a stashed error) on failure.

    ``multiprocessing.Pool`` does not surface initializer exceptions
    directly — a failed init leaves the pool respawning workers or
    hanging on the first result forever. Running one warmup task per
    worker slot from the parent before real dispatch forces any init
    failure to surface as a ``pool.apply`` exception, so the parent
    can abort fast instead of waiting on a job that will never
    progress. See CPython issues 43306 / 35311 / cpython #103061."""
    if _worker_uploader is None:
        raise RuntimeError("upload worker initializer did not populate globals")
    return True


def _worker_upload_task(dpla_id: str):
    """Process one DPLA item in the pool worker; return
    ``(dpla_id, tracker_delta, newly_created_delta)``.

    ``tracker_delta`` is the change in the worker's tracker counters
    across this one item — the parent merges it into its own tracker
    so the final ``str(tracker)`` line and Slack summary reflect work
    done across all workers. Uses the same ``snapshot`` → ``diff``
    pattern as sdc-sync's parallel path so a long-lived worker doesn't
    double-count across successive tasks.

    ``newly_created_delta`` is the set of institution QIDs the worker's
    :class:`CategoryEnsurer` created on Commons during this item.
    The parent unions these across all tasks and feeds the combined
    set into ``_post_upload_touch_new_institutions``, since each
    spawned worker has its own ``category_ensurer.newly_created``
    that the parent's ensurer would otherwise never see — leaving
    first-batch files stranded in Category:Unknown institution.

    Wraps ``process_item`` in the same slot-acquire that the single-
    worker path uses. A worker-level exception is logged and swallowed
    so a bad item doesn't kill the pool worker; ``CsrfRecoveryFailed``
    re-raises so the pool sees it and the parent's outer try/except
    can abort the whole run cleanly rather than skip-and-recur on
    every subsequent item.
    """
    prior = _worker_uploader.tracker.snapshot()
    prior_newly_created = set(_worker_uploader.category_ensurer.newly_created)
    try:
        with _worker_slot_budget.acquire():
            _worker_uploader.process_item(
                dpla_id,
                _worker_providers_json,
                _worker_partner,
                _worker_verbose,
                _worker_dry_run,
            )
    except CsrfRecoveryFailed:
        raise
    except Exception:
        logging.exception(f" -- Item {dpla_id}: worker task raised; skipping.")
    delta = _worker_uploader.tracker.diff(prior)
    newly_created_delta = (
        _worker_uploader.category_ensurer.newly_created - prior_newly_created
    )
    return dpla_id, delta, newly_created_delta


def _run_upload_pool(
    *,
    dpla_ids,
    partner: str,
    dry_run: bool,
    verbose: bool,
    no_create: bool,
    workers_budget: int,
    providers_json: dict,
    workers: int,
    tracker,
    newly_created: set[str],
) -> None:
    """Dispatch upload across ``workers`` spawned processes.

    Creates the ``multiprocessing.Semaphore(1)`` fallback-gate and the
    ``multiprocessing.Value('i', 0)`` priority-holdings counter shared
    across all workers of this session, then runs the pool with
    ``imap_unordered`` and merges each task's tracker delta into the
    parent tracker. Cross-process log routing via ``QueueListener``
    on the parent's already-open ``-upload.log`` handlers so worker
    log lines land in the same file the operator is tailing.

    Uses ``spawn`` start_method explicitly so workers don't inherit
    the parent's pywikibot session sockets — fork-then-use of an
    already-authenticated Site has been a source of half-broken
    connections in similar bot setups.
    """
    import logging.handlers
    import multiprocessing

    ctx = multiprocessing.get_context("spawn")
    log_queue = ctx.Manager().Queue(-1)
    listener = logging.handlers.QueueListener(
        log_queue, *logging.getLogger().handlers, respect_handler_level=True
    )
    listener.start()
    fallback_gate = ctx.Semaphore(1)
    priority_holdings = ctx.Value("i", 0)
    try:
        with ctx.Pool(
            processes=workers,
            initializer=_init_upload_worker,
            initargs=(
                log_queue,
                workers_budget,
                partner,
                dry_run,
                verbose,
                no_create,
                providers_json,
                fallback_gate,
                priority_holdings,
            ),
        ) as pool:
            # Warmup — force any ``_init_upload_worker`` failure to surface
            # here instead of hanging the pool later. One task per configured
            # worker slot so every worker's initializer is exercised. If any
            # raises, ``AsyncResult.get`` re-raises in the parent, we log
            # and abort — the ``with`` block terminates the whole pool.
            warmup_results = [
                pool.apply_async(_worker_warmup, (i,)) for i in range(workers)
            ]
            for r in warmup_results:
                r.get(timeout=120)
            for dpla_id, delta, newly_created_delta in tqdm(
                pool.imap_unordered(_worker_upload_task, dpla_ids),
                total=len(dpla_ids),
                desc="Uploading Items",
                unit="Item",
                ncols=100,
            ):
                tracker.merge(delta)
                newly_created.update(newly_created_delta)
    finally:
        listener.stop()


@click.command()
@click.argument("ids_file", type=click.File("r"))
@click.argument("partner")
@click.option("--dry-run", is_flag=True)
@click.option("--verbose", is_flag=True)
@click.option(
    "--no-create",
    is_flag=True,
    help=(
        "Maintain mode: never create a new Commons File page. Overwrites of "
        "existing files (new versions) and moves/edits still proceed, but any "
        "upload that would create a not-yet-existing File page is blocked and "
        "recorded as UPLOAD_SKIPPED_WOULD_CREATE. Use when maintaining "
        "already-uploaded files for institutions no longer authorized for new "
        "uploads."
    ),
)
@click.option(
    "--workers-budget",
    type=int,
    default=0,
    help=(
        "Box-wide cap on concurrent Commons-writing processes across ALL "
        "wikimedia sessions on the host, shared with the SDC-sync phase. "
        "0 (default) disables the budget — correct for a standalone run, "
        "which has no peers to coordinate with. Pass --workers-budget 16 "
        "to join the shared box-wide cap (the launch workflow does this). "
        "See ingest_wikimedia.worker_slots.WorkerSlotBudget."
    ),
)
@click.option(
    "--workers",
    "workers",
    type=int,
    default=UPLOADER_PRIORITY_SLOTS,
    show_default=True,
    help=(
        "Parallel uploader worker processes for this session. Each worker "
        "acquires one slot per item from the priority pool (spilling into "
        "the shared pool only when its session doesn't already hold a "
        "priority slot, gated to at most one shared slot per session — see "
        "WorkerSlotBudget's fallback_gate/priority_holdings). Default "
        "matches UPLOADER_PRIORITY_SLOTS so a single uploader session can "
        "saturate the priority pool by itself; 4 concurrent sessions "
        "settle to ~1 priority slot each. Pass 1 for the legacy single-"
        "process for-loop path."
    ),
)
def main(
    ids_file,
    partner: str,
    dry_run: bool,
    verbose: bool,
    no_create: bool,
    workers_budget: int,
    workers: int,
) -> None:
    start_time = time.time()
    # ``setup_logging`` is the first thing we do so its
    # ``_install_logging_excepthook`` covers the whole run — including
    # pre-``try`` setup calls (``check_partner``, ``WorkerSlotBudget``,
    # ``get_site``) that historically ran
    # before it. Without this, any of those raising produced an exit-1 with
    # no log at all: the launcher's failure handler would then attach a
    # log from an entirely different (older) run, leading operators to
    # diagnose the wrong incident.
    setup_logging(partner, "upload", logging.INFO)
    tools_context = ToolsContext.init(partner)

    commons_site = get_site()
    category_ensurer = CategoryEnsurer(commons_site, dry_run=dry_run)

    uploader = Uploader(
        tools_context.get_tracker(),
        tools_context.get_local_fs(),
        tools_context.get_s3_client(),
        tools_context.get_dpla(),
        commons_site,
        category_ensurer,
        no_create=no_create,
        sha1_lock_dir=SHA1_LOCK_DIR,
    )

    dpla = tools_context.get_dpla()
    local_fs = tools_context.get_local_fs()
    tracker = tools_context.get_tracker()

    # ``--no-create`` is the uploader's maintain signal — it gates the
    # no-create fence inside the per-item loop and is exactly what the
    # launcher passes in maintain mode. Reusing it as the maintain
    # flag for ``check_partner`` keeps the CLI surface unchanged (no
    # new ``--maintain`` flag here, unlike the downloader).
    dpla.check_partner(partner, maintain=no_create)

    # Box-wide Commons-write budget. The uploader is single-process, so
    # it holds exactly one slot while working an item — counting as one
    # writer against the cap so concurrent upload + SDC-sync sessions
    # across the host don't collectively overrun Commons' maxlag
    # threshold. Per-item acquire (not whole-run) so the uploader doesn't
    # stall at startup holding a slot through its S3 reads when the pool
    # is momentarily full, and briefly frees the slot between items.
    #
    # Two-tier: primary = dedicated uploader priority pool, fallback =
    # the box-wide shared pool the SDC workers contend over. The
    # uploader's per-item ``slot_budget.acquire()`` tries the priority
    # pool first and only spills into the shared pool when fewer than
    # ``UPLOADER_PRIORITY_SLOTS`` uploader items would otherwise be
    # serviced — which means an uploader is never blocked by SDC
    # workers as long as box-wide uploader concurrency stays within the
    # priority pool's size. See ``ingest_wikimedia.worker_slots`` for
    # the rationale (additive, not carved out of the shared budget).
    #
    # ``workers_budget <= 0`` disables both pools (standalone run).
    # Built before the try so the post-upload touch in the finally can
    # reuse it.
    if workers_budget > 0:
        shared_budget = WorkerSlotBudget(workers_budget)
        slot_budget = WorkerSlotBudget(
            UPLOADER_PRIORITY_SLOTS,
            slot_dir=UPLOADER_PRIORITY_SLOT_DIR,
            fallback=shared_budget,
        )
    else:
        slot_budget = WorkerSlotBudget(0)

    # Suppresses the "Upload Complete" Slack notification in the finally
    # when the run aborted mid-loop. Defined *before* the outer try so
    # it's in scope for the finally even if an early setup step raises.
    session_aborted = False

    try:
        local_fs.setup_temp_dir()
        notify_phase_start(partner, "upload")
        if dry_run:
            logging.warning("---=== DRY RUN ===---")
        if no_create:
            logging.warning(
                "---=== MAINTAIN MODE (no-create): no new File pages will be created ===---"
            )

        providers_json = dpla.get_providers_data()
        logging.info(f"Starting upload for {partner}")

        dpla_ids = load_ids(ids_file)

        try:
            if workers > 1:
                # Workers each have their own CategoryEnsurer with a private
                # ``newly_created`` set — the parent's ensurer wouldn't otherwise
                # see them, and the end-of-run touch helper would find nothing to
                # touch. Union the deltas from each task and fold them into the
                # parent ensurer so ``_post_upload_touch_new_institutions`` fires
                # exactly once, box-serialised, for every institution any worker
                # created.
                worker_newly_created: set[str] = set()
                _run_upload_pool(
                    dpla_ids=dpla_ids,
                    partner=partner,
                    dry_run=dry_run,
                    verbose=verbose,
                    no_create=no_create,
                    workers_budget=workers_budget,
                    providers_json=providers_json,
                    workers=workers,
                    tracker=tracker,
                    newly_created=worker_newly_created,
                )
                # ``newly_created`` is a copy-returning property; mutate the
                # underlying set directly so the touch helper sees the union.
                category_ensurer._newly_created.update(worker_newly_created)
            else:
                for dpla_id in tqdm(
                    dpla_ids, desc="Uploading Items", unit="Item", ncols=100
                ):
                    with slot_budget.acquire():
                        uploader.process_item(
                            dpla_id, providers_json, partner, verbose, dry_run
                        )
        except CsrfRecoveryFailed as ex:
            # Session's auth is broken and unrecoverable. Abort — do NOT
            # continue to remaining items (every one would hit the same
            # error). Re-raise after notifying so the process exits
            # non-zero and the tmux `&&` chain doesn't proceed to sdc-sync.
            session_aborted = True
            logging.error("Aborting upload: %s", ex)
            notify_upload_aborted(
                tracker=tracker,
                partner_label=partner,
                elapsed_seconds=time.time() - start_time,
                reason=str(ex),
            )
            raise

    finally:
        elapsed = time.time() - start_time
        logging.info("\n" + str(tracker))
        logging.info(f"{elapsed} seconds.")
        local_fs.cleanup_temp_dir()
        # On a session-aborted run ``notify_upload_aborted`` already
        # posted the failure message; skip the "Upload Complete" summary
        # AND the Commons-writing touch helper, since the auth state is
        # broken and further writes would fail noisily (potentially
        # masking the original CSRF-fatal in logs).
        if not session_aborted:
            # Touch files for any institutions we set up this session.  Closes the
            # Wikidata-replication-lag race that lands first-batch files in the
            # "unknown institution" category; without this we rely on a periodic
            # run of fix-unknown-categories to clean up after the fact.  The 10s
            # pause is a cheap belt-and-suspenders against very-late ensure()
            # calls — for typical runs replication has settled long before this.
            #
            # Passes the slot budget through: these touches are Commons writes,
            # so the helper holds a slot around them (see its docstring).
            _post_upload_touch_new_institutions(
                commons_site, category_ensurer, dry_run, slot_budget
            )
            notify_upload_complete(
                # Bare partner slug, not "wikimedia-<partner>" — notify_upload_complete
                # always prepends "wikimedia-" itself (matching the bare-label
                # convention notify_phase_start and notify_download_complete use).
                # Passing the pre-prefixed form yielded "wikimedia-wikimedia-<partner>"
                # in standalone runs after PR #199 refactored these helpers.
                tracker=tracker,
                partner_label=partner,
                elapsed_seconds=elapsed,
                dry_run=dry_run,
            )


# Wait between the last ensure() and the first touch.  Wikidata→Commons
# replication is usually sub-second but we've seen first-file misses in
# practice, so give it real headroom.  Only paid when there's something to
# touch, which is the rare "new institution this session" case.
_REPLICATION_SETTLE_SECS = 10

# Cap the per-extension trailing-orphan probe so a runaway naming scheme
# never produces an unbounded loop of FilePage.exists() calls.  No real
# DPLA item has more pages than this.
_ORPHAN_PROBE_CEILING = 500

# Tolerate small gaps in the probe sequence — orphans aren't always
# contiguous.  E.g. a previous session may have moved or deleted (page N)
# while leaving (page N+1) stranded.  Two consecutive misses is enough to
# call the trail finished.
_ORPHAN_GAP_TOLERANCE = 2


def _post_item_orphan_check(
    site,
    s3_client: S3Client,
    tracker: Tracker,
    dpla_id: str,
    item_title: str,
    partner: str,
    ordinal_exts: dict[int, str],
    page_labels: dict[int, str],
    dry_run: bool,
) -> None:
    """Audit (log-only) Commons files whose page-number suffix exceeds the
    current source asset count for this item — "trailing-page orphans" left
    behind when the source truncated one or more pages from the end of a
    multi-page item.

    These are invisible to process_file (which only iterates current asset
    list positions). Under the SHA1-uniqueness redesign (PR C+D) this probe
    no longer emits ``{{Duplicate}}`` tags; the entire tag apparatus is
    retired. It remains as an audit: every trailing-page orphan found is
    logged and counted under ``Result.ORPHANS_FLAGGED`` so operators can see
    what a human may want to reconcile.

    For each extension used by the item:
      - Compute the expected per-extension page count from ordinal_exts.
      - If count == 1 the expected Commons title is the no-suffix variant,
        so any (page N) at all is an orphan — probe from page 1.
      - If count >= 2 the expected titles are (page 1)..(page N), so probe
        from page N+1.
      - Probe upward via FilePage.exists() (page-info API, not search),
        tolerating up to _ORPHAN_GAP_TOLERANCE consecutive missing pages
        — orphans aren't always contiguous (e.g. a prior session may have
        moved or deleted (page N) while leaving (page N+1) stranded).
      - Each orphan found is logged and flagged; nothing is written to
        Commons.

    ``dry_run`` is accepted for call-site symmetry with the upload phase but
    is not consulted — this audit never writes to Commons regardless.
    """
    # Declared per-extension page count from the pre-scan.  This is what
    # determines the legitimate (page 1)…(page N) range for the item — we
    # MUST derive the probe start from this and not from `per_ext` below,
    # because an ordinal whose SHA1 we can't read still occupies a real
    # page slot.  Underestimating expected_count would make the probe
    # overlap a legitimate page and risk tagging it as a duplicate.
    declared_ext_counts: dict[str, int] = {}
    for ordinal in ordinal_exts:
        ext = ordinal_exts[ordinal]
        if not ext:
            continue  # stub / octet-stream ordinal — not a per-extension slot
        declared_ext_counts[ext] = declared_ext_counts.get(ext, 0) + 1

    # Build per-extension SHA1→kept_title map for the assets we can hash.
    # Used only to decide whether a found orphan is a duplicate of a known
    # source asset — the probe boundary is driven by declared_ext_counts.
    per_ext: dict[str, list[tuple[str, str]]] = {}
    for ordinal in sorted(ordinal_exts):
        ext = ordinal_exts[ordinal]
        if not ext:
            continue  # stub / octet-stream ordinal — no per-extension entry
        page_label = page_labels.get(ordinal, "")
        s3_path = s3_client.get_media_s3_path(dpla_id, ordinal, partner)
        try:
            s3_obj = s3_client.get_s3().Object(S3_BUCKET, s3_path)
            sha1 = (s3_obj.metadata or {}).get(CHECKSUM)
        except Exception as e:
            logging.warning(
                f"Orphan check: could not read SHA1 for {dpla_id} ord {ordinal}: {e}"
            )
            continue
        if not sha1:
            continue
        kept_title = get_page_title(
            item_title=item_title,
            dpla_identifier=dpla_id,
            suffix=ext,
            page=page_label,
        )
        per_ext.setdefault(ext, []).append((sha1, kept_title))

    for ext, expected_count in declared_ext_counts.items():
        entries = per_ext.get(ext, [])
        if expected_count >= 2:
            start_page = expected_count + 1
        elif expected_count == 1:
            start_page = 1
        else:
            continue

        # First-seen-wins: if the same SHA1 appears at multiple kept titles
        # (rare — happens when a source mediaMaster lists the same asset twice),
        # any of the kept titles is a valid duplicate target.
        sha1_to_kept: dict[str, str] = {}
        for sha1, kept_title in entries:
            sha1_to_kept.setdefault(sha1, kept_title)

        consecutive_misses = 0
        for k in range(start_page, start_page + _ORPHAN_PROBE_CEILING):
            candidate_title = get_page_title(
                item_title=item_title,
                dpla_identifier=dpla_id,
                suffix=ext,
                page=str(k),
            )
            # Fresh FilePage each iteration so .exists() doesn't return a
            # cached result from a previous call within this session.
            candidate = pywikibot.FilePage(site, candidate_title)
            if not candidate.exists():
                consecutive_misses += 1
                if consecutive_misses > _ORPHAN_GAP_TOLERANCE:
                    break  # trail of misses long enough; stop scanning ext
                continue
            consecutive_misses = 0

            # Redirects are not orphan files — they already point at the
            # correct target and have no file content of their own. Tagging
            # them as duplicates produces wikitext like
            #   {{Duplicate|<target>|...}}
            #   #REDIRECT [[<target>]]
            # which is meaningless and pollutes the page with a stray
            # template above the redirect. Worse, the SHA1 match path below
            # silently approves the tag because pywikibot's
            # latest_file_info follows the redirect and returns the
            # *target's* SHA1, so a redirect to a kept asset always passes
            # the sha1_to_kept check. Skip redirects entirely — they are
            # already doing what {{Duplicate}} is meant to do.
            if candidate.isRedirectPage():
                logging.info(
                    f"Orphan check: skipping [[File:{candidate_title}]] — "
                    f"already a redirect to its target."
                )
                continue

            try:
                orphan_sha1 = candidate.latest_file_info.sha1
            except Exception as e:
                logging.warning(
                    f"Orphan check: could not read orphan SHA1 for "
                    f"[[File:{candidate_title}]]: {e}"
                )
                tracker.increment(Result.ORPHANS_FLAGGED)
                continue

            if orphan_sha1 in sha1_to_kept:
                keep_title = sha1_to_kept[orphan_sha1]
                # process_file may have SKIPPED the ordinal whose SHA1 we
                # matched (bad mime, octet-stream, empty file, etc.), or the
                # item may have aborted on UploadTimeoutError before reaching
                # it.  In either case the kept_title we'd point at doesn't
                # actually exist on Commons.  Confirm it does before pointing
                # an orphan at a phantom target.
                # Log-only audit (SHA1-uniqueness redesign): the trailing-page
                # orphan matches a kept asset's SHA1, so it would formerly have
                # been tagged {{Duplicate}}. That apparatus is retired; we no
                # longer tag. Record it as FLAGGED (audit) so the run summary
                # reflects the follow-up a human may want to make.
                keep_page = pywikibot.FilePage(site, keep_title)
                if not keep_page.exists():
                    logging.warning(
                        f"Orphan [[File:{candidate_title}]] (SHA1 {orphan_sha1}) "
                        f"matches [[File:{keep_title}]] which does not exist on "
                        f"Commons (asset likely skipped or upload aborted); "
                        f"flagging for audit."
                    )
                else:
                    logging.info(
                        f"Trailing-page orphan [[File:{candidate_title}]] "
                        f"(SHA1 {orphan_sha1}) duplicates kept asset "
                        f"[[File:{keep_title}]] (DPLA ID {dpla_id}); "
                        f"flagging for audit (no tag emitted)."
                    )
                tracker.increment(Result.ORPHANS_FLAGGED)
            else:
                logging.warning(
                    f"Orphan beyond asset count: [[File:{candidate_title}]] "
                    f"(SHA1 {orphan_sha1}) — not present in current S3 assets "
                    f"for DPLA ID {dpla_id}; manual review needed."
                )
                tracker.increment(Result.ORPHANS_FLAGGED)


def _post_upload_touch_new_institutions(
    commons_site,
    category_ensurer: CategoryEnsurer,
    dry_run: bool,
    slot_budget: WorkerSlotBudget,
) -> None:
    """At end-of-run, force-rerender files for any institutions whose P8464
    was first added this session — see touch_institution_files() docstring.

    These touches are Commons writes, so they're done under a box-wide slot
    (``slot_budget``). The slot is acquired *after* the early-return guards
    so the common no-op case (no new institutions) never blocks waiting for
    capacity it doesn't need."""
    if not category_ensurer or dry_run:
        return
    newly_created = category_ensurer.newly_created
    if not newly_created:
        return
    logging.info(
        f"Touching files for {len(newly_created)} newly-created institution(s) "
        f"to clear any Wikidata-replication race; sleeping "
        f"{_REPLICATION_SETTLE_SECS}s first..."
    )
    time.sleep(_REPLICATION_SETTLE_SECS)
    total = 0
    with slot_budget.acquire():
        for inst_qid in sorted(newly_created):
            try:
                n = touch_institution_files(commons_site, inst_qid)
            except CsrfRecoveryFailed:
                # Session-level fatal from a wrapped .touch() inside
                # touch_institution_files — propagate so main() ends
                # the run rather than logging a warning per remaining
                # institution while writes stay broken.
                raise
            except Exception as e:
                logging.warning(f"Search/touch for {inst_qid} failed: {e}")
                continue
            logging.info(f"  Touched {n} files for {inst_qid}")
            total += n
    logging.info(
        f"Post-upload touch complete: {total} files across {len(newly_created)} institution(s)"
    )


if __name__ == "__main__":
    main()
