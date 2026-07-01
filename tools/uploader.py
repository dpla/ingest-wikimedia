import concurrent.futures
import datetime
import gc
import json
import logging
import mimetypes
import os
import random
import time

import click
import pywikibot
from pywikibot.site import BaseSite

from tqdm import tqdm

from ingest_wikimedia.dup_throttle import DuplicateCategoryThrottle
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
from ingest_wikimedia.worker_slots import (
    UPLOADER_PRIORITY_SLOT_DIR,
    UPLOADER_PRIORITY_SLOTS,
    WorkerSlotBudget,
)
from ingest_wikimedia.categories import CategoryEnsurer, touch_institution_files
from ingest_wikimedia.dpla import (
    SOURCE_RESOURCE_FIELD_NAME,
    DC_TITLE_FIELD_NAME,
    DATA_PROVIDER_FIELD_NAME,
    EDM_AGENT_NAME,
    WIKIDATA_FIELD_NAME,
    DPLA,
)
from ingest_wikimedia.slack import (
    notify_dup_drain_incomplete,
    notify_upload_aborted,
    notify_upload_complete,
)
from ingest_wikimedia.wikimedia import (
    WMC_UPLOAD_CHUNK_SIZE,
    IGNORE_WIKIMEDIA_WARNINGS,
    MIME_UNKNOWN_EXT,
    build_title_drift_move_reason,
    collect_duplicate_source_sha1s,
    compute_ordinal_exts_and_page_labels,
    get_page_title,
    get_wiki_text,
    wikimedia_url,
    find_file_by_hash,
    extract_dpla_id_from_commons_title,
    is_same_item_redirect_relic,
    merge_preserved_wikitext,
    tag_as_duplicate,
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

    ``True`` means it's safe to take the upload-only branch — the per-ordinal
    iteration is preserving that title intentionally and we can upload our
    own ordinal alongside it.

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
ORDINAL_SKIPPED = "SKIPPED"  # existing Commons file matches our SHA1
ORDINAL_NOT_PRESENT = "NOT_PRESENT"  # no S3 asset to upload (downloader gap)
ORDINAL_INELIGIBLE = "INELIGIBLE"  # S3 asset present but uploader chose not
# to upload (bad MIME, download-only, unguessable extension, etc.)
ORDINAL_FAILED = "FAILED"  # upload attempted, raised, did not land
ORDINAL_DEFERRED = (
    "DEFERRED"  # {{duplicate}}-tagging upload deferred: Category:Duplicate at capacity
)


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


class CsrfRecoveryFailed(RuntimeError):
    """Raised when the pywikibot session's CSRF token cannot be recovered.

    Routed AROUND the per-ordinal generic FAILED-counter catch so a
    session-level fatal aborts the run instead of looping the same
    error against every remaining item.
    """


# Session-scoped ceiling on CSRF recoveries per run; exceeding it raises
# ``CsrfRecoveryFailed``. This is the hard guardrail against unbounded
# looping on a persistently invalid session.
MAX_CSRF_RECOVERIES = 3

_CSRF_TOKEN_ERROR_MARKER = "Invalid token 'csrf'"


def _is_csrf_token_error(ex: BaseException) -> bool:
    """True if ``ex`` is the pywikibot ``KeyError`` from ``TokenWallet``
    whose message signals an invalidated CSRF token — narrowed on the
    marker so unrelated ``KeyError``s don't trigger session recovery."""
    return isinstance(ex, KeyError) and _CSRF_TOKEN_ERROR_MARKER in str(ex)


def _recover_commons_session(site) -> None:
    """Drop the current pywikibot session, re-authenticate, and clear
    the cached TokenWallet so the next ``site.tokens['csrf']`` refetches.

    ``site.logout()`` is required first: ``site.login()`` on an
    already-"logged-in" site is a no-op in pywikibot's default flow, so
    clearing tokens alone would just re-fetch against the same bad
    session.
    """
    logging.info("Recovering Commons session: logout + login + token-wallet clear")
    site.logout()
    site.login()
    site.tokens.clear()


class Uploader:
    def __init__(
        self,
        tracker: Tracker,
        local_fs: LocalFS,
        s3_client: S3Client,
        dpla: DPLA,
        site: BaseSite,
        category_ensurer: CategoryEnsurer | None = None,
        no_create: bool = False,
        dup_throttle: DuplicateCategoryThrottle | None = None,
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
        # dup_throttle: gate on the Case-2 hash-drift tag-emitting path so a
        # run can't flood the human-maintained Category:Duplicate. None disables
        # the gate (standalone / unit-test construction); main() injects one.
        self.dup_throttle = dup_throttle
        # Capped at :data:`MAX_CSRF_RECOVERIES`; once exhausted the next
        # CSRF error raises :class:`CsrfRecoveryFailed` and aborts the run.
        self._csrf_recoveries_used = 0

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
    ) -> dict:
        """Process one ordinal's source asset and return a per-ordinal result dict.

        Return shape (consumed by process_item to assemble upload-result.json):
          {"status": <ORDINAL_*>, "title": str | None,
           "pageid": int | None, "error": str | None}

        The status drives the SDC sync phase (PR 4): UPLOADED and SKIPPED
        ordinals are eligible for wbsetclaims; NOT_PRESENT, INELIGIBLE, and
        FAILED ordinals are not. `title` and `pageid` are populated only for
        UPLOADED and SKIPPED; everything else has no canonical Commons page
        to attach structured data to.
        """
        temp_file = self.local_fs.get_temp_file()

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

            if not dry_run and not file_downloaded:
                # Resolve hash drift before downloading — Case 3 (simple move)
                # needs no file download, so detecting it first avoids wasted I/O.
                drift_old_filename: str | None = None
                drift_action: str | None = None
                force_ignore_warnings = False
                if existing_file is not None:
                    # The duplicate-source-SHA1 short-circuit only applies
                    # when the existing Commons file is at one of THIS item's
                    # own expected titles — see
                    # is_dup_sha1_sibling_at_expected_title's docstring for
                    # why a legacy NARA-bot title with the same SHA1 must
                    # NOT take this branch (it would leave an orphan
                    # duplicate alongside our (page N) uploads).
                    existing_title = existing_file.title(with_ns=False)
                    if is_dup_sha1_sibling_at_expected_title(
                        sha1=sha1,
                        existing_file_title=existing_title,
                        duplicate_source_sha1s=duplicate_source_sha1s,
                        expected_item_titles=expected_item_titles,
                    ):
                        logging.info(
                            f"Source asset list contains the same SHA1 at "
                            f"multiple positions for {dpla_id} {ordinal}; "
                            f"existing file at "
                            f"[[File:{existing_title}]] "
                            f"is a legitimate sibling, not drift. Uploading "
                            f"to [[File:{page_title}]] without disturbing it."
                        )
                        drift_action = "upload_only"
                        force_ignore_warnings = True
                    else:
                        drift_action = self._resolve_hash_drift(
                            existing_file=existing_file,
                            page_title=page_title,
                            dpla_id=dpla_id,
                            ordinal=ordinal,
                            wiki_markup=wiki_markup,
                            expected_item_titles=expected_item_titles,
                        )
                        if drift_action == "moved":
                            self.tracker.increment(Result.UPLOADED)
                            # After a successful move the same file page lives
                            # at page_title; existing_file.pageid is preserved
                            # by MediaWiki across moves so we can stamp it here.
                            return {
                                "status": ORDINAL_UPLOADED,
                                "title": page_title,
                                "pageid": existing_file.pageid,
                            }
                        elif drift_action == "upload_and_tag":
                            # This ordinal would upload its bytes to the
                            # canonical title AND tag the stranded sha1-sibling
                            # {{duplicate}}. If Category:Duplicate is at
                            # capacity, defer the WHOLE op — not just the tag —
                            # so we never upload the duplicating bytes and leave
                            # an untagged duplicate behind. The drain pass
                            # re-runs this item once the category drains. The
                            # gate is consulted only here (the rare tag path),
                            # so ordinary uploads are unaffected.
                            if (
                                self.dup_throttle is not None
                                and not self.dup_throttle.try_acquire()
                            ):
                                logging.info(
                                    f"Deferring {dpla_id} {ordinal}: "
                                    f"Category:Duplicate at capacity; will retry "
                                    f"upload + duplicate-tag in the drain pass."
                                )
                                self.tracker.increment(
                                    Result.UPLOAD_DEFERRED_DUP_CATEGORY
                                )
                                return {
                                    "status": ORDINAL_DEFERRED,
                                    "title": page_title,
                                    "pageid": None,
                                }
                            drift_old_filename = existing_file.title(with_ns=False)
                            force_ignore_warnings = True
                        else:  # "upload_only"
                            force_ignore_warnings = True

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

                # If the intended title is a redirect, route through the
                # redirect-handler regardless of what `_resolve_hash_drift`
                # returned. Uploading directly onto a redirect page fails with
                # `fileexists-shared-forbidden` — the API treats the upload as
                # creating a duplicate of the redirect's target. The
                # redirect-handler below picks the right strategy (move, or
                # overwrite-in-place with the appropriate metadata
                # preservation) based on the redirect target, and always sets
                # `force_ignore_warnings=True` for the subsequent upload.
                #
                # Earlier this branch was gated on `drift_action != "upload_only"`,
                # but `_resolve_hash_drift` can legitimately return "upload_only"
                # while the intended title is still a redirect — specifically
                # when the redirect's target is a sibling page rather than the
                # SHA1's current location. The misleading warning at line ~880
                # in `_resolve_hash_drift` was the symptom of that earlier
                # gating mistake.
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
                            wiki_file_page, redirect_old_filename = (
                                self._resolve_redirect_overwrite(
                                    wiki_file_page, dpla_id, wiki_markup
                                )
                            )
                            force_ignore_warnings = True
                            if not drift_old_filename:
                                drift_old_filename = redirect_old_filename
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
                            logging.info(
                                f"Cross-item or no-DPLA redirect for {dpla_id}: "
                                f"[[File:{wiki_file_page.title(with_ns=False)}]] → "
                                f"[[File:{target_title}]]; overwriting redirect "
                                f"with fresh wikitext (no metadata preservation)."
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
                    for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
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
                            break
                        except Exception as ex:
                            is_backend_fail = ERROR_BACKEND_FAIL in str(ex)
                            is_csrf_error = _is_csrf_token_error(ex)
                            if is_csrf_error:
                                # Session was invalidated — every subsequent
                                # upload will hit the same KeyError until
                                # we re-authenticate. Retrying the same call
                                # can't help; refresh the session instead.
                                # Session cap or recovery failure → fatal.
                                if self._csrf_recoveries_used >= MAX_CSRF_RECOVERIES:
                                    raise CsrfRecoveryFailed(
                                        f"Commons session invalidated: CSRF"
                                        f" token still invalid after"
                                        f" {self._csrf_recoveries_used}"
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
                                    self._csrf_recoveries_used + 1,
                                    MAX_CSRF_RECOVERIES,
                                )
                                try:
                                    _recover_commons_session(self.site)
                                except Exception as recover_ex:
                                    raise CsrfRecoveryFailed(
                                        f"Commons session recovery threw"
                                        f" ({recover_ex!r}); aborting rather"
                                        f" than looping unrecoverable auth"
                                        f" errors."
                                    ) from recover_ex
                                self._csrf_recoveries_used += 1
                                del future
                                gc.collect()
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
                    # upload() returned None — file exists under a different page
                    # title, likely due to DPLA ID drift between runs. The
                    # ``raise`` below propagates to the generic
                    # ``except Exception`` at the end of process_file, which
                    # increments FAILED — no counter bump needed here.
                    raise RuntimeError(
                        "File linked to another page (possible ID drift)"
                    )

                logging.info(f"Uploaded to {wikimedia_url(page_title)}")
                if drift_old_filename:
                    self._tag_drift_duplicate(
                        drift_old_filename, page_title, wiki_markup, dpla_id
                    )
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
        redirect_target.move(
            wiki_file_page.title(),
            reason=reason,
            movetalk=False,
            noredirect=False,  # leave a redirect at the old title
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

        Works for *any* redirect target — same-item different-page relic,
        cross-item dedup (e.g. a 2022 human "redirecting to duplicate file"
        edit), or a no-DPLA-ID legacy title. The S3 sha1 must land at the
        intended title regardless of where the redirect points.

        When `preserve_from_target` is True (default), license/Image-extracted/
        category metadata from the redirect target is carried into the new
        page. Callers should pass False when the target is a foreign DPLA
        item, since its categories and Image-extracted parent link don't
        apply to our page.

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
            new_text = merge_preserved_wikitext(redirect_target.text or "", wiki_markup)
        else:
            new_text = wiki_markup
        wiki_file_page.text = new_text
        wiki_file_page.save(
            summary=(
                f"Replacing redirect with DPLA metadata for title drift "
                f"correction (DPLA ID [[dpla:{dpla_id}|{dpla_id}]])"
            ),
            minor=False,
        )
        wiki_file_page.clear_cache()
        return wiki_file_page, old_filename

    def _move_to_correct_title(
        self,
        existing_file: pywikibot.FilePage,
        intended_page: pywikibot.FilePage,
        dpla_id: str,
        case_label: str,
        wiki_markup: str | None = None,
        post_commonsdelinker: bool = True,
    ) -> None:
        """Move existing_file to intended_page and post a CommonsDelinker request.

        If wiki_markup is provided, the moved page's description is updated to
        reflect current DPLA metadata after the move.

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
        existing_file.move(
            intended_page.title(),
            reason=reason,
            movetalk=False,
            noredirect=False,
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

        if wiki_markup:
            moved_page = get_page(self.site, intended_page.title())
            if moved_page.exists() and not moved_page.isRedirectPage():
                # After the move, moved_page carries the original page's
                # wikitext. Preserve license, Image-extracted, and category
                # metadata from it before replacing with the {{DPLA metadata}} block.
                moved_page.text = merge_preserved_wikitext(
                    moved_page.text or "", wiki_markup
                )
                moved_page.save(
                    summary=(
                        f"Update description after title drift correction "
                        f"(DPLA ID [[dpla:{dpla_id}|{dpla_id}]])"
                    ),
                    minor=False,
                )

    def _resolve_hash_drift(
        self,
        existing_file: pywikibot.FilePage,
        page_title: str,
        dpla_id: str,
        ordinal: int,
        wiki_markup: str | None = None,
        expected_item_titles: set[str] | None = None,
    ) -> str:
        """
        Determine and where possible resolve the case where our file's SHA1
        already lives on Commons at a different title than intended.

        Returns one of:
          "moved"          — Case 1/3: file moved to correct title; caller should
                             increment UPLOADED and return (no upload needed).
          "upload_and_tag" — Case 2: correct title has a different file; caller
                             should upload (ignore_warnings=True) then tag the
                             orphaned old title as a duplicate.
          "upload_only"    — Cross-item hash collision with a still-valid DPLA ID;
                             upload to the correct title but leave the other file alone.
        """
        actual_filename = existing_file.title(with_ns=False)

        # --- Hash collision safety check ---
        # Cross-item collision: the file at the wrong title was uploaded for a
        # different DPLA ID.  If that other ID is still a valid item, we don't
        # move or tag — just upload our hash to the correct title and leave
        # their file alone.  This prevents ping-pong renaming between two
        # valid items that happen to share a hash.
        #
        # Same-item collision (same DPLA ID, different title) — including the
        # post-PR-#173 case where the page-suffix on the existing file no
        # longer matches the new naming scheme — always falls through to the
        # Case 1/2/3 migration logic below.  A previous version of this code
        # special-cased "same DPLA ID, different parsed ordinal" by returning
        # "upload_only" to avoid disturbing a hypothetical hash-coincidence
        # between two pages of the same item, but that branch fired routinely
        # whenever PR #173's per-extension page-label scheme produced a
        # different (or no) (page N) suffix from the existing Commons file —
        # creating a silent duplicate at the new title while the old file
        # stayed orphaned.  The invariant is: a given SHA1 should live at
        # exactly one Commons title for a given DPLA ID, so always migrate.
        existing_dpla_id = extract_dpla_id_from_commons_title(actual_filename)

        if existing_dpla_id and existing_dpla_id != dpla_id:
            try:
                other_item = self.dpla.get_item_metadata(existing_dpla_id)
            except Exception as ex:
                # A 404 from the DPLA API for the colliding file's DPLA ID
                # is the strongest possible signal that the existing
                # Commons file is an orphan: that ID no longer resolves to
                # any item, so the previous bot upload's DPLA-side anchor
                # is gone. Treat it exactly like ``other_item is None`` —
                # fall through to the Case 1/2/3 migration so we move the
                # orphan to the new ID's title (or upload-and-tag it),
                # rather than silently creating a duplicate alongside it.
                #
                # Distinguishing 404 from other exceptions matters because
                # the catch-all path is reached by network timeouts, 5xx
                # responses, JSON parse errors, etc. — none of which carry
                # the same definitive "the old ID is gone" meaning. Those
                # stay on the conservative ``upload_only`` fallback so a
                # transient API blip doesn't trigger a destructive move on
                # a file that still has a valid sibling item.
                status = getattr(getattr(ex, "response", None), "status_code", None)
                if status == 404:
                    logging.info(
                        f"Hash drift for {dpla_id} {ordinal}: colliding "
                        f"DPLA item {existing_dpla_id} no longer exists "
                        f"(404); treating [[File:{actual_filename}]] as "
                        f"an orphan and migrating."
                    )
                    other_item = None
                else:
                    logging.warning(
                        f"Hash drift for {dpla_id} {ordinal}: failed to verify "
                        f"colliding DPLA item {existing_dpla_id}: {ex}; "
                        f"falling back to upload_only."
                    )
                    return "upload_only"
            if other_item:
                logging.info(
                    f"Hash drift for {dpla_id} {ordinal}: "
                    f"[[File:{actual_filename}]] belongs to valid DPLA item "
                    f"{existing_dpla_id}; uploading to correct title only."
                )
                return "upload_only"

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
            # Case 3: nothing at the intended title — simple move.
            self._move_to_correct_title(
                existing_file,
                intended_page,
                dpla_id,
                "Case 3",
                wiki_markup,
                post_commonsdelinker=not sibling_slot,
            )
            return "moved"

        if intended_page.isRedirectPage():
            # Case 1 (via hash lookup): intended title is a redirect. If it
            # redirects to exactly our existing file (same filename), move over it.
            redirect_target = intended_page.getRedirectTarget()
            if redirect_target.title(with_ns=False) == actual_filename:
                self._move_to_correct_title(
                    existing_file,
                    intended_page,
                    dpla_id,
                    "Case 1",
                    wiki_markup,
                    post_commonsdelinker=not sibling_slot,
                )
                return "moved"
            # Intended title is a redirect, but its target is somewhere
            # other than where our SHA1 currently lives. We can't apply
            # the Case 1 move; instead let the caller's redirect-handler
            # decide (overwrite as same-item relic, overwrite as
            # cross-item, etc). "upload_only" here just means
            # "drift-resolution didn't move or tag anything"; the
            # redirect-handler still runs unconditionally now.
            logging.info(
                f"Hash drift for {dpla_id} {ordinal}: intended title "
                f"[[File:{intended_page.title(with_ns=False)}]] is a redirect to "
                f"[[File:{redirect_target.title(with_ns=False)}]], which is not "
                f"the location of our SHA1 ([[File:{actual_filename}]]); "
                f"deferring to the redirect-handler in process_file."
            )
            return "upload_only"

        # Case 2: intended title has real content with a different hash, and
        # the file found at the wrong title belongs to a different item (or
        # has no recognisable DPLA ID). Normally we upload the correct hash
        # to the intended title AND tag the orphaned old title as a duplicate
        # so it can be cleaned up.
        #
        # BUT: if the "old title" is itself an expected title for one of THIS
        # item's other current asset positions, it is not an orphan — it's a
        # legitimate ordinal that will be (or has been) processed by its own
        # iteration of process_file in this same run, and will get its own
        # correct content written there.  Tagging it as a duplicate at the
        # current instant produces a tag pointing at a page whose content is
        # about to change, which makes the tag wrong (and triggers admin
        # deletion of a still-valid Commons file).  Skip the tag in that case
        # and just upload our content to the intended title.
        if expected_item_titles and actual_filename in expected_item_titles:
            logging.info(
                f"Title drift (Case 2 → upload_only): "
                f"[[File:{intended_page.title(with_ns=False)}]] has a different "
                f"hash and our SHA1 currently lives at "
                f"[[File:{actual_filename}]], but that title is one of this "
                f"item's current asset positions — it will be overwritten by "
                f"its own ordinal's iteration. Uploading to "
                f"[[File:{page_title}]] without tagging."
            )
            return "upload_only"

        logging.info(
            f"Title drift (Case 2): [[File:{intended_page.title(with_ns=False)}]] "
            f"has a different hash; will upload correct hash and tag "
            f"[[File:{actual_filename}]] as duplicate."
        )
        return "upload_and_tag"

    def _tag_drift_duplicate(
        self,
        old_filename: str,
        new_filename: str,
        wiki_markup: str,
        dpla_id: str,
    ) -> None:
        """Tag a stranded file as duplicate of the new (correct-title)
        file, AND carry any community-contributed metadata from the
        old page across to the new one before the admin-side delete.

        Mirrors what ``_move_to_correct_title`` and
        ``_resolve_redirect_overwrite`` already do in the move and
        redirect-overwrite paths: license tags, assessment templates,
        ``{{Image extracted}}`` parents, and category links community
        editors have added are preserved via
        :func:`merge_preserved_wikitext`.

        Rescue and tag are independent best-effort steps — a failure
        on either is logged and does NOT block the other. The old
        file's revision history still contains the community
        contributions even if the rescue's save fails, so manual
        recovery from page history is always possible.
        """
        old_page = get_page(self.site, f"File:{old_filename}")

        # Rescue community contributions, if any.
        try:
            if old_page.exists():
                merged = merge_preserved_wikitext(old_page.text or "", wiki_markup)
                # Equality means nothing matched merge_preserved_wikitext's
                # patterns. Skip the save so we don't emit a no-op revision
                # on the new file (the other two preserve sites — move,
                # redirect-overwrite — don't need this guard because their
                # destination text always differs from wiki_markup before
                # the merge, but here new_page already carries wiki_markup
                # from the upload we just completed).
                if merged.rstrip() != wiki_markup.rstrip():
                    # `get_page` is a constructor — no API hit — and we
                    # know the page exists and isn't a redirect because
                    # we just uploaded it. Skip the exists/redirect
                    # round-trips and write directly.
                    new_page = get_page(self.site, f"File:{new_filename}")
                    new_page.text = merged
                    new_page.save(
                        summary=(
                            f"Rescue community-contributed metadata from "
                            f"[[File:{old_filename}]] (DPLA ID "
                            f"[[dpla:{dpla_id}|{dpla_id}]])"
                        ),
                        minor=False,
                    )
                    logging.info(
                        f"Rescued community-contributed metadata from "
                        f"[[File:{old_filename}]] into "
                        f"[[File:{new_filename}]] (DPLA ID {dpla_id})"
                    )
        except Exception as ex:
            logging.warning(
                f"Failed to rescue community contributions from "
                f"[[File:{old_filename}]]: {ex}"
            )

        # Tag the stranded file. Independent of the rescue: even if the
        # save above failed, queue the old title for speedy deletion so
        # the search-time SHA1 collision is resolved.
        try:
            tag_as_duplicate(
                self.site,
                old_page,
                correct_filename=new_filename,
                reason="Other file has the correct title.",
            )
            logging.info(
                f"Tagged [[File:{old_filename}]] as duplicate of "
                f"[[File:{new_filename}]] (DPLA ID {dpla_id})"
            )
        except Exception as ex:
            logging.warning(f"Failed to tag [[File:{old_filename}]] as duplicate: {ex}")

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

            # Pre-scan via the shared helper so the verifier can reconstruct
            # the same page-label assignments without duplicating the logic.
            ordinal_exts, page_labels = compute_ordinal_exts_and_page_labels(
                self.s3_client, dpla_id, partner, len(files)
            )

            # Identify SHA1s that legitimately appear at MORE THAN ONE
            # position in the source asset list.  process_file uses this to
            # short-circuit drift correction when its SHA1 is in the set —
            # the existing Commons file at another title is a valid sibling,
            # not a drift artefact, and both positions should remain as
            # separate Commons pages.
            duplicate_source_sha1s = collect_duplicate_source_sha1s(
                self.s3_client, dpla_id, partner, len(files)
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
                    )
                    ordinal_results[str(ordinal)] = result
                except UploadTimeoutError as ex:
                    ordinal_results[str(ordinal)] = {
                        "status": ORDINAL_FAILED,
                        "error": str(ex),
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
                    }
                    continue

            # After the per-asset loop, look for "trailing-page orphan" Commons
            # files for this item — pages whose ordinal exceeds the current
            # source asset count for that extension. These are invisible to
            # process_file (it only iterates the current asset list), so the
            # Case 2 tag-as-duplicate path never fires for them when the
            # source truncated pages off the end of a multi-page item.
            # Wrap separately so a check failure doesn't get charged as
            # FAILED against the item itself — the per-asset uploads have
            # already succeeded at this point.
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
            except Exception as ex:
                logging.warning(f"Orphan check failed for {dpla_id}: {ex}; continuing")

            # Persist the per-ordinal results so the SDC sync phase (PR 4)
            # knows which ordinals to attempt structured-data writes on. Fires
            # even when ordinal_results is empty (e.g. zero files in
            # file_list.txt) so a previous run's results don't get treated as
            # the current truth.
            self._persist_upload_result(partner, dpla_id, ordinal_results, dry_run)

            # Return how many ordinals the dup-category throttle deferred, so
            # main() knows to re-run this item in the drain pass and the drain
            # loop can measure progress per ordinal (a multi-page item can clear
            # some deferred ordinals while others remain). 0 = nothing deferred;
            # other exits return None, also falsy.
            return sum(
                r.get("status") == ORDINAL_DEFERRED for r in ordinal_results.values()
            )

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

        if error:
            logging.error(f"Failed: {message}", exc_info=ex)
        else:
            logging.warning(f"Failed: {message}", exc_info=ex)


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
        "The uploader is single-process (no parallelism), so it checks out "
        "exactly ONE slot while working an item — making it count as one "
        "writer against the shared budget alongside SDC-sync workers. "
        "0 (default) disables the budget — correct for a standalone run, "
        "which has no peers to coordinate with. Pass --workers-budget 16 "
        "to join the shared box-wide cap (the launch workflow does this). "
        "See ingest_wikimedia.worker_slots.WorkerSlotBudget."
    ),
)
def main(
    ids_file,
    partner: str,
    dry_run: bool,
    verbose: bool,
    no_create: bool,
    workers_budget: int,
) -> None:
    start_time = time.time()
    tools_context = ToolsContext.init(partner)

    commons_site = get_site()
    category_ensurer = CategoryEnsurer(commons_site, dry_run=dry_run)

    # Caps how many files this run may add to the human-maintained
    # Category:Duplicate (via {{duplicate}} tags on Case-2 hash-drift uploads).
    # Consulted only on that rare tag-emitting path; ordinary uploads never
    # touch it and it queries the category size at most once per ~100 tags.
    # A dry run never tags, so the throttle is simply never consulted.
    dup_throttle = DuplicateCategoryThrottle(commons_site)

    uploader = Uploader(
        tools_context.get_tracker(),
        tools_context.get_local_fs(),
        tools_context.get_s3_client(),
        tools_context.get_dpla(),
        commons_site,
        category_ensurer,
        no_create=no_create,
        dup_throttle=dup_throttle,
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
        setup_logging(partner, "upload", logging.INFO)
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

        # Map of dpla_id -> count of ordinals the dup-category throttle deferred.
        deferred: dict[str, int] = {}
        try:
            for dpla_id in tqdm(
                dpla_ids, desc="Uploading Items", unit="Item", ncols=100
            ):
                with slot_budget.acquire():
                    deferred_count = uploader.process_item(
                        dpla_id, providers_json, partner, verbose, dry_run
                    )
                    if deferred_count:
                        deferred[dpla_id] = deferred_count
        except CsrfRecoveryFailed as ex:
            # Session's auth is broken and unrecoverable. Abort — do NOT
            # continue to remaining items (every one would hit the same
            # error). The drain pass below is skipped for the same reason.
            session_aborted = True
            logging.error("Aborting upload: %s", ex)
            notify_upload_aborted(
                tracker=tracker,
                partner_label=partner,
                elapsed_seconds=time.time() - start_time,
                reason=str(ex),
            )

        # Any item whose {{duplicate}}-tagging upload was deferred because
        # Category:Duplicate was full: wait for the category to drain, then
        # re-run those items in this same session (not an operator re-run).
        if deferred and not session_aborted:
            _drain_deferred_dups(
                uploader,
                dup_throttle,
                deferred,
                providers_json,
                partner,
                verbose,
                dry_run,
                slot_budget,
            )

    finally:
        elapsed = time.time() - start_time
        logging.info("\n" + str(tracker))
        logging.info(f"{elapsed} seconds.")
        local_fs.cleanup_temp_dir()
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
        # On a session-aborted run ``notify_upload_aborted`` already
        # posted the failure message; skip the "Upload Complete" summary.
        if not session_aborted:
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


# Upper bound on how long the drain pass will wait for Category:Duplicate to
# fall back below the throttle's resume threshold. Volunteers clear the
# category on human timescales, so give it real headroom — but cap it so a
# session never hangs indefinitely. On timeout the remaining files are left
# un-uploaded and the operator is pinged to re-run after the category drains.
_DRAIN_MAX_WAIT_SECS = 4 * 60 * 60  # 4 hours


def _drain_deferred_dups(
    uploader: "Uploader",
    throttle: DuplicateCategoryThrottle,
    deferred: dict[str, int],
    providers_json,
    partner: str,
    verbose: bool,
    dry_run: bool,
    slot_budget: WorkerSlotBudget,
    *,
    max_wait_secs: float = _DRAIN_MAX_WAIT_SECS,
) -> dict[str, int]:
    """Re-run items whose {{duplicate}}-tagging upload was deferred, once
    Category:Duplicate has drained.

    ``deferred`` maps each item's dpla_id to its count of deferred ordinals.
    Each round waits (via the throttle) for the category to fall below its
    resume threshold, then re-processes the still-deferred items. Re-processing
    may itself defer again if the category refills mid-drain, so it loops until
    nothing remains, no progress is made, or the wait budget is exhausted.
    Returns the {id: count} still deferred at exit (empty on full success).

    Progress is measured by total deferred *ordinals*, not items: a single
    multi-page item can clear some ordinals while others remain, which is real
    progress and must not read as a stall.
    """
    remaining = dict(deferred)
    deadline = time.monotonic() + max_wait_secs
    logging.info(
        "Drain pass: %d file(s) across %d item(s) deferred while "
        "Category:Duplicate was at capacity; waiting for it to drain, then "
        "retrying their uploads.",
        sum(remaining.values()),
        len(remaining),
    )
    while remaining:
        budget = deadline - time.monotonic()
        if budget <= 0 or not throttle.wait_for_capacity(budget):
            still = sum(remaining.values())
            logging.error(
                "Drain pass timed out with %d file(s) still deferred; their "
                "uploads + {{duplicate}} tags were NOT emitted. Clear "
                "Category:Duplicate, then re-run the partner to finish them.",
                still,
            )
            notify_dup_drain_incomplete(partner, still)
            return remaining

        still_deferred: dict[str, int] = {}
        for dpla_id in remaining:
            with slot_budget.acquire():
                count = uploader.process_item(
                    dpla_id, providers_json, partner, verbose, dry_run
                )
                if count:
                    still_deferred[dpla_id] = count

        # A batch larger than the throttle's headroom (recheck_cap) legitimately
        # takes several rounds — each round re-queries and clears up to a cap's
        # worth, so partial progress is normal, not a stall. But if a round that
        # began with confirmed capacity cleared NO ordinals, the category is
        # refilling as fast as we drain (or another mechanism keeps deferring) —
        # bail rather than spin a hot loop re-querying the category every round.
        if sum(still_deferred.values()) >= sum(remaining.values()):
            still = sum(still_deferred.values())
            logging.error(
                "Drain pass made no progress despite available capacity (%d "
                "file(s) still deferred); aborting drain. Clear "
                "Category:Duplicate, then re-run the partner to finish them.",
                still,
            )
            notify_dup_drain_incomplete(partner, still)
            return still_deferred
        remaining = still_deferred

    logging.info("Drain pass complete: all deferred uploads + duplicate-tags emitted.")
    return remaining


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
    """Tag Commons files whose page-number suffix exceeds the current source
    asset count for this item — "trailing-page orphans" left behind when the
    source truncated one or more pages from the end of a multi-page item.

    These are invisible to process_file (which only iterates current asset
    list positions), so Case 2's tag-as-duplicate never fires for them.

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
      - For each orphan found, compare its SHA1 to the SHA1 set of this
        item's S3 assets of the same extension:
          - match → tag as duplicate of the matching uploaded title
          - no match → log a WARNING for manual review (could be a real
            unrelated upload at that title that we shouldn't touch)
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
                keep_page = pywikibot.FilePage(site, keep_title)
                if not keep_page.exists():
                    logging.warning(
                        f"Orphan [[File:{candidate_title}]] (SHA1 {orphan_sha1}) "
                        f"would point at [[File:{keep_title}]] but that title "
                        f"does not exist on Commons (asset likely skipped or "
                        f"upload aborted); flagging instead of tagging."
                    )
                    tracker.increment(Result.ORPHANS_FLAGGED)
                    continue
                if dry_run:
                    logging.info(
                        f"[DRY RUN] would tag orphan [[File:{candidate_title}]] "
                        f"as duplicate of [[File:{keep_title}]] (DPLA ID {dpla_id})"
                    )
                    tracker.increment(Result.ORPHANS_TAGGED)
                    continue
                try:
                    tag_as_duplicate(
                        site,
                        candidate,
                        correct_filename=keep_title,
                        reason=(
                            "Trailing-page orphan: this title has no "
                            "corresponding asset in the current DPLA source "
                            "for this item; the matching asset is uploaded at "
                            f"[[:File:{keep_title}]]."
                        ),
                    )
                    logging.info(
                        f"Tagged trailing-page orphan [[File:{candidate_title}]] "
                        f"as duplicate of [[File:{keep_title}]] (DPLA ID {dpla_id})"
                    )
                    tracker.increment(Result.ORPHANS_TAGGED)
                except Exception as e:
                    # The orphan remains unresolved; record as FLAGGED so the
                    # run summary accurately reflects follow-up work needed.
                    logging.warning(
                        f"Failed to tag orphan [[File:{candidate_title}]]: {e}"
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
