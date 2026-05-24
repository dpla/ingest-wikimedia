import concurrent.futures
import json
import logging
import mimetypes
import random
import time

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
from ingest_wikimedia.categories import CategoryEnsurer, touch_institution_files
from ingest_wikimedia.dpla import (
    SOURCE_RESOURCE_FIELD_NAME,
    DC_TITLE_FIELD_NAME,
    DATA_PROVIDER_FIELD_NAME,
    EDM_AGENT_NAME,
    WIKIDATA_FIELD_NAME,
    DPLA,
)
from ingest_wikimedia.slack import notify_upload_complete
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
    post_commonsdelinker_request,
)

MAX_UPLOAD_RETRIES = 3
UPLOAD_RETRY_BASE_DELAY_SECS = 5
UPLOAD_RETRY_MAX_DELAY_SECS = 60
# pywikibot's async upload polls Commons indefinitely when the job queue is stuck.
# This cap ensures a single hung upload never freezes the whole session.
UPLOAD_TIMEOUT_SECS = 3600  # 1 hour


class UploadTimeoutError(RuntimeError):
    """Raised when a single file upload exceeds UPLOAD_TIMEOUT_SECS.

    Distinct from RuntimeError so it can escape process_file()'s catch-all
    and break the remaining-files loop in process_item() — no point attempting
    further pages when Commons' job queue is stuck.
    """


class Uploader:
    def __init__(
        self,
        tracker: Tracker,
        local_fs: LocalFS,
        s3_client: S3Client,
        dpla: DPLA,
        site: BaseSite,
        category_ensurer: CategoryEnsurer | None = None,
    ):
        self.tracker = tracker
        self.local_fs = local_fs
        self.s3_client = s3_client
        self.site = site
        self.dpla = dpla
        self.category_ensurer = category_ensurer

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
    ):
        temp_file = self.local_fs.get_temp_file()

        try:
            wiki_markup = get_wiki_text(dpla_id, item_metadata, provider, data_provider)
            s3_path = self.s3_client.get_media_s3_path(dpla_id, ordinal, partner)
            upload_comment = f'Uploading DPLA ID "[[dpla:{dpla_id}|{dpla_id}]]".'
            if not self.s3_client.s3_file_exists(s3_path):
                logging.info(f"{dpla_id} {ordinal} not present.")
                self.tracker.increment(Result.SKIPPED)
                return

            s3_object = self.s3_client.get_s3().Object(S3_BUCKET, s3_path)
            file_size = s3_object.content_length

            if file_size == 0:
                logging.info(f"Skipping {dpla_id} {ordinal}: File size is 0.")
                self.tracker.increment(Result.SKIPPED)
                return

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
                    self.tracker.increment(Result.SKIPPED)
                    return

            if not check_content_type(mime):
                logging.info(f"Skipping {dpla_id} {ordinal}: Bad content type: {mime}")
                self.tracker.increment(Result.SKIPPED)
                return

            if is_download_only(mime):
                logging.info(
                    f"Skipping {dpla_id} {ordinal}: {mime} staged for conversion, not uploaded."
                )
                self.tracker.increment(Result.SKIPPED)
                return

            ext = mimetypes.guess_extension(mime)

            if not ext or ext == MIME_UNKNOWN_EXT:
                logging.info(
                    f"Skipping {dpla_id} {ordinal}: Unable to guess extension for {mime}"
                )
                self.tracker.increment(Result.SKIPPED)
                return

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
                    return
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
                    if duplicate_source_sha1s and sha1 in duplicate_source_sha1s:
                        # The source data legitimately lists this SHA1 at
                        # multiple positions within the item.  The existing
                        # Commons file at another title is a valid sibling,
                        # not drift to be migrated — both positions should
                        # exist as separate Commons pages.  Upload to our
                        # title and leave the other alone.
                        logging.info(
                            f"Source asset list contains the same SHA1 at "
                            f"multiple positions for {dpla_id} {ordinal}; "
                            f"existing file at "
                            f"[[File:{existing_file.title(with_ns=False)}]] "
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
                        )
                        if drift_action == "moved":
                            self.tracker.increment(Result.UPLOADED)
                            return
                        elif drift_action == "upload_and_tag":
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

                # If the intended title is a redirect caused by title drift,
                # move the file there first so the upload lands at the right name.
                # This path is reached when the hash is new (not yet on Commons)
                # and the intended title is a redirect from a prior drift correction.
                # Skip when drift resolution returned "upload_only" — in that case
                # we've decided not to touch any other file; just upload directly.
                if wiki_file_page.isRedirectPage() and drift_action != "upload_only":
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

                # Use direct upload (chunk_size=0) + ignore_warnings=True when the
                # file page already exists, or when the hash already lives elsewhere
                # on Commons (drift case). The stash-commit path raises
                # fileexists-shared-forbidden even on valid overwrites; the direct
                # path with ignorewarnings=1 bypasses it. True (vs IGNORE_WIKIMEDIA_WARNINGS)
                # is intentional — for an overwrite we want to suppress all warnings,
                # including 'exists' variants that would fire on the direct path.
                file_exists = (
                    wiki_file_page.exists() and not wiki_file_page.isRedirectPage()
                )
                chunk_size = (
                    0
                    if (file_exists or force_ignore_warnings)
                    else WMC_UPLOAD_CHUNK_SIZE
                )
                upload_warnings = (
                    True
                    if (file_exists or force_ignore_warnings)
                    else IGNORE_WIKIMEDIA_WARNINGS
                )

                result = None
                # Avoid the `with executor:` context manager — its __exit__ calls
                # shutdown(wait=True), which would block until pywikibot's stuck
                # polling thread exits on its own, defeating the timeout entirely.
                # Use try/finally to guarantee shutdown(wait=False) on all paths.
                executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                try:
                    for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
                        try:
                            future = executor.submit(
                                self.site.upload,
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
                                time.sleep(delay)
                            else:
                                if is_backend_fail:
                                    self.tracker.increment(Result.FAILED)
                                raise
                finally:
                    executor.shutdown(wait=False)

                if not result:
                    # upload() returned None — file exists under a different page
                    # title, likely due to DPLA ID drift between runs.
                    self.tracker.increment(Result.FAILED)
                    raise RuntimeError(
                        "File linked to another page (possible ID drift)"
                    )

                logging.info(f"Uploaded to {wikimedia_url(page_title)}")
                if drift_old_filename:
                    self._tag_drift_duplicate(drift_old_filename, page_title, dpla_id)
                self.tracker.increment(Result.UPLOADED)
                self.tracker.increment(Result.BYTES, file_size)

        except UploadTimeoutError:
            raise
        except Exception as ex:
            self.handle_upload_exception(ex)

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
        post_commonsdelinker_request(self.site, old_filename, new_filename)

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
    ) -> None:
        """Move existing_file to intended_page and post a CommonsDelinker request.

        If wiki_markup is provided, the moved page's description is updated to
        reflect current DPLA metadata after the move.
        """
        actual_filename = existing_file.title(with_ns=False)
        intended_filename = intended_page.title(with_ns=False)
        reason = build_title_drift_move_reason(
            actual_filename, intended_filename, dpla_id, self.site.user()
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
        post_commonsdelinker_request(self.site, actual_filename, intended_filename)

        if wiki_markup:
            moved_page = get_page(self.site, intended_page.title())
            if moved_page.exists() and not moved_page.isRedirectPage():
                # After the move, moved_page carries the original page's
                # wikitext. Preserve license, Image-extracted, and category
                # metadata from it before replacing with the DPLA Artwork block.
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

        if not intended_page.exists():
            # Case 3: nothing at the intended title — simple move.
            self._move_to_correct_title(
                existing_file, intended_page, dpla_id, "Case 3", wiki_markup
            )
            return "moved"

        if intended_page.isRedirectPage():
            # Case 1 (via hash lookup): intended title is a redirect. If it
            # redirects to exactly our existing file (same filename), move over it.
            redirect_target = intended_page.getRedirectTarget()
            if redirect_target.title(with_ns=False) == actual_filename:
                self._move_to_correct_title(
                    existing_file, intended_page, dpla_id, "Case 1", wiki_markup
                )
                return "moved"
            logging.warning(
                f"Hash drift for {dpla_id} {ordinal}: intended title "
                f"[[File:{intended_page.title(with_ns=False)}]] redirects to "
                f"{redirect_target.title(with_ns=False)!r}, "
                f"which does not share DPLA ID {dpla_id}; uploading anyway."
            )
            return "upload_only"

        # Case 2: intended title has real content with a different hash, and the
        # file found at the wrong title belongs to a different item (or has no
        # recognisable DPLA ID). Upload the correct hash and tag the orphaned
        # old title as a duplicate so it can be cleaned up.
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
        dpla_id: str,
    ) -> None:
        """Tag a stranded file page as a duplicate after its hash was uploaded elsewhere."""
        try:
            old_page = get_page(self.site, f"File:{old_filename}")
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
                self.tracker.increment(Result.ITEM_NOT_PRESENT)
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

            for ordinal, _ in enumerate(
                tqdm(
                    files, desc="Uploading Files", leave=False, unit="File", ncols=100
                ),
                start=1,
            ):
                logging.info(f"Page {ordinal}")
                page_label = page_labels.get(ordinal, "")
                try:
                    self.process_file(
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
                    )
                except UploadTimeoutError as ex:
                    self.handle_upload_exception(ex)
                    break

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

        except Exception as ex:
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
def main(ids_file, partner: str, dry_run: bool, verbose: bool) -> None:
    start_time = time.time()
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
    )

    dpla = tools_context.get_dpla()
    local_fs = tools_context.get_local_fs()
    tracker = tools_context.get_tracker()

    dpla.check_partner(partner)

    try:
        local_fs.setup_temp_dir()
        setup_logging(partner, "upload", logging.INFO)
        notify_phase_start(partner, "upload")
        if dry_run:
            logging.warning("---=== DRY RUN ===---")

        providers_json = dpla.get_providers_data()
        logging.info(f"Starting upload for {partner}")

        dpla_ids = load_ids(ids_file)

        for dpla_id in tqdm(dpla_ids, desc="Uploading Items", unit="Item", ncols=100):
            uploader.process_item(dpla_id, providers_json, partner, verbose, dry_run)

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
        _post_upload_touch_new_institutions(commons_site, category_ensurer, dry_run)
        notify_upload_complete(
            tracker=tracker,
            partner_label=f"wikimedia-{partner}",
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
    commons_site, category_ensurer: CategoryEnsurer, dry_run: bool
) -> None:
    """At end-of-run, force-rerender files for any institutions whose P8464
    was first added this session — see touch_institution_files() docstring."""
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
