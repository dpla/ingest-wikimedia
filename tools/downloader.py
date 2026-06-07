import json
import logging
import os
import time
from datetime import datetime, timezone

from ingest_wikimedia.dpla import (
    MEDIA_MASTER_FIELD_NAME,
    IIIF_MANIFEST_FIELD_NAME,
)
from ingest_wikimedia.iiif import IIIF
from ingest_wikimedia.localfs import LocalFS
from ingest_wikimedia.s3 import S3_BUCKET, S3_KEY_METADATA, S3Client, FILE_LIST_TXT
from typing import IO

import click
from botocore.exceptions import ClientError, CredentialRetrievalError
from tqdm import tqdm

from ingest_wikimedia.common import (
    load_ids,
    get_list,
    get_str,
    CHECKSUM,
    CONTENT_TYPE,
)
from ingest_wikimedia.logs import setup_logging
from ingest_wikimedia.slack import notify_download_complete, notify_phase_start
from ingest_wikimedia.tools_context import ToolsContext
from ingest_wikimedia.tracker import Result, Tracker
from ingest_wikimedia.web import Web
from ingest_wikimedia.wikimedia import check_content_type

DOWNLOAD_BUFFER_SIZE = 4 * 1024 * 1024  # 4 MB
CREDENTIAL_RETRY_MAX = 3
CREDENTIAL_RETRY_BASE_DELAY_SECS = 5


class Downloader:
    """
    Downloads partner media files from remote sources (IIIF manifests or
    direct URLs in DPLA item metadata) and stages them to S3 for the
    uploader to consume.
    """

    def __init__(
        self,
        provider: str,
        tracker: Tracker,
        s3_client: S3Client,
        web: Web,
        local_fs: LocalFS,
        iiif: IIIF,
    ):
        self.provider = provider
        self.tracker = tracker
        self.s3_client = s3_client
        self.local_fs = local_fs
        self.iiif = iiif
        self.http_session = web.get_http_session(provider=provider)

    def upload_file_to_s3(
        self,
        file: str,
        destination_path: str,
        content_type: str,
        sha1: str,
        force_overwrite: bool = False,
    ) -> bool:
        """
        Uploads the file to S3. Returns True if the file was uploaded (or
        touched via copy-self), False if it already existed with a matching
        checksum and the caller did not request a forced overwrite.

        `force_overwrite=True` is set by `process_media` whenever the caller
        intends the resulting S3 object to reflect a fresh fetch — either
        the caller passed `overwrite=True`, or the per-key age check
        decided the existing S3 object was too old to trust. Without this
        flag, an existing-SHA1 match silently skipped the upload, leaving
        the S3 `LastModified` timestamp pinned to the original (long-ago)
        write. Every subsequent refresh run then saw the same stale
        timestamp, re-downloaded the same bytes, hit the same skip, and
        the file's age in S3 never moved — wasting hours of network and
        compute on every periodic refresh.
        """
        try:
            # Defence-in-depth: refuse to upload a 0-byte local file regardless
            # of whether an S3 object already exists. The primary check lives
            # in download_file_to_temp_path (which raises before reaching this
            # point), but if anything ever calls upload_file_to_s3 with a
            # 0-byte file, we must not turn it into a stub. Stubs poison the
            # uploader's page-label counter and require a subsequent re-run
            # to heal — see the "Graceful failure handling: audit ALL code
            # paths" lesson.
            if os.stat(file).st_size == 0:
                logging.warning(
                    f"Refusing to upload 0-byte file to "
                    f"s3://{S3_BUCKET}/{destination_path}; "
                    f"treat as failed download"
                )
                self.tracker.increment(Result.FAILED)
                return False

            with open(file, "rb") as f:
                s3 = self.s3_client.get_s3()
                obj = s3.Object(S3_BUCKET, destination_path)
                obj_metadata = None
                try:
                    # this throws if obj doesn't exist yet
                    obj_metadata = obj.metadata
                except ClientError as e:
                    if "Error" in e.response and "Code" in e.response["Error"]:
                        if e.response["Error"]["Code"] != "404":
                            raise e
                    else:
                        # Just in case (dunno why this would happen)
                        raise e

                if obj_metadata and obj_metadata.get(CHECKSUM) == sha1:
                    try:
                        if int(obj.content_length) > 0:
                            if force_overwrite:
                                # Refresh / overwrite path: SHA1 matches the
                                # existing S3 object, so re-uploading the same
                                # bytes is wasteful. Use copy-object onto the
                                # same key to update LastModified without
                                # re-transferring the body. MetadataDirective
                                # "REPLACE" silently drops any metadata not
                                # explicitly passed (per lessons.md
                                # "AWS S3 copy_object with MetadataDirective"),
                                # so we re-supply the full metadata dict.
                                logging.info(
                                    "Already at correct SHA1; touching S3 "
                                    "LastModified via copy-object."
                                )
                                new_metadata = dict(obj_metadata)
                                new_metadata[CHECKSUM] = sha1
                                s3.meta.client.copy_object(
                                    Bucket=S3_BUCKET,
                                    Key=destination_path,
                                    CopySource={
                                        "Bucket": S3_BUCKET,
                                        "Key": destination_path,
                                    },
                                    ContentType=content_type,
                                    Metadata=new_metadata,
                                    MetadataDirective="REPLACE",
                                )
                                self.tracker.increment(Result.DOWNLOADED)
                                return True
                            logging.info("Already exists.")
                            self.tracker.increment(Result.SKIPPED)
                            return False
                    except (TypeError, ValueError):
                        pass  # zero-byte stub or unreadable length — fall through to upload

                with tqdm(
                    total=os.stat(f.name).st_size,
                    desc="S3 Upload",
                    leave=False,
                    unit="B",
                    unit_divisor=1024,
                    unit_scale=True,
                    delay=2,
                    ncols=100,
                ) as t:
                    obj.upload_fileobj(
                        Fileobj=f,
                        ExtraArgs={
                            CONTENT_TYPE: content_type,
                            S3_KEY_METADATA: {CHECKSUM: sha1},
                        },
                        Callback=lambda bytes_xfer: t.update(bytes_xfer),
                    )
                self.tracker.increment(Result.DOWNLOADED)
                return True

        except CredentialRetrievalError:
            raise  # let the caller's retry loop handle transient credential blips
        except Exception as e:
            raise RuntimeError(
                f"Error uploading to s3://{S3_BUCKET}/{destination_path}"
            ) from e

    def download_file_to_temp_path(self, media_url: str, local_file: str):
        """
        Tries to get a local copy of a file to stick in S3 later.

        Raises RuntimeError if the download fails OR if the response body
        was empty. A 0-byte download (HTTP 200 + empty body, or a clean
        close after the headers with no chunks) must be treated as a
        failure: silently accepting it would cause upload_file_to_s3 to
        write a 0-byte stub to S3 (the existing 0-byte guard there only
        protects against overwriting an *existing* non-zero S3 file,
        not against creating a stub from scratch). Stubs in S3 then
        poison the uploader's page-label counter and have to be healed
        on a subsequent run.
        """
        bytes_written = 0
        try:
            response = self.http_session.get(media_url, stream=True)
            response.raise_for_status()
            total_size = int(response.headers.get("content-length", 0))
            with tqdm(
                total=total_size,
                desc="HTTP Download",
                leave=False,
                unit="B",
                unit_divisor=1024,
                unit_scale=True,
                delay=2,
                ncols=100,
            ) as t:
                with open(local_file, "wb") as f:
                    for chunk in response.iter_content(DOWNLOAD_BUFFER_SIZE):
                        t.update(len(chunk))
                        f.write(chunk)
                        bytes_written += len(chunk)

        except Exception as e:
            raise RuntimeError(f"Failed downloading {media_url} to local") from e

        if bytes_written == 0:
            raise RuntimeError(
                f"Downloaded 0 bytes from {media_url} — treating as failure "
                f"(HTTP 200 + empty body or stalled stream)"
            )

    def _s3_key_age_days(self, s3_path: str) -> float | None:
        """Return the age in days of an S3 object, or None if it does not exist
        or is a 0-byte stub left by a failed/interrupted download.

        Treating 0-byte stubs as absent forces the downloader to re-attempt,
        matching the design intent documented on S3Client.s3_file_exists.
        Without this, a single corrupted download persists forever — the
        uploader's pre-scan classifies the stub as "" placeholder, which
        shifts every subsequent page-label and corrupts Commons numbering.
        """
        try:
            obj = self.s3_client.get_s3().Object(S3_BUCKET, s3_path)
            if int(obj.content_length or 0) == 0:
                return None
            age = datetime.now(tz=timezone.utc) - obj.last_modified
            return age.total_seconds() / 86400
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "404":
                return None
            raise

    def process_media(
        self,
        partner: str,
        dpla_id: str,
        ordinal: int,
        media_url: str,
        overwrite: bool,
        max_age_days: int | None,
        sleep_secs: float,
    ) -> str:
        """
        For a given capture for a given item, downloads it if we don't have it or are
        overwriting, gets the mime and sha1, and sticks it in S3.

        If max_age_days is set, files already in S3 are re-downloaded once they are
        older than that threshold. The existing S3 file is kept if the new download
        is 0 bytes (see upload_file_to_s3).

        upload_file_to_s3() is retried on CredentialRetrievalError: EC2 instance profile
        credentials occasionally fail to refresh from IMDS, causing a brief blip that
        resolves within seconds. Only the S3 upload is retried — the HTTP download is
        not repeated, since the file is already on disk when the upload step fails.

        Returns a per-ordinal status string used by ``process_item`` to emit a
        single-line per-item summary at the end of the ordinal loop. The
        existing global ``self.tracker`` counters are still incremented as
        before — the return value is purely so callers can show fine-grained
        per-item progress without re-scanning the log:

        - ``"SKIPPED"``   — S3 already had a fresh-enough key, no network work
        - ``"FETCHED"``   — first-time fresh download to S3
        - ``"REFRESHED"`` — S3 had a stale key (older than ``max_age_days``);
                            re-downloaded and overwritten
        - ``"FAILED"``    — the ordinal raised at some point
        """
        temp_file = self.local_fs.get_temp_file()
        temp_file_name = temp_file.name

        try:
            destination_path = self.s3_client.get_media_s3_path(
                dpla_id, ordinal, partner
            )
            # Track whether we're proceeding because the caller asked to
            # overwrite or because the age check decided to refresh. Either
            # way, upload_file_to_s3 must persist the new write even when
            # the SHA1 matches what's already in S3 — otherwise the
            # LastModified timestamp never advances and the file is
            # re-attempted indefinitely on subsequent refresh runs.
            is_refresh = False
            if not overwrite:
                age_days = self._s3_key_age_days(destination_path)
                if age_days is not None:
                    if max_age_days is None or age_days < max_age_days:
                        logging.info("Key already in S3.")
                        self.tracker.increment(Result.SKIPPED)
                        return "SKIPPED"
                    logging.info(
                        f"Refreshing {dpla_id} {ordinal}: S3 file is "
                        f"{age_days:.0f} days old (threshold: {max_age_days})."
                    )
                    is_refresh = True

            if sleep_secs != 0:
                time.sleep(sleep_secs)
            # Time the fetch + S3 upload pair so the per-ordinal "Fetched"
            # line below reports honest wall-clock cost. ``Downloading`` (the
            # pre-check line in process_item) is emitted for every ordinal
            # regardless of whether work happens, which makes log volume
            # alone a poor proxy for actual network time — the "Fetched"
            # line fires only when bytes really moved.
            fetch_start = time.time()
            self.download_file_to_temp_path(media_url, temp_file_name)

            content_type = self.local_fs.get_content_type(temp_file_name)
            if not check_content_type(content_type):
                logging.info(f"Bad content type: {content_type}")
                self.tracker.increment(Result.SKIPPED)
                return "SKIPPED"

            sha1 = self.local_fs.get_file_hash(temp_file_name)

            force_overwrite = overwrite or is_refresh
            for attempt in range(1, CREDENTIAL_RETRY_MAX + 1):
                try:
                    if self.upload_file_to_s3(
                        temp_file_name,
                        destination_path,
                        content_type,
                        sha1,
                        force_overwrite=force_overwrite,
                    ):
                        size_bytes = os.stat(temp_file_name).st_size
                        self.tracker.increment(Result.BYTES, size_bytes)
                        elapsed = time.time() - fetch_start
                        # ADDITIVE companion to the pre-check ``Downloading``
                        # line: emitted only when a network fetch actually
                        # happened (fresh or refresh). Grep history for the
                        # old ``Downloading nara`` pattern is unchanged; the
                        # new ``Fetched nara`` pattern lets you count real
                        # network work and visually distinguish skips in the
                        # log live.
                        logging.info(
                            f"Fetched {partner} {dpla_id} {ordinal}"
                            f" ({size_bytes:,} bytes, {elapsed:.1f}s)."
                        )
                    return "REFRESHED" if is_refresh else "FETCHED"
                except CredentialRetrievalError as e:
                    if attempt < CREDENTIAL_RETRY_MAX:
                        delay = CREDENTIAL_RETRY_BASE_DELAY_SECS * (2 ** (attempt - 1))
                        logging.warning(
                            f"IAM credential refresh failed for {dpla_id} {ordinal} "
                            f"(attempt {attempt}/{CREDENTIAL_RETRY_MAX}), "
                            f"retrying in {delay}s: {e}"
                        )
                        time.sleep(delay)
                    else:
                        raise

        except Exception as e:
            self.tracker.increment(Result.FAILED)
            logging.warning(f"Failed: {dpla_id} {ordinal}", exc_info=e)
            return "FAILED"

        finally:
            self.local_fs.clean_up_tmp_file(temp_file)

        # Defensive: unreachable in normal flow because every branch above
        # either returns explicitly or falls through to the exception
        # handler. Returning a known status here keeps callers' per-item
        # tallies sound if the control flow ever evolves.
        return "FAILED"

    def process_item(
        self,
        overwrite: bool,
        dry_run: bool,
        verbose: bool,
        partner: str,
        dpla_id: str,
        sleep_secs: float,
        max_age_days: int | None = 365,
    ) -> None:
        """
        For every item, tries to get a list of files for it and stores the
        metadata in S3. Then calls process_media_file on the list.

        Eligibility is enforced at ID generation time (get-ids-es), so no
        runtime eligibility check is performed here. If staged metadata is
        missing or lacks the get-ids-es marker, the item is skipped — re-run
        get-ids-es to regenerate it.
        """

        try:
            item_metadata_str = self.s3_client.get_item_metadata(partner, dpla_id)
            item_metadata = None
            if item_metadata_str:
                try:
                    candidate = json.loads(item_metadata_str)
                    if candidate.get("_staged_by_get_ids_es"):
                        item_metadata = candidate
                except (json.JSONDecodeError, AttributeError):
                    pass  # Malformed metadata — treated as absent; handled below.

            if item_metadata is None:
                # Metadata missing or lacks the get-ids-es staging marker.
                # Re-run get-ids-es to regenerate staged metadata for this partner.
                logging.warning(f"{dpla_id} has no valid staged metadata; skipping.")
                self.tracker.increment(Result.SKIPPED)
                return

            if MEDIA_MASTER_FIELD_NAME in item_metadata:
                media_urls = get_list(item_metadata, MEDIA_MASTER_FIELD_NAME)
                self.s3_client.write_file_list(partner, dpla_id, media_urls)

            elif IIIF_MANIFEST_FIELD_NAME in item_metadata:
                cached_urls = self.s3_client.get_file_list(partner, dpla_id)
                use_cache = not overwrite and bool(cached_urls)
                if use_cache and max_age_days is not None:
                    file_list_path = self.s3_client.get_item_s3_path(
                        dpla_id, FILE_LIST_TXT, partner
                    )
                    cache_age = self._s3_key_age_days(file_list_path)
                    # cache_age is None means the key vanished since get_file_list
                    # read it (TOCTOU); treat as stale so the manifest is re-fetched.
                    if cache_age is None or cache_age >= max_age_days:
                        use_cache = False
                if use_cache:
                    media_urls = cached_urls
                else:
                    manifest_url = get_str(item_metadata, IIIF_MANIFEST_FIELD_NAME)
                    manifest = self.iiif.get_iiif_manifest(manifest_url)
                    if not manifest:
                        logging.warning(
                            f"Could not retrieve IIIF manifest for {dpla_id}: {manifest_url}"
                        )
                        self.tracker.increment(Result.FAILED)
                        return
                    self.s3_client.write_iiif_manifest(
                        partner, dpla_id, json.dumps(manifest)
                    )
                    media_urls = self.iiif.get_iiif_urls(manifest)
                    if not media_urls:
                        logging.warning(
                            f"No image URLs extracted from IIIF manifest for {dpla_id}: {manifest_url}"
                        )
                        self.tracker.increment(Result.FAILED)
                        return
                    self.s3_client.write_file_list(partner, dpla_id, media_urls)

            else:
                # item metadata has neither media_master nor IIIF manifest field
                self.tracker.increment(Result.SKIPPED)
                return

        except Exception as e:
            self.tracker.increment(Result.FAILED)
            logging.warning(
                f"Caught exception getting media urls for {dpla_id}.", exc_info=e
            )
            return

        logging.info(f"{len(media_urls)} files.")
        count = 0
        if verbose:
            logging.info(f"DPLA ID: {dpla_id}")
            logging.info(f"Metadata: {item_metadata}")

        # Per-item tally for the end-of-item summary line. Keeps the
        # global ``self.tracker`` counters untouched (they continue to
        # accumulate across the whole run); these locals exist only to
        # produce the one ``Item {dpla_id}: ...`` summary at the end of
        # the ordinal loop. Lets operators grep
        # ``grep "Item .*fetched=[1-9]"`` to see which items actually
        # had network work, vs scrolling thousands of per-ordinal lines.
        item_counts = {"SKIPPED": 0, "FETCHED": 0, "REFRESHED": 0, "FAILED": 0}

        for media_url in tqdm(
            media_urls, desc="Downloading Files", leave=False, unit="File", ncols=100
        ):
            count += 1
            if not media_url:
                logging.warning(f"Skipping {dpla_id} ordinal {count}: empty URL.")
                self.tracker.increment(Result.SKIPPED)
                item_counts["SKIPPED"] += 1
                continue
            # NARA item URLs sporadically arrive with a malformed scheme of
            # "https/..." (missing the colon) — repair before requesting.
            # Root cause is upstream NARA metadata; safe to patch here because
            # any genuine non-URL starting with "https/" would already be
            # rejected by the HTTP fetch below.
            if media_url.startswith("https/"):
                media_url = media_url.replace("https/", "https:/")
            logging.info(f"Downloading {partner} {dpla_id} {count} from {media_url}")
            if not dry_run:
                status = self.process_media(
                    partner,
                    dpla_id,
                    count,
                    media_url,
                    overwrite,
                    max_age_days,
                    sleep_secs,
                )
                item_counts[status] = item_counts.get(status, 0) + 1

        # Per-item summary: one concise line operators can grep to find
        # items that actually had network work (vs scrolling thousands of
        # per-ordinal lines). Companion to the per-ordinal ``Fetched``
        # line emitted by ``process_media`` — together they make the log
        # honest about how much of the work was real downloads vs how
        # much was already-staged-skip churn.
        if not dry_run:
            logging.info(
                f"Item {dpla_id}: {len(media_urls)} ordinals"
                f" (skipped={item_counts['SKIPPED']},"
                f" fetched={item_counts['FETCHED']},"
                f" refreshed={item_counts['REFRESHED']},"
                f" failed={item_counts['FAILED']})."
            )


@click.command()
@click.argument("ids-file", type=click.File("r"))
@click.argument("partner")
@click.option("--dry-run", is_flag=True)
@click.option("--verbose", is_flag=True)
@click.option("--overwrite", is_flag=True, help="Overwrite already downloaded media.")
@click.option(
    "--sleep",
    default=0.0,
    help="Interval to wait in between http requests in float seconds.",
)
@click.option(
    "--max-age-days",
    default=365,
    type=click.IntRange(min=0),
    help="Re-download files already in S3 if older than N days (default: 365).",
)
@click.option(
    "--notify-complete",
    is_flag=True,
    help="Post a download-complete summary to Slack when finished (used for refresh runs).",
)
def main(
    ids_file: IO,
    partner: str,
    dry_run: bool,
    verbose: bool,
    overwrite: bool,
    sleep: float,
    max_age_days: int | None,
    notify_complete: bool,
):
    setup_logging(partner, "download", logging.INFO)
    start_time = time.time()
    tools_context = ToolsContext.init(partner)

    downloader = Downloader(
        partner,
        tools_context.get_tracker(),
        tools_context.get_s3_client(),
        tools_context.get_web(),
        tools_context.get_local_fs(),
        tools_context.get_iiif(),
    )

    if dry_run:
        logging.warning("---=== DRY RUN ===---")

    local_fs = tools_context.get_local_fs()
    dpla = tools_context.get_dpla()
    tracker = tools_context.get_tracker()

    dpla.check_partner(partner)
    notify_phase_start(partner, "download")
    logging.info(f"Starting download for {partner}")

    try:
        local_fs.setup_temp_dir()
        dpla_ids = load_ids(ids_file)
        for dpla_id in tqdm(dpla_ids, desc="Downloading Items", unit="Item", ncols=100):
            logging.info(f"DPLA ID: {dpla_id}")
            downloader.process_item(
                overwrite,
                dry_run,
                verbose,
                partner,
                dpla_id,
                sleep,
                max_age_days,
            )

    finally:
        elapsed = time.time() - start_time
        logging.info("\n" + str(tracker))
        logging.info(f"{elapsed} seconds.")
        local_fs.cleanup_temp_dir()
        if notify_complete:
            notify_download_complete(
                tracker=tracker,
                partner_label=partner,
                elapsed_seconds=elapsed,
                dry_run=dry_run,
            )


if __name__ == "__main__":
    main()
