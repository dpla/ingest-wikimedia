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
    A class to handle uploading files to S3.
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
    ) -> bool:
        """
        Uploads the file to S3. Returns True if the file was uploaded, False if it
        already existed with a matching checksum and was skipped.
        """
        try:
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

                # Don't overwrite a valid existing S3 file with a 0-byte download
                # (e.g. a transient empty HTTP response from the source server).
                # Use `is not None` rather than truthiness — empty metadata dicts ({})
                # are falsy but still indicate the object exists.
                # Only access obj.content_length when obj_metadata is not None: accessing
                # it on a non-existent object triggers a second HEAD request (another 404).
                if obj_metadata is not None and os.stat(file).st_size == 0:
                    existing_size = int(obj.content_length or 0)
                    if existing_size > 0:
                        logging.warning(
                            f"New download is 0 bytes; keeping existing file at "
                            f"s3://{S3_BUCKET}/{destination_path}"
                        )
                        self.tracker.increment(Result.SKIPPED)
                        return False

                if obj_metadata and obj_metadata.get(CHECKSUM) == sha1:
                    try:
                        if int(obj.content_length) > 0:
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
        Tries to get a local copy of a file to stick in S3 later
        """
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

        except Exception as e:
            raise RuntimeError(f"Failed downloading {media_url} to local") from e

    def _s3_key_age_days(self, s3_path: str) -> float | None:
        """Return the age in days of an S3 object, or None if it does not exist."""
        try:
            obj = self.s3_client.get_s3().Object(S3_BUCKET, s3_path)
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
    ) -> None:
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
        """
        temp_file = self.local_fs.get_temp_file()
        temp_file_name = temp_file.name

        try:
            destination_path = self.s3_client.get_media_s3_path(
                dpla_id, ordinal, partner
            )
            if not overwrite:
                age_days = self._s3_key_age_days(destination_path)
                if age_days is not None:
                    if max_age_days is None or age_days < max_age_days:
                        logging.info("Key already in S3.")
                        self.tracker.increment(Result.SKIPPED)
                        return
                    logging.info(
                        f"Refreshing {dpla_id} {ordinal}: S3 file is "
                        f"{age_days:.0f} days old (threshold: {max_age_days})."
                    )

            if sleep_secs != 0:
                time.sleep(sleep_secs)
            self.download_file_to_temp_path(media_url, temp_file_name)

            content_type = self.local_fs.get_content_type(temp_file_name)
            if not check_content_type(content_type):
                logging.info(f"Bad content type: {content_type}")
                self.tracker.increment(Result.SKIPPED)
                return

            sha1 = self.local_fs.get_file_hash(temp_file_name)

            for attempt in range(1, CREDENTIAL_RETRY_MAX + 1):
                try:
                    if self.upload_file_to_s3(
                        temp_file_name, destination_path, content_type, sha1
                    ):
                        self.tracker.increment(
                            Result.BYTES, os.stat(temp_file_name).st_size
                        )
                    return
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

        finally:
            self.local_fs.clean_up_tmp_file(temp_file)

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

        for media_url in tqdm(
            media_urls, desc="Downloading Files", leave=False, unit="File", ncols=100
        ):
            count += 1
            if not media_url:
                logging.warning(f"Skipping {dpla_id} ordinal {count}: empty URL.")
                self.tracker.increment(Result.SKIPPED)
                continue
            # hack to fix bad nara data
            if media_url.startswith("https/"):
                media_url = media_url.replace("https/", "https:/")
            logging.info(f"Downloading {partner} {dpla_id} {count} from {media_url}")
            if not dry_run:
                self.process_media(
                    partner,
                    dpla_id,
                    count,
                    media_url,
                    overwrite,
                    max_age_days,
                    sleep_secs,
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
