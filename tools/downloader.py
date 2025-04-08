import json
import logging
import os
import time

from requests import Session

from ingest_wikimedia.dpla import (
    DPLA,
    MEDIA_MASTER_FIELD_NAME,
    IIIF_MANIFEST_FIELD_NAME,
)
from ingest_wikimedia.iiif import IIIF
from ingest_wikimedia.localfs import LocalFS
from ingest_wikimedia.s3 import S3_BUCKET, S3_KEY_METADATA, S3Client
from typing import IO

import click
from botocore.exceptions import ClientError
from tqdm import tqdm

from ingest_wikimedia.common import (
    load_ids,
    get_list,
    get_str,
    CHECKSUM,
    CONTENT_TYPE,
)
from ingest_wikimedia.logs import setup_logging
from ingest_wikimedia.tools_context import ToolsContext
from ingest_wikimedia.tracker import Result, Tracker
from ingest_wikimedia.wikimedia import check_content_type


class Downloader:
    """
    A class to handle uploading files to S3.
    """

    def __init__(
        self,
        tracker: Tracker,
        s3_client: S3Client,
        http_session: Session,
        local_fs: LocalFS,
        dpla: DPLA,
        iiif: IIIF,
    ):
        self.tracker = tracker
        self.s3_client = s3_client
        self.http_session = http_session
        self.local_fs = local_fs
        self.dpla = dpla
        self.iiif = iiif

    def upload_file_to_s3(
        self,
        file: str,
        destination_path: str,
        content_type: str,
        sha1: str,
    ):
        """
        Once we have a valid file to store in S3, this puts it there.
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

                if obj_metadata and obj_metadata.get(CHECKSUM, None) == sha1:
                    # Already uploaded, move on.
                    logging.info("Already exists.")
                    self.tracker.increment(Result.SKIPPED)
                    return

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
                    for chunk in response.iter_content(None):
                        t.update(len(chunk))
                        f.write(chunk)

        except Exception as e:
            raise RuntimeError(f"Failed downloading {media_url} to local") from e

    def process_media(
        self,
        partner: str,
        dpla_id: str,
        ordinal: int,
        media_url: str,
        overwrite: bool,
        sleep_secs: float,
    ) -> None:
        """
        For a given capture for a given item, downloads it if we don't have it or are
        overwriting, gets the mime and sha1, and sticks it in S3.
        """
        temp_file = self.local_fs.get_temp_file()
        temp_file_name = temp_file.name

        try:
            destination_path = self.s3_client.get_media_s3_path(
                dpla_id, ordinal, partner
            )
            if not overwrite and self.s3_client.s3_file_exists(destination_path):
                logging.info("Key already in S3.")
                self.tracker.increment(Result.SKIPPED)
                return

            if sleep_secs != 0:
                time.sleep(sleep_secs)
            self.download_file_to_temp_path(media_url, temp_file_name)

            content_type = self.local_fs.get_content_type(temp_file_name)
            if not check_content_type(content_type):
                logging.info(f"Bad content type: {content_type}")
                self.tracker.increment(Result.SKIPPED)
                return

            sha1 = self.local_fs.get_file_hash(temp_file_name)
            self.upload_file_to_s3(temp_file_name, destination_path, content_type, sha1)
            self.tracker.increment(Result.DOWNLOADED)
            self.tracker.increment(Result.BYTES, os.stat(temp_file_name).st_size)

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
        providers_json: dict,
        api_key: str,
        sleep_secs: float,
    ) -> None:
        """
        For every item, makes sure it's eligible, tries to get a list of files for it, and
        stores the metadata in S3. Then calls process_media_file on the list.
        """

        try:
            item_metadata = self.dpla.get_item_metadata(dpla_id, api_key)

            if not item_metadata:
                logging.info(f"{dpla_id} was not found in the DPLA API.")
                self.tracker.increment(Result.SKIPPED)
                return

            self.s3_client.write_item_metadata(
                partner, dpla_id, json.dumps(item_metadata)
            )

            provider, data_provider = self.dpla.get_provider_and_data_provider(
                item_metadata, providers_json
            )

            if not self.dpla.check_record_partner(partner, item_metadata):
                logging.info(f"{dpla_id} is from the wrong partner.")
                self.tracker.increment(Result.SKIPPED)
                return

            if not self.dpla.is_wiki_eligible(
                dpla_id, item_metadata, provider, data_provider
            ):
                logging.info(f"{dpla_id} is not eligible.")
                self.tracker.increment(Result.SKIPPED)
                return

            if MEDIA_MASTER_FIELD_NAME in item_metadata:
                media_urls = get_list(item_metadata, MEDIA_MASTER_FIELD_NAME)

            elif IIIF_MANIFEST_FIELD_NAME in item_metadata:
                manifest_url = get_str(item_metadata, IIIF_MANIFEST_FIELD_NAME)
                manifest = self.iiif.get_iiif_manifest(manifest_url)
                if not manifest:
                    self.tracker.increment(Result.SKIPPED)
                    return
                self.s3_client.write_iiif_manifest(
                    partner, dpla_id, json.dumps(manifest)
                )
                media_urls = self.iiif.get_iiif_urls(manifest)

            else:
                # not sure how we got here
                self.tracker.increment(Result.SKIPPED)
                return

            self.s3_client.write_file_list(partner, dpla_id, media_urls)

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
            logging.info(f"Provider: {self.dpla.provider_str(provider)}")
            logging.info(f"Data Provider: {self.dpla.provider_str(data_provider)}")

        for media_url in tqdm(
            media_urls, desc="Downloading Files", leave=False, unit="File", ncols=100
        ):
            count += 1
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
                    sleep_secs,
                )


@click.command()
@click.argument("ids_file", type=click.File("r"))
@click.argument("partner")
@click.argument("api_key")
@click.option("--dry-run", is_flag=True)
@click.option("--verbose", is_flag=True)
@click.option("--overwrite", is_flag=True, help="Overwrite already downloaded media.")
@click.option(
    "--sleep",
    default=0.0,
    help="Interval to wait in between http requests in float seconds.",
)
def main(
    ids_file: IO,
    partner: str,
    api_key: str,
    dry_run: bool,
    verbose: bool,
    overwrite: bool,
    sleep: float,
):
    setup_logging(partner, "download", logging.INFO)
    start_time = time.time()
    tools_context = ToolsContext.init()

    downloader = Downloader(
        tools_context.get_tracker(),
        tools_context.get_s3_client(),
        tools_context.get_http_session(),
        tools_context.get_local_fs(),
        tools_context.get_dpla(),
        tools_context.get_iiif(),
    )

    if dry_run:
        logging.warning("---=== DRY RUN ===---")

    local_fs = tools_context.get_local_fs()
    dpla = tools_context.get_dpla()
    tracker = tools_context.get_tracker()

    dpla.check_partner(partner)
    logging.info(f"Starting download for {partner}")

    try:
        local_fs.setup_temp_dir()
        providers_json = dpla.get_providers_data()
        dpla_ids = load_ids(ids_file)
        for dpla_id in tqdm(dpla_ids, desc="Downloading Items", unit="Item", ncols=100):
            logging.info(f"DPLA ID: {dpla_id}")
            downloader.process_item(
                overwrite,
                dry_run,
                verbose,
                partner,
                dpla_id,
                providers_json,
                api_key,
                sleep,
            )

    finally:
        logging.info("\n" + str(tracker))
        logging.info(f"{time.time() - start_time} seconds.")
        local_fs.cleanup_temp_dir()


if __name__ == "__main__":
    main()
