import json
import logging
import os
import time

from ingest_wikimedia.web import get_http_session
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
from ingest_wikimedia.metadata import (
    check_partner,
    get_item_metadata,
    get_provider_and_data_provider,
    get_providers_data,
    is_wiki_eligible,
    provider_str,
    MEDIA_MASTER_FIELD_NAME,
    IIIF_MANIFEST_FIELD_NAME,
    get_iiif_manifest,
    get_iiif_urls,
    check_record_partner,
)
from ingest_wikimedia.logs import setup_logging
from ingest_wikimedia.s3 import (
    get_s3,
    get_media_s3_path,
    s3_file_exists,
    S3_BUCKET,
    S3_KEY_METADATA,
    write_item_metadata,
    write_file_list,
    write_iiif_manifest,
)
from ingest_wikimedia.local import (
    cleanup_temp_dir,
    setup_temp_dir,
    get_file_hash,
    get_content_type,
    get_temp_file,
)
from ingest_wikimedia.tracker import Result, Tracker
from ingest_wikimedia.wikimedia import check_content_type


def upload_file_to_s3(file: str, destination_path: str, content_type: str, sha1: str):
    """
    Once we have a valid file to store in S3, this puts it there.
    """
    tracker = Tracker()
    try:
        with open(file, "rb") as file:
            s3 = get_s3()
            obj = s3.Object(S3_BUCKET, destination_path)
            obj_metadata = None
            try:
                # this throws if obj doesn't exist yet
                obj_metadata = obj.metadata
            except ClientError as e:
                if not e.response["Error"]["Code"] == "404":
                    raise e

            if obj_metadata and obj_metadata.get(CHECKSUM, None) == sha1:
                # Already uploaded, move on.
                logging.info("Already exists.")
                tracker.increment(Result.SKIPPED)
                return

            with tqdm(
                total=os.stat(file.name).st_size,
                desc="S3 Upload",
                leave=False,
                unit="B",
                unit_divisor=1024,
                unit_scale=True,
                delay=2,
            ) as t:
                obj.upload_fileobj(
                    Fileobj=file,
                    ExtraArgs={
                        CONTENT_TYPE: content_type,
                        S3_KEY_METADATA: {CHECKSUM: sha1},
                    },
                    Callback=lambda bytes_xfer: t.update(bytes_xfer),
                )
            tracker.increment(Result.DOWNLOADED)

    except Exception as e:
        raise Exception(
            f"Error uploading to s3://{S3_BUCKET}/{destination_path}"
        ) from e


def download_file_to_temp_path(media_url: str, local_file: str):
    """
    Tries to get a local copy of a file to stick in S3 later
    """
    try:
        response = get_http_session().get(media_url, stream=True)
        total_size = int(response.headers.get("content-length", 0))
        with tqdm(
            total=total_size,
            desc="HTTP Download",
            leave=False,
            unit="B",
            unit_divisor=1024,
            unit_scale=True,
            delay=2,
        ) as t:
            with open(local_file, "wb") as f:
                for chunk in response.iter_content(None):
                    t.update(len(chunk))
                    f.write(chunk)

    except Exception as e:
        raise Exception(f"Failed downloading {media_url} to local") from e


def process_media(
    partner: str,
    dpla_id: str,
    ordinal: int,
    media_url: str,
    overwrite: bool,
) -> None:
    """
    For a given capture for a given item, downloads it if we don't have it or are
    overwriting, gets the mime and sha1, and sticks it in S3.
    """
    temp_file = get_temp_file()
    temp_file_name = temp_file.name
    tracker = Tracker()
    try:
        destination_path = get_media_s3_path(dpla_id, ordinal, partner)
        if not overwrite and s3_file_exists(destination_path):
            logging.info("Key already in S3.")
            tracker.increment(Result.SKIPPED)
            return

        download_file_to_temp_path(media_url, temp_file_name)

        content_type = get_content_type(temp_file_name)
        if not check_content_type(content_type):
            logging.info(f"Bad content type: {content_type}")
            tracker.increment(Result.SKIPPED)
            return

        sha1 = get_file_hash(temp_file_name)
        upload_file_to_s3(temp_file_name, destination_path, content_type, sha1)

    finally:
        if temp_file:
            temp_file.close()
            os.unlink(temp_file.name)


def process_item(
    overwrite: bool,
    dry_run: bool,
    verbose: bool,
    partner: str,
    dpla_id: str,
    providers_json: dict,
    api_key: str,
) -> None:
    """
    For every item, makes sure it's eligible, tries to get a list of files for it, and
    stores the metadata in S3. Then calls process_media_file on the list.
    """
    tracker = Tracker()
    try:
        item_metadata = get_item_metadata(dpla_id, api_key)
        write_item_metadata(partner, dpla_id, json.dumps(item_metadata))
        provider, data_provider = get_provider_and_data_provider(
            item_metadata, providers_json
        )

        if not check_record_partner(partner, item_metadata):
            logging.info(f"{dpla_id} is from the wrong partner.")
            tracker.increment(Result.SKIPPED)
            return

        if not is_wiki_eligible(item_metadata, provider, data_provider):
            logging.info(f"{dpla_id} is not eligible.")
            tracker.increment(Result.SKIPPED)
            return

        if MEDIA_MASTER_FIELD_NAME in item_metadata:
            media_urls = get_list(item_metadata, MEDIA_MASTER_FIELD_NAME)

        elif IIIF_MANIFEST_FIELD_NAME in item_metadata:
            manifest_url = get_str(item_metadata, IIIF_MANIFEST_FIELD_NAME)
            manifest = get_iiif_manifest(manifest_url)
            write_iiif_manifest(partner, dpla_id, json.dumps(manifest))
            media_urls = get_iiif_urls(manifest)

        else:
            raise NotImplementedError(
                f"No {MEDIA_MASTER_FIELD_NAME} or {IIIF_MANIFEST_FIELD_NAME}"
            )

        write_file_list(partner, dpla_id, media_urls)

    except Exception as e:
        tracker.increment(Result.FAILED)
        logging.warning(
            f"Caught exception getting media urls for {dpla_id}.", exc_info=e
        )
        return

    logging.info(f"{len(media_urls)} files.")
    count = 0
    if verbose:
        logging.info(f"DPLA ID: {dpla_id}")
        logging.info(f"Metadata: {item_metadata}")
        logging.info(f"Provider: {provider_str(provider)}")
        logging.info(f"Data Provider: {provider_str(data_provider)}")

    for media_url in tqdm(
        media_urls, desc="Downloading Files", leave=False, unit="File"
    ):
        count += 1
        # hack to fix bad nara data
        if media_url.startswith("https/"):
            media_url = media_url.replace("https/", "https:/")
        logging.info(f"Downloading {partner} {dpla_id} {count} from {media_url}")
        try:
            if not dry_run:
                process_media(partner, dpla_id, count, media_url, overwrite)

        except Exception as e:
            tracker.increment(Result.FAILED)
            logging.warning(f"Failed: {dpla_id} {count}", exc_info=e)


@click.command()
@click.argument("ids_file", type=click.File("r"))
@click.argument("partner")
@click.argument("api_key")
@click.option("--dry-run", is_flag=True)
@click.option("--verbose", is_flag=True)
@click.option("--overwrite", is_flag=True)
def main(
    ids_file: IO,
    partner: str,
    api_key: str,
    dry_run: bool,
    verbose: bool,
    overwrite: bool,
):
    start_time = time.time()
    tracker = Tracker()
    setup_logging(partner, "download", logging.INFO)

    if dry_run:
        logging.warning("---=== DRY RUN ===---")

    check_partner(partner)
    logging.info(f"Starting download for {partner}")

    try:
        setup_temp_dir()

        providers_json = get_providers_data()
        dpla_ids = load_ids(ids_file)
        for dpla_id in tqdm(dpla_ids, desc="Downloading Items", unit="Item"):
            logging.info(f"DPLA ID: {dpla_id}")
            process_item(
                overwrite,
                dry_run,
                verbose,
                partner,
                dpla_id,
                providers_json,
                api_key,
            )

    finally:
        logging.info("\n" + str(tracker))
        logging.info(f"{time.time() - start_time} seconds.")
        cleanup_temp_dir()


if __name__ == "__main__":
    main()
