import csv
import hashlib
import logging
import os
import time

import click
import magic
from botocore.exceptions import ClientError
from mypy_boto3_s3.service_resource import S3ServiceResource

from common import (
    get_item_metadata,
    extract_urls,
    get_s3_path,
    get_temp_file,
    setup_temp_dir,
    cleanup_temp_dir,
    get_s3,
    Tracker,
    setup_logging,
    get_provider_and_data_provider,
    get_providers_data,
    is_wiki_eligible,
    Result,
    check_partner,
    provider_str,
    get_http_session,
    s3_file_exists,
)
from constants import (
    S3_BUCKET,
    S3_KEY_CHECKSUM,
    INVALID_CONTENT_TYPES,
    S3_KEY_METADATA,
    S3_KEY_CONTENT_TYPE,
)


def download_media(
    partner: str,
    dpla_id: str,
    ordinal: int,
    media_url: str,
    overwrite: bool,
    s3: S3ServiceResource,
    tracker: Tracker,
) -> None:
    temp_file = None
    try:
        destination_path = get_s3_path(dpla_id, ordinal, partner)
        if not overwrite and s3_file_exists(destination_path, s3):
            logging.info("Key already in S3.")
            tracker.increment(Result.SKIPPED)
            return
        temp_file = download_file_to_temp_path(media_url)
        content_type = get_content_type(temp_file)
        sha1 = get_file_hash(temp_file)
        upload_temp_file(
            content_type, destination_path, media_url, s3, sha1, temp_file, tracker
        )

    finally:
        if temp_file:
            temp_file.close()
            os.unlink(temp_file.name)


def upload_temp_file(
    content_type: str,
    destination_path: str,
    media_url: str,
    s3: S3ServiceResource,
    sha1: str,
    temp_file,
    tracker: Tracker,
):
    try:
        with open(temp_file.name, "rb") as file:
            obj = s3.Object(S3_BUCKET, destination_path)
            obj_metadata = None
            try:
                # this throws if obj doesn't exist yet
                obj_metadata = obj.metadata
            except ClientError as e:
                if not e.response["Error"]["Code"] == "404":
                    raise e

            if obj_metadata and obj_metadata.get(S3_KEY_CHECKSUM, None) == sha1:
                # Already uploaded, move on.
                logging.info("Already exists.")
                tracker.increment(Result.SKIPPED)
                return

            obj.upload_fileobj(
                Fileobj=file,
                ExtraArgs={
                    S3_KEY_CONTENT_TYPE: content_type,
                    S3_KEY_METADATA: {S3_KEY_CHECKSUM: sha1},
                },
            )
            tracker.increment(Result.DOWNLOADED)
    except Exception as e:
        raise Exception(
            f"Error uploading {media_url} to s3://{S3_BUCKET}/{destination_path}"
        ) from e


def get_file_hash(temp_file):
    return hashlib.file_digest(temp_file, S3_KEY_CHECKSUM).hexdigest()


def get_content_type(temp_file):
    content_type = magic.from_file(temp_file.name, mime=True)
    if content_type in INVALID_CONTENT_TYPES:
        raise Exception(f"Invalid content-type: {content_type}")
    return content_type


def download_file_to_temp_path(media_url: str):
    temp_file = get_temp_file()
    try:
        response = get_http_session().get(media_url)
        with open(temp_file.name, "wb") as f:
            f.write(response.content)

    except Exception as e:
        raise Exception(f"Failed saving {media_url} to local") from e
    return temp_file


@click.command()
@click.argument("ids_file", type=click.File("r"))
@click.argument("partner")
@click.argument("api_key")
@click.option("--dry-run", is_flag=True)
@click.option("--verbose", is_flag=True)
@click.option("--overwrite", is_flag=True)
def main(
    ids_file: str,
    partner: str,
    api_key: str,
    dry_run: bool,
    verbose: bool,
    overwrite: bool,
):
    start_time = time.time()
    tracker = Tracker()

    check_partner(partner)

    try:
        setup_temp_dir()
        setup_logging(partner, "download", logging.INFO)
        if dry_run:
            logging.warning("---=== DRY RUN ===---")

        s3 = get_s3()
        providers_json = get_providers_data()

        logging.info(f"Starting download for {partner}")

        csv_reader = csv.reader(ids_file)
        for row in csv_reader:
            dpla_id = row[0]
            logging.info(f"DPLA ID: {dpla_id}")
            try:
                item_metadata = get_item_metadata(dpla_id, api_key)
                provider, data_provider = get_provider_and_data_provider(
                    item_metadata, providers_json
                )
                if not is_wiki_eligible(item_metadata, provider, data_provider):
                    logging.info(f"{dpla_id} is not eligible.")
                    tracker.increment(Result.SKIPPED)
                    continue
                media_urls = extract_urls(item_metadata)
            except Exception as e:
                tracker.increment(Result.FAILED)
                logging.warning(
                    f"Caught exception getting media urls for {dpla_id}.", e
                )
                continue

            logging.info(f"{len(media_urls)} files.")
            count = 0
            if verbose:
                logging.info(f"DPLA ID: {dpla_id}")
                logging.info(f"Metadata: {item_metadata}")
                logging.info(f"Provider: {provider_str(provider)}")
                logging.info(f"Data Provider: {provider_str(data_provider)}")

            for media_url in media_urls:
                count += 1
                # hack to fix bad nara data
                if media_url.startswith("https/"):
                    media_url = media_url.replace("https/", "https:/")
                logging.info(
                    f"Downloading {partner} {dpla_id} {count} from {media_url}"
                )
                try:
                    if not dry_run:
                        download_media(
                            partner, dpla_id, count, media_url, overwrite, s3, tracker
                        )

                except Exception as e:
                    tracker.increment(Result.FAILED)
                    logging.warning(f"Failed: {str(e)}", exc_info=True, stack_info=True)

    finally:
        logging.info("\n" + str(tracker))
        logging.info(f"{time.time() - start_time} seconds.")
        cleanup_temp_dir()


if __name__ == "__main__":
    main()
