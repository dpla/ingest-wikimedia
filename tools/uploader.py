import json
import logging
import mimetypes
import time

import click

from tqdm import tqdm

from ingest_wikimedia.common import (
    get_list,
    get_dict,
    load_ids,
    CHECKSUM,
)
from ingest_wikimedia.logs import setup_logging
from ingest_wikimedia.s3 import (
    get_media_s3_path,
    get_s3,
    S3_BUCKET,
    s3_file_exists,
    get_item_metadata,
    get_file_list,
)
from ingest_wikimedia.tracker import Result, Tracker
from ingest_wikimedia.local import (
    setup_temp_dir,
    cleanup_temp_dir,
    get_temp_file,
    clean_up_tmp_file,
)
from ingest_wikimedia.metadata import (
    check_partner,
    is_wiki_eligible,
    get_provider_and_data_provider,
    get_providers_data,
    provider_str,
    SOURCE_RESOURCE_FIELD_NAME,
    DC_TITLE_FIELD_NAME,
)
from ingest_wikimedia.wikimedia import (
    ERROR_FILEEXISTS,
    ERROR_MIME,
    ERROR_BANNED,
    ERROR_DUPLICATE,
    ERROR_NOCHANGE,
    WMC_UPLOAD_CHUNK_SIZE,
    IGNORE_WIKIMEDIA_WARNINGS,
    get_page_title,
    get_wiki_text,
    wikimedia_url,
    get_page,
    wiki_file_exists,
    check_content_type,
    get_site,
)


def process_file(
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
):
    tracker = Tracker()
    temp_file = get_temp_file()
    s3 = get_s3()
    site = get_site()

    try:
        wiki_markup = get_wiki_text(dpla_id, item_metadata, provider, data_provider)
        s3_path = get_media_s3_path(dpla_id, ordinal, partner)
        upload_comment = f'Uploading DPLA ID "[[dpla:{dpla_id}|{dpla_id}]]".'
        if not s3_file_exists(s3_path):
            logging.info(f"{dpla_id} {ordinal} not present.")
            tracker.increment(Result.SKIPPED)
            return

        s3_object = s3.Object(S3_BUCKET, s3_path)
        file_size = s3_object.content_length
        sha1 = s3_object.metadata.get(CHECKSUM, "")
        mime = s3_object.content_type

        if not check_content_type(mime):
            logging.info(f"Skipping {dpla_id} {ordinal}: Bad content type: {mime}")
            tracker.increment(Result.SKIPPED)
            return

        ext = mimetypes.guess_extension(mime)

        if not ext:
            logging.info(
                f"Skipping {dpla_id} {ordinal}: "
                f"Unable to guess extension for {mime}"
            )
            tracker.increment(Result.SKIPPED)
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
            logging.info(f"Provider: {provider_str(provider)}")
            logging.info(f"Data Provider: {provider_str(data_provider)}")
            logging.info(f"MIME: {mime}")
            logging.info(f"Extension: {ext}")
            logging.info(f"File size: {file_size}")
            logging.info(f"SHA-1: {sha1}")
            logging.info(f"Upload comment: {upload_comment}")
            logging.info(f"Wikitext: \n {wiki_markup}")

        if wiki_file_exists(sha1):
            logging.info(f"Skipping {dpla_id} {ordinal}: Already exists on commons.")
            tracker.increment(Result.SKIPPED)
            return

        if not dry_run:
            with tqdm(
                total=s3_object.content_length,
                leave=False,
                desc="S3 Download",
                unit="B",
                unit_scale=1024,
                unit_divisor=True,
                delay=2,
            ) as t:
                s3_object.download_file(
                    temp_file.name,
                    Callback=lambda bytes_xfer: t.update(bytes_xfer),
                )

            wiki_file_page = get_page(site, page_title)

            result = site.upload(
                filepage=wiki_file_page,
                source_filename=temp_file.name,
                comment=upload_comment,
                text=wiki_markup,
                ignore_warnings=IGNORE_WIKIMEDIA_WARNINGS,
                asynchronous=True,
                chunk_size=WMC_UPLOAD_CHUNK_SIZE,
            )

            if not result:
                # These error message accounts for Page does not exist,
                # but File does exist and is linked to another Page
                # (ex. DPLA ID drift)
                tracker.increment(Result.FAILED)
                raise Exception("File linked to another page (possible ID drift)")

            logging.info(f"Uploaded to {wikimedia_url(page_title)}")
            tracker.increment(Result.UPLOADED)
            tracker.increment(Result.BYTES, file_size)

    except Exception as ex:
        handle_upload_exception(ex)

    finally:
        clean_up_tmp_file(temp_file)


def process_item(
    dpla_id: str,
    providers_json: dict,
    partner: str,
    verbose: bool,
    dry_run: bool,
):
    tracker = Tracker()
    try:
        logging.info(f"DPLA ID: {dpla_id}")

        item_metadata = json.loads(get_item_metadata(partner, dpla_id))

        provider, data_provider = get_provider_and_data_provider(
            item_metadata, providers_json
        )

        if not is_wiki_eligible(item_metadata, provider, data_provider):
            tracker.increment(Result.SKIPPED)
            return

        titles = get_list(
            get_dict(item_metadata, SOURCE_RESOURCE_FIELD_NAME),
            DC_TITLE_FIELD_NAME,
        )

        # playing it safe in case titles is empty
        title = titles[0] if titles else ""

        ordinal = 0
        files = get_file_list(partner, dpla_id)

        for _ in tqdm(files, desc="Uploading Files", leave=False, unit="File"):
            ordinal += 1  # todo should this come from the list or the name?
            logging.info(f"Page {ordinal}")
            # one-pagers don't have page numbers in their titles
            page_label = None if len(files) == 1 else ordinal
            process_file(
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
            )

    except Exception as ex:
        logging.warning(
            f"Caught exception getting item info for {dpla_id}", exc_info=ex
        )
        tracker.increment(Result.FAILED)


@click.command()
@click.argument("ids_file", type=click.File("r"))
@click.argument("partner")
@click.option("--dry-run", is_flag=True)
@click.option("--verbose", is_flag=True)
def main(ids_file, partner: str, dry_run: bool, verbose: bool) -> None:
    start_time = time.time()
    tracker = Tracker()

    check_partner(partner)

    try:
        setup_temp_dir()
        setup_logging(partner, "upload", logging.INFO)
        if dry_run:
            logging.warning("---=== DRY RUN ===---")

        providers_json = get_providers_data()
        logging.info(f"Starting upload for {partner}")

        dpla_ids = load_ids(ids_file)

        for dpla_id in tqdm(dpla_ids, desc="Uploading Items", unit="Item"):
            process_item(dpla_id, providers_json, partner, verbose, dry_run)

    finally:
        logging.info("\n" + str(tracker))
        logging.info(f"{time.time() - start_time} seconds.")
        cleanup_temp_dir()


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

    if error:
        logging.error(f"Failed: {message}", exc_info=ex)
    else:
        logging.warning(f"Failed: {message}", exc_info=ex)


if __name__ == "__main__":
    main()
