import json
import logging
import mimetypes
import time

import click
from pywikibot.site import BaseSite
from requests import Session

from tqdm import tqdm

from ingest_wikimedia.common import (
    get_list,
    get_dict,
    load_ids,
    CHECKSUM,
)
from ingest_wikimedia.localfs import LocalFS
from ingest_wikimedia.logs import setup_logging
from ingest_wikimedia.s3 import (
    S3_BUCKET,
    S3Client,
)
from ingest_wikimedia.tools_context import ToolsContext
from ingest_wikimedia.tracker import Result, Tracker
from ingest_wikimedia.dpla import (
    SOURCE_RESOURCE_FIELD_NAME,
    DC_TITLE_FIELD_NAME,
    DPLA,
)
from ingest_wikimedia.wikimedia import (
    WMC_UPLOAD_CHUNK_SIZE,
    IGNORE_WIKIMEDIA_WARNINGS,
    get_page_title,
    get_wiki_text,
    wikimedia_url,
    wiki_file_exists,
    check_content_type,
    get_page,
    ERROR_FILEEXISTS,
    ERROR_MIME,
    ERROR_BANNED,
    ERROR_DUPLICATE,
    ERROR_NOCHANGE,
    get_site,
)


class Uploader:
    def __init__(
        self,
        tracker: Tracker,
        local_fs: LocalFS,
        s3_client: S3Client,
        dpla: DPLA,
        http_session: Session,
        site: BaseSite,
    ):
        self.tracker = tracker
        self.local_fs = local_fs
        self.s3_client = s3_client
        self.site = site
        self.dpla = dpla
        self.http_session = http_session

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
            sha1 = s3_object.metadata.get(CHECKSUM, "")
            mime = s3_object.content_type

            if not check_content_type(mime):
                logging.info(f"Skipping {dpla_id} {ordinal}: Bad content type: {mime}")
                self.tracker.increment(Result.SKIPPED)
                return

            ext = mimetypes.guess_extension(mime)

            if not ext:
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

            if wiki_file_exists(self.site, sha1):
                logging.info(
                    f"Skipping {dpla_id} {ordinal}: Already exists on commons."
                )
                self.tracker.increment(Result.SKIPPED)
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
                    ncols=100,
                ) as t:
                    s3_object.download_file(
                        temp_file.name,
                        Callback=lambda bytes_xfer: t.update(bytes_xfer),
                    )

                wiki_file_page = get_page(self.site, page_title)

                result = self.site.upload(
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
                    self.tracker.increment(Result.FAILED)
                    raise RuntimeError(
                        "File linked to another page (possible ID drift)"
                    )

                logging.info(f"Uploaded to {wikimedia_url(page_title)}")
                self.tracker.increment(Result.UPLOADED)
                self.tracker.increment(Result.BYTES, file_size)

        except Exception as ex:
            self.handle_upload_exception(ex)

        finally:
            self.local_fs.clean_up_tmp_file(temp_file)

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

            titles = get_list(
                get_dict(item_metadata, SOURCE_RESOURCE_FIELD_NAME),
                DC_TITLE_FIELD_NAME,
            )

            # playing it safe in case titles is empty
            title = titles[0] if titles else ""

            ordinal = 0
            files = self.s3_client.get_file_list(partner, dpla_id)

            for _ in tqdm(
                files, desc="Uploading Files", leave=False, unit="File", ncols=100
            ):
                ordinal += 1
                logging.info(f"Page {ordinal}")
                # one-pagers don't have page numbers in their titles
                page_label = "" if len(files) == 1 else str(ordinal)
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
                )

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
    tools_context = ToolsContext.init()

    uploader = Uploader(
        tools_context.get_tracker(),
        tools_context.get_local_fs(),
        tools_context.get_s3_client(),
        tools_context.get_dpla(),
        tools_context.get_http_session(),
        get_site(),
    )

    dpla = tools_context.get_dpla()
    local_fs = tools_context.get_local_fs()
    tracker = tools_context.get_tracker()

    dpla.check_partner(partner)

    try:
        local_fs.setup_temp_dir()
        setup_logging(partner, "upload", logging.INFO)
        if dry_run:
            logging.warning("---=== DRY RUN ===---")

        providers_json = dpla.get_providers_data()
        logging.info(f"Starting upload for {partner}")

        dpla_ids = load_ids(ids_file)

        for dpla_id in tqdm(dpla_ids, desc="Uploading Items", unit="Item", ncols=100):
            uploader.process_item(dpla_id, providers_json, partner, verbose, dry_run)

    finally:
        logging.info("\n" + str(tracker))
        logging.info(f"{time.time() - start_time} seconds.")
        local_fs.cleanup_temp_dir()


if __name__ == "__main__":
    main()
