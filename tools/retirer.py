import click
import time
import mimetypes
import logging

from pywikibot.site import BaseSite
from tqdm import tqdm

from ingest_wikimedia.common import get_list, get_dict, CHECKSUM
from ingest_wikimedia.s3 import S3Client, S3_BUCKET
from ingest_wikimedia.tools_context import ToolsContext
from ingest_wikimedia.dpla import DPLA
from ingest_wikimedia.logs import setup_logging
from ingest_wikimedia.tracker import Result, Tracker
from ingest_wikimedia.wikimedia import (
    wiki_file_exists,
    get_page_title,
    check_content_type,
    get_site,
    get_page,
)


class Retirer:
    def __init__(
        self, tracker: Tracker, s3_client: S3Client, dpla: DPLA, site: BaseSite
    ):
        self.tracker = tracker
        self.s3_client = s3_client
        self.dpla = dpla
        self.site = site

    def process_item(
        self, providers_json: dict, partner: str, dry_run: bool, item_metadata: dict
    ) -> None:
        dpla_id = item_metadata.get("id", "")

        if not dpla_id:
            logging.warning("Skipping item with no DPLA ID.")
            self.tracker.increment(Result.SKIPPED)
            return

        provider, data_provider = DPLA.get_provider_and_data_provider(
            item_metadata, providers_json
        )

        if not self.dpla.is_wiki_eligible(
            dpla_id, item_metadata, provider, data_provider
        ):
            logging.info(f"Skipping {dpla_id}: Not eligible.")
            self.tracker.increment(Result.SKIPPED)
            return

        titles = get_list(
            get_dict(item_metadata, "sourceResource"),
            "title",
        )

        file_list = self.s3_client.get_file_list(partner, dpla_id)
        title = titles[0] if titles else ""
        ordinal = 0

        for _ in tqdm(
            file_list, desc="Processing Files", leave=False, unit="Item", ncols=100
        ):
            ordinal += 1
            page_label = "" if len(file_list) == 1 else str(ordinal)
            self.process_file(page_label, dpla_id, ordinal, partner, title, dry_run)

    def process_file(
        self,
        page_label: str,
        dpla_id: str,
        ordinal: int,
        partner: str,
        title: str,
        dry_run: bool,
    ) -> None:
        s3_path = self.s3_client.get_media_s3_path(dpla_id, ordinal, partner)

        if not self.s3_client.s3_file_exists(s3_path):
            logging.info(f"{dpla_id} {ordinal} not present.")
            self.tracker.increment(Result.SKIPPED)
            return

        s3_object = self.s3_client.get_s3().Object(S3_BUCKET, s3_path)
        file_size = s3_object.content_length

        if file_size == 0:
            logging.info(f"{dpla_id} {ordinal} already blanked out.")
            self.tracker.increment(Result.SKIPPED)
            return

        sha1 = s3_object.metadata.get(CHECKSUM, "")

        if not sha1:
            logging.info(f"{dpla_id} {ordinal} has no checksum.")
            self.tracker.increment(Result.SKIPPED)
            return

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

        file_exists = wiki_file_exists(self.site, sha1)
        wiki_page = get_page(self.site, page_title)
        page_exists = False if wiki_page is None else True
        logging.info(f"Wiki page: {wiki_page}")

        if file_exists and page_exists:
            if not dry_run:
                metadata = s3_object.metadata
                s3_object.put(Body="", Metadata=metadata)
            self.tracker.increment(Result.RETIRED)


@click.command()
@click.argument("partner")
@click.option("--dry-run", is_flag=True)
def main(partner: str, dry_run: bool) -> None:
    start_time = time.time()
    tools_context = ToolsContext.init()
    retirer = Retirer(
        tools_context.get_tracker(),
        tools_context.get_s3_client(),
        tools_context.get_dpla(),
        get_site(),
    )
    dpla = tools_context.get_dpla()
    tracker = tools_context.get_tracker()

    dpla.check_partner(partner)

    try:
        setup_logging(partner, "retirer", logging.INFO)
        if dry_run:
            logging.warning("---=== DRY RUN ===---")

        s3 = tools_context.get_s3_client()
        providers_json = dpla.get_providers_data()

        for item_metadata in tqdm(
            s3.get_metadata_files_for_partner(partner),
            desc="Processing Items",
            unit="Item",
            ncols=100,
        ):
            retirer.process_item(providers_json, partner, dry_run, item_metadata)

    finally:
        logging.info("\n" + str(tracker))
        logging.info(f"{time.time() - start_time} seconds.")


if __name__ == "__main__":
    main()
