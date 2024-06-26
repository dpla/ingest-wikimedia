"""
Upload images to Wikimedia Commons

"""

import logging

from entry import Entry
from wikimedia.executors.uploader import Uploader
from wikimedia.utilities.exceptions import UploadException, UploadWarning
from wikimedia.utilities.helpers import S3Helper, Text, InputHelper
from wikimedia.utilities.tracker import Result, Tracker


class UploadEntry(Entry):
    """ """

    # This is the schema emitted by the ingest-wikimedia download process
    READ_COLUMNS = {
        "_1": "dpla_id",
        "_2": "path",
        "_3": "size",
        "_4": "title",
        "_5": "markup",
        "_6": "page",
    }

    log = logging.getLogger(__name__)
    uploader = None
    tracker = None

    def __init__(self, tracker: Tracker):
        self.uploader = Uploader()
        self.tracker = tracker

    def execute(self, **kwargs):
        """ """
        s3_helper = S3Helper()
        input_base = kwargs.get("input", None)
        partner = kwargs.get("partner", None)
        input_partner = InputHelper.upload_input(base=input_base, partner=partner)
        # Get the most recent parquet file from the input path
        bucket, key = s3_helper.get_bucket_key(input_partner)
        recent_key = s3_helper.most_recent(bucket=bucket, key=key, type="object")
        input_path = f"s3://{bucket}/{recent_key}"
        # Read in most recent parquet file
        df = Entry.load_data(data_in=input_path, columns=self.READ_COLUMNS).rename(
            columns=self.READ_COLUMNS
        )
        unique_ids = self.uploader._unique_ids(df)
        # Set the total number DPLA records and intended uploads
        self.tracker.set_dpla_count(len(unique_ids))
        self.tracker.set_total(len(df))
        # Summary of input parameters
        self.log.info(f"Input............{input_path}")
        self.log.info(f"Images...........{len(df)}")
        self.log.info(f"DPLA records.....{self.tracker.item_cnt}")

        # TODO parallelize this
        for row in df.itertuples():
            dpla_id, path, title, wiki_markup, size = None, None, None, None, None
            try:
                dpla_id = row.dpla_id
                path = row.path
                size = row.size
                title = row.title
                wiki_markup = row.markup
                page = row.page
            except AttributeError as attribute_error:
                self.log.error(f"No attributes from row {row}, {str(attribute_error)}")
                continue

            # If there is only one record for this dpla_id, then page is `None`
            # and pagination will not be used in the Wikimedia page title
            page = None if unique_ids[dpla_id] == 1 else page
            # Get file extension
            ext = self.uploader.get_extension(path)
            # Create Wikimedia page title
            page_title = None
            wikimedia_page = None
            try:
                page_title = self.uploader.get_page_title(
                    title=title, dpla_identifier=dpla_id, suffix=ext, page=page
                )
            except UploadException as error:
                self.log.error(f"{str(error)}")
                self.tracker.increment(Result.FAILED)
                continue
            try:
                wikimedia_page = self.uploader.get_page(title=page_title)
            except UploadException as error:
                self.log.error(f"{str(error)}")
                self.tracker.increment(Result.FAILED)
                continue
            if wikimedia_page is None:
                self.log.info(f"Exists {Text.wikimedia_url(page_title)}")
                self.tracker.increment(Result.SKIPPED)
                continue

            # Upload image to Wikimedia page
            try:
                # Upload image to wiki page
                self.uploader.upload(
                    wiki_file_page=wikimedia_page,
                    dpla_identifier=dpla_id,
                    text=wiki_markup,
                    file=path,
                    page_title=page_title,
                )
                self.tracker.increment(Result.UPLOADED, size=size)

            except UploadWarning:
                self.log.info(f"Exists {Text.wikimedia_url(page_title)}")
                self.tracker.increment(Result.SKIPPED)
                continue
            except UploadException as error:
                self.log.error(f"{str(error)} -- {Text.wikimedia_url(page_title)}")
                self.tracker.increment(Result.FAILED)
                continue
            except Exception as error:
                self.log.error(f"{str(error)} -- {Text.wikimedia_url(page_title)}")
                self.tracker.increment(Result.FAILED)
                continue
