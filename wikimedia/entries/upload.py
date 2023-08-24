"""
Upload images to Wikimedia Commons

"""
import logging

from entries.entry import Entry
from executors.uploader import Uploader
from utilities.exceptions import UploadException
from utilities.helpers import S3Helper, wikimedia_url
from utilities.tracker import Result, Tracker


class UploadEntry(Entry):
    """
    """
    # This is the schema emitted by the ingest-wikimedia download process
    READ_COLUMNS = {"_1": "dpla_id",
                    "_2": "path",
                    "_3": "size",
                    "_4": "title",
                    "_5": "markup",
                    "_6": "page"}

    log = logging.getLogger(__name__)
    uploader = None
    tracker = None

    def __init__(self, tracker: Tracker):
        self.uploader = Uploader()
        self.tracker = tracker

    def execute(self, **kwargs):
        """
        """
        s3_helper = S3Helper()
        base_input = kwargs.get('input', None)
        # Get the most recent parquet file from the input path
        bucket, key = s3_helper.get_bucket_key(base_input)
        recent_key = s3_helper.most_recent(bucket=bucket, key=key, type='object')
        input = f"s3://{bucket}/{recent_key}"

        # Read in most recent parquet file
        df = Entry.load_data(data_in=input)
        unique_ids = self.uploader._unique_ids(df)

        # Summary of input parameters
        self.log.info(f"Input............{input}")
        self.log.info(f"Images...........{len(df)}")
        self.log.info(f"DPLA records.....{self.tracker.item_cnt}")

        # Set the total number DPLA records and intended uploads
        self.tracker.set_dpla_count(len(unique_ids))
        self.tracker.set_total(len(df))

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
            page_title = self.uploader.create_wiki_page_title(title=title,
                                                     dpla_identifier=dpla_id,
                                                     suffix=ext,
                                                     page=page)

            # Create wiki page using Wikimedia page title
            wiki_page = self.uploader.create_wiki_file_page(title=page_title)

            if wiki_page is None:
                self.log.info(f"Exists {wikimedia_url(page_title)}")
                self.tracker.increment(Result.SKIPPED)
                continue
            try:
                # Upload image to wiki page
                self.uploader.upload(wiki_file_page=wiki_page,
                            dpla_identifier=dpla_id,
                            text=wiki_markup,
                            file=path,
                            page_title=page_title)
                self.tracker.increment(Result.UPLOADED, size=size)
            except UploadException as exec:
                self.log.error(f"{str(exec)} -- {wikimedia_url(page_title)}")
                self.tracker.increment(Result.FAILED)
                continue
            except Exception as exception:
                self.log.error(f"{str(exception)} -- {wikimedia_url(page_title)}")
                self.tracker.increment(Result.FAILED)
                continue
