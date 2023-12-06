"""
Upload images to Wikimedia Commons

"""
import logging

from entries.entry import Entry
from executors.uploader import Uploader
from utilities.exceptions import UploadException, UploadWarning
from utilities.helpers import S3Helper, Text, InputHelper
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
        input_base=kwargs.get('input', None)
        partner = kwargs.get('partner', None)
        input_partner = InputHelper.upload_input(base=input_base, partner=partner)
        # Get the most recent parquet file from the input path
        bucket, key = s3_helper.get_bucket_key(input_partner)
        recent_key = s3_helper.most_recent(bucket=bucket, key=key, type='object')
        input = f"s3://{bucket}/{recent_key}"
        # Read in most recent parquet file
        df = Entry.load_data(data_in=input,
                             columns=self.READ_COLUMNS).rename(columns=self.READ_COLUMNS)
        unique_ids = self.uploader._unique_ids(df)
        # Set the total number DPLA records and intended uploads
        self.tracker.set_dpla_count(len(unique_ids))
        self.tracker.set_total(len(df))
        # Summary of input parameters
        self.log.info(f"Input............{input}")
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
                page_title = self.uploader.get_page_title(title=title,
                                                     dpla_identifier=dpla_id,
                                                     suffix=ext,
                                                     page=page)
            except UploadException as exec:
                self.log.error(f"{str(exec)}")
                self.tracker.increment(Result.FAILED)
                continue

            # Generate a Wikimedia Page object, it may or may not already exist on Commons
            try:
                wikimedia_page = self.uploader.get_page(title=page_title)
            except UploadException as exec:
                self.log.error(f"{str(exec)}")
                self.tracker.increment(Result.FAILED)
                continue

            ########################################################################
            # If wikimedia_page already exists...
            if wikimedia_page.exists():
                wikimedia_sha1 = wikimedia_page.latest_file_info.sha1

                bucket, key = self.s3_helper.get_bucket_key(path=path)
                s3_sha1 = self.s3_helper.get_sha1(bucket=bucket, key=key)

                # Case 1
                # Logical conditions:   Incoming Page Exists
                #                       Page SHA1 matches S3 SHA1
                # Action:   Skip
                #           Log SKIP message
                if (s3_sha1 == wikimedia_sha1):
                    self.log.info(f"Exists {Text.wikimedia_url(page_title)}")
                    self.tracker.increment(Result.SKIPPED)

                # Case 2: Title exists, but the sha1 hashes are not aligned
                elif(s3_sha1 != wikimedia_sha1):
                    # The difference between 2a and 2b is whether the image on s3 has been
                    # uploaded to Commons before. That may(will?) dictate whether the action
                    # take is to MOVE the image to a page or UPLOAD & REPLACE the image on a page.

                    # Case 2a
                    # Logical conditions:   Incoming Page Exists
                    #                       Page SHA1 does not match S3 SHA1
                    #                       S3 SHA1 exists on Commons
                    #                       S3 SHA1 is not attached to the correct page (incoming page)
                    # Action:   Do not perform a new upload (image already exists)
                    #           Replace image on existing page?
                    #           Move existing image to existing page?
                    if(s3_sha1.exists() is True):
                        replace = True
                    # Case 2b
                    # Logical conditions:   Page Exists
                    #                       Page SHA1 does not match S3 SHA1
                    #                       S3 SHA1 **DOES NOT** exists on Commons
                    # Action:   Upload new images to Commons
                    #           Replace image on existing page.
                    #           Log REPLACE message
                    elif(s3_sha1.exists() is False):
                        replace = False

                    # TODO This block is a placeholder, need to resolve the logic of 2a and 2b
                    self.uploader.upload(wiki_file_page=wikimedia_page,
                                         dpla_identifier=dpla_id,
                                         text=wiki_markup,
                                         file=path,
                                         page_title=page_title,
                                         replace=replace)

                continue
            # Case 3 Page does not exist
            elif not wikimedia_page.exists():
                # Case 3a
                # Logical conditions:   s3 sha1 exists on Commons
                #                       The page does not exist on Commons
                #
                # Action:   Do not UPLOAD image to Commons
                #           Create a new page
                #           MOVE the existing image to the new page
                #           Log MOVE message

                # Case 3b
                # Logical conditions:   s3 sha1 does not exists on Commons
                #                       The page does not exist on Commons
                # Action:   UPLOAD image to Commons
                #           Log new UPLOAD message
                continue


            ########################################################################
            # Upload image to Wikimedia page
            try:
                # Upload image to wiki page
                self.uploader.upload(wiki_file_page=wikimedia_page,
                                     dpla_identifier=dpla_id,
                                     text=wiki_markup,
                                     file=path,
                                     page_title=page_title)
                self.tracker.increment(Result.UPLOADED, size=size)

            except UploadWarning as _:
                self.log.info(f"Exists {Text.wikimedia_url(page_title)}")
                self.tracker.increment(Result.SKIPPED)
                continue
            except UploadException as exec:
                self.log.error(f"{str(exec)} -- {Text.wikimedia_url(page_title)}")
                self.tracker.increment(Result.FAILED)
                continue
            except Exception as exception:
                self.log.error(f"{str(exception)} -- {Text.wikimedia_url(page_title)}")
                self.tracker.increment(Result.FAILED)
                continue
