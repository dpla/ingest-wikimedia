"""
Upload images to Wikimedia Commons

"""

import logging
import mimetypes
import tempfile

import boto3
import botocore
import numpy as np
import pywikibot
from wikimedia.utilities.exceptions import UploadException, UploadWarning
from wikimedia.utilities.helpers import S3Helper
from wikimedia.utilities.helpers import Text


class Uploader:
    """
    Upload to Wikimedia Commons
    """

    # This list exists mainly to exclude 'duplicate' records/images from being uploaded
    # Full list of warnings:
    #   https://doc.wikimedia.org/pywikibot/master/_modules/pywikibot/site/_upload.html

    IGNORE_WARNINGS = [
        "bad-prefix",  # Target filename has a bad prefix {msg}.
        "badfilename",  # Target filename is invalid.
        "duplicate-archive",  # The file is a duplicate of a deleted file {msg}.
        "duplicate-version",  # The upload is an exact duplicate of older version(s)
        # of this file
        "empty-file",  # File {msg} is empty.
        "exists",  # File [Page] {msg} already exists
        "exists-normalized",  # File exists with different extension as {msg}.
        "filetype-unwanted-type",  # File {msg} type is unwanted type.
        "page-exists",  # Target filename exists but with a different file {msg}
        "was-deleted",  # The file {msg} was previously deleted.
        #
        # 'duplicate', # Uploaded file is a duplicate of {msg}
        # 'no-change', # The upload is an exact duplicate of the current version
        # of this file
    ]

    log = logging.getLogger(__name__)

    s3_helper = S3Helper()
    s3_resource = boto3.resource("s3")  # Used for downloading from s3
    s3_client = boto3.client("s3")  # Used for head_object on s3 object
    wikimedia = None

    def __init__(self):
        self.wikimedia = pywikibot.Site()
        self.wikimedia.login()
        self.log.info(f"Logged: {self.wikimedia.user()} in {self.wikimedia.family}")

        # Set logging level for pywikibot (kludged)
        for d in logging.Logger.manager.loggerDict:
            if d.startswith("pywiki"):
                logging.getLogger(d).setLevel(logging.ERROR)

    # TODO This function maybe be better in the S3Helper class
    def download(self, bucket, key, destination):
        """
        Download file from s3 to local file system

        :param bucket: s3 bucket
        :param key: s3 key
        :param destination: Full path to save the asset
        :return:    output_path: Full path to downloaded asset
        """
        with open(destination.name, "wb") as _:
            try:
                self.s3_resource.Bucket(bucket).download_file(key, destination.name)
                return destination.name
            except botocore.exceptions.ClientError as cex:
                if cex.response["Error"]["Code"] == "404":
                    raise UploadException(f"Does not exist: {bucket}{key}") from cex
                if cex.response["Error"]["Code"] == "403":
                    raise UploadException(f"Access denied: {bucket}{key}") from cex
                # TODO include more specific client errors
                else:
                    raise UploadException(
                        f"Unable to download {bucket}{key} \
                                          to {destination.name}: {str(cex)}"
                    ) from cex

    @staticmethod
    def _unique_ids(df):
        """
        Return a dictionary of unique dpla_ids and their counts"""
        unique, counts = np.unique(df["dpla_id"], return_counts=True)
        return dict(zip(unique, counts))

    def upload(self, wiki_file_page, dpla_identifier, text, file, page_title):
        """
        :param wiki_file_page:
        :param dpla_identifier:
        :param text
        :param file
        :param page_title
        :return:
        """
        comment = f'Uploading DPLA ID "[[dpla:{dpla_identifier}|{dpla_identifier}]]".'

        # Kludged because direct s3 upload via source_url is not allowed
        if not file.startswith("s3"):
            raise UploadException("File must be on s3")
        # Download from S3 to local temporary file
        temp_file = tempfile.NamedTemporaryFile()
        bucket, key = self.s3_helper.get_bucket_key(file)
        self.download(bucket=bucket, key=key, destination=temp_file)
        try:
            result = self.wikimedia.upload(
                filepage=wiki_file_page,
                source_filename=temp_file.name,
                comment=comment,
                text=text,
                ignore_warnings=self.IGNORE_WARNINGS,
                asynchronous=True,
                chunk_size=3000000,  # 3MB
            )
            if not result:
                # Thise error message accounts for Page does not exist, but File does
                # exist and is linked to another Page (ex. DPLA ID drift)
                raise UploadException("File linked to another page (possible ID drift)")
            self.log.info(f"Uploaded to {Text.wikimedia_url(page_title)}")
            # FIXME this is dumb and should be better, it either raises and exception
            # or returns True; kinda worthless?
            return True
        except Exception as error:
            error_string = str(error)
            # TODO what does this error message actually mean? Page name?
            if "fileexists-shared-forbidden:" in error_string:
                raise UploadWarning("File already uploaded") from error
            if "filetype-badmime" in error_string:
                raise UploadException("Invalid MIME type") from error
            if "filetype-banned" in error_string:
                raise UploadException("Banned file type") from error
            # TODO what does this error message actually mean? MD5 hash collision?
            if "duplicate" in error_string:
                raise UploadWarning(f"File already exists, {error_string}") from error
            if "no-change" in error_string:
                raise UploadWarning(
                    f"File exists, no change, {error_string}"
                ) from error
            raise UploadException(f"Failed: {error_string}") from error

    @staticmethod
    def get_page_title(title, dpla_identifier, suffix, page=None):
        """
        Makes a proper Wikimedia page title from the DPLA identifier and
        the title of the image.

        :param title: DPLA title
        :param dpla_identifier: DPLA identifier
        :param suffix: file suffix
        :param page: If this is a multi-page document, the page number
        :return: Propertly escaped and formatted Wikimedia page title
        """
        escaped_title = (
            title[0:181]
            .replace("[", "(")
            .replace("]", ")")
            .replace("{", "(")
            .replace("}", ")")
            .replace("/", "-")
            .replace(":", "-")
            .replace("#", "-")
        )

        # Check to see if the page contains invisible characters and is invalid
        if pywikibot.tools.chars.contains_invisible(title):
            raise UploadException(f"Invalid title due to invisible characters: {title}")

        # Add pagination to page title if needed
        if page:
            return f"{escaped_title} - DPLA - {dpla_identifier} (page {page}){suffix}"
        return f"{escaped_title} - DPLA - {dpla_identifier}{suffix}"

    def get_page(self, title):
        """
        Create a Wikimedia page for the image if it does not already exist.If it
        does exist then return None.

        :param title: Title of the record in DPLA
        :return: pywikibot.FilePage object if the page was created, None if the
        page already exists
        """
        try:
            wiki_page = pywikibot.FilePage(self.wikimedia, title=title)
        except pywikibot.exceptions.InvalidTitleError as itex:
            raise UploadException(f"Invalid title {title}: {str(itex)}") from itex
        except Exception as ex:
            raise UploadException(f"Unable to create page {title}: {str(exec)}") from ex
        if wiki_page.exists():
            return None
        return wiki_page

    def get_extension(self, path):
        """
        Derive the file extension from the MIME type

        :param path: The path to the file
        :return: The file extension
        """
        mime = None
        try:
            if "s3://" in path:
                bucket, key = self.s3_helper.get_bucket_key(path)
                response = self.s3_client.head_object(Bucket=bucket, Key=key)
                mime = response["ContentType"]
            else:
                mime = mimetypes.guess_type(path)[0]
            return mimetypes.guess_extension(mime)
        except Exception as error:
            raise UploadException(f"No extension {path}: {str(error)}") from exec
