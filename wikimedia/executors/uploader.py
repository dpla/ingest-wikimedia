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
from utilities.exceptions import UploadException, UploadWarning
from utilities.helpers import S3Helper
from utilities.helpers import Text


class Uploader:
    """
    Upload to Wikimedia Commons
    """
    # This list exists mainly to exclude 'duplicate' records/images from being uploaded
    # Full list of warnings:
    #   https://doc.wikimedia.org/pywikibot/master/_modules/pywikibot/site/_upload.html
    IGNORE_WARNINGS = [
        'bad-prefix',
        'badfilename',
        'duplicate-archive',
        'duplicate-version',
        'empty-file',
        'exists',
        'exists-normalized',
        'filetype-unwanted-type',
        'page-exists',
        'was-deleted'
    ]

    log = logging.getLogger(__name__)

    s3_helper = S3Helper()
    s3_resource = boto3.resource('s3')  # Used for downloading from s3
    s3_client = boto3.client('s3')      # Used for head_object on s3 object
    wikimedia = None

    def __init__(self):
        self.wikimedia = pywikibot.Site()
        self.wikimedia.login()
        self.log.info(f"Logged: {self.wikimedia.user()} in {self.wikimedia.family}")

        # Set logging level for pywikibot (kludged)
        for d in logging.Logger.manager.loggerDict:
            if d.startswith('pywiki'):
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
                if cex.response['Error']['Code'] == "404":
                    raise UploadException(f"Does not exist: {bucket}{key}") from cex
                if cex.response['Error']['Code'] == "403":
                    raise UploadException(f"Access denied: {bucket}{key}") from cex
                # TODO include more specific client errors
                else:
                    raise UploadException(f"Unable to download {bucket}{key} \
                                          to {destination.name}: {str(cex)}") from cex

    def _unique_ids(self, df):
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
        :return:
        """
        comment = f"Uploading DPLA ID \"[[dpla:{dpla_identifier}|{dpla_identifier}]]\"."

        # Kludged because direct s3 upload via source_url is not allowed
        if not file.startswith("s3"):
            raise UploadException("File must be on s3")
        # Download from S3 to local temporary file
        temp_file = tempfile.NamedTemporaryFile()
        bucket, key = self.s3_helper.get_bucket_key(file)
        self.download(bucket=bucket, key=key, destination=temp_file)
        try:
            result = self.wikimedia.upload(filepage=wiki_file_page,
                             source_filename=temp_file.name,
                             comment=comment,
                             text=text,
                             ignore_warnings=self.IGNORE_WARNINGS,
                             asynchronous= True,
                             chunk_size=3000000 # 3MB

                            )
            if not result:
                # Thise error message accounts for Page does not exist, but File does
                # exist and is linked to another Page (ex. DPLA ID drift)
                raise UploadException("wikimedi.upload() returned `False`")
            self.log.info(f"Uploaded to {Text.wikimedia_url(page_title)}")
            # FIXME this is dumb and should be better, it either raises and exception
            # or returns True; kinda worthless?
            return True
        except Exception as exec:
            error_string = str(exec)
            # TODO what does this error message actually mean? Page name?
            if 'fileexists-shared-forbidden:' in error_string:
                raise UploadWarning("File already uploaded") from exec
            if 'filetype-badmime' in error_string:
                raise UploadException("Invalid MIME type") from exec
            if 'filetype-banned' in error_string:
                raise UploadException("Banned file type") from exec
            # TODO what does this error message actually mean? MD5 hash collision?
            if 'duplicate' in error_string:
                raise UploadWarning(f"File already exists, {error_string}") from exec
            if 'no-change' in error_string:
                raise UploadWarning(f"File exists, no change, {error_string}") from exec
            raise UploadException(f"Failed to upload {error_string}") from exec

    def create_wiki_page_title(self, title, dpla_identifier, suffix, page=None):
        """
        Makes a proper Wikimedia page title from the DPLA identifier and
        the title of the image.

        :param title: DPLA title
        :param dpla_identifier: DPLA identifier
        :param suffix: file suffix
        :param page: If this is a multi-page document, the page number
        :return: Propertly escaped and formatted Wikimedia page title
        """
        escaped_title = title[0:181] \
            .replace('[', '(') \
            .replace(']', ')') \
            .replace('{', '(') \
            .replace('}', ')') \
            .replace('/', '-') \
            .replace(':', '-') \

        # Check to see if the page contains invisible characters and is invalid
        if pywikibot.tools.chars.contains_invisible(title):
            self.log.error(f"Invalid title due to invisible characters: {title}")
            return None

        # Add pagination to page title if needed
        if page:
            return f"{escaped_title} - DPLA - {dpla_identifier} (page {page}){suffix}"
        return f"{escaped_title} - DPLA - {dpla_identifier}{suffix}"

    def create_wiki_file_page(self, title):
        """
        Create a Wikimedia page for the image if it does not already exist.If it
        does exist then return None.

        :param title: Title of the record in DPLA
        :return: pywikibot.FilePage object if the page was created, None if the
        page already exists
        """
        wiki_page = pywikibot.FilePage(self.wikimedia, title=title)
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
                mime = response['ContentType']
            else:
                mime = mimetypes.guess_type(path)[0]
            return mimetypes.guess_extension(mime)
        except Exception as exec:
            raise UploadException(f"No extension {path}: {str(exec)}") from exec
