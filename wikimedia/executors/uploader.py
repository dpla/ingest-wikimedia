
"""
Upload images to Wikimedia Commons

"""
import mimetypes
import tempfile

import pywikibot
import boto3
import botocore
import numpy as np
import logging

from utilities.exceptions import UploadException
from utilities.fs import S3Helper
from utilities.fs import FileSystem
from trackers.tracker import Tracker

class Uploader:
    """
    Upload to Wikimedia Commons
    """
    COMMONS_PREFIX = "https://commons.wikimedia.org/wiki/File:"

    log = logging.getLogger(__name__)

    s3_helper = S3Helper()
    s3_resource = boto3.resource('s3')  # Used for download
    s3_client = boto3.client('s3')      # Used for head_object

    _site = None
    _tracker = Tracker()

    # List of warning codes to ignore. This list exists mainly to exclude 'duplicate' (i.e.,
    # abort upload if it's a duplicate, but not other cases)Full list of warnings here:
    # https://doc.wikimedia.org/pywikibot/master/_modules/pywikibot/site/_upload.html
    warnings_to_ignore = [
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

    def __init__(self):
        self._site = pywikibot.Site()
        self._site.login()
        self.log.info(f"Logged in user is: {self._site.user()} in {self._site.family}")

        # Set logging level for pywikibot
        for d in logging.Logger.manager.loggerDict:
            if d.startswith('pywiki'):
                logging.getLogger(d).setLevel(logging.ERROR)

    def download(self, bucket, key, destination):
        """
        Download file from s3 to local file system

        # TODO this may be redundant with utils.utils.download, but I'm not sure yet.

        :param bucket: s3 bucket
        :param key: s3 key
        :param destination: Full path to save the asset
        :return:    output_path: Full path to downloaded asset
        """
        with open(destination.name, "wb") as f:
            try:
                self.s3_resource.Bucket(bucket).download_file(key, destination.name)
                return destination.name
            except botocore.exceptions.ClientError as client_error:
                if client_error.response['Error']['Code'] == "404":
                    raise UploadException(f"Does not exist: {bucket}{key}") from client_error
                if client_error.response['Error']['Code'] == "403":
                    raise UploadException(f"Access denied: {bucket}{key}") from client_error
                # TODO include more specific client errors
                else:
                    raise UploadException(f"Unable to download {bucket}{key} to {destination.name}: \
                                          {str(client_error)}") from client_error
    def get_tracker(self):
        """
        Return the status of the upload
        """
        return self._tracker

    def _unique_ids(self, df):
        """
        Return a dictionary of unique dpla_ids and their counts"""
        unique, counts = np.unique(df["dpla_id"], return_counts=True)
        return dict(zip(unique, counts))

    def load_data(self, data_in):
        """
        Load data from parquet file and filter out ids if a file filter is provided
        """
        fs = FileSystem()
        return fs.read_parquet(data_in)

    def upload(self, wiki_file_page, dpla_identifier, text, file, page_title):
        """

        :param wiki_file_page:
        :param dpla_identifier:
        :param text
        :param file
        :return:
        """
        comment = f"Uploading DPLA ID \"[[dpla:{dpla_identifier}|{dpla_identifier}]]\"."

        # This is a massive kludge because direct s3 upload via source_url is not allowed.
        # Download from s3 to temp location on box then upload local file to wikimeida
        if not file.startswith("s3"):
            raise UploadException("File must be on s3")
        bucket, key = self.s3_helper.get_bucket_key(file)
        # Download to temp file on local file system
        # Will raise exceptions if the file cannot be downloaded
        temp_file = tempfile.NamedTemporaryFile()
        self.download(bucket=bucket, key=key, destination=temp_file)
        try:
            self._site.upload(filepage=wiki_file_page,
                             source_filename=temp_file.name,
                             comment=comment,
                             text=text,
                             ignore_warnings=self.warnings_to_ignore,
                             asynchronous= True,
                             chunk_size=3000000 # 3MB

                            )
            self.log.info(f"Uploaded to {self.COMMONS_PREFIX}{page_title.replace(' ', '_')}")
            # FIXME this is dumb and should be better, it either raises and exception or returns True; kinda worthless?
            return True
        except Exception as exception:
            error_string = str(exception)
            # TODO what does this error message actually mean? Page name?
            if 'fileexists-shared-forbidden:' in error_string:
                raise UploadException(f"File already uploaded") from exception
            if 'filetype-badmime' in error_string:
                raise UploadException(f"Invalid MIME type") from exception
            if 'filetype-banned' in error_string:
                raise UploadException(f"Banned file type") from exception
            # TODO what does this error message actually mean? MD5 hash collision?
            if 'duplicate' in error_string:
                raise UploadException(f"File already exists, {error_string}") from exception
            raise UploadException(f"Failed to upload {error_string}") from exception

    def create_wiki_page_title(self, title, dpla_identifier, suffix, page=None):
        """
        Makes a proper Wikimedia page title from the DPLA identifier and the title of the image.

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
        # This is probably unnecessary, but it's here just in case
        if pywikibot.tools.chars.contains_invisible(title):
            self.log.error(f"Invalid title due to invisible characters: {title}")
            return None

        # Add pagination to page title if needed
        if page:
            return f"{escaped_title} - DPLA - {dpla_identifier} (page {page}){suffix}"
        return f"{escaped_title} - DPLA - {dpla_identifier}{suffix}"

    def create_wiki_file_page(self, title):
        """
        Create a Wikimedia page for the image if it does not already exist. If it does exist, return None.

        :param title: Title of the record in DPLA
        :return: None if the page already exists or the title , otherwise a pywikibot.FilePage object
        """
        wiki_page = pywikibot.FilePage(self._site, title=title)
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
        except Exception as exception:
            raise UploadException(f"Unable to get extension for {path}: {str(exception)}") from exception

    def wikimedia_url(self, title):
        return f"{self.COMMONS_PREFIX}{title.replace(' ', '_')}"