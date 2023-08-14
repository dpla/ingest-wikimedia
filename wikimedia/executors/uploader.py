"""
Upload images to Wikimedia Commons

"""
import mimetypes
import tempfile

import pywikibot
import boto3
import botocore
import numpy as np

from utilities.exceptions import UploadException
from utilities.fs import FileSystem

class UploadStatus:
    """
    Status of uploads
    """
    SKIPPED = "SKIPPED"
    UPLOADED = "UPLOADED"
    FAILED = "FAILED"

    dpla_count = 0
    skip_count = 0
    fail_count = 0
    upload_count = 0
    attempted = 0
    cumulative_size = 0

    def __init__(self):
        pass

    def set_dpla_count(self, count):
        """
        Set the number of DPLA items
        """
        UploadStatus.dpla_count = count

    def set_total(self, total):
        """
        Set the total number of uploads
        """
        UploadStatus.attempted = total

    def increment(self, status, size=None):
        """
        Increment the status
        """
        if status == UploadStatus.SKIPPED:
            UploadStatus.skip_count += 1
        elif status == UploadStatus.UPLOADED:
            UploadStatus.upload_count += 1
            UploadStatus.cumulative_size += size
        elif status == UploadStatus.FAILED:
            UploadStatus.fail_count += 1
        else:
            raise UploadException(f"Unknown status: {status}")

class Uploader:
    """
    Upload to Wikimedia Commons
    """
    IN_COULUMNS = {
            "dpla_id": "dpla_id",
            "path": "path",
            "size": "size",
            "title": "title",
            "markup": "markup",
            "page": "page"
           }

    _site = None
    _log = None
    _status = None
    _fs = FileSystem()
    s3 = boto3.resource('s3')       # Used for download
    s3_client = boto3.client('s3')  # Used for head_object


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

    def __init__(self, logger):
        self._log = logger
        self._status = UploadStatus()
        self._site = pywikibot.Site()
        self._site.login()
        self._log.info(f"Logged in user is: {self._site.user()}")

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
                self.s3.Bucket(bucket).download_file(key, destination.name)
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
    def get_status(self):
        """
        Return the status of the upload
        """
        return self._status

    def _unique_ids(self, df):
        """
        Return a dictionary of unique dpla_ids and their counts"""
        unique, counts = np.unique(df["dpla_id"], return_counts=True)
        return dict(zip(unique, counts))

    def execute_upload(self, df):
        """
        Upload images to Wikimedia Commons"""
        unique_ids = self._unique_ids(df)
        # Set the total number of intended uploads and number of unique DPLA records
        self._status.set_total(len(df))
        self._status.set_dpla_count(len(unique_ids))

        for row in df.itertuples(index=self.IN_COULUMNS):
            dpla_id, path, title, wiki_markup, size = None, None, None, None, None
            try:
                # Load record from dataframe, skip size column
                dpla_id, path, size, title, wiki_markup, page = self.get_metadata(row)
                # If there is only one record for this dpla_id, then page is `None` and pagination will not
                # be used in the Wikimedia page title
                page = None if unique_ids[dpla_id] == 1 else page
                # Get file extension
                ext = self.get_extension(path)
                # Create Wikimedia page title
                page_title = self.create_wiki_page_title(title=title,
                                                            dpla_identifier=dpla_id,
                                                            suffix=ext,
                                                            page=page)

                # Create wiki page using Wikimedia page title
                wiki_page = self.create_wiki_file_page(title=page_title)

                if wiki_page is None:
                    # Create a working URL for the file from the page title. Helpful for verifying the page in Wikimedia
                    self._log.info(f"Skipping, exists https://commons.wikimedia.org/wiki/File:{page_title.replace(' ', '_')}")
                    self._status.increment(UploadStatus.SKIPPED)
                    continue

                # Upload image to wiki page
                self.upload(wiki_file_page=wiki_page,
                            dpla_identifier=dpla_id,
                            text=wiki_markup,
                            file=path,
                            page_title=page_title)
                self._status.increment(UploadStatus.UPLOADED, size=size)
            except UploadException as upload_exec:
                self._log.error("Upload error: %s", str(upload_exec))
                self._status.increment(UploadStatus.FAILED)
                continue
            except Exception as exception:
                self._log.error(f"Unknown error: {str(exception)}")
                self._status.increment(UploadStatus.FAILED)
                continue


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
        bucket, key = self._fs.get_bucket_key(file)
        # Download to temp file on local file system
        # Will raise exceptions if the file cannot be downloaded
        temp_file = tempfile.NamedTemporaryFile()
        self.download(bucket=bucket, key=key, destination=temp_file)
        try:
            # self._site.upload(filepage=wiki_file_page,
            #                  source_filename=temp_file.name,
            #                  comment=comment,
            #                  text=text,
            #                  ignore_warnings=self.warnings_to_ignore,
            #                  asynchronous= True,
            #                  chunk_size=3000000 # 3MB
            #                 )
            self._log.info(f"Uploading to https://commons.wikimedia.org/wiki/File:{page_title.replace(' ', '_')}")
            # FIXME this is dumb and should be better, it either raises and exception or returns True; kinda worthless?
            return True
        except Exception as exception:
            error_string = str(exception)
            if 'fileexists-shared-forbidden:' in error_string:
                raise UploadException(f"Failed '{page_title}', File already uploaded") from exception
            if 'filetype-badmime' in error_string:
                raise UploadException(f"Failed '{page_title}', Invalid MIME type") from exception
            if 'filetype-banned' in error_string:
                raise UploadException(f"Failed '{page_title}', Banned file type") from exception
            if 'duplicate' in error_string:
                raise UploadException(f"Failed '{page_title}', File already exists, {error_string}") from exception
            raise UploadException(f"Failed to upload '{page_title}' - {dpla_identifier}, {error_string}") from exception

    def create_wiki_page_title(self, title, dpla_identifier, suffix, page=None):
        """
        Makes a proper Wikimedia page title from the DPLA identifier and the title of the image.

        - only use the first 181 characters of image file name
        - replace [ with (
        - replace ] with )
        - replace { with (
        - replace } with )
        - replace / with -
        - replace : with -

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
            self._log.error(f"Invalid title due to invisible characters: {title}")
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

    def get_metadata(self, row):
        """
        Get metadata for a DPLA record from the row emitted by the download process

        :param dpla_id: Row from the data for a DPLA record and associated image
                        file except for file size.
                        TODO this should only parse the columns gien.
        :return: A tuple containing
                    DPLA identifier
                    Path to the file
                    Size of the file
                    Title of the DPLA record
                    Wiki markup for the page (generated by ingestion3)
                    Page if this is a multi-page document
        """
        try:
            dpla_id = getattr(row, 'dpla_id')
            path = getattr(row, 'path')
            size = getattr(row, 'size')
            title = getattr(row, 'title')
            wiki_markup = getattr(row, 'markup')
            page = getattr(row, 'page')

            return dpla_id, path, size, title, wiki_markup, page
        except AttributeError as attribute_error:
            raise UploadException(f"Unable to get attributes from row {row}: {attribute_error.__str__}") from attribute_error

    def get_extension(self, path):
        """
        Derive the file extension from the MIME type

        :param path: The path to the file
        :return: The file extension
        """
        mime = None
        try:
            if "s3://" in path:
                bucket, key = self._fs.get_bucket_key(path)
                response = self.s3_client.head_object(Bucket=bucket, Key=key)
                mime = response['ContentType']
            else:
                mime = mimetypes.guess_type(path)[0]
            return mimetypes.guess_extension(mime)
        except Exception as exception:
            raise UploadException(f"Unable to get extension for {path}: {str(exception)}") from exception
