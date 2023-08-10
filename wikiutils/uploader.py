"""
Upload images to Wikimedia Commons

"""
import mimetypes
import os
import tempfile
from urllib.parse import urlparse

import pywikibot
import boto3
import botocore

from wikiutils.exceptions import UploadException
from wikiutils.logger import WikimediaLogger
from wikiutils.utils import Utils as WikimediaUtils

class Uploader:
    """
    Upload to Wikimedia Commons
    """
    site = None
    log = None
    s3 = boto3.client('s3')
    wikiutils = None

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

    def __init__(self, partner_name, logger):
        self.log = logger
        self.wikiutils = WikimediaUtils()
        self.site = pywikibot.Site()
        self.site.login()
        self.log.log_info(f"Logged in user is: {self.site.user()}")

    def upload(self, wiki_file_page, dpla_identifier, text, file, page_title):
        """

        :param wiki_file_page:
        :param dpla_identifier:
        :param text
        :param file
        :return:
        """
        comment = f"Uploading DPLA ID \"[[dpla:{dpla_identifier}|{dpla_identifier}]]\"."
        temp_file = None

        # This is a massive kludge because direct s3 upload via source_url is not allowed.
        # Download from s3 to temp location on box then upload local file to wikimeida
        if file.startswith("s3"):
            s3 = boto3.resource('s3')
            bucket, key = self.wikiutils.get_bucket_key(file)
        else: 
            raise UploadException("File must be on s3")

        temp_file = tempfile.NamedTemporaryFile(delete=False)
        with open(temp_file.name, "wb") as f:
            try:
                s3.Bucket(bucket).download_file(key, temp_file.name)
            except botocore.exceptions.ClientError as client_error:
                if client_error.response['Error']['Code'] == "404":
                    raise UploadException(f"Does not exist: {bucket}{key}") from client_error
                raise UploadException(f"Unable to download {bucket}{key} to {temp_file.name}: {str(client_error)}") from client_error
        try:
            self.site.upload(filepage=wiki_file_page,
                             source_filename=temp_file.name,
                             comment=comment,
                             text=text,
                             ignore_warnings=self.warnings_to_ignore,
                             asynchronous= True,
                             chunk_size=3000000 # 3MB
                            )
            self.log.log_info(f"Uploaded '{page_title}'")
            # FIXME this is dumb and should be betterm, it either raises and exception or returns true
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
        finally:
            if temp_file:
                os.unlink(temp_file.name)

    def create_wiki_page_title(self, title, dpla_identifier, suffix, page=None):
        """
        Makes a proper Wikimedia page title from the DPLA identifier and the title of the image.

        - only use the first 181 characters of image file name
        - replace [ with (
        - replace ] with )
        - replace / with -

        :param title:
        :param dpla_identifier:
        :param suffix:
        :param page:
        :return:
        """
        escaped_title = title[0:181] \
            .replace('[', '(') \
            .replace(']', ')') \
            .replace('{', '(') \
            .replace('}', ')') \
            .replace('/', '-') \
            .replace(':', '-') \

        # Add pagination to page title if needed
        if page:
            return f"{escaped_title} - DPLA - {dpla_identifier} (page {page}){suffix}"
        return f"{escaped_title} - DPLA - {dpla_identifier}{suffix}"

    # noinspection PyStatementEffect
    def create_wiki_file_page(self, title):
        """
        Create a Wikimedia page for the image if it does not already exist. If it does exist, return None.

        :param title: Title of the record in DPLA
        :return: None if the page already exists, otherwise a pywikibot.FilePage object
        """
        # Check to see if the page contains invisible characters and is invalid
        # This is probably unnecessary, but it's here just in case
        invaild_name = pywikibot.tools.chars.contains_invisible(title)
        if invaild_name:
            self.log.log_info(f"Invalid name: {invaild_name}")
            return None

        wiki_page = pywikibot.FilePage(self.site, title=title)
        if wiki_page.exists():
            return None
        return wiki_page    # Create a function to get the extension from the mime type
    
    def get_metadata(self, row):
        """
        Get metadata for a DPLA record

        :param dpla_id: The DPLA identifier
        :return: A
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
                bucket, key = self.wikiutils.get_bucket_key(path)
                response = self.s3.head_object(Bucket=bucket, Key=key)
                mime = response['ContentType']
            else:
                mime = mimetypes.guess_type(path)[0]
            return mimetypes.guess_extension(mime)
        except Exception as exception:
            raise UploadException(f"Unable to get extension for {path}: {exception.__str__}") from exception
        