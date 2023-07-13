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

from wikiutils.logger import Logger

class Uploader:
    """
    Upload to Wikimedia Commons
    """
    site = None
    log = None
    s3 = boto3.client('s3')

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
        self.log = Logger(type="upload")
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
            s3_path = urlparse(file)
            bucket = s3_path.netloc
            key = s3_path.path.replace('//', '/').lstrip('/')
        else: 
            raise Exception("File must be on s3")

        temp_file = tempfile.NamedTemporaryFile(delete=False)

        with open(temp_file.name, "wb") as f:
            try:
                s3.Bucket(bucket).download_file(key, temp_file.name)
                file = temp_file.name
            except botocore.exceptions.ClientError as client_error:
                if client_error.response['Error']['Code'] == "404":
                    raise Exception(f"S3 object does not exist: {bucket}{key} ") from client_error
                raise Exception(f"Unable to download {bucket}{key} to {temp_file.name}: \
                                {client_error.__str__}") from client_error
        # TODO Resolve the correct combination of report_success and ignore_warnings
        #      And route output to parse JSON and log clearer messages
        try:
            self.site.upload(filepage=wiki_file_page,
                             source_filename=file,
                             comment=comment,
                             text=text,
                             ignore_warnings=self.warnings_to_ignore,
                             asynchronous= True,
                             chunk_size=50000000
                            )
            return True
        except Exception as exception:
            if 'fileexists-shared-forbidden:' in exception.__str__():
                raise Exception(f"Failed to upload '{page_title}' for {dpla_identifier}, File already uploaded") from exception
            elif 'filetype-badmime' in exception.__str__():
                raise Exception(f"Failed to upload '{page_title}' for {dpla_identifier}, Invalid MIME type") from exception
            elif 'filetype-banned' in exception.__str__():
                raise Exception(f"Failed to upload '{page_title}' for {dpla_identifier}, Banned file type") from exception
            else:
                raise Exception("Failed to upload '{page_title}' for {dpla_identifier}, {e.__str__()}") from exception
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

        try:
            escaped_title = title[0:181] \
                .replace('[', '(') \
                .replace(']', ')') \
                .replace('/', '-') \
                .replace('{', '(') \
                .replace('}', ')')

            # Add pagination to page title if needed
            if page is None:
                return f"{escaped_title} - DPLA - {dpla_identifier}{suffix}"
            else:
                return f"{escaped_title} - DPLA - {dpla_identifier} (page {page}){suffix}"
        except Exception as e:
            self.log.log_error(f"Unable to generate page title for:  {title} - {dpla_identifier} - {e.__str__}")
        
    # noinspection PyStatementEffect
    def create_wiki_file_page(self, title):
        """
        Create a Wikimedia page for the image if it does not already exist. If it does exist, return None.

        :param title: Title of the record in DPLA
        :return: None if the page already exists, otherwise a pywikibot.FilePage object
        """
        wiki_page = pywikibot.FilePage(self.site, title=title)
        try:
            wiki_page.latest_file_info
            return None
        except Exception as e:
            # Raising an exception indicates that the page does not exist and the image is not a duplicate
            return wiki_page

    # Create a function to get the extension from the mime type
    def get_extension(self, path):
        """
        Get file extension from path

        :param path: The path to the file
        :return: The file extension if it can be determined
        """
        try:        
            mime = self.get_mime(path)
            extension = self.get_extension_from_mime(mime)
            return extension
        except Exception as e:
            self.log.log_error(f"Unable to determine mimetype/extension for {path}")

    def get_extension_from_mime(self, mime):
        """

        :param file:
        :return:
        """
        try:
            extension = mimetypes.guess_extension(mime)
            if extension is None:
                raise Exception(f"Unable to determine file type for {mime}")
            return extension
        except Exception as e:
            raise Exception(f"Unable to determine file type for {mime}. {e}")

    def get_mime(self, path):
        """

        :param path:
        :return:
        """
        mime = None
        # Use boto3 to get mimetype from header metadata
        if "s3://" in path:
            path_url = urlparse(path)
            bucket = path_url.netloc
            # generate full s3 key using file name from url and path generate previously
            key_parsed = f"{path_url.path.replace('//', '/').lstrip('/')}"

            response = self.s3.head_object(Bucket=bucket, Key=key_parsed)
            mime = response['ContentType']
        # Assume file is on filesystem
        else:
            mime = mimetypes.guess_type(path)[0]

        if mime is None:
            raise Exception(f"Unable to determine ContentType for {path}")
        return mime
    
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
        except Exception as e:
            raise Exception(f"Unable to get attributes from row {row}: {e.__str__}")
