"""
Upload images to Wikimedia Commons

"""
import mimetypes
import os
import tempfile

import pywikibot
import boto3
import botocore

from wikiutils.exceptions import UploadException
from wikiutils.utils import Utils as WikimediaUtils

class Uploader:
    """
    Upload to Wikimedia Commons
    """
    site = None
    log = None
    s3 = boto3.resource('s3')
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

    def __init__(self, logger):
        self.log = logger
        self.wikiutils = WikimediaUtils()
        self.site = pywikibot.Site()
        self.site.login()
        self.log.log(f"Logged in user is: {self.site.user()}")

    def download(self, bucket, key, destination):
        """
        Download file from s3 to local file system
        
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
                elif client_error.response['Error']['Code'] == "403":
                    raise UploadException(f"Access denied: {bucket}{key}") from client_error  
                # TODO include specific client errors here  
                else:   
                    raise UploadException(f"Unable to download {bucket}{key} to {destination.name}: {str(client_error)}") from client_error   
                     
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
        bucket, key = self.wikiutils.get_bucket_key(file)
            

        # Download to temp file on local file system
        # Will raise exceptions if the file cannot be downloaded
        temp_file = tempfile.NamedTemporaryFile()
        self.download(bucket=bucket, key=key, destination=temp_file)

        try:
            self.site.upload(filepage=wiki_file_page,
                             source_filename=temp_file.name,
                             comment=comment,
                             text=text,
                             ignore_warnings=self.warnings_to_ignore,
                             asynchronous= True,
                             chunk_size=3000000 # 3MB
                            )
            self.log.info(f"Uploaded '{page_title}'")
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
        wiki_page = pywikibot.FilePage(self.site, title=title)
        if wiki_page.exists():
            return None
        return wiki_page
    
    def get_metadata(self, row):
        """
        Get metadata for a DPLA record from the row emitted by the download process

        :param dpla_id: Row from the data for a DPLA record and associated image file
        :return: A tuple containing 
                    DPLA identifier
                    Path to the file
                    Size of the file,
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
                bucket, key = self.wikiutils.get_bucket_key(path)
                response = self.s3.head_object(Bucket=bucket, Key=key)
                mime = response['ContentType']
            else:
                mime = mimetypes.guess_type(path)[0]
            return mimetypes.guess_extension(mime)
        except Exception as exception:
            raise UploadException(f"Unable to get extension for {path}: {str(exception)}") from exception
        