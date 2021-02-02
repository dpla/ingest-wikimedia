import os

import logging
from time import process_time

import boto3
import botocore
import logging
import pywikibot
import tempfile
from pywikibot import UploadWarning
from urllib.parse import urlparse
from botocore.exceptions import ClientError
import getopt
import sys

from wikiutils.utils import Utils
import magic
import mimetypes

"""
This needs a "batch" folder for input 
Read parquet file and then upload assets 


"""


class Upload:
    site = None
    s3 = boto3.client('s3')

    def __init__(self):
        #  This is only required for the uploader
        self.site = pywikibot.Site()
        self.site.login()

        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

        logging.info(f"Logged in user is: {self.site.user()}")

    def upload(self, wiki_file_page, dpla_identifier, text, file):
        """

        :parama wiki_file_page:
        :param dpla_identifier:
        :param text
        :param file
        :return:
        """

        comment = f"Uploading DPLA ID {dpla_identifier}"
        temp_file = None
        try:
            # This is a massive kludge because direct s3 upload via source_url is not allowed.
            # Download from s3 to temp location on box then upload local file to wikimeida
            if file.startswith("s3"):
                s3 = boto3.resource('s3')
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=file[file.rfind('.'):])
                o = urlparse(file)
                bucket = o.netloc
                key = o.path.replace('//', '/').lstrip('/')

                with open(temp_file.name, "wb") as f:
                    try:
                        s3.Bucket(bucket).download_file(key, temp_file.name)
                        file = temp_file.name
                    except botocore.exceptions.ClientError as e:
                        if e.response['Error']['Code'] == "404":
                            logging.info("The object does not exist.")
                            return False
                        else:
                            raise
            # upload to Wikimedia
            # TODO Resolve the correct combination of report_success and ignore_warnings
            #      And route output to parse JSON and log clearer messages
            return self.site.upload(filepage=wiki_file_page,
                                    source_filename=file,
                                    comment=comment,
                                    text=text,
                                    report_success=True,
                                    ignore_warnings=True
                                    )
        except Exception as e:
            logging.info(f"Error uploading {wiki_file_page}: {e}")
            return False
        finally:
            if temp_file:
                os.unlink(temp_file.name)

    def create_wiki_page_title(self, title, dpla_identifier, suffix, page=None):
        """

        :param title:
        :param dpla_identifier:
        :param suffix:
        :param page:
        :return:
        """

        # take only the first 181 characters of image file name
        # replace [ with (
        # replace ] with )
        # replace / with -
        escaped_title = title[0:181] \
            .replace('[', '(') \
            .replace(']', ')') \
            .replace('/', '-')

        # Add pagination to page title if needed
        if page is None:
            return f"{escaped_title} - DPLA - {dpla_identifier}{suffix}"
        else:
            return f"{escaped_title} - DPLA - {dpla_identifier} (page {page}){suffix}"

    def create_wiki_file_page(self, title):
        """
        Create wiki file page
        :param title:
        :return:
        """
        try:
            return pywikibot.FilePage(self.site, title=title)
        except Exception as e:
            logging.error(f"Unable to create FilePage: {e}")

    def get_extension(self, path):
        """

        :param path:
        :return:
        """
        mime = self.get_mime(path)
        extension = self.get_extension_from_mime(mime)
        return extension

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


utils = Utils()
uploader = Upload()
columns = {"dpla_id": "dpla_id",
           "path": "path",
           "size": "size",
           "title": "title",
           "markup": "markup",
           "page": "page"}
input = None
upload_count = 1

try:
    opts, args = getopt.getopt(sys.argv[1:], "hi:u:o:", ["input="])
except getopt.GetoptError:
    print('upload.py --input <path to parquet>')
    sys.exit(2)

for opt, arg in opts:
    if opt == '-h':
        print(
            'upload.py --input <path to parquet>')
        sys.exit()
    elif opt in ("-i", "--input"):
        input = arg

logging.info(f"Input:      {input}")

# Input file path to parquet files generated by "downloader". Basically the ./data/ directory
# This parquet file will specify what to upload and where it can be found

# read parquet files

file_list = utils.get_parquet_files(path=input)

for parquet_file in file_list:
    logging.info(f"Processing...{parquet_file}")
    df = utils.get_df(parquet_file, columns=columns)

    for row in df.itertuples(index=columns):
        dpla_id, path, size, title, wiki_markup = None, None, None, None, None
        try:
            dpla_id = getattr(row, 'dpla_id')
            path = getattr(row, 'path')
            size = getattr(row, 'size')
            title = getattr(row, 'title')
            wiki_markup = getattr(row, 'markup')
            page = getattr(row, 'page')
        except Exception as e:
            logging.error(f"Unable to get attributes from row {row}: {e}")
            break

        page = None if len(df.loc[df['dpla_id'] == dpla_id]) == 1 else page

        # Create Wikimedia page title
        logging.info(f"Starting file {dpla_id} {path}")
        ext = uploader.get_extension(path)
        page_title = uploader.create_wiki_page_title(title=title,
                                                     dpla_identifier=dpla_id,
                                                     suffix=ext,
                                                     page=page)

        # Create wiki page
        wiki_page = uploader.create_wiki_file_page(title=page_title)

        # Upload to wiki page
        try:
            upload_status = uploader.upload(
                            wiki_file_page=wiki_page,
                            dpla_identifier=dpla_id,
                            text=wiki_markup,
                            file=path)
            if upload_status:
                logging.info(f"Uploaded {dpla_id}. Uploaded count {upload_count}")
                upload_count = upload_count + 1
        except Exception as e:
            logging.error(f"Unable to upload: {e}\nTarget file {path}")
