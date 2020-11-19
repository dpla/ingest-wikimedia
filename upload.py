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

from duploader.dupload import Dupload
from duploader.utils import Utils

"""
This needs a "batch" folder for input 
Read parquet file and then upload assets 


"""


class Upload:
    site = None
    download = Dupload()

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
            # Download from s3 to temp location on ec2
            # Upload to wikimeida
            if file.startswith("s3"):
                s3 = boto3.resource('s3')
                temp_file = tempfile.NamedTemporaryFile(delete=False)
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
                        else:
                            raise
                self.site.upload(filepage=wiki_file_page,
                                 source_filename=file,
                                 comment=comment,
                                 text=text)
                logging.info(f"Uploaded local file: {file}")
        except UploadWarning as upload_warning:
            logging.warning(f"{upload_warning}")
        except Exception as e:
            logging.error(f"Error uploading {wiki_file_page}: {e}")
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


utils = Utils()
upload = Upload()
columns = {"dpla_id": "dpla_id", "path": "path", "size": "size", "title": "title", "markup": "markup"}
input = None

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
        except Exception as e:
            logging.error(f"Unable to get attributes from row {row}: {e}")

        # Create Wikimedia page title
        page_title = upload.create_wiki_page_title(title=title,
                                                   dpla_identifier=dpla_id,
                                                   suffix=path[path.rfind('.'):])

        logging.info(f"Create page title {page_title}")

        # Create wiki page
        wiki_page = upload.create_wiki_file_page(title=page_title)

        # Upload to wiki page
        try:
            upload.upload(wiki_file_page=wiki_page,
                          dpla_identifier=dpla_id,
                          text=wiki_markup,
                          file=path)
            logging.info(f"Uploaded {dpla_id}")
        except Exception as e:
            logging.error(f"Unable to upload: {e}\nTarget file {path}")


        # Force abort
        sys.exit()
