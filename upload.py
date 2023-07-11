import sys
import time

import boto3
import botocore
import getopt
import logging
import mimetypes
import os
import pywikibot
import tempfile
from botocore.exceptions import ClientError
from urllib.parse import urlparse

from wikiutils.utils import Utils

"""
This needs a "batch" folder for input 
Read parquet file and then upload assets 


"""


class Upload:
    site = None
    s3 = boto3.client('s3')
    log = None
    log_file_name = None

    def __init__(self):
        self.log = logging.getLogger('logger')
        self.site = pywikibot.Site()
        self.site.login()
        self.log.info(f"Logged in user is: {self.site.user()}")

    def upload(self, wiki_file_page, dpla_identifier, text, file, logger, page_title):
        """

        :param wiki_file_page:
        :param dpla_identifier:
        :param text
        :param file
        :return:
        """

        comment = f"Uploading DPLA ID \"[[dpla:{dpla_identifier}|{dpla_identifier}]]\"."
        temp_file = None

        try:
            # This is a massive kludge because direct s3 upload via source_url is not allowed.
            # Download from s3 to temp location on box then upload local file to wikimeida
            if file.startswith("s3"):
                s3 = boto3.resource('s3')

                temp_file = tempfile.NamedTemporaryFile(delete=False)
                o = urlparse(file)
                bucket = o.netloc
                key = o.path.replace('//', '/').lstrip('/')
                s3_file_name = key.split('/')[-1]
                
                with open(temp_file.name, "wb") as f:
                    try:
                        s3.Bucket(bucket).download_file(key, temp_file.name)
                        file = temp_file.name
                    except botocore.exceptions.ClientError as e:
                        if e.response['Error']['Code'] == "404":
                            log.error(f"S3 object does not exist: {bucket}{key} ")
                            return False
                        else:
                            raise

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
                
            # upload to Wikimedia
            # TODO Resolve the correct combination of report_success and ignore_warnings
            #      And route output to parse JSON and log clearer messages
            upload_result = self.site.upload(filepage=wiki_file_page,
                                             source_filename=file,
                                             comment=comment,
                                             text=text,
                                             ignore_warnings=warnings_to_ignore,
                                             asynchronous= True,
                                             chunk_size=50000000
                                            )
            log.info(f"Successfully uploaded '{page_title}'")

            return upload_result

        except Exception as e:
            if 'fileexists-shared-forbidden:' in e.__str__():
                log.error(f"Failed to upload '{page_title}' for {dpla_identifier}, File already uploaded")
            elif 'filetype-badmime' in e.__str__():
                 log.error(f"Failed to upload '{page_title}' for {dpla_identifier}, Invalid MIME type")
            elif 'filetype-banned' in e.__str__():
                log.error(f"Failed to upload '{page_title}' for {dpla_identifier}, Banned file type")
            else:
                log.exception("Reason")
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
            .replace('/', '-') \
            .replace('{', '(') \
            .replace('}', ')')

        # Add pagination to page title if needed
        if page is None:
            return f"{escaped_title} - DPLA - {dpla_identifier}{suffix}"
        else:
            return f"{escaped_title} - DPLA - {dpla_identifier} (page {page}){suffix}"

    # noinspection PyStatementEffect
    def create_wiki_file_page(self, title):
        """
        Create wiki file page
        :param title:
        :return:
        """
        page = pywikibot.FilePage(self.site, title=title)
        try:
            page.latest_file_info
            self.log.info(f"Page already exists in Wikimedia '{title}'")
            return None
        except Exception as e:
            return page


    # Create a function to get the extension from the mime type
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

# Setup log config
timestr = time.strftime("%Y%m%d-%H%M%S")
log_file_name = f"upload-{timestr}.log"

logging.basicConfig(
    level=logging.DEBUG, 
    filemode='a',
    datefmt='%H:%M:%S',
    # format='%(asctime)s %(message)s'
    format='%(filename)s: '    
            '%(levelname)s: '
            '%(funcName)s(): '
            '%(lineno)d:\t'
            '%(message)s'
    )

file_handler = logging.FileHandler(log_file_name)
file_handler.setLevel(logging.DEBUG)

log = logging.getLogger('logger')
log.addHandler(file_handler)

# Create utils
utils = Utils()
uploader = Upload()
columns = {"dpla_id": "dpla_id",
           "path": "path",
           "size": "size",
           "title": "title",
           "markup": "markup",
           "page": "page"}
input = None
failed_count, upload_count = 0

# Get input parameters 
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

log.info(f"Input: {input}")

# Input file path to parquet files generated by "downloader". Basically the ./data/ directory
# This parquet file will specify what to upload and where it can be found
file_list = utils.get_parquet_files(path=input)

for parquet_file in file_list:
    log.info(f"Processing {parquet_file}")
    df = utils.get_df(parquet_file, columns=columns)

    for row in df.itertuples(index=columns):
        start_image = time.perf_counter()
        start_image_proc = time.process_time()
        dpla_id, path, size, title, wiki_markup = None, None, None, None, None

        # Load record from dataframe
        try:
            dpla_id = getattr(row, 'dpla_id')
            path = getattr(row, 'path')
            size = getattr(row, 'size')
            title = getattr(row, 'title')
            wiki_markup = getattr(row, 'markup')
            page = getattr(row, 'page')
        except Exception as e:
            log.error(f"Unable to get attributes from row {row} in {parquet_file}: {e}")
            break

        page = None if len(df.loc[df['dpla_id'] == dpla_id]) == 1 else page

        # Get file extension
        try: 
            ext = uploader.get_extension(path)
        except Exception as e:
            log.error(f"Unable to determine mimetype/extension for {path}")
            break

        # Create Wikimedia page title
        try: 
            page_title = uploader.create_wiki_page_title(title=title,
                                                     dpla_identifier=dpla_id,
                                                     suffix=ext,
                                                     page=page)
        except Exception as e:
            log.error("Unable to generate page title for {dpla_id} - {path}")
            break

        # Create wiki page
        try:
            wiki_page = uploader.create_wiki_file_page(title=page_title)
        except Exception as e:
            log.error("Unable to generate wiki page for {dpla_id} - {path}")
            break
        
        # Do not continue if page already exists
        # This would be the place to possibly do metadata sync.
        if wiki_page is None:
            continue

        # Upload image to wiki page
        try:
            upload_status = uploader.upload(
                wiki_file_page=wiki_page,
                dpla_identifier=dpla_id,
                text=wiki_markup,
                file=path,
                logger=log,
                page_title=page_title)
            if upload_status:
                upload_count = upload_count + 1
            else:
                failed_count = failed_count +1
        except Exception as e:
            log.error(f"Unable to upload {path} because {e}")

log.info(f"Finished upload for {input}")
log.info(f"Uploaded {upload_count} new files")
log.info(f"Failed {failed_count} files")


o = urlparse(input)
bucket = o.netloc
# generate full s3 key using file name from url and path generate previously
key = f"{o.path.replace('//', '/').lstrip('/')}"

with open(log_file_name, "rb") as f:
    utils.upload_to_s3(file=f, bucket=bucket, key=f"{key}log/{log_file_name}", content_type="text/plain")

