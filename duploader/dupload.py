import boto3
import logging
import mimetypes
import os
import re
import requests
import tempfile
from botocore.exceptions import ClientError
from duploader.utils import Utils
from pathlib import Path
from time import process_time
from urllib.parse import urlparse
import mimetypes
import magic


class Dupload:
    site = None
    utils = None

    def __init__(self):
        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
        self.utils = Utils()

    def download_s3(self, url, out):
        """

        :param url:
        :param out:
        :param name
        :return:
        """
        start = process_time()
        s3 = boto3.client('s3')

        o = urlparse(out)
        bucket = o.netloc
        # generate full s3 key using file name from url and path generate previously
        key = f"{o.path.replace('//', '/').lstrip('/')}"

        try:
            # logging.info(f"Checking | aws s3api head-object --bucket {bucket} --key {key}")
            response = s3.head_object(Bucket=bucket, Key=key)
            size = response['ContentLength']
            logging.info(f"{key} already exists, skipping download")
            return out, 0, size  # Return if file already exists in s3
        except ClientError as ex:
            # swallow exception generated from checking ContentLength on non-existant item
            # File does not exist in S3, need to download
            pass

        # download file to tmp local
        temp_file = tempfile.NamedTemporaryFile(delete=False)

        # download local returns
        logging.info(f"Downloading {url}")
        out, time, size = self.download_local(url=url, file=temp_file.name, overwrite=True)

        content_type = magic.from_file(temp_file.name, mime=True)
        logging.info(f"Got {content_type} from {temp_file.name}")

        try:
            with open(temp_file.name, "rb") as f:
                s3.upload_fileobj(Fileobj=f, Bucket=bucket, Key=key, ExtraArgs={'ContentType': content_type})
                end = process_time()
                logging.info(f"Saved to s3://{bucket}/{key}")
                return f"s3://{bucket}/{key}", (end - start), size
        finally:
            # cleanup temp file
            os.unlink(temp_file.name)

    def download_local(self, url, file, overwrite=False):
        """

        :param overwrite: Boolean
        :param url:
        :param file:
        :return:
        """
        try:
            # Image already exists, do nothing
            if Path(file).exists() and not overwrite:
                return file, 0, os.path.getsize(file)
            else:
                start = process_time()
                response = requests.get(url)
                with open(file, 'wb') as f:
                    f.write(response.content)
                end = process_time()
                file_size = os.path.getsize(file)

                logging.info(f"Download to: {file} \n"
                             f"\tSize: {self.utils.sizeof_fmt(file_size)}")
                return file, (end - start), file_size
        except Exception as e:
            # TODO cleaner error handling here
            raise Exception(f"Failed to download {url}: {e}")

    def download(self, url, out):
        """

        :param url:
        :param out:
        :return: url, time to process download, filesize (in bytes), name
        """
        try:
            if out.startswith("s3"):
                return self.download_s3(url=url, out=out)
            else:
                return self.download_local(url=url, file=out, overwrite=False)

        except Exception as e:
            raise Exception(f"Failed to download {url}: {e}")

    def get_extension_from_file(self, file):
        """

        :param file:
        :return:
        """
        try:
            mime = magic.from_file(file, mime=True)
            ext = mimetypes.guess_extension(mime)

            logging.info(f"{file} is {mime}")
            logging.info(f"Using {ext}")
            return ext
        except Exception as e:
            raise Exception(f"Unable to determine file type for {file}")

    def get_extension_from_mime(self, mime):
        """

        :param file:
        :return:
        """
        try:
            ext = mimetypes.guess_extension(mime)
            logging.info(f"For {mime} using `{ext}`")
            return ext
        except Exception as e:
            raise Exception(f"Unable to determine file type for {mime}")

    def download_single_item(self, url, save_location):
        """

        :param url: URL from metadata record
        :param save_location: Base bath to save file
        :param name: Name of file (sans extension)
        :return:
        """
        url = url if isinstance(url, bytes) else url.encode('utf-8')
        return self.download(url=url, out=save_location)




