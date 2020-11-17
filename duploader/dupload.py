import os

import logging
from time import process_time

import boto3
import logging
import requests
import tempfile
from pathlib import Path
from urllib.parse import urlparse
from botocore.exceptions import ClientError
from duploader.utils import Utils


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
        :return:
        """
        start = process_time()

        s3 = boto3.client("s3")

        o = urlparse(out)
        bucket = o.netloc
        key = o.path.replace('//', '/').lstrip('/')

        try:
            # logging.info(f"Checking | aws s3api head-object --bucket {bucket} --key {key}")
            response = s3.head_object(Bucket=bucket, Key=key)
            size = response['ContentLength']
            logging.info(f"{out} already exists, skipping download")
            return out, 0, size  # Return if file already exists in s3
        except ClientError as ex:
            # File does not exist in S3, need to download
            pass

        temp_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            out, time, size = self.download_local(url, temp_file.name, overwrite=True)
            with open(temp_file.name, "rb") as f:
                s3.upload_fileobj(f, bucket, key)
                end = process_time()
                logging.info(f"Saved to s3://{bucket}/{key}")
                return f"s3://{bucket}/{key}", (end - start), size
        finally:
            os.unlink(temp_file.name)

    def download_local(self, url, out, overwrite=False):
        """

        :param overwrite: Boolean
        :param url:
        :param out:
        :return:
        """
        try:
            # Image already exists, do nothing
            if Path(out).exists() and not overwrite:
                logging.info(f"{out} already exists, skipping download")
                return out, 0, os.path.getsize(out)
            else:
                start = process_time()
                request = requests.get(url)
                with open(out, 'wb') as f:
                    f.write(request.content)
                end = process_time()
                file_size = os.path.getsize(out)

                logging.info(f"Download to: {out} \n"
                             f"\tSize: {self.utils.sizeof_fmt(file_size)}")
                return out, (end - start), file_size
        except Exception as e:
            # TODO cleaner error handling here
            raise Exception(f"Failed to download {url}: {e}")

    def download(self, url, out):
        """

        :param url:
        :param out:
        :return: url, time to process download, filesize (in bytes)
        """
        try:

            # TODO handle write to S3
            if out.startswith("s3"):
                return self.download_s3(url, out)
            else:
                return self.download_local(url, out)

        except Exception as e:
            # TODO cleaner error handling here
            raise Exception(f"Failed to download {url}: {e}")

    def download_single_item(self, url, save_location):
        # TODO confirm replacing .jp2 with .jpeg
        # .replace('.jp2', '.jpeg')
        url = url if isinstance(url, bytes) else url.encode('utf-8')

        file = url.split(b'/')[-1]
        output_file = f"{save_location}/{file}"
        return self.download(url=url, out=output_file)


