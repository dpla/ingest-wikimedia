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


class Dupload:
    site = None

    def __init__(self):
        #  This is only required for the uploader
        # self.site = pywikibot.Site()
        # self.site.login()

        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

        # logging.info(f"Logged in user is: {self.site.user()}")

    def download_s3(self, url, out):
        """

        :param url:
        :param out:
        :return:
        """
        start = process_time()

        s3 = boto3.client('s3')

        o = urlparse(out)
        bucket = o.netloc
        key = o.path.replace('//', '/').lstrip('/')

        try:
            logging.info(f"Checking | aws s3api head-object --bucket {bucket} --key {key}")
            response = s3.head_object(Bucket=bucket, Key=key)
            size = response['ContentLength']
            return out, 0, size
        except ClientError as ex:
            # File does not exist in S3, need to download
            pass

        temp_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            out, time, size = self.download_local(url, temp_file.name, overwrite=True)
            with open(temp_file.name, "rb") as f:
                s3.upload_fileobj(f, bucket, key)
                end = process_time()
                logging.info(f"Uploaded to s3://{bucket}/{key}")
                return f"s3://{bucket}/{key}", (end - start), size
        finally:
            # logging.info(f"Deleting temp file {temp_file.name}")
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
            # Download image
            else:
                start = process_time()
                request = requests.get(url)
                with open(out, 'wb') as f:
                    f.write(request.content)
                end = process_time()
                file_size = os.path.getsize(out)

                logging.info(f"Downloaded to: {out}. Size: {file_size} (bytes)")
                return out, (end - start), file_size
        except Exception as e:
            # TODO cleaner error handling here
            # logging.error(f"Failed to download {url}: {e}")
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
        output_file = f"{save_location}/{url.split('/')[-1]}"
        return self.download(url=url, out=output_file)


