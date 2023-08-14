"""
Filesystem utilities
"""
__author__ = "DPLA"
__version__ = "0.0.1"
__license__ = "MIT"

from pathlib import Path
from urllib.parse import urlparse

import boto3
import pandas as pd

from awswrangler import s3
from botocore.config import Config
from botocore.exceptions import ClientError

class FileSystem:
    """
    """

    s3 = boto3.client(service_name='s3', config=Config(signature_version='s3v4'))
    # Remove retry handler for s3, this is to prevent the botocore retry handler from retrying
    # taken from https://stackoverflow.com/questions/73910120/can-i-disable-region-redirector-s3regionredirector-in-boto3
    deq = s3.meta.events._emitter._handlers.prefix_search("needs-retry.s3")
    while len(deq) > 0:
        s3.meta.events.unregister("needs-retry.s3", handler=deq.pop())

    def __init__(self):
        pass

    def read_parquet(self, path, cols):
        """Reads parquet file and returns a dataframe"""
        temp = []
        for file in self._get_parquet_files(path=path):
            temp.append(pd.read_parquet(file, engine='fastparquet').rename(columns=cols))
        return pd.concat(temp, axis=0, ignore_index=True)

    def write_parquet(self, path, data, columns):
        """
        Write data to parquet file

        :param path: Path to write parquet file
        :param data: Data to write
        :param columns: Columns to write
        :return: None
        """
        pd.DataFrame(data, columns=columns).to_parquet(path)

    def _get_parquet_files(self, path):
        """
        Get parquet files from path, either local or s3

        :param path: Path to parquet files
        :return: List of parquet files
        """
        return s3.list_objects(path, suffix=".parquet") if path.startswith("s3") else Path(path).glob('*.parquet')

    def file_exists_s3(self, bucket, key):
        """
        Check to see if the file exists in s3

        :param bucket: S3 bucket
        :param key: S3 key
        """
        try:
            # this does not check to see if the image is the same image, just that the file exists
            # is should check the md5 hash of the file to see if it is the same and needs to be
            # uploaded and replace the existing file.
            response = self.s3.head_object(Bucket=bucket, Key=key)
            size = response['ContentLength']
            return True, size
        except ClientError:
            # The head request fails therefore we assume the file does not exist in s3
            return False, 0

    def get_bucket_key(self, s3_url):
        """
        Parse S3 url and return bucket and key

        :param s3_url: S3 url
        :return: bucket, key
        """
        s3_url_parsed = urlparse(s3_url)
        bucket = s3_url_parsed.netloc
        key = f"{s3_url_parsed.path.replace('//', '/').lstrip('/')}"
        return bucket, key


    def upload_to_s3(self, bucket, key, file, extra_args=None):
        """
        Uploads file to S3

        :param bucket: S3 bucket
        :param key: S3 key
        :param file: File to upload
        :param extra_args: Extra arguments to pass to upload_fileobj
        return: None
        """
        if extra_args is not None:
            self.s3.upload_fileobj(Fileobj=file, Bucket=bucket, Key=key, ExtraArgs=extra_args)
        else:
            self.s3.upload_fileobj(Fileobj=file, Bucket=bucket, Key=key)
