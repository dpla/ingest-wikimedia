"""
Filesystem utilities
"""
__author__ = "DPLA"
__version__ = "0.0.1"
__license__ = "MIT"

from time import strftime
from pathlib import Path
from urllib.parse import urlparse

import boto3
import pandas as pd

from awswrangler import s3 as s3wrangler
from botocore.config import Config
from botocore.exceptions import ClientError

def get_datetime_prefix():
    """
    Get a datetime prefix for the log file

    ex 20230525_133102

    :return: datetime prefix
    """
    date = strftime("%Y%m%d")
    time = strftime("%H%M%S")
    return f"{date}_{time}"

class S3Helper:
    """
    """
    # Used for most s3 operations
    s3_resource = boto3.resource('s3')
    # Used for head operation in file_exists and upload_fileobj in upload
    s3_client = boto3.client(service_name='s3', config=Config(signature_version='s3v4', max_pool_connections=25, retries={'max_attempts': 3}))

    # Remove retry handler for s3, this is to prevent the botocore retry
    # handler from retrying. Taken from https://tinyurl.com/jd27xjz4
    deq = s3_client.meta.events._emitter._handlers.prefix_search("needs-retry.s3")
    while len(deq) > 0:
        s3_client.meta.events.unregister("needs-retry.s3", handler=deq.pop())

    def __init__(self):
        pass

    def write_log_s3(self, key, bucket, file, extra_args=None):
        """
        Upload log file to s3
        :param key: Key to upload log file to
        :param bucket: Bucket to upload log file to
        :param extra_args: Extra arguments to pass to s3 upload_fileobj
        :return: The URL of the uploaded log file
        """
        s3_log_key = f"{key}"

        # Default extra_args for log files are text/plain and public read.
        # These can be overridden by passing in extra_args
        default_args = {"ACL": "public-read", "ContentType": "text/plain"}
        if extra_args:
            default_args.update(extra_args)

        with open(file, "rb") as file:
            self.upload(file=file,
                            bucket=bucket,
                            key=key,
                            extra_args=default_args)

        # The publicly accessible S3 url for the log file
        return f"https://{bucket}.s3.amazonaws.com/{key}"

    def file_exists(self, bucket, key):
        """
        Check to see if the file exists in s3

        :param bucket: S3 bucket
        :param key: S3 key
        """
        try:
            # this does not check to see if the image is the same image, just that the file exists
            # is should check the md5 hash of the file to see if it is the same and needs to be
            # uploaded and replace the existing file.
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
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

    def list_files(self, path, suffix):
        """
        Returns a list of files in the s3 path with the given suffix
        """
        # FIXME this can be done with standard libraries and will elminate the need for wranger library
        return s3wrangler.list_objects(path=path, suffix=suffix)

    def upload(self, bucket, key, file, extra_args=None):
        """
        Uploads file to S3

        :param bucket: S3 bucket
        :param key: S3 key
        :param file: File to upload
        :param extra_args: Extra arguments to pass to upload_fileobj
        return: None
        """
        if extra_args is not None:
            self.s3_client.upload_fileobj(Fileobj=file, Bucket=bucket, Key=key, ExtraArgs=extra_args)
        else:
            self.s3_client.upload_fileobj(Fileobj=file, Bucket=bucket, Key=key)

class FileSystem:
    """
    """

    s3 = S3Helper()

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
        return S3Helper().list_files(path, suffix=".parquet") if path.startswith("s3") else Path(path).glob('*.parquet')
