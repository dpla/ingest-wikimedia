import os
from pathlib import Path
from time import strftime
from urllib.parse import urlparse

import boto3
import pandas as pd
from awswrangler import s3 as s3wrangler
from botocore.config import Config
from botocore.exceptions import ClientError


class Text:
    @staticmethod
    def datetime():
        """
        Get datetime value as YYYYMMDD_HHMMSS.  It is sortable and used for
        file names and directories.

        Ex  20230101_100002_foo_bar.ext
            20230525_133102_foo_bar.ext
        :return: datetime
        """
        date = strftime("%Y%m%d")
        time = strftime("%H%M%S")
        return f"{date}_{time}"

    @staticmethod
    def log_file(partner, event_type, log_dir="./logs"):
        """
        Create a log file path for a given partner and event type

        Ex. ./logs/20230525_133102-ohio-upload.log

        :param partner: DPLA partner
        :param event_type: event type (upload, download)
        :param log_dir: log directory, default: ./logs
        """
        os.makedirs("./logs", exist_ok=True)
        log_file_name = f"{Text.datetime()}-{partner}-{event_type}.log"
        return f"{log_dir}/{log_file_name}"

    @staticmethod
    def sizeof_fmt(num, suffix='B'):
        """
        Convert bytes to human readable format

        :param num: number of bytes
        :param suffix: suffix to append to number
        :return: human readable string
        """
        for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Yi', suffix)

    @staticmethod
    def number_fmt(num):
        """
        Convert number to human readable format

        :param num: number
        :return: human readable string
        """
        return "{:,}".format(num)


class S3Helper:
    """
    """
    # Used for most s3 operations
    s3_resource = boto3.resource('s3')
    # Used for head operation in file_exists and upload_fileobj in upload
    s3_client = boto3.client(service_name='s3', config=Config(signature_version='s3v4', max_pool_connections=25, retries={'max_attempts': 3}))

    def __init__(self):
        pass

    # TODO The uploader and downloader read parquet files slightly differently
    # The uploader reads the a single parquet file (object) and downloader reads
    # a directory of parquet files (prefix).  This is a hack to get around that
    #
    # FIX: The downloader should have to ____.parquet/***.parquet (same as ingestion3)
    # and the uploader should read a directory of parquet files
    def most_recent(self, bucket, key, type: str):
        """
        Find the most recent object or prefix in a path in s3
        """
        keys = self.s3_client.list_objects(Bucket=bucket, Prefix=key, Delimiter='/')

        if type == 'prefix':
            values = [k.get('Prefix') for k in keys.get('CommonPrefixes', None)]
        elif type == 'object':
            values = [k.get('Key') for k in keys.get('Contents', None)]
        else:
            raise ValueError(f"Invalid type {type} passed to most_recent")
        return max(values)


    def write_log_s3(self, bucket, key, file, extra_args=None):
        """
        Upload log file to s3

        :param bucket: Bucket to upload log file to
        :param key: Key to upload log file to
        :param extra_args: Extra arguments to pass to s3 upload_fileobj
        :return: The URL of the uploaded log file
        """
        s3_name = Path(file).name
        log_key = f"{key}/logs/{s3_name}"
        # Default extra_args for log files are text/plain and public read.
        # These can be overridden by passing in extra_args
        default_args = {"ACL": "public-read", "ContentType": "text/plain"}
        if extra_args:
            default_args.update(extra_args)

        with open(file, "rb") as file:
            self.upload(file=file,
                            bucket=bucket,
                            key=log_key,
                            extra_args=default_args)

        # The publicly accessible S3 url for the log file
        return f"https://{bucket}.s3.amazonaws.com/{log_key}"

    def file_exists(self, bucket, key):
        """
        Check to see if the file exists in s3

        :param bucket: S3 bucket
        :param key: S3 key
        """
        # TODO does not check to see if the image is the same image, just that the file exists
        # is should check the md5 hash of the file to see if it is the same and needs to be
        # uploaded and replace the existing file.
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
            size = response.get('ContentLength', 0)
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


class ParquetHelper:
    """
    """

    s3 = S3Helper()

    def __init__(self):
        pass

    def read_parquet(self, path, cols=None):
        """Reads parquet file and returns a dataframe"""
        temp = []
        for file in self.parquet_files(path=path):
            if cols:
                temp.append(pd.read_parquet(file, engine='fastparquet').rename(columns=cols))
            temp.append(pd.read_parquet(file, engine='fastparquet'))
        return pd.concat(temp, axis=0, ignore_index=True)

    def parquet_files(self, path):
        """
        Get parquet files from path, either local or s3

        :param path: Path to parquet files
        :return: List of parquet files
        """
        return S3Helper().list_files(path, suffix=".parquet") if path.startswith("s3") else Path(path).glob('*.parquet')
