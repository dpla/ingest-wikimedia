"""
Utility functions for Wikimedia project
"""
__author__ = "DPLA"
__version__ = "0.0.1"
__license__ = "MIT"

import json
from pathlib import Path
from urllib.parse import urlparse

import boto3
import pandas as pd
import requests

from awswrangler import s3
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectionError

from wikiutils.exceptions import IIIFException

class Utils:
    """General utility functions for Wikimedia project"""

    # Index and column names for the input parquet file
    columns = { "_1": "id",
                "_2": "wiki_markup",
                "_3": "iiif",
                "_4": "media_master",
                "_5": "title"}

    s3 = boto3.client(service_name='s3', config=Config(signature_version='s3v4'))
    # Remove retry handler for s3, this is to prevent the botocore retry handler from retrying
    # taken from https://stackoverflow.com/questions/73910120/can-i-disable-region-redirector-s3regionredirector-in-boto3
    deq = s3.meta.events._emitter._handlers.prefix_search("needs-retry.s3")
    while len(deq) > 0:
        s3.meta.events.unregister("needs-retry.s3", handler=deq.pop())

    def __init__(self):
        pass

    def read_parquet(self, path):
        """Reads parquet file and returns a dataframe"""
        temp = []
        for file in self._get_parquet_files(path=path):
            temp.append(pd.read_parquet(file, engine='fastparquet').rename(columns=self.columns))
        return pd.concat(temp, axis=0, ignore_index=True)

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

    def sizeof_fmt(self, num, suffix='B'):
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

    def write_parquet(self, path, data, columns):
        """
        Write data to parquet file

        :param path: Path to write parquet file
        :param data: Data to write
        :param columns: Columns to write
        :return: None
        """
        pd.DataFrame(data, columns=columns).to_parquet(path)

    # IIIF Manifest functions
    def iiif_v2_urls(self, iiif):
        """
        Extracts image URLs from IIIF manfiest and returns them as a list
        # TODO
        """

    def iiif__v3_urls(self, iiif):
        """
        Needs to be implemented for Georgia uploads to Wikimedia Commons
        To be done by October 2023
        # TODO
        """

    def get_iiif_urls(self, iiif):
        """
        Extracts image URLs from IIIF manfiest and returns them as a list
        Currently only supports IIIF v2

        :param iiif: IIIF manifest URL
        :return: List of image URLs
        """

        canvases = []
        images_urls = []

        iiif_manifest = self._get_iiif_manifest(iiif)
        # if 'sequences' in iiif_manifest and there is one sequence value
        if 'sequences' in iiif_manifest and len(iiif_manifest['sequences']) == 1:
            canvases = iiif_manifest['sequences'][0]['canvases'] if 'canvases' in iiif_manifest['sequences'][0] else []
        else:
            # More than one sequence, return empty list and log some kind of message
            raise IIIFException(f"Got more than one IIIF sequence. Unsure of meaning. {iiif}")
            # self.logger.info("Got more than one IIIF sequence. Unsure of meaning. %s", iiif)
            # return []

        for canvas in canvases:
            try:
                image_url = canvas['images'][0]['resource']['@id']
                # if missing file extension add it to URL to be requested
                image_url = image_url if '.' in image_url[image_url.rfind('/'):] else f"{image_url}.jpg"
                images_urls.append(image_url)
            except KeyError as keyerr:
                raise IIIFException(f"No `image` key for: {iiif}") from keyerr
                # self.logger.error("No images defined in %s", iiif)
        return images_urls

    def _get_iiif_manifest(self, url):
        """
        :return: JSON object
        """
        try:
            request = requests.get(url, timeout=30)
            data = request.content
            return json.loads(data)
        except ConnectionError as connection_error:
            raise Exception(f"Unable to request {url}: {str(connection_error)}") from connection_error
