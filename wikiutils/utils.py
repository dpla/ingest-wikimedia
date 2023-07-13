"""
Utility functions for Wikimedia project
"""
__author__ = "DPLA"
__version__ = "0.0.1"
__license__ = "MIT"

import json
import logging
import mimetypes
import os
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import boto3
import magic
import pandas as pd
import requests

from awswrangler import s3
from botocore.config import Config
from botocore.exceptions import ClientError


class Utils:
    """General utility functions for Wikimedia project"""

    logger = logging.getLogger('logger')

    s3 = boto3.client(service_name='s3', config=Config(signature_version='s3v4'))

    def __init__(self):
        pass

    def create_path(self, path):
        """

        :param path:
        :return:
        """
        prefix = b's3' if isinstance(path, bytes) else "s3"
        if not path.startswith(prefix) and not Path(path).exists():
            Path(path).mkdir(parents=True)

    def download(self, url, out):
        """

        :param url: URL of asset to download
        :param out: Full path to save asset
        :return: url, time to process download, filesize (in bytes), name
        """
        url = url if isinstance(url, bytes) else url.encode('utf-8')
        try:
            if out.startswith("s3"):
                return self.download_s3(url=url, out=out)
            return self.download_local(url=url, file=out, overwrite=False)
        except Exception as exception:
            raise Exception(f"Failed to download %s: %s", url, exception.__str__) from exception

    def download_local(self, url, file, overwrite=False):
        """
        Download images locally

        :param overwrite: Boolean
        :param url:
        :param filexception:
        :return:
        """
        exists = Path(file).exists()
        try:
            # Image already exists, do nothing
            if exists and not overwrite:
                return file, 0, os.path.getsize(file)
            if exists:
                os.remove(file)
            response = requests.get(url, timeout=10)
            with open(file, 'wb') as f:
                f.write(response.content)
            file_size = os.path.getsize(file)
            self.logger.info(f"Download {url}, {self.sizeof_fmt(file_size)}")
            return file, file_size
        except Exception as exception:
            raise Exception(f"Failed to download local {url}: {exception.__str__}") from exception

    def download_s3(self, url, out):
        """

        :param url:
        :param out:
        :param name
        :return:
        """
        o = urlparse(out)
        bucket = o.netloc
        # Generate full s3 key using file name from url and path generate previously
        key = f"{o.path.replace('//', '/').lstrip('/')}"

        try:
            response = self.s3.head_object(Bucket=bucket, Key=key)
            size = response['ContentLength']
            self.logger.info(f"%s already exists, skipping download", key)
            return out, 0, size  # Return if file already exists in s3
        except ClientError as client_error:
            # swallow exception generated from checking ContentLength on non-existant item
            # File does not exist in S3, need to download
            pass

        try:
            # Create tmp local file for download
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            out, size = self.download_local(url=url, file=temp_file.name, overwrite=True)
            # Get content type from file, used in metadata for s3 upload
            content_type = magic.from_file(temp_file.name, mime=True)
            # Upload to s3
            with open(temp_file.name, "rb") as f:
                self.s3.upload_fileobj(Fileobj=f, Bucket=bucket, Key=key, ExtraArgs={'ContentType': content_type})
                self.logger.info(f"Uploaded to s3://{bucket}/{key}")
                return f"s3://{bucket}/{key}", size
        finally:
            # cleanup temp file
            os.unlink(temp_file.name)

    def upload_to_s3(self, bucket, key, file, content_type): 
        """
        Uploads file to S3
        
        :param bucket: S3 bucket
        :param key: S3 key
        :param filexception: File to upload
        :param content_typexception: Content type of file
        return: None
        """
        self.s3.upload_fileobj(Fileobj=file, Bucket=bucket, Key=key, ExtraArgs={'ContentType': content_type})

    def get_df_s3(self, path, columns):
        """
        Get dataframe from S3 path

        :param path: S3 path
        :param columns: Columns to rename
        :return: Dataframe
        """
        return s3.read_parquet(path=path).rename(columns=columns)

    def get_df_local(self, path, columns):
        """
        Get dataframe from local path

        :param path: Local path
        :param columns: Columns to rename
        :return: Dataframe
        """
        return pd.read_parquet(path, engine='fastparquet').rename(columns=columns)

    def get_df(self, path, columns):
        """
        Get datqframe from path

        :param path: Path to data
        :param columns: Columns to rename
        :return: Dataframe
        """
        return self.get_df_local(path, columns)

    def get_extension_from_file(self, file):
        """
        Guess the extension from the file type
        :param filexception:
        :return:
        """
        mime = magic.from_file(file, mime=True)
        extension = mimetypes.guess_extension(mime)
        if extension is None:
            raise Exception(f"Unable to determine file type for %s", file)
        return extension            

    def get_extension_from_mime(self, mime):
        """
        Guess the extension from the mime type
        :param filexception: file to guess extension for
        :return: extension or Exception
        """
        ext = mimetypes.guess_extension(mime)
        if ext is None:
            raise Exception(f"Unable to determine file type for {mime}")
        return ext
            

    def get_iiif_urls(self, iiif):
        """

        :param iiif:
        :return:
        """
        # sequences \ [array] — [0, default]  \ canvases \ [array] \ images \ [array, 0 default] \ resource \ @id
        canvases = None
        sequences = None
        try:
            request = requests.get(iiif, timeout=10)
            data = request.content
            iiif_manifest = json.loads(data)
            sequences = iiif_manifest['sequences']
        except ConnectionError as connection_error:
            self.logger.error("Unable to request %s: %s", iiif, connection_error.__str__)
            return []
        except Exception as exception:
            self.logger.error(f"Unknown error requesting {iiif}: {exception}")
            return []

        if len(sequences) > 1:
            # More than one sequence, return empty list and log some kind of message
            self.logger.info(f"Got more than one IIIF sequence. Unsure of meaning. %s", iiif)
            return []
        if len(sequences) == 1:
            try:
                canvases = sequences[0]['canvases']
            except KeyError as key_error:
                self.logger.info(f'No canvasses defined in %s', iiif)
            except Exception as exception:
                self.logger.info("Error extracting canvasses  %s: %s", iiif, exception.__str__)

        if canvases is None:
            self.logger.info(f"No sequences or canvases in IIIF manifest: {iiif}")
            return []

        images_urls = []
        for canvas in canvases:
            try:
                image_url = canvas['images'][0]['resource']['@id']
                # if missing file extension add it to URL to be requested
                image_url = image_url if '.' in image_url[image_url.rfind('/'):] else f"{image_url}.jpg"
                images_urls.append(image_url)
            except KeyError as key_error:
                self.logger.info(f'No images defined in  {iiif}')
            except Exception as exception:
                self.logger.info(f'Error extracting canvasses  {iiif} because {exception}')
        return images_urls

    def get_parquet_files(self, path):
        """
        Get parquet files from path, either local or s3
        
        :param path: Path to parquet files
        :return: List of parquet files
        """
        return s3.list_objects(path, suffix=".parquet") if path.startswith("s3") else self.get_local_parquet(path)

    def get_local_parquet(self, path):
        """
        Get local parquet files
        
        :param path: Path to local parquet files
        :return: List of parquet files
        """
        return Path(path).glob('*.parquet')

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

    def write_parquet(self, path, data, columns):
        """
        Write data to parquet file
        
        :param path: Path to write parquet file
        :param data: Data to write
        :param columns: Columns to write
        :return: None
        """
        self.logger.info(f"Saving {path}")
        df_out = pd.DataFrame(data, columns=columns)
        df_out.to_parquet(path)
