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
from botocore.exceptions import ClientError, ConnectionError

class Utils:
    """General utility functions for Wikimedia project"""

    logger = logging.getLogger('logger')

    s3 = boto3.client(service_name='s3', config=Config(signature_version='s3v4'))
    # Remove retry handler for s3, this is to prevent the botocore retry handler from retrying
    # taken from
    #   https://stackoverflow.com/questions/73910120/can-i-disable-region-redirector-s3regionredirector-in-boto3
    deq = s3.meta.events._emitter._handlers.prefix_search("needs-retry.s3")
    while len(deq) > 0:
        s3.meta.events.unregister("needs-retry.s3", handler=deq.pop())

    def __init__(self):
        pass

    def create_path(self, path):
        """

        :param path:
        :return:
        """
        # WTF is this doing? I'm really not sure what or why this exists. 
        # This doesn't seem like it is necessary for s3 since there is not need to 
        # create a path for s3.
        prefix = b's3' if isinstance(path, bytes) else "s3"
        if not path.startswith(prefix) and not Path(path).exists():
            Path(path).mkdir(parents=True)

    def download(self, source, destination):
        """
        Download asset to local file system or s3
        
        :param source: URL of asset to download
        :param destination: Full path to save asset
        :return:    url
                    time to process download
                    filesize (in bytes)
                    name
        """
        # Endcode source to bytes if it is not already
        # source = source if isinstance(source, bytes) else source.encode('utf-8')
        try:
            # If the destination is s3, then check to see if the file already exists
            if destination.startswith("s3://"):
                bucket, key = self.get_bucket_key(destination)
                exists, size =  self.file_exists_s3(bucket=bucket, key=key)
                if not exists:
                    return self.download_to_s3(source=source, bucket=bucket, key=key)
                return destination, size
            
            # If the destination is local file system, then download the file and don't overwrite it
            else: 
                return self.download_local(source=source, file=destination)
        except Exception as exception:
            raise Exception("Failed to download %s: %s", source, str(exception))

    def get_file_info(self, file, overwrite):
        """
        Does the file exist? If so, return the file path and the file size in bytes

        :param file: Full path to save asset (ex /tmp/image.jpg)
        :param overwrite: Boolean to overwrite existing downloaded file
        :return: None, 0 if the image already exists and overwrite is True otherwise 
                            return the file path. 
                file, 0 if the file does not exist and needs to be downloaded for 
                        the first time
                file, file_size if the file does exist and overwrite is False
        """
        if Path(file).exists():
            file_size = os.path.getsize(file)
            if overwrite:
                os.remove(file)
                return None, 0
            return file, file_size
        else:
            pass
            # new file to download
            return file, 0
   
    def download_local(self, source, file, overwrite=False):
        """
        Download images to local file system and return path to file 
        and the size of the file in bytes

        :param source: The url to download
        :param file: Full path to save asset (ex /tmp/image.jpg)
        :param overwrite: Boolean to overwrite existing downloaded file
        :param filexception:
        :return:
        """
        # self.download_to_temp(file, file_exists, source, overwrite)
        if overwrite: 
            os.remove(file)
        try:
            
            response = requests.get(source, timeout=30)
            with open(file, 'wb') as f:
                f.write(response.content)
            file_size = os.path.getsize(file)
            return file, file_size
        except Exception as exception:
            raise Exception("Error in download_local() %s", str(exception))

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

    def download_to_s3(self, source, bucket, key):
        """

        Tries to download a file from the source url and save it to s3. If the file
        already exists in s3 then this step is skipped. To achive this 
        the file is downloaded to a temp file on the local file system and then uploaded
        to s3. 

        :param url:
        :param out:
        :param name
        :return:
        """
        # Create temp local file and download source file to it
        temp_file = tempfile.NamedTemporaryFile(delete=False)

        out, size = self.download_local(source=source, file=temp_file.name, overwrite=True)
        # Get content type from file, used in metadata for s3 upload
        content_type = magic.from_file(temp_file.name, mime=True)
        try:
            # Upload temp file to s3
            with open(temp_file.name, "rb") as file:
                self.s3.upload_fileobj(Fileobj=file, Bucket=bucket, Key=key, ExtraArgs={'ContentType': content_type})
                return f"s3://{bucket}/{key}", size
        finally:
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
        

    def get_df(self, path, columns):
        """
        Get datqframe from path

        :param path: Path to data
        :param columns: Columns to rename
        :return: Dataframe
        """
        return pd.read_parquet(path, engine='fastparquet').rename(columns=columns)
        # return self.get_df_local(path, columns)
    
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
    
    def write_parquet(self, path, data, columns):
        """
        Write data to parquet file
        
        :param path: Path to write parquet file
        :param data: Data to write
        :param columns: Columns to write
        :return: None
        """
        self.logger.info("Saving %s", path)
        pd.DataFrame(data, columns=columns).to_parquet(path)

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
            

    def get_iiif_manifest(self, url):
        """
        :return: JSON object
        """
        try:
            request = requests.get(url, timeout=30)
            data = request.content
            return json.loads(data)
        except ConnectionError as connection_error:
            # self.logger.error("Unable to request %s: %s", url, str(connection_error))
            raise Exception(f"Unable to request {url}: {str(connection_error)}")

    def get_iiif_urls(self, iiif):
        """

        :param iiif:
        :return:
        """
        # sequences \ [array] — [0, default]  \ canvases \ [array] \ images \ [array, 0 default] \ resource \ @id
        canvases = []
        images_urls = []

        iiif_manifest = self.get_iiif_manifest(iiif)
        # if 'sequences' in iiif_manifest and there is one sequence value
        if 'sequences' in iiif_manifest and len(iiif_manifest['sequences']) == 1:
            canvases = iiif_manifest['sequences'][0]['canvases'] if 'canvases' in iiif_manifest['sequences'][0] else []
        else: 
            # More than one sequence, return empty list and log some kind of message
            self.logger.info("Got more than one IIIF sequence. Unsure of meaning. %s", iiif)
            return []

        for canvas in canvases:
            try:
                image_url = canvas['images'][0]['resource']['@id']
                # if missing file extension add it to URL to be requested
                image_url = image_url if '.' in image_url[image_url.rfind('/'):] else f"{image_url}.jpg"
                images_urls.append(image_url)
            except KeyError:
                self.logger.error("No images defined in %s", iiif)
        return images_urls

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
    
    def create_destination_path(self, base, batch, count, dpla_id):
        """
        Create destination path to download file to
        """
        return f"{base}/batch_{batch}/assets/{dpla_id[0]}/{dpla_id[1]}/{dpla_id[2]}/{dpla_id[3]}/{dpla_id}/{count}_{dpla_id}".strip()
