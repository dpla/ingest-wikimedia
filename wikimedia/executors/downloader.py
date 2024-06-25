"""
Download images from parters

"""

import logging
import os
import tempfile

import magic
import requests
from wikimedia.utilities.exceptions import DownloadException
from wikimedia.utilities.helpers import S3Helper
from wikimedia.utilities.tracker import Result, Tracker


class Downloader:
    """
    Download images from parters
    """

    log = logging.getLogger(__name__)

    s3_helper = S3Helper()
    tracker = Tracker()

    def __init__(self):
        pass

    def download(self, source, destination):
        """
        Download asset to local file system or s3

        :param source: URL of asset to download
        :param destination: Full path to save the asset

        TODO: this return value doesn't make sense, I'm just returning the destination.
        It should be returning the status of the download
        - Skipped
        - Downloaded
        - Failed
        :return: output path, filesize
        """
        try:
            # Destination is local
            if not destination.startswith("s3://"):
                return self.save_to_local(source=source, file=destination)
            # Just destination is s3 :pray:
            bucket, key = self.s3_helper.get_bucket_key(destination)
            exists, size = self.s3_helper.file_exists(bucket=bucket, key=key)
            if exists:
                self.log.info(f" - Skipping {destination}, already exists in s3")
                self.tracker.increment(Result.SKIPPED, size=size)
                return destination, size
            self.log.info(f" - Downloading {source} to {destination}")
            destination, size = self._save_to_s3(source=source, bucket=bucket, key=key)
            self.tracker.increment(Result.DOWNLOADED, size=size)
            return destination, size
        except Exception as exec:
            self.tracker.increment(Result.FAILED)
            raise DownloadException(f"Failed download {source} - {str(exec)}") from exec

    # TODO This maybe better in the FileSystem class
    def save_to_local(self, source, file):
        """
        Download images to local file system and return path to file
        and the size of the file in bytes

        :param source: The url to download
        :param file: Full path to save asset (ex /tmp/image.jpg)
        :param filexception:
        :return:
        """
        try:
            response = requests.get(source, timeout=30)
            with open(file, "wb") as f:
                f.write(response.content)
            file_size = os.path.getsize(file)
            return file, file_size
        except Exception as exec:
            raise DownloadException(f"Failed saving to local {str(exec)}") from exec

    # TODO This maybe better in the S3Helper class; if so rename as save()
    def _save_to_s3(self, source, bucket, key):
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
        # Create temp local file and download file at source to it
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        _, size = self.download(source=source, destination=temp_file.name)
        # Get content type from file, used in metadata for s3 upload
        content_type = magic.from_file(temp_file.name, mime=True)
        # Upload temp file to s3
        try:
            with open(temp_file.name, "rb") as file:
                self.s3_helper.upload(
                    file=file,
                    bucket=bucket,
                    key=key,
                    extra_args={"ContentType": content_type},
                )
            return f"s3://{bucket}/{key}", size
        except Exception as ex:
            raise DownloadException(
                f"Error uploading {source} to s3://{bucket}/{key} \
                                     - {str(ex)}"
            ) from ex
        finally:
            os.unlink(temp_file.name)
