import os
import tempfile
import requests
import magic

from utilities.fs import S3Helper
from utilities.exceptions import DownloadException
from trackers.tracker import Tracker

class Downloader:
    """
    Download images from parters
    """
    log = None
    _s3 = S3Helper()
    _tracker = Tracker()

    # Column names for the output parquet file
    UPLOAD_PARQUET_COLUMNS = ['dpla_id', 'path', 'size', 'title', 'markup', 'page']

    def __init__(self, logger):
        self.log = logger

    def get_status(self):
        """
        Get the status of the download
        """
        return self._status

    def destination_path(self, base, batch, count, dpla_id):
        """
        Create destination path to download file to
        """
        return f"{base}/batch_{batch}/assets/{dpla_id[0]}/{dpla_id[1]}/{dpla_id[2]}/{dpla_id[3]}/{dpla_id}/{count}_{dpla_id}".strip()

    def download(self, source, destination):
        """
        Download asset to local file system or s3

        :param source: URL of asset to download
        :param destination: Full path to save the asset

        TODO: this return value doesn't make sense, I'm just returning the destination. It should
        be returning the status of the download
        - Skipped
        - Downloaded
        - Failed
        :return:    output_path: Full path to downloaded asset
                    filesize (in bytes)
        """
        try:
            # Destination is local
            if not destination.startswith("s3://"):
                return self._download_to_local(source=source, file=destination)
            # Just destination is s3 :pray:
            bucket, key = self._s3.get_bucket_key(destination)
            exists, size = self._s3.file_exists(bucket=bucket, key=key)
            if exists:
                self.log.info(f" - Skipping {destination}, already exists in s3")
                self._tracker.increment(Tracker.SKIPPED)
                return destination, size
            self.log.info(f" - Downloading {source} to {destination}")
            destination, size = self._download_to_s3(source=source, bucket=bucket, key=key)
            self._tracker.increment(Tracker.DOWNLOADED, size=size)
            return destination, size
        except Exception as exeception:
            self._tracker.increment(Tracker.FAILED)
            raise DownloadException(f"Failed to download {source}\n\t{str(exeception)}") from exeception

    def _download_to_local(self, source, file):
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
            with open(file, 'wb') as f:
                f.write(response.content)
            file_size = os.path.getsize(file)
            return file, file_size
        except Exception as exeception:
            raise DownloadException(f"Error in download_local() {str(exeception)}") from exeception

    def _download_to_s3(self, source, bucket, key):
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
        _, size = self.download(source=source, destination=temp_file.name)
        # Get content type from file, used in metadata for s3 upload
        content_type = magic.from_file(temp_file.name, mime=True)
        try:
            # Upload temp file to s3
            with open(temp_file.name, "rb") as file:
                self._s3.upload(file=file, bucket=bucket, key=key, extra_args={"ContentType": content_type})
            return f"s3://{bucket}/{key}", size
        except Exception as ex:
            raise DownloadException(f"Error uploading to s3 - s3://{bucket}/{key} -- {str(ex)}") from ex
        finally:
            os.unlink(temp_file.name)


    # TODO remove method, batching is irrelevant and the output
    # path is /base_output
    def batch_parquet_path(self, base, n):
        """
        Returns the path to the parquet file for the batch of downloaded files
        """
        return f"{self._batch_data_output(base, n)}batch_{n}.parquet"

    # TODO remove method, batching is irrelevant and the output
    # path is /base_output/datetime_partner.parquet of the like
    def _batch_data_output(self, base,n):
        """
        Returns the output path for the batch of downloaded files
        """
        return f"{base}/batch_{n}/data/"
