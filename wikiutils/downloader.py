
import os
import tempfile
import requests
import magic

from wikiutils.utils import Utils
from wikiutils.exceptions import DownloadException

class Downloader:
    """
    Download images from parters
    """
    log = None
    utils = Utils()

    def __init__(self, logger):
        self.log = logger

    # Path to save the dataframe which holds all the metadata for the batch of downloaded files
    def batch_data_output(self, base,n):
        """
        Returns the output path for the batch of downloaded files
        """
        return f"{base}/batch_{n}/data/"

    def batch_parquet_path(self, base, n):
        """
        """
        return f"{self.batch_data_output(base, n)}batch_{n}.parquet"

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
            # Destination is s3
            if destination.startswith("s3://"):
                bucket, key = self.utils.get_bucket_key(destination)
                exists, size = self.utils.file_exists_s3(bucket=bucket, key=key)
                if exists:
                    return destination, size
                return self._download_to_s3(source=source, bucket=bucket, key=key)
            # Download to local file system
            return self._download_to_local(source=source, file=destination)
        except Exception as exeception:
            raise DownloadException(f"Failed to download {source}\n\t{str(exeception)}") from exeception

    def _download_to_local(self, source, file, overwrite=False):
        """
        Download images to local file system and return path to file 
        and the size of the file in bytes

        :param source: The url to download
        :param file: Full path to save asset (ex /tmp/image.jpg)
        :param overwrite: Boolean to overwrite existing downloaded file
        :param filexception:
        :return:
        """
        if overwrite:
            os.remove(file)
        try:
            response = requests.get(source, timeout=30)
            with open(file, 'wb') as f:
                f.write(response.content)
        except Exception as exeception:
            raise DownloadException(f"Error in download_local() {str(exeception)}") from exeception
        
        file_size = os.path.getsize(file)
        return file, file_size

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

        _, size = self._download_to_local(source=source, file=temp_file.name, overwrite=True)
        # Get content type from file, used in metadata for s3 upload
        content_type = magic.from_file(temp_file.name, mime=True)
        try:
            # Upload temp file to s3
            with open(temp_file.name, "rb") as file:
                self.utils.upload_to_s3(file=file, bucket=bucket, key=key, content_type=content_type)
                return f"s3://{bucket}/{key}", size
        finally:
            os.unlink(temp_file.name) 