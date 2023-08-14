import os
import tempfile
import requests
import magic

from wikiutils.utils import Utils
from wikiutils.exceptions import DownloadException

class DownloadStatus:
    """
    Status of download
    """
    SKIPPED = "SKIPPED"
    DOWNLOADED = "DOWNLOADED"
    FAILED = "FAILED"

    skip_count = 0
    fail_count = 0
    download_count = 0

    def __init__(self):
        pass

    def increment(self, status):
        """
        Increment the status
        """
        if status == DownloadStatus.SKIPPED:
            DownloadStatus.skip_count += 1
        elif status == DownloadStatus.DOWNLOADED:
            DownloadStatus.download_count += 1
        elif status == DownloadStatus.FAILED:
            DownloadStatus.fail_count += 1
        else:
            raise DownloadException(f"Unknown status: {status}")

class Downloader:
    """
    Download images from parters
    """
    log = None
    utils = Utils()
    _status = DownloadStatus()

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
            # Destination is s3
            if destination.startswith("s3://"):
                bucket, key = self.utils.get_bucket_key(destination)
                exists, size = self.utils.file_exists_s3(bucket=bucket, key=key)
                if exists:
                    self.log.info(f" - Skipping {destination}, already exists in s3")
                    self._status.increment(DownloadStatus.SKIPPED)
                    return destination, size
                self.log.info(f" - Downloading {source} to {destination}")
                self._status.increment(DownloadStatus.DOWNLOADED)
                return self._download_to_s3(source=source, bucket=bucket, key=key)
            # Download to local file system
            return self._download_to_local(source=source, file=destination)
        except Exception as exeception:
            self._status.increment(DownloadStatus.FAILED)
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
                self.utils.upload_to_s3(file=file, bucket=bucket, key=key, extra_args={"ContentType": content_type})
        finally:
            os.unlink(temp_file.name)
        return f"s3://{bucket}/{key}", size
    
    
    def save(self, batch, base, rows):
            """
            Save the dataframe that contains all the metadata for the batch of downloaded files

            """
            p_out = self._batch_parquet_path(base, batch)
            self.utils.write_parquet(p_out, rows, self.UPLOAD_PARQUET_COLUMNS)

    # TODO These might be over abstracted.
    #       Additionally, these are probably mute once I've ripped out the batching of assets.
    def _batch_parquet_path(self, base, n):
        """
        Returns the path to the parquet file for the batch of downloaded files
        """
        return f"{self._batch_data_output(base, n)}batch_{n}.parquet"
    
    def _batch_data_output(self, base,n):
        """
        Returns the output path for the batch of downloaded files
        """
        return f"{base}/batch_{n}/data/"


# TODO Fix this and have it write out for all records rather than just the last one.
# write a summary of the images downloaded
# input_data = pd.DataFrame({  'dpla_id': [dpla_id],
#                             'title': [title],
#                             'wiki_markup': [wiki_markup],
#                             'iiif': [iiif],
#                             'media_master': [media_master],
#                             'downloaded': [out],
#                             'download_time': [time],
#                             'download_size': [size]
#                         })
# input_data.to_parquet(f"{output_base}/input.parquet")