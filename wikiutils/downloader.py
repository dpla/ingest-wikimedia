
import os
import tempfile
import requests

import magic
import boto3

from wikiutils.utils import Utils
# from wikiutils.logger import WikimediaLogger
from wikiutils.exceptions import DownloadException

class Downloader:
    """
    Download images from parters
    """
    log = None
    s3 = boto3.client('s3')
    utils = Utils()

    def __init__(self, logger):
        self.log = logger # WikimediaLogger(partner_name=partner_name, event_type="download")

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
                bucket, key = self.utils.get_bucket_key(destination)
                exists, size = self.utils.file_exists_s3(bucket=bucket, key=key)
                if not exists:
                    return self.utils.download_to_s3(source=source, bucket=bucket, key=key)
                return destination, size
            # If the destination is local file system, then download the file and don't overwrite it
            else: 
                return self._download_local(source=source, file=destination)
        except Exception as exeception:
            raise DownloadException("Failed to download %s: %s" % (source, str(exeception))) from exeception

    # TODO refactor as download_to_local
    def _download_local(self, source, file, overwrite=False):
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

        _, size = self._download_local(source=source, file=temp_file.name, overwrite=True)
        # Get content type from file, used in metadata for s3 upload
        content_type = magic.from_file(temp_file.name, mime=True)
        try:
            # Upload temp file to s3
            with open(temp_file.name, "rb") as file:
                self.s3.upload_fileobj(Fileobj=file, Bucket=bucket, Key=key, ExtraArgs={'ContentType': content_type})
                return f"s3://{bucket}/{key}", size
        finally:
            os.unlink(temp_file.name) 