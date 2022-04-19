from time import process_time

from awswrangler import s3
import boto3
import json
import logging
import magic
import mimetypes
import os
import pandas as pd
import requests
import tempfile
from botocore.config import Config
from botocore.exceptions import ClientError
from pathlib import Path
from urllib.parse import urlparse

import traceback


class Utils:
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
            else:
                return self.download_local(url=url, file=out, overwrite=False)

        except Exception as e:
            raise Exception(f"Failed to download {url}: {traceback.format_exc()}")

    def download_local(self, url, file, overwrite=False):
        """

        :param overwrite: Boolean
        :param url:
        :param file:
        :return:
        """
        exists = Path(file).exists()
        try:
            # Image already exists, do nothing
            if exists and not overwrite:
                return file, 0, os.path.getsize(file)
            else:
                if exists:
                    os.remove(file)
                start = process_time()
                response = requests.get(url)
                with open(file, 'wb') as f:
                    f.write(response.content)
                end = process_time()
                file_size = os.path.getsize(file)

                self.logger.info(f"Download {url}, {self.sizeof_fmt(file_size)}")
                return file, (end - start), file_size
        except Exception as e:
            # TODO cleaner error handling here
            raise Exception(f"Failed to download local {url}: {e}")

    def download_s3(self, url, out):
        """

        :param url:
        :param out:
        :param name
        :return:
        """
        start = process_time()


        o = urlparse(out)
        bucket = o.netloc
        # generate full s3 key using file name from url and path generate previously
        key = f"{o.path.replace('//', '/').lstrip('/')}"

        try:
            response = self.s3.head_object(Bucket=bucket, Key=key)
            size = response['ContentLength']
            self.logger.info(f"{key} already exists, skipping download")
            return out, 0, size  # Return if file already exists in s3
        except ClientError as ex:
            # swallow exception generated from checking ContentLength on non-existant item
            # File does not exist in S3, need to download
            pass

        # Create tmp local file for download
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        out, time, size = self.download_local(url=url, file=temp_file.name, overwrite=True)
        # Get content type from file, used in metadata for s3 upload
        content_type = magic.from_file(temp_file.name, mime=True)
        try:
            with open(temp_file.name, "rb") as f:
                self.s3.upload_fileobj(Fileobj=f, Bucket=bucket, Key=key, ExtraArgs={'ContentType': content_type})
                end = process_time()
                self.logger.info(f"Uploaded to s3://{bucket}/{key}")
                return f"s3://{bucket}/{key}", (end - start), size
        finally:
            # cleanup temp file
            os.unlink(temp_file.name)

    def get_df_s3(self, path, columns):
        return s3 \
            .read_parquet(path=path) \
            .rename(columns=columns)

    def get_df_local(self, path, columns):
        return pd.read_parquet(path, engine='fastparquet').rename(columns=columns)

    def get_df(self, path, columns):
        return self.get_df_local(path, columns)

    def get_extension_from_file(self, file):
        """

        :param file:
        :return:
        """
        try:
            mime = magic.from_file(file, mime=True)
            ext = mimetypes.guess_extension(mime)

            self.logger.info(f"{file} is {mime}")
            self.logger.info(f"Using {ext}")
            return ext
        except Exception as e:
            raise Exception(f"Unable to determine file type for {file}")

    def get_extension_from_mime(self, mime):
        """

        :param file:
        :return:
        """
        try:
            ext = mimetypes.guess_extension(mime)
            self.logger.info(f"For {mime} using `{ext}`")
            return ext
        except Exception as e:
            raise Exception(f"Unable to determine file type for {mime}")

    def get_iiif_urls(self, iiif):
        """

        :param iiif:
        :return:
        """
        # sequences \ [array] — [0, default]  \ canvases \ [array] \ images \ [array, 0 default] \ resource \ @id
        canvases = None
        sequences = None
        try:
            request = requests.get(iiif)
            data = request.content
            iiif_manifest = json.loads(data)
            sequences = iiif_manifest['sequences']
        except ConnectionError as ce:
            self.logger.error(f"Unable to request {iiif}: {ce}")
            return list()
        except Exception as e:
            self.logger.error(f"Unknown error requesting {iiif}: {e}")
            return list()

        if len(sequences) > 1:
            # More than one sequence, return empty list and log some kind of message
            self.logger.info(f"Got more than one IIIF sequence. Unsure of meaning. {iiif}")
            return list()
        elif len(sequences) == 1:
            try:
                canvases = sequences[0]['canvases']
            except KeyError as e:
                self.logger.info(f'No canvasses defined in  {iiif}')
            except Exception as e:
                self.logger.info(f'Error extracting canvasses  {iiif} because {e}')

        if canvases is None:
            self.logger.info(f"No sequences or canvases in IIIF manifest: {iiif}")
            return list()

        images_urls = list()
        for canvas in canvases:
            image_url = canvas['images'][0]['resource']['@id']
            # if missing file extension add it to URL to be requested
            image_url = image_url if '.' in image_url[image_url.rfind('/'):] else f"{image_url}.jpg"
            images_urls.append(image_url)

        return images_urls

    def get_parquet_files(self, path):
        return s3.list_objects(path, suffix=".parquet") if path.startswith("s3") else self.get_local_parquet(path)

    def get_local_parquet(self, path):
        return Path(path).glob('*.parquet')

    def sizeof_fmt(self, num, suffix='B'):
        for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Yi', suffix)

    def timer_message(self, msg, start, end):
        time = "{:.5f}".format(round(end - start, 2))
        return f"{time}: {msg}"

    def write_parquet(self, path, data, columns):
        self.logger.info(f"Saving {path}")
        df_out = pd.DataFrame(data, columns=columns)
        df_out.to_parquet(path)
