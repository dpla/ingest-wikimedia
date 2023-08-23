
"""
Downloads Wikimedia eligible images from a DPLA partner

"""
from itertools import chain
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import pandas as pd
import logging

from entries.entry import Entry
from executors.downloader import Downloader
from trackers.tracker import Result, Tracker
from utilities.iiif import IIIF
from utilities.fs import FileSystem, get_datetime_prefix
from utilities.exceptions import DownloadException, IIIFException
from utilities.format import sizeof_fmt

class DownloadEntry(Entry):
    """
    Downloads Wikimedia eligible images from a DPLA partner
    """
    # Columns names emitted by the ingestion3 process
    READ_COLUMNS = { "_1": "id",
                    "_2": "wiki_markup",
                    "_3": "iiif",
                    "_4": "media_master",
                    "_5": "title"}

    TUPLE_INDEX = list(READ_COLUMNS.values())
    # Column names for the output parquet file
    WRITE_COLUMNS = ['dpla_id','path','size','title','markup','page']
    BASE_OUT = None

    downloader = None
    tracker = None

    log = logging.getLogger(__name__)

    def __init__(self, tracker: Tracker):
        self.downloader = Downloader()
        self.tracker = tracker

    def execute(self, **kwargs):
        """
        """
        from utilities.fs import S3Helper
        s3_helper = S3Helper()

        # FIXME this is a kludge to pass this var to get_images which is called during paralleization
        self.BASE_OUT = kwargs.get('output', None)
        partner = kwargs.get('partner', None)
        filter = kwargs.get('file_filter', None)

        base_input = kwargs.get('input', None)
        bucket, key = s3_helper.get_bucket_key(base_input)
        recent_key = s3_helper.most_recent_prefix(bucket=bucket, key=key)
        data_in = f"s3://{bucket}/{recent_key}"

        df = Entry.load_data(data_in=data_in, columns=self.READ_COLUMNS, file_filter=filter).head(10) # FIXME remove head(10)
        # Set the total number of DPLA items to be attempted
        self.tracker.set_dpla_count(len(df))
        # Full path to the output parquet file (partner/data/datatime prefix)
        data_out = self.output_path(partner)
        # Summary of input parameters
        self.log.info(f"Input............{data_in}")
        self.log.info(f"Output...........{data_out}")
        self.log.info(f"DPLA records.....{self.tracker.item_cnt}")

        records = df.to_dict('records')
        with ThreadPoolExecutor() as executor:
            results = [executor.submit(self.process_rows, chunk) for chunk in records]
        image_rows = [result.result() for result in results]
        self.log.info(f"Downloaded {self.tracker.image_success_cnt} images ({sizeof_fmt(self.tracker.get_size())})")

        # TODO dig into a better way to flatten this nested list
        # Flatten data and create a dataframe
        flat = list(chain.from_iterable(image_rows))
        df = pd.DataFrame(flat, columns=self.WRITE_COLUMNS)
        # Write dataframe out
        fs = FileSystem()
        fs.write_parquet(path=data_out, data=df, columns=self.WRITE_COLUMNS)

    def output_path(self, name):
        """
        Create the full path to the output parquet file

        e.g. s3://bucket/path/to/data/20200101-120000_partner_download.parquet"""
        return f"{self.BASE_OUT}/data/{get_datetime_prefix()}_{name}_download.parquet"

    def image_path(self, count, dpla_id):
        """
        Create destination path to download file to
        """
        path = f"{self.BASE_OUT}/images/{dpla_id[0]}/{dpla_id[1]}/{dpla_id[2]}/{dpla_id[3]}/{dpla_id}/{count}_{dpla_id}".strip()

        if self.BASE_OUT.startswith("s3://"):
            return path
        Path.mkdir(Path(path), parents=True, exist_ok=True)
        return path

    def get_images(self, urls, dpla_id) :
        """
        Download images for a single DPLA record from a list of urls
        """
        page = 1
        image_rows = []
        for url in urls:
            filesize = 0

            # Creates the destination path for the asset (ex. batch_x/0/0/0/0/1_dpla_id)
            image_path = self.image_path(count=page, dpla_id=dpla_id)
            try:
                output, filesize = self.downloader.download(source=url, destination=image_path)
                if output is None and len(urls) > 1:
                    err_msg = f"Multi-page record, page {page} {str(de)}"
                    raise DownloadException(err_msg)
            except Exception as de:
                raise DownloadException(f"{url}: {str(de)}") from de
            page += 1

            # Create a row for a single asset and if multiple assests exist them append them to the rows list
            # When a single asset fails to download then this object is destroyed by the `break` above and
            # the rows for already downloaded assets are not added to the final dataframe
            image_row = {
                'dpla_id': dpla_id,
                'path': output,
                'size': filesize,
                'page': page
            }
            image_rows.append(image_row)
        return image_rows

    def update_metadata(self, images, title, wiki_markup):
        """
        Update the metadata for a list of images
        """
        update = list()
        for image in images:
            image.update({'title': title, 'markup': wiki_markup})
            update.append(image)
        return update

    def process_rows(self, rows):
        """

        returns: rows: list of dicts for images and metadata associated with a single DPLA record
        """
        iiif = IIIF()
        images = []

        dpla_id = rows.get('id', None)
        title = rows.get('title', None)
        wiki_markup = rows.get('wiki_markup', None)
        manifest = rows.get('iiif', None)
        media_master = rows.get('media_master', None)

        # If the IIIF manfiest is defined that parse the manfiest to get the download urls
        # otherwise use the media_master url
        try:
            images = iiif.get_iiif_urls(manifest) if manifest else media_master
        except IIIFException as iffex:
            self.tracker.increment(Result.FAILED)
            self.log.error(f"Error getting IIIF urls for \n{dpla_id} from {manifest}\n- {str(iffex)}")
            return []

        try:
            self.log.info(f"https://dp.la/item/{dpla_id} has {len(images)} assets")
            # FIXME get_images expect base_path
            images = self.get_images(images, dpla_id)
            # Update images with metadata applicable to all images
            images = self.update_metadata(images, title, wiki_markup)
        except DownloadException as de:
            images = []
            self.tracker.increment(Result.FAILED)
            self.log.error(f"Failed download(s) for {dpla_id}\n - {str(de)}")
        return images
