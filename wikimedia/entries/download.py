
"""
Downloads Wikimedia eligible images from a DPLA partner

"""
from itertools import chain
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import logging

from utilities.iiif import IIIF
from executors.downloader import Downloader
from utilities.fs import FileSystem, get_datetime_prefix
from utilities.exceptions import DownloadException, IIIFException
from utilities.format import sizeof_fmt

class DownloadEntry():
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

    downloader = None
    args = None
    log = logging.getLogger(__name__)

    def __init__(self, args):
        self.args = args
        self.downloader = Downloader()

    def load_data(self, data_in, file_filter = None):
        """
        Load data from parquet file and filter out ids if a file filter is provided
        """
        fs = FileSystem()

        if file_filter:
            self.log.info(f"Using filter: {file_filter}")
            exclude_ids = []
            with open(file_filter, encoding='utf-8') as f:
                exclude_ids = [line.rstrip() for line in f]
            return fs.read_parquet(data_in, cols=self.READ_COLUMNS).filter(lambda x: x.id in exclude_ids)

        return fs.read_parquet(data_in, cols=self.READ_COLUMNS)

    def data_out_path(self, base, name):
        """
        Create the full path to the output parquet file

        e.g. s3://bucket/path/to/data/20200101-120000_partner_download.parquet
        """
        return f"{base}/data/{get_datetime_prefix()}_{name}_download.parquet"

    def get_images(self, urls, dpla_id) :
        """
        Download images for a single DPLA record from a list of urls
        """
        page = 1
        image_rows = []
        for url in urls:
            filesize = 0
            # Creates the destination path for the asset (ex. batch_x/0/0/0/0/1_dpla_id)
            dest_path = self.downloader.destination_path(self.args.get('output_base'),
                                                         page,
                                                         dpla_id)
            try:
                output, filesize = self.downloader.download(source=url, destination=dest_path)
                if output is None and len(urls) > 1:
                    err_msg = f"Failure in multi-page record {dpla_id}. {page} files were saved but" \
                                "metadata for all images will not be saved in output." \
                                f"  \n - {str(de)}"
                    raise DownloadException(err_msg)
            except Exception as de:
                raise DownloadException(f"Download failed for {url}: {str(de)}") from de
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

    def execute(self):
        """
        """
        # Read data in
        data_in = self.load_data(self.args.get('input_data'), self.args.get('file_filter', None)).head(1)
        # Set the total number of DPLA items to be attempted
        self.downloader._tracker.set_dpla_count(len(data_in))
        # Full path to the output parquet file
        data_out = self.data_out_path(self.args.get('output_base'), self.args.get('partner_name'))

        # Summary of input parameters
        self.log.info(f"Input:           {self.args.get('input_data')}")
        self.log.info(f"Base out:        {self.args.get('output_base')}")
        self.log.info(f"Data out:        {data_out}")
        self.log.info(f"DPLA records:    {self.downloader._tracker.item_cnt}")

        records = data_in.to_dict('records')
        with ThreadPoolExecutor() as executor:
            results = [executor.submit(self.process_rows, chunk) for chunk in records]
        image_rows = [result.result() for result in results]
        self.log.info(f"Downloaded {self.downloader._tracker.image_success_cnt} images ({sizeof_fmt(self.downloader._tracker.get_size())})")

        # TODO dig into a better way to flatten this nested list
        # Flatten data and create a dataframe
        flat = list(chain.from_iterable(image_rows))
        df = pd.DataFrame(flat, columns=self.WRITE_COLUMNS)
        # Write dataframe out
        fs = FileSystem()
        fs.write_parquet(path=data_out, data=df, columns=self.WRITE_COLUMNS)

    def process_rows(self, rows):
        """

        returns: rows: list of dicts for images and metadata associated with a single DPLA record
        """
        iiif = IIIF()
        images = []

        dpla_id = rows.get('id')
        title = rows.get('title')
        wiki_markup = rows.get('wiki_markup')
        manifest = rows.get('iiif')
        media_master = rows.get('media_master')

        # If the IIIF manfiest is defined that parse the manfiest to get the download urls
        # otherwise use the media_master url
        try:
            images = iiif.get_iiif_urls(manifest) if manifest else media_master
        except IIIFException as iffex:
            self.downloader._tracker.image_fail_cnt += 1
            self.log.error(f"Error getting IIIF urls for \n{dpla_id} from {manifest}\n- {str(iffex)}")
            return []
        try:
            self.log.info(f"https://dp.la/item/{dpla_id} has {len(images)} assets")
            images = self.get_images(images, dpla_id)
            # Update images with metadata applicable to all images
            images = self.update_metadata(images, title, wiki_markup)
        except DownloadException as de:
            images = []
            self.downloader._tracker.image_fail_cnt += 1
            self.log.error(f"Failed download(s) for {dpla_id}\n - {str(de)}")
        return images
