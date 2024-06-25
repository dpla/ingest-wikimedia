"""
Downloads Wikimedia eligible images from a DPLA partner

"""

import logging
from concurrent.futures import ThreadPoolExecutor
from itertools import chain
from pathlib import Path

import pandas as pd
from wikimedia.entries.entry import Entry
from wikimedia.executors.downloader import Downloader
from wikimedia.utilities.exceptions import DownloadException, IIIFException
from wikimedia.utilities.helpers import S3Helper, Text, InputHelper
from wikimedia.utilities.iiif import IIIF
from wikimedia.utilities.tracker import Result, Tracker


class DownloadEntry(Entry):
    """
    Downloads Wikimedia eligible images from a DPLA partner
    """

    OUTPUT_BASE = None
    # Columns names emitted by the ingestion3 process
    READ_COLUMNS = {
        "_1": "id",
        "_2": "wiki_markup",
        "_3": "iiif",
        "_4": "media_master",
        "_5": "title",
    }
    # Column names for the output parquet file
    WRITE_COLUMNS = ["dpla_id", "path", "size", "title", "markup", "page"]

    log = logging.getLogger(__name__)
    downloader = None
    tracker = None

    def __init__(self, tracker: Tracker):
        self.downloader = Downloader()
        self.tracker = tracker

    def execute(self, **kwargs):
        """ """
        s3_helper = S3Helper()

        partner = kwargs.get("partner", None)
        filter = kwargs.get("file_filter", None)

        # Output and Input are the top level paths and should contain a directory
        # for each partner
        output = kwargs.get("output", None)
        input = kwargs.get("input", None)
        # FIXME this is a kludge to pass OUTPUT_BASE var to get_images() which is
        # called during paralleization
        self.OUTPUT_BASE = f"{output}/{partner}"
        input_base = InputHelper.download_input(base=input, partner=partner)
        # Get the most recent parquet file from the input path
        bucket, key = s3_helper.get_bucket_key(input_base)
        recent_key = s3_helper.most_recent(bucket=bucket, key=key, type="prefix")
        input_recent = f"s3://{bucket}/{recent_key}"

        # Read in most recent parquet file
        df = Entry.load_data(
            data_in=input_recent, columns=self.READ_COLUMNS, file_filter=filter
        ).rename(columns=self.READ_COLUMNS)
        # data_out is the full path to the output parquet file
        data_out = InputHelper.download_output(base=output, partner=partner)
        # Set the total number of DPLA items to be attempted
        self.tracker.set_dpla_count(len(df))
        # Summary of input parameters
        self.log.info(f"Input............{input_recent}")
        self.log.info(f"Output...........{data_out}")
        self.log.info(f"DPLA records.....{self.tracker.item_cnt}")

        records = df.to_dict("records")
        with ThreadPoolExecutor() as executor:
            results = [executor.submit(self.process_rows, chunk) for chunk in records]
        image_rows = [result.result() for result in results]

        self.log.info(
            f"Downloaded {self.tracker.image_success_cnt} images"
            + f" ({Text.sizeof_fmt(self.tracker.get_size())})"
        )

        # TODO dig into a better way to flatten this nested list
        # Flatten data and create a dataframe
        flat = list(chain.from_iterable(image_rows))
        df = pd.DataFrame(flat, columns=self.WRITE_COLUMNS)
        # Write dataframe out to parquet
        pd.DataFrame(df, columns=self.WRITE_COLUMNS).to_parquet(
            data_out, compression="snappy"
        )

    def process_rows(self, rows):
        """

        Return list of dicts for images and metadata associated with a
        xsingle DPLA record

        :param rows: dict of DPLA record
        returns: rows:
        """
        iiif = IIIF()
        images = []

        dpla_id = rows.get("id", None)
        title = rows.get("title", None)
        wiki_markup = rows.get("wiki_markup", None)
        manifest = rows.get("iiif", None)
        media_master = rows.get("media_master", None)

        # If the IIIF manfiest is defined that parse the manfiest to get the
        # download urls otherwise use the media_master url
        try:
            images = iiif.get_iiif_urls(manifest) if manifest else media_master
        except IIIFException as iffex:
            self.tracker.increment(Result.FAILED)
            self.log.error(f"No image urls {dpla_id} -- {manifest} -- {str(iffex)}")
            return []
        try:
            self.log.info(f"https://dp.la/item/{dpla_id} has {len(images)} assets")
            # FIXME get_images expect base_path, kludged in with class var BASE_OUT
            images = self.get_images(images, dpla_id)
            # Update images with metadata applicable to all images
            images = self.update_metadata(images, title, wiki_markup)
        except DownloadException as de:
            images = []
            self.tracker.increment(Result.FAILED)
            self.log.error(f"Failed download(s) for {dpla_id}\n - {str(de)}")
        return images

    def get_images(self, urls, dpla_id):
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
                output, filesize = self.downloader.download(
                    source=url, destination=image_path
                )
                if output is None and len(urls) > 1:
                    err_msg = f"Multi-page record, page {page}"
                    raise DownloadException(err_msg)
            except Exception as de:
                raise DownloadException(f"{url}: {str(de)}") from de

            # Create a row for a single asset and if multiple assests exist them
            # append them to the rows list. When a single asset fails to download
            # then this object is destroyed by the `break` above and the rows for
            # already downloaded assets are not added to the final dataframe
            image_row = {
                "dpla_id": dpla_id,
                "path": output,
                "size": filesize,
                "page": page,
            }
            image_rows.append(image_row)
            page += 1
        return image_rows

    def image_path(self, count, dpla_id):
        """
        Create destination path to download file to
        """
        path = f"{self.OUTPUT_BASE}/images/{dpla_id[0]}/{dpla_id[1]}/{dpla_id[2]}/{dpla_id[3]}/{dpla_id}/{count}_{dpla_id}".strip()  # noqa: E501

        if self.OUTPUT_BASE.startswith("s3://"):
            return path
        Path.mkdir(Path(path), parents=True, exist_ok=True)
        return path

    def update_metadata(self, images, title, wiki_markup):
        """
        Update the metadata for a list of images
        """
        update = list()
        for image in images:
            image.update({"title": title, "markup": wiki_markup})
            update.append(image)
        return update
