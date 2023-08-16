
"""
Downloads Wikimedia eligible images from a DPLA partner

"""

from utilities.iiif import IIIF
from executors.downloader import Downloader
from utilities.fs import FileSystem, get_datetime_prefix
from utilities.exceptions import DownloadException, IIIFException
from utilities.format import sizeof_fmt
from utilities.arguements import get_download_args

import logging

import pandas as pd
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


    def __init__(self, args, log=None):
        self.args = args

        self.downloader = Downloader(logger=logging.getLogger('wikimedia_logger'))

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

        return fs.read_parquet(data_in, cols=self.READ_COLUMNS).head(40)

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
            destination_path = self.downloader.destination_path(self.args.get('output_base'),
                                                                page,
                                                                dpla_id)
            try:
                output, filesize = self.downloader.download(source=url, destination=destination_path)
                if output is None and len(urls) > 1:
                    self.log.error(f"Failure in multi-page record {dpla_id}. {page} files were saved but" \
                                "metadata for all images will not be saved in output." \
                                f"  \n - {str(de)}")
                    raise DownloadException(f"Download failed for {url}")
            except Exception as de:
                raise DownloadException(f"Download failed for {url}") from de
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
        # For IIIF and FileSystem stuff
        iiif = IIIF()
        fs = FileSystem()
        image_rows = list()

        # read data in
        data_in = self.load_data(self.args.get('input_data'), self.args.get('file_filter', None))
        # Set the total number of DPLA items to be attempted
        self.downloader._tracker.set_dpla_count(len(data_in))
        # Full path to the output parquet file
        data_out = self.data_out_path(self.args.get('output_base'), self.args.get('partner_name'))

        # Summary of input parameters
        self.log.info(f"Input:           {self.args.get('input_data')}")
        self.log.info(f"Base out:        {self.args.get('output_base')}")
        self.log.info(f"Data out:        {data_out}")
        self.log.info(f"DPLA records:    {self.downloader._tracker.dpla_count}")

        # TODO thread around this around processing rows and not images within a row
        # FIXME hardcoded for testing with 20 images
        from concurrent.futures import ThreadPoolExecutor
        import os

        processors = os.cpu_count()

        df = data_in.to_dict('records')
        batches = [df[i:i+processors] for i in range(0,len(df),processors)]
        with ThreadPoolExecutor() as executor:
            results = [executor.submit(self.process_rows, chunk) for chunk in df]

        df_processed = [result.result() for result in results]

        ## SEE BELOW

        ###

        self.log.info(f"Downloaded {self.downloader._tracker.success_count} images ({sizeof_fmt(self.downloader._tracker.get_size())})")

        # Write data out
        fs = FileSystem()
        fs.write_parquet(path=data_out, data=image_rows, columns=self.WRITE_COLUMNS)

    def process_rows(self, rows):
        iiif = IIIF()

        urls = []
        # print(rows)
        dpla_id = rows.get('id')
        title = rows.get('title')
        wiki_markup = rows.get('wiki_markup')
        manifest = rows.get('iiif')
        media_master = rows.get('media_master')
        # If the IIIF manfiest is defined that parse the manfiest to get the download urls
        # otherwise use the media_master url
        try:
            urls = iiif.get_iiif_urls(manifest) if manifest else media_master
        except IIIFException as iffex:
            self.log.error(f"Error getting IIIF urls for \n{dpla_id} from {manifest}\n- {str(iffex)}")
            return images

        # TODO this will raise an exception if downloads fail. We should catch that and continue
        try:
            self.log.info(f"https://dp.la/item/{dpla_id} has {len(urls)} assets")
            images = self.get_images(urls, dpla_id)
            # update images with metadata applicable to all images
            images = self.update_metadata(images, title, wiki_markup)
        except DownloadException as de:
            raise DownloadException(f"Failed download(s) for {dpla_id}\n - {str(de)}")

        # Add all assets/rows for a given metadata record. len(images) check is necessary because
        # we'd get [a,b,[]] extending with an empty list
        if len(images) > 0:
            return images

        return images





###############
################

        # If the total limit is set and we've exceeded it then stop processing
        # if 0 < self.args.get('total_limit', 0) < self.downloader._tracker.get_size():
        #     return None





        # print(result)

        # tuples = data_in.itertuples(index=False)

        # start = time.time()
        # number_of_chunks = 4
        # chunk_size = 5
        # executor = ThreadPoolExecutor(max_workers=number_of_chunks)
        # futures = []

        # for i in range(number_of_chunks):
        #     chunk = tuples[i*chunk_size:(i+1)*chunk_size]
        #     futures.append(executor.submit(self.process_rows, chunk))

        # for future in concurrent.futures.as_completed(futures):
        #     pass
        # print('Time:', time.time() - start)





        # for row in data_in.itertuples(index=False):
            # dpla_id = row.id
            # title = row.title
            # wiki_markup = row.wiki_markup
            # manifest = row.iiif
            # media_master = row.media_master

            # If the IIIF manfiest is defined that parse the manfiest to get the download urls
            # otherwise use the media_master url
            # try:
            #     urls = iiif.get_iiif_urls(manifest) if manifest else media_master
            #     self.log.info(f"https://dp.la/item/{dpla_id} has {len(urls)} assets")
            # except IIIFException as iffex:
            #     self.log.error(f"Unable to get IIIF urls: {dpla_id} from {manifest}\n- {str(iffex)}")
            #     continue

            # # TODO this will raise an exception if downloads fail. We should catch that and continue
            # try:
            #     images = self.get_images(urls, dpla_id)
            #     # update images with metadata applicable to all images
            #     images = self.update_metadata(images, title, wiki_markup)
            # except DownloadException as de:
            #     self.log.error(f"Failed download(s) for {dpla_id}\n - {str(de)}")
            #     continue

            # # Add all assets/rows for a given metadata record. len(images) check is necessary because
            # # we'd get [a,b,[]] extending with an empty list
            # if len(images) > 0:
            #     image_rows.extend(images)

            # # If the total limit is set and we've exceeded it then stop processing
            # if 0 < self.args.get('total_limit', 0) < self.downloader._tracker.get_size():
            #     break