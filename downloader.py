"""
Downloads user specified amount of images

Production use case:
    python download.py --limit 1000000000000 --source /path/to/wiki-parquet/ --output /path/to/save/images

"""

import sys

import getopt
import logging
import pandas as pd

from duploader.dupload import Dupload
from duploader.utils import Utils

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")


# Helper classes
duploader = Dupload()
utils = Utils()

# Input parameters
download_limit = 0  # Total number of bytes to download
batch_size = 0  # Size of download batches (in bytes)
input_df = ""  # input of parquet files generated by ingestion3 wiki job
save_location = ""  # output location

# Controlling vars
df_rows = list()  #
total_downloaded = 0  # Running incrementer for tracking total bytes downloaded
batch_downloaded = 0  # Running incrementer for tracking total bytes downloaded in a batch
batch_number = 1  # This will break apart the input parquet file into batches defined by batch_size

columns = {"_1": "id", "_2": "wiki_markup", "_3": "iiif", "_4": "media_master", "_5": "title"}
upload_parquet_columns = ['dpla_id', 'path', 'size', 'title', 'markup']
try:
    opts, args = getopt.getopt(sys.argv[1:], "hi:u:o:", ["limit=", "batch_size=", "input=", "output="])
except getopt.GetoptError:
    print('downloader.py --limit <bytes> --batch_size <bytes> --input <path to parquet> --output <path to save files>')
    sys.exit(2)

for opt, arg in opts:
    if opt == '-h':
        print(
            'downloader.py --limit <bytes> --batch_size <bytes> --input <path to parquet> --output <path to save files>')
        sys.exit()
    elif opt in ("-l", "--limit"):
        download_limit = int(arg)
    elif opt in ("-i", "--input"):
        input_df = arg
    elif opt in ("-o", "--output"):
        save_location = arg.rstrip('/')
    elif opt in ("-b", "--batch_size"):
        batch_size = int(arg)

logging.info(f"Size limit: {utils.sizeof_fmt(download_limit)}")  # 1 TB === 1000000000000, 0 for no limit
logging.info(f"Batch size: {utils.sizeof_fmt(batch_size)}")  # 1 TB === 1000000000000
logging.info(f"Input:      {input_df}")
logging.info(f"Output:     {save_location}")

file_list = utils.get_parquet_files(path=input_df)

for parquet_file in file_list:
    logging.info(f"Processing...{parquet_file}")
    df = utils.get_df(parquet_file, columns=columns)
    for row in df.itertuples(index=['id', 'wiki_markup', 'iiif', 'media_master', 'title']):
        try:
            dpla_id = getattr(row, 'id')
            title = getattr(row, 'title')
            wiki_markup = getattr(row, 'wiki_markup')
            iiif = getattr(row, 'iiif')
            media_master = getattr(row, 'media_master')
        except Exception as e:
            logging.error(f"Unable to get attributes from row {row}: {e}")

        asset_path = f"{save_location}/batch_{batch_number}/assets/{dpla_id[0]}/{dpla_id[1]}/{dpla_id[2]}/{dpla_id[3]}/"
        df_output_path = f"{save_location}/batch_{batch_number}/data/"

        utils.create_path(asset_path)
        utils.create_path(df_output_path)

        # Are we working with iiif or media_master?
        download_urls = utils.get_iiif_urls(iiif) if iiif else media_master

        """
        Get urls to download
        Download images up to 1TB 
        Store them and then upload later 
        
        Generate values required by uploader 
        upload parquet file
            - dpla id
            - Page title 
            - Path to asset
            - size of asset 
            - Wiki markup 
        """

        out, time, size = save_location, 0, 0  # Defaults

        if len(download_urls) > 1:
            # TODO handle multi-asset upload for single item
            # - page title creation
            # - filename
            # - upload asset
            logging.info("Unsupported multi-asset upload")
            for url in download_urls:
                single_file_upload_size = 0
            break

        elif len(download_urls) == 1:
            # Handle single asset upload
            url = download_urls[0]
            # download asset and swallow Exceptions
            try:
                out, time, size = duploader.download_single_item(url=url, save_location=asset_path)
            except Exception as e:
                logging.error(e)
                out = None
                time = 0
                size = 0
                continue

            # Update size
            batch_downloaded = batch_downloaded + size
            total_downloaded = total_downloaded + size

            # create row for "upload" parquet file
            #   - dpla id, its just good to have
            #   - path to asset to upload
            #   - size of asset to upload
            #   - title/wiki page name
            #   - wiki markup

            if out is not None:
                row = {
                    'dpla_id': dpla_id,
                    'path': out,
                    'size': size,
                    'title': title,
                    'markup': wiki_markup
                }

                df_rows.append(row)
        else:
            logging.info("Undefined condition met")

        # logging.info(f"Item {utils.sizeof_fmt(size)} -- Batch total {utils.sizeof_fmt(batch_uploaded)}")

        if batch_downloaded > batch_size:
            logging.info(f"Upload quota met for batch {batch_number}")
            logging.info(f"\n\tBatch {batch_number} \n" \
                             f"\t{len(df_rows)} files \n" \
                             f"\t{utils.sizeof_fmt(batch_downloaded)}")

            # Save upload info dataframe
            batch_parquet_out_path = f"{df_output_path}batch_{batch_number}.parquet"
            utils.write_parquet(batch_parquet_out_path, df_rows, upload_parquet_columns)

            # Reset batch control vars
            df_rows = list()
            batch_number = batch_number + 1
            batch_downloaded = 0
            logging.info(f"Starting batch number {batch_number}")

        # If there is a total limit in place then abort after it has been breached.
        if 0 < download_limit < total_downloaded:
            logging.info(f"Total download limit breached at {utils.sizeof_fmt(total_downloaded)}. Stopping run.")
            logging.info(f"\n\tBatch {batch_number} \n" \
                             f"\t{len(df_rows)} files \n" \
                             f"\t{utils.sizeof_fmt(batch_downloaded)}")

            batch_parquet_out_path = f"{df_output_path}batch_{batch_number}.parquet"
            utils.write_parquet(batch_parquet_out_path, df_rows, upload_parquet_columns)
            break
