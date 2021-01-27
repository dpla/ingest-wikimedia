"""
Downloads user specified amount of images

Production use case:
    python download.py --limit 1000000000000 --source /path/to/wiki-parquet/ --output /path/to/save/images

    s3://wiki/plainstopeaks/20210120/batch_1/assets/0/0/0/1/00001121215151/1/image.jpeg
    s3://wiki/plainstopeaks/20210120/batch_1/assets/0/0/0/1/00001121215151/2/image.jpeg
    s3://wiki/plainstopeaks/20210120/batch_1/assets/0/0/0/1/00001121215151/3/image.jpeg
    s3://wiki/plainstopeaks/20210120/batch_1/data/
"""

import sys

import getopt
import logging
import traceback
import pandas as pd
import mimetypes

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
upload_parquet_columns = ['dpla_id', 'path', 'size', 'title', 'markup', 'page']
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
df_output_path = f"{save_location}/batch_{batch_number}/data/"
record_count = 1

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
            logging.error(f"Unable to get attributes from row {row}: {e}. Aborting...")
            break

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



        # TODO `out` should be the root save location or the asset path?
        out, time, size = save_location, 0, 0  # Defaults
        asset_count = 1

        # MULTI-ASSET SUPPORT

        rows = list()
        for url in download_urls:
            # Create asset path
            asset_path = f"{save_location}/batch_{batch_number}/assets/{dpla_id[0]}/{dpla_id[1]}/{dpla_id[2]}/{dpla_id[3]}/{dpla_id}/{asset_count}_{dpla_id}"

            utils.create_path(asset_path)
            utils.create_path(df_output_path)

            # The asset path should be {0}/{1}/{2}/{3}/{dpla_id}/{assetcount_dplaId}
            #
            try:
                out, time, size = duploader.download_single_item(url=url, save_location=asset_path)
                # name is ignored
            except Exception as e:
                logging.error(f"Aborting all assets for {dpla_id}\n{e}\n{traceback.format_exc()}")
                rows = list()
                out = None
                time = 0
                size = 0
                break

            # Update size
            batch_downloaded = batch_downloaded + size
            total_downloaded = total_downloaded + size

            # create row for "upload" parquet file
            #   - dpla id, its just good to have
            #   - path to asset to upload
            #   - size of asset to upload
            #   - title/wiki page name
            #   - wiki markup
            #   - page

            if out is not None:
                row = {
                    'dpla_id': dpla_id,
                    'path': out,
                    'size': size,
                    'title': title,
                    'markup': wiki_markup,
                    'page': asset_count
                }
                rows.append(row)
            logging.info(f"Page {asset_count} for {dpla_id}")
            asset_count = asset_count + 1  # increment asset count

        # append all assets/rows for a given metadata record
        logging.info(f"Appending {len(rows)} rows to df")
        df_rows.extend(rows)

        # Only log a message every 100 records
        if(record_count % 100 == 0):
            logging.info(f"{record_count} records and {len(df_rows)} images")

        record_count = record_count + 1

        if batch_downloaded > batch_size:
            logging.info(f"Upload quota met for batch {batch_number}")
            logging.info(f"\n\tBatch {batch_number} \n" \
                             f"\t{len(df_rows)} files \n" \
                             f"\t{utils.sizeof_fmt(batch_downloaded)}")

            # Save upload info dataframe
            batch_parquet_out_path = f"{df_output_path}batch_{batch_number}.parquet"
            utils.write_parquet(batch_parquet_out_path, df_rows, upload_parquet_columns)

            # Reset batch control vars, update df_output_path
            df_rows = list()
            batch_number = batch_number + 1
            batch_downloaded = 0
            df_output_path = f"{save_location}/batch_{batch_number}/data/"
            logging.info(f"Starting batch number {batch_number}")

        # If there is a total limit in place then abort after it has been breached.
        if 0 < download_limit < total_downloaded:
            logging.info(f"Total download limit breached at {utils.sizeof_fmt(total_downloaded)}. Stopping run.")
            logging.info(f"\n\tBatch {batch_number} \n" \
                             f"\t{len(df_rows)} files \n" \
                             f"\t{utils.sizeof_fmt(batch_downloaded)}")

            batch_parquet_out_path = f"{df_output_path}batch_{batch_number}.parquet"
            utils.write_parquet(batch_parquet_out_path, df_rows, upload_parquet_columns)
            df_rows = list()
            logging.info("Exiting...")
            sys.exit()

# If finished processing parquet files without breaching limits then write data out
if df_rows:
    batch_parquet_out_path = f"{df_output_path}batch_{batch_number}.parquet"
    utils.write_parquet(batch_parquet_out_path, df_rows, upload_parquet_columns)
    df_rows = list()  # reset
