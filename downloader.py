"""
Downloads user specified amount of images

Production use case:
    python download.py --limit 1000000000000 --source /path/to/wiki-parquet/ --output /path/to/save/images

    s3://wiki/plainstopeaks/20210120/batch_1/assets/0/0/0/1/00001121215151/1/image.jpeg
    s3://wiki/plainstopeaks/20210120/batch_1/assets/0/0/0/1/00001121215151/2/image.jpeg
    s3://wiki/plainstopeaks/20210120/batch_1/assets/0/0/0/1/00001121215151/3/image.jpeg
    s3://wiki/plainstopeaks/20210120/batch_1/data/
"""
import os
import sys
import time

import getopt
import logging
import traceback
import ssl

ssl.SSLContext.verify_mode = ssl.CERT_OPTIONAL

from wikiutils.utils import Utils

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

# Helper classes
utils = Utils()

# Input parameters
download_limit = 0  # Total number of bytes to download
batch_size = 0  # Size of download batches (in bytes)
input_df = ""  # input of parquet files generated by ingestion3 wiki job
base_output_path = ""  # output location
max_filesize = 104857600  # Default max file size is 100mb

# Controlling vars
df_rows = list()  #
total_downloaded = 0  # Running incrementer for tracking total bytes downloaded
batch_downloaded = 0  # Running incrementer for tracking total bytes downloaded in a batch
batch_number = 1  # This will break apart the input parquet file into batches defined by batch_size

columns = {"_1": "id", "_2": "wiki_markup", "_3": "iiif", "_4": "media_master", "_5": "title"}
upload_parquet_columns = ['dpla_id', 'path', 'size', 'title', 'markup', 'page']
try:
    opts, args = getopt.getopt(sys.argv[1:], "hi:u:o:", ["limit=", "batch_size=", "input=", "output=", "max_filesize="])
except getopt.GetoptError:
    print(
        'downloader.py --limit <bytes> --batch_size <bytes> --input <path to parquet> --output <path to save files> --max_filesize <max filesize>')
    sys.exit(2)

for opt, arg in opts:
    if opt == '-h':
        print(
            'downloader.py --limit <bytes> --batch_size <bytes> --input <path to parquet> --output <path to save '
            'files> --max_filesize <max file size in bytes>')
        sys.exit()
    elif opt in ("-l", "--limit"):
        download_limit = int(arg)
    elif opt in ("-i", "--input"):
        input_df = arg
    elif opt in ("-o", "--output"):
        base_output_path = arg.rstrip('/')
    elif opt in ("-b", "--batch_size"):
        batch_size = int(arg)
    elif opt in ("-m", "--max_filesize"):
        max_filesize = int(arg)

# Setup log config
timestr = time.strftime("%Y%m%d-%H%M%S")
log_dir = "./logs/"
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')

logger = logging.getLogger('logger')

file_handler = logging.FileHandler(f"{log_dir}/download-{timestr}.log")
file_handler.setLevel(logging.INFO)
# create console handler with a higher log level
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.ERROR)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

logger.info(f"Total download limit: {utils.sizeof_fmt(download_limit)}")  # 1 TB === 1000000000000, 0 for no limit
logger.info(f"Batch size: {utils.sizeof_fmt(batch_size)}")  # 1 TB === 1000000000000
logger.info(f"Max file size: {utils.sizeof_fmt(max_filesize)}")
logger.info(f"Input: {input_df}")
logger.info(f"Output: {base_output_path}")

# Get individual parquet files from ingestion3 wiki output
file_list = utils.get_parquet_files(path=input_df)

df_batch_out = f"{base_output_path}/batch_{batch_number}/data/"
dpla_item_count = 1

logger.info(f"{file_list.__sizeof__()} files to process")

for parquet_file in file_list:
    utils.create_path(df_batch_out)
    # read parquet file
    df = utils.get_df(parquet_file, columns=columns)

    logger.info(f"Processing...{df.shape[0]} rows in {parquet_file}")

    for row in df.itertuples(index=['id', 'wiki_markup', 'iiif', 'media_master', 'title']):
        try:
            dpla_id = getattr(row, 'id')
            title = getattr(row, 'title')
            wiki_markup = getattr(row, 'wiki_markup')
            iiif = getattr(row, 'iiif')
            media_master = getattr(row, 'media_master')
        except Exception as e:
            logger.error(f"Unable to get attributes from row {row}: {e}. Aborting...")
            break

        # Are we working with IIIF or media_master?
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
        out, time, size = base_output_path, 0, 0  # Defaults
        asset_count = 1

        # MULTI-ASSET SUPPORT

        rows = list()
        for url in download_urls:
            # Create asset path
            asset_path = f"{base_output_path}/batch_{batch_number}/assets/" \
                f"{dpla_id[0]}/{dpla_id[1]}/{dpla_id[2]}/{dpla_id[3]}/{dpla_id}/{asset_count}_{dpla_id}".strip()

            utils.create_path(asset_path)
            try:
                out, time, size = utils.download(url=url, out=asset_path)
                if size > max_filesize:
                    raise Exception(f"file size {utils.sizeof_fmt(size)} exceeds max file size limit {utils.sizeof_fmt(max_filesize)} for {url}")
            except Exception as e:
                # If a single asset files for a multi-asset upload then all assets are dropped
                logger.error(f"Aborting all assets for {dpla_id}\n\t{e}")
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

            asset_count = asset_count + 1  # increment asset count

        # append all assets/rows for a given metadata record
        if len(rows) > 0:
            df_rows.extend(rows)

        # Only log a message every 100 records
        if dpla_item_count % 100 == 0:
            logger.info(f"{dpla_item_count} records and {len(df_rows)} assets")

        dpla_item_count = dpla_item_count + 1

        if batch_downloaded > batch_size:
            logger.info(f"Upload quota met for batch {batch_number}")
            logger.info(f"\n\tBatch {batch_number} \n" \
                            f"\t{len(df_rows)} files \n" \
                            f"\t{utils.sizeof_fmt(batch_downloaded)}")

            # Save upload info dataframe
            batch_parquet_out_path = f"{df_batch_out}batch_{batch_number}.parquet"
            utils.write_parquet(batch_parquet_out_path, df_rows, upload_parquet_columns)

            # Reset batch control vars, update df_output_path
            df_rows = list()
            batch_number = batch_number + 1
            batch_downloaded = 0
            df_batch_out = f"{base_output_path}/batch_{batch_number}/data/"
            logger.info(f"Starting batch number {batch_number}")

        # If there is a total limit in place then abort after it has been breached.
        if 0 < download_limit < total_downloaded:
            logger.info(f"Total download limit breached at {utils.sizeof_fmt(total_downloaded)}. Stopping run.")
            logger.info(f"\n\tBatch {batch_number} \n" \
                            f"\t{len(df_rows)} files \n" \
                            f"\t{utils.sizeof_fmt(batch_downloaded)}")

            batch_parquet_out_path = f"{df_batch_out}batch_{batch_number}.parquet"
            utils.write_parquet(batch_parquet_out_path, df_rows, upload_parquet_columns)
            df_rows = list()
            logger.info("Exiting...")
            sys.exit()

# If finished processing parquet files without breaching limits then write data out
if df_rows:
    batch_parquet_out_path = f"{df_batch_out}batch_{batch_number}.parquet"
    utils.write_parquet(batch_parquet_out_path, df_rows, upload_parquet_columns)
    df_rows = list()  # reset

logger.info(f"FINISHED download for {input_df}")
