"""
Downloads images from a DPLA partner to be uploaded to Wikimedia Commons.
    - Downloads images in batches
    - Writes out a parquet file with metadata for each image
    - Uploads parquet file to S3

    python download.py 
        --partner nwdh \
        --limit 1000000000000 \
        --source s3://dpla-master-dataset/nwdh/wiki/[]/ \
        --output s3://dpla-wikimeida/nwdh/[date]

    s3://wiki/plainstopeaks/20210120/batch_1/assets/0/0/0/1/00001121215151/1/image.jpeg
    s3://wiki/plainstopeaks/20210120/batch_1/assets/0/0/0/1/00001121215151/2/image.jpeg
    s3://wiki/plainstopeaks/20210120/batch_1/assets/0/0/0/1/00001121215151/3/image.jpeg
    s3://wiki/plainstopeaks/20210120/batch_1/data/
"""
import sys
import getopt
from wikiutils.utils import Utils as WikimediaUtils
from wikiutils.downloader import Downloader as WikimediaDownloader
from wikiutils.logger import WikimediaLogger

if __name__ == "__main__":
    pass

# Input parameters
# partner_name is the hub abbreviation and is used to create the output path
partner_name = ""
# Download limit is the total number of bytes to download
download_limit = 0  # Total number of bytes to download
# batch_size is the number of bytes to download in a batch
# Default value is 250 GB
batch_size = 268435456000 
# Input parquet file (the wiki output of ingestion3)
input_df = ""  
# Output path is where the assets are saved to
base_output_path = ""  # output location
# Max file size is the maximum size of a file to download
# Default value is 10GB
max_filesize = 10737418240
# File filter is a file that contains a list of DPLA IDs to download and is used to only 
# upload a specific set of DPLA IDs
file_filter = ""

# Controlling vars
df_rows = list()  #
total_downloaded = 0  # Running incrementer for tracking total bytes downloaded
batch_downloaded = 0  # Running incrementer for tracking total bytes downloaded in a batch
batch_number = 1  # This will break apart the input parquet file into batches defined by batch_size
dpla_item_count = 1  # Running incrementer for tracking total number of DPLA items downloaded

# Index and column names for the input parquet file
columns = {"_1": "id", 
           "_2": "wiki_markup", 
           "_3": "iiif", 
           "_4": "media_master", 
           "_5": "title"}

# Column names for the output parquet file
upload_parquet_columns = ['dpla_id', 'path', 'size', 'title', 'markup', 'page']

try:
    opts, args = getopt.getopt(sys.argv[1:],
                               "hi:u:o:", 
                               ["partner=",
                                "limit=", 
                                "batch_size=", 
                                "input=", 
                                "output=", 
                                "max_filesize=", 
                                "file_filter="])
except getopt.GetoptError:
    print(
        "downloader.py\n" \
        "--partner <dpla partner name>\n" \
        "--limit <bytes>\n" \
        "--batch_size <bytes>\n" \
        "--input <path to parquet>\n" \
        "--output <path to save files>\n" \
        "--max_filesize <max filesize>\n" \
        "--file_filter <ids>" \
        )
    sys.exit(2)

for opt, arg in opts:
    if opt == '-h':
        print(
            "downloader.py\n" \
                "--partner <dpla partner name>\n" \
                "--limit <total limit in bytes>\n" \
                "--batch_size <batch size limit in bytes>\n" \
                "--input <path to wikimedia parquet file>\n" \
                "--output <path to save files>\n" \
                "--max_filesize <max file size in bytes>" \
                "--file_filter <file that specifies DPLA ids to download>"
                )
        sys.exit()
    elif opt in ("-p", "--partner"):
        partner_name = arg
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
    elif opt in ("-f", "--file_filter"):
        file_filter = arg

# Helper classes
utils = WikimediaUtils()
logger = WikimediaLogger(partner_name=partner_name, event_type="download")
downloader = WikimediaDownloader(logger=logger)

# Summary of input parameters
logger.log_info(f"Total download limit: {utils.sizeof_fmt(download_limit)}")
logger.log_info(f"Batch size: {utils.sizeof_fmt(batch_size)}")
logger.log_info(f"Max file size: {utils.sizeof_fmt(max_filesize)}")
logger.log_info(f"Input: {input_df}")
logger.log_info(f"Output: {base_output_path}")

# If using file filter then read in the file and create a list of DPLA IDs
ids = list()
if file_filter:
    logger.log_info("Using filter: %s" % file_filter)
    with open(file_filter, encoding='utf-8') as f:
        ids = [line.rstrip() for line in f]
    logger.log_info("Attempting %s DPLA records" % len(ids))

# Get individual parquet files from ingestion3 wiki output
file_list = utils.get_parquet_files(path=input_df)

# Path to save the dataframe which holds all the metadata for the batch of downloaded files
df_batch_out = f"{base_output_path}/batch_{batch_number}/data/"

# TODO rewrite this to use a generator
for parquet_file in file_list:
    utils.create_path(df_batch_out)
    df = utils.get_df(parquet_file, columns=columns)

    # TODO rewrite this to use a generator
    for row in df.itertuples(index=['id', 'wiki_markup', 'iiif', 'media_master', 'title']):
        try:
            dpla_id = getattr(row, 'id')
            title = getattr(row, 'title')
            wiki_markup = getattr(row, 'wiki_markup')
            iiif = getattr(row, 'iiif')
            media_master = getattr(row, 'media_master')
        except Exception as e:
            logger.log_error(f"Unable to get attributes from row {row}: {e}. Aborting...")
            break

        # If a file_filter paramter is specified then only download files that match the DPLA IDs in 
        # the file
        # TODO Rewrite this as a prefilter of the parquet files
        if file_filter and (dpla_id not in ids):
            continue

        # Are we working with IIIF or media_master?
        download_urls = utils.get_iiif_urls(iiif) if iiif else media_master

        # TODO `out` should be the root save location or the asset path?
        out, time, size = base_output_path, 0, 0  # Defaults
        asset_count = 1

        # MULTI-ASSET SUPPORT
        rows = list()
        logger.log_info(f"https://dp.la/item/{dpla_id} has {len(download_urls)} assets")
        for url in download_urls:
            # Creates the destination path for the asset
            destination_path = utils.create_destination_path(base_output_path, batch_number, asset_count, dpla_id) 
            # If the destination path is not an S3 path then create the path on the local file system
            if not base_output_path.startswith("s3://"):
                utils.create_path(destination_path.lstrip('/'))
            
            try:
                out, size = downloader.download(source=url, destination=destination_path)
            except Exception as e:
                # If a single asset fails for a multi-asset upload then all assets are dropped
                logger.log_error(f"Aborting all assets for {dpla_id}\n- {str(e)}")
                rows = list()
                out = None
                time, size = 0, 0
                break
            
            if out is None: 
                logger.log_info(f"Asset {url} failed to download. Skipping...")
                break

            row = {
                'dpla_id': dpla_id,
                'path': out,
                'size': size,
                'title': title,
                'markup': wiki_markup,
                'page': asset_count
            }
            rows.append(row)

            asset_count =+ 1  # increment asset count
            batch_downloaded =+ size # track the cumluative size of this batch
            total_downloaded =+ size # track the cumluative size of the total download

        # append all assets/rows for a given metadata record
        if len(rows) > 0:
            df_rows.extend(rows)

        dpla_item_count =+ 1

        if batch_downloaded > batch_size:
            logger.log_info(f"Download quota met for batch {batch_number}")
            logger.log_info(f"- {len(df_rows)} files")
            logger.log_info(f"- {utils.sizeof_fmt(batch_downloaded)} size")

            # Save the dataframe that contains all the metadata for the batch of downloaded files
            batch_parquet_out_path = f"{df_batch_out}batch_{batch_number}.parquet"
            utils.write_parquet(batch_parquet_out_path, df_rows, upload_parquet_columns)

            # Reset batch control vars, update df_output_path
            df_rows = list()
            batch_number =+ 1
            df_batch_out = f"{base_output_path}/batch_{batch_number}/data/"
            batch_downloaded = 0

        # If there is a total limit in place then abort after it has been breached.
        if 0 < download_limit < total_downloaded:
            logger.log_info(f"Total download limit reacbed {utils.sizeof_fmt(total_downloaded)}")
            batch_parquet_out_path = f"{df_batch_out}batch_{batch_number}.parquet"
            utils.write_parquet(batch_parquet_out_path, df_rows, upload_parquet_columns)
            df_rows = list()

# If finished processing parquet files without breaching limits then write data out
if df_rows:
    batch_parquet_out_path = f"{df_batch_out}batch_{batch_number}.parquet"
    utils.write_parquet(batch_parquet_out_path, df_rows, upload_parquet_columns)

# TODO Fix this and have it write out for all records rather than just the last one.
# write a summary of the images downloaded
# input_df = pd.DataFrame({   'dpla_id': [dpla_id],
#                             'title': [title],
#                             'wiki_markup': [wiki_markup],
#                             'iiif': [iiif],
#                             'media_master': [media_master],
#                             'downloaded': [out],
#                             'download_time': [time],
#                             'download_size': [size]
#                         })
# input_df.to_parquet(f"{base_output_path}/input.parquet")

logger.log_info(f"Total download size: {utils.sizeof_fmt(total_downloaded)}")
logger.log_info("Finished.")
# Save the log file to S3
bucket, key = utils.get_bucket_key(input)
logger.write_log_s3(bucket=bucket, key=key)
