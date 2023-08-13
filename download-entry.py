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
from wikiutils.exceptions import DownloadException
from wikiutils.logger import WikimediaLogger
from wikiutils.emailer import SesMailSender, SesDestination, DownloadSummary


if __name__ == "__main__":
    pass

TUPLE_INDEX = ['id', 'wiki_markup', 'iiif', 'media_master', 'title']

# partner_name is the hub abbreviation and is used to create the output path
partner_name = ""
# Download limit is the total number of bytes to download
total_limit = 0  # Total number of bytes to download
# batch_limit is the number of bytes to download in a batch
# Default value is 250 GB
batch_limit = 268435456000
# Input parquet file (the wiki output of ingestion3)
input_data = ""
# Output path is where the assets are saved to
output_base = ""  # output location
# Max file size is the maximum size of a file to download
# Default value is 10GB
max_filesize = 10737418240
# File filter is a file that contains a list of DPLA IDs to download and is used to only 
# upload a specific set of DPLA IDs
file_filter = None

# Controlling vars
batch_rows = list()     #
total_downloaded = 0    # Running incrementer for tracking total bytes downloaded
batch_downloaded = 0    # Running incrementer for tracking total bytes downloaded in a batch
batch_number = 1        # This will break apart the input parquet file into batches defined by batch_limit
dpla_item_count = 1     # Running incrementer for tracking total number of DPLA items downloaded

try:
    opts, args = getopt.getopt(sys.argv[1:],
                               "hi:u:o:", 
                               ["partner=",
                                "limit=", 
                                "batch_limit=", 
                                "input=", 
                                "output=", 
                                "max_filesize=", 
                                "file_filter="])
except getopt.GetoptError:
    print(
        "downloader.py\n" \
        "--partner <dpla partner name>\n" \
        "--limit <bytes>\n" \
        "--batch_limit <bytes>\n" \
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
                "--batch_limit <batch size limit in bytes>\n" \
                "--input <path to wikimedia parquet file>\n" \
                "--output <path to save files>\n" \
                "--max_filesize <max file size in bytes>" \
                "--file_filter <file that specifies DPLA ids to download>"
                )
        sys.exit()
    elif opt in ("-p", "--partner"):
        partner_name = arg
    elif opt in ("-l", "--limit"):
        total_limit = int(arg)
    elif opt in ("-i", "--input"):
        input_data = arg
    elif opt in ("-o", "--output"):
        output_base = arg.rstrip('/')
    elif opt in ("-b", "--batch_limit"):
        batch_limit = int(arg)
    elif opt in ("-m", "--max_filesize"):
        max_filesize = int(arg)
    elif opt in ("-f", "--file_filter"):
        file_filter = arg

# Helper classes
# These need to remain below the input parameters
utils = WikimediaUtils()
log = WikimediaLogger(partner_name=partner_name, event_type="download")
downloader = WikimediaDownloader(logger=log)

# Summary of input parameters
log.info(f"Total download limit: {utils.sizeof_fmt(total_limit)}")
log.info(f"Batch size: {utils.sizeof_fmt(batch_limit)}")
log.info(f"Max file size: {utils.sizeof_fmt(max_filesize)}")
log.info(f"Input: {input_data}")
log.info(f"Output: {output_base}")


# If using file filter then read in the file and create a list of DPLA IDs
ids = []
if file_filter:
    log.info("Using filter: %s", file_filter)
    with open(file_filter, encoding='utf-8') as f:
        ids = [line.rstrip() for line in f]
    log.info("Attempting %s DPLA records", len(ids))

data_in = utils.read_parquet(input_data)

for row in data_in.itertuples(index=TUPLE_INDEX):
    try:
        dpla_id = getattr(row, 'id')
        title = getattr(row, 'title')
        wiki_markup = getattr(row, 'wiki_markup')
        iiif = getattr(row, 'iiif')
        media_master = getattr(row, 'media_master')
    except AttributeError as ae:
        log.error("Unable to get all attributes from row %s: %s", row, str(ae))
        break

    # If a file_filter paramter is specified then only download files that match the DPLA IDs in 
    # the file
    # TODO Rewrite this as a prefilter of the parquet files
    if file_filter and (dpla_id not in ids):
        continue

    # Are we working with IIIF or media_master?
    download_urls = utils.get_iiif_urls(iiif) if iiif else media_master
    log.info(f"https://dp.la/item/{dpla_id} has {len(download_urls)} assets")
    
    page = 1
    images = []
    for url in download_urls:
        filesize = 0 
        # Creates the destination path for the asset (ex. batch_x/0/0/0/0/1_dpla_id)
        destination_path = downloader.destination_path(output_base, batch_number, page, dpla_id) 
        # If the destination path is not an S3 path then create the parent dirs on the local file system
        try:
            output, filesize = downloader.download(source=url, destination=destination_path)
            if output is None:
                raise DownloadException(f"Download failed for {url}")
        except DownloadException as de:
            # If a single asset fails for a multi-asset upload then all assets are dropped
            log.error("Aborting all assets for %s \n- %s", dpla_id, str(de))
            images = []
            break

        # Create a row for a single asset and if multiple assests exist them append them to the rows list
        # When a single asset fails to download then this object is destroyed by the `break`` above and 
        # the rows for already downloaded assets are not added to the final dataframe
        row = {
            'dpla_id': dpla_id,
            'path': output,
            'size': filesize,
            'title': title,
            'markup': wiki_markup,
            'page': page
        }
        images.append(row)

        page += 1  # increment asset count
        batch_downloaded += filesize # track the cumluative size of this batch
        total_downloaded += filesize # track the cumluative size of the total download

    # append all assets/rows for a given metadata record
    if len(images) > 0:
        batch_rows.extend(images)

    dpla_item_count += 1

    # If the batch exceeds the batch limit size then write out the dataframe and reset the metrics
    if batch_downloaded > batch_limit:
        downloader.save(base=output_base, batch=batch_number, rows=batch_rows)
        # Reset batch control vars
        batch_rows = []
        batch_number += 1
        batch_downloaded = 0
    elif 0 < total_limit < total_downloaded:
        log.info("Total download limit reached")
        downloader.save(base=output_base, batch=batch_number, rows=batch_rows)
        break

# If finished processing parquet files without breaching limits then write data out
if batch_rows:
    downloader.save(base=output_base, batch=batch_number, rows=batch_rows)

# Save the log file to S3
bucket, key = utils.get_bucket_key(output_base)
public_url = log.write_log_s3(bucket=bucket, key=key)
log.info(f"Log file saved to {public_url}")
log.info(f"Total download size: {utils.sizeof_fmt(total_downloaded)}")
log.info("Fin.")

import boto3
client = boto3.client('ses')
emailer = SesMailSender(client)
summary = DownloadSummary()

emailer.send_email(source="tech@dp.la",
                   destination=SesDestination(tos=["scott@dp.la"]), 
                   subject=summary.subject(partner_name=partner_name),
                   text=summary.body_text(log_url=public_url),
                   html=summary.body_html(log_url=public_url, total_download=utils.sizeof_fmt(total_downloaded)),
                   reply_tos=["tech@dp.la"])
