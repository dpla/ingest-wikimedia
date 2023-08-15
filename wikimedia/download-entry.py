"""
Downloads images from a DPLA partner to be uploaded to Wikimedia Commons.
    - Downloads images in batches
    - Writes out a parquet file with metadata for each image
    - Uploads parquet file to S3
"""
import sys
import getopt
import boto3

from wikimedia.utilities.iiif import IIIF
from wikimedia.executors.downloader import Downloader
from wikimedia.utilities.fs import FileSystem, S3Helper
from wikimedia.utilities.exceptions import DownloadException, IIIFException
from wikimedia.utilities.logger import WikimediaLogger
from wikimedia.utilities.emailer import SesMailSender, SesDestination, DownloadSummary
from wikimedia.utilities.format import sizeof_fmt

if __name__ == "__main__":
    pass

# These are the columns emitted by the ingestion3 process
READ_COLUMNS = { "_1": "id",
                "_2": "wiki_markup",
                "_3": "iiif",
                "_4": "media_master",
                "_5": "title"}

TUPLE_INDEX = list(READ_COLUMNS.values())

WRITE_COLUMNS = ['dpla_id',
                 'path',
                 'size',
                 'title',
                 'markup',
                 'page']

# partner_name is the hub abbreviation and is used to create the output path
partner_name = ""

# Input parquet file (the wiki output of ingestion3)
input_data = ""
# Output path is where the assets are saved to
output_base = ""  # output location

# TODO this is also a little bit tied to total_limit where is only applies to NARA or SI where
# we don't want to download the entire dataset and have a target set of records.

# File filter is a file that contains a list of DPLA IDs to download and is used to only
# upload a specific set of DPLA IDs
file_filter = None

# TODO This will still generally be a flag but the most common use will be fore
# NARA so we don't download 15TB on a single run. Documentation should reflect this.


total_limit = 0  # the total number of bytes to download, 0 is no limit

# TODO This should be removed on a future PR. We are moving
# entirely away from batching.
batch_limit = 268435456000 # batch size

# TODO this is no longer required and should be removed entirely. Will also remove a if/else check below.
max_filesize = 10737418240 # Default value is 10GB

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
# utils = WikimediaUtils()
iiif = IIIF()
fs = FileSystem()
s3 = S3Helper()
log = WikimediaLogger(partner_name=partner_name, event_type="download")
downloader = Downloader(logger=log)

# Summary of input parameters
log.info(f"Input: {input_data}")
log.info(f"Output: {output_base}")

# If using file filter then read in the file and create a list of DPLA IDs
ids = []
if file_filter:
    log.info("Using filter: %s", file_filter)
    with open(file_filter, encoding='utf-8') as f:
        ids = [line.rstrip() for line in f]
    log.info("Attempting %s DPLA records", len(ids))

data_in = fs.read_parquet(input_data, columns=READ_COLUMNS)

for row in data_in.itertuples(index=TUPLE_INDEX):
    batch_out = downloader.batch_parquet_path(base=output_base, n=batch_number)

    try:
        dpla_id = getattr(row, 'id')
        title = getattr(row, 'title')
        wiki_markup = getattr(row, 'wiki_markup')
        iiif_manifest = getattr(row, 'iiif')
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
    try:
        download_urls = iiif.get_iiif_urls(iiif_manifest) if iiif else media_master
        log.info(f"https://dp.la/item/{dpla_id} has {len(download_urls)} assets")
    except IIIFException as iffex:
        log.error("Unable to get IIIF urls for %s: %s", dpla_id, str(iffex))
        continue

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
    # TODO this will get removed on migration away from batch.
    if batch_downloaded > batch_limit:
        fs.write_parquet(path=batch_out, data=batch_rows, columns=WRITE_COLUMNS)
        # Reset batch control vars
        batch_rows = []
        batch_number += 1
        batch_downloaded = 0
    elif 0 < total_limit < total_downloaded:
        log.info("Total download limit reached")
        fs.write_parquet(path=batch_out, data=batch_rows, columns=WRITE_COLUMNS)
        break

# If finished processing parquet files without breaching limits then write data out
if batch_rows:
    # downloader.save(base=output_base, batch=batch_number, rows=batch_rows)
    fs.write_parquet(path=batch_out, data=batch_rows, columns=WRITE_COLUMNS)

# Save the log file to S3
bucket, key = s3.get_bucket_key(output_base)
public_url = log.write_log_s3(bucket=bucket, key=key)
log.info(f"Log file saved to {public_url}")
log.info(f"Total download size: {sizeof_fmt(total_downloaded)}")
log.info("Fin")

# Send email notification
ses_client = boto3.client('ses', region_name='us-east-1')
emailer = SesMailSender(ses_client)
summary = DownloadSummary(partner=partner_name,
                          log_url=public_url,
                          status=downloader.get_status())

emailer.send_email(source="tech@dp.la",
                   destination=SesDestination(tos=["scott@dp.la"]),  # FIXME dominic@dp.la should be here. Who else?
                   subject=summary.subject(),
                   text=summary.body_text(),
                   html=summary.body_html(),
                   reply_tos=["tech@dp.la"])
