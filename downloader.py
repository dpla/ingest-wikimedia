"""
Downloads user specified amount of images

Production use case:
    python download.py --limit 1000000000000 --source /path/to/wiki-parquet/ --output /path/to/save/images

"""

#
# |https://ark.digitalcommonwealth.org/ark:/50959/9s16d0970/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/6w924h417/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/q524k8304/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/7d2791485/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/h989vt29f/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/z890sp16f/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/5h73w2962/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/ng451p176/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/rn301940c/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/9g54xx71j/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/pc28bm61x/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/4455d475k/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/kh04nh08r/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/st74dk86w/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/0r96h2963/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/x346gv416/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/5h73sc04p/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/n009xc00t/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/c247kb43p/manifest|
# |https://ark.digitalcommonwealth.org/ark:/50959/8s45r279b/manifest|
# +-----------------------------------------------------------------+
import json
import requests
from pathlib import Path

from duploader.dupload import Dupload
import awswrangler as wr
import getopt
import sys
import pandas as pd

import logging

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")


def download(url, out):
    """
    Download file

    :param url:     URL of asset to download
    :return:        Path to downloaded file
    :return:        Time to download file
    :return:        Size of downloaded file (in bytes)
    """
    return duploader.download_single_item(url=url, save_location=out)


# FIXME This is duplicating the above functions and consolidated into a single method
def dupload(dpla_id, title, wiki_markup, download_urls):
    # FIXME hard code taking the first element for NARA, this works for only one image but won't for (page N)
    # Needs explode() on media_master and then passing page number value to create_wiki_page_title()
    if len(download_urls) != 1:
        logging.info("Got more than one URL. Unable to take action. Functionality TBI")  # FIXME
        return 0  # return 0 to indicate no upload

    # logging.info(f"{download_urls[0]}")
    # Download file. Record local destination, time to download and file size
    try:
        # duploader.duownload() will raise an exception if file cannot be downloaded
        file_out, time, size = duploader.download_single_item(url=download_urls[0],
                                                              save_location=save_location)

        # Create Wikimedia page title
        page_title = duploader.create_wiki_page_title(title=title,
                                                      dpla_identifier=dpla_id,
                                                      suffix=file_out[-4:]  # TODO unpack why trimt suffix
                                                      )

        # Create wiki page
        wiki_page = duploader.create_wiki_file_page(title=page_title)
        # Upload to wiki page
        duploader.upload(wiki_file_page=wiki_page,
                         dpla_identifier=dpla_id,
                         text=wiki_markup,
                         file=file_out)
        logging.info(f"Uploaded {file_out}")
        # Return file size to add to accumulator
        return size
    except Exception as e:
        logging.error(f"failed {e}")
        return 0


def get_iiif_urls(iiif):
    request = requests.get(iiif)
    data = request.content
    jsonData = json.loads(data)
    # More than one sequence, return empty list and log some kind of message
    sequences = jsonData['sequences']
    if len(sequences) > 1:
        return list()
    elif len(sequences) == 1:
        print(f"Got IIIF at {iiif}")
        print(f"{sequences[0]}")
        return list()  # ['canvases']['images']['resource']['@id']


def get_df_s3(path):
    return wr.s3 \
        .read_parquet(path=path, dataset=True) \
        .rename(columns={"_1": "id", "_2": "wiki_markup", "_3": "iiif", "_4": "media_master", "_5": "title"})


def get_df_local(path):
    return pd.read_parquet(path, engine='fastparquet') \
        .rename(columns={"_1": "id", "_2": "wiki_markup", "_3": "iiif", "_4": "media_master", "_5": "title"})


def get_df(path):
    return get_df_s3(path) if input_wiki_parquet_file.startswith("s3") else get_df_local(path)


def create_path(path):
    if not Path(path).exists():
        Path(path).mkdir(parents=True)


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


upload_limit = 0
batch_size = 0
input_wiki_parquet_file = ""
save_location = ""

duploader = Dupload()

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
        upload_limit = int(arg)
    elif opt in ("-i", "--input"):
        input_wiki_parquet_file = arg
    elif opt in ("-o", "--output"):
        save_location = arg.rstrip('/')
    elif opt in ("-b", "--batch_size"):
        batch_size = int(arg)

logging.info(f"Size limit: {sizeof_fmt(upload_limit)}")  # 1 TB === 1000000000000, 0 for no limit
logging.info(f"Batch size: {sizeof_fmt(batch_size)}")  # 1 TB === 1000000000000
logging.info(f"Input:      {input_wiki_parquet_file}")
logging.info(f"Output:     {save_location}")

df = get_df(input_wiki_parquet_file)

upload_rows = list()
total_uploaded = 0
batch_uploaded = 0
batch_number = 1  # This will break apart the input parquet file into batches defined by batch_size

for row in df.itertuples(index=['id', 'wiki_markup', 'iiif', 'media_master', 'title']):
    dpla_id = getattr(row, 'id')
    title = getattr(row, 'title')
    wiki_markup = getattr(row, 'wiki_markup')
    iiif = getattr(row, 'iiif')
    media_master = getattr(row, 'media_master')

    asset_path = f"{save_location}/batch_{batch_number}/assets/{dpla_id[0]}/{dpla_id[1]}/{dpla_id[2]}/{dpla_id[3]}/"
    df_output_path = f"{save_location}/batch_{batch_number}/data/"

    create_path(asset_path)
    create_path(df_output_path)

    # Are we working with iiif or media_master?
    download_urls = get_iiif_urls(iiif) if iiif else media_master

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

    upload_size = 0

    out, time, size = None, 0, 0  # Defaults

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
            out, time, size = download(url, asset_path)
        except Exception as e:
            out = None
            time = 0
            size = 0

        # Update size
        batch_uploaded = batch_uploaded + size
        total_uploaded = total_uploaded + size

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

            upload_rows.append(row)
    else:
        logging.info("Undefined condition met")

    if batch_uploaded > batch_size:
        logging.info(f"Upload quota met for batch {batch_number}")
        logging.info(f"\n\tBatch {batch_number} \n" \
                         f"\t{len(upload_rows)} files \n" \
                         f"\t{sizeof_fmt(batch_uploaded)}")

        # Save upload info dataframe
        df_out = pd.DataFrame(upload_rows, columns=['dpla_id', 'path', 'size', 'title', 'markup'])
        batch_parquet_out_path = f"{df_output_path}batch_{batch_number}.parquet"
        logging.info(f"Saving {batch_parquet_out_path}")
        df_out.to_parquet(batch_parquet_out_path)

        # Reset
        # logging.info(f"Resetting `upload_row` to empty list()")
        upload_rows = list()
        batch_number = batch_number + 1
        batch_uploaded = 0
        logging.info(f"Starting batch number {batch_number}")

    # If there is a total limit in place then abort after it has been breached.
    if 0 < upload_limit < total_uploaded:
        logging.info(f"Total upload limit breached at {sizeof_fmt(total_uploaded)}. Stopping run.")
        logging.info(f"\n\tBatch {batch_number} \n" \
                         f"\t{len(upload_rows)} files \n" \
                         f"\t{sizeof_fmt(batch_uploaded)}")

        df_out = pd.DataFrame(upload_rows, columns=['dpla_id', 'path', 'size', 'title', 'markup'])
        batch_parquet_out_path = f"{df_output_path}batch_{batch_number}.parquet"
        logging.info(f"Saving {batch_parquet_out_path}")
        df_out.to_parquet(batch_parquet_out_path)
        break
