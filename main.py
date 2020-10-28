from duploader.dupload import Dupload
import awswrangler as wr
import getopt
import sys

import logging

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")


# v2
# - read from parquet file
# - build queue

# >>> my work starts here
# - download image
# - upload image and metadata to wikimedia


"""
230fb9b31d162faa008fde08db61359e
{
   "datasource":"datasource_tbd",
   "timestamp":"timestamp_tbd",
   "title": ""
   "wikiMarkup":"",
   "assetsToDownload":[
      "https://texashistory.unt.edu/ark:/67531/metapth503909/manifest/"
   ]
}
"""

dpla_identifier = ""
file_url = ""
save_location = ""

try:
    opts, args = getopt.getopt(sys.argv[1:], "hi:u:o:", ["identifier=", "url=", "output="])
except getopt.GetoptError:
    print('main.py -i <identifier> -u <url>')
    sys.exit(2)

for opt, arg in opts:
    if opt == '-h':
        print('main.py -i <identifier> -u <url> -o <output>')
        sys.exit()
    elif opt in ("-i", "--identifier"):
        dpla_identifier = arg
    elif opt in ("-u", "--url"):
        file_url = arg
    elif opt in ("-o", "--output"):
        save_location = arg

logging.info(f"DPLA ID: {dpla_identifier}")
logging.info(f"URL:     {file_url}")
logging.info(f"Out:     {save_location}")

# Do the downloading and uploading

duploader = Dupload()

uploaded_bytes = 0

"""
    Read parquet file for assets to upload 
    ... how to keep a log of what has already been uploaded...? 
    f
    iterate over line 


"""


#
# # while uploaded bytes < 1000000000000
#
# # Download
# downloaded_file, download_time, filesize = duploader.download_single_item(url=file_url, save_location=save_location)
#
# # Create wiki page title
# wikiPageTitle = duploader.create_wiki_page_title(title=title, dpla_identifier=dpla_identifier, suffix=downloaded_file[-4:])
#
# # Create file page in wiki
# wikiFilePage = duploader.create_wiki_file_page(wikiPageTitle)
#
# # Upload to wikimedia
# duploader.upload(wiki_file_page=wikiFilePage, dpla_identifier=dpla_identifier, text=wiki_markup, file=downloaded_file)
#
# uploaded_bytes = uploaded_bytes + filesize
#
# # end while


# parquet file reading


#
# For python 3.6+ AWS has a library called aws-data-wrangler that helps with the integration between Pandas/S3/Parquet
#
# to install do;
#
# pip install awswrangler
#
# to read partitioned parquet from s3 using awswrangler 1.x.x and above, do;


def download(url):
    """
    Download file

    :param url: Array of URLs to download
    :return: Path to downloaded file
    :return: Time to download file
    :return: Size of downloaded file (in bytes)
    """

    # download_urls = row['media_master']
    # FIXME hard code taking the first element for NARA, this works for only one image but won't for (page N)
    # Needs explode() on media_master and then passing page number value to create_wiki_page_title()

    logging.info(f"Downloading {url}")
    file_out, time, size = duploader.download_single_item(url=url, save_location=save_location)

    return file_out, time, size

    # Removed. To be used only when used by calling apply()
    # row['file_out'] = file_out
    # row['time'] = time
    # row['size'] = size
    # return row


def create_title(row):
    page_title = duploader.create_wiki_page_title(title=row['title'], dpla_identifier=row['id'], suffix=row['file_out'][-4:])
    row['page_title'] = page_title
    return row


def upload(row):
    status = 'incomplete'
    try:
        # wiki_page = duploader.create_wiki_file_page(title=row['title'])
        # duploader.upload(wiki_file_page=wikiFilePage, dpla_identifier=dpla_identifier, text=wiki_markup, file=downloaded_file)
        # duploader.upload(wiki_file_page=wiki_page, dpla_identifier=row['id'], text=row['wiki_markup'], file=row['file_out'])
        logging.info(f"Uploaded {row['file_out']}")
        status = "complete"
    except Exception as e:
        status = f"failed {e}"

    row['status'] = status
    return row

def get_iiif_urls(iiif):
    pass

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
                                                      suffix=file_out[-4:] # TODO unpack why trimt suffix
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


# /Users/scott/dpla/i3/nara/wiki/20200811_152647-nara-wiki/
nara_wiki_path_s3 = "s3://dpla-master-dataset/nara/wiki/20200924_171757-nara-wiki.parquet/*.parquet"

df = wr.s3\
    .read_parquet(path=nara_wiki_path_s3, dataset=True) \
    .rename(columns={"_1": "id", "_2": "wiki_markup", "_3": "iiif", "_4": "media_master", "_5": "title"})


total_uploaded = 0
upload_limit = 3500000  # TODO HARD CODE to 1 TB [1000000000000]

for row in df.head(10).itertuples(index=['id','wiki_markup','iiif','media_master', 'title']):

    dpla_id = getattr(row,'id')
    title = getattr(row,'title')
    wiki_markup = getattr(row,'wiki_markup')
    iiif = getattr(row,'iiif') # FIXME unused
    media_master = getattr(row,'media_master')

    # Are we working with iiif or media_master?
    download_urls = get_iiif_urls(iiif) if iiif else media_master

    upload_size = 0
    if len(download_urls > 1):
        # TODO handle multi-asset upload for single item
        # - page title creation
        # - filename
        # - upload asset
        logging.info("Unsupported multi-asset upload")

        for url in download_urls:
            single_file_upload_size =
        break

    else:

    upload_size = dupload(dpla_id=dpla_id,
                          title=title,
                          wiki_markup=wiki_markup,
                          download_urls=download_urls)

    # Track total upload amount
    total_uploaded = total_uploaded + upload_size

    if total_uploaded > upload_limit:
        logging.info("Upload quota met.")
        break




# Join against wiki data harvest avro to identify records to upload
# Process rows
#   - iiif or media_master?
#       - is there more than one? This determines page name construction/filename
#       - this is not a problem atmm