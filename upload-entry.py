"""
Upload to Wikimedia Commons

This needs a "batch" folder for input 
Read parquet file and then upload assets 

"""
import getopt
import sys

from wikiutils.utils import Utils
from wikiutils.logger import WikimediaLogger
from wikiutils.uploader import Uploader
from wikiutils.exceptions import UploadException

utils = Utils()
partner_name, input_df = None, None
failed_count, upload_count, skip_count, total_count = 0, 0, 0, 0
columns = {
            "dpla_id": "dpla_id",
            "path": "path",
            "size": "size",
            "title": "title",
            "markup": "markup",
            "page": "page"
           }

# Get input parameters
try:
    opts, args = getopt.getopt(sys.argv[1:], 
                               "hi:u:o:", 
                               ["input=",
                                "partner="])
except getopt.GetoptError:
    print('upload-entry.py --partner <dpla partner abbreviation> --input <path to parquet>')
    sys.exit(2)

for opt, arg in opts:
    if opt == '-h':
        print(
            'upload-entry.py --partner <DPLA hub abbreviation> --input <path to parquet>')
        sys.exit()
    elif opt in ("-i", "--input"):
        input_df = arg
    elif opt in ("-p", "--partner"):
        partner_name = arg

log = WikimediaLogger(partner_name=partner_name, event_type="upload")
uploader = Uploader(log)

log.info(f"Input: {input_df}")
data_in = utils.read_parquet(input_df)
total_count = len(data_in)

for row in data_in.itertuples(index=columns):
    dpla_id, path, size, title, wiki_markup = None, None, None, None, None
    try:
        # Load record from dataframe
        dpla_id, path, size, title, wiki_markup, page = uploader.get_metadata(row)
        # If there is only one record for this dpla_id, then page is `None` and pagination will not
        # be used in the Wikimedia page title
        page = None if len(data_in.loc[data_in['dpla_id'] == dpla_id]) == 1 else page
        # Get file extension
        ext = uploader.get_extension(path)
        # Create Wikimedia page title
        page_title = uploader.create_wiki_page_title(title=title,
                                                dpla_identifier=dpla_id,
                                                suffix=ext,
                                                page=page)

        # Create wiki page using Wikimedia page title
        wiki_page = uploader.create_wiki_file_page(title=page_title)

        if wiki_page is None:
            # Create a working URL for the file from the page title. Helpful for verifying the page in Wikimedia
            log.info(f"Skipping, exists https://commons.wikimedia.org/wiki/File:{page_title.replace(' ', '_')}")
            skip_count += 1
            continue

        # Upload image to wiki page
        # FIXME -- Commented out for --dry-run testing 
        uploader.upload(wiki_file_page=wiki_page,
                        dpla_identifier=dpla_id,
                        text=wiki_markup,
                        file=path,
                        page_title=page_title
                        )
        upload_count += 1
    except UploadException as upload_exec:
        log.error("Upload error: %s", str(upload_exec))
        failed_count += 1
        continue
    except Exception as exception:
        log.error("Unknown error: %s", str(exception))
        failed_count += 1
        continue

# Summarize upload
log.info(f"Finished upload for {input_df}")
log.info(f"Attempted: {total_count} file")
log.info(f"Uploaded {upload_count} new files")
log.info(f"Failed {failed_count} files")
log.info(f"Skipped {skip_count} files")

# Upload log file to s3
bucket, key = utils.get_bucket_key(input_df)
public_url = log.write_log_s3(bucket=bucket, key=key)
log.info(f"Log file saved to {public_url}")
log.info("Fin.")