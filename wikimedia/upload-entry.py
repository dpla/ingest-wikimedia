"""
Upload to Wikimedia Commons

This needs a "batch" folder for input
Read parquet file and then upload assets

"""
import getopt
import sys
import boto3

from utilities.fs import FileSystem
from utilities.logger import WikimediaLogger
from executors.uploader import Uploader
from utilities.emailer import SesMailSender, SesDestination, UploadSummary

# Get input parameters
partner_name, input_path = None, None
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
        input_path = arg
    elif opt in ("-p", "--partner"):
        partner_name = arg

log = WikimediaLogger(partner_name=partner_name, event_type="upload")
uploader = Uploader(log)
fs = FileSystem()

# This is the schema emitted by the ingest-wikimedia download process
READ_COLUMNS = {"_1": "dpla_id",
                "_2": "path",
                "_3": "size",
                "_4": "title",
                "_5": "markup",
                "_6": "page"}

data_in = fs.read_parquet(input_path, cols=READ_COLUMNS)

log.info(f"Read {len(data_in)} from {input_path}")
uploader.execute_upload(data_in)

# Summarize upload
status = uploader.get_status()
log.info(f"Attempted: {status.attempted} files for {status.dpla_count} DPLA records")
log.info(f"Uploaded {status.upload_count} new files")
log.info(f"Failed {status.fail_count} files")
log.info(f"Skipped {status.skip_count} files")

# Upload log file to s3
bucket, key = fs.get_bucket_key(input_path)
public_url = log.write_log_s3(bucket=bucket, key=key)
log.info(f"Log file saved to {public_url}")
log.info("Fin")

# Send email notification
ses_client = boto3.client('ses', region_name='us-east-1')
emailer = SesMailSender(ses_client)
summary = UploadSummary(partner=partner_name,
                        log_url=public_url,
                        status=status)

emailer.send_email(source="DPLA Tech Bot<tech@dp.la>",
                   destination=SesDestination(tos=["scott@dp.la"]),  # FIXME dominic@dp.la should be here. Who else?
                   subject=summary.subject(),
                   text=summary.body_text(),
                   html=summary.body_html(),
                   reply_tos=["DPLA Tech Bot<tech@dp.la>"])
