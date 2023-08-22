"""
Upload to Wikimedia Commons

"""
import sys
import boto3
import logging

from executors.uploader import Uploader
from utilities.fs import S3Helper, FileSystem, log_file
from utilities.arguements import get_upload_args
from utilities.emailer import SesMailSender, SesDestination, UploadSummary

# Get input parameters
args = get_upload_args(sys.argv[1:])

file = log_file(partner_name=args.get('partner_name'), event_type="upload")
log = logging
log.basicConfig(level=logging.INFO,
                datefmt='%H:%M:%S',
                handlers=[logging.StreamHandler(),
                            logging.FileHandler(filename=file, mode="w")],
                            format='[%(levelname)s] '
                            '%(asctime)s: '
                            '%(message)s')

fs = FileSystem()
s3 = S3Helper()
uploader = Uploader()

log.info("Starting upload")

#
# TODO  The reading in of data should be moved out of the run and into the exectutor
#
#
# Get the most recent parquet file from the input path
bucket, key = s3.get_bucket_key(args.get('input'))
recent_key = s3.most_recent_object(bucket=bucket, key=key)
args['input'] = f"s3://{bucket}/{recent_key}"
data_in = fs.read_parquet(args.get('input'))

log.info(f"Read {len(data_in)} image rows from {args.get('input')}")

# TODO begin the run entry here. 

# Run upload
uploader.execute_upload(data_in)

# Summarize upload
tracker = uploader.get_tracker()
log.info(f"Attempted: {tracker.image_attempted_cnt} files for {tracker.item_cnt} DPLA records")
log.info(f"Uploaded {tracker.image_success_cnt} new files")
log.info(f"Failed {tracker.image_fail_cnt} files")
log.info(f"Skipped {tracker.image_skip_cnt} files")

# Upload log file to s3
bucket, key = s3.get_bucket_key(args['input'])
public_url = s3.write_log_s3(bucket=bucket, key=f"{args.get('partner_name')}", file=file)
log.info(f"Log file saved to {public_url}")
log.info("Fin")

# Send email notification
ses_client = boto3.client('ses', region_name='us-east-1')
emailer = SesMailSender(ses_client)
summary = UploadSummary(partner=args.get('partner_name'),
                        log_url=public_url,
                        tracker=tracker)

emailer.send_email(source="DPLA Tech Bot<tech@dp.la>",
                   destination=SesDestination(tos=["scott@dp.la"]),  # FIXME dominic@dp.la should be here. Who else?
                   subject=summary.subject(),
                   text=summary.body_text(),
                   html=summary.body_html(),
                   reply_tos=["DPLA Tech Bot<tech@dp.la>"])
