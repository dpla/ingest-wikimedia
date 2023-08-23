"""
Generic runner

"""

import sys
import boto3
import logging

from executors.uploader import Uploader
from entries.download import DownloadEntry
from utilities.fs import S3Helper, log_file
from utilities.arguements import get_args
from utilities.emailer import SesMailSender, SesDestination, Summary

# Email source and destination
EMAIL_SOURCE    = "DPLA Tech Bot<tech@dp.la>"
EMAMIL_REPLY    = ["DPLA Tech Bot<tech@dp.la>"]
EMAIL_TO        = ["Scott<scott@dp.la>"] # TODO replace with tech@dp.la or dominic@dp.la

def main():
    args = get_args(sys.argv[1:])

    tracker = None
    entry = None
    s3 = S3Helper()

    # Arguements required by run.py; default values of None if not provided
    partner = args.get('partner', None)
    event_type = args.get('type', None)
    input = args.get('input', None)

    # Setup logging
    file = log_file(partner=partner, event_type=event_type)
    log = logging
    log.basicConfig(level=logging.INFO,
                    datefmt='%H:%M:%S',
                    handlers=[logging.StreamHandler(),
                              logging.FileHandler(filename=file, mode="w")],
                              format='[%(levelname)s] '
                                '%(asctime)s: '
                                '%(message)s')

    log.info(f"Starting {event_type} for {partner}")

    match event_type:
        case "upload":
            entry = Uploader()
            # FIXME UploadEntry()
            # FIXME execute_upload() --> execute() to match DownloadEntry
            entry.execute_upload(input) # Upload only needs the input path
            pass
        case "download":
            entry = DownloadEntry()
            entry.execute(**args)
        case _:
            log.critical(f"Event type {event_type} is not valid. Must be `upload` or `download`")
            sys.exit(-1)

    # get_tracker to be implemented by other
    tracker = entry.get_tracker()

    # Upload log file to s3
    bucket, _ = s3.get_bucket_key(input)
    public_url = s3.write_log_s3(bucket=bucket, key=partner, file=file)
    log.info(f"Log file saved to {public_url}")
    log.info("fin.")

    # Generate event summary
    summary = Summary(partner=partner,
                      log_url=public_url,
                      tracker=tracker,
                      event_type=event_type)

    # Send notification email
    ses_client = boto3.client('ses', region_name='us-east-1')
    emailer = SesMailSender(ses_client)
    emailer.send_email(source=EMAIL_SOURCE,
                       destination=SesDestination(tos=EMAIL_TO),
                       subject=summary.subject(),
                       text=summary.body_text(),
                       html=summary.body_html(),
                       reply_tos=EMAMIL_REPLY)

if __name__ == "__main__":
    main()
