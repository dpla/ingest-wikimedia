"""
Downloads Wikimedia eligible images from a DPLA partner

"""
import sys
import boto3

from utilities.fs import S3Helper
from utilities.logger import WikimediaLogger
from utilities.emailer import SesMailSender, SesDestination, DownloadSummary
from utilities.arguements import get_download_args
from entries.download import DownloadEntry

import os
import logging

def main():
    args = get_download_args(sys.argv[1:])

    s3 = S3Helper()
    # log = WikimediaLogger(partner_name=args.get('partner_name'), event_type="download")

    entry = DownloadEntry(args)

    log.info("Starting download")

    # kick off the download
    entry.execute()

    # We are done.
    log.info("fin.")

    # Save the log file to S3
    bucket, key = s3.get_bucket_key(args.get('output_base'))
    log_file_key = f"{key}/logs/{_log_file}"

    public_url = s3.write_log_s3(bucket=bucket, key=log_file_key, file=log_file)

    # public_url = log.write_log_s3(bucket=bucket, key=key)

    # Statement does not write to file but useful in the console
    log.info(f"Log file saved to {public_url}")

    # Build summary of download (for email)
    summary = DownloadSummary(partner=args.get('partner_name'),
                            log_url=public_url,
                            # FIXME this is B.S. here.
                            tracker=entry.downloader.get_status())
    # Send email notification
    ses_client = boto3.client('ses', region_name='us-east-1')
    emailer = SesMailSender(ses_client)
    emailer.send_email(source="tech@dp.la",
                    destination=SesDestination(tos=["scott@dp.la"]),  # FIXME dominic@dp.la should be here. Who else?
                    subject=summary.subject(),
                    text=summary.body_text(),
                    html=summary.body_html(),
                    reply_tos=["tech@dp.la"])


if __name__ == "__main__":
    import time
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    # FIX THIS
    # I have now hard coded some really wonky shit together to get the s3 public access to log files work
    # this probably has a lot of downstream consequences I don't know about yet and need to be patched up together.
    # the filesytem path is relative to this project directory (.logs/*.log)
    # the s3 log file path is bucket/name/logs/name-event_type-date.log
    _log_file = f"{timestamp}.log"
    os.makedirs("./logs/", exist_ok=True)
    log_file = f"./logs/{_log_file}"

    log = logging
    log.basicConfig(
        level=logging.INFO,
        datefmt='%H:%M:%S',
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(log_file, mode="w")],
        format= '[%(levelname)s] '
                '%(asctime)s: '
                '%(message)s'
    )
    main()
