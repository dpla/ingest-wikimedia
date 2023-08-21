"""
Downloads Wikimedia eligible images from a DPLA partner

"""
import sys
import boto3

from utilities.fs import S3Helper, log_file
from utilities.emailer import SesMailSender, SesDestination, DownloadSummary
from utilities.arguements import get_download_args
from entries.download import DownloadEntry

import logging

def main():
    args = get_download_args(sys.argv[1:])

    EMAIL_SOURCE = "tech@dp.la"
    EMAMIL_REPLY = ["tech@dp.la"]
    EMAIL_TO = ["scott@dp.la"]

    s3 = S3Helper()

    # Get the most recent parquet file from the input path
    bucket, key = s3.get_bucket_key(args.get('input_data'))
    recent_key = s3.most_recent_prefix(bucket=bucket, key=key)
    args['input_data'] = f"s3://{bucket}/{recent_key}"

    entry = DownloadEntry(args)

    file = log_file(partner_name=args.get('partner_name'), event_type="download")

    log = logging
    log.basicConfig(level=logging.INFO,
                    datefmt='%H:%M:%S',
                    handlers=[logging.StreamHandler(),
                              logging.FileHandler(filename=file, mode="w")],
                              format='[%(levelname)s] '
                              '%(asctime)s: '
                              '%(message)s')

    log.info("Starting download")
    entry.execute()
    log.info("fin.")

    # Save the log file to S3
    bucket, key = s3.get_bucket_key(args.get('output_base'))
    public_url = s3.write_log_s3(bucket=bucket, key=key, file=file)

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
    emailer.send_email(source=EMAIL_SOURCE,
                       destination=SesDestination(tos=EMAIL_TO),
                       reply_tos=EMAMIL_REPLY,
                       subject=summary.subject(),
                       text=summary.body_text(),
                       html=summary.body_html()
                    )


if __name__ == "__main__":
    main()
