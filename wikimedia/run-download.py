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


def main():
    args = get_download_args(sys.argv[1:])

    s3 = S3Helper()
    log = WikimediaLogger(partner_name=args.get('partner_name'), event_type="download")
    entry = DownloadEntry(args, log)

    # kick off the download
    entry.execute()

    # We are done.
    log.info("fin.")

    # Save the log file to S3
    bucket, key = s3.get_bucket_key(args.get('output_base'))
    public_url = log.write_log_s3(bucket=bucket, key=key)

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
    main()
