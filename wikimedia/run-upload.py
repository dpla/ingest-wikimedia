"""
Upload to Wikimedia Commons

"""
import sys
import boto3
import logging

from executors.uploader import Uploader
from utilities.fs import S3Helper, log_file
from utilities.arguements import get_upload_args
from utilities.emailer import SesMailSender, SesDestination, Summary

def main():
    args = get_upload_args(sys.argv[1:])

    s3 = S3Helper()
    uploader = Uploader()

    # email sources and destinations
    EMAIL_SOURCE = "DPLA Tech Bot<tech@dp.la>"
    EMAMIL_REPLY = ["DPLA Tech Bot<tech@dp.la>"]
    EMAIL_TO = ["Scott<scott@dp.la>"] # TODO replace with tech@dp.la or dominic@dp.la

    partner = args.get('partner_name')

    # Setup logging
    file = log_file(partner_name=partner, event_type="upload")
    log = logging
    log.basicConfig(level=logging.INFO,
                    datefmt='%H:%M:%S',
                    handlers=[logging.StreamHandler(),
                              logging.FileHandler(filename=file, mode="w")],
                              format='[%(levelname)s] '
                                '%(asctime)s: '
                                '%(message)s')

    log.info("Starting upload")

    # Run upload
    uploader.execute_upload(args)

    # Upload log file to s3
    bucket, _ = s3.get_bucket_key(args['input'])
    public_url = s3.write_log_s3(bucket=bucket, key=partner, file=file)
    log.info(f"Log file saved to {public_url}")
    log.info("fin.")

    tracker = uploader.get_tracker()
    # Send email summary and notification
    ses_client = boto3.client('ses', region_name='us-east-1')
    emailer = SesMailSender(ses_client)
    summary = Summary(partner=partner,
                      log_url=public_url,
                      tracker=tracker,
                      event_type=Summary.UPLOAD)

    emailer.send_email(source=EMAIL_SOURCE,
                       destination=SesDestination(tos=EMAIL_TO),
                       subject=summary.subject(),
                       text=summary.body_text(),
                       html=summary.body_html(),
                       reply_tos=EMAMIL_REPLY)

if __name__ == "__main__":
    main()