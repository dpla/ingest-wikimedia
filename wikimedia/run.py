"""
Generic runner

"""
import logging
import sys

import boto3
# TODO Move `entries.upload import UploadEntry` back up after logging
# issue is resolved (see below)
from entries.download import DownloadEntry
from utilities.emailer import SesDestination, SesMailSender, Summary
from utilities.helpers import S3Helper, Text
from utilities.tracker import Tracker
from utilities.helpers import get_args

# Email source and destination
EMAIL_SOURCE    = "DPLA Tech Bot<tech@dp.la>"
EMAMIL_REPLY    = ["DPLA Tech Bot<tech@dp.la>"]
EMAIL_TO        = ["Scott<scott@dp.la>"]

def main():
    tracker = Tracker()
    s3 = S3Helper()
    entry = None

    # Get arguements
    args = get_args(sys.argv[1:])
    # Arguements required by run.py; default values of None
    partner = args.get('partner', None)
    event_type = args.get('type', None)
    input = args.get('input', None)

    # Setup logging
    filename = Text.log_file(partner=partner, event_type=event_type)
    log = logging
    log.basicConfig(level=logging.INFO,
                    datefmt='%H:%M:%S',
                    handlers=[logging.StreamHandler(),
                              logging.FileHandler(filename=filename, mode="w")],
                              format='[%(levelname)s] '
                                '%(asctime)s: '
                                '%(message)s')

    log.info(f"Starting {event_type} for {partner}")

    match event_type:
        case "upload":
            # We do this here because I can't figure out how to prevent the
            # instantiation of pywikibot in Uploader.__init__ from writing to
            # the log file and dumping all that verbose logging to the
            # screen
            # TODO - contact pywikibot devs to see if there's a better way to do this
            from entries.upload import UploadEntry
            entry = UploadEntry(tracker)
        case "download":
            entry = DownloadEntry(tracker)
        case _:
            log.critical(f"{event_type} is not valid. Must be `upload` or `download`")
            sys.exit(-1)

    entry.execute(**args)

    # Upload log file to s3
    bucket, _ = s3.get_bucket_key(input)
    public_url = s3.write_log_s3(bucket=bucket, key=partner, file=filename)
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
