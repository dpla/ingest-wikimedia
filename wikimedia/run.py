"""
Generic runner

"""
import getopt
import logging
import sys

import boto3
# TODO Move `entries.upload import UploadEntry` back up after logging issue is resolved (see below)
from entries.download import DownloadEntry
from utilities.emailer import SesDestination, SesMailSender, Summary
from utilities.helpers import S3Helper, log_file
from utilities.tracker import Tracker

# Email source and destination
EMAIL_SOURCE    = "DPLA Tech Bot<tech@dp.la>"
EMAMIL_REPLY    = ["DPLA Tech Bot<tech@dp.la>"]
EMAIL_TO        = ["DPLA Tech<tech@dp.la>"]

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
            # We do this here because I can't figure out how to prevent the instantiation of pywikibot
            # in Uploader.__init__ from writing to the log file and dumping all that verbose logging to the
            # screen
            # TODO - contact pywikibot devs to see if there's a better way to do this
            from entries.upload import UploadEntry
            entry = UploadEntry(tracker)
        case "download":
            entry = DownloadEntry(tracker)
        case _:
            log.critical(f"Event type {event_type} is not valid. Must be `upload` or `download`")
            sys.exit(-1)

    entry.execute(**args)

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


@staticmethod
def get_args(args):
    params = {}

    try:
        opts, args = getopt.getopt(args,
                                "hi:u:o:",
                                ["partner=",
                                 "limit=",
                                 "input=",
                                 "output=",
                                 "file_filter=",
                                 "type="])
    except getopt.GetoptError:
        print(
            "run.py\n" \
            "--partner <dpla partner name>\n" \
            "--limit <bytes>\n" \
            "--input <path to parquet>\n" \
            "--output <path to save files>\n" \
            "--file_filter <ids>" \
            # TODO EVENT_TYPE
            )
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print(
                "run.py\n" \
                    "--partner <dpla partner name>\n" \
                    "--input <ingestion3 wiki ouput>\n" \
                    "--output <path to save files>\n" \
                    "--limit <total Download limit in bytes>\n" \
                    "--file_filter <Download only these DPLA ids to download>"
                    # TODO EVENT_TYPE
                    )
            sys.exit()
        elif opt in ("-p", "--partner"):
            params["partner"] = arg
        elif opt in ("-i", "--input"):
            params["input"] = arg
        elif opt in ("-o", "--output"):
            params["output"] = arg.rstrip('/')
        elif opt in ('-t', '--type'):
            params['type'] = arg
        # DOWNLOAD ONLY PARAMS
        elif opt in ("-l", "--limit"):
            params["total_limit"] = int(arg)
        elif opt in ("-f", "--file_filter"):
            params["file_filter"] = arg
    return params
