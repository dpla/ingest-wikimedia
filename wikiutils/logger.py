"""
Logging wrapper
"""

import time
import logging
import os

from wikiutils.utils import Utils

class WikimediaLogger(logging.Logger):
    """
    Wikimedia logger
    """
    log = None
    _log_file = None
    utils = Utils()

    def __init__(self, partner_name, event_type):
        super().__init__(name="wikimedia_logger")

        timestamp = time.strftime("%Y%m%d-%H%M%S")
        # FIX THIS
        # I have now hard coded some really wonky shit together to get the s3 public access to log files work 
        # this probably has a lot of downstream consequences I don't know about yet and need to be patched up together. 
        # the filesytem path is relative to this project directory (.logs/*.log)
        # the s3 log file path is bucket/name/logs/name-event_type-date.log
        self._log_file = f"{partner_name}-{event_type}-{timestamp}.log"
        os.makedirs("./logs/", exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO, 
            datefmt='%H:%M:%S',
            handlers=[logging.StreamHandler(),
                      logging.FileHandler(f"./logs/{self._log_file}", mode="w")],
            format= '[%(levelname)s] '
                    '%(asctime)s: '
                    '%(message)s'
        )
        self.log = logging.getLogger('wikimedia_logger')

    def info(self, msg, *args, **kwargs):
        self.log.info(msg)

    def warning(self, msg, *args, **kwargs):
        self.log.warning(msg)

    def debug(self, msg, *args, **kwargs):
        self.log.debug(msg)

    def error(self, msg, *args, **kwargs):
        self.log.error(msg)
    
    def fatal(self, msg, *args, **kwargs):
        self.log.fatal(msg)

    def get_log_file_name(self): 
        return self._log_file
    
    def write_log_s3(self, key, bucket, extra_args=None):
        """
        Upload log file to s3
        :param key: Key to upload log file to
        :param bucket: Bucket to upload log file to
        :param extra_args: Extra arguments to pass to s3 upload_fileobj
        :return: The URL of the uploaded log file
        """
        s3_log_key = f"{key}/logs/{self.get_log_file_name}"
        # default extra_args for log files are text/plain and public read. These can be overridden by passing in extra_args
        default_args = {"ACL": "public-read", "ContentType": "text/plain"}
        if extra_args: 
            default_args.update(extra_args)

        with open(f"./logs/{self._log_file}", "rb") as f:
            self.utils.upload_to_s3(file=f,
                                    bucket=bucket,
                                    key=s3_log_key,
                                    extra_args=default_args)
        # Return the publicly accessible url to the log file
        return f"https://{bucket}.s3.amazonaws.com/{s3_log_key}"
             
