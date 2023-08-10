"""
Logging wrapper
"""

import time
import logging
import os

from wikiutils.utils import Utils

class WikimediaLogger(logging.Logger):
    """
    Logging wrapper
    """
    log = None
    utils = Utils()

    def __init__(self, partner_name, event_type):
        super().__init__(name="wikimedia_logger")

        timestamp = time.strftime("%Y%m%d-%H%M%S")
        log_file = f"./logs/{partner_name}-{event_type}-{timestamp}.log"
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO, 
            datefmt='%H:%M:%S',
            handlers=[logging.StreamHandler(),
                      logging.FileHandler(log_file, mode="w")],
            format= '[%(levelname)s] '
                    '%(asctime)s: '
                    '%(message)s'
        )
        self.log = logging.getLogger('wikimedia_logger')

    def info(self, msg, *args, **kwargs):
        """
        Wrapper for logging.info
        :param message:
        """
        self.log.info(msg)

    # def error(self, message):
    #     """
    #     Wrapper for logging.error
    #     :param message:"""
    #     self.log.error(message)

    # def log_info(self, message):
    #     """
    #     Wrapper for logging.info
    #     :param message:
    #     """
    #     self.log.info(message)

    # def log_error(self, message):
    #     """
    #     Wrapper for logging.error
    #     :param message:"""
    #     self.log.error(message)

    def write_log_s3(self, key, bucket): 
        """
        Upload log file to s3
        :param out_path: s3 path to upload log file to"""
        with open(self.log.getLogFileName, "rb") as f:
            self.utils.upload_to_s3(file=f, bucket=bucket, key=f"{key}log/{self.log.getLogFileName}", content_type="text/plain")
