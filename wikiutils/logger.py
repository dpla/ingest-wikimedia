
import sys
import time
import logging

from urllib.parse import urlparse
from wikiutils.utils import Utils

class Logger:
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    log_file = None
    log = None
    utils = Utils()

    def __init__(self, type):
        self.log_file = f"{type}-{self.timestamp}.log"

        logging.basicConfig(
            level=logging.NOTSET, 
            filemode='a',
            datefmt='%H:%M:%S',
            format='%(filename)s: '    
                    '%(levelname)s: '
                    '%(funcName)s(): '
                    '%(lineno)d:\t'
                    '%(message)s'
        )


        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.NOTSET)

        self.log = logging.getLogger('logger')
        self.log.addHandler(file_handler)
        
    def log_info(self, message):
        self.log.info(message)

    def log_error(self, message):
        self.log.error(message)

    def write_log_s3(self, out_path): 
        # Get bucket and key path 
        out_parsed = urlparse(out_path)
        bucket = out_parsed.netloc
        key = f"{out_parsed.path.replace('//', '/').lstrip('/')}"

        with open(self.log_file, "rb") as f:
            self.utils.upload_to_s3(file=f, bucket=bucket, key=f"{key}log/{self.log_file}", content_type="text/plain")


