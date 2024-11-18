import os
import logging
from datetime import datetime

from tqdm import tqdm


class TqdmLoggingHandler(logging.Handler):
    """
    This class redirects logging's console output through tqdm so the progress
    bars don't get mangled.
    """

    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


def setup_logging(partner: str, event_type: str, level: int = logging.INFO) -> None:
    """
    Creates a logfile for this process with a unique timestamp and with the partner's
    name. Passes local logging through tqdm so the progress bars don't get mangled.
    Suppresses pywikibot logging below ERROR.
    """
    os.makedirs(LOGS_DIR_BASE, exist_ok=True)
    time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_file_name = f"{time_str}-{partner}-{event_type}.log"
    filename = f"{LOGS_DIR_BASE}/{log_file_name}"
    logging.basicConfig(
        level=level,
        datefmt="%H:%M:%S",
        handlers=[
            TqdmLoggingHandler(),
            logging.FileHandler(filename=filename, mode="w"),
        ],
        format="[%(levelname)s] " "%(asctime)s: " "%(message)s",
    )
    logging.info(f"Logging to {filename}.")
    for d in logging.Logger.manager.loggerDict:
        if d.startswith("pywiki"):
            logging.getLogger(d).setLevel(logging.ERROR)


LOGS_DIR_BASE = "logs"
