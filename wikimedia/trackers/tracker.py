
from utilities.exceptions import UploadException
from enum import Enum

class Result(Enum):
    DOWNLOADED = "DOWNLOADED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    UPLOADED = "UPLOADED"

class Tracker:
    # Item tracking
    item_cnt = 0
    item_fail_cnt = 0

    # Image tracking
    image_attempted_cnt = 0
    image_fail_cnt = 0
    image_skip_cnt = 0
    image_success_cnt = 0

    # Size tracking
    image_size_session = 0
    image_size_total = 0

    def __init__(self):
        pass

    @staticmethod
    def set_dpla_count(count):
        """
        Set the number of DPLA items"""
        Tracker.item_cnt = count

    @staticmethod
    def set_total(total):
        """
        Set the total number of uploads"""
        Tracker.image_attempted_cnt = total

    @staticmethod
    def get_size():
        """
        Get the cumulative size of all files"""
        return Tracker.image_size_session

    @staticmethod
    def increment(status, size=0):
        """
        Increment the status"""
        if status == Result.SKIPPED:
            Tracker.image_skip_cnt += 1
            Tracker.image_size_total += size
        elif status == Result.FAILED:
            Tracker.image_fail_cnt += 1
        elif status == Result.DOWNLOADED or status == Result.UPLOADED:
            Tracker.image_success_cnt += 1
            Tracker.image_size_session += size
            Tracker.image_size_total += size
        else:
            # TODO Raise generic exception
            raise UploadException(f"Unknown status: {status}")
