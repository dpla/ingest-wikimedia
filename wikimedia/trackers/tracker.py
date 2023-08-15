
from utilities.exceptions import DownloadException, UploadException

class Tracker:
    DOWNLOADED = "DOWNLOADED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    UPLOADED = "UPLOADED"

    dpla_count = 0           # TODO implement in Download.download()
    skip_count = 0
    fail_count = 0
    success_count = 0
    cumulative_size = 0     # TODO implement in Download.download()
    attempted = 0           # TODO implement in Download.download()

    def __init__(self):
        pass

    def set_dpla_count(self, count):
        """
        Set the number of DPLA items
        """
        Tracker.dpla_count = count

    def set_total(self, total):
        """
        Set the total number of uploads
        """
        Tracker.attempted = total

    def increment(self, status, size=0):
        """
        Increment the status
        """
        if status == Tracker.SKIPPED:
            Tracker.skip_count += 1
        elif status == Tracker.DOWNLOADED or status == Tracker.UPLOADED:
            Tracker.success_count += 1
            Tracker.cumulative_size += size
        elif status == Tracker.FAILED:
            Tracker.fail_count += 1
        else:
            raise UploadException(f"Unknown status: {status}")
