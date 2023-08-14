
from utilities.exceptions import DownloadException, UploadException

class DownloadTracker:
    """
    Status of download
    """
    SKIPPED = "SKIPPED"
    DOWNLOADED = "DOWNLOADED"
    FAILED = "FAILED"

    skip_count = 0
    fail_count = 0
    download_count = 0

    def __init__(self):
        pass

    def increment(self, status):
        """
        Increment the stats
        """
        if status == DownloadTracker.SKIPPED:
            DownloadTracker.skip_count += 1
        elif status == DownloadTracker.DOWNLOADED:
            DownloadTracker.download_count += 1
        elif status == DownloadTracker.FAILED:
            DownloadTracker.fail_count += 1
        else:
            raise DownloadException(f"Unknown status: {status}")


class UploadTracker:
    """
    Status of uploads
    """
    SKIPPED = "SKIPPED"
    UPLOADED = "UPLOADED"
    FAILED = "FAILED"

    dpla_count = 0
    skip_count = 0
    fail_count = 0
    upload_count = 0
    attempted = 0
    cumulative_size = 0

    def __init__(self):
        pass

    def set_dpla_count(self, count):
        """
        Set the number of DPLA items
        """
        UploadTracker.dpla_count = count

    def set_total(self, total):
        """
        Set the total number of uploads
        """
        UploadTracker.attempted = total

    def increment(self, status, size=None):
        """
        Increment the status
        """
        if status == UploadTracker.SKIPPED:
            UploadTracker.skip_count += 1
        elif status == UploadTracker.UPLOADED:
            UploadTracker.upload_count += 1
            UploadTracker.cumulative_size += size
        elif status == UploadTracker.FAILED:
            UploadTracker.fail_count += 1
        else:
            raise UploadException(f"Unknown status: {status}")
