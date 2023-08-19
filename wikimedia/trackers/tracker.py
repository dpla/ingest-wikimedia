
from utilities.exceptions import DownloadException, UploadException

class Tracker:
    DOWNLOADED = "DOWNLOADED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    UPLOADED = "UPLOADED"

    dpla_count = 0           # TODO implement in Download.download()
    dpla_fail_count = 0      # TODO implement in Download.download() this is the number of failed DPLA records which is
                             #    distinct from the number of failed images
    skip_count = 0
    fail_count = 0
    success_count = 0
    cumulative_size = 0     # This is the size of the images that were downloaded in the current session
    total_size = 0          # This is the total size of all images for the provider (skipped and downloaded)
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

    def get_size(self):
        """
        Get the cumulative size of all files
        """
        return Tracker.cumulative_size

    def increment(self, status, size=0):
        """
        Increment the status
        """
        if status == Tracker.SKIPPED:
            Tracker.skip_count += 1
            Tracker.total_size += size
            print(f"Skipping {Tracker.skip_count} of {Tracker.total_size}")
        elif status == Tracker.DOWNLOADED or status == Tracker.UPLOADED:
            Tracker.success_count += 1
            Tracker.cumulative_size += size # this session
            Tracker.total_size += size      # total of all images
        elif status == Tracker.FAILED:
            Tracker.fail_count += 1
        else:
            raise UploadException(f"Unknown status: {status}")
