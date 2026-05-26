from enum import Enum, auto


class Result(Enum):
    DOWNLOADED = auto()
    FAILED = auto()
    SKIPPED = auto()
    UPLOADED = auto()
    BYTES = auto()
    ITEM_NOT_PRESENT = auto()
    BAD_IIIF_MANIFEST = auto()
    NO_MEDIA = auto()
    BAD_IMAGE_API = auto()
    RETIRED = auto()
    ORPHANS_TAGGED = auto()
    ORPHANS_FLAGGED = auto()
    # SDC phase counters. `Tracker.__str__` already prints only non-zero
    # values, so adding these here doesn't affect downloader/uploader output.
    SDC_ITEMS_SYNCED = auto()
    SDC_CLAIMS_ADDED = auto()
    SDC_REFS_ADDED = auto()
    SDC_REMOVALS = auto()
    SDC_ITEMS_SKIPPED_NO_SIDECAR = auto()
    SDC_ITEMS_SKIPPED_MAPPING = auto()
    # Ordinals whose SDC sync raised an unexpected exception
    # (pywikibot APIError, network timeout, deep KeyError, etc.).
    # Per-ordinal granularity so transient failures don't abort the
    # whole partner batch — the matching try/except is in
    # tools/sdc_sync.py::_run_partner_mode.
    SDC_ORDINALS_SKIPPED_ERROR = auto()


class Tracker:
    def __init__(self):
        self.data = {}
        for value in Result:
            self.data[value] = 0

    def increment(self, status: Result, amount=1) -> None:
        self.data[status] = self.data[status] + amount

    def count(self, status: Result) -> int:
        return self.data[status]

    def reset(self):
        for value in Result:
            self.data[value] = 0

    def __str__(self) -> str:
        result = "COUNTS:\n"
        for key in self.data:
            value = self.data[key]
            if value > 0:
                result += f"{key.name}: {value}\n"
        return result
