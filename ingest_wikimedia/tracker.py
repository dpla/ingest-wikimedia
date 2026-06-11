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
    # Ordinals where Commons returned `no-such-entity` for the staged
    # M-id — the file page has been deleted (often as a duplicate by a
    # human curator) or was never uploaded in this run path. Not a
    # failure of the SDC phase — the SDC phase only writes to existing
    # MediaInfo entities; ensuring the entity exists is the upload
    # phase's responsibility. Tracked separately so genuine errors
    # (claim rejection, throttle, etc.) stay distinguishable from
    # not-our-problem skips.
    SDC_ORDINALS_SKIPPED_MISSING_ENTITY = auto()
    # Items where every eligible ordinal hit the per-ordinal exception
    # path — i.e., the item didn't fail due to malformed data
    # (MAPPING) or missing sidecars (NO_SIDECAR), but because all of
    # its SDC writes raised at runtime. Without this counter such items
    # would be misclassified as MAPPING skips.
    SDC_ITEMS_SKIPPED_ERROR = auto()
    # Phase-3b legacy-Artwork migration counters. Driven by
    # tools/sdc_sync.py::_run_legacy_migration_mode (and any future
    # standalone migration tool). Tracking is at per-ordinal
    # granularity — one Commons file = one migration attempt — with
    # the exception of LEGACY_IMPORTS_POSTED, which sums the count
    # of import claims across all files (same shape as
    # SDC_CLAIMS_ADDED / BYTES — see the inline comment for that one).
    LEGACY_MIGRATED = auto()  # wikitext rewritten + any imports posted
    LEGACY_IMPORTS_POSTED = auto()  # community-import claims (sum across files)
    LEGACY_SKIPPED_NOT_LEGACY = auto()  # page didn't carry a legacy template
    LEGACY_SKIPPED_ALREADY = auto()  # already migrated (idempotency hit)
    LEGACY_SKIPPED_ERROR = auto()  # raised at runtime, isolated by exc boundary


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
