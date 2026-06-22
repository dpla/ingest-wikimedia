from enum import Enum, auto


class Result(Enum):
    DOWNLOADED = auto()
    FAILED = auto()
    SKIPPED = auto()
    # Uploader skip-class breakdowns. Both also bump ``SKIPPED`` so
    # legacy dashboards keep working — the granular counters add
    # detail without replacing the aggregate. ``NOT_PRESENT`` covers
    # the upstream gap (no S3 asset, downloader didn't stage the
    # file). ``INELIGIBLE`` covers files that exist in S3 but the
    # uploader chose not to upload (bad MIME, missing extension,
    # download-only formats staged for conversion).
    UPLOAD_SKIPPED_NOT_PRESENT = auto()
    UPLOAD_SKIPPED_INELIGIBLE = auto()
    # Maintain (no-create) mode: an upload would have created a File page
    # that does not already exist on Commons, so the fence blocked it. The
    # core safety invariant of maintain mode — maintenance never emits a new
    # File page — surfaces here so operators can audit that it held.
    UPLOAD_SKIPPED_WOULD_CREATE = auto()
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
    # Items where at least one ordinal synced cleanly AND at least
    # one sibling ordinal hit ``had_ordinal_error`` (the typical
    # shape: one ordinal's null-pageid skip + the rest succeeding).
    # Distinguished from full-sync ``SDC_ITEMS_SYNCED`` so dashboards
    # keying on "items fully done" don't accidentally count partial
    # results as healthy. Items in this bucket DO get post-SDC
    # cleanup on their synced ordinals — the partial state is real
    # progress, not a failure to be retried wholesale.
    SDC_ITEMS_PARTIALLY_SYNCED = auto()
    # Qualifier-only ``wbeditentity`` fragments committed by the SDC
    # dispatcher. Exists so the write-delta detection that feeds
    # ``SDC_PAGES_EDITED`` doesn't miss a page edit whose only fragment
    # was a qualifier amend (rare — needs every DPLA claim on the file
    # to already carry today's ``P813``, so the opportunistic refresh
    # adds no reference-update fragments — but real). Not surfaced in
    # the SDC Slack summary; counted purely so ``_sdc_writes_total()``
    # picks up the change.
    SDC_QUALIFIER_UPDATES = auto()
    # Distinct Commons file pages this SDC run actually wrote to — counted
    # at per-ordinal granularity (one file page = one ordinal in partner
    # mode; one file page per call in the legacy --file/--cat/--list
    # paths). A page that received both a MediaInfo entity write AND a
    # follow-up wikitext cleanup edit counts once.  Surfaces the real
    # batch size to operators, who can't infer it from ITEMS_SYNCED alone
    # (a 1-file item and a 1,000-file item both count once there).
    SDC_PAGES_EDITED = auto()
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
    # Ordinals where upload-result.json carries a missing / null / zero
    # ``pageid`` and sdc-sync's title→pageid fallback couldn't resolve
    # it from Commons either (page actually doesn't exist, API error,
    # etc.). Distinguished from the generic ERROR bucket so operators
    # can spot uploader sidecar defects in the Slack summary rather
    # than having them blend in with runtime API failures.
    SDC_ORDINALS_SKIPPED_MISSING_PAGEID = auto()
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
    # Aggregate worker-seconds blocked on a box-wide slot (WorkerSlotBudget
    # contention). Summed across workers; the consumer divides by worker count.
    SDC_SLOT_WAIT_SECONDS = auto()


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

    def snapshot(self) -> dict["Result", int]:
        """Return a shallow copy of the counter state, suitable for
        per-task delta computation across a multiprocessing Pool.

        Pair with :meth:`diff` to compute what changed during a unit of
        work, and :meth:`merge` on the parent's tracker to absorb the
        delta returned from a worker process.
        """
        return dict(self.data)

    def diff(self, prior: dict["Result", int]) -> dict["Result", int]:
        """Return ``{key: self.data[key] - prior[key]}`` for every
        counter, treating missing keys in ``prior`` as zero. Used to
        capture only the counts a worker added during one task —
        contrast with returning the full ``self.data`` from a
        long-lived worker, which would double-count across tasks."""
        return {key: self.data[key] - prior.get(key, 0) for key in self.data}

    def merge(self, delta: dict["Result", int]) -> None:
        """Add each counter in ``delta`` into ``self.data``. Used by
        the parent process to aggregate per-task deltas returned from
        ``multiprocessing.Pool`` workers. Unknown keys in ``delta``
        (e.g. an enum added in a future Tracker schema) are silently
        ignored rather than raising — workers can be slightly newer
        than the parent during a rolling deploy."""
        for key, count in delta.items():
            if key in self.data:
                self.data[key] += count

    def __str__(self) -> str:
        result = "COUNTS:\n"
        for key in self.data:
            value = self.data[key]
            if value > 0:
                result += f"{key.name}: {value}\n"
        return result
