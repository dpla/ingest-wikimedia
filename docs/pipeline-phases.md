{% raw %}
# Pipeline Phases

Four sequential upload phases per target. All are idempotent for Commons/S3 state — re-running picks up where it left off. The one exception is the uploader's `hand-fix.jsonl` sidecar, which is append-only: unresolved hand-fix cases are re-recorded on each run. They operate per-partner, reading and writing under `s3://dpla-wikimedia/<partner>/images/<sharded-prefix>/<dpla-id>/` (see [sidecars.md](sidecars.md) for the path layout and file inventory).

```text
get-ids-es  →  downloader  →  uploader  →  sdc-sync
   ▼              ▼            ▼              ▼
dpla-map.json   media bytes   Commons        MediaInfo
sdc.json        file-list.txt upload-        statements
                iiif.json     result.json
```

## 1. `get-ids-es` — ID enumeration & metadata staging

Source: `tools/get_ids_es.py`.

**Inputs.** A partner slug (required), plus optional `--institution NAME` (repeatable), `--collection NAME` (requires exactly one `--institution`), or `--single-id <id>` (mutually exclusive with the other two).

**What it does.**

1. Resolves the hub display name via `PARTNER_HUBS[partner]`.
2. Loads upload-eligible institution names from `institutions_v2.json` (live-fetched from `dpla/ingestion3`). An institution is eligible iff both its hub and itself have non-empty Wikidata QIDs and at least one of them has `upload: true`. Per-name keys, not per-QID, so two name variants pointing at the same QID can carry independent flags.
3. Paginates the `dpla_alias` Elasticsearch index with `search_after` (size 500, sort `["id", "_doc"]`), applying:
   - `provider.name.not_analyzed == <hub-name>`
   - `rightsCategory == "Unlimited Re-Use"`
   - `dataProvider.name.not_analyzed in <eligible-names>` (skipped under `--single-id`)
   - asset gate: `mediaMaster` exists OR `iiifManifest` exists OR `isShownAt` matches a CONTENTdm IIIF-derivable wildcard
   - banlist check against `dpla-id-banlist.txt`
4. For CONTENTdm items with no `mediaMaster` and no `iiifManifest`, derives the IIIF manifest URL from `isShownAt` and patches it into the document as `iiifManifest`.
5. Stamps `_staged_by_get_ids_es: true` on every document so the downloader can refuse to operate on legacy unstaged metadata.
6. Stages each document to S3 as `dpla-map.json` via a `ThreadPoolExecutor` (max 10 workers, bounded semaphore of 40 in-flight tasks to bound memory).
7. Collects each DPLA ID (with a Commons-title sort key) during enumeration, then — after the Phase 3 `sdc.json` staging below completes — prints them all to stdout **sorted by Commons file-title prefix**. This sorted stream **is** the IDs CSV the downloader consumes (the caller redirects it), so every downstream phase processes items in human-readable alphabetical order. Deferring the emission to the very end also means a mid-run crash never produces a partial-but-misleading CSV.
8. **Phase 3 — `sdc.json` pre-compute.** After enumeration, re-reads each item's `dpla-map.json` from S3 (not from in-memory state, to keep peak RAM at O(unique-subjects) rather than O(hub-size)), runs `build_claims_for_doc()` from `ingest_wikimedia/sdc.py`, and stages the resulting `{"claims": [...]}` envelope to S3 as `sdc.json`. Items whose provider/dataProvider don't resolve into `institutions_v2` are skipped at this step silently — their `dpla-map.json` is still written so the downloader/uploader can proceed; only the SDC phase will be a no-op.

**Reliability features.**

- `SIGALRM`-based 120 s hard wall-clock timeout on every ES request (`requests.timeout=30` cannot catch stalled mid-response reads).
- `check_es_response()` rejects any response with `timed_out=true` or `_shards.failed > 0` so a 200-OK partial response cannot silently terminate a `search_after` paginator.
- Bounded-semaphore-protected S3 writes so the worker pool's task queue doesn't OOM on a 100 K-item hub.

**Flags.** No `--dry-run` or `--max-records`. To bound a run, redirect stdout to a temporary CSV and `head` it. Two maintain-mode flags exist: `--maintain` (relax the institution upload-eligibility gate to QID-only, so already-uploaded items of no-longer-opted-in institutions are still enumerated) and `--skip-media-filter` (drop the per-item media/rights gate so the full Commons category can be reconciled — used by the category-anchored maintain staging scan, i.e. both the hash and lite routes; the id-list-anchored single-DPLA-id/collection sub-path keeps the filter). Both are set by the launcher's maintain pipelines; see Alternate run modes → `maintain` below. Pre-flight Slack notification fires from `notify_phase_start("get-ids-es")`.

### NARA's special enumerator (`tools/get_ids_nara.py`)

The general `get-ids-es` enumeration would never complete on NARA's 18M+-item hub under hub-wide pagination. `tools/get_ids_nara.py` replaces it for NARA with five priority strategies executed sequentially (smallest first, with cross-strategy dedup):

1. **Format** — `terms` on `sourceResource.format` for every bucket below 12,000 items, batched 6 per query.
2. **mediaMaster extension** — `regexp` on `mediaMaster` matching `.+\.(mp3|mpg|png|gif|wav)`.
3. **Identifier** — `regexp` on `sourceResource.identifier` matching `[0-9]{1,6}` (≤ 6-digit numerics).
4. **Collection** — currently disabled; will re-enable once the first three phases complete. Excludes high-volume top-level collections by prefix and substring.
5. **Language** — `terms` on `sourceResource.language.name` excluding English, batched 10 per query.

After enumeration, NARA runs the same Phase 2 (Wikidata reconciliation of `exactMatch` subjects) and Phase 3 (`sdc.json` staging) as the general enumerator.

NARA cannot be launched from Slack — it's a hand-managed batch process.

### Single-item mode

`get-ids-es <partner> --single-id <id>` does one ES `term` query (size 2 to make the defensive "expected exactly one hit" check reachable), bypasses hub-eligibility filtering (the caller is presumed to have eligibility-checked via `resolve-dpla-ids`), and stages the single document's sidecars. Used by `/wikimedia-upload <dpla-id>` flows to re-stage sidecars with current mapping code before downstream phases run.

## 2. `downloader` — media → S3

Source: `tools/downloader.py`.

**Inputs.** `<ids.csv> <partner>` plus optional `--max-age-days N` (default 365), `--notify-complete`, `--overwrite`, `--dry-run`, `--verbose`, `--sleep`.

**What it does.**

1. Reads the IDs CSV.
2. For each item: reads `dpla-map.json` from S3. **Refuses** to operate if the document lacks `_staged_by_get_ids_es: true` — legacy unstaged metadata is no longer supported. Operator must re-run `get-ids-es`.
3. Chooses between `mediaMaster` (direct list of media URLs) and `iiifManifest` (fetch + parse). For IIIF: fetches the manifest, walks v2 or v3 to produce per-canvas image-API URLs (full-size, not tiles), stages the manifest to `iiif.json`, and writes the URL list to `file-list.txt`.
4. For each media URL (1-indexed ordinal): downloads to a temp file, then uploads to `s3://dpla-wikimedia/<partner>/images/<a>/<b>/<c>/<d>/<dpla_id>/<ordinal>_<dpla_id>`. The S3 object's user-metadata is stamped with the SHA1 in the `CHECKSUM` field.

**Idempotency and refresh.**

- `s3_file_exists()` treats any S3 object with `content_length == 0` as **absent** so corrupted stubs get re-attempted automatically.
- `--max-age-days N` re-downloads keys with `last_modified` older than N days, used for refresh sweeps.
- `--overwrite` forces unconditional re-download.
- Default behaviour: skip a file if it already exists in S3 with non-zero length.

**Defence-in-depth against silent corruption.**

- `download_file_to_temp_path()` tracks `bytes_written` and raises if the HTTP response yielded 0 bytes — defends against the case where `requests` returns 200 OK with an empty body, which would otherwise plant a 0-byte stub.
- `upload_file_to_s3()` refuses to upload any 0-byte local file.
- When re-uploading the same key with metadata changes (e.g. content-type fix), it uses `s3.copy_object(MetadataDirective="REPLACE", Metadata=dict(s3_object.metadata))` — the explicit `Metadata=` is load-bearing because `REPLACE` silently drops every metadata field not provided. An earlier version lost the `CHECKSUM` metadata via this exact bug; a lesson note captures it.

**Error handling.**

| Failure | Behaviour |
|---|---|
| 404 / 5xx during download | Per-ordinal `Result.FAILED`; per-item processing continues |
| NARA `https/` malformed-scheme URLs | Repaired in place (add the missing colon) |
| Disallowed MIME / octet-stream / download-only | `Result.SKIPPED`; per-item continues |
| IAM credential blip | `CredentialRetrievalError` retry loop (up to 3 attempts) |
| 0-byte response | Raise + per-item continue |

**Output.** A per-item summary line `Item <dpla_id>: N ordinals (skipped=..., fetched=..., refreshed=..., rejected=..., failed=...)`, plus a phase-end `COUNTS:` block of `tracker.py` counters.

`--notify-complete` is set by the `refresh_only` run mode so the refresh-style sweeps post their own `Wikimedia Download Refresh Complete:` Slack summary at the end. During a normal `get-ids-es → downloader → uploader → sdc-sync` chain, the downloader is silent — the uploader's completion message subsumes it.

## 3. `uploader` — S3 → Wikimedia Commons

Source: `tools/uploader.py`. The most complex phase.

**Inputs.** `<ids.csv> <partner>` plus optional `--dry-run`, `--verbose`, `--no-create` (maintain fence — never create a new Commons File page; a would-be net-new upload is blocked and recorded as `UPLOAD_SKIPPED_WOULD_CREATE`), `--workers N` (parallel worker processes; default `UPLOADER_PRIORITY_SLOTS` = 4, so a standalone or launched run uploads in parallel by default — pass `--workers 1` for the legacy single-process for-loop), and `--workers-budget N` (box-wide Commons-write cap shared with sdc-sync; `0` — the standalone default — disables it, launch runs pass `24`).

**What it does (per item).**

1. Reads `dpla-map.json` and `file-list.txt` from S3.
2. Computes per-ordinal Commons file titles via `get_page_title()` and `compute_ordinal_exts_and_page_labels()` — see [special-cases.md](special-cases.md#title-generation) for the sanitization rules.
3. For each ordinal: downloads the S3 object to a temp file, computes (or reads from S3 metadata) its SHA1, and decides what to do with it. Under the SHA1-uniqueness constraint (PR C+D) **the uploader never creates a second Commons file for a SHA1 that already exists**, so once `find_file_by_hash` returns a hit it NEVER uploads a second byte-identical copy — it resolves inline to exactly one non-upload outcome: SKIP (our content is already at the intended title), MOVE-rename (the intended title is empty or a redirect-to-self, via `_move_to_correct_title`), MERGE_AND_REDIRECT (legitimate source duplication — merge this item's SDC onto the earliest canonical file and leave a `#REDIRECT` at the intended title), or HAND_FIX (the rename is blocked, or the match is a community upload — recorded to `hand-fix.jsonl`, no upload). See [special-cases.md](special-cases.md#hash-drift-resolution) for the full decision tree.
4. Composes Wikimedia file-page wikitext via `get_wiki_text()` — a `{{DPLA metadata}}` block with `| source = {{DPLA|...}}` and `| Institution = {{Institution|wikidata=...}}` parameters. See [templates.md](templates.md).
5. Uploads via pywikibot's `site.upload()`. Catches `fileexists-shared-forbidden`, `filetype-badmime`, `filetype-banned`, `duplicate`, `no-change`, `backend-fail-internal` and other Wikimedia errors per the `IGNORE_WIKIMEDIA_WARNINGS` list (which suppresses cosmetic warnings but lets actual conflicts surface).
6. After the per-ordinal loop, runs a **post-item orphan check**: probes `(page N+1)`, `(page N+2)`, etc. for stale trailing-page files an earlier run left behind after the source media was truncated. The probe is **log-only** — it writes nothing to Commons (the `{{Duplicate}}`-tag apparatus is retired), counting each orphan found under `Result.ORPHANS_FLAGGED` so an operator can reconcile it by hand. See [special-cases.md](special-cases.md#orphan-audit).
7. Writes `upload-result.json` to S3 with one entry per ordinal (status / title / pageid / error). This sidecar is the SDC phase's source of truth for which Commons pages exist.

**Dispatch.** When `--workers > 1` (the default, `UPLOADER_PRIORITY_SLOTS` = 4) the items are handed to a spawn-start `multiprocessing.Pool` via `imap_unordered`, each worker process holding its own pywikibot session; `--workers 1` walks the items serially in-process. Either path uploads each item under one box-wide `WorkerSlotBudget` slot when `--workers-budget > 0`.

**Per-ordinal statuses written to `upload-result.json`.** `UPLOADED`, `SKIPPED`, `NOT_PRESENT`, `INELIGIBLE`, `FAILED`, `MERGED`, `HAND_FIX`. Only `UPLOADED` and `SKIPPED` are SDC-eligible. `MERGED` carries the canonical file's `title`/`pageid` but is deliberately *not* SDC-eligible — its SDC was merged onto the canonical file inline, so re-targeting our redirect title would double-write. `HAND_FIX` means the SHA1 match couldn't be safely resolved (rename blocked, or a community-file match) and was recorded to `hand-fix.jsonl` for a human. See [sidecars.md](sidecars.md#upload-resultjson) for the full status table.

**Critical correctness detail: pageid refresh.** `wiki_file_page.pageid` returns the cached `0` from the pre-upload existence check rather than the just-assigned ID. `process_file` constructs a fresh `FilePage` and forces `.exists()` to refresh `pageid` before persisting (`uploader.py:625-649`). On any failure, `pageid` is set to `None` (not `0`) so `sdc_sync.py`'s `if not pageid` guard cleanly skips malformed entries.

**Output.** Phase-end `COUNTS:` block plus per-item summary lines; the block surfaces `UPLOAD_MERGED_TO_CANONICAL` and `UPLOAD_HAND_FIX` alongside the usual uploaded/skipped/failed totals, and the `#tech-alerts` completion summary reports the same figures as its `MERGED:` and `HAND-FIX:` lines. A "Retry Complete" Slack message variant fires when the session label starts with `retry-` — the helper folds in download-phase failures so operators see one combined count instead of two confusing messages.

## 4. `sdc-sync` — SDC sync

Source: `tools/sdc_sync.py`. See [sdc-sync.md](sdc-sync.md) for the full SDC deep dive. This section covers the phase's role in the pipeline.

**Inputs.** Partner mode: `--partner <partner> [--ids-file PATH]`, plus:

- `--workers N` (default 1) — number of worker processes for partner-mode sync. `N > 1` dispatches per-DPLA-item work to a spawn-start `multiprocessing.Pool`, each worker holding its own pywikibot session; `N = 1` keeps the single-process path. Production launches pass `24` (matched to `--workers-budget`) so a solo session can saturate the box-wide slot pool.
- `--workers-budget N` (default 0) — box-wide cap on concurrent Commons-writing items across **all** sdc-sync and uploader sessions on the host (see [architecture.md § Intra-host write throttle](architecture.md#intra-host-write-throttle-workerslotbudget)). `0` disables it; production launches pass `24`. Must be identical across concurrent sessions.
- `--migrate-legacy` — swaps SDC sync for the standalone legacy-Artwork migration mode (see below).
- `--normalize-wikitext` / `--no-normalize-wikitext` (default on) — controls the post-SDC wikitext-cleanup strip.

Legacy mode: `--file "File:Title.jpg"` (repeatable) or `--cat <Category>` or `--lists <dir>` (Slack runs never use the legacy mode; these single-purpose manual modes do not participate in the worker-slot budget).

**What it does (partner mode).**

1. Reads the IDs CSV.
2. **Dispatch.** When `--workers 1` (the default), the parent walks items in-process. When `--workers > 1`, items are dispatched one-per-task to a `multiprocessing.Pool` via `imap_unordered`; each worker re-logs into Commons, processes one item end-to-end, and returns its per-task counter delta, which the parent merges into the shared `Tracker`. Every ordinal of every item has a unique M-id, so workers never write to the same MediaInfo entity. **Either path** acquires one box-wide `WorkerSlotBudget` slot around each item (a no-op when `--workers-budget` is 0), so a 1-worker session still counts against the cap exactly like a parallel one; time spent blocked on a slot accrues into `SDC_SLOT_WAIT_SECONDS`.
3. Per item (`_process_one_partner_item`): reads `sdc.json`, `upload-result.json`, and `file-list.txt` from S3. Items missing the `sdc.json` / `upload-result.json` sidecars are skipped (counter `SDC_ITEMS_SKIPPED_NO_SIDECAR`); a missing `file-list.txt` is non-fatal (P2699 qualifiers are simply not materialized).
4. Filters to ordinals whose `upload-result.json` status is `UPLOADED` or `SKIPPED`. Computes per-ordinal P304 page numbers via `_compute_page_numbers()` for multi-file extension groups (e.g. items with both jpg and pdf get separate page-number sequences per extension).
5. For each eligible ordinal: looks up its `download_url` from `file-list.txt` by 1-based index, derives `mediaid = M<pageid>` from `upload-result.json`, then calls `process_one_from_sdc(mediaid, dpla_id, sdc_payload, download_url, page_number)`.
6. Inside `process_one_from_sdc`: walks the staged `sdc.json` claims, runs the `check()` matcher against current Commons-side state, accumulates new claims / qualifier amends / reference updates / removals into per-file lists, then **flushes everything as one `wbeditentity` revision** via `_flush_per_file_edits()`. The previous design (multiple separate `wbsetclaim` / `wbsetqualifier` / `wbremoveclaims` POSTs per file) could leak orphaned stale statements if an intermediate call failed; the consolidated dispatcher is all-or-nothing per file.
7. **Post-SDC wikitext cleanup** (on by default; `--no-normalize-wikitext` disables). For each synced file, dispatches on the page's current wikitext shape: a `{{DPLA metadata}}` page gets its now-redundant params stripped (`wikitext_normalize.normalize_page`); a legacy `{{Artwork}}` / `{{Information}}` / `{{Photograph}}` page is run through the same migration as `--migrate-legacy` (see below). This strip is the documented end of the per-file lifecycle (upload → SDC → wikitext cleanup).

**Per-ordinal qualifiers materialised at write time** (not baked into `sdc.json`, because they vary per ordinal):

- P304 (page number) qualifier on P760, only when the ordinal is part of a multi-file extension group.
- P2699 (URL) qualifier on P7482, with the per-ordinal direct download URL from `file-list.txt`.

**Legacy-Artwork migration mode (`--migrate-legacy`).** Instead of running SDC sync, `sdc-sync --partner <partner> --migrate-legacy` walks the same partner IDs CSV and migrates each item's Commons files from the legacy `{{Artwork}}` form to `{{DPLA metadata}}`. For each file it walks the revision history to distinguish DPLA-bot-authored values (overwrite-safe with canonical data) from community contributions, preserves the community values by importing them as SDC statements with the `P887 → Q131783016` ("inferred from Wikitext") + `P4656` (Wikimedia import URL permalink) reference shape, then rewrites the wikitext. The exact same migration also runs automatically inside the post-SDC cleanup pass whenever a normal sync encounters a file still in the legacy form, so the standalone mode is just a way to drive it for a whole partner without re-syncing. `ingest_wikimedia/legacy_artwork.py` holds the logic; counters land in the `LEGACY_*` set.

**Counters tracked.** Sync counters: `SDC_ITEMS_SYNCED` (item fully synced — every eligible ordinal succeeded), `SDC_ITEMS_PARTIALLY_SYNCED` (at least one ordinal synced *and* at least one sibling ordinal errored — broken out so dashboards keying on "items fully done" don't count mixed results as healthy), `SDC_CLAIMS_ADDED`, `SDC_REFS_ADDED`, `SDC_REMOVALS`, `SDC_QUALIFIER_UPDATES`, `SDC_PAGES_EDITED` (distinct Commons file pages actually written), `SDC_ITEMS_SKIPPED_NO_SIDECAR`, `SDC_ITEMS_SKIPPED_MAPPING`, `SDC_ITEMS_SKIPPED_ERROR`, `SDC_ORDINALS_SKIPPED_ERROR`, `SDC_ORDINALS_SKIPPED_MISSING_ENTITY`, `SDC_ORDINALS_SKIPPED_MISSING_PAGEID` (uploader recorded a null/zero pageid and the title→pageid fallback couldn't resolve it either), and `SDC_SLOT_WAIT_SECONDS` (aggregate worker-seconds blocked on a `WorkerSlotBudget` slot, summed across workers). The error / mapping / missing-entity split is deliberate — operators need to distinguish bad-data items from bad-network items from upload-never-happened items. Migration mode adds the `LEGACY_*` set (`LEGACY_MIGRATED`, `LEGACY_IMPORTS_POSTED`, `LEGACY_SKIPPED_NOT_LEGACY`, `LEGACY_SKIPPED_ALREADY`, `LEGACY_SKIPPED_ERROR`).

---

## Alternate run modes

`wikimedia-launch.yml` accepts three boolean inputs that swap the default upload chain for a different variant. They are mutually exclusive — pick at most one.

### `refresh_only=true`

Chain: `get-ids-es → downloader` only. No uploader, no SDC. The downloader is invoked with `--notify-complete` (and `--max-age-days N` from the workflow input, default 365) so its own Slack summary fires at the end. For re-downloading aged master copies of media files without re-uploading.

### `sdc_only=true`

Chain: `get-ids-es → sdc-sync` only. No downloader, no uploader. The `get-ids-es` step re-stages each item's `sdc.json` from current ingestion3 data; `sdc-sync` then reconciles Commons against that. Used for backfilling or refreshing SDC after pipeline changes.

**Caveat.** Single-item DPLA IDs and NARA hub-level targets do not re-stage `sdc.json` — the former uses `resolve-dpla-ids` (which stages `dpla-map.json` only), the latter uses `get-ids-nara`. For these, `sdc-sync` replays the existing sidecar. The launcher prints a warning to stderr when this combination is detected.

### `maintain=true`

In-place upkeep of files **already** on Commons for a hub or institution — re-links ID-drifted files and re-syncs SDC / legacy templates, creating nothing new. It is anchored on the live Commons category (`sdc-sync --cat … --maintain`), not an ES id-list, so every already-uploaded file is reconciled; the uploader runs `--no-create` so no net-new File page is ever created. Two sub-flags pick the route:

- Default (**hash**) route: a `get-ids-es --maintain --skip-media-filter` staging scan → `downloader --maintain` → `uploader --no-create` → `sdc-sync --cat … --maintain`. The download + `--no-create` pass repairs content drift for the media-bearing subset; the `--cat` sync reconciles SDC for the whole category.
- `--lite`: skips the download and upload passes — it stages sidecars, then re-links + SDC-syncs via `sdc-sync --cat … --maintain --from-s3` only.
- `--count-only` (forces `--lite`): a read-only pre-flight that only resolves how each file would re-link and writes nothing.

Single-DPLA-ID and collection-scoped maintain targets have no whole category to walk, so they keep the id-list-anchored route (`sdc-sync --partner --ids-file`) instead of `--cat`.

---

## The Tracker

`ingest_wikimedia/tracker.py` is a tiny dict-backed counter, keyed by `Result` enum values. Its `__str__` produces a `COUNTS:` block emitting only keys with `value > 0` so the per-phase output stays clean.

```python
COUNTS:
UPLOADED: 1244
SKIPPED: 312
FAILED: 3
BYTES: 4831234567
```

End-of-phase emission in each phase's `main()`:

```python
logging.info("\n" + str(tracker))
notify_*_complete(tracker, partner_label, elapsed, dry_run)
```

The SDC phase resets the tracker at the start of `_run_partner_mode` so per-partner counts don't accumulate across invocations in the same process. Its completion call differs from the other phases': `notify_sdc_complete(tracker, partner_label, elapsed_seconds, dry_run=False, workers=1, maintain=False)` additionally takes `workers` and `maintain` on top of the usual `dry_run`. The `workers` value lets the Slack summary report the average per-worker slot wait (`SDC_SLOT_WAIT_SECONDS / workers`).

## Phase-start notifications

`notify_phase_start()` fires from inside each phase's `main()` (the `get-ids-es`, `get-ids-nara`, `downloader`, `uploader`, `sdc-sync` entry points). Suppressed when `WIKIMEDIA_SINGLE_ITEM=1` so single-item runs don't spam channel with per-phase chatter.

| Phase | Emoji | Message |
|---|---|---|
| `get-ids-es` | 🔍 | `wikimedia-<label>: starting get-ids-es` |
| `downloader` | ⬇ | `wikimedia-<label>: starting downloader` |
| `uploader` | ⬆ | `wikimedia-<label>: starting uploader` |
| `sdc-sync` | 🔗 | `wikimedia-<label>: starting sdc-sync` |
{% endraw %}
