{% raw %}
# S3 Sidecars

The five pipeline phases communicate via JSON / text sidecar files, plus the media bytes themselves. Every per-item sidecar lives in S3 under one prefix:

```text
s3://dpla-wikimedia/<partner>/images/<a>/<b>/<c>/<d>/<dpla-id>/
```

where `<a><b><c><d>` are the first four lowercase hex characters of the DPLA ID. This sharding keeps any one S3 list-objects call to ≤ 4 K items in practice.

Path construction lives in `ingest_wikimedia/s3.py`:

```python
S3_BUCKET = "dpla-wikimedia"

def get_item_s3_path(dpla_id, filename, partner):
    return (
        f"{partner}/images/{dpla_id[0]}/{dpla_id[1]}/"
        f"{dpla_id[2]}/{dpla_id[3]}/{dpla_id}/{filename}"
    )
```

One special slug-to-directory mapping: `si` (Smithsonian) maps to `smithsonian/` for the local EC2 working directory, but the S3 prefix is still `si/`.

One sidecar is the exception to the S3 rule: the per-partner `deferred-drain.json` queue lives on the EC2 working disk, not S3 — see [`deferred-drain.json`](#deferred-drainjson-local-disk-not-s3) below.

## Inventory

| File | Writer | Readers | Content type |
|---|---|---|---|
| `dpla-map.json` | `get-ids-es` (and `get-ids-nara`, `resolve-dpla-ids`) | `downloader`, `uploader`, `get-ids-es` Phase 3, `sdc-sync` legacy mode | `application/json` |
| `sdc.json` | `get-ids-es` Phase 3 (and `get-ids-nara` Phase 3) | `sdc-sync` partner mode | `application/json` |
| `iiif.json` | `downloader` | (cache; no programmatic reader currently) | `application/json` |
| `file-list.txt` | `downloader` | `uploader`, `sdc-sync` | `text/plain` |
| `upload-result.json` | `uploader` | `sdc-sync` partner mode | `application/json` |
| `<N>_<dpla-id>` | `downloader` | `uploader` (1-indexed media bytes) | (sniffed MIME) |

---

## `dpla-map.json`

The full Elasticsearch `_source` document for the DPLA item, with two pipeline-added markers.

**Writers.**

- `tools/get_ids_es.py` — staged during Phase 1 enumeration.
- `tools/get_ids_nara.py` — same shape, NARA-only enumerator.
- `tools/resolve_dpla_ids.py` — for single-item launches.

**Readers.**

- `tools/downloader.py` — refuses to operate if the marker `_staged_by_get_ids_es` is absent.
- `tools/uploader.py` — reads `dataProvider`, `provider`, `sourceResource.title`, `sourceResource.identifier`, etc. to compose file titles + wikitext.
- `tools/get_ids_es.py` Phase 3 — re-reads each item's `dpla-map.json` to build `sdc.json`, rather than buffering all source docs in memory (keeps peak RAM at O(unique-subjects) rather than O(hub-size)).
- `tools/sdc_sync.py` legacy mode (`--from-s3 <partner>` path) — for runs not driven by `--partner`.

**Top-level keys** (DPLA's ES schema):

```text
id                            DPLA item ID (32-hex)
provider                      {name, exactMatch, ...}        — hub
dataProvider                  {name, exactMatch, ...}        — institution
rightsCategory                "Unlimited Re-Use" for eligible items
rights                        URI to license / rights statement
isShownAt                     URL to source-institution catalog page
sourceResource                {title, subject, date, identifier, description, format, language, collection}
mediaMaster                   [URL, ...] (when provider exposes direct media)
iiifManifest                  URL (when provider exposes IIIF; auto-derived for CONTENTdm)
_staged_by_get_ids_es         true                            — pipeline marker
ingestType / ingestDate / @context etc.                       — DPLA boilerplate
```

The `_staged_by_get_ids_es` marker is what the downloader uses to fence off legacy unstaged metadata.

**Lifecycle.** Rewritten on every `get-ids-es` / `get-ids-nara` run. The metadata is treated as ephemeral — there's no archival snapshot.

---

## `sdc.json`

The pre-computed Wikibase claim envelope for the item, ready to feed `wbeditentity`.

**Writer.** `get-ids-es` / `get-ids-nara` Phase 3 (via `staging.stage_sdc_to_s3` → `S3Client.write_item_file`).

**Reader.** `tools/sdc_sync.py::_run_partner_mode`.

**Shape:**

```json
{
  "claims": [
    {
      "mainsnak": {
        "property": "P760",
        "snaktype": "value",
        "datavalue": {"value": "06b558045c5fd4cc5dc697248272159a", "type": "string"}
      },
      "type": "statement",
      "rank": "normal",
      "qualifiers": {
        "P459": [<determination-method snak>]
      },
      "references": [
        {
          "snaks": {
            "P854": [<DPLA item URL>],
            "P123": [<DPLA Q2944483>],
            "P813": [<retrieval date>]
          }
        }
      ]
    },
    ...
  ]
}
```

Every DPLA-authored statement carries the same canonical envelope:

- **Qualifier `P459 = Q61848113`** (determination method = heuristic) on every claim, so the SDC phase can re-identify "this is our claim" later.
- **Reference triple** on every claim: `P854` (DPLA item URL) + `P123` (publisher = Q2944483, DPLA) + `P813` (retrieval date).

**Properties written** (depends on what the source document carries — see [sdc-sync.md](sdc-sync.md) for the full mapping):

P760 (DPLA ID), P1476 (title), P195 (collection), P170 (creator), P571 (date), P10358 (description), P9126 (maintained by), P7482 (described at source), P217 (local identifier), P275/P6216/P6426 (rights cluster), P4272 + P921 (subjects), plus NARA-only P1225 / P7228 / P6224.

**Per-ordinal qualifiers NOT in `sdc.json`:**

- **P304** (page number) — per-ordinal, materialised at SDC sync time in `process_one_from_sdc`. Only added when the ordinal belongs to a multi-file extension group within the item.
- **P2699** (URL = download URL) — per-ordinal, materialised at SDC sync time from `file-list.txt`. Different ordinals of the same item have different download URLs, so this can't live in the per-item `sdc.json`.

**Chunked claims for long values.** Strings or monolingual-text values exceeding Wikibase's per-string limit (1500) are split across multiple statements, each carrying a `P1545` (series ordinal) qualifier — `A1`, `A2`, ..., then `B1`, `B2`, ... for the second long value. The Lua module `Module:DPLA` on Commons reassembles them at render time. See [sdc-sync.md](sdc-sync.md#chunked-claims) for the full convention.

**Lifecycle.** Rewritten on every `get-ids-es` / `get-ids-nara` run. The SDC phase reads whichever sidecar is present, so sdc.json drift between runs is the failure mode `--single-id` was added to address (it re-stages sdc.json before running sdc-sync).

---

## `iiif.json`

The raw IIIF manifest JSON, cached so subsequent runs don't refetch.

**Writer.** `tools/downloader.py` — written only for IIIF-sourced items.

**Reader.** None currently — this is an operator/debug cache. The downloader walks the manifest in memory and writes the resulting URL list to `file-list.txt`.

**Lifecycle.** Written by every downloader run that resolves an IIIF item.

---

## `file-list.txt`

The ordered list of media URLs for the item, one URL per line.

**Writer.** `tools/downloader.py`, after resolving either `mediaMaster` or the IIIF manifest. URL ordering matches the ordinal numbering used by the upload step (1-indexed).

**Readers.**

- `tools/uploader.py` — iteration count, per-ordinal URL.
- `tools/sdc_sync.py::_run_partner_mode` — per-ordinal URL used as the value for the `P2699` (URL) qualifier on `P7482` (described at source).

**Lifecycle.** Rewritten by every downloader run. The uploader treats the cached version as authoritative subject to `--max-age-days` (when refreshed, both the cached IIIF manifest and the regenerated URL list are updated together).

---

## `upload-result.json`

Per-item upload verdict — the SDC phase's source of truth for which Commons pages exist for each ordinal.

**Writer.** `tools/uploader.py::Uploader._persist_upload_result` at every non-exception exit through `process_item`. Critically NOT written on the catch-all exception path — so a transient failure doesn't blow away a previously accurate verdict.

**Reader.** `tools/sdc_sync.py::_run_partner_mode`. Skips ordinals whose status is anything other than `UPLOADED` or `SKIPPED`.

**Shape:**

```json
{
  "run_at": "2026-06-08T13:24:55+00:00",
  "ordinals": {
    "1": {"status": "UPLOADED",    "title": "...", "pageid": 123456},
    "2": {"status": "SKIPPED",     "title": "...", "pageid": 234567},
    "3": {"status": "NOT_PRESENT"},
    "4": {"status": "INELIGIBLE"},
    "5": {"status": "FAILED",      "error": "..."},
    "6": {"status": "DEFERRED",    "title": "...", "pageid": null}
  }
}
```

**Status meanings:**

| Status | Meaning |
|---|---|
| `UPLOADED` | New file landed on Commons in this run |
| `SKIPPED` | File already on Commons (SHA1 match at the correct title) |
| `NOT_PRESENT` | No corresponding S3 object for this ordinal (downloader didn't write it) |
| `INELIGIBLE` | Pre-flight rejected (bad MIME, banned filetype, etc.) |
| `FAILED` | Catastrophic failure; see `error` field |
| `DEFERRED` | `{{duplicate}}`-tagging upload deferred because Category:Duplicate was at capacity; retried by the drain-deferred phase |

**Critical correctness detail.** `pageid` is `None` on failure (not `0`) so `sdc_sync.py`'s `if not pageid:` guard cleanly skips malformed entries — `M0` is not a valid Commons mediaid and would propagate downstream as a confusing pywikibot APIError.

**Lifecycle.** Overwritten on every uploader run. Dry-runs are no-ops for sidecar writes. The catch-all-exception exit path deliberately does *not* write the sidecar, so a transient infrastructure failure during a re-run doesn't erase a prior accurate verdict.

---

## `<ordinal>_<dpla-id>` (media bytes)

The actual media file, one S3 object per ordinal. 1-indexed.

**Writer.** `tools/downloader.py::Downloader.upload_file_to_s3`.

**Reader.** `tools/uploader.py::process_item` — downloads to a temp file, hashes, then uploads to Commons.

**User-metadata stamped at write:**

- `CHECKSUM` — SHA1 hex digest of the file. Used by:
  - `uploader.py` — duplicate detection (cross-S3 + against Commons via `allimages?sha1=`).
  - `uploader.py::collect_duplicate_source_sha1s` — to detect legitimate intra-item duplicates that shouldn't be treated as drift.
  - `retirer.py` — eligibility for retirement (file must have a valid SHA1).
- Content-Type — libmagic-sniffed MIME.

**Lifecycle.** Written once per ordinal. Re-downloaded only when `--max-age-days` triggers or `--overwrite` is set. Can be "retired" (body replaced with empty bytes, metadata preserved) via `tools/retirer.py` — see [maintenance-tools.md](maintenance-tools.md).

---

## `deferred-drain.json` (local disk, not S3)

The per-partner queue of DPLA IDs whose Case-2 hash-drift upload+tag was deferred because `Category:Duplicate` on Commons was at capacity. Unlike every other sidecar it lives on the EC2 working disk, not S3: `<INGEST_WIKI_ROOT>/<partner-dir>/deferred-drain.json` (e.g. `/home/ec2-user/ingest-wikimedia/smithsonian/deferred-drain.json` for `si`).

**Writer.** `tools/uploader.py` via `drain_sidecar.merge_sidecar` — appends (never overwrites) the deferred IDs at the end of the upload pass.

**Reader / remover.** `tools/drain_deferred.py` — the `drain-deferred` phase. Patient mode (the default, terminal phase of a batch) blocks on `DuplicateCategoryThrottle.wait_for_capacity`, polling `Category:Duplicate` every 300 s with no time budget until it drops below the resume threshold of 140; `--no-wait` runs a single opportunistic round and exits immediately if the category is at capacity. Each round removes its IDs from the sidecar, re-invokes `uploader` + `sdc-sync` on them as subprocesses, and the patient loop repeats until the sidecar is empty. A host-level `flock` at `/home/ec2-user/ingest-wikimedia/.drain-lock` serialises drains across partners.

**Shape:**

```json
{"partner": "<slug>", "deferred_dpla_ids": ["<dpla-id>", "..."]}
```

Atomic write (tempfile + `os.replace`); the file is removed when the queue empties.

**Throttle.** `ingest_wikimedia/dup_throttle.py::DuplicateCategoryThrottle` gates the hash-drift `{{duplicate}}` tag path: defer once `Category:Duplicate` reaches 190 members, resume draining once it falls below 140 (a 50-member hysteresis band).

---

## Helper APIs

`ingest_wikimedia/s3.py` provides these accessors:

```python
class S3Client:
    def write_item_metadata(self, partner, dpla_id, body_bytes)        # dpla-map.json
    def write_item_file(self, partner, dpla_id, data, filename, content_type) # arbitrary sidecar
    def write_file_list(self, partner, dpla_id, urls)                  # file-list.txt
    def write_iiif_manifest(self, partner, dpla_id, body_bytes)        # iiif.json
    def get_item_metadata(self, partner, dpla_id) -> str | None        # dpla-map.json
    def get_sdc_json(self, partner, dpla_id) -> str | None             # sdc.json
    def get_upload_result(self, partner, dpla_id) -> str | None        # upload-result.json
    def get_file_list(self, partner, dpla_id) -> list[str]             # file-list.txt
    def get_item_file(self, partner, dpla_id, file_name) -> str | None # arbitrary sidecar
    def get_metadata_files_for_partner(self, partner) -> Generator[dict] # walks all dpla-map.json under a partner
```

`get_metadata_files_for_partner()` is used by the `retirer` maintenance tool to iterate every item under a partner without holding the full ID list in memory. It yields each item's parsed `dpla-map.json` dict lazily. (`nuke` and `sign` walk the partner prefix with `bucket.objects.filter(Prefix=…)` directly rather than through this helper.)

---

## Sidecar handshakes across phases

The phases hand off via these sidecars:

| Handoff | Producer side | Consumer side |
|---|---|---|
| Enumeration → Download | `dpla-map.json` (`_staged_by_get_ids_es: true`) | downloader refuses to operate without the marker |
| Download → Upload | media bytes + `file-list.txt` + `dpla-map.json` | uploader iterates `file-list.txt`, reads media bytes, reads metadata |
| Upload → SDC | `upload-result.json` | sdc-sync skips ordinals whose status isn't `UPLOADED`/`SKIPPED` |
| Enumeration → SDC (separate pass) | `sdc.json` | sdc-sync diffs against Commons-side state |
| Download → SDC (separate pass) | `file-list.txt` | sdc-sync uses per-ordinal URL for `P2699` qualifier |
| Upload → Drain-deferred | `deferred-drain.json` (local disk) | drain-deferred re-invokes uploader + sdc-sync on the queued IDs once Category:Duplicate drains below 140 |

This decoupling is what lets `sdc-sync` re-run independently of the other phases (the `sdc_only` mode), and lets `refresh-only` re-download without disturbing already-uploaded files.
{% endraw %}
