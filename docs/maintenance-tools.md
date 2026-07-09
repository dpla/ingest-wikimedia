{% raw %}
# Maintenance Tools

These tools sit alongside the four pipeline phases but are not part of the Slack-launched pipeline chain. They are invoked manually by operators (usually via SSM on EC2) for specific maintenance jobs.

| Tool | Purpose |
|---|---|
| [`verify-item`](#verify-item) | End-to-end verification of one DPLA item against S3 + Commons |
| [`get-incomplete-items`](#get-incomplete-items) | Detect items whose download phase didn't complete |
| [`resolve-dpla-ids`](#resolve-dpla-ids) | Single-item eligibility check + staging for Slack-driven launches |
| [`sign`](#sign) | Backfill SHA1 metadata on legacy S3 objects |
| [`retirer`](#retirer) | "Retire" ineligible items by zeroing their S3 bytes |
| [`nuke`](#nuke) | Hard-delete S3 contents for a list of DPLA IDs |
| [`get-ids-retry`](#get-ids-retry) | Parse logs to build per-hub retry CSVs |
| [`fix-unknown-categories`](#fix-unknown-categories) | Backfill maintenance categories after partner registry changes |
| [`sdc-sync --migrate-legacy`](#sdc-sync---migrate-legacy) | One-time migration of legacy `{{Artwork}}` files to `{{DPLA metadata}}` |

---

## `verify-item`

Source: `tools/verify_item.py`.

End-to-end consistency checker for one DPLA item. Given `<dpla_id> <partner>`:

1. Reads each ordinal in the item's `file-list.txt` from S3.
2. For each S3 object: reads `sha1`, size, and content-type from user metadata.
3. Calls `compute_ordinal_exts_and_page_labels` (the same helper the uploader uses, so titles align exactly) to predict the Commons file title for each ordinal.
4. Batches `action=query&prop=imageinfo|info&iiprop=sha1` requests against the Commons API (50 titles per batch).
5. Classifies each ordinal as `CORRECT`, `MISMATCH`, `REDIRECT`, `MISSING`, or `SKIPPED`.
6. Writes the full per-ordinal report to `/tmp/verify_<dpla_id>.json` for triage.

Exit code 0 only when every uploadable ordinal is `CORRECT`. Used as a deploy-verification probe — runs after a fixed sdc-sync or uploader bug to confirm a known-good item is unaffected.

---

## `get-incomplete-items`

Source: `tools/get_incomplete_items.py`.

Walks `s3://dpla-wikimedia/<partner>/images/` looking for items whose downloader phase didn't complete:

1. For each `file-list.txt`, count the URL lines.
2. Count the media-file objects in the same folder (excluding `file-list.txt`, `dpla-map.json`, `iiif.json`).
3. If the two counts differ, print the DPLA ID.

Output is one DPLA ID per line on stdout. Pipe it back into the downloader for a targeted re-download:

```bash
get-incomplete-items <partner> > <partner>-incomplete.csv
downloader <partner>-incomplete.csv <partner>
```

---

## `resolve-dpla-ids`

Source: `tools/resolve_dpla_ids.py`.

Used by `wikimedia_launch.py` for single-item Slack launches (`/wikimedia-upload <dpla-id>`). Takes one or more DPLA IDs on the CLI; does ONE batched ES `terms` query; then per-ID applies the eligibility filter:

- Banlist check against `dpla-id-banlist.txt`.
- `rightsCategory == "Unlimited Re-Use"`.
- Has-media (`mediaMaster` / `iiifManifest` / CONTENTdm IIIF URL).
- Hub resolves to a known slug.
- Institution is upload-eligible per `institutions_v2.json`.

For eligible items: stages the ES document to S3 as `dpla-map.json` with `_staged_by_get_ids_es: true`.

Output (one line per ID, parsed by `wikimedia_launch.py`):

```text
<id> HUB=<slug>            # eligible
<id> NOT_FOUND
<id> INELIGIBLE:<reason>
<id> ERROR:<msg>
```

Operators can also call `resolve-dpla-ids` by hand to check eligibility without launching anything.

---

## `sign`

Source: `tools/sign.py`.

Walks `s3://dpla-wikimedia/<partner>/images/` looking for objects with no `CHECKSUM` (SHA1) in user metadata. For each:

1. Downloads the object.
2. Computes SHA1 + libmagic-detected content-type.
3. Updates the S3 object in place via `copy_from(MetadataDirective="REPLACE", ContentType=..., Metadata=...)`.

Idempotent — skips objects whose `CHECKSUM` is already set.

Used to backfill SHA1 metadata on items uploaded to S3 before the SHA1-on-write convention was established. The pipeline today writes SHA1 on every download (`downloader.py::upload_file_to_s3`), so `sign` is only needed for legacy backlog.

---

## `retirer`

Source: `tools/retirer.py`.

"Retires" S3 objects by replacing their body with empty bytes while preserving user metadata. Does NOT delete anything on Commons. The S3 bytes are zeroed (so the bucket stops paying for them) but the metadata footprint stays (so a future audit can tell "this WAS a file at this title with this SHA1").

A file is retired when ANY of these is true:

- The DPLA item is no longer wiki-eligible (banlist, rights change, institution flag toggled).
- The S3 object has no `CHECKSUM` user-metadata.
- The content-type is missing or unrecognised.
- `mimetypes.guess_extension(mime)` returns nothing usable.
- BOTH a file with this SHA1 exists on Commons AND a Commons page exists at the expected title (i.e. successful upload — we don't need the S3 master anymore).

Entry: `python -m tools.retirer <partner> [--dry-run]`. Always run with `--dry-run` first to confirm the candidate list.

---

## `nuke`

Source: `tools/nuke.py`.

41 lines. Takes an IDs file and a partner; for each DPLA ID runs:

```bash
aws s3 rm s3://dpla-wikimedia/<partner>/images/<a>/<b>/<c>/<d>/<dpla_id>/ --recursive
```

(Path built via `S3Client.get_item_s3_path`.) Used for hard purges — typically after a partner is removed from `institutions_v2.json` entirely, or a contractual takedown. `--dry-run` appends `--dryrun` to the underlying `aws s3 rm` call so the deletion is previewed without taking effect.

Distinct from `retirer`: nuke hard-deletes everything (sidecars, media, metadata); retirer only zeroes the body and only on ineligibility / completion criteria.

---

## `get-ids-retry`

Source: `tools/get_ids_retry.py`. Invoked by `scripts/wikimedia_retry.py` (which is triggered by `/wikimedia-upload retry`).

Parses recent upload + download + SDC logs and classifies failures into three retry types:

### `upload-retry`

Matches against `UPLOAD_TRANSIENT_ERRORS`:

- `lockmanager-fail-conflict`
- `lockmanager-fail-svr-acquire`
- `stashfailed: Could not acquire lock`
- `stashfailed: Server failed to publish temporary file`
- `uploadstash-exception`
- `backend-fail-internal`
- `File linked to another page`
- `ArticleExistsConflictError`
- `fileexists-shared-forbidden`

For these, S3 is already complete — just re-run the uploader.

### `download-retry`

Matches `Failed downloading <url>` patterns, excluding the empty-URL IIIF-parser bug fixed in PR #180.

For these, the S3 bytes are absent — re-run the downloader with `--max-age-days 1` to force a re-fetch of any partial / stale state.

### `sdc-retry`

Matches `-- Ordinal N (Mxxx) for <DPLA-ID>: SDC sync failed; skipping ordinal.` markers in `*-sdc.log`, then classifies the trailing traceback against `SDC_TRANSIENT_ERRORS`:

- `MaxlagTimeoutError`, `maxlag`, `readonly`, `ratelimited` — Wikibase replica lag, read-only mode, rate limiting
- `ServerError` — HTTP 5xx from MediaWiki / Wikibase
- `ReadTimeoutError`, `ReadTimeout`, `ConnectTimeoutError`, `EndpointConnectionError`, `ChunkedEncodingError`, `ProtocolError`, `ConnectionError` — botocore / requests / urllib3 transients
- `internal_api_error_DBQueryError`, `internal_api_error_DBConnectionError` — MediaWiki API DB blips
- `editconflict`, `failed-save` — race / save retry storm
- `SlowDown`, `RequestTimeout`, `ServiceUnavailable`, `InternalError` — S3 transients during sidecar reads

Structural failures (`invalid-claim`, `permissiondenied`, `no-such-entity`, code bugs like `KeyError` / `AttributeError`) are deliberately excluded — re-running won't help, so they're left to surface in the next regular sync where an operator will catch them and fix the underlying issue.

For SDC retries, just re-run `sdc-sync --partner <slug> --ids-file <csv>`. The SDC sync is idempotent so the IDs that were already done re-run as no-ops; only the ones that hit transient failures actually write.

### Output

One CSV per partner per type to `--output-dir` (default `<INGEST_WIKIMEDIA_DIR>/retry/`). Logs are processed oldest-first per partner so a later clean run can supersede an earlier failure for the same item.

The retry script merges upload-retry + download-retry CSVs per hub into one combined `<slug>-retry-<days>d-combined.csv` and runs the uploader once on the combined list, so an operator sees one "Retry Complete" summary per hub instead of one-per-type. If an SDC-retry CSV also exists for the same hub, `sdc-sync` runs as a final step after the uploader so it sees the freshly-refreshed `upload-result.json` sidecars. An SDC-only retry (no upload / download failures) skips the uploader and downloader entirely.

---

## `fix-unknown-categories`

Source: `tools/fix_unknown_categories.py`.

Drains `Category:Media contributed by the Digital Public Library of America with unknown institution` one institution at a time — files that landed there because their institution had no Commons category page yet. For each remaining file it reads the institution and hub Q-IDs already present in the file's own wikitext (`{{Institution|…|wikidata=Q…}}` / `{{DPLA|…|hub=Q…}}`), ensures that institution's Commons category-page infrastructure exists (via `CategoryEnsurer`), then touches every Commons file for that institution so the Wikidata Infobox template re-evaluates and moves them out of the unknown-institution category. Files whose Q-IDs can't be parsed are recorded and skipped so the loop doesn't retry them forever.

Used after new institutions are added to `institutions_v2.json` (or existing ones first get a category page), to clear the maintenance category that accumulated while those institutions had no category to sort into.

---

## `sdc-sync --migrate-legacy`

Source: `tools/sdc_sync.py` (`_run_legacy_migration_mode`).

A one-time bulk migration mode of the same `sdc-sync` entrypoint that runs Phase 4. Instead of running an SDC sync, `--migrate-legacy` walks the partner's files and migrates any still wrapped in the legacy `{{Artwork}}` (or `{{Information}}` / `{{Photograph}}`) template to `{{DPLA metadata}}`. For each file it:

1. Walks the file's revision history to separate DPLA-bot values (overwrite-safe) from community contributions.
2. Imports the community values as SDC statements, referenced with `P887`→`Q131783016` (inferred from) + `P4656` (imported-from permalink).
3. Rewrites the wikitext from the legacy wrapper to `{{DPLA metadata}}`.

```bash
sdc-sync --partner <partner> --migrate-legacy
```

**In-pipeline vs. explicit mode.** The same legacy migration — *plus* the redundant-param strip — also runs automatically as the wikitext-cleanup step of **every** partner-mode SDC sync (i.e. on every Phase 4 ordinal; see [operations](operations.md#phase-4-sdc-sync-sdc-sync)). The dispatch is per-file: legacy-wrapped files get migrated, `{{DPLA metadata}}` files get stripped, anything else is skipped. `--migrate-legacy` is the explicit one-time mode that does *only* the migration walk across a whole partner, for clearing a backlog of legacy files in one pass rather than waiting for each to be touched by a regular sync.

### `--workers` / `--workers-budget`

`sdc-sync` accepts two parallelism flags, normally set by the launcher / workflow (defaults `6` / `24`) but settable by hand for a manual partner sync:

- `--workers N` — number of worker processes for partner-mode SDC sync. Argparse default is **`1`** (single-process, standalone-safe); the launcher and workflow pass **`6`**. `N>1` dispatches per-DPLA-item work to a multiprocessing pool, each worker holding its own pywikibot session. Items are independent (every ordinal has a unique M-id), so workers never write to the same MediaInfo entity.
- `--workers-budget N` — box-wide cap on concurrent Commons-writing slots shared across **all** sdc-sync sessions on the host (and the uploader). Argparse default is **`0`** (unlimited / budget disabled); the launcher and workflow pass **`24`** (production runs `~16`+ to keep 6+ concurrent sessions from oversubscribing Commons' parser pool). The single-purpose manual modes (`--list` / `--file` / `--cat`) do not participate in the budget. See [Worker-slot budget](operations.md#worker-slot-budget) for the slot mechanics and ops inspection.

### `--no-normalize-wikitext`

The post-SDC wikitext cleanup (legacy migration + redundant-param strip) is **on by default** on every partner sync. Pass `--no-normalize-wikitext` to disable the strip for diagnostic runs that need the pre-cleanup wikitext left intact.

---

## Where these tools run

All maintenance tools are typically invoked on EC2 via SSM, from the partner's working directory:

```bash
# Example: dry-run retire for bpl
aws ssm send-command \
  --instance-ids i-033eff6c8c168f999 \
  --document-name AWS-RunShellScript \
  --parameters 'commands=["sudo -u ec2-user bash -lc \"cd /home/ec2-user/ingest-wikimedia/bpl && uv run python -m tools.retirer bpl --dry-run\""]'
```

No Slack slash command exists for any of these — they're considered "operator-level" maintenance, and the operational friction of going through SSM is intentional.
{% endraw %}
