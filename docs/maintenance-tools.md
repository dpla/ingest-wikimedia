# Maintenance Tools

These tools sit alongside the four pipeline phases but are not part of the Slack-launched pipeline chain. They are invoked manually by operators (usually via SSM on EC2) for specific maintenance jobs.

| Tool | Purpose |
|---|---|
| [`verify-item`](#verify-item) | End-to-end verification of one DPLA item against S3 + Commons |
| [`get-incomplete-items`](#get-incomplete-items) | Detect items whose download phase didn't complete |
| [`resolve-dpla-ids`](#resolve-dpla-ids) | Single-item eligibility check + staging for Slack-driven launches |
| [`sign`](#sign) | Backfill SHA1 metadata on legacy S3 objects |
| [`remimer`](#remimer) | Re-detect and fix wrong content-types on S3 objects |
| [`retirer`](#retirer) | "Retire" ineligible items by zeroing their S3 bytes |
| [`nuke`](#nuke) | Hard-delete S3 contents for a list of DPLA IDs |
| [`get-ids-retry`](#get-ids-retry) | Parse logs to build per-hub retry CSVs |
| [`fix-unknown-categories`](#fix-unknown-categories) | Backfill maintenance categories after partner registry changes |

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

```
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

## `remimer`

Source: `tools/remimer.py`.

Walks `s3://dpla-wikimedia/<partner>/images/` looking for objects whose stored content-type is `binary/octet-stream` or `application/octet-stream`. For each:

1. Downloads to a temp file.
2. Re-detects MIME via `local_fs.get_content_type()` (libmagic).
3. Writes the detected content-type back to S3 via `copy_object(MetadataDirective="REPLACE", ContentType=detected, CopySource=...)`.

Used when the original downloader couldn't determine MIME (typically because the source server returned `application/octet-stream`). Once libmagic identifies the actual type, the uploader can pick a sensible file extension.

The companion `MetadataDirective="REPLACE"` rule applies — `Metadata=dict(s3_object.metadata)` is explicitly passed to preserve the `CHECKSUM` field across the copy. See the lesson at `~/.claude/lessons.md` under "AWS S3 `copy_object` with `MetadataDirective="REPLACE"`".

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

42 lines. Takes an IDs file and a partner; for each DPLA ID runs:

```bash
aws s3 rm s3://dpla-wikimedia/<partner>/images/<a>/<b>/<c>/<d>/<dpla_id>/ --recursive
```

(Path built via `S3Client.get_item_s3_path`.) Used for hard purges — typically after a partner is removed from `institutions_v2.json` entirely, or a contractual takedown. `--dry-run` appends `--dryrun` to the underlying `aws s3 rm` call so the deletion is previewed without taking effect.

Distinct from `retirer`: nuke hard-deletes everything (sidecars, media, metadata); retirer only zeroes the body and only on ineligibility / completion criteria.

---

## `get-ids-retry`

Source: `tools/get_ids_retry.py`. Invoked by `scripts/wikimedia_retry.py` (which is triggered by `/wikimedia-upload retry`).

Parses recent upload + download logs and classifies failures into two retry types:

### `upload-retry`

Matches against `UPLOAD_TRANSIENT_ERRORS`:

- `lockmanager-fail-conflict`
- `stashfailed: Could not acquire lock`
- `uploadstash-exception`
- `backend-fail-internal`
- `File linked to another page`
- `ArticleExistsConflictError`
- `fileexists-shared-forbidden`

For these, S3 is already complete — just re-run the uploader.

### `download-retry`

Matches `Failed downloading <url>` patterns, excluding the empty-URL IIIF-parser bug fixed in PR #180.

For these, the S3 bytes are absent — re-run the downloader with `--max-age-days 1` to force a re-fetch of any partial / stale state.

### Output

One CSV per partner per type to `--output-dir` (default `<INGEST_WIKIMEDIA_DIR>/retry/`). Logs are processed oldest-first per partner so a later clean run can supersede an earlier failure for the same item.

The retry script merges upload-retry + download-retry CSVs per hub into one combined `<slug>-retry-<days>d-combined.csv` and runs the uploader once on the combined list, so an operator sees one "Retry Complete" summary per hub instead of one-per-type.

---

## `fix-unknown-categories`

Source: `tools/fix_unknown_categories.py`.

Walks Commons looking for files in `Category:Media contributed by the Digital Public Library of America with unknown partner` or `... with unknown institution` — files where the Lua module couldn't resolve a hub or institution category from SDC. For each, attempts to re-resolve against current `institutions_v2.json` mappings and writes the corrected categories back via wikitext edit.

Used after a major `institutions_v2.json` change (new hub added, institution moved between hubs, partner names normalised) to clean up the maintenance categories that accumulated under the old mappings.

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
