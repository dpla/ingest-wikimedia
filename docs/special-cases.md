{% raw %}
# Special Cases: Duplicate Detection, File Renames, CommonsDelinker

The uploader carries the bulk of the pipeline's "what if Commons-side state doesn't match what I'm about to upload?" logic. This document covers:

1. [Title generation](#title-generation)
2. [Duplicate-detection decision tree](#duplicate-detection-decision-tree)
3. [Hash drift resolution](#hash-drift-resolution)
4. [Redirect handling](#redirect-handling)
5. [CommonsDelinker integration](#commonsdelinker-integration)
6. [Orphan audit](#orphan-audit)
7. [Wikimedia error codes](#wikimedia-error-codes)
8. [Content hubs vs. service hubs](#content-hubs-vs-service-hubs)
9. [Per-ordinal status and pageid rescue](#per-ordinal-status-and-pageid-rescue)
10. [Limitations](#limitations)

## Title generation

`get_page_title()` in `ingest_wikimedia/wikimedia.py` is the single source of truth for Commons file titles. The final form is:

```text
<sanitized-source-title> - DPLA - <dpla-id>[ (page N)]<extension>
```

The `(page N)` suffix is added by `compute_ordinal_exts_and_page_labels()` only when the same extension appears on more than one ordinal of the item. Single-extension multipage items get `(page 1)`, `(page 2)`, etc.; an item with one JPG and one PDF gets no page suffix on either (separate "series" per extension).

### Sanitisation rules

Applied in order to the first 181 chars of the source title (right-trimmed first):

| Rule | Why |
|---|---|
| `& → +` and `= → -` (when both present) | Commons' title blacklist rule fires only when both characters are present together — typical of unencoded query-string titles |
| `: → -` | MediaWiki strips mid-title colons from File-namespace titles (5 NARA items orphaned in May 2026 by an earlier version that left them alone) |
| `/ → -` | MediaWiki rejects subpage-parsed names on `UploadBase::isValidName` (4,114 NARA items hit `imageinvalidfilename` during Case-3 moves before this was added; dates like `1/5/1966` were the culprit) |
| `\ → -`, `< → -`, `> → -` | Outside `Title::legalChars()` |
| `'' → "` | Titleblacklist double-apostrophe rule |
| `[ → (`, `] → )`, `{ → (`, `} → )`, `# → -`, `\| → -` | Forbidden in MediaWiki page names; `\|` additionally breaks Commons' file-extension detection |
| `� → '` | Unicode replacement char → right single quote |
| `_ → space` | MediaWiki treats `_` as a space in titles; an unmatched underscore form reads as false Case-2 drift |
| `replace_invisible()` | Strip zero-width / bidi characters that Commons rejects |
| whitespace-run collapse + trim | MediaWiki collapses any whitespace run to a single space and trims edges in stored titles (`" ".join(s.split())`) |
| first-char uppercase | `File:` is a capitalised namespace (`Title::capitalize`); slice-and-upper preserves internal case |

There is no separate "title blacklist" file the uploader reads; the rules are encoded as the substitution table above. Each was added in response to a specific incident: the colon and slash substitutions trace back to NARA imports in May 2026 where the prior code silently orphaned items whose source titles contained mid-title `:` or `/`; the `&...=` together-only rule replaced an earlier unconditional `&`-replacer that was too aggressive (Commons' actual blacklist only matches when both characters appear together, typical of unencoded query-string titles). The pattern in all cases: match the *actual* MediaWiki rejection rule (which usually involves multiple characters together), not any single character in isolation, so the substitution doesn't damage benign titles.

## Duplicate-detection decision tree

The uploader detects "this content already exists on Commons" along two axes: SHA1 hash, and Commons file title. Three primitives in `ingest_wikimedia/wikimedia.py`:

```python
wiki_file_exists(site, sha1)                          # bool
find_file_by_hash(site, sha1, preferred_title=None)   # FilePage or None
collect_duplicate_source_sha1s(s3, dpla_id, partner)  # set of sha1s appearing > 1× in this item
```

`find_file_by_hash` calls Commons' `allimages?sha1=...` API; if any result lives at the `preferred_title`, returns it (same-title fast path); otherwise, when more than one file shares the SHA1, it returns the **earliest existing upload** (the file whose `oldest_file_info.timestamp` is earliest), matching the SHA1-uniqueness redesign's canonical = earliest-upload rule — falling back to the API's first (alphabetical) result only if no upload timestamp can be read. `collect_duplicate_source_sha1s` walks each S3 ordinal's user metadata and returns SHA1s that appear at two-or-more positions in the same DPLA item — used to short-circuit drift detection for legitimate intra-item duplicates.

### The hot path (`process_file`)

For each ordinal:

```text
sha1 = read from S3 user-metadata 'CHECKSUM'
page_title = get_page_title(...)
existing = find_file_by_hash(site, sha1, preferred_title=page_title)

if existing is None:
    upload to page_title                     # net-new file — the ONLY path that uploads
elif existing.title == page_title:
    SKIPPED                                  # exact match, our content is already there
elif _is_community_file(existing):
    HAND_FIX (reason=community_file)         # never touch a community upload
elif is_dup_sha1_sibling_at_expected_title(...):
    MERGE_AND_REDIRECT (within_item=True)    # legitimate intra-item duplicate
else:
    drift_action = _resolve_hash_drift(...)  # MOVED / MERGE_AND_REDIRECT / HAND_FIX / ALREADY_CORRECT
```

Under the SHA1-uniqueness constraint (PR C+D) **no two Commons files may share a SHA1**: once `find_file_by_hash` returns a hit, the uploader NEVER uploads a second byte-identical copy — it resolves to exactly one non-upload outcome and returns. (The community / sibling / drift branches all run on the live, non-`dry_run` path.)

## Hash drift resolution

`_resolve_hash_drift()` is invoked when our SHA1 already exists on Commons but at a different title than we want. Under the SHA1-uniqueness constraint (PR C+D) **no two Commons files may share a SHA1**, so none of its outcomes uploads a second byte-identical copy — every one is a rename, a merge-and-redirect, or a hand-off. It returns a `DriftResolution` str-enum: `MOVED`, `MERGE_AND_REDIRECT`, `HAND_FIX`, or `ALREADY_CORRECT`.

Two of the SHA1-already-present outcomes are decided by `process_file` *before* it reaches `_resolve_hash_drift`:

- **Community file → `HAND_FIX` (`community_file`).** If the matched file is a community upload (see [Community files](#community-files)), the uploader never renames, merges onto, redirects, or migrates it — it records a `community_file` hand-fix and stops.
- **Within-item sibling → `MERGE_AND_REDIRECT`.** If `is_dup_sha1_sibling_at_expected_title` shows our SHA1 already sits at one of THIS item's own current asset positions, that sibling is the canonical file: merge this ordinal's SDC onto it (stamping the page number) and redirect our intended title to it.

Everything else falls through to `_resolve_hash_drift`, whose outcomes are:

### `MOVED` — rename into place

The intended title is empty, or is a redirect pointing at our OWN file. `_move_to_correct_title` renames the existing file to the canonical title via `existing_file.move(..., noredirect=False)` (leaving a redirect at the old title) and posts a CommonsDelinker request (gated on inbound usage; suppressed for sibling slots — see [CommonsDelinker integration](#commonsdelinker-integration)). The move preserves the page's content inherently: its description is deliberately left untouched, because distinguishing community edits from drifted DPLA-bot fields needs the revision-history provenance walk that the post-SDC `sdc-sync` cleanup (`_post_sdc_cleanup_for_page`) runs on the moved file later in the same pipeline. The caller records the ordinal `UPLOADED` (the file now lives at the canonical title; MediaWiki preserves `pageid` across the move).

### `MERGE_AND_REDIRECT` — source duplication

Legitimate source duplication: either the existing title encodes a *different* DPLA ID that still resolves in `api.dp.la` (two live items genuinely share a hash — cross-item / cross-institution), or our SHA1 sits at one of THIS item's own asset positions (within-item). Rather than a second byte-identical file:

1. Merge THIS item's structured data onto the earliest existing (canonical) file's MediaInfo entity via `sdc_sync.merge_item_onto_canonical` — add-only (`reconcile=False`), so the canonical owner's and any other contributor's statements are preserved. Within-item duplication stamps the ordinal's page number as a **P304** qualifier; cross-item adds this item's DPLA ID as an **additional P760** value and stamps no page number.
2. Leave a `#REDIRECT [[File:<canonical>]]` at our intended title (`_create_redirect_to_canonical`, idempotent; it refuses to clobber a real file that already sits at the intended title).

The caller records the ordinal `MERGED`. The result carries the canonical file's `title`/`pageid`, but `MERGED` is deliberately **not** in the SDC-sync eligibility set — the SDC was merged inline, so re-targeting our redirect title would double-write.

### `HAND_FIX` — the bot can't safely resolve it

No upload and nothing renamed; a descriptive record is appended to the per-partner `hand-fix.jsonl` sidecar (see [sidecars.md](sidecars.md#hand-fixjsonl-local-disk-not-s3)) for a human, and the ordinal is counted `Result.UPLOAD_HAND_FIX`. Two reasons:

- **`rename_blocked`** — our SHA1 is at a wrong title and the intended title is occupied by a **different** (bad-hash) real file, or by a redirect to some *third* file, or the colliding file's DPLA ID could not be verified (a non-404 API error). The bot can neither upload a duplicate nor clobber the occupant, and it must not pick a winner between two distinct files.
- **`community_file`** — the matched file is a community upload (decided in `process_file`, above).

### Cross-item collision: the 404-vs-live distinction

When the existing (wrong-title) file encodes a *different* DPLA ID (extracted via `extract_dpla_id_from_commons_title`, regex `- DPLA - ([0-9a-f]{32})`), the outcome hinges on whether that other ID still resolves:

- **Other item still valid in `api.dp.la`** — two genuinely distinct live items share a hash → `MERGE_AND_REDIRECT` (centralize on their canonical file, redirect ours).
- **404 on the colliding DPLA ID** — its DPLA-side anchor is gone, so the existing file is an orphan: fall through to the empty-intended / redirect / occupied logic and reclaim our title (a `MOVED`, or a `HAND_FIX` if the intended title turns out to be occupied by a different file).
- **Any *other* error (network timeout, 5xx, JSON parse failure)** — none of these mean "the old ID is gone," so we route to `HAND_FIX` rather than act destructively on a transient blip.

The 404-vs-other distinction is read off `ex.response.status_code`.

### `ALREADY_CORRECT` — normalized identity

The file the SHA1 lookup returned IS the file at the intended title once MediaWiki's title normalisation (whitespace-run collapse, `_` → space, first-letter uppercase) is applied — typically a truncation/underscore artefact of `get_page_title`. There is no drift to resolve; falling through to the other branches here would attempt a move-to-self, or a redirect/hand-fix against the same page. The guard compares `_canonicalize_commons_title` on both sides; the caller records SKIPPED against the pywikibot-normalised title.

### Community files

`_is_community_file` treats a file as community-owned — off-limits to automated rename / SDC-merge / redirect / template-migration — only when **both** signals say it isn't ours: (1) its title lacks the DPLA/NARA naming shape (`- DPLA - ` / `- NARA - `), **and** (2) its original uploader (`first_uploader`) is neither `DPLA bot` nor `US National Archives bot`. The AND is deliberate: a bot upload with a malformed title is still ours to fix, and a DPLA-shaped title uploaded from a personal account is still ours to act on. When the uploader can't be read, the file is treated as community (hands-off).

## Redirect handling

The other axis is: what if our intended title is itself a redirect? `process_file` checks `wiki_file_page.isRedirectPage()` early. Direct upload onto a redirect fails with `fileexists-shared-forbidden`. Two recovery strategies:

### `_resolve_redirect_move`

Used when the redirect's target carries the same DPLA ID at the same logical page (not a same-item different-page relic — `is_same_item_redirect_relic` filters this). Moves the redirect target to the intended title via `redirect_target.move(..., noredirect=False)` and posts a CommonsDelinker request.

### `_resolve_redirect_overwrite`

Used otherwise. Replaces the redirect text with fresh wikitext, optionally merging preserved license / category / `{{Image extracted|...}}` / assessment-class templates (`{{Picture of the day}}`, `{{Featured picture}}`, etc.) from the prior wikitext via `merge_preserved_wikitext`. The new `{{DPLA metadata}}` block is always authoritative for the description; preserved blocks are appended.

## CommonsDelinker integration

`User:CommonsDelinker/commands/filemovers` on Commons is a community-operated bot's instruction queue. The pipeline appends `{{universal replace|<old>|<new>|reason=...}}` lines to it; CommonsDelinker reads the page out-of-band, rewrites every link to `[[File:<old>]]` on every Wikimedia wiki to point at `[[File:<new>]]`, and deletes the obsolete redirect.

**One function, one API.** `post_commonsdelinker_request(site, old_filename, new_filename, check_usage=True)` in `ingest_wikimedia/wikimedia.py` is the only call site. It uses MediaWiki's `appendtext` API parameter (not read-modify-write) for two reasons:

1. The page is currently ~230 KB and growing. Loading it via `page.text` then re-saving the whole thing would burn ~500 KB per request — contributed to a recent OOM episode.
2. `page.text` reads from a replica that can lag the primary by seconds; under burst load (a hub move-storm can post hundreds of requests in a minute) read-modify-write produced self-inflicted `editconflict` rejections. `appendtext` is server-side concatenation against the primary, no GET, atomic.

**Gated on actual inbound usage.** The function posts a request only if the old title actually has links to relink. `file_has_inbound_usage(site, filename)` queries `globalusage|fileusage` for the title and returns `True` if the file is used on another wiki or by *another* local Commons page. It **excludes the file's own description page** from `fileusage`: a DPLA file page renders `{{Artwork}}`/`{{Information}}` with no explicit image parameter, so MediaWiki auto-displays the page's own image and lists the file as a user of itself. Counting that self-reference made the gate fire for every file, defeating its purpose — hence the `fulimit=2` (self plus at most one other is enough to distinguish "used by something else" from "only itself") and the explicit `title != self_title` filter. The check **fails open** (returns `True`) on any error, so a needed relink is never silently dropped.

**The check runs *before* the move.** All three call sites verify usage while the old title is still the live file, then pass `check_usage=False` to `post_commonsdelinker_request`. Once a file has moved, the old title is a redirect and the usage query is unreliable — it can transiently read as "used" right after the move, defeating the gate. The internal `check_usage=True` default remains for any caller asking about a title that has *not* just been moved.

**Reason string.** The default is `"[[COM:FR|File renamed]]: [[COM:FR#FR4|Criterion 4]] (harmonize the names of a set of images)"` — Commons' File Renaming policy criterion 4 (harmonising a set of related files). `build_title_drift_move_reason` iteratively shortens the reason string until it fits MediaWiki's 500-byte `CommentStore::COMMENT_CHARACTER_LIMIT` once username + filenames + the 36-byte fixed prefix overhead is accounted for.

**Three call sites in `tools/uploader.py`:** each computes `needs_relink = file_has_inbound_usage(...)` *before* the move, then posts (with `check_usage=False`) only if true.

| Call site | When | Posts a delinker request when… |
|---|---|---|
| `_resolve_redirect_move` | After moving a redirect target to the intended title | The old title has inbound usage |
| `_move_to_correct_title` (empty intended title) | After moving a file from a wrong title to the (empty) intended title | The old title has inbound usage AND is not a suppressed sibling slot |
| `_move_to_correct_title` (redirect-to-self at intended title) | After moving a file when the intended title was a redirect to it | Same: inbound usage AND not a sibling slot |

In other words: a delinker request is posted only if the old title has inbound usage and is not a suppressed sibling slot, and the usage check always runs before the move. A sibling slot is suppressed (`post_commonsdelinker=False`) because a later ordinal in the same session will overwrite the redirect at that title with different content, making any link-rewrite request invalid.

The `MERGE_AND_REDIRECT` and `HAND_FIX` outcomes do NOT post a delinker request: a merge moves no file (it re-homes SDC onto the pre-existing canonical file and leaves a `#REDIRECT` at our *own* intended title), and a hand-fix touches nothing on Commons — so there is no old→new rename for CommonsDelinker to propagate.

## Orphan audit

The post-item orphan check sweeps for stale files left behind by source-media truncations. If a previous run uploaded `Foo (page 5).jpg` but the source now lists only 4 pages, page 5 is an orphan on Commons.

`_post_item_orphan_check()` probes `(page N+1)`, `(page N+2)`, etc. above the highest kept page label for each extension group (via `FilePage.exists()`, tolerating up to `_ORPHAN_GAP_TOLERANCE` consecutive gaps, since orphans aren't always contiguous). For each found orphan it compares the orphan's `latest_file_info.sha1` against the per-extension `sha1 → kept_title` map built from this run's surviving ordinals.

**Log-only (SHA1-uniqueness redesign).** The probe no longer writes anything to Commons — the entire `{{Duplicate}}`-tag apparatus (tagging, the `Category:Duplicate` throttle, and the deferred-drain phase) is retired. Every trailing-page orphan found is logged and counted under `Result.ORPHANS_FLAGGED` — whether or not it matches a kept ordinal's SHA1 — so an operator can see what a human may want to reconcile. Redirects are skipped outright (a redirect already points at its target and has no file content of its own; `latest_file_info` would follow it and falsely match). `Result.ORPHANS_TAGGED` survives in the `Result` enum for backward compatibility but is never incremented.

## Wikimedia error codes

`ingest_wikimedia/wikimedia.py` exports the error-code constants:

```python
ERROR_FILEEXISTS    = "fileexists-shared-forbidden"
ERROR_MIME          = "filetype-badmime"
ERROR_BANNED        = "filetype-banned"
ERROR_DUPLICATE     = "duplicate"
ERROR_NOCHANGE      = "no-change"
ERROR_BACKEND_FAIL  = "backend-fail-internal"
```

`IGNORE_WIKIMEDIA_WARNINGS` (passed to pywikibot's `ignore_warnings=` argument) suppresses cosmetic warnings:

- `bad-prefix`, `badfilename`, `duplicate-archive`, `duplicate-version`, `empty-file`, `exists`, `exists-normalized`, `filetype-unwanted-type`, `page-exists`, `was-deleted`.

But deliberately does NOT include `duplicate` or `no-change`, so pywikibot still raises on both. `duplicate` is a hard failure. `no-change` (Commons's `fileexists-no-change` response) is instead caught in `process_file`'s exception handler and, when Commons already holds our content at the intended title, is treated as an invariant-satisfied SKIP (see [Commons-dedup byte-drift](#commons-dedup-byte-drift) below) rather than a FAILED.

### Commons-dedup byte-drift

When our SHA1 differs from the file already at the intended title but Commons rejects the re-upload as an exact duplicate of the *current* version there — e.g. partner PDFs with a trailing byte Commons strips on ingest — the file on Commons is already correct after its server-side normalisation. The uploader records `ORDINAL_SKIPPED` (not FAILED) and increments `Result.UPLOAD_SKIPPED_COMMONS_DEDUP` so the class is auditable separately from real failures. It is detected in two paths: the chunked-upload path where `site.upload()` returns `None` (`_detect_commons_dedup_skip`), and the direct-upload path that catches Commons's `fileexists-no-change` exception (`_detect_commons_dedup_from_nochange_error`, which reuses `_detect_commons_dedup_skip` for the result).

### "File linked to another page"

When `site.upload(...)` returns falsy (pywikibot returns `None` when the file exists under a different page title — typically a redirect not caught by the redirect-handling branch above), the uploader first checks for the [Commons-dedup byte-drift](#commons-dedup-byte-drift) class (`_detect_commons_dedup_skip`) and records `ORDINAL_SKIPPED` if it matches. Only if byte-drift is ruled out does it raise `RuntimeError("File linked to another page (possible ID drift)")`, which `handle_upload_exception` logs as `File linked to another page (unhandled drift shape)`. The retry classifier (`tools/get_ids_retry.py`) matches the `File linked to another page` substring and marks the item retryable.

### Retryable failure patterns

`tools/get_ids_retry.py` parses logs for these patterns (full list in `UPLOAD_TRANSIENT_ERRORS`):

- `lockmanager-fail-conflict`
- `lockmanager-fail-svr-acquire`
- `stashfailed: Could not acquire lock`
- `stashfailed: Server failed to publish temporary file`
- `uploadstash-exception`
- `backend-fail-internal`
- `File linked to another page`
- `ArticleExistsConflictError`
- `fileexists-shared-forbidden`

These all mean "transient or correctable" — re-running the upload usually succeeds. The retry CSV merges download-retry and upload-retry into one combined CSV per hub so a single uploader invocation handles both.

## Content hubs vs. service hubs

DPLA distinguishes two kinds of hub, and `ingest_wikimedia/sdc.py` encodes the partnership chain (the three-statement P9126 "operator" chain, each statement carrying a P3831 *object has role* qualifier) differently for each. The split is enumerated in code because `institutions_v2.json` carries no hub-type flag:

```python
Q_NARA = "Q518155"          # National Archives and Records Administration
Q_SMITHSONIAN = "Q131626"   # Smithsonian Institution
CONTENT_HUB_QIDS = frozenset({Q_NARA, Q_SMITHSONIAN})
```

The role-qualifier values (named for their actual Wikidata roles — renamed from the earlier misnamed `Q_SMITHSONIAN`/`Q_PUBLISHER`/`Q_AGGREGATOR`):

```python
Q_ROLE_AGGREGATOR   = "Q393351"      # aggregator (DPLA, and any service hub)
Q_ROLE_REPOSITORY   = "Q108296843"   # repository
Q_ROLE_CONTRIBUTING = "Q108296919"   # contributing / custodial unit
```

DPLA itself is always the top `aggregator` in P9126. The hub and institution roles then differ:

| | Hub role | Institution role | Sits in P195 (collection) |
|---|---|---|---|
| **Content hub** (NARA, Smithsonian) | `repository` (`Q_ROLE_REPOSITORY`) | `contributing` (`Q_ROLE_CONTRIBUTING`) | The **hub** |
| **Service hub** (everything else) | `aggregator` (`Q_ROLE_AGGREGATOR`, like DPLA) | `repository` (`Q_ROLE_REPOSITORY`) | The **institution** |

A content hub *is* the providing institution — a `repository` that sits in P195 — and its "data providers" are internal departments, tagged as the `contributing` (custodial) unit. A service hub is an aggregating intermediary whose institutions are distinct organizations that each sit in P195.

These role/P195 shapes match what is already on Commons; the read side (`Module:DPLA`) and `dpla_claims()` depend on them, so changing them without a coordinated Commons-side rewrite would make every existing P9126 statement look "unexpected" and trigger a remove+re-add on every re-sync.

## Per-ordinal status and pageid rescue

SDC eligibility is **decoupled from upload-result status**. The uploader writes a per-item `<partner>/<dpla_id>/upload-result.json` sidecar keyed by ordinal, with one `ORDINAL_*` status per ordinal:

```python
ORDINAL_UPLOADED     # file just uploaded (or drift-moved into place)
ORDINAL_SKIPPED      # existing Commons file matches our SHA1
ORDINAL_NOT_PRESENT  # no S3 asset to upload (downloader gap)
ORDINAL_INELIGIBLE   # S3 asset present but uploader chose not to upload
ORDINAL_FAILED       # upload attempted, raised, did not land
ORDINAL_MERGED       # source-duplicate SHA1: this item's SDC merged onto the
                     #   canonical file, #REDIRECT left at our intended title
ORDINAL_HAND_FIX     # SHA1 at a wrong title with the intended title blocked, or
                     #   a community-file match: recorded to hand-fix.jsonl, no upload
```

UPLOADED and SKIPPED ordinals carry a `title` and `pageid` and are eligible for `wbsetclaims`. MERGED carries the canonical file's `title`/`pageid` but is deliberately NOT SDC-eligible — its SDC was merged onto the canonical inline, so re-targeting our redirect title would double-write. NOT_PRESENT / INELIGIBLE / HAND_FIX / FAILED have no canonical Commons page of our own to attach structured data to.

**Pageid resolution and the fail-closed contract.** Commons can accept a large (chunked) upload and return without a real `.pageid` (or return `0`) while indexing lags. `_refresh_pageid_with_retries(page_title)` (in `tools/uploader.py`) re-fetches the page with bounded backoff to ride out that lag. If the budget is exhausted without a real id, it returns `None` and the sidecar records `pageid: null` rather than a malformed `0`. That `pageid: null` is a deliberate **fail-closed contract**: `sdc-sync`'s `if not pageid` guard feeds the title→pageid fallback, which recovers the real pageid on the next run.

**Rescue by DPLA ID.** When the uploader couldn't confirm an ordinal (NOT_PRESENT / INELIGIBLE / FAILED) but a Commons file with that DPLA ID and ordinal already exists from a prior successful run, `_find_existing_commons_files_by_dpla_id(dpla_id)` (in `tools/sdc_sync.py`) rescues the ordinal → `{title, pageid}` mapping. It uses CirrusSearch `intitle:"<dpla_id>"` (the 32-hex ID is unique enough that the result set is bounded by the item's file count) and lets SDC sync against the existing file rather than skipping the data-side work because of a transient binary-side failure. It returns an empty dict on any failure mode, in which case the caller's existing skip path runs.

## Limitations

**Duplicate detection is SHA1-only.** `find_file_by_hash` queries Commons' `allimages?sha1=...`, so the entire decision tree above keys on exact byte-for-byte hash matches. It does **not** catch visually-identical re-encodes: a different encoding of the same image produces different bytes → a different SHA1 → no match, so the uploader treats it as net-new (or uploads a new version over the existing title). This is known and accepted behavior, not a bug.
{% endraw %}
