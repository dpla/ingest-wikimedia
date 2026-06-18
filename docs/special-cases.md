{% raw %}
# Special Cases: Duplicate Detection, File Renames, CommonsDelinker

The uploader carries the bulk of the pipeline's "what if Commons-side state doesn't match what I'm about to upload?" logic. This document covers:

1. [Title generation](#title-generation)
2. [Duplicate-detection decision tree](#duplicate-detection-decision-tree)
3. [Hash drift: the four cases](#hash-drift-the-four-cases)
4. [Redirect handling](#redirect-handling)
5. [CommonsDelinker integration](#commonsdelinker-integration)
6. [Orphan tagging](#orphan-tagging)
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

Applied in order to the first 181 chars of the source title:

| Rule | Why |
|---|---|
| `& → +` and `= → -` (when both present) | Commons' title blacklist rule fires only when both characters are present together — typical of unencoded query-string titles |
| `: → -` | MediaWiki strips mid-title colons from File-namespace titles (5 NARA items orphaned in May 2026 by an earlier version that left them alone) |
| `/ → -` | MediaWiki rejects subpage-parsed names on `UploadBase::isValidName` (4,114 NARA items hit `imageinvalidfilename` during Case-3 moves before this was added; dates like `1/5/1966` were the culprit) |
| `\ → -`, `< → -`, `> → -` | Outside `Title::legalChars()` |
| `'' → "` | Titleblacklist double-apostrophe rule |
| `[ → (`, `] → )`, `{ → (`, `} → )`, `# → -`, `\| → -` | Forbidden in MediaWiki page names; `\|` additionally breaks Commons' file-extension detection |
| `� → '` | Unicode replacement char → right single quote |
| `replace_invisible()` | Strip zero-width / bidi characters that Commons rejects |

There is no separate "title blacklist" file the uploader reads; the rules are encoded as the substitution table above. Each was added in response to a specific incident: the colon and slash substitutions trace back to NARA imports in May 2026 where the prior code silently orphaned items whose source titles contained mid-title `:` or `/`; the `&...=` together-only rule replaced an earlier unconditional `&`-replacer that was too aggressive (Commons' actual blacklist only matches when both characters appear together, typical of unencoded query-string titles). The pattern in all cases: match the *actual* MediaWiki rejection rule (which usually involves multiple characters together), not any single character in isolation, so the substitution doesn't damage benign titles.

## Duplicate-detection decision tree

The uploader detects "this content already exists on Commons" along two axes: SHA1 hash, and Commons file title. Three primitives in `ingest_wikimedia/wikimedia.py`:

```python
wiki_file_exists(site, sha1)                          # bool
find_file_by_hash(site, sha1, preferred_title=None)   # FilePage or None
collect_duplicate_source_sha1s(s3, dpla_id, partner)  # set of sha1s appearing > 1× in this item
```

`find_file_by_hash` calls Commons' `allimages?sha1=...` API; if any result lives at the `preferred_title`, returns it; otherwise returns the first result alphabetically. `collect_duplicate_source_sha1s` walks each S3 ordinal's user metadata and returns SHA1s that appear at two-or-more positions in the same DPLA item — used to short-circuit drift detection for legitimate intra-item duplicates.

### The hot path (`process_file`)

For each ordinal:

```text
sha1 = read from S3 user-metadata 'CHECKSUM'
page_title = get_page_title(...)
existing = find_file_by_hash(site, sha1, preferred_title=page_title)

if existing is None:
    upload to page_title             # net-new file
elif existing.title == page_title:
    SKIPPED                          # exact match, our content is already there
elif is_dup_sha1_sibling_at_expected_title(...):
    proceed with "upload_only"       # legitimate intra-item duplicate, not drift
else:
    drift_action = _resolve_hash_drift(...)  # see next section
```

## Hash drift: the four cases

`_resolve_hash_drift()` is invoked when our SHA1 already exists on Commons but at a different title than we want. Four distinguishable cases:

### Case 0 — Cross-item collision

The existing title encodes a *different* DPLA ID (extracted via `extract_dpla_id_from_commons_title`, regex `- DPLA - ([0-9a-f]{32})`). Whether we leave the existing file alone or migrate it hinges on whether that other DPLA ID still resolves:

- **Other item still valid in `api.dp.la`** — two genuinely distinct items happen to share a hash. Both files coexist; we upload to our intended title without touching the existing file. Return `"upload_only"`.
- **404 on the colliding DPLA ID** — the strongest possible signal that the existing Commons file is an orphan: its DPLA-side anchor is gone. We treat the 404 exactly like `other_item is None` and **fall through to Case 1/2/3 migration** (move or upload-and-tag), rather than silently creating a duplicate alongside the orphan.
- **Any *other* error (network timeout, 5xx, JSON parse failure)** — none of these mean "the old ID is gone," so we stay on the conservative `"upload_only"` fallback. A transient API blip must not trigger a destructive move on a file that may still have a valid sibling item.

The 404-vs-other distinction is read off `ex.response.status_code`.

### Case 1 — Intended title is a redirect to the existing file

The existing file lives at some other title, and our intended title is a redirect pointing at it. Treat as Case 3 — move the existing file to our intended title.

### Case 2 — Intended title has real content with a different hash

Conflict. The intended title is occupied by unrelated content; the existing file at the wrong title is ours. Three actions:

1. Upload our SHA1 to the intended title with `force_ignore_warnings=True` (overwriting the unrelated content).
2. Tag the old (wrong-title) file with `{{Duplicate|<correct-filename>|...}}` via `_tag_drift_duplicate` / `tag_as_duplicate` — using `prependtext` and the idempotent `_DUPLICATE_TAG_RE` regex so re-runs don't pile up tags.
3. No CommonsDelinker request (we didn't move anything; we overwrote and tagged).

Return: `"upload_and_tag"`.

**Sibling-slot fallback.** If the wrong-title file is itself one of this item's other expected ordinal positions (a sibling slot about to be overwritten this run), the duplicate tag is suppressed and the function returns `"upload_only"` instead — the about-to-be-overwritten sibling shouldn't be tagged as a duplicate of a title we're still in the middle of writing.

### Case 3 — Intended title is unoccupied

The existing file is ours but at the wrong title; the intended title has nothing at it. Two actions:

1. Move the existing file to the intended title via `existing_file.move(..., noredirect=False)` — leaving a redirect at the old title.
2. Post a CommonsDelinker request to rewrite cross-wiki links pointing at the old title.

Exception: if the old title is one of *this item's own* expected ordinal positions (a sibling slot about to be overwritten this same run), the move still happens but the delinker request is suppressed (`post_commonsdelinker=False`) — otherwise we'd ask the delinker to rewrite links pointing at a title we're about to clobber.

Return: `"moved"`.

### Short-circuit: intra-item duplicate-SHA1

`is_dup_sha1_sibling_at_expected_title` checks whether the existing Commons file is one of THIS item's other expected ordinal positions. If so, the existing file is a legitimate sibling (same source media listed at multiple ordinals), not drift; we go straight to `"upload_only"` and proceed.

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
| `_move_to_correct_title` (Case 3) | After moving a file from a wrong title to the intended title | The old title has inbound usage AND is not a suppressed sibling slot |
| `_move_to_correct_title` (Case 1) | After moving a file when the intended title was a redirect to it | Same: inbound usage AND not a sibling slot |

In other words: a delinker request is posted only if the old title has inbound usage and is not a suppressed sibling slot, and the usage check always runs before the move. A sibling slot is suppressed (`post_commonsdelinker=False`) because a later ordinal in the same session will overwrite the redirect at that title with different content, making any link-rewrite request invalid.

The Case-2 path does NOT post a delinker request — there, `_tag_drift_duplicate` flags the old file for human review instead.

## Orphan tagging

The post-item orphan check sweeps for stale files left behind by source-media truncations. If a previous run uploaded `Foo (page 5).jpg` but the source now lists only 4 pages, page 5 is an orphan on Commons.

`_post_item_orphan_check()` probes `(page N+1)`, `(page N+2)`, etc. above the highest kept page label for each extension group. For each found orphan, it compares the orphan's `latest_file_info.sha1` against the per-extension `sha1 → kept_title` map built from this run's surviving ordinals. On match, `tag_as_duplicate` prepends `{{Duplicate|<kept-title>|...}}` to the orphan.

**Counters:** `Result.ORPHANS_TAGGED` (matched the SHA1 of a kept ordinal and got the duplicate tag), `Result.ORPHANS_FLAGGED` (found but didn't match — left untouched for human review).

`{{Duplicate}}` tagging is idempotent via the regex `_DUPLICATE_TAG_RE` that recognises `{{Duplicate}}` / `{{duplicate}}` with whitespace tolerance and a `(?:\||\}\})` lookahead so existing `{{DuplicateImageFinder|…}}` tags don't false-match.

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

But deliberately does NOT include `duplicate` or `no-change` — those still hard-fail because they indicate state we need to react to (different from a cosmetic "this filename is a bit weird" warning).

### "File linked to another page"

When `site.upload(...)` returns falsy (pywikibot returns `None` when the file exists under a different page title — typically a redirect not caught by the redirect-handling branch above), the uploader raises `RuntimeError("File linked to another page (possible ID drift)")`. The retry classifier (`tools/get_ids_retry.py`) catches this string and marks the item retryable.

### Retryable failure patterns

`tools/get_ids_retry.py` parses logs for these patterns (full list in `UPLOAD_TRANSIENT_ERRORS`):

- `lockmanager-fail-conflict`
- `stashfailed: Could not acquire lock`
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
```

UPLOADED and SKIPPED ordinals carry a `title` and `pageid` and are eligible for `wbsetclaims`; NOT_PRESENT / INELIGIBLE / FAILED have no canonical Commons page to attach structured data to.

**Pageid resolution and the fail-closed contract.** Commons can accept a large (chunked) upload and return without a real `.pageid` (or return `0`) while indexing lags. `_refresh_pageid_with_retries(page_title)` (in `tools/uploader.py`) re-fetches the page with bounded backoff to ride out that lag. If the budget is exhausted without a real id, it returns `None` and the sidecar records `pageid: null` rather than a malformed `0`. That `pageid: null` is a deliberate **fail-closed contract**: `sdc-sync`'s `if not pageid` guard feeds the title→pageid fallback, which recovers the real pageid on the next run.

**Rescue by DPLA ID.** When the uploader couldn't confirm an ordinal (NOT_PRESENT / INELIGIBLE / FAILED) but a Commons file with that DPLA ID and ordinal already exists from a prior successful run, `_find_existing_commons_files_by_dpla_id(dpla_id)` (in `tools/sdc_sync.py`) rescues the ordinal → `{title, pageid}` mapping. It uses CirrusSearch `intitle:"<dpla_id>"` (the 32-hex ID is unique enough that the result set is bounded by the item's file count) and lets SDC sync against the existing file rather than skipping the data-side work because of a transient binary-side failure. It returns an empty dict on any failure mode, in which case the caller's existing skip path runs.

## Limitations

**Duplicate detection is SHA1-only.** `find_file_by_hash` queries Commons' `allimages?sha1=...`, so the entire decision tree above keys on exact byte-for-byte hash matches. It does **not** catch visually-identical re-encodes: a different encoding of the same image produces different bytes → a different SHA1 → no match, so the uploader treats it as net-new (or uploads a new version over the existing title). This is known and accepted behavior, not a bug.
{% endraw %}
