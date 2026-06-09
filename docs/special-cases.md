# Special Cases: Duplicate Detection, File Renames, CommonsDelinker

The uploader carries the bulk of the pipeline's "what if Commons-side state doesn't match what I'm about to upload?" logic. This document covers:

1. [Title generation](#title-generation)
2. [Duplicate-detection decision tree](#duplicate-detection-decision-tree)
3. [Hash drift: the four cases](#hash-drift-the-four-cases)
4. [Redirect handling](#redirect-handling)
5. [CommonsDelinker integration](#commonsdelinker-integration)
6. [Orphan tagging](#orphan-tagging)
7. [Wikimedia error codes](#wikimedia-error-codes)

## Title generation

`get_page_title()` in `ingest_wikimedia/wikimedia.py` is the single source of truth for Commons file titles. The final form is:

```
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

There is no separate "title blacklist" file the uploader reads; the rules are encoded as the substitution table above. Each was added in response to a specific incident; the lesson at `~/.claude/lessons.md` under "Title-blacklist character substitutions: guard on the actual rule, not on any single character" captures the most-recent revision (the `&...=` together-only rule replacing an unconditional always-replace-`&` from an earlier version).

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

The existing title encodes a different DPLA ID (extracted via the regex `- DPLA - ([0-9a-f]{32})`), AND that other DPLA item is still valid in `api.dp.la`. Both files coexist; we upload to our intended title without touching the existing file.

Return: `"upload_only"`.

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

Used otherwise. Replaces the redirect text with fresh wikitext, optionally merging preserved license / category / `{{Image extracted|...}}` / assessment-class templates (`{{Picture of the day}}`, `{{Featured picture}}`, etc.) from the prior wikitext via `merge_preserved_wikitext`. The new `{{Artwork}}` block is always authoritative for the description; preserved blocks are appended.

## CommonsDelinker integration

`User:CommonsDelinker/commands/filemovers` on Commons is a community-operated bot's instruction queue. The pipeline appends `{{universal replace|<old>|<new>|reason=...}}` lines to it; CommonsDelinker reads the page out-of-band, rewrites every link to `[[File:<old>]]` on every Wikimedia wiki to point at `[[File:<new>]]`, and deletes the obsolete redirect.

**One function, one API.** `post_commonsdelinker_request(site, old_filename, new_filename)` in `ingest_wikimedia/wikimedia.py` is the only call site. It uses MediaWiki's `appendtext` API parameter (not read-modify-write) for two reasons:

1. The page is currently ~230 KB and growing. Loading it via `page.text` then re-saving the whole thing would burn ~500 KB per request — contributed to a recent OOM episode.
2. `page.text` reads from a replica that can lag the primary by seconds; under burst load (a hub move-storm can post hundreds of requests in a minute) read-modify-write produced self-inflicted `editconflict` rejections. `appendtext` is server-side concatenation against the primary, no GET, atomic.

**Reason string.** The default is `"[[COM:FR|File renamed]]: [[COM:FR#FR4|Criterion 4]] (harmonize the names of a set of images)"` — Commons' File Renaming policy criterion 4 (harmonising a set of related files). `build_title_drift_move_reason` iteratively shortens the reason string until it fits MediaWiki's 500-byte `CommentStore::COMMENT_CHARACTER_LIMIT` once username + filenames + the 36-byte fixed prefix overhead is accounted for.

**Three call sites in `tools/uploader.py`:**

| Call site | When | Notes |
|---|---|---|
| `_resolve_redirect_move` | After moving a redirect target to the intended title | Always posts a delinker request |
| `_move_to_correct_title` (Case 3) | After moving a file from a wrong title to the intended title | Posts unless the old title is a sibling slot |
| `_move_to_correct_title` (Case 1) | After moving a file when the intended title was a redirect to it | Same suppression rule |

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

- `bad-prefix`, `duplicate-archive`, `duplicate-version`, `empty-file`, `exists`, `exists-normalized`, `was-deleted`, `large-file`, etc.

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
