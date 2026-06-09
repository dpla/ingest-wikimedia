# SDC Sync

The `sdc-sync` phase reconciles each DPLA-uploaded Commons file's MediaInfo structured data against the per-item `sdc.json` envelope staged by `get-ids-es`. It is the only phase that talks to Commons' Wikibase API directly.

This document covers the *internals* — every property the bot writes, the atomic dispatcher design, idempotency, chunked claims, and known Wikibase API gotchas. For the phase's role in the pipeline, see [pipeline-phases.md](pipeline-phases.md#4-sdc-sync--sdc-sync).

## Table of contents

1. [Entry points](#entry-points)
2. [The atomic dispatcher](#the-atomic-dispatcher)
3. [What the bot writes (per property)](#what-the-bot-writes-per-property)
4. [The canonical reference triple](#the-canonical-reference-triple)
5. [Idempotency: `check()`](#idempotency-check)
6. [Chunked claims](#chunked-claims)
7. [The P813 refresh](#the-p813-refresh)
8. [Wikibase API gotchas](#wikibase-api-gotchas)

---

## Entry points

```python
# Partner mode (Slack-launched runs)
sdc-sync --partner <partner> [--ids-file <path>]

# Legacy modes (operator runs, not used in pipeline)
sdc-sync --file "File:Title.jpg" [--file "File:Other.jpg" ...]
sdc-sync --cat <Commons-category> [--recurse] [--limit N]
sdc-sync --lists <directory-of-txt-files>
```

**Partner mode** drives off the S3 sidecars only (no `api.dp.la` calls). It iterates the IDs CSV, reads `sdc.json` + `upload-result.json` + `file-list.txt` per item, picks `UPLOADED` / `SKIPPED` ordinals from `upload-result.json`, and calls `process_one_from_sdc(mediaid, dpla_id, sdc_payload, download_url, page_number)` per ordinal.

**Legacy mode** builds claims at runtime via `parsed()`, fetching the source document either from S3 (`--from-s3 <partner>`) or from `api.dp.la` directly (one retry on network failure). Calls `process_one(mediaid, dpla_id)`. `--file` is repeatable (pass once per title); `--lists` reads every `.txt` file in a directory as a list of titles. Slack-launched runs never go through this path; it remains supported for one-off operator runs.

Both paths converge on the per-file accumulators + atomic `wbeditentity` dispatcher.

## The atomic dispatcher

`_submit_per_item_edit(mediaid, dpla_id, summary, new_claims=, reference_updates=, qualifier_updates=, removals=)` is the single chokepoint for every SDC write. It accepts four fragment kinds and bundles them into one `wbeditentity` revision:

| Fragment kind | Shape | When emitted |
|---|---|---|
| `new_claims` | Full claim dict, no `id` | A property that doesn't yet exist on the file |
| `reference_updates` | `{id, type, mainsnak, qualifiers, rank, references}` | The P813 refresh (see below) |
| `qualifier_updates` | `{id, type, mainsnak, qualifiers, rank, references}` | Amending qualifiers on an existing DPLA-authored claim |
| `removals` | `{id, remove: ""}` | The reconciler decided a stale DPLA claim must go |

**Why "atomic" matters.** The previous design issued five-to-seven separate API calls per file (one `wbeditentity` for new claims, one for references, `wbsetqualifier` per qualifier amend, `wbremovequalifiers` for stale qualifier cleanup, `wbremoveclaims` for reconciler removals). A failure between calls could leak orphaned stale statements onto Commons. The consolidated dispatcher is all-or-nothing per file — the bundle lands as one revision or none of it does.

**Defence-in-depth.** Every non-removal fragment gets `type: "statement"` and `mainsnak` injected if missing — Wikibase rejects the entire bundle with `invalid-claim: Type is missing` or `invalid-claim: Attribute "mainsnak" is missing` otherwise, and atomic semantics mean one malformed fragment drops every other edit on the file. Two PRs (#285, #286) fixed sub-cases where partial-update fragments missed these fields.

**Per-file accumulators** (module-level globals in `tools/sdc_sync.py`):

- `claims["claims"]` — new claims to add.
- `refclaims["claims"]` — reference attachments to existing claims (used by `check()` when it finds a matching DPLA-authored statement that lacks the reference).
- `qualifier_amends` — list of `(claimid, prop, snak)` triples for qualifier adds.
- `qualifier_removals` — list of `(claimid, snak_hash)` for stale qualifier removals.
- `removals` — list of statement IDs to delete.

`_reset_per_file_accumulators()` clears all five at the start of each `process_one*` call so cross-file state can't leak.

## What the bot writes (per property)

All properties below are defined in `build_claims_for_doc()` (`ingest_wikimedia/sdc.py`) for new uploads and reconciled by `tools/sdc_sync.py` on re-runs.

| Property | Snaktype | Source | Notes |
|---|---|---|---|
| **P760** (DPLA ID) | string | `dpla-map.json` `id` | Per-ordinal `P304` qualifier added at sync time for multi-file extension groups |
| **P1476** (title) | monolingualtext | `sourceResource.title` | Chunked across statements with `P1545` ordinal when value exceeds 1500 chars |
| **P195** (collection) | wikibase-entityid | institution Q-ID (Smithsonian: hub Q-ID = institution) | |
| **P170** (creator) | somevalue + `P2093` stated-as | `sourceResource.creator` | Always somevalue + stated-as text qualifier; never a direct entity link |
| **P571** (inception/date) | time + `P1932` stated-as, or somevalue + `P1932` stated-as | `sourceResource.date` | `parse_dpla_date` extracts a value-typed time when parseable; `P1932` (stated as) qualifier always carries the verbatim source string on both branches. When `parse_dpla_date` flags the value as approximate, a `P1480 = Q5727902` (circa) qualifier is added |
| **P10358** (description) | monolingualtext | `sourceResource.description` | Chunked across statements |
| **P9126** (maintained by) | wikibase-entityid (×3 statements) | DPLA Q2944483 + hub Q-ID + institution Q-ID | Each carries an explicit `P3831` (object role) qualifier: publisher / aggregator / contributing institution |
| **P7482** (described at source catalog) | wikibase-entityid | `isShownAt` URL | `P973` qualifier with the URL; `P6108` qualifier with IIIF manifest URL when valid; per-ordinal `P2699` qualifier added at sync time |
| **P217** (local identifier) | string + `P195` qualifier | `sourceResource.identifier` | Chunked if needed |
| **P275** (copyright license) | wikibase-entityid | `rights` URI → Q via `rights.json` | Rights cluster: one of `P275`, `P6216`, or `P6426` |
| **P6216** (copyright status) | wikibase-entityid | `rights` URI → Q | Rights cluster |
| **P6426** (rights status as a creator) | wikibase-entityid | `rights` URI → Q | Rights cluster |
| **P4272** (subject string) | string | `sourceResource.subject[].name` | Always emitted, even when entity reconciliation succeeds |
| **P921** (main subject) | wikibase-entityid | `sourceResource.subject[].exactMatch` → Wikidata Q-ID | Only when Wikidata reconciliation succeeded in get-ids-es Phase 2 |

**NARA-only** (parsed from the originalRecord XML by `parse_nara_access_level` via stdlib ElementTree since PR #278):

| Property | Snaktype | Meaning |
|---|---|---|
| **P1225** (NARA NAID) | string | NARA Authority ID |
| **P7228** (access level) | wikibase-entityid | Q-ID for the access level |
| **P6224** (level of description) | wikibase-entityid | Q-ID for the level |

**Per-ordinal qualifiers materialised at write time** (in `process_one_from_sdc`, not baked into `sdc.json`):

- **`P304`** (page number) qualifier on `P760`. Only added when the ordinal belongs to a multi-file extension group within the item (so an item with 3 JPGs and 2 PDFs gets `P304=1,2,3` on the JPG slots and `P304=1,2` on the PDF slots; a 1-JPG-1-PDF item gets no P304).
- **`P2699`** (URL = download URL) qualifier on `P7482`. The per-ordinal direct download URL from `file-list.txt`, indexed 1-based with an explicit range guard (Python's negative-index would silently corrupt against an off-by-one).

**Legacy-state backfills** (run inside `process_one_from_sdc` against files uploaded before these qualifiers existed):

- `_amend_p7482_url_qualifiers` — looks up the DPLA-authored P7482 statement on Commons, checks for missing `P2699` / `P6108` qualifiers, and queues a qualifier-update fragment if they're absent.
- `_amend_p760_page_qualifier` — same shape for `P304` on `P760`.

## The canonical reference triple

Every DPLA-authored statement carries the same reference triple:

```json
{
  "snaks": {
    "P854": [<DPLA item URL = https://dp.la/item/<dpla_id>>],
    "P123": [<Q2944483 = Digital Public Library of America>],
    "P813": [<retrieval date, precision day>]
  }
}
```

`_is_dpla_reference()` checks for `P123 = Q2944483` to identify "this is OUR reference" later. User-added references on the same claim (which carry different / no P123) are preserved verbatim.

Every DPLA-authored statement also carries a `P459 = Q61848113` (determination method = heuristic) qualifier, so the SDC phase can re-identify "this is OUR claim" on subsequent runs without trusting the reference (which a user could conceivably alter).

## Idempotency: `check()`

`check(mediaid, qid, prop)` is the per-property duplicate-detection gate. Returns `(should_add: bool, ref_claim_id_to_amend: str)`. The four outcomes per property `kind` (`item`, `string`, `monolingualtext`, `somevalue`, `time`, `source`):

1. **Matching no-reference DPLA-shaped statement** found → capture its ID as `ref` (caller will stamp the DPLA reference via `add_ref` and bundle into the dispatcher).
2. **Matching statement exists without qualifiers** → call `add_det` to stamp the `P459` qualifier; return `False` for add.
3. **Matching DPLA-shaped statement with qualifiers AND references** → already our write; return `(False, ref)`. No edits queued.
4. **Matching foreign statement** → return `(True, "")` so the dispatcher adds the DPLA-authored claim alongside without disturbing the foreign one.

`_is_safe_to_amend_in_place()` gates the amend-in-place decision (#1, #2 above): only amends when *every* qualifier and *every* reference on the existing statement is DPLA-authored. That guarantees the wholesale-replace round-trip of `wbeditentity` cannot erase user-added data.

**String and monolingualtext branches** recognise chunked claims by comparing `(value, p1545)` tuples — see below. The bare-add-det branch is restricted to unchunked values (`target_p1545 is None`) because grafting onto an unchunked pre-existing statement would conflate different chunks of the new value.

**Time comparison** uses `_time_claim_comparable` and the canonical `time|precision[|circa]` comparable string from `_extract_comparable_value` — so a `2026-06-01` precision-day claim matches an existing `2026-06-01` precision-day claim regardless of which property they're on.

## Chunked claims

Wikibase enforces a 1500-character cap on `string` and `monolingualtext` values. DPLA descriptions and titles often exceed this. The bot splits long values into per-chunk statements, each carrying a `P1545` (series ordinal) qualifier to preserve ordering.

**The convention:**

- First chunk of the first long value for `(prop, language)`: `P1545 = "A1"`. Second chunk: `"A2"`. Etc.
- First chunk of the second long value: `"B1"`. `"B2"`. Etc.
- A non-chunked value (≤ 1500 chars) gets no `P1545` qualifier.
- Letters wrap `Z` → `AA` → `AB` → … (`_advance_series_letter` handles the rollover; an early version had `Z` → `[` which slipped through tests because no DPLA value had > 26 distinct long-value series in a single language; PR #282 caught it during property-mapping review).

`_chunk_and_emit_claims()` (`sdc.py`) is the chunker. `_normalize_string_value` collapses whitespace before chunking. `_truncate` enforces the 1499-char limit on each chunk (the off-by-one to 1499 avoids edge cases with multi-byte UTF-8 truncation).

**At render time on Commons**, `Module:DPLA` reassembles the chunks by walking the `(prop, P1545)` pairs in order and concatenating their values. See [templates.md](templates.md).

**On re-sync**, `check()` uses `_extract_comparable_value` to compare `(value, p1545)` tuples — so the chunked-statement set is treated atomically. A re-run only writes when the chunks differ from what's on Commons.

## The P813 refresh

When the dispatcher is making *any* other edit on a file, it also refreshes `P813` (retrieved on) on every DPLA-authored claim whose existing P813 isn't already today's date. The idea: when the bot has re-verified a file's metadata against DPLA, every DPLA assertion on the file is effectively "as-of today" — useful provenance for downstream consumers, free inside an edit being made anyway.

`_build_p813_refresh_fragments()`:

1. Computes today's P813 snak via `_build_p813_snak(datetime.date.today())`.
2. Walks every statement on the file's MediaInfo entity.
3. Skips statements that are *already* being touched by another fragment (`already_touched_ids` from the dispatcher).
4. Skips statements with no references.
5. Per-statement, walks the existing references list: keeps user-added references verbatim, keeps DPLA references whose P813 is already today verbatim, and replaces DPLA references with stale P813.
6. Emits a `reference_updates` fragment per modified statement — full statement shape (`mainsnak`/`qualifiers`/`rank`/new `references`) because Wikibase's partial-update semantics require it.

**Conditional.** `_build_p813_refresh_fragments` is only invoked when the per-file dispatcher already has other edits to make (`has_any_other_edit`). Files where nothing else changed do *not* generate spurious revisions just to bump P813.

**Cosmetic note on diffs.** When `P813` changes, Wikibase's diff renderer shows the entire reference (URL, publisher, retrieved date) twice — once removed, once added — because reference identity is content-hash, not a stable ID. The data is correct; the diff is just noisy. Switching to `wbsetreference` for in-place reference updates is theoretically possible but would break the atomic-bundle property (separate API calls per refreshed reference) without changing the visual diff in any documented way.

## Wikibase API gotchas

Three rules that caused real production incidents:

### Non-removal fragments must carry `type: "statement"`

Without it, Wikibase rejects the entire bundle with `invalid-claim: Type is missing`. Because the dispatcher is atomic, ONE malformed fragment drops every other edit. The dispatcher backstops this by stamping `type: "statement"` on every non-removal fragment that's missing it.

### Non-removal fragments must carry `mainsnak`

`wbeditentity` treats every non-removal claim entry as a wholesale-replace operation, so the fragment must carry the full statement context (`mainsnak`, `rank`, `qualifiers`, `references`) — not just the field being amended. Partial fragments shaped `{id, type, qualifiers}` or `{id, type, references}` are rejected with `invalid-claim: Attribute "mainsnak" is missing`, and atomicity drops every other edit with them.

The two fragment builders (`_build_qualifier_update_fragments`, `_build_p813_refresh_fragments`) copy `mainsnak` / `rank` / the sibling field from the cached statement so the diff is minimal but the fragment is complete. Deep-copies guard against subsequent helpers mutating shared cache state.

### Qualifier wholesale-replace semantics

`wbeditentity` replaces the entire qualifier set when a `qualifiers` field is included. The dispatcher merges new qualifier snaks with existing qualifier set (preserving snak hashes so Wikibase recognises unchanged snaks and doesn't dirty them in the diff). `_merge_qualifier_snaks` and `_exclude_qualifier_snaks` are the two transform helpers.

The hash-preserving merge is what keeps a P2699 backfill from re-stamping every existing P973/P137/P459 qualifier as "changed" — the existing snaks come back with their pre-existing hashes, Wikibase recognises them, and the diff shows only the new P2699 snak.

### Lessons file

The lesson stored at `~/.claude/lessons.md` under "wbeditentity claim entries must carry `type: "statement"`" captures the broader pattern: any hand-built dict appended to `wbeditentity`'s `data.claims` list that isn't a removal must carry `type: "statement"` AND `mainsnak`. Tests that mock `_submit_sdc_write` don't catch this — the fragment shape must be asserted directly against the payload. The regression test `test_flush_emits_type_statement_on_every_non_removal_fragment` does exactly that for every fragment kind.
