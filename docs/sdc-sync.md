{% raw %}
# SDC Sync

The `sdc-sync` phase reconciles each DPLA-uploaded Commons file's MediaInfo structured data against the per-item `sdc.json` envelope staged by `get-ids-es`. It is the only phase that talks to Commons' Wikibase API directly.

This document covers the *internals* — the worker model and box-wide write budget, every property the bot writes, the atomic dispatcher design, idempotency, chunked claims, legacy `{{Artwork}}` migration, the post-SDC wikitext cleanup, and known Wikibase API gotchas. For the phase's role in the pipeline, see [pipeline-phases.md](pipeline-phases.md#4-sdc-sync--sdc-sync).

## Table of contents

1. [Entry points](#entry-points)
2. [Parallelism & worker model](#parallelism--worker-model)
3. [Box-wide worker-slot budget](#box-wide-worker-slot-budget)
4. [Eligibility, ordinal rescue & pageid self-heal](#eligibility-ordinal-rescue--pageid-self-heal)
5. [The atomic dispatcher](#the-atomic-dispatcher)
6. [What the bot writes (per property)](#what-the-bot-writes-per-property)
7. [The canonical reference triple](#the-canonical-reference-triple)
8. [Idempotency: `check()`](#idempotency-check)
9. [Chunked claims](#chunked-claims)
10. [The reference refresh](#the-reference-refresh-p813--canonical-repair)
11. [Legacy `{{Artwork}}` migration](#legacy-artwork-migration)
12. [Post-SDC wikitext normalization (strip)](#post-sdc-wikitext-normalization-strip)
13. [Wikibase API gotchas](#wikibase-api-gotchas)

---

## Entry points

```python
# Partner mode (Slack-launched runs)
sdc-sync --partner <partner> [--ids-file <path>]
         [--workers N] [--workers-budget N]
         [--normalize-wikitext | --no-normalize-wikitext]

# Legacy {{Artwork}} → {{DPLA metadata}} migration (partner-scoped sub-command)
sdc-sync --partner <partner> --migrate-legacy [--ids-file <path>]

# Manual modes (operator runs, not used in pipeline)
sdc-sync --file "File:Title.jpg" [--file "File:Other.jpg" ...]
sdc-sync --cat <Commons-category> [--recurse] [--limit N]
sdc-sync --lists <directory-of-txt-files>
```

`_build_parser()` (`tools/sdc_sync.py`) defines every flag. The defaults that matter:

| Flag | argparse default | Production value | Set by |
|---|---|---|---|
| `--workers` | `1` | `6` | launcher, not sdc-sync |
| `--workers-budget` | `0` (disabled) | `24` | launcher, not sdc-sync |
| `--normalize-wikitext` / `--no-normalize-wikitext` | on (`default=True`, `BooleanOptionalAction`) | on | — |
| `--migrate-legacy` | `False` | `False` | — |

The production `6` / `24` figures come from the launcher `scripts/wikimedia_launch.py` (`--workers default="6"`, `--workers-budget default="24"`) and `.github/workflows/wikimedia-launch.yml`, which always pass them explicitly — **not** from sdc-sync's own defaults. Run `sdc-sync --partner <p>` by hand and you get single-worker, no slot budget.

**Partner mode** drives off the S3 sidecars only (no `api.dp.la` calls). It iterates the IDs CSV, reads `sdc.json` + `upload-result.json` + `file-list.txt` per item, picks `UPLOADED` / `SKIPPED` ordinals from `upload-result.json`, and calls `process_one_from_sdc(mediaid, dpla_id, sdc_payload, download_url, page_number)` per ordinal. (Ordinals the uploader left in a non-eligible state are still rescued — see [Eligibility, ordinal rescue & pageid self-heal](#eligibility-ordinal-rescue--pageid-self-heal).)

**Manual modes** (`--file` / `--cat` / `--lists`) build claims at runtime via `parsed()`, fetching the source document either from S3 (`--from-s3 <partner>`) or from `api.dp.la` directly (one retry on network failure). Calls `process_one(mediaid, dpla_id)`. `--file` is repeatable (pass once per title); `--lists` reads every `.txt` file in a directory as a list of titles. Slack-launched runs don't use these modes; they remain supported for one-off operator runs.

**Note — the post-SDC cleanup runs on *every* mode.** Whether the file arrived via partner mode or a manual `--file`/`--cat`/`--lists` run, the per-file lifecycle ends the same way: after SDC sync the dispatcher runs the wikitext-cleanup pass (`_post_sdc_cleanup_for_page`), which strips redundant `{{DPLA metadata}}` params and, on any file still carrying a legacy `{{Artwork}}` template, auto-migrates it. So a manual rerun is *not* a pure SDC-only path the way it once was — see [Post-SDC wikitext normalization](#post-sdc-wikitext-normalization-strip) and [Legacy `{{Artwork}}` migration](#legacy-artwork-migration).

All paths converge on the per-file accumulators + atomic `wbeditentity` dispatcher.

## Parallelism & worker model

Partner mode fans out per-DPLA-item work across a `multiprocessing.Pool` sized by `--workers N`. `_run_partner_mode_parallel(partner, dpla_ids, workers)` builds the pool with an explicit **spawn** context (`multiprocessing.get_context("spawn")`) so workers don't inherit the parent's pywikibot sockets, then dispatches via `pool.imap_unordered(_worker_partner_task, tasks)`. `--workers 1` keeps the original single-process path unchanged.

The unit of parallelism is the DPLA item, never the M-id: every ordinal of every item has a unique MediaInfo entity, so no two workers ever write the same Commons entity.

- **Per-worker pywikibot login.** `_init_partner_worker(...)` runs once per worker as the Pool `initializer`: it builds a fresh `pywikibot.Site("commons", "commons")` and calls `site.login()`, so each worker holds its own authenticated session. It also receives the parent's `hubs` / `rights` / `subject-ids` data and the `--no-normalize-wikitext` / `--workers-budget` settings so workers behave identically to the parent.
- **Per-task tracker deltas.** Each task (`_worker_partner_task`) snapshots the worker's `Tracker` before the item (`tracker.snapshot()`), processes the item, and returns the diff; the parent merges every delta back with `tracker.merge(delta)`. Counters therefore aggregate correctly across workers without shared mutable state.
- **Log aggregation.** Workers route their `logging` records through a `QueueHandler` into a `Manager().Queue`; the parent runs a `logging.handlers.QueueListener` against its real handlers, so all worker output interleaves into one log stream.

Wall-clock scales roughly linearly with `N` up to Commons-side parser-pool headroom — which is exactly what the slot budget below exists to bound.

## Box-wide worker-slot budget

`WorkerSlotBudget` (`ingest_wikimedia/worker_slots.py`) caps concurrent Commons-write load **across every `sdc-sync` and `uploader` process on the host**, not just within one run. It is a set of `N` `fcntl`-flock lock files at `/tmp/sdc-sync-worker-slots/slot-0` … `slot-N-1` (`DEFAULT_SLOT_DIR = "/tmp/sdc-sync-worker-slots"`), where `N` is `--workers-budget`. To start its per-item Commons work a process must `flock` an exclusive lock on one slot file; if all `N` are held it blocks until one frees.

- **Every item checks out a slot — even at `--workers 1`.** Both the parallel path (around each `_process_one_partner_item` call) and the single-process partner loop wrap the per-item work in `with slot_budget.acquire():`. The uploader (`tools/uploader.py`) builds a `WorkerSlotBudget(workers_budget)` and acquires a slot per item too, so **upload sessions and sdc-sync sessions across the whole box cooperatively share the same cap** — the budget protects the MediaWiki parser pool / `maxlag` regardless of how many runs are live or how many workers each launched with.
- **The budget value must be identical across concurrent sessions.** Slots are positional lock files; two sessions launched with different `--workers-budget` would disagree on how many slots exist. Production pins it at `24` for all sessions.
- **`budget <= 0` disables it** — `acquire()` becomes a no-op `contextmanager` (the manual `--file`/`--cat`/`--lists` modes never acquire a slot at all, independent of the budget value).
- **Crash-safe.** `flock` locks release automatically when the holding fd is closed *or the process dies*, so a killed worker never strands a slot — no reaper or cleanup pass is needed.
- **Wait time is measured.** Time spent blocked on a slot is accumulated into the `SDC_SLOT_WAIT_SECONDS` tracker counter and surfaced in the Slack summary, so oversubscription shows up as visible queueing.

## Eligibility, ordinal rescue & pageid self-heal

Whether an ordinal gets an SDC sync is **decoupled from the upload-result status of *this* run.** The uploader writes `UPLOADED` / `SKIPPED` for ordinals it confirmed, but also `NOT_PRESENT` / `INELIGIBLE` / `FAILED` for ordinals it couldn't confirm (broken upstream URL, S3 hiccup, etc.). Those last three don't block data-side maintenance:

- **Commons discovery rescue.** Any ordinal left non-eligible is deferred to a post-loop pass that calls `_find_existing_commons_files_by_dpla_id(dpla_id)` — a CirrusSearch `intitle:` query scoped to namespace 6 (`File:`). If a matching Commons file already exists from a prior run, its title + pageid are grafted onto the eligible set (tagged `discovered_via_dpla_id`) and synced normally. The search is lazy: it only fires when at least one ordinal needs rescuing, so healthy items pay zero extra API cost.
- **Pageid self-heal.** `_resolve_pageid_from_title(...)` repairs sidecars whose recorded pageid is null or `0` by resolving it from the file title, so a malformed `upload-result.json` doesn't strand an otherwise-syncable ordinal.

Ordinals that can't be salvaged are counted distinctly: `SDC_ORDINALS_SKIPPED_ERROR`, `SDC_ORDINALS_SKIPPED_MISSING_ENTITY` (Commons returned `no-such-entity` for the M-id), and `SDC_ORDINALS_SKIPPED_MISSING_PAGEID`. Item-level outcomes split full from partial progress: `_classify_item_outcome` returns `SDC_ITEMS_PARTIALLY_SYNCED` when an item has at least one synced ordinal *and* at least one errored sibling, so a dashboard keying on `SDC_ITEMS_SYNCED` doesn't read mixed-result items as fully healthy.

## The atomic dispatcher

`_submit_per_item_edit(mediaid, dpla_id, summary, new_claims=, reference_updates=, qualifier_updates=, removals=)` is the single chokepoint for every SDC write. It accepts four fragment kinds and bundles them into one `wbeditentity` revision:

| Fragment kind | Shape | When emitted |
|---|---|---|
| `new_claims` | Full claim dict, no `id` | A property that doesn't yet exist on the file |
| `reference_updates` | `{id, type, mainsnak, qualifiers, rank, references}` | The reference refresh (see below) |
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

All properties below are defined in `build_claims_for_doc()` (`ingest_wikimedia/sdc.py`) for new uploads and reconciled by `tools/sdc_sync.py` on re-runs. The `sdc.json` envelope is `{"claims": [...], "ingest_date": "YYYY-MM-DD"}`; the top-level `ingest_date` (the DPLA item's `ingestDate`) anchors the P813 reference date. `process_one_from_sdc` raises if it is absent, so sidecars staged before P813 was pinned to the ingest date must be regenerated with a fresh `get-ids-*` run.

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
    "P813": [<retrieved on = the item's DPLA ingestDate, precision day>]
  }
}
```

P813 is pinned to the item's DPLA `ingestDate` (via `ingest_wikimedia.sdc.ingest_date_from_doc`), not the sync run's date, so a re-sync of unchanged partner data leaves the reference byte-identical.

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

**The `time` branch and the somevalue → value-typed migration.** The `time` kind handles `P571` when `parse_dpla_date` produced a value-typed date, mirroring the `item`/`string`/`somevalue` branches above. Crucially, a Commons statement carrying the *old* `somevalue + P1932` date shape does **not** match the new value-typed time claim, so `check()` returns "add" for the value-typed claim — and on the same reconcile cycle the stale `somevalue` statement is queued for removal by `_reconcile_existing_claims` (its `P1932` verbatim string is no longer in `expected` once the `sdc.json` carries the value-typed equivalent). One pass migrates the file from the old shape to the new without ever leaving the date duplicated. A malformed Commons time datavalue (missing `time`/`precision`) is treated as non-matching so the bad statement is replaced rather than crashing the ordinal.

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

## The reference refresh (P813 + canonical repair)

When the dispatcher is making *any* other edit on a file, it also rewrites each DPLA-authored claim's DPLA reference to the full canonical `P854` + `P123` + `P813` triple — repairing a partial or stale DPLA reference (e.g. a foreign-bot claim that carries `P123` but no `P854`) and collapsing a duplicate second DPLA reference, all in the same revision. `P813` (retrieved on) is pinned to the item's DPLA `ingestDate`, **not** `datetime.date.today()`. Because re-asserting an already-canonical reference is a Wikibase no-op, back-to-back syncs of unchanged partner data produce no reference churn at all.

`_build_reference_refresh_fragments(mediaid, dpla_id, already_touched_ids, ingest_date)`:

1. Receives the item's `ingest_date`; the dispatcher reads it from `_current_ingest_date` (set by the entry point, ultimately from `ingest_wikimedia.sdc.ingest_date_from_doc`) via `_require_ingest_date()`.
2. Walks every statement on the file's MediaInfo entity.
3. Skips statements that are *already* being touched by another fragment (`already_touched_ids` from the dispatcher) and statements with no references.
4. Per-statement, walks the existing references list: keeps user-added (non-DPLA) references verbatim; keeps an already-canonical DPLA reference (`_dpla_reference_is_canonical` — correct `P854` and `P813` == `ingestDate`) verbatim; rebuilds a non-canonical DPLA reference via `_build_dpla_reference`; drops any duplicate second DPLA reference.
5. Emits a `reference_updates` fragment per modified statement — full statement shape (`mainsnak`/`qualifiers`/`rank`/new `references`) because Wikibase's partial-update semantics require it.

**Conditional.** `_build_reference_refresh_fragments` is only invoked when the per-file dispatcher already has other edits to make (`has_any_other_edit`). Files where nothing else changed do *not* generate spurious revisions just to touch references.

**Cosmetic note on diffs.** When a reference actually changes, Wikibase's diff renderer shows the entire reference (URL, publisher, retrieved date) twice — once removed, once added — because reference identity is content-hash, not a stable ID. The data is correct; the diff is just noisy. Because `P813` now tracks the item's `ingestDate` rather than `today()`, that noisy diff only appears on a real DPLA-side change, not on every re-sync. Switching to `wbsetreference` for in-place reference updates is theoretically possible but would break the atomic-bundle property (separate API calls per refreshed reference) without changing the visual diff in any documented way.

## Legacy `{{Artwork}}` migration

Some early DPLA uploads used the `{{Artwork}}` template instead of `{{DPLA metadata}}`. `ingest_wikimedia/legacy_artwork.py` migrates them. It reaches files two ways:

- **As a standalone sub-command.** `--migrate-legacy` (with `--partner`) runs `_run_legacy_migration_mode` instead of an SDC sync — it walks the partner's IDs CSV and migrates every legacy file.
- **Opportunistically, on every mode.** The post-SDC cleanup dispatcher (`_post_sdc_cleanup_for_page`) inspects each file's wikitext after sync; if `legacy_artwork.find_legacy_template(text)` finds a `{{Artwork}}` invocation still present, it auto-migrates that file inline. So legacy files surface and convert during ordinary partner runs, not only under `--migrate-legacy`.

**Preserving community edits.** A naive overwrite would clobber translations and corrections made by Commons editors after the original DPLA upload. The migrator walks the file's revision history (`fetch_revision_snapshots` → `trace_param_provenance` → `classify_param_provenance`) to label each `{{Artwork}}` parameter value as **DPLA-bot-authored** (members of `DPLA_BOT_ACCOUNTS`, overwrite-safe) or **community-contributed**. Community values that differ from the canonical DPLA value are not discarded — they're preserved as SDC import statements carrying a distinct reference shape:

```
P887 (based on heuristic) → Q131783016 (inferred from Wikitext)
P4656 (Wikimedia import URL) → <revision permalink>
```

so the provenance of the editor's contribution survives on the entity.

**Write order is load-bearing.** The SDC import is POSTed **first**, the wikitext rewrite **second**. If the process dies between them, the file stays in legacy `{{Artwork}}` form (it'll be retried) — the reverse order would irrecoverably lose the community values before they were captured in SDC.

**Idempotency.** `entity_was_already_migrated` returns true when any mapped property already carries the legacy-import reference signature (`P887 → Q131783016`), so a re-run is a cheap no-op rather than a double migration.

**Counters** (in `ingest_wikimedia/tracker.py`): `LEGACY_MIGRATED`, `LEGACY_IMPORTS_POSTED`, `LEGACY_SKIPPED_NOT_LEGACY`, `LEGACY_SKIPPED_ALREADY`, `LEGACY_SKIPPED_ERROR`.

## Post-SDC wikitext normalization (strip)

`ingest_wikimedia/wikitext_normalize.py` is the final step of the per-file lifecycle: **upload → SDC → wikitext cleanup.** It is **on by default** (`--normalize-wikitext` / `--no-normalize-wikitext`, a `BooleanOptionalAction`) and runs on every trigger mode via `_post_sdc_cleanup_for_page`.

Once the SDC the bot just wrote makes a `{{DPLA metadata}}` template param redundant, the template param is pure duplication on the file page. `normalize(wikitext, expected_params)` strips each scalar param (`title`, `description`, `date`, `creator`, `hub`, `institution`, `url`, `dpla_id`, `local_id`, `permission`) whose verbatim value matches the canonical DPLA value. It is conservative on every edge case — missing template, multiple templates, an unrecognised wrapping template, a param it can't compare — all leave that param untouched, because the cost of an un-stripped match is just a redundant param the next pass catches, while the cost of a wrong strip is data loss.

**Safety guard against stripping the wrong file.** Before stripping anything, the dispatcher confirms the entity actually carries DPLA-attributed SDC via `_entity_has_dpla_attributed_claims` (a statement with the `P459 = Q61848113` qualifier — the same "this is OUR claim" marker described above). If the entity has no DPLA-attributed claims, the strip is refused: there's nothing the wikitext would be redundant *with*.

**Per-item language unwrapping.** For non-English items, an editor may have wrapped the canonical value in a single-language template (e.g. `{{es|<canonical Spanish title>}}`). `_value_matches` unwraps such a `{{<code>|...}}` wrapper before comparing — but **only** if `<code>` is in the per-item allowlist of safe-to-unwrap codes. That allowlist (`_extract_unwrap_languages` in `ingest_wikimedia/wikimedia.py`) always seeds `en` and adds any ISO-639-1 codes derived from the record's `sourceResource.language`. So `{{es|…}}` is unwrapped (and the param stripped) on a Spanish-declared item, but on an English item `{{es|A Title}}` survives even if the inner text byte-matches the canonical English — there the `es` tag is editor-contributed translation metadata, not a redundant wrapper. `{{LangSwitch|…}}` and other non-language-wrapper templates are always treated as a mismatch and preserved.

> Note: this unwrapping affects **comparison only**. The SDC monolingualtext claims the bot writes (`P1476`, `P10358`, …) are still hardcoded `language="en"` — see `_build_monolingual_claim` in `ingest_wikimedia/sdc.py`. Don't infer that the written claim language tracks the source language.

**`{{other date}}` expansion + year-range dedup.** On re-sync, `_reconcile_inferred_from_wikitext_dupes(mediaid)` (called from `process_one_from_sdc`) removes inferred-from-Wikitext date claims that have become redundant with a DPLA-attributed date claim. The comparison expands `{{other date}}` invocations (`parse_other_date_template`) and treats year-range equivalence (`parse_date_range`, both in `sdc.py`) as a match — so an inferred date claim whose comparable value equals a DPLA-attributed claim is pruned even when the two were written in different syntaxes. It also prunes an inferred statement whose value is a `"; "`-joined concatenation of several DPLA values (typical of a multi-valued `description`/P10358): the reconciler splits the inferred text on `"; "` and removes it when every folded piece matches a DPLA-attributed value for the same property (`_inferred_multi_value_matches_dpla_set`). Comparison first unescapes MediaWiki magic words such as `{{!}}` (`casefold_for_compare` → `unescape_wikitext_magic_words`) so an escaped pipe matches the literal DPLA value.

## Wikibase API gotchas

Three rules that caused real production incidents:

### Non-removal fragments must carry `type: "statement"`

Without it, Wikibase rejects the entire bundle with `invalid-claim: Type is missing`. Because the dispatcher is atomic, ONE malformed fragment drops every other edit. The dispatcher backstops this by stamping `type: "statement"` on every non-removal fragment that's missing it.

### Non-removal fragments must carry `mainsnak`

`wbeditentity` treats every non-removal claim entry as a wholesale-replace operation, so the fragment must carry the full statement context (`mainsnak`, `rank`, `qualifiers`, `references`) — not just the field being amended. Partial fragments shaped `{id, type, qualifiers}` or `{id, type, references}` are rejected with `invalid-claim: Attribute "mainsnak" is missing`, and atomicity drops every other edit with them.

The two fragment builders (`_build_qualifier_update_fragments`, `_build_reference_refresh_fragments`) copy `mainsnak` / `rank` / the sibling field from the cached statement so the diff is minimal but the fragment is complete. Deep-copies guard against subsequent helpers mutating shared cache state.

### Qualifier wholesale-replace semantics

`wbeditentity` replaces the entire qualifier set when a `qualifiers` field is included. The dispatcher merges new qualifier snaks with existing qualifier set (preserving snak hashes so Wikibase recognises unchanged snaks and doesn't dirty them in the diff). `_merge_qualifier_snaks` and `_exclude_qualifier_snaks` are the two transform helpers.

The hash-preserving merge is what keeps a P2699 backfill from re-stamping every existing P973/P137/P459 qualifier as "changed" — the existing snaks come back with their pre-existing hashes, Wikibase recognises them, and the diff shows only the new P2699 snak.

### Regression test

The shape contract — every hand-built dict in `wbeditentity`'s `data.claims` list that isn't a removal must carry `type: "statement"` AND `mainsnak` — is asserted by the regression test `test_flush_emits_type_statement_on_every_non_removal_fragment`. Tests that mock `_submit_sdc_write` don't catch this class of bug because they never inspect the actual payload shape; the regression test exercises a realistic mix of fragment kinds (new claim, qualifier amend, P813 refresh, removal) and asserts both fields are present on every non-removal entry of the bundle. New fragment builders should be added to the same scenario so the contract stays enforced.
{% endraw %}
