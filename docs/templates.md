{% raw %}
# Templates and the Lua Module

How the pipeline uses Commons templates to display item metadata — and how the transition from the legacy `{{Artwork}}`-based wikitext to the SDC-backed `{{DPLA metadata}}` works.

This document is the *pipeline's* view of the templates. For the user-facing template documentation (what editors see and how they augment it), see the [Template:DPLA metadata/doc](https://commons.wikimedia.org/wiki/Template:DPLA_metadata/doc) page on Commons.

## Current state: flat-param `{{DPLA metadata}}` at upload, then SDC, then strip

The uploader writes a fully-rendered `{{DPLA metadata}}` block as the file page's wikitext at upload time. After SDC sync lands the same metadata as MediaInfo statements, the explicit params and the SDC carry the same values; the file page renders both side-by-side (yellow box from params + blue box from SDC). A post-SDC cleanup pass then strips the now-redundant params — this pass is implemented and on by default (see [Per-file lifecycle](#per-file-lifecycle) below), not a future plan.

### Upload-time wikitext

`get_wiki_text(dpla_id, item_metadata, provider, data_provider)` in `ingest_wikimedia/wikimedia.py` composes the file-page wikitext. The shape is **flat**: every param is a top-level scalar — there are no `{{InFi}}` / `{{DPLA}}` / `{{Institution}}` sub-templates inside the block. The template literal:

```wikitext
== {{int:filedesc}} ==

{{DPLA metadata
| creator = <creator>
| title = <title>
| description = <description>
| date = <date_string>
| permission = {{<permissions-template>}}
| hub = <provider-qid>
| institution = <data-provider-qid>
| url = <isShownAt>
| dpla_id = <dpla_id>
| local_id = <local_id>
}}
```

Notable wiring:

- The wrapper template is `{{DPLA metadata}}` (the DPLA-owned template backed by `Module:DPLA`), not `{{Artwork}}`.
- **The values are computed by `dpla_metadata_params(dpla_id, item_metadata, provider, data_provider)`, the single source of truth feeding both the writer (`get_wiki_text`) and the post-SDC comparator (`wikitext_normalize`).** Both sides derive their expectations from this one helper, so the value the uploader emits and the value the strip pass tests against can never drift.
- **Why flat.** The previous nested sub-templates (`{{DPLA|...}}` in `source`, `{{Institution|...}}`, `{{InFi|Creator|…}}` in `Other fields 1`) emitted wikitext-table markup that, when it landed inside `Module:DPLA`'s HTML `<td>`, produced a table-syntax-in-cell rendering bug. Collapsing to flat scalars — which the module's yellow box reads directly via the same parametric helpers that drive the blue box — eliminates that bug.
- The `| creator` row is **suppressed entirely** when DPLA has no creator string, to avoid a blank `creator =` row. The template also accepts `author` and `artist` as aliases for `creator` (editor familiarity with `{{Information}}` / `{{Artwork}}` conventions); `creator` is preferred for the [archival-records sense the SAA assigns to it](https://dictionary.archivists.org/entry/creator.html), which matches DPLA's source collections better than "Author."
- `| permission` resolves to one of `NKC`, `NoC-US`, `PD-US`, `cc-zero`, or a CC-by/by-sa code per `license_to_markup_code()` (mapping is in `ingest_wikimedia/wikimedia.py`), wrapped as `{{...}}`. When the rights URI is unmapped, the param renders **empty** (`| permission =`) rather than a malformed `{{}}` invocation.
- `| hub` and `| institution` are Wikidata Q-IDs (provider and data-provider respectively). The institution Q-ID doubles as the Institution-row driver: `Module:DPLA` expands `{{Institution|wikidata=<inst>}}` on its side, so no `{{Institution}}` sub-template appears in the wikitext.
- `| url` / `| dpla_id` / `| local_id` carry DPLA's catalog link, the DPLA item id, and the contributor's local identifier as plain strings. The DPLA-specific source rendering (catalog link, hub attribution, local identifier) is done by `Module:DPLA` from these scalars, not by a `{{DPLA|...}}` sub-template in the wikitext. (`Template:DPLA`, the source sub-template, is distinct from `Module:DPLA`; it still appears on un-migrated legacy pages — see below.)

### Files uploaded in the legacy `{{Artwork}}` form

Older pages on Commons carry `{{Artwork}}` blocks (or the older nested-`{{DPLA metadata}}` sub-template form). `Module:DPLA` renders an `{{Artwork}}`-wikitext file's SDC the same way it renders a flat-`{{DPLA metadata}}` file's, so the visible difference is only in the wikitext-tab view, not the rendered file description. The legacy `{{Artwork}}` → `{{DPLA metadata}}` migration is implemented and runs from the post-SDC cleanup dispatcher (see [Legacy migration](#legacy-artwork--dpla-metadata-migration-implemented) below).

### Post-upload rendering: `{{DPLA metadata}}` + `Module:DPLA`

`Template:DPLA metadata` on Commons is a thin wrapper:

```wikitext
{{#invoke:DPLA|render_metadata_table}}
```

`Module:DPLA` reads the file's MediaInfo entity directly and renders a two-box block:

- **Blue box** — every field is sourced from a DPLA-determined SDC statement (recognised by the `P459 = Q61848113` qualifier). Updated automatically when the SDC sync rewrites; no editor intervention.
- **Yellow box** — augmented from explicit template parameters (Artwork/Information-style names) plus any non-DPLA SDC statements on the same properties. The yellow box gives Commons editors a place to add corrections, translations, or supplementary metadata without conflicting with the bot's authoritative blue-box values.

The module also writes three tracking categories per file with DPLA SDC: the DPLA umbrella category, the regional hub category (e.g. `Plains to Peaks Collective`), and the contributing institution category (e.g. `Denver Public Library`). When the hub partner is the institution itself (NARA, Smithsonian), the hub-role P9126 statement is omitted by the bot and the module falls back to the institution Q-ID for the hub category — preventing the "unknown partner" maintenance category for these special cases.

The module supports chunked claims (P1545 series ordinals) and reassembles them at render time. See [sdc-sync.md](sdc-sync.md#chunked-claims).

### Where `Module:DPLA` is referenced in this repo

The module's existence is acknowledged in a handful of code comments inside the SDC builders:

- `ingest_wikimedia/sdc.py` — chunked-claim convention notes ("so the Lua [module] can reassemble the chunks").
- `tools/sdc_sync.py` — same chunking notes; "unchunked variants of the same text are kept separate so the Lua template..."

The pipeline does NOT directly invoke the module, render it client-side, or test it. Module:DPLA lives on Commons and is maintained as a Wikimedia-side artefact; this repo's responsibility is to write SDC the module can render correctly. See [this test file on Commons](https://commons.wikimedia.org/wiki/File:Plate_49_of_King%27s_1933_aerial_coverage_of_Denver,_Colorado_-_DPLA_-_4c3f5ad9bfac4097b95c9f8deb8e1a21.jpg) for a live render.

## The per-file lifecycle and the strip/migrate pass

The goal is for SDC to *be* the metadata: once a file's display is entirely SDC-driven, any future DPLA sync that updates the source data flows through to the rendered page automatically via `Module:DPLA`, with no wikitext re-edit needed across thousands of files. Two forces make this work — wikitext params are needed at upload (the [upload API](https://www.mediawiki.org/wiki/API:Upload) can't attach SDC in the same request, and Commons won't tolerate a file landing with no readable description even briefly), but stale params left behind would *override* SDC on display and mask future corrections, so they have to be stripped once the SDC counterpart exists.

### Per-file lifecycle

Three edits per file, in order:

1. **Upload.** The uploader writes flat-param `{{DPLA metadata}}` (the shape above). The module renders these params in the yellow box on a fresh upload, before any SDC is populated.
2. **SDC sync.** A subsequent edit posts the same metadata as MediaInfo statements via `wbeditentity`. After this edit, the wikitext params and the SDC contain the same values — the displayed page now has the SDC-driven blue box *and* the param-driven yellow box, both rendering identical content.
3. **Post-SDC cleanup.** A follow-up edit strips the now-redundant wikitext params, leaving (when every param strips) a bare single-line `{{DPLA metadata}}` invocation plus the licensing template. From this point on the file's display is entirely SDC-driven. The licensing template stays in the wikitext because Commons' file-curation conventions require the license to remain visible there (not just in SDC) for human review.

### The post-SDC cleanup pass (implemented, on by default)

Step 3 is implemented in `ingest_wikimedia/wikitext_normalize.py` and runs from `tools/sdc_sync.py`. It is **on by default** — there is no separate `tools/strip_redundant_params.py`; the logic lives in `wikitext_normalize`.

- **Entry point.** `tools/sdc_sync.py::_post_sdc_cleanup_for_page` is the dispatcher. After the SDC write for a page, it inspects the current wikitext: a flat-`{{DPLA metadata}}` page goes through the strip path (`wikitext_normalize.normalize_page`); a legacy `{{Artwork}}` (or `{{Information}}` / `{{Photograph}}`) page goes through the migration path (see below).
- **Flag.** `--normalize-wikitext` / `--no-normalize-wikitext` (default on). Pass `--no-normalize-wikitext` for diagnostic runs that need the pre-strip wikitext intact. The flag propagates to worker processes in partner mode.
- **What it strips.** `normalize` removes each `{{DPLA metadata}}` param whose value equals the SDC-backed value computed by `dpla_metadata_params` (compared via `_value_matches`). `canonicalize` then enforces the canonical whitespace shape: left-justified template, exactly one blank line after `== {{int:filedesc}} ==`, one `| key = value` per line, closing `}}` on its own line — or, when *every* param has been stripped, collapse to a single-line `{{DPLA metadata}}`.
- **Safety guard.** Before stripping, the dispatcher fetches the file's MediaInfo entity and refuses to strip if it has *no* DPLA-attributed SDC (`_entity_has_dpla_attributed_claims`). This prevents the failure mode where a dropped or null-pageid SDC write would otherwise leave the page with metadata in *neither* representation. An API failure during the guard is also a hard skip.

### The dual-path comparator

`wikitext_normalize` is shape-tolerant: it strips both the **new flat params** and the **old nested sub-template rows**, so old- and new-shape pages converge to the same stripped output. The legacy-row strippers — `_strip_legacy_source` (`source = {{DPLA|...}}`), `_strip_legacy_institution` (`Institution = {{Institution|wikidata=...}}`), and `_strip_legacy_creator` (`Other fields 1 = {{InFi|Creator|…}}`) — parse each sub-template's inner params and compare them against the flat-canonical equivalents via `_value_matches`. Any unexpected extra arg on the wikitext side disqualifies the strip (an editor may have added something the comparator doesn't know to match).

Several finer behaviours of the comparator:

- **Language-wrapper unwrapping.** A non-rendered `languages` key on the params dict (built by `_extract_unwrap_languages`) carries the per-item allowlist of ISO 639-1 codes the comparator may safely unwrap — always `en`, plus any language the DPLA record itself declares in `sourceResource.language`. The comparator will unwrap a single-language wrapper like `{{es|...}}` only for an allowlisted language; `{{LangSwitch}}` and any unknown/un-allowlisted wrapper are always preserved (the file was deliberately multilingualised by an editor).
- **Date dedup in migration.** When migrating, an editor's `{{other date|...}}` / circa / year-range value is expanded server-side (`_expand_wikitext_for_date_parse`, via MediaWiki `expandtemplates`) and checked against DPLA's existing `P571` (`_existing_dpla_date_matches_parsed` / `_existing_dpla_date_range_matches`) so a value like `{{other date|between|1934|1948}}` doesn't get imported as a parallel statement alongside the DPLA date it already matches.
- **Magic-word unescaping.** A MediaWiki character-escape magic word like `{{!}}` (the escaped form of a literal `|` inside a template param, e.g. from an AWB pass) is unescaped (`unescape_wikitext_magic_words`) both when parsing the legacy params and inside the comparator key (`casefold_for_compare`), so a community edit that only rewrote a raw `|` to `{{!}}` isn't imported as a diverging value.
- **Pipe-truncation repair.** `parse_artwork_params` stitches a `| description = A | B | C` value back together after `mwparserfromhell` splits it into positional overflow fragments at each `|`, so the same AWB `{{!}}`-vs-`|` rewrite doesn't read as a content change to the provenance walker.
- **Multi-value subset.** A `"; "`-joined multi-value param (e.g. a `description` list) counts as DPLA-originated when its value-set is a subset of DPLA's canonical list (`_multi_value_subset_of_canonical`), tolerating DPLA-side add/drop/reorder drift since upload rather than importing it as a community contribution.

### Legacy `{{Artwork}}` → `{{DPLA metadata}}` migration (implemented)

The migration is implemented in `ingest_wikimedia/legacy_artwork.py` (`migrate_legacy_file`, `plan_migration`, `build_legacy_import_claims`) and runs via the `--migrate-legacy` mode of `tools/sdc_sync.py` as well as from the post-SDC cleanup dispatcher when it encounters a legacy template.

The problem it solves: a legacy file may carry community edits made years after the original DPLA upload. A naïve overwrite would discard them. So `plan_migration` walks the file's revision history to separate values DPLA's bot last touched (safe to overwrite with canonical data) from values a community editor contributed (must be preserved). Community values are preserved as SDC *imports* — statements carrying a `P887 → Q131783016` ("inferred from Wikitext") + `P4656 → <permalink-to-source-revision>` reference shape, deliberately *without* the standard DPLA qualifiers (`P459/Q61848113`, `P813`) that would misrepresent them as DPLA-sourced. Community-curated *creator* contributions get special handling: a legacy `Other fields N = {{InFi|Creator|…}}` value wrapping a `{{Creator:Foo}}` page or `{{creator|Wikidata=Q…}}` (unwrapped by `parse_artwork_params`) is preserved as a `P170` import — resolved to a Wikidata Q-ID where possible (`_resolve_commons_creator_qid`), or falling back to a `P170` `somevalue` + `P2093` stated-as claim (`materialize_pending_creator_claim`) — so an editor's curated creator survives the `{{Artwork}}` → `{{DPLA metadata}}` swap rather than being silently dropped.

The order is load-bearing and crash-safe: **SDC import first, wikitext rewrite second.** If the rewrite fails after the import succeeded, the file carries the imported SDC but still has the legacy template, and a follow-up sweep completes it; the reverse order could irrecoverably lose community values. An `entity_was_already_migrated` idempotency guard (it looks for the `P887 → Q131783016` reference shape on the entity) makes re-runs safe. The migrated wikitext is run through the same strip + `canonicalize` pass as the flat-`{{DPLA metadata}}` path, so a migrated file lands in the post-strip steady state in one save.

One value shape can't become SDC at all: when a community editor *extends* a DPLA-authored param past the DPLA text with structural wikitext (gallery, HR, wikitable, list, embedded template — anything introducing vertical whitespace), Wikibase's monolingual-text validator rejects it. Rather than aborting the whole file's migration, `_split_extension_extras` keeps the DPLA-authored prefix as DPLA-attributed SDC and re-injects only the community remainder verbatim onto the migrated template (`_inject_preserved_extras`), where the yellow-box renderer picks it up.

### Remaining work

The uploader's title-drift rescue paths — the move (`_move_to_correct_title`), the redirect-overwrite (`_resolve_redirect_overwrite`), and the Case-2 upload-and-tag (`_tag_drift_duplicate`) — all now preserve *page-level* community metadata (license tags, `{{Assessment}}` templates, `{{Image extracted}}` parents, category links) via `merge_preserved_wikitext`. One narrow gap remains: none of them yet preserve community-contributed `{{DPLA metadata}}` *template params* as provenance-aware SDC imports — they overwrite the metadata template wholesale, discarding any params an editor added between the original upload and the rescue. A `TODO` on `merge_preserved_wikitext` (in `ingest_wikimedia/wikimedia.py`) flags the integration point: the `legacy_artwork` provenance logic should import community param values as SDC first, then overwrite. (The rename and `{{Duplicate}}` tagging of the original work today — see [special-cases.md](special-cases.md#hash-drift-the-four-cases).)

## How SDC and templates interact today

A summary diagram:

```text
            ┌──────────────────────────────────────────────┐
            │  S3 sidecars (from get-ids-es, Phase 1)      │
            │    dpla-map.json      ──┐                    │
            │    sdc.json           ──┘                    │
            └────────┬─────────────────────────────────────┘
                     │
       ┌─────────────┴────────────┐
       │                          │
       ▼                          ▼
 ┌─────────────────┐       ┌─────────────────────┐
 │ uploader writes │       │ sdc-sync writes     │
 │ {{DPLA metadata}}│      │   MediaInfo SDC     │
 │   wikitext      │       │   (P760, P1476,     │
 │   (file page)   │       │    P195, P9126, …)  │
 └────────┬────────┘       └──────────┬──────────┘
          │                           │
          ▼                           ▼
   File page wikitext           MediaInfo entity
          │                           │
          ▼                           ▼
   ┌──────────────────────────────────────────────┐
   │  Commons file page                           │
   │                                              │
   │  {{DPLA metadata}} block with explicit       │
   │    params (yellow box) + SDC-driven render   │
   │    via Module:DPLA (blue box)                │
   └──────────────────────────────────────────────┘
```

Step 3 of the lifecycle (the post-SDC cleanup pass) then strips the left branch's redundant params — once every param is stripped the file-page wikitext becomes a bare single-line `{{DPLA metadata}}` invocation plus the licensing template, and all displayed metadata comes from the SDC sync's writes via `Module:DPLA`. Between the SDC write and the strip, the explicit params and the SDC carry redundant copies of the same values; the cleanup pass runs in the same `sdc_sync` invocation by default.
{% endraw %}
