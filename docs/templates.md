# Templates and the Lua Module

How the pipeline uses Commons templates to display item metadata — and how the planned transition from the legacy `{{Artwork}}`-based wikitext to the SDC-backed `{{DPLA metadata}}` will work.

This document is the *pipeline's* view of the templates. For the user-facing template documentation (what editors see and how they augment it), see the [Template:DPLA metadata/doc](https://commons.wikimedia.org/wiki/Template:DPLA_metadata/doc) page on Commons.

## Current state: `{{DPLA metadata}}` with explicit params at upload, then SDC

As of [PR #291](https://github.com/dpla/ingest-wikimedia/pull/291), the uploader writes a fully-rendered `{{DPLA metadata}}` block as the file page's wikitext at upload time. After SDC sync lands the same metadata as MediaInfo statements, the explicit params and the SDC carry the same values; the file page renders both side-by-side (yellow-box from params + blue-box from SDC) until the planned cleanup pass strips the now-redundant params (see [Planned transition](#planned-transition) below).

### Upload-time wikitext

`get_wiki_text(dpla_id, item_metadata, provider, data_provider)` in `ingest_wikimedia/wikimedia.py` composes the file-page wikitext. The current template literal:

```wikitext
== {{int:filedesc}} ==
{{ DPLA metadata
   | Other fields 1 = {{ InFi | Creator | <creator> | id=fileinfotpl_aut }}
   | title       = <title>
   | description = <description>
   | date        = <date_string>
   | permission  = {{<permissions-template>}}
   | source      = {{ DPLA
                       | <data-provider>
                       | hub      = <provider>
                       | url      = <isShownAt>
                       | dpla_id  = <dpla_id>
                       | local_id = <local_id>
                   }}
   | Institution = {{ Institution | wikidata = <data-provider-qid> }}
}}
```

Notable wiring:

- The wrapper template is `{{DPLA metadata}}` (the DPLA-owned template backed by `Module:DPLA`), not `{{Artwork}}`. PR #291 swapped just the wrapper name; the inner parameter set is the same one the prior `{{Artwork}}`-based uploader had been emitting.
- The `| Other fields 1` row is included only when a creator is present. It uses the same `{{InFi|Creator|…}}` idiom Artwork had, which the DPLA template renders compatibly. Once SDC sync lands the value as a creator statement, the cleanup pass would replace this row with a top-level `|creator=` param (yellow-box-compatible) before stripping it entirely.
- `| source = {{DPLA|...}}` is the DPLA-specific source sub-template. `Template:DPLA` on Commons renders DPLA's catalog link, hub attribution, and local identifier inside the source slot. This `Template:DPLA` is distinct from `Module:DPLA` (the Lua module backing `{{DPLA metadata}}`); the sub-template stays for legacy pages even after the SDC sync lands the same data as `P7482` / `P760` / `P9126` claims.
- `| permission = {{<permissions-template>}}` resolves to one of `NKC`, `NoC-US`, `PD-US`, `cc-zero`, or a CC-by/by-sa code per `license_to_markup_code()` (mapping is in `ingest_wikimedia/wikimedia.py`).
- `| Institution = {{Institution|wikidata=...}}` uses Commons' standard `{{Institution}}` template for the institution row.

### Files uploaded before PR #291

Pages already on Commons keep their `{{Artwork}}` blocks until they're individually edited. There's no backfill pass yet; `Module:DPLA` happily renders an `{{Artwork}}`-wikitext file's SDC the same way it renders a `{{DPLA metadata}}`-wikitext file's, so the visible difference is only in the wikitext-tab view, not the rendered file description.

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

## Planned transition

The roadmap is to ship `{{DPLA metadata}}` as the *primary* format the pipeline writes at upload time, replacing the `{{Artwork}}`-based wikitext block. Two reasons:

1. **Single source of truth.** Today the metadata is duplicated — once as wikitext templated by the uploader, once as SDC reconciled by the sync phase. Switching the primary display to `{{DPLA metadata}}` means the SDC IS the metadata, full stop. The file-page wikitext becomes a bare `{{DPLA metadata}}` invocation that picks everything up from SDC.
2. **Live updates.** When `Module:DPLA` reads SDC, every file gets the current rendering of the current data — no need to re-edit thousands of wikitext blobs to push out a display change.

### Per-file lifecycle

Three edits per file, in order:

1. **Upload — *done* (PR #291).** The uploader writes `{{DPLA metadata}}` with explicit wikitext parameters: `title`, `description`, `date`, `permission`, `source` (as a `{{DPLA|...}}` sub-template carrying catalog link, hub attribution, local identifier), `Institution`, plus a `{{InFi|Creator|…}}` row in `Other fields 1` when a creator is known. Wikitext params are necessary at this step because MediaWiki's [upload API](https://www.mediawiki.org/wiki/API:Upload) doesn't allow SDC statements to be attached in the same request, and Commons will not tolerate a file landing with no readable description even briefly. The Lua module renders these params in the yellow box on a fresh upload (with no SDC populated yet).
2. **SDC sync — *done* (existing phase).** A subsequent edit posts the same metadata as MediaInfo statements via `wbeditentity`. After this edit, the wikitext params and the SDC contain the same values — the displayed page now has the SDC-driven blue box *and* the param-driven yellow box, both rendering identical content.
3. **Cleanup edit — *planned*.** A one-time follow-up edit will strip the now-redundant wikitext params, leaving a bare `{{DPLA metadata}}` invocation plus the licensing template. From this point on the file's display is entirely SDC-driven, and any future DPLA sync that updates the source data flows through to the rendered page automatically — no wikitext re-edit needed.

Step 3 matters because explicit wikitext params *override* SDC on display (see [`Template:DPLA metadata/doc`](https://commons.wikimedia.org/wiki/Template:DPLA_metadata/doc) on Commons). Leaving stale params in place would mask any future SDC corrections. As of June 2026, files uploaded by PR #291 are in the intermediate "step 1 done, awaiting step 3" state.

### Adoption of community-uploaded files

The duplicate-detection logic already handles a related case: a file from a DPLA partner that some Commons editor has already uploaded directly (typically from the partner's online catalog or from Flickr). When the pipeline detects this overlap and adopts the file into DPLA's system — renaming it to the canonical `... - DPLA - <id>.<ext>` form — the planned workflow migrates whatever metadata the original uploader recorded in the existing wikitext into `{{DPLA metadata}}`'s yellow-box parameters. SDC sync then layers the DPLA-authoritative values on top: blue box shows DPLA, yellow box preserves the migrated editor data verbatim.

The migration step is not yet implemented; today's duplicate-detection flow handles the rename and the `{{Duplicate}}` tagging of the original (see [special-cases.md](special-cases.md#hash-drift-the-four-cases)), but leaves the wikitext at the new title in its `{{Artwork}}` form.

### Code changes — current and remaining

**Done in PR #291:** `ingest_wikimedia/wikimedia.py::get_wiki_text` was switched from `{{Artwork}}` to `{{DPLA metadata}}` as the wrapper template. The inner parameter set kept the same shape (title / description / date / permission / source-via-`{{DPLA|...}}` / Institution / Creator-via-`InFi`), so no rewrite was needed — just the one-line wrapper-name change. The yellow user-contributed box on a fresh upload is now populated by these params automatically; SDC sync layers the blue box on top in the subsequent phase.

The template also accepts `author` and `artist` as aliases for `creator` (for editor familiarity with `{{Information}}` and `{{Artwork}}` conventions); `creator` is preferred because of the [archival-records sense the SAA assigns to it](https://dictionary.archivists.org/entry/creator.html), which matches DPLA's source collections better than "Author."

**Remaining for step 3 (cleanup):** a new maintenance pass (e.g. `tools/strip_redundant_params.py`) that runs at some interval after SDC sync, scans for files where SDC and wikitext params agree, and strips the params, leaving `{{DPLA metadata}}` as a bare invocation. The licensing template stays separate because Commons' file-curation conventions require the license to remain visible in the wikitext (not just SDC) for human review.

### Workstreams the transition would touch

- ~~`ingest_wikimedia/wikimedia.py::get_wiki_text` — new template body with explicit params.~~ Done in PR #291.
- A new maintenance pass (e.g. `tools/strip_redundant_params.py`) for step 3.
- The adoption-migration path in `tools/uploader.py::Uploader._resolve_hash_drift` — extend the rename branches to salvage the original editor's wikitext into yellow-box params.
- Commons-side [`Template:DPLA metadata/doc`](https://commons.wikimedia.org/wiki/Template:DPLA_metadata/doc) — already documents the planned lifecycle.
- `Commons:Digital Public Library of America/Modeling` — already updated to describe the SDC properties the module reads.
- Operator runbook in [operations.md](operations.md) — note the change in the "what's on a fresh upload" wording.
- A staged rollout — flip one hub at a time so a regression doesn't take down every partner.

No code changes have landed for this transition yet; no TODO/FIXME comments in `tools/uploader.py` or `ingest_wikimedia/wikimedia.py` reference it.

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

In the planned future state (step 3 of the lifecycle), the left branch's wikitext params get stripped — the file page wikitext becomes a bare `{{DPLA metadata}}` invocation, and all displayed metadata comes from the SDC sync's writes via `Module:DPLA`. Until that pass exists, the explicit params and the SDC carry redundant copies of the same values.
