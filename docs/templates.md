# Templates and the Lua Module

How the pipeline uses Commons templates to display item metadata — and how the planned transition from the legacy `{{Artwork}}`-based wikitext to the SDC-backed `{{DPLA metadata}}` will work.

This document is the *pipeline's* view of the templates. For the user-facing template documentation (what editors see and how they augment it), see the [Template:DPLA metadata/doc](https://commons.wikimedia.org/wiki/Template:DPLA_metadata/doc) page on Commons.

## Current state: `{{Artwork}}` at upload, `{{DPLA metadata}}` from SDC

At upload time, the pipeline writes a fully-rendered `{{Artwork}}` block as the file page's wikitext. After SDC sync lands the same metadata as structured data, that metadata can *also* be rendered by `{{DPLA metadata}}` — but the pipeline does NOT include `{{DPLA metadata}}` in the wikitext it uploads. The two systems run side-by-side; `{{DPLA metadata}}` is currently maintained as a parallel rendering option and would replace `{{Artwork}}` only in a future PR (see [Planned transition](#planned-transition) below).

### Upload-time wikitext (the `{{Artwork}}` path)

`get_wiki_text(dpla_id, item_metadata, provider, data_provider)` in `ingest_wikimedia/wikimedia.py` composes the file-page wikitext. The current template literal:

```wikitext
== {{int:filedesc}} ==
{{ Artwork
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

- The `| Other fields 1` row is included only when a creator is present.
- `| source = {{DPLA|...}}` is the DPLA-specific glue: `Template:DPLA` on Commons renders DPLA's catalog link, hub attribution, and local identifier inside `{{Artwork}}`'s source slot. This `Template:DPLA` is distinct from `Module:DPLA` (the Lua module backing the new `{{DPLA metadata}}` template).
- `| permission = {{<permissions-template>}}` resolves to one of `NKC`, `NoC-US`, `PD-US`, `cc-zero`, or a CC-by/by-sa code per `license_to_markup_code()` (mapping is in `ingest_wikimedia/wikimedia.py`).
- `| Institution = {{Institution|wikidata=...}}` uses Commons' standard `{{Institution}}` template for the institution row.

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

### Per-file lifecycle (planned)

Three edits per file, in order:

1. **Upload.** The uploader writes `{{DPLA metadata}}` with explicit wikitext parameters for every available field (title, description, creator, date, institution, subject, source). Wikitext params are necessary here because MediaWiki's [upload API](https://www.mediawiki.org/wiki/API:Upload) doesn't allow SDC statements to be attached in the same request, and Commons will not tolerate a file landing with no readable description even briefly. The Lua module already supports the explicit-param path (yellow box on a fresh upload, with no SDC populated yet).
2. **SDC sync.** A subsequent edit posts the same metadata as MediaInfo statements via `wbeditentity`. After this edit, the wikitext params and the SDC contain the same values — the displayed page now has the SDC-driven blue box *and* the param-driven yellow box, both rendering identical content.
3. **Cleanup edit.** A one-time follow-up edit strips the now-redundant wikitext params, leaving a bare `{{DPLA metadata}}` invocation. From this point on the file's display is entirely SDC-driven, and any future DPLA sync that updates the source data flows through to the rendered page automatically — no wikitext re-edit needed.

Step 3 matters because explicit wikitext params *override* SDC on display (see [`Template:DPLA metadata/doc`](https://commons.wikimedia.org/wiki/Template:DPLA_metadata/doc) on Commons). Leaving stale params in place would mask any future SDC corrections.

### Adoption of community-uploaded files

The duplicate-detection logic already handles a related case: a file from a DPLA partner that some Commons editor has already uploaded directly (typically from the partner's online catalog or from Flickr). When the pipeline detects this overlap and adopts the file into DPLA's system — renaming it to the canonical `... - DPLA - <id>.<ext>` form — the planned workflow migrates whatever metadata the original uploader recorded in the existing wikitext into `{{DPLA metadata}}`'s yellow-box parameters. SDC sync then layers the DPLA-authoritative values on top: blue box shows DPLA, yellow box preserves the migrated editor data verbatim.

The migration step is not yet implemented; today's duplicate-detection flow handles the rename and the `{{Duplicate}}` tagging of the original (see [special-cases.md](special-cases.md#hash-drift-the-four-cases)), but leaves the wikitext at the new title in its `{{Artwork}}` form.

### Uploader code changes required

`ingest_wikimedia/wikimedia.py::get_wiki_text` would need to be rewritten to emit `{{DPLA metadata}}` with explicit params instead of an `{{Artwork}}` block:

```wikitext
== {{int:filedesc}} ==
{{DPLA metadata
 |title       = ...
 |description = ...
 |author      = ...
 |date        = ...
 |institution = ...
 |subject     = ...
}}

== {{int:license-header}} ==
{{<permissions-template>}}
```

The licensing line stays separate because Commons' file-curation conventions require the license template to be visible in the wikitext (not just SDC) for human review.

The cleanup edit (step 3 of the lifecycle) would be a new piece of code — likely a separate maintenance pass that runs at some interval after SDC sync, scans for files where SDC and wikitext params agree, and strips the params.

### Workstreams the transition would touch

- `ingest_wikimedia/wikimedia.py::get_wiki_text` — new template body with explicit params.
- A new maintenance pass (e.g. `tools/strip_redundant_params.py`) for step 3.
- The adoption-migration path in `tools/uploader.py::_resolve_hash_drift` — extend the rename branches to salvage the original editor's wikitext into yellow-box params.
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
 │   {{Artwork}}   │       │   MediaInfo SDC     │
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
   │  {{Artwork}} block (today's primary display) │
   │  + (optionally) {{DPLA metadata}} block      │
   │    rendered by Module:DPLA from SDC          │
   └──────────────────────────────────────────────┘
```

In the planned future state, the left branch goes away — the uploader writes only `{{DPLA metadata}}`, and everything else comes from the SDC sync's writes via `Module:DPLA`.
