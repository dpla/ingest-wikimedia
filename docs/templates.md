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

- **Blue box** — every field is sourced from a DPLA-determined SDC statement (recognised by the `P459 = Q61848113` qualifier). Updated automatically when the SDC sync re-writes; no editor intervention.
- **Yellow box** — augmented from explicit template parameters (Artwork/Information-style names) plus any non-DPLA SDC statements on the same properties. The yellow box gives Commons editors a place to add corrections, translations, or supplementary metadata without conflicting with the bot's authoritative blue-box values.

The module also writes three tracking categories per file with DPLA SDC: the DPLA umbrella category, the regional hub category (e.g. `Plains to Peaks Collective`), and the contributing institution category (e.g. `Denver Public Library`). When the hub partner is the institution itself (NARA, Smithsonian), the hub-role P9126 statement is omitted by the bot and the module falls back to the institution Q-ID for the hub category — preventing the "unknown partner" maintenance category for these special cases.

The module supports chunked claims (P1545 series ordinals) and reassembles them at render time. See [sdc-sync.md](sdc-sync.md#chunked-claims).

### Where `Module:DPLA` is referenced in this repo

The module's existence is acknowledged in a handful of code comments inside the SDC builders:

- `ingest_wikimedia/sdc.py` — chunked-claim convention notes ("so the Lua [module] can reassemble the chunks").
- `tools/sdc_sync.py` — same chunking notes; "unchunked variants of the same text are kept separate so the Lua template..."

The pipeline does NOT directly invoke the module, render it client-side, or test it. Module:DPLA lives on Commons and is maintained as a Wikimedia-side artefact; this repo's responsibility is to write SDC the module can render correctly. See the test file at `commons.wikimedia.org/wiki/File:Plate_49_of_King's_1933_aerial_coverage_of_Denver,_Colorado_-_DPLA_-_4c3f5ad9bfac4097b95c9f8deb8e1a21.jpg` for a live render.

## Planned transition

The roadmap is to ship `{{DPLA metadata}}` as the *primary* format the pipeline writes at upload time, replacing the `{{Artwork}}`-based wikitext block. Two reasons:

1. **Single source of truth.** Today the metadata is duplicated — once as wikitext templated by the uploader, once as SDC reconciled by the sync phase. Switching the primary display to `{{DPLA metadata}}` means the SDC IS the metadata, full stop. The file-page wikitext becomes a one-line `{{DPLA metadata}}` invocation that picks everything up from SDC.
2. **Live updates.** When `Module:DPLA` reads SDC, every file gets the current rendering of the current data — no need to re-edit thousands of wikitext blobs to push out a display change.

### What would need to change in the uploader

`get_wiki_text()` would need to be rewritten to emit (essentially) just `{{DPLA metadata}}` plus the licensing template — the rest comes from SDC. Concretely:

```wikitext
== {{int:filedesc}} ==
{{DPLA metadata}}

== {{int:license-header}} ==
{{<permissions-template>}}
```

The licensing line stays separate because Commons' file curation conventions require the license template to be visible in the wikitext (not just SDC) for human review.

### What would need to change in the phase order

Today the upload phase writes wikitext *before* the SDC phase posts the SDC. If `{{DPLA metadata}}` is the primary format, the wikitext is empty of metadata until SDC arrives — files would temporarily look blank between upload and SDC sync. Two ways to handle this:

1. **Reverse the phase order.** Make the uploader post a placeholder `{{DPLA metadata}}` block at upload, then have a new "SDC pre-stage" phase run *before* the upload commits, writing the SDC into a staging entity. This is doable but invasive — Commons' upload API doesn't allow SDC + file-page in one request, so the staging would still happen post-upload, just before publishing the wikitext.
2. **Pre-populate template parameters.** Use the explicit-param path of `{{DPLA metadata}}` so the uploader writes `{{DPLA metadata|title=...|description=...|...}}` with the same values it would have put into `{{Artwork}}`. The Lua module already supports this (explicit params override SDC fallbacks). Then SDC sync lands the same values as structured data, and over time the params can be dropped from the wikitext as SDC becomes authoritative.

Option 2 is the lower-risk path and is what's currently planned. The wikitext becomes a transition vehicle: explicit params today, gradual dropping of params as SDC coverage stabilises, eventually a bare `{{DPLA metadata}}`.

### Workstreams the transition would touch

- `ingest_wikimedia/wikimedia.py::get_wiki_text` — new template body.
- Commons-side `Template:DPLA metadata/doc` — already updated to describe both the blue (SDC) and yellow (user/explicit-param) boxes.
- `Commons:Digital Public Library of America/Modeling` — already updated to describe the SDC properties the module reads.
- Operator runbook in [operations.md](operations.md) — note the change in the "what's on a fresh upload" wording.
- A staged rollout — flip one hub at a time so a regression doesn't take down every partner.

No code changes have landed for this transition yet; no TODO/FIXME comments in `tools/uploader.py` or `ingest_wikimedia/wikimedia.py` reference it.

## How SDC and templates interact today

A summary diagram:

```
            ┌──────────────────────────────────────────────┐
            │  S3 sidecars (from get-ids-es Phase 3)       │
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
