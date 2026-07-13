{% raw %}
# Pipeline Architecture

This document describes how an upload run flows from a Slack slash command through the dispatch chain into a long-running pipeline on EC2, and how the components fit together. For operator-level instructions (how to *use* the pipeline), see [slack-guide.md](slack-guide.md) and [operations.md](operations.md).

## Table of contents

1. [End-to-end picture](#end-to-end-picture)
2. [Why Elasticsearch instead of api.dp.la](#why-elasticsearch-instead-of-apidpla)
3. [The dispatch chain](#the-dispatch-chain)
4. [Target resolution](#target-resolution)
5. [Session naming and concurrency](#session-naming-and-concurrency)
6. [Failure semantics](#failure-semantics)
7. [Component map](#component-map)

---

## End-to-end picture

```text
Slack user
    │ /wikimedia-upload bpl pa
    ▼
AWS Lambda  (wikimedia-slack-dispatch)
    │  HMAC-validates Slack signature
    │  Pre-validates hub slugs / QID format
    │  Computes SHA-256 concurrency key
    │  Returns ack to Slack (<3 s)
    ▼
GitHub Actions  (wikimedia-launch.yml)
    │  scripts/wikimedia_launch.py runs:
    │    • resolves QIDs / DPLA IDs via institutions_v2.json + ES
    │    • re-checks eligibility, conflicts, EC2 memory
    │    • base64-encodes the pipeline shell script
    │    • copies updated source code onto EC2
    ▼
AWS SSM  →  EC2  (i-033eff6c8c168f999, "wiki downloads")
    │  tmux session: wikimedia-bpl+pa
    │  Per target, sequentially:
    │    1. get-ids-es <partner> > <partner>.csv
    │    2. downloader <partner>.csv <partner>
    │    3. uploader   <partner>.csv <partner> --workers-budget 24
    │    4. sdc-sync   --partner <partner> --ids-file <partner>.csv \
    │                    --workers 24 --workers-budget 24
    │    5. drain-deferred --no-wait <partner>   (opportunistic duplicate-tag drain)
    │  Then once per unique partner, at batch end:
    │    6. drain-deferred <partner>             (terminal patient duplicate-tag drain)
    ▼
S3 (s3://dpla-wikimedia/)        Wikimedia Commons
    sidecars + media bytes       file pages + MediaInfo SDC
```

Phase completion notifications and pipeline failures post to **#tech-alerts** via the Tech Reports bot (`DPLA_SLACK_BOT_TOKEN`). User-level errors (unknown hub, invalid target, etc.) come back to the slash-command caller as ephemeral Slack messages.

## Why Elasticsearch instead of api.dp.la

Before March 2026, `tools/get_ids_api.py` enumerated DPLA IDs by paginating the public DPLA API at `api.dp.la`. That tool is now a one-line shim; the cutover (commit `15375ab`) combined two changes:

1. **`tools/get_ids_es.py`** queries DPLA's internal Elasticsearch alias `dpla_alias` at `search-prod1.internal.dp.la:9200` directly. Pagination uses `search_after` with `sort = ["id", "_doc"]` and `size = 500`. Every page is validated for `timed_out=true` or partial-shard failures so a 200 OK with `_shards.failed > 0` cannot silently terminate a long-running paginator. A `SIGALRM`-based 120 s hard timeout catches stalled mid-response reads that `requests.timeout=30` doesn't.
2. **Sidecar staging.** As `get-ids-es` enumerates, it writes the full ES `_source` for each item to S3 as `dpla-map.json`, and pre-computes the file's Wikibase claim envelope into `sdc.json`. The downloader and uploader read these sidecars instead of re-fetching per-item metadata from `api.dp.la`, which removed thousands of round-trips per partner from the hot path.

The result: `get-ids-es` is now both the ID enumerator *and* the metadata pre-stager. The downloader, uploader, and SDC-sync phases no longer talk to `api.dp.la` at all in partner-mode runs. (See [pipeline-phases.md](pipeline-phases.md) and [sidecars.md](sidecars.md) for the details.)

## The dispatch chain

### Step 1 — Lambda (`lambda/wikimedia-slack-dispatch/handler.py`)

The Slack app sends every `/wikimedia-*` slash command to a Lambda Function URL. The handler:

1. **Verifies the request.** Parses `x-slack-request-timestamp` (rejecting anything > 300 s old to defeat replay), builds the canonical signing string `v0:<ts>:<body>`, HMAC-SHA-256s it with `SLACK_SIGNING_SECRET`, and compares against `x-slack-signature` using `hmac.compare_digest`.
2. **Decodes the body.** Slack sends `application/x-www-form-urlencoded`; API-Gateway base64-encodes it. `command`, `text`, and `response_url` are extracted.
3. **Routes by subcommand.** Each subcommand (`upload`, `kill`, `retry`, `sdc`, `refresh`, status) builds the right inputs and posts to `https://api.github.com/repos/dpla/ingest-wikimedia/actions/workflows/<workflow>.yml/dispatches`. The dispatch call has a 2 s timeout — well inside Slack's 3 s ack window.
4. **Pre-validates inputs.** For `/wikimedia-upload`, hub slugs are looked up in the local `PARTNER_HUBS` registry and rejected with an ephemeral Slack error if unknown. QIDs are format-validated against `^Q\d+$` but resolved later (inside the workflow) because the Lambda would have to fetch `institutions_v2.json` to do it here, blowing the 3 s budget.
5. **Computes a concurrency key.** A 16-char SHA-256 hex prefix of the joined target string is passed as `concurrency_key` to the workflow so its concurrency group name stays under GitHub's 400-char limit while still folding equivalent inputs together.

Environment variables: `SLACK_SIGNING_SECRET`, `GH_TOKEN` (fine-grained PAT with `actions:write` on `dpla/ingest-wikimedia`), `GH_REPO` (default `dpla/ingest-wikimedia`).

### Step 2 — GitHub Actions workflow

Four `workflow_dispatch`-only workflows handle the slash-command traffic:

| Workflow | Triggered by | Concurrency group |
|---|---|---|
| `wikimedia-launch.yml` | `/wikimedia-upload`, `/wikimedia-upload sdc`, `/wikimedia-upload refresh`, `/wikimedia-upload maintain` | `wikimedia-launch-${concurrency_key \|\| run_id}` |
| `wikimedia-kill.yml` | `/wikimedia-upload kill` | none — fire-and-forget |
| `wikimedia-retry.yml` | `/wikimedia-upload retry` | `wikimedia-retry-${partner \|\| 'all'}` |
| `wikimedia-upload-status.yml` | `/wikimedia-status` + cron `0 */6 * * *` | fixed `wikimedia-upload-status` |

Each workflow installs Python + `boto3`/`requests`, then invokes a single Python script from `scripts/`. The scripts hold all the orchestration logic — the YAML is intentionally thin.

### Step 3 — `scripts/wikimedia_launch.py`

This is the bulk of the dispatch logic. In order, it:

1. **Parses the `--partner` input** via `shlex.split` and classifies each token as hub slug, `hub|institution` pair, `hub|institution|collection` triple, Wikidata QID, or DPLA ID (32-hex).
2. **Resolves targets.** QIDs are looked up against `institutions_v2.json` (live-fetched from `github.com/dpla/ingestion3/.../wiki/institutions_v2.json`, module-cached so warm Lambda calls don't refetch). DPLA IDs are batched into one SSM call to `resolve-dpla-ids` on EC2, which does an ES `terms` query and applies the full eligibility filter.
3. **Checks EC2 memory.** Aborts when available RAM is below 30 %. Each session uses ~300–500 MB on a 7.6 GB instance, so 30 % free leaves room for 4–5 concurrent sessions.
4. **Detects session conflicts.** Hub-level, institution-level, and collection-level targets define conflict rules so that, e.g., a hub run blocks any institution-level run inside that hub. `force=true` (GH-Actions-manual-only — the Lambda never sets it) kills offending sessions instead of aborting.
5. **Updates EC2 source code (and stages config).** Clones `dpla/ingest-wikimedia` at `GITHUB_SHA` into `/tmp` and runs `cp -r` over `ingest_wikimedia/`, `tools/`, `pyproject.toml`, `uv.lock` onto the EC2 install, then runs `uv sync`. The EC2 install is an editable install, so updated source files take effect immediately. The same SSM command also stages ingestion3's config JSON — it fetches `institutions_v2.json` and `subjects.json` once to `/home/ec2-user/ingest-wikimedia/` (atomically, best-effort) so the many short-lived per-target processes read config local-first from disk instead of each re-fetching from `raw.githubusercontent.com` and tripping its anonymous HTTP-429 rate limit. `partners.load_institutions`/`load_subjects` fall back to a retrying live fetch when the staged file is absent (the Lambda / GitHub Actions path, where no local checkout exists).
6. **Builds the pipeline shell script.** Each target becomes a `cd <base> && get-ids-es ... && downloader ... && uploader ... && sdc-sync ... || { notify_pipeline_fail; }` block; the blocks are joined with `"; "` so one target's failure doesn't abort the batch (only the `&&` chain inside one target).
7. **Launches the tmux session via SSM.** The script is base64-encoded, written to `/tmp/wm-pipeline-<sha1>.sh` on EC2, then started under `tmux new-session -d -s <session_name> -c <cwd> 'bash <script>'`. Base64 avoids SSM's command-length cap (~25 KB) and shell-metacharacter escaping issues — large batches of 20+ targets hit the cap before this change.
8. **Posts a launch confirmation** to #tech-alerts.

### Step 4 — EC2 execution

The tmux session runs detached. Its `setup` prefix exports `WIKIMEDIA_INSTITUTIONS_FILE` and `WIKIMEDIA_SUBJECTS_FILE` once (session-wide, pointing at the launch-staged config on disk) so every phase loads config local-first. Then, for each target block, `bash` exports `WIKIMEDIA_SESSION_LABEL`, `WIKIMEDIA_PARTNER_DIR`, `WIKIMEDIA_TARGET_IS_LAST`, and (for single-item targets) `WIKIMEDIA_SINGLE_ITEM`. These per-target env vars are read by the Slack-notification helpers inside the Python phase tools so completion / failure messages identify the right target.

Phase output is logged to `<partner_dir>/logs/<timestamp>-<label>-<phase>.log` (download / upload / sdc). The status workflow tails these logs to derive progress.

### Maintain mode

`/wikimedia-upload maintain [lite|count] <target> …` dispatches `wikimedia-launch.yml` with `maintain=true` (plus optional `lite`/`count_only`) to reconcile files **already on Commons** for a hub or institution *in place* — no new items are downloaded and no new File pages are created (the uploader runs behind a `--no-create` fence). Because nothing new is uploaded, upload-ineligible (de-opted, `upload:false`) targets are allowed. Three routes:

- **default (hash)** — downloads media and content-reconciles: re-links drifted files, overwrites changed bytes, then SDC.
- **`lite`** — no-download route: SDC-in-place + name-drift rename only.
- **`count`** — read-only pre-flight sizing; writes nothing.

`partners.resolve_wikidata_id(qid, maintain=True)` drops the upload-eligibility filter (see [Target resolution](#target-resolution)) so a de-opted institution's QID still resolves, and the launcher likewise relaxes the id-list scan's institution gate to QID-only. The `ingest_wikimedia/maintain.py` re-link engine resolves the current DPLA ID for a possibly-drifted Commons file (via a provider-agnostic `isShownAt`/wildcard ladder against the live index).

## Target resolution

`/wikimedia-upload` accepts six target forms, mixed freely on the same command:

| Form | Example | Resolves to |
|---|---|---|
| Hub slug | `bpl` | One full-hub target |
| Hub alias | `ma`, `mass` (= `bpl`) | One full-hub target, slug normalised |
| `hub\|institution` | `"indiana\|Indiana State Library"` | One institution-level target |
| `hub\|institution\|collection` | `"indiana\|Indiana State Library\|Cushman Photographs"` | One collection-level target |
| `hub\|\|collection` | `"nara\|\|General Records of the United States Government"` | One hub-wide collection target — the collection across every upload-eligible institution in the hub (empty institution slot) |
| Wikidata QID | `Q72380652` | One or more targets, looked up in `institutions_v2.json` |
| DPLA item ID | `06b558045c5fd4cc5dc697248272159a` | One single-item target (32-hex) |

**Wikidata QID resolution.** `partners.resolve_wikidata_id(qid)` walks every hub in `institutions_v2.json` and returns every **upload-eligible** match — a QID at hub level becomes a full-hub target (kept only if the hub has `upload:true`); a QID at institution level becomes an institution-level target (kept only if the hub or the institution has `upload:true`); a QID matching multiple institutions inside the same hub becomes one combined target with all matching institution names ORed in the ES filter. If a QID matches *both* a hub and one of its own institutions (e.g. an institution that runs its own hub), the hub-level scope wins and the per-institution matches are discarded as strictly narrower. Matches that fail the eligibility filter — e.g. `Q131454` (Library of Congress), listed but not opted in — are dropped and reported back as "not upload-eligible per institutions_v2.json" rather than routed onto the broad `lc` hub. maintain mode (`resolve_wikidata_id(qid, maintain=True)`) deliberately drops the filter so files of de-opted (`upload:false`) institutions can still be reconciled.

**DPLA ID resolution.** DPLA IDs are batched into a single SSM call to `tools/resolve_dpla_ids.py`, which does one ES `terms` query and applies the eligibility filter (banlist, `rightsCategory == "Unlimited Re-Use"`, has-media, hub resolves, institution upload-eligible). Eligible IDs become single-item targets and get re-staged through `get-ids-es --single-id <id>` so their `dpla-map.json` and `sdc.json` reflect the latest mapping code before the downstream phases run.

**Collection-level QIDs are not supported.** The QID resolver only walks hub and institution Wikidata fields. To target a collection, use the explicit `hub|institution|collection` triple syntax.

## Session naming and concurrency

The tmux session name is `wikimedia-<label1>+<label2>+...` where each `<label>` is:

- Hub-level → `<canonical-slug>`
- Institution-level → `<hub>+<institution-slug>`
- Collection-level → `<hub>+<institution-slug>+<collection-slug>`
- Single-item DPLA ID → `<hub>+<first8hex>`
- Multi-institution QID resolving to N institutions → `<hub>+<first-slug>-and-N-more`

Slugification (`partners.slugify_session_label_component`) lowercases, replaces spaces with `-`, and strips everything outside `[a-z0-9-]`. Both the launch script and `wikimedia_kill.py` import this same function so the kill matcher and launch labeler stay in sync.

Session conflict rules:

- **Hub-level requests** conflict with any active session whose label is the canonical slug or starts with `<canonical>+`.
- **Institution-level requests** conflict with hub-level for the same hub, with an exact-label match, or with collection-level sessions under the same institution.
- **Collection-level requests** conflict with hub-level for the same hub, with the institution-level parent, or with an exact-triple match.
- **Single-item DPLA IDs** conflict only with a hub-wide session for the same hub or an exact-duplicate single-item label.

GitHub Actions concurrency keys layer over the SSM-level conflict detection. The Lambda passes `concurrency_key = sha256(shlex.join(targets))[:16]`; two identical dispatches collapse to the same group and queue. Different targets get different keys and run in parallel inside the workflow (the EC2-side conflict check is what serialises them if needed).

### Intra-host write throttle (`WorkerSlotBudget`)

The session-conflict rules above keep *distinct targets* from clobbering each other, but they don't bound how hard the box collectively hammers Commons. That's a separate, finer-grained layer.

Two phases write to Commons: the **uploader** (single-process) and **sdc-sync** (parallelised across a spawn-start `multiprocessing.Pool` via `--workers N`). Each sdc-sync worker — and the uploader — does per-DPLA-*item* work, and Commons' MediaWiki parser pool only tolerates a limited number of concurrent bot writes before maxlag starts to bind. Per-session worker counts can't see each other, so 6 sessions × 24 workers would oversubscribe — the box-wide slot budget is what enforces the actual cap regardless of how many workers each session was launched with.

`ingest_wikimedia/worker_slots.py` provides `WorkerSlotBudget`: a box-wide N-permit semaphore backed by `N` `fcntl`-flock slot files. There are **two pools**. The **shared pool** (`/tmp/sdc-sync-worker-slots`, sized by `--workers-budget N`) is the box-wide budget every sdc-sync Pool worker contends over — one slot per item. The **uploader priority pool** (`/tmp/dpla-uploader-priority-slots`, `UPLOADER_PRIORITY_SLOTS = 4`) is a smaller dedicated pool only uploaders use: the uploader's `WorkerSlotBudget` is wired with the priority pool as primary and the shared pool as `fallback`, so it tries a priority slot first and spills into the shared pool only when all priority slots are held by other uploaders. sdc-sync workers construct no fallback, so they can *never* lock a priority slot. The priority pool is **additive** — not carved out of the shared budget — so total box-wide Commons writers = shared budget + 4. Net effect: an uploader is never blocked by sdc-sync workers as long as fewer than 4 uploader items are in flight box-wide. The downloader is deliberately in neither pool (it writes to source sites, not Commons). `budget <= 0` disables the semaphore (acquire is a no-op); `flock` makes it crash-safe (a dead holder's slot frees automatically when its fd closes).

```text
  Shared pool   --workers-budget 24        → /tmp/sdc-sync-worker-slots/{slot-0 … slot-23}
  Priority pool UPLOADER_PRIORITY_SLOTS=4   → /tmp/dpla-uploader-priority-slots/{slot-0 … slot-3}

   ┌───────────────────────┐
   │ sdc-sync session A     │──┐
   │  Pool(--workers 24)    │  │
   │   w1 w2 … w24          │  ├──▶ shared pool (24 slots, box-wide)
   └───────────────────────┘  │        ▲   (sdc-sync can ONLY use the shared pool)
   ┌───────────────────────┐  │        │
   │ sdc-sync session B …   │──┘        │  overflow: only when all 4 priority slots held
   └───────────────────────┘           │
   ┌───────────────────────┐           │
   │ uploader (any session) │──▶ priority pool (4 slots) ──┘
   └───────────────────────┘   (tries priority first; 1 slot/item; additive to shared)
```

**Invariant: the budget value must be identical across every concurrent session.** It is the cap's whole meaning — if two sessions disagree (24 vs 12), the effective cap degrades to the larger value while the smaller-budget session only ever competes for the lower slots. In practice the value comes from one launch-time default (see below), so all sessions agree.

`tools/sdc_sync.py` itself defaults to `--workers 1 --workers-budget 0` (the single-process, no-cap path, so a hand-run sdc-sync behaves exactly as before). The production values **24 / 24** come from `scripts/wikimedia_launch.py` and the `workers` / `workers_budget` inputs on `wikimedia-launch.yml`: the launcher appends `--workers 24 --workers-budget 24` to the sdc-sync step and `--workers-budget 24` to the uploader step. Matching `--workers` to `--workers-budget` lets a solo sdc-sync session saturate the box-wide pool; concurrent sessions block on the flock semaphore and pick up slots as items complete. The cap therefore belongs to the launch path, not to the tool's own defaults.

## Failure semantics

Inside a target block, steps are chained with `&&` — any step failing aborts that target's remaining steps and triggers the `||` failure handler. The handler runs `notify_pipeline_fail()` from `ingest_wikimedia.slack`, which reads `WIKIMEDIA_LAST_EXIT` and posts a #tech-alerts message including:

- The exit-code hint table: 137 → SIGKILL (likely OOM), 143 → SIGTERM, 139 → SIGSEGV, 134 → SIGABRT, 130 → SIGINT, anything else > 128 reported as `signal {rc-128}`.
- The latest log path (located by `_find_latest_log` globbing `<partner_dir>/logs/*-<label>-*.log` and picking the most-recent mtime).
- A four-marker count from `_summarize_log` (uploads / skips / downloads / failures).
- The last 8 lines of the log in a fenced code block.

Between target blocks the separator is `;` (not `&&`), so a failed target does *not* abort the batch — `notify_pipeline_fail` posts, then the next target's `&&` chain begins fresh. `WIKIMEDIA_TARGET_IS_LAST=1` controls the suffix wording (`no further targets in batch` vs `skipping to next target`).

The downloader and uploader also handle per-item failures internally; they only let the phase fail if something catastrophic happens (e.g. ES timeout, AWS credentials missing). Most "this item didn't work" outcomes are caught and counted in `Result.FAILED` without aborting the phase.

## Component map

| Component | Path | Responsibility |
|---|---|---|
| Slack-dispatch Lambda | `lambda/wikimedia-slack-dispatch/handler.py` | Validate Slack signature, route subcommands, fire `workflow_dispatch` |
| Launch script | `scripts/wikimedia_launch.py` | Target resolution, EC2 health checks, conflict detection, tmux launch |
| Kill script | `scripts/wikimedia_kill.py` | List + kill matching `wikimedia-*` tmux sessions |
| Retry script | `scripts/wikimedia_retry.py` | Parse logs for retryable failures, build per-hub retry CSVs |
| Status script | `scripts/wikimedia_upload_status.py` | Read latest logs, derive phase + progress, post status |
| ID enumeration | `tools/get_ids_es.py` (general) / `tools/get_ids_nara.py` (NARA) | Query ES, stage `dpla-map.json` + `sdc.json` |
| Single-item resolver | `tools/resolve_dpla_ids.py` | One ES batch query + eligibility check + stage |
| Download | `tools/downloader.py` | Iterate IDs, download media (mediaMaster / IIIF), stage to S3, write `file-list.txt` + `iiif.json` |
| Upload | `tools/uploader.py` | Iterate IDs, upload from S3 to Commons, resolve hash drift, write `upload-result.json` |
| SDC sync | `tools/sdc_sync.py` | Read sidecars, post atomic per-file `wbeditentity` to Commons MediaInfo |
| Deferred-tag drain | `tools/drain_deferred.py` | Patient (terminal) + opportunistic (`--no-wait`) drain of the per-partner deferred-tag sidecar — re-runs the deferred `{{duplicate}}`-tag work once `Category:Duplicate` clears |
| Partner registry | `ingest_wikimedia/partners.py` | Hub slugs, aliases, eligibility lookup, slugification, parsing |
| ES client | `ingest_wikimedia/es.py` | Validated `post_es`, hard timeout, partial-response guards |
| S3 client | `ingest_wikimedia/s3.py` | `dpla-wikimedia` bucket, sidecar paths, get/put helpers |
| SSM client | `ingest_wikimedia/ssm.py` | `ssm_run`, `stage_and_launch_tmux` |
| Slack helpers | `ingest_wikimedia/slack.py` | All `notify_*` functions, `notify_pipeline_fail`, log summariser |
| Tracker | `ingest_wikimedia/tracker.py` | Per-phase counter set, used in completion messages |
| SDC builders | `ingest_wikimedia/sdc.py` | `build_claims_for_doc`, P1545 chunking, rights mapping, NARA XML parsing, content-hub/service-hub partnership model (P195 + P3831 roles, `CONTENT_HUB_QIDS`) |
| Legacy migration | `ingest_wikimedia/legacy_artwork.py` | `{{Artwork}}`→`{{DPLA metadata}}` migration: provenance walk, community-value import as SDC, wikitext rewrite |
| Worker-slot budget | `ingest_wikimedia/worker_slots.py` | Box-wide `flock`-backed concurrent-Commons-write cap: a shared sdc-sync pool plus an additive dedicated uploader priority pool |
| Deferred-drain sidecar | `ingest_wikimedia/drain_sidecar.py` | Persistent per-partner deferred-tag queue (`<partner>/deferred-drain.json`) the uploader writes when the duplicate-tag throttle defers |
| Duplicate-tag throttle | `ingest_wikimedia/dup_throttle.py` | `Category:Duplicate` capacity gate — defers `{{duplicate}}` tags at/above `threshold`, resumes below `resume_below` (hysteresis) |
| Wikimedia helpers | `ingest_wikimedia/wikimedia.py` | Title generation, hash-drift handling, CommonsDelinker post |
| Banlist | `ingest_wikimedia/banlist.py` + `dpla-id-banlist.txt` | Per-DPLA-ID skip list |

The shared library (`ingest_wikimedia/`) is deliberately layered so `partners.py` is stdlib-only — the Lambda only needs that one module and `urllib`, no AWS SDK, no requests, no pywikibot. That keeps the Lambda cold-start fast.
{% endraw %}
