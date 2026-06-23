{% raw %}
# Wikimedia Upload Pipeline — Operations Guide

This guide covers running, monitoring, and troubleshooting the DPLA Wikimedia upload pipeline. The pipeline uploads public-domain media from DPLA partner collections to Wikimedia Commons.

---

## Table of contents

1. [Architecture overview](#architecture-overview)
2. [Slack slash commands](#slack-slash-commands)
3. [Target formats](#target-formats)
4. [Partner hub registry](#partner-hub-registry)
5. [Upload eligibility](#upload-eligibility)
6. [Pipeline phases](#pipeline-phases)
7. [Maintain mode](#maintain-mode)
8. [Session naming and chaining](#session-naming-and-chaining)
9. [GitHub Actions workflows](#github-actions-workflows)
10. [Lambda dispatch mechanism](#lambda-dispatch-mechanism)
11. [EC2 infrastructure](#ec2-infrastructure)
12. [Monitoring](#monitoring)
13. [Troubleshooting](#troubleshooting)

---

## Architecture overview

```text
Slack user
    │
    │  POST /wikimedia-upload bpl pa
    ▼
Lambda (wikimedia-slack-dispatch)
    │  Validates Slack signature
    │  Validates hub slugs / QID format
    │  Returns immediate ack to Slack (<3s)
    ▼
GitHub Actions (wikimedia-launch.yml)
    │  Resolves targets, checks eligibility
    │  Updates EC2 code (git clone → cp)
    │  Checks memory headroom on EC2
    │  Checks for conflicting tmux sessions
    ▼
EC2 (i-033eff6c8c168f999) via AWS SSM
    │  tmux session: wikimedia-bpl+pa
    │  Runs four phases per target (sequentially):
    │    1. get-ids-es → <partner>.csv  (also stages per-item sdc.json)
    │    2. downloader <partner>.csv <partner>
    │    3. uploader   <partner>.csv <partner>  (writes per-item upload-result.json)
    │    4. sdc-sync   --partner <partner> --ids-file <partner>.csv
    ▼
S3 (s3://dpla-wikimedia/)     Wikimedia Commons
```

Results (success or failure) post to **#tech-alerts** via the `DPLA_SLACK_BOT_TOKEN` bot.

---

## Slack slash commands

### `/wikimedia-upload <target> [<target> ...]`

Launches the full upload pipeline (ID generation → download → upload → SDC sync) for one or more targets. Targets run sequentially in a single tmux session. If a step fails for a given target, that target's pipeline stops and a Slack failure notification posts — the launcher then continues with the next target in the batch.

```text
/wikimedia-upload bpl
/wikimedia-upload bpl pa
/wikimedia-upload "indiana|Indiana State Library"
/wikimedia-upload bpl "indiana|Indiana State Library" pa
/wikimedia-upload Q72380652
/wikimedia-upload Q72380652 Q14688462
```

The immediate Slack reply confirms that the workflow was dispatched. A confirmation with the actual tmux session name posts to **#tech-alerts** once the session starts (~1–2 minutes later).

### `/wikimedia-upload kill <label> [<label> ...]`

Stops one or more running pipeline sessions. Use any `+`-delimited component of the session name shown by `/wikimedia-status` — for example, both `indiana` and `indiana-state-library` match the session `wikimedia-indiana+indiana-state-library`. Wikidata QIDs are also accepted and resolved to their label components.

```text
/wikimedia-upload kill bpl
/wikimedia-upload kill bpl pa
/wikimedia-upload kill indiana
/wikimedia-upload kill indiana-state-library
/wikimedia-upload kill Q72380652
```

Result posts to **#tech-alerts**.

### `/wikimedia-upload maintain [lite|count] <target> [<target> ...]`

Reconciles files already on Commons for a hub/institution (incl. no-longer-opted-in institutions); never creates new files. Default downloads + content-reconciles (re-link by hash + overwrite changed bytes + SDC); `lite` is the quick no-download SDC-in-place + rename route; `count` is a read-only pre-flight sizing pass. See [Maintain mode](#maintain-mode) for the full behavior.

### `/wikimedia-status`

Checks for active upload sessions and posts a status summary to **#tech-alerts**. Shows each session's current phase and progress (e.g. `Downloading (1,234 / 5,678 items, ~21.7%)`).

The status workflow also runs automatically every 6 hours; it only posts when sessions are active (unlike `/wikimedia-status`, which always posts).

---

## Target formats

Targets can be specified in three formats:

### Hub slug

A short identifier for a DPLA partner hub. Runs the full hub.

```text
bpl          → Digital Commonwealth (full hub)
pa           → PA Digital (full hub)
indiana      → Indiana Memory (full hub)
```

See [Partner hub registry](#partner-hub-registry) for all valid slugs and aliases.

### Hub|institution pair

Runs only a specific institution within a hub. The institution name must match exactly as it appears in `institutions_v2.json`. Quote the argument in Slack if the institution name contains spaces.

```text
indiana|Indiana State Library
indiana|Indiana University
"pa|Free Library of Philadelphia"
```

Multiple `hub|institution` targets for the same hub are allowed when they refer to different institutions. They run as part of the same tmux session.

### Wikidata QID

A Wikidata entity identifier (e.g. `Q72380652`). The launch script resolves the QID against `institutions_v2.json`:

- If the QID matches a **hub-level** Wikidata field → full hub run (equivalent to a plain slug)
- If the QID matches an **institution-level** Wikidata field → institution-level run (equivalent to `hub|Institution Name`)
- If the QID maps to multiple institutions (same or different hubs) → all matching entries run as separate targets

The Lambda handler validates QID format (`^Q\d+$`) and passes QIDs through unchanged; resolution happens in the launch script where `institutions_v2.json` is already cached.

---

## Partner hub registry

Canonical slugs and their hub display names:

| Slug | Hub name |
|------|----------|
| `artstor` | Artstor |
| `bhl` | Biodiversity Heritage Library |
| `bpl` | Digital Commonwealth |
| `cdl` | California Digital Library |
| `community-webs` | Community Webs |
| `ct` | Connecticut Digital Archive |
| `david-rumsey` | David Rumsey |
| `dc` | District Digital |
| `digitalnc` | North Carolina Digital Heritage Center |
| `florida` | Sunshine State Digital Network |
| `georgia` | Digital Library of Georgia |
| `getty` | J. Paul Getty Trust |
| `gpo` | United States Government Publishing Office (GPO) |
| `harvard` | Harvard Library |
| `hathi` | HathiTrust |
| `heartland` | Heartland Hub |
| `ia` | Internet Archive |
| `il` | Illinois Digital Heritage Hub |
| `indiana` | Indiana Memory |
| `jh3` | Jewish Heritage and History Hub |
| `lc` | Library of Congress |
| `maine` | Digital Maine |
| `maryland` | Digital Maryland |
| `mi` | Michigan Service Hub |
| `minnesota` | Minnesota Digital Library |
| `mississippi` | Mississippi Digital Library |
| `mwdl` | Mountain West Digital Library |
| `nara` | National Archives and Records Administration |
| `njde` | NJ/DE Digital Collective |
| `northwest-heritage` | Northwest Digital Heritage |
| `nypl` | The New York Public Library |
| `ohio` | Ohio Digital Network |
| `oklahoma` | OKHub |
| `p2p` | Plains to Peaks Collective |
| `pa` | PA Digital |
| `scdl` | South Carolina Digital Library |
| `si` | Smithsonian Institution |
| `texas` | The Portal to Texas History |
| `tn` | Digital Library of Tennessee |
| `txdl` | Texas Digital Library |
| `virginias` | Digital Virginias |
| `vt` | Vermont Green Mountain Digital Archive |
| `washington` | University of Washington |
| `wisconsin` | Recollection Wisconsin |

**Accepted aliases** (map to the canonical slug above):

| Alias | Canonical |
|-------|-----------|
| `ma`, `mass` | `bpl` |
| `in` | `indiana` |
| `oh`, `odn` | `ohio` |
| `mn` | `minnesota` |
| `ga`, `dlg` | `georgia` |
| `fl`, `ssdn` | `florida` |
| `ms` | `mississippi` |
| `rw` | `wisconsin` |
| `hh` | `heartland` |
| `idhh` | `il` |
| `jhn` | `jh3` |
| `nwdh` | `northwest-heritage` |
| `ppc` | `p2p` |
| `smithsonian` | `si` |

**NARA** cannot be launched via this pipeline. It requires a separate process.

---

## Upload eligibility

Not every hub in the registry is upload-eligible. Eligibility is determined per-hub (and per-institution within a hub) by the `upload: true` flag in [`institutions_v2.json`](https://github.com/dpla/ingestion3/blob/main/src/main/resources/wiki/institutions_v2.json) in the ingestion3 repository.

The launch script checks eligibility before launching and rejects ineligible targets with an ephemeral Slack error. A hub is eligible if:
- The hub-level object has `"upload": true`, **or**
- Any institution within the hub has `"upload": true`

Hub-level eligibility does not imply every institution within it is eligible — institution-level runs respect individual eligibility flags.

---

## Pipeline phases

Each target runs four sequential phases within the tmux session. The `&&` chain ensures a later phase only starts if the previous one succeeded.

### Phase 1: ID generation (`get-ids-es`)

Queries DPLA's Elasticsearch index for items belonging to the partner hub (or a specific institution). Writes a CSV of DPLA IDs to `<partner>.csv` in the partner's working directory and stages each item's `dpla-map.json` plus a precomputed `sdc.json` claim envelope to S3 under `<partner>/images/<sharded-prefix>/<dpla-id>/`.

```bash
get-ids-es <partner>                              # full hub
get-ids-es <partner> --institution "Inst Name"   # single institution
```

### Phase 2: Download (`downloader`)

Reads `<partner>.csv`, downloads the original media files, and stages them to `s3://dpla-wikimedia/`. Skips files already present in S3 (by key existence). Re-downloads zero-byte S3 stubs automatically.

```bash
downloader <partner>.csv <partner>
```

### Phase 3: Upload (`uploader`)

Reads `<partner>.csv`, retrieves media from S3, and uploads each file to Wikimedia Commons with structured SDC (Structured Data on Commons) metadata. Skips files already present on Commons. Writes a per-item `upload-result.json` sidecar to S3 with each ordinal's outcome (status / page title / pageid), which the SDC phase consumes.

```bash
uploader <partner>.csv <partner>
```

### Phase 4: SDC sync (`sdc-sync`)

Reads each item's precomputed `sdc.json` and `upload-result.json` from S3, and for every ordinal whose upload status is `UPLOADED` or `SKIPPED`, posts MediaInfo statements (and references) to the corresponding Commons file via the wbeditentity API. Idempotent — re-syncing a fully-synced item produces zero writes.

```bash
sdc-sync --partner <partner> --ids-file <partner>.csv --workers 6 --workers-budget 24
```

`--workers N` runs the partner sync across N worker processes (the launcher and workflow default to `6`); `--workers-budget N` caps concurrent Commons writers box-wide across all sessions (default `24`). See [Worker-slot budget](#worker-slot-budget) below.

**Wikitext cleanup runs in this phase too.** After posting SDC for an ordinal, sdc-sync also reconciles the file's wikitext:

- Files still on a legacy `{{Artwork}}` / `{{Information}}` / `{{Photograph}}` wrapper are auto-migrated to `{{DPLA metadata}}` — walking each file's revision history to separate DPLA-bot values (overwrite-safe) from community contributions (preserved as SDC with `P887→Q131783016` + `P4656` permalink references), then rewriting the wikitext.
- Files already on `{{DPLA metadata}}` have template params that are now redundant with SDC stripped out.

This cleanup is on by default; pass `--no-normalize-wikitext` to leave the pre-strip wikitext intact for diagnostic runs. (A separate explicit one-time mode, `sdc-sync --migrate-legacy`, runs the legacy migration *instead of* a normal sync — see [maintenance tools](maintenance-tools.md).)

All phases are idempotent — re-running is safe and picks up where it left off.

### Alternate run modes

The `wikimedia-launch.yml` workflow accepts two boolean inputs that swap the default 4-phase chain for a shorter variant. They are mutually exclusive — pick at most one.

- **`refresh_only=true`** — runs `get-ids-es → downloader` only (no uploader, no SDC). For re-downloading aged media files in S3 without re-uploading. The downloader is invoked with `--notify-complete` so its own Slack summary fires at the end. `max_age_days=N` controls the re-download threshold (default: > 365 days).
- **`sdc_only=true`** — runs `get-ids-es → sdc-sync` only (no downloader, no uploader). For backfilling or refreshing SDC on items that were uploaded in a prior session, including picking up changes to ingestion3's `institutions_v2.json` / `subjects.json` mappings. The `get-ids-es` step re-stages each item's `sdc.json` from current ingestion3 data; `sdc-sync` then reconciles Commons against that.
  - **Targets without sdc.json re-staging**: single-item DPLA IDs (which use `resolve-dpla-ids` + `printf`) and NARA hub-level targets (which use `get-ids-nara`) do **not** re-stage `sdc.json`. For these, `sdc-sync` replays whatever sidecar the original upload run wrote. The launcher prints a stderr warning when this combination is detected. Useful for re-running fixed sdc-sync logic against a specific item; not useful for picking up upstream mapping changes.
  - `max_age_days` is ignored in `sdc_only` mode (no download phase runs).

---

## Maintain mode

Maintain mode **reconciles files that are already on Commons** for a hub or institution — including institutions that are no longer authorized for *new* uploads (`upload=false` in `institutions_v2.json`). The only institution gate is a **Wikidata QID** (needed for the P195 institution claim and the Commons category); the `upload` flag is ignored. The safety guarantee is the uploader/sync **no-create fence**: maintain never creates a new Commons File page — it only edits, moves, or overwrites files that already exist.

Trigger from Slack:

```text
/wikimedia-upload maintain <target> [<target> ...]          # default: hash route
/wikimedia-upload maintain lite <target> [<target> ...]     # quick no-download route
/wikimedia-upload maintain count <target> [<target> ...]    # pre-flight sizing, writes nothing
```

Targets use the normal `hub` / `hub|institution` formats. Because the default route downloads media, in practice you'll usually run at **`hub|institution`** granularity to bound how much is fetched (a whole service hub can be enormous).

### Default route (hash)

`maintain <target>` reconciles **SDC + legacy templates across every file in the live Commons category** (the primary goal) and additionally repairs **content drift** for the media-bearing subset. For a `hub` / `hub|institution` target it runs (bare-hub `nara` stages via `get-ids-nara` instead of `get-ids-es`, matching the normal pipeline):

```text
get-ids-es … --maintain --skip-media-filter  →  downloader  →  uploader --no-create  →  sdc-sync --cat … --maintain --from-s3
```

- `get-ids-es --maintain --skip-media-filter` stages each item's `sdc.json` + `dpla-map.json` for the **whole** QID-bearing scope (QID-only gate, *and* the per-item rights/media-URL filters dropped) — the same broad scan as the lite route, so sidecars exist for the entire category.
- The downloader pulls each fetchable current master into S3 (keyed by the item's *current* DPLA id, with its SHA1); items with no fetchable master are simply skipped.
- `uploader --no-create` reuses the existing hash-drift machinery against that fresh S3 set:
  - **Re-link by content (exact SHA1):** an orphaned Commons file whose embedded id is dead but whose bytes match a current item is **moved** to that item's canonical title.
  - **Overwrite on content drift:** a Commons file whose bytes differ from the current master is re-uploaded as a new version at the canonical title.
  - **No-create fence:** an item not already on Commons is skipped (`UPLOAD_SKIPPED_WOULD_CREATE`) — never uploaded fresh.
- `sdc-sync --cat … --maintain` then walks the **live Commons category** and reconciles SDC + legacy templates for *every* file there — not just the items get-ids matched — reading each (re-linked) file's claims from its staged `sdc.json` (`--from-s3`).

Anchoring SDC on the category (not the get-ids id list) is deliberate: an institution whose current index docs no longer pass the rights/media filter still has files on Commons, and those must be reconciled. The content-drift repair is the lesser, best-effort benefit layered on top — matching is **exact SHA1 only** (no fuzzy/perceptual matching), so an item whose master was re-encoded upstream or has no fetchable media is simply not re-linked; the SDC `--cat` pass still reconciles its file in place.

> **Single-DPLA-id / collection targets** have no whole category to walk, so they keep the **id-list-anchored** route — get-ids → downloader → `uploader --no-create` → `sdc-sync --partner --ids-file` — reconciling exactly the matched items. (A single-id re-stages that one item with `get-ids-es --single-id` — no `--maintain` flag; a collection uses `get-ids-es --collection … --maintain`.) Use these for targeted drift repair of one item or collection.

### Lite route

`maintain lite <target>` is the quick, **no-download** sidecar route. It walks the institution's Commons category and, per file:

- re-links the DPLA id via the URL ladder (embedded id still live → exact `isShownAt` → institution-scoped wildcard);
- **renames** a title-drifted file to its canonical title when that title is free or a redirect back to the file (else logs `MAINTAIN_RENAME_BLOCKED`);
- syncs SDC in place from the precomputed `sdc.json` sidecar (`--from-s3`).

`get-ids-es` here adds `--skip-media-filter` (drops the rights + media-URL item filters): lite touches whatever is already on Commons, so it must not exclude items whose current index doc lost a media URL or free-rights category. Lite runs under the same `--workers` / `--workers-budget` pool as a partner-mode SDC sync (files are grouped by DPLA id so an item's pages stay on one worker). It cannot re-link or overwrite by content (no download) — use it for routine SDC refresh + cheap name-drift fixes.

### Count route (pre-flight sizing)

`maintain count <target>` is a read-only lite pre-flight: it walks the category and resolves how each file *would* re-link (embedded / isShownAt / wildcard / unresolved), prints a per-anchor breakdown, and writes nothing. Run it first to size a maintain job and spot a scope that re-links poorly.

### Reading the result

Maintain reports through the same surfaces as a normal SDC run — the per-target `…-sdc.log` and the `/wikimedia-status` poller (`Generating IDs → SDC syncing (n/total) → SDC complete`), plus a `#tech-alerts` completion summary. In the COUNTS summary:

- **`SDC_ITEMS_SKIPPED_NO_SIDECAR`** (lite) / a file left untouched (hash): the re-link found no current match — the item left the index, or (hash route) its bytes/media URL changed so no exact match exists. This is expected and safe, not an error; flagged for human review.
- **`MAINTAIN_RENAMED` / `MAINTAIN_RENAME_BLOCKED`** (lite): title-drift renames applied / left for follow-up.
- **`UPLOAD_SKIPPED_WOULD_CREATE`** (hash): items not already on Commons — correctly not created.

---

## Session naming and chaining

When multiple targets are specified, they all run in a single tmux session. The session name is:

```text
wikimedia-<label1>+<label2>+...
```

The label for a full-hub target is its canonical slug. The label for an institution-level target is `{canonical}+{institution}`, where the institution name is lowercased with spaces replaced by hyphens. The hub slug prefix lets the status script locate the correct EC2 directory. `/wikimedia-upload kill` accepts any `+`-delimited component of the session name (see kill command section above).

Examples:
- `/wikimedia-upload bpl` → session `wikimedia-bpl`
- `/wikimedia-upload bpl pa` → session `wikimedia-bpl+pa`
- `/wikimedia-upload "indiana|Indiana State Library"` → session `wikimedia-indiana+indiana-state-library`
- `/wikimedia-upload bpl "indiana|Indiana State Library"` → session `wikimedia-bpl+indiana+indiana-state-library`

The `+` separator is unambiguous because labels use `-` as their only separator character.

Within the session, targets run sequentially with `&&` chaining — if `bpl`'s upload fails, `pa` does not start.

**Conflict detection**: before launching, the script checks whether any existing tmux session contains any of the requested hub slugs. If a conflict is found, the launch fails with an ephemeral error listing the conflicting session(s). To override, trigger `wikimedia-launch.yml` manually from GitHub Actions with `force: true`.

**Memory check**: the launch script verifies that at least 30% of RAM is available on EC2 before starting. Each ingest session uses ~300–500 MB; the EC2 instance has 7.6 GB total. At the 30% threshold (~2.3 GB free), 4–5 concurrent sessions are the practical limit.

---

## GitHub Actions workflows

### `wikimedia-launch.yml`

Triggered by: Slack `/wikimedia-upload`, or manually from the Actions tab.

Inputs:
- `partner` (required): shlex-encoded target list, e.g. `bpl` or `bpl pa` or `bpl "indiana|Indiana State Library"` or `Q72380652`
- `force` (boolean, default `false`): kill any conflicting sessions before launching
- `response_url` (optional): Slack response URL for ephemeral error feedback
- `concurrency_key` (optional): short SHA256 prefix of the partner string, used by the GitHub Actions concurrency group to keep the key under the 400-char limit; set by the Slack dispatcher
- `max_age_days` (optional integer): for `refresh_only` runs, re-download files older than N days in S3 (default: 365)
- `refresh_only` (boolean, default `false`): run `get-ids-es → downloader` only — skip the upload and SDC phases (see "Alternate run modes" above)
- `sdc_only` (boolean, default `false`): run `get-ids-es → sdc-sync` only — skip the download and upload phases (see "Alternate run modes" above). Mutually exclusive with `refresh_only`.
- `workers` (string, default `"6"`): number of SDC-sync worker processes per session. `1` runs single-process; higher values parallelize the partner sync across that many processes. Passed through to `sdc-sync --workers`.
- `workers_budget` (string, default `"24"`): box-wide cap on concurrent Commons-writing slots shared across all sessions on EC2 (`0` = unlimited). Passed through to `sdc-sync --workers-budget` and the uploader's `--workers-budget`. See [Worker-slot budget](#worker-slot-budget). Both inputs are blank-safe — an empty value falls back to the launcher default (`6` / `24`).

Steps:
1. Installs `boto3` and `requests`
2. Runs `scripts/wikimedia_launch.py`, which:
   - Resolves and validates all targets
   - Checks EC2 memory headroom
   - Checks for conflicting tmux sessions (kills them if `force=true`)
   - Updates EC2 code from GitHub
   - Launches the tmux session
   - Verifies the session started
   - Posts confirmation to #tech-alerts

### `wikimedia-kill.yml`

Triggered by: Slack `/wikimedia-upload kill`, or manually from the Actions tab.

Inputs:
- `partner` (required): space-separated session label suffixes or QIDs to kill (e.g. `bpl`, `indiana-state-library`, `Q72380652`)
- `response_url` (optional): Slack response URL for ephemeral error feedback

Steps: runs `scripts/wikimedia_kill.py`, which lists all `wikimedia-*` tmux sessions, kills any whose `+`-delimited components intersect the specified labels, and posts the result to #tech-alerts.

### `wikimedia-upload-status.yml`

Triggered by: schedule (every 6 hours), or Slack `/wikimedia-status`.

When scheduled: posts to #tech-alerts only if sessions are active.
When triggered by Slack: always posts (even if no sessions are running).

For each active session, reports the current phase and progress percentage by inspecting the most recent log file.

---

## Lambda dispatch mechanism

The Lambda function (`wikimedia-slack-dispatch`) sits behind a Lambda Function URL and handles all Slack slash commands for the `dpla` workspace.

**Validation**:
1. Checks `x-slack-request-timestamp` and `x-slack-signature` headers are present
2. Decodes base64 body if `isBase64Encoded` is set
3. Verifies HMAC-SHA256 signature using `SLACK_SIGNING_SECRET`
4. Rejects requests with a timestamp more than 5 minutes old

**Dispatch**: after validation, the handler calls the GitHub API to trigger the appropriate `workflow_dispatch` event, then returns an immediate acknowledgement to Slack. The GitHub API call has a 2-second timeout to stay within Slack's 3-second ack limit.

**QID handling in Lambda**: the Lambda validates QID format (`^Q\d+$`) but does **not** resolve QIDs against `institutions_v2.json` — that would require a network call within the 3-second window. QIDs are passed through to the launch/kill scripts, which resolve them after the workflow starts.

**Hub slug validation in Lambda**: the Lambda resolves hub slugs and validates them against the local `PARTNER_HUBS` registry before dispatching. This catches typos immediately with an ephemeral error, before a GitHub Actions run is started.

**Environment variables**:
- `SLACK_SIGNING_SECRET`: from the Slack app's Basic Information page
- `GH_TOKEN`: GitHub fine-grained PAT with `actions:write` on `dpla/ingest-wikimedia`
- `GH_REPO`: repository (default: `dpla/ingest-wikimedia`)

---

## EC2 infrastructure

- **Instance**: `i-033eff6c8c168f999` (name: "wiki downloads") — always running
- **Resources**: ~7.6 GiB RAM, **no swap** — see [Memory and concurrency planning](#memory-and-concurrency-planning) before launching several runs at once
- **Access**: AWS SSM (`ssm:SendCommand` / `ssm:GetCommandInvocation`)
- **Region**: `us-east-1`
- **Repo root**: `/home/ec2-user/ingest-wikimedia/`
- **Per-partner working directory**: `/home/ec2-user/ingest-wikimedia/<partner>/`
  - Exception: `si` (Smithsonian) maps to `smithsonian/`, not `si/`
- **Venv**: `/home/ec2-user/ingest-wikimedia/.venv/bin/activate`
- **uv binary**: `/home/ec2-user/.local/bin/uv`

The EC2 directory is **not a git repo** — code is deployed by cloning to `/tmp` and copying files. The package is an editable install, so updating the `.py` source files changes the running code immediately.

The launch script updates EC2 code on every run before launching:

```bash
cd /tmp && rm -rf ingest-wikimedia-update && \
git clone --depth 1 https://github.com/dpla/ingest-wikimedia.git ingest-wikimedia-update && \
cp -r ingest-wikimedia-update/ingest_wikimedia/* /home/ec2-user/ingest-wikimedia/ingest_wikimedia/ && \
cp -r ingest-wikimedia-update/tools/* /home/ec2-user/ingest-wikimedia/tools/ && \
cp ingest-wikimedia-update/pyproject.toml /home/ec2-user/ingest-wikimedia/pyproject.toml && \
cp ingest-wikimedia-update/uv.lock /home/ec2-user/ingest-wikimedia/uv.lock && \
/home/ec2-user/.local/bin/uv sync --project /home/ec2-user/ingest-wikimedia
```

**S3 staging bucket**: `s3://dpla-wikimedia/`

---

## Monitoring

### Active sessions

Check what's running via SSM:

```bash
aws ssm send-command \
  --instance-ids i-033eff6c8c168f999 \
  --document-name AWS-RunShellScript \
  --parameters '{"commands":["sudo -u ec2-user tmux ls 2>/dev/null"]}' \
  --query 'Command.CommandId' --output text
```

Or trigger `/wikimedia-status` from Slack.

### Worker-slot budget

SDC-sync parallelism (`--workers N`) and the uploader share a single **box-wide** budget of concurrent Commons writers, so the 6+ sessions that typically run at once don't collectively overrun Commons' parser pool (which binds on maxlag past ~16 concurrent bot writes). The budget is a set of flock-backed slot files — one permit each — under:

```text
/tmp/sdc-sync-worker-slots/slot-0 … slot-N
```

Each SDC worker (and the single-process uploader) checks out one slot per item it writes and releases it on return; slots auto-release if the holding process dies (the flock drops on `close`/exit). The downloader does **not** participate — it writes to source sites, not Commons.

Inspect live holders:

```bash
# Who currently holds a slot
lslocks | grep sdc-sync-worker-slots
# Or just see the slot files
ls /tmp/sdc-sync-worker-slots/
```

**Budget-consistency invariant**: the budget value (`24` in production) must be identical across all concurrent sessions for the cap to mean what it says. In practice every session inherits it from the same launch-time default, so they agree. If two sessions disagree (e.g. 24 vs 8), the effective cap is the *larger* N — a benign degradation, not a correctness break.

**Detecting a session blocked on the cap**: when every slot is held, a worker logs a line containing `worker slots busy` before it polls for capacity. Grep the sdc-sync log tail for that marker to tell that a session is waiting on the budget rather than stuck:

```bash
tail -200 /home/ec2-user/ingest-wikimedia/<partner>/logs/<latest>-sdc.log | grep "worker slots busy"
```

A reboot that clears `/tmp` is safe — it also kills every slot holder, so there's nothing to preserve.

### Memory and concurrency planning

The box has **~7.6 GiB RAM and no swap**, so an over-budget allocation is killed by the OOM killer — abruptly and silently (no Slack notice; the session just dies mid-phase). The [worker-slot budget](#worker-slot-budget) does **not** protect against this: it caps concurrent Commons *writers* (maxlag protection), not memory. You can have most slots free and still be near the memory ceiling, so "slots available" is not a signal that it's safe to launch more.

Two facts drive memory use:

- **The uploader is the heavy phase, and its footprint scales with media file size.** It holds the file being uploaded in memory. Large-media hubs — **NARA** above all (large TIFFs, motion-picture video) — run **~1–2 GiB per uploader**. Image hubs (most ContentDM/IIIF library hubs, ~1–3 MB JPEGs) run a few hundred MiB, dominated by interpreter + batch overhead rather than the file. Downloaders (~100–150 MiB) and SDC workers (~80 MiB each) are comparatively cheap.

- **Most downloaders are queued uploaders.** In a standard upload run — and in the maintain hash route — download is the cheap front end of a chain that auto-advances `download → upload → sdc-sync` in the same session. The run's memory *peak* arrives later, in the upload phase. Because downloads are fast and Commons uploads are throttled, runs **accumulate** in the upload phase, so memory climbs *after* phase changes, not at launch. Treat "a NARA download is running" as "a NARA upload is already booked." (The exception is [`refresh_only`](#alternate-run-modes), which runs `get-ids-es → downloader` alone — no upload follows, so it stays at downloader weight.)

**Planning rule:** budget by the number of concurrent *runs*, each weighted by its hub's media size — not by current phase, and not by free slots. On this box, keep **at most one large-media (NARA-class) run** in flight at a time; image-hub runs can stack several deep safely. Stagger launches so two large upload phases don't converge. If large-media batches become routine, the fix is a bigger instance (or adding swap as a backstop), not throttling slots — the slot knob doesn't touch what actually fills RAM.

Spot-check resident memory by category before launching (`free -h` for the headroom, `ps` for the breakdown):

```bash
# Top memory holders by category — run via SSM on the instance
free -h
ps -eo rss,args | grep -E "uploader|downloader|sdc-sync" | grep -v grep \
  | awk '{mb=$1/1024; print int(mb) " MiB  " $0}' | sort -rn | head
```

RSS overcounts memory shared across a worker pool's forked children, so the per-process numbers run high for SDC pools — fine for a gut-check, but read `free -h` for the true figure. For an exact per-process number, sum `Pss` from `/proc/<pid>/smaps_rollup`.

### Log files

Logs are written to `/home/ec2-user/ingest-wikimedia/<partner>/logs/` with names like:

```text
20240601-120000-bpl-download.log
20240601-140000-bpl-upload.log
```

Key log lines:

| Line | Meaning |
|------|---------|
| `Downloading https://...` | Downloading a file from source |
| `Key already in S3: ...` | File already staged, skipping download |
| `Uploaded to https://commons.wikimedia.org/wiki/File:...` | Successful upload |
| `Skipping <id>: Already exists on commons.` | File already on Commons, skipping |
| `COUNTS:` followed by `UPLOADED: N`, `SKIPPED: N`, `FAILED: N`, `BYTES: N` | Phase complete |
| `Bad provider.` | Data provider not configured for upload |
| ` -- All N worker slots busy; waiting for capacity.` | SDC/upload worker blocked on the box-wide [worker-slot budget](#worker-slot-budget) — expected under heavy concurrency |

### Slack notifications

- **#tech-alerts** receives: launch confirmation, kill confirmation, status reports, and phase completion summaries (when `DPLA_SLACK_BOT_TOKEN` is set in the Actions environment)
- Ephemeral errors (invalid hub, ineligible hub, conflicting session) go only to the user who ran the slash command

---

## Troubleshooting

### "Unknown hub" error in Slack

The slug is not in the hub registry. Check `ingest_wikimedia/partners.py` for valid slugs and aliases. Hub display names (e.g. "Digital Commonwealth") are also accepted and resolve to the canonical slug.

### "Hub is not upload-eligible"

The hub exists in the registry but `institutions_v2.json` does not mark it (or any of its institutions) as `upload: true`. This is a data-side setting in the [ingestion3 repo](https://github.com/dpla/ingestion3/blob/main/src/main/resources/wiki/institutions_v2.json).

### "Session already running" / conflict error

A tmux session with an overlapping hub is already active. Options:
1. Wait for it to finish
2. Kill it with `/wikimedia-upload kill <label>` (use the label shown by `/wikimedia-status`)
3. Re-trigger `wikimedia-launch.yml` from GitHub Actions with `force: true`

### "Only N% memory available" error

EC2 is below the 30% memory threshold. Check what sessions are running (`/wikimedia-status`) and wait for one to complete, or kill a session to free memory.

### Pipeline started but no #tech-alerts confirmation

1. Check the GitHub Actions run for the workflow — look for errors in the launch script output
2. Verify `DPLA_SLACK_BOT_TOKEN` is set as a repository secret
3. Check if the tmux session actually started: run `/wikimedia-status` or check via SSM directly

### Session disappeared but no completion message

The pipeline likely crashed. Check the most recent log file:

```bash
ls -t /home/ec2-user/ingest-wikimedia/<partner>/logs/ | head -3
tail -50 /home/ec2-user/ingest-wikimedia/<partner>/logs/<latest>.log
```

Common crash causes:
- **`RuntimeError: File linked to another page`**: DPLA ID drift — file already on Commons under a different title. Known/expected, not fixable per-run.
- **`titleblacklist-forbidden`**: Title contains `''` (double apostrophes), `&`, or `=` matching Wikimedia's blacklist. Requires fix in title generation.
- **`fileexists-shared-forbidden`**: File name collides with an existing Wikimedia shared repo file. Not fixable; these items are skipped.
- **`.bin` extension / `ValueError: does not have a valid extension`**: Downloader couldn't determine MIME type; stored with `.bin` fallback. Commons rejects it.
- **`protectedpage`**: Target file page is protected on Commons. Not fixable; skip.

### Re-running after a crash

All phases are idempotent — re-launching is safe. The downloader skips already-staged S3 objects; the uploader skips files already on Commons; sdc-sync re-syncs each item but writes nothing when the Commons-side state already matches the precomputed sdc.json. Just trigger `/wikimedia-upload <hub>` again.

If the ID file (`<partner>.csv`) is partial or empty, delete it before re-running — the launch script will regenerate it from scratch.

### Orphaned temp files

An interrupted download can leave `wiki-tmp-*.tmp` files in partner directories:

```bash
find /home/ec2-user/ingest-wikimedia -name 'wiki-tmp-*' -type f -delete
```

### Log disk usage

Logs accumulate over time and can reach several GB per partner for long-running jobs. Check usage:

```bash
du -sh /home/ec2-user/ingest-wikimedia/*/logs/
```

Old logs can be deleted safely — they are not read by the pipeline after the run completes.
{% endraw %}
