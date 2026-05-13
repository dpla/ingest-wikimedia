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
7. [Session naming and chaining](#session-naming-and-chaining)
8. [GitHub Actions workflows](#github-actions-workflows)
9. [Lambda dispatch mechanism](#lambda-dispatch-mechanism)
10. [EC2 infrastructure](#ec2-infrastructure)
11. [Monitoring](#monitoring)
12. [Troubleshooting](#troubleshooting)

---

## Architecture overview

```
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
    │  Runs three phases per target (sequentially):
    │    1. get-ids-es → <partner>.csv
    │    2. downloader <partner>.csv <partner>
    │    3. uploader   <partner>.csv <partner>
    ▼
S3 (s3://dpla-wikimedia/)     Wikimedia Commons
```

Results (success or failure) post to **#tech-alerts** via the `DPLA_SLACK_BOT_TOKEN` bot.

---

## Slack slash commands

### `/wikimedia-upload <target> [<target> ...]`

Launches the full upload pipeline (ID generation → download → upload) for one or more targets. All targets run sequentially in a single tmux session. If any step fails, the chain stops.

```
/wikimedia-upload bpl
/wikimedia-upload bpl pa
/wikimedia-upload "indiana|Indiana State Library"
/wikimedia-upload bpl "indiana|Indiana State Library" pa
/wikimedia-upload Q72380652
/wikimedia-upload Q72380652 Q14688462
```

The immediate Slack reply confirms that the workflow was dispatched. A confirmation with the actual tmux session name posts to **#tech-alerts** once the session starts (~1–2 minutes later).

### `/wikimedia-upload kill <hub> [<hub> ...]`

Stops one or more running pipeline sessions. Kills any tmux session whose name contains the specified hub(s) as a `+`-delimited component.

```
/wikimedia-upload kill bpl
/wikimedia-upload kill bpl pa
/wikimedia-upload kill Q72380652
```

Result posts to **#tech-alerts**.

### `/wikimedia-status`

Checks for active upload sessions and posts a status summary to **#tech-alerts**. Shows each session's current phase and progress (e.g. `Downloading (1,234 / 5,678 items, ~21.7%)`).

The status workflow also runs automatically every 6 hours; it only posts when sessions are active (unlike `/wikimedia-status`, which always posts).

---

## Target formats

Targets can be specified in three formats:

### Hub slug

A short identifier for a DPLA partner hub. Runs the full hub.

```
bpl          → Digital Commonwealth (full hub)
pa           → PA Digital (full hub)
indiana      → Indiana Memory (full hub)
```

See [Partner hub registry](#partner-hub-registry) for all valid slugs and aliases.

### Hub|institution pair

Runs only a specific institution within a hub. The institution name must match exactly as it appears in `institutions_v2.json`. Quote the argument in Slack if the institution name contains spaces.

```
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

Each target runs three sequential phases within the tmux session. The `&&` chain ensures a later phase only starts if the previous one succeeded.

### Phase 1: ID generation (`get-ids-es`)

Queries DPLA's Elasticsearch index for items belonging to the partner hub (or a specific institution). Writes a CSV of DPLA IDs and associated metadata to `<partner>.csv` in the partner's working directory.

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

Reads `<partner>.csv`, retrieves media from S3, and uploads each file to Wikimedia Commons with structured SDC (Structured Data on Commons) metadata. Skips files already present on Commons.

```bash
uploader <partner>.csv <partner>
```

All three phases are idempotent — re-running is safe and picks up where it left off.

---

## Session naming and chaining

When multiple targets are specified, they all run in a single tmux session. The session name is:

```
wikimedia-<canonical1>+<canonical2>+...
```

Examples:
- `/wikimedia-upload bpl` → session `wikimedia-bpl`
- `/wikimedia-upload bpl pa` → session `wikimedia-bpl+pa`
- `/wikimedia-upload bpl "indiana|Indiana State Library"` → session `wikimedia-bpl+indiana`
  (session name uses canonical slugs only; institution detail is in the pipeline command)

The `+` separator is unambiguous because canonical slugs use `-` as their only separator character.

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
- `partner` (required): space-separated hub slugs or QIDs to kill
- `response_url` (optional): Slack response URL for ephemeral error feedback

Steps: runs `scripts/wikimedia_kill.py`, which lists all `wikimedia-*` tmux sessions, kills any that contain the specified hub(s), and posts the result to #tech-alerts.

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

### Log files

Logs are written to `/home/ec2-user/ingest-wikimedia/<partner>/logs/` with names like:

```
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

A tmux session with an overlapping hub slug is already active. Options:
1. Wait for it to finish
2. Kill it with `/wikimedia-upload kill <hub>`
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

All phases are idempotent — re-launching is safe. The downloader skips already-staged S3 objects; the uploader skips files already on Commons. Just trigger `/wikimedia-upload <hub>` again.

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
