# Slack Guide

Non-technical reference for triggering Wikimedia upload runs from Slack. Every command in this guide is something you type into a Slack channel; nothing here requires shell access or a GitHub login. For the full operations reference, see [operations.md](operations.md).

## Quick reference

| What you want to do | Slack command |
|---|---|
| Upload an entire DPLA hub | `/wikimedia-upload <hub-slug>` |
| Upload a single institution within a hub | `/wikimedia-upload "<hub>\|<institution>"` |
| Upload one specific DPLA item | `/wikimedia-upload <dpla-id>` |
| Upload by Wikidata identifier (hub or institution) | `/wikimedia-upload <QID>` |
| Re-sync SDC only (no re-upload) | `/wikimedia-upload sdc <target>` |
| Refresh aged media in S3 (no upload, no SDC) | `/wikimedia-upload refresh <target> [<target> ...] <days>` |
| Retry recent failures | `/wikimedia-upload retry <days> [<hub>]` |
| Stop a running upload | `/wikimedia-upload kill <label>` |
| See what's running | `/wikimedia-status` |

Slack-side outcomes (unknown hub, invalid syntax, ineligible institution) come back as ephemeral replies visible only to you. Once the workflow has accepted the command, ongoing progress and completion messages post to #tech-alerts — see [What posts to #tech-alerts](#what-posts-to-tech-alerts-and-what-you-see-ephemerally) at the bottom of this guide for the full split.

---

## 1. Full-hub uploads

A hub slug runs every upload-eligible institution in that hub.

```text
/wikimedia-upload bpl
/wikimedia-upload indiana
/wikimedia-upload p2p
```

Multiple hubs in one command run sequentially in a single tmux session:

```text
/wikimedia-upload bpl pa indiana
```

If `bpl` fails partway through, `pa` still starts after `bpl`'s failure notification fires; only the failed target's remaining steps are skipped.

Common slug aliases (full list in [operations.md](operations.md#partner-hub-registry)):

| Type | Aliases |
|---|---|
| Massachusetts (Digital Commonwealth) | `bpl`, `ma`, `mass` |
| Indiana Memory | `indiana`, `in` |
| Ohio Digital Network | `ohio`, `oh`, `odn` |
| Minnesota Digital Library | `minnesota`, `mn` |
| Digital Library of Georgia | `georgia`, `ga`, `dlg` |
| Sunshine State Digital Network | `florida`, `fl`, `ssdn` |
| Illinois Digital Heritage Hub | `il`, `idhh` |
| Plains to Peaks Collective | `p2p`, `ppc` |
| Northwest Digital Heritage | `northwest-heritage`, `nwdh` |
| Smithsonian Institution | `si`, `smithsonian` |

NARA cannot be launched via this command — it uses a separate process.

## 2. Institution-level uploads

Use `hub|institution` syntax to upload only items from one institution. Quote the whole argument so Slack treats it as one token. The institution name must match `institutions_v2.json` exactly:

```text
/wikimedia-upload "indiana|Indiana State Library"
/wikimedia-upload "pa|Free Library of Philadelphia"
/wikimedia-upload "p2p|Denver Public Library"
```

Mix institution-level and hub-level targets freely:

```text
/wikimedia-upload bpl "indiana|Indiana State Library" pa
```

Each runs as its own block in the same session.

## 3. Collection-level uploads

Use `hub|institution|collection` to scope down to a single named collection within an institution:

```text
/wikimedia-upload "indiana|Indiana State Library|Cushman Photographs"
```

The collection name must match exactly what appears in `sourceResource.collection.title` for items in DPLA's index. If you're not sure of the exact string, run an institution-level upload instead — extra items are skipped at item level by eligibility, so over-scoping is cheap.

## 4. Wikidata QID

If you know a hub's or institution's Wikidata QID, pass it instead of a slug. The launcher resolves QIDs against `institutions_v2.json`:

```text
/wikimedia-upload Q72380652     # full hub or single institution
/wikimedia-upload Q14688462
```

Behaviour:

- QID matches a **hub** Wikidata field → equivalent to the hub slug.
- QID matches an **institution** Wikidata field → equivalent to `hub|Institution Name`.
- QID matches multiple institutions inside one hub → all of them are ORed together as a single target.
- QID matches both a hub and one of its own institutions → the hub wins (broader scope).

Collection-level QIDs are *not* supported as bare arguments — use the explicit `hub|institution|collection` syntax for collections.

## 5. Single-item DPLA ID

If you only need one specific item, pass its 32-character DPLA ID (hex):

```text
/wikimedia-upload 06b558045c5fd4cc5dc697248272159a
/wikimedia-upload c6505b825ae42e53f5aee419973bb24a 4c3f5ad9bfac4097b95c9f8deb8e1a21
```

Single-item runs:

- Re-stage their `dpla-map.json` and `sdc.json` from current code before downloading.
- Skip the per-phase "starting download" / "starting upload" Slack messages (single-item runs are usually quick; per-phase chatter would be noise).
- Get a session label like `wikimedia-nara+06b55804` (hub + first 8 hex of the ID) so multiple single-item runs can be distinguished.

## 6. SDC-only re-sync

`/wikimedia-upload sdc <target>` runs `get-ids-es → sdc-sync` only — no downloader, no uploader. Useful when:

- You changed something in the SDC pipeline (date parser, chunking rule, new property) and want to roll it out to already-uploaded files.
- The bot wrote stale SDC under an old data model and you want to re-reconcile against current ingestion3 mappings.

```text
/wikimedia-upload sdc bpl
/wikimedia-upload sdc "indiana|Indiana State Library"
/wikimedia-upload sdc Q72380652
/wikimedia-upload sdc 06b558045c5fd4cc5dc697248272159a
```

`get-ids-es` re-stages `sdc.json` from current mapping code, and `sdc-sync` posts only the diff against existing Commons-side state. SDC sync is fully idempotent — a re-sync against an already-correct file produces zero writes.

**Caveat for single-item and NARA targets:** these targets don't re-stage `sdc.json` (NARA uses a separate enumerator and single-item targets use `resolve-dpla-ids`). For these, `sdc-sync` replays whatever sidecar was written by the last regular run. Useful for re-running fixed sdc-sync logic; not useful for picking up upstream mapping changes.

## 7. Refresh-only (re-download)

`/wikimedia-upload refresh <target> [<target> ...] <days>` re-downloads aged media files in S3 without re-uploading. Used to refresh master copies for partners whose source URLs may have rotated. One or more targets, with the `<days>` threshold as the trailing positional.

```text
/wikimedia-upload refresh bpl 365
/wikimedia-upload refresh "indiana|Indiana State Library" 180
/wikimedia-upload refresh bpl pa indiana 90
```

The trailing number is a `--max-age-days` threshold — only S3 keys older than N days are re-downloaded. `<days>` is required (no default at the Slack layer to prevent accidental full refreshes).

The downloader is invoked with `--notify-complete` so a #tech-alerts summary fires when it finishes (`Wikimedia Download Refresh Complete:`).

## 8. Retry recent failures

`/wikimedia-upload retry <days> [<hub>]` scans the last N days of upload + download logs across all partners (or one, if specified), classifies retryable failures, and launches new uploader / downloader runs to clean them up.

```text
/wikimedia-upload retry 7              # all partners, last 7 days
/wikimedia-upload retry 14 bpl         # bpl only, last 14 days
```

Retryable failure types (see [maintenance-tools.md](maintenance-tools.md#get-ids-retry) for the full list):

- Upload-side: `lockmanager-fail-conflict`, `stashfailed: Could not acquire lock`, `backend-fail-internal`, hash drift, redirect collisions.
- Download-side: `Failed downloading <url>` (excluding the empty-URL IIIF bug pattern).

The retry response is ephemeral (only you see the immediate Slack reply); the actual retry session posts to #tech-alerts normally once it starts.

## 9. Kill a running session

```text
/wikimedia-upload kill bpl
/wikimedia-upload kill indiana-state-library
/wikimedia-upload kill bpl pa
/wikimedia-upload kill Q72380652
```

`kill` matches against any `+`-delimited component of an active session name. So all of these match a session named `wikimedia-indiana+indiana-state-library`:

- `/wikimedia-upload kill indiana`
- `/wikimedia-upload kill indiana-state-library`
- The hub's or institution's Wikidata QID (resolved to the matching label components)

If your kill argument matches zero active sessions, the response says so but no error is raised. If it matches multiple (e.g. `/wikimedia-upload kill bpl` against a session with three institutions inside the bpl hub), all of them in the same session are killed together — sessions are not split.

## 10. Check status

```text
/wikimedia-status
```

Posts to #tech-alerts with each active session and its current phase + progress:

```text
🟢 Active Wikimedia upload sessions
• wikimedia-bpl — Uploading (3,214 / 11,503, ~28%)
• wikimedia-indiana+indiana-state-library — SDC syncing (812 / 4,002, ~20%)
```

The status workflow runs automatically every 6 hours and posts only when something is active. The Slack-triggered version always posts (so an empty result confirms "nothing's running" rather than looking like the command silently failed).

---

## Common scenarios

### "I added a new institution to `institutions_v2.json`. How do I upload it?"

Once ingestion3's `main` branch has merged your change, the launcher will pick it up on the next invocation (it fetches `institutions_v2.json` fresh from GitHub each cold start). Then:

```text
/wikimedia-upload "<hub>|<Your New Institution Name>"
```

### "I just merged a fix to the SDC sync code and need to redo every file."

A full SDC-only sweep, per hub:

```text
/wikimedia-upload sdc bpl
/wikimedia-upload sdc indiana
# ...etc.
```

The SDC sync is idempotent, so any item that doesn't need changes produces no writes. Files that *do* need changes get exactly the changes you fixed.

### "A run failed overnight — how do I see what happened?"

The #tech-alerts message includes the latest log path on EC2 and the last 8 lines tail of that log. For more detail:

```text
/wikimedia-status   # confirms whether the session is still running or fully dead
```

Then triage the log directly on EC2 (see [operations.md](operations.md#troubleshooting)).

### "Two operators issued the same `/wikimedia-upload bpl` at the same time."

The first one launches; the second one is dropped with a "session already running" ephemeral error (the GitHub Actions concurrency-key collapses identical dispatches as well, but the EC2-side conflict detection is the authoritative gate). To force-kill the existing session and start over, an operator can re-trigger `wikimedia-launch.yml` manually from the GitHub Actions tab with `force: true` — that's the only place that switch is exposed.

### "I want to refresh all of bpl's source media but not re-upload."

```text
/wikimedia-upload refresh bpl 30
```

Re-downloads any S3 key older than 30 days. No uploads, no SDC sync. The completion message in #tech-alerts will read `Wikimedia Download Refresh Complete:` with the refreshed / skipped / failed counts.

---

## What posts to #tech-alerts (and what you see ephemerally)

| Event | Where it posts |
|---|---|
| You hit `/wikimedia-upload`, command accepted | Ephemeral ack to you |
| Unknown hub / invalid target / no such institution | Ephemeral error to you |
| Launch confirmation | #tech-alerts |
| Each phase begins (download/upload/SDC) | #tech-alerts (suppressed for single-item runs) |
| Phase completes successfully | #tech-alerts (full counts and runtime) |
| Pipeline step fails | #tech-alerts (exit-code hint + log tail) |
| Kill confirmation | #tech-alerts |
| `/wikimedia-status` results | #tech-alerts |
| Scheduled status report (every 6 h, only when sessions active) | #tech-alerts |
| `/wikimedia-upload retry` immediate response | Ephemeral to you |
| `/wikimedia-upload refresh` download-complete summary | #tech-alerts |
