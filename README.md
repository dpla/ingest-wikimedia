![Build Badge](https://github.com/dpla/ingest-wikimedia/actions/workflows/pytest.yml/badge.svg)

# ingest-wikimedia

Uploads public-domain media from DPLA partner collections to [Wikimedia Commons](https://commons.wikimedia.org/) and synchronises structured data (SDC) statements on every uploaded file's MediaInfo entity.

A run is triggered from Slack (e.g. `/wikimedia-upload bpl`), dispatched through AWS Lambda → GitHub Actions → AWS SSM into a long-running tmux session on a pinned EC2 instance, where it executes four sequential phases: enumerate IDs from DPLA's Elasticsearch index, download media to S3, upload to Commons, then post SDC. Each phase is idempotent and re-running is safe.

## Documentation

### Operators

- **[docs/slack-guide.md](docs/slack-guide.md)** — Copy-paste Slack examples for every run type (hub-level, institution-level, Wikidata QID, single DPLA ID, refresh-only, SDC-only, retry, kill, status). Non-technical.
- **[docs/operations.md](docs/operations.md)** — Full operations reference: hub registry, eligibility rules, session naming, monitoring, troubleshooting playbook.

### Architecture and internals

- **[docs/architecture.md](docs/architecture.md)** — End-to-end system architecture: Slack → Lambda → GitHub Actions → SSM → EC2; concurrency keys; target resolution; failure semantics.
- **[docs/pipeline-phases.md](docs/pipeline-phases.md)** — Deep dive on each of the four phases (`get-ids-es`, `downloader`, `uploader`, `sdc-sync`), the Elasticsearch-vs-API switch, NARA's special enumeration, and the `--single-id`/`--sdc-only`/`--refresh-only` modes.
- **[docs/sidecars.md](docs/sidecars.md)** — The S3 sidecar files (`dpla-map.json`, `sdc.json`, `upload-result.json`, `file-list.txt`, `iiif.json`) that connect the phases. Schemas, writers, readers, lifecycle.
- **[docs/sdc-sync.md](docs/sdc-sync.md)** — SDC sync phase in depth: the atomic single-`wbeditentity` dispatcher, every property the bot writes, idempotency via `check()`, the P1545 chunked-claim convention, the P813 refresh, and the Wikibase API gotchas (type/mainsnak required on partial-update fragments).
- **[docs/special-cases.md](docs/special-cases.md)** — Duplicate detection (SHA1-based and title-based), the four hash-drift cases the uploader resolves, file renames, CommonsDelinker integration, redirect handling, orphan tagging.
- **[docs/templates.md](docs/templates.md)** — How `{{Artwork}}` is currently emitted at upload, how `{{DPLA metadata}}` + `Module:DPLA` read SDC after upload, and the planned transition to `{{DPLA metadata}}` as the primary template.
- **[docs/metrics.md](docs/metrics.md)** — The scheduled CIM-pageviews workflow that publishes monthly pageview data to Commons `Data:` pages, plus the GitHub Pages site that consumes them.
- **[docs/maintenance-tools.md](docs/maintenance-tools.md)** — `verify-item`, `retirer`, `nuke`, `remimer`, `sign`, `get-incomplete-items`, `get-ids-retry`, `fix-unknown-categories`.

## Development setup

This project is managed with [uv](https://docs.astral.sh/uv/).

```bash
uv sync
source .venv/bin/activate
```

Pre-commit hooks use [ruff](https://docs.astral.sh/ruff/) for linting and formatting, and [ggshield](https://www.gitguardian.com/ggshield) for secret scanning. Install with `pre-commit install`.

## Project structure

| Path | Purpose |
|------|---------|
| `ingest_wikimedia/` | Shared library: partner registry, ES client, S3, Slack, SSM, Wikimedia API, SDC builders |
| `tools/` | CLI entry points (`get-ids-es`, `downloader`, `uploader`, `sdc-sync`, plus maintenance utilities) |
| `scripts/` | GitHub-Actions step scripts (`wikimedia_launch.py`, `wikimedia_kill.py`, `wikimedia_retry.py`, `wikimedia_upload_status.py`) |
| `lambda/wikimedia-slack-dispatch/` | AWS Lambda handler for `/wikimedia-*` Slack slash commands |
| `.github/workflows/` | Workflow definitions for launch, kill, retry, status, CIM pageviews, plus CI (pytest, ruff, codeql) |
| `metrics/` | Source for the GitHub-Pages site plus the scheduled `CIMviews.py` bot |
| `tests/` | Pytest suite (one `test_<module>.py` per source module) |
| `dpla-id-banlist.txt` | DPLA IDs to skip in eligibility checks |
| `rights.json` | Rights-URI → Wikidata QID mapping consumed by SDC builders |

## CLI reference

```text
get-ids-es <partner> [--institution NAME ...] [--collection NAME] [--single-id ID]
downloader <ids.csv> <partner> [--max-age-days N] [--notify-complete] [--overwrite] [--dry-run] [--verbose]
uploader   <ids.csv> <partner> [--dry-run] [--verbose]
sdc-sync   --partner <partner> [--ids-file PATH]
sdc-sync   --file "File:Title.jpg" [--file ...] | --cat <Category> | --lists <dir>
```

Pass `--help` to any command for full option details. In practice nothing here is invoked by hand — runs are launched via Slack and orchestrated by `scripts/wikimedia_launch.py`. See [docs/architecture.md](docs/architecture.md) for the dispatch chain.
