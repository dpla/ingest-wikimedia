![Build Badge](https://github.com/dpla/ingest-wikimedia/actions/workflows/pytest.yml/badge.svg)

# ingest-wikimedia

Uploads public-domain media from DPLA partner collections to [Wikimedia Commons](https://commons.wikimedia.org/). Each pipeline run fetches item IDs from DPLA's Elasticsearch index, stages media files to S3, then uploads them to Commons with structured metadata.

## Development setup

This project is managed with [uv](https://docs.astral.sh/uv/).

```bash
uv sync
source .venv/bin/activate
```

Pre-commit hooks use [ruff](https://docs.astral.sh/ruff/) for linting and formatting, and [ggshield](https://www.gitguardian.com/ggshield) for secret scanning. Install with `pre-commit install`.

## Running the pipeline

The pipeline has four sequential phases, each invoked as a CLI command:

```bash
# 1. Generate IDs and stage metadata (writes <partner>.csv + per-item sdc.json sidecars)
get-ids-es <partner>
get-ids-es <partner> --institution "Institution Name"   # institution-level run

# 2. Download media to S3
downloader <partner>.csv <partner>

# 3. Upload from S3 to Wikimedia Commons
uploader <partner>.csv <partner>

# 4. Reconcile SDC (MediaInfo statements) on Commons against the staged sdc.json
sdc-sync --partner <partner> --ids-file <partner>.csv
```

All four must run from the partner's working directory on EC2 (e.g. `/home/ec2-user/ingest-wikimedia/bpl/`), where `config.toml` is resolved from CWD.

**In practice, pipelines are launched via Slack or GitHub Actions** — see [docs/operations.md](docs/operations.md) for the full operations guide.

## Project structure

| Path | Purpose |
|------|---------|
| `ingest_wikimedia/` | Shared library (partner registry, S3, Slack, SSM, Wikimedia API) |
| `tools/` | CLI entry points (`get-ids-es`, `downloader`, `uploader`, `sdc-sync`) |
| `scripts/` | GitHub Actions step scripts (`wikimedia_launch.py`, `wikimedia_kill.py`, `wikimedia_upload_status.py`) |
| `lambda/wikimedia-slack-dispatch/` | Lambda handler for Slack slash commands |
| `.github/workflows/` | Workflow definitions for launch, kill, and status checks |

## Options

```text
get-ids-es <partner> [--institution NAME] [--dry-run]
downloader <partner>.csv <partner> [--dry-run] [--verbose]
uploader   <partner>.csv <partner> [--dry-run] [--verbose]
sdc-sync   --partner <partner> [--ids-file PATH]
```

Pass `--help` to any command for full option details.

## Operations

See **[docs/operations.md](docs/operations.md)** for:
- Slack slash command reference
- Target formats (hub slugs, institution-level runs, Wikidata QIDs)
- Partner hub registry
- Upload eligibility system
- EC2 infrastructure details
- Monitoring and troubleshooting
