# ingest-wikimedia

PREREQUISITE: This project is managed with [uv](https://docs.astral.sh/uv/).

To set the project up for execution:

1. Run `uv build` 
2. Run `source .venv/bin/activate`

```downloader [OPTIONS] IDS_FILE PARTNER API_KEY```

```uploader [OPTIONS] IDS_FILE PARTNER```


Options:
- `--dry-run`: Does everything save for actually write to Commons.
- `--verbose`: Logs all the data generated for each file for inspection.

`IDS_FILE` is a simple text/csv file with one DPLA ID per line.

`PARTNER` is one of the strings in the `DPLA_PARTNERS` list in `constants.py`.

`API_KEY` is a DPLA API key.

You can provide `--help` to these commands to see more options.

## Logs

Log files are written out on the local file system in `logs`. This directory is specified in `logs.py`.

This project makes use of a temporary directory that it creates with a unique name locally.

## Development

This project makes use of [ruff](https://docs.astral.sh/ruff/), 
[ggshield](https://www.gitguardian.com/ggshield), and 
[pre-commit](https://pre-commit.com/) to enforce standards prior to commits to source 
control. You may want to configure your editor to run ruff on save, as well.