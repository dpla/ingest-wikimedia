# ingest-wikimedia

PREREQUISITE: This project is managed with [uv](https://docs.astral.sh/uv/).

```uv run downloader.py [OPTIONS] IDS_FILE PARTNER API_KEY```

```uv run uploader.py [OPTIONS] IDS_FILE PARTNER API_KEY```

Options:
- `--dry-run`: Does everything save for actually write to Commons.
- `--verbose`: Logs all the data generated for each file for inspection.

`IDS_FILE` is a simple text/csv file with one DPLA ID per line.

`PARTNER` is one of the strings in the `DPLA_PARTNERS` list in `constants.py`.

`API_KEY` is a DPLA API key.

## Logs

Log files are written out on the local file system in `logs`. This directory is specified in `constants.py`.

This project makes use of a temporary directory. By default, it uses `tmp`. This is specified in `constants.py` as well. 