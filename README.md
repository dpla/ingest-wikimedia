# ingest-wikimedia

PREREQUISITE: This project is managed with [https://docs.astral.sh/uv/](uv).

uv run downloader.py [OPTIONS] IDS_FILE PARTNER API_KEY

uv run uploader.py [OPTIONS] IDS_FILE PARTNER API_KEY
Options:
--dry-run
--verbose

IDS_FILE is a simple text/csv file with one DPLA ID per line.
PARTNER is one of the strings in the `DPLA_PARTNERS` list in constants.py.
API_KEY is a DPLA API key.

## Logs

Log files are written out on the local file system in `./ingest-wikimedia/logs/`.
