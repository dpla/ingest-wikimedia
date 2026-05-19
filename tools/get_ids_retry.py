"""
Extract DPLA IDs from recent upload/download logs for items that failed due to
transient issues and should be retried.

Two failure types are identified:

  upload   — Wikimedia-side transient errors (lock contention, backend storage
              failures).  S3 assets are already present; only the uploader
              needs to run.

  download — Media-server HTTP failures after all retries are exhausted.  Both
              the downloader and uploader need to run.

Output: one CSV per partner per failure type, written to --output-dir.
A summary table is printed to stdout.

Usage:
    get-ids-retry <days> [--partner PARTNER] [--output-dir DIR]
"""

import csv
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click


BASE_DIR = Path(
    os.environ.get("INGEST_WIKIMEDIA_DIR", "/home/ec2-user/ingest-wikimedia")
)

UPLOAD_TRANSIENT_ERRORS = (
    "lockmanager-fail-conflict",
    "lockmanager-fail-svr-acquire",
    "stashfailed: Could not acquire lock",
    "stashfailed: Server failed to publish temporary file",
    "backend-fail-internal",
    "uploadstash-exception",
)

UPLOAD_TRANSIENT_RE = re.compile(
    "|".join(re.escape(e) for e in UPLOAD_TRANSIENT_ERRORS)
)

DPLA_ID_RE = re.compile(r"DPLA ID: ([0-9a-f]{32})")
DOWNLOAD_FAILED_RE = re.compile(r"Failed: ([0-9a-f]{32})")

# The double space indicates an empty URL — produced by the IIIF parsing bug fixed in PR #180.
EMPTY_URL_FAILURE = "Failed downloading  to"


def parse_upload_log(path: Path) -> tuple[set[str], set[str]]:
    """Return (transient_failure_ids, successfully_uploaded_ids) from an upload log.

    transient_failure_ids  — IDs that hit lock-contention or backend-storage errors;
                             the S3 asset is present but the upload didn't land on Commons.
    successfully_uploaded_ids — IDs for which at least one file reached Commons ("Uploaded to").
    """
    failures: set[str] = set()
    successes: set[str] = set()
    current_id: str | None = None
    with open(path, errors="replace") as f:
        for line in f:
            m = DPLA_ID_RE.search(line)
            if m:
                current_id = m.group(1)
            elif current_id:
                if UPLOAD_TRANSIENT_RE.search(line):
                    failures.add(current_id)
                elif "Uploaded to" in line:
                    successes.add(current_id)
    return failures, successes


def parse_download_log(path: Path) -> set[str]:
    """Return DPLA IDs that hit media-server download failures (non-empty URL)."""
    failed: set[str] = set()
    current_id: str | None = None
    with open(path, errors="replace") as f:
        for line in f:
            m = DOWNLOAD_FAILED_RE.search(line)
            if m:
                current_id = m.group(1)
            elif current_id and "Failed downloading" in line:
                if EMPTY_URL_FAILURE not in line:
                    failed.add(current_id)
                current_id = None
    return failed


def collect_partner_ids(partner: str, cutoff: datetime) -> tuple[set[str], set[str]]:
    """Scan logs for *partner* and return (upload_failures, download_failures).

    IDs that were successfully uploaded within the window are excluded from both
    sets — they are already on Commons and retrying them would only produce skips.
    """
    log_dir = BASE_DIR / partner / "logs"
    upload_failures: set[str] = set()
    upload_successes: set[str] = set()
    download_failures: set[str] = set()

    for log_file in sorted(log_dir.glob("*-upload.log")):
        if datetime.fromtimestamp(log_file.stat().st_mtime, tz=timezone.utc) < cutoff:
            continue
        failures, successes = parse_upload_log(log_file)
        upload_failures.update(failures)
        upload_successes.update(successes)

    for log_file in sorted(log_dir.glob("*-download.log")):
        if datetime.fromtimestamp(log_file.stat().st_mtime, tz=timezone.utc) < cutoff:
            continue
        download_failures.update(parse_download_log(log_file))

    upload_failures -= upload_successes
    download_failures -= upload_successes
    # Avoid scheduling the same ID for both retry types.
    upload_failures -= download_failures
    return upload_failures, download_failures


def write_ids(path: Path, ids: set[str]) -> None:
    # Write-side counterpart to ingest_wikimedia.common.load_ids.
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        for dpla_id in sorted(ids):
            writer.writerow([dpla_id])


def _discover_partners() -> list[str]:
    return sorted(
        d.name
        for d in BASE_DIR.iterdir()
        if d.is_dir() and (d / "logs").is_dir() and not d.name.startswith(".")
    )


@click.command()
@click.argument("days", type=int)
@click.option(
    "--partner",
    default=None,
    help="Limit to a single partner hub. Defaults to all partners.",
)
@click.option(
    "--output-dir",
    default=str(BASE_DIR / "retry"),
    show_default=True,
    help="Directory to write retry CSV files.",
)
def main(days: int, partner: str | None, output_dir: str) -> None:
    """Extract failed DPLA IDs from the last DAYS days of logs for retry.

    Writes one CSV per partner per failure type to OUTPUT_DIR:
      <partner>-upload-retry.csv   — run uploader only
      <partner>-download-retry.csv — run downloader then uploader
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    partners = [partner] if partner else _discover_partners()

    rows: list[tuple[str, str, int, Path]] = []

    for p in partners:
        log_dir = BASE_DIR / p / "logs"
        if not log_dir.is_dir():
            logging.warning("No logs directory for partner '%s', skipping.", p)
            continue

        upload_ids, download_ids = collect_partner_ids(p, cutoff)
        for suffix, ids in (("upload", upload_ids), ("download", download_ids)):
            if ids:
                csv_path = out / f"{p}-{suffix}-retry.csv"
                write_ids(csv_path, ids)
                rows.append((p, suffix, len(ids), csv_path))

    if not rows:
        print(f"No retryable failures found in the last {days} days.")
        sys.exit(0)

    col_w = max(len(r[0]) for r in rows)
    print(f"\n{'Partner':<{col_w}}  {'Type':<8}  {'IDs':>6}  File")
    print("-" * (col_w + 2 + 8 + 2 + 6 + 2 + 40))
    for partner_name, failure_type, count, csv_file in rows:
        print(f"{partner_name:<{col_w}}  {failure_type:<8}  {count:>6}  {csv_file}")
    print()


if __name__ == "__main__":
    main()
