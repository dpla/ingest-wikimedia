"""
Extract DPLA IDs from recent upload/download/sdc logs for items that failed due
to transient or now-resolvable issues and should be retried.

Three failure types are identified:

  upload   — Wikimedia-side transient errors (lock contention, backend storage
              failures) and title/hash-drift errors that the uploader can now
              correct.  S3 assets are already present; only the uploader needs
              to run.

  download — Media-server HTTP failures after all retries are exhausted.  Both
              the downloader and uploader need to run.

  sdc      — Wikibase API transient failures during SDC sync (maxlag, replica
              lag, rate limiting, 5xx, network blips).  Filtered to exceptions
              a bare retry is likely to succeed against — structural errors
              (invalid-claim, no-such-entity, permission denied) are NOT
              classified as retryable because re-running won't help.

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
    # Wikimedia API / storage transient errors
    "lockmanager-fail-conflict",
    "lockmanager-fail-svr-acquire",
    "stashfailed: Could not acquire lock",
    "stashfailed: Server failed to publish temporary file",
    "backend-fail-internal",
    "uploadstash-exception",
    # Title / hash drift errors — the uploader's drift-correction logic resolves
    # these on retry.  Both appear in log tracebacks (via exc_info=) attached to
    # "Failed: Unknown" warnings, where the line-by-line regex below matches them.
    "File linked to another page",  # RuntimeError: file exists on Commons at wrong title
    "ArticleExistsConflictError",  # pywikibot: move-over-redirect blocked by insufficient rights
    "fileexists-shared-forbidden",  # Wikimedia API: different file already at intended title
)

UPLOAD_TRANSIENT_RE = re.compile(
    "|".join(re.escape(e) for e in UPLOAD_TRANSIENT_ERRORS)
)

DPLA_ID_RE = re.compile(r"DPLA ID: ([0-9a-f]{32})")
DOWNLOAD_FAILED_RE = re.compile(r"Failed: ([0-9a-f]{32})")

# The double space indicates an empty URL — produced by the IIIF parsing bug fixed in PR #180.
EMPTY_URL_FAILURE = "Failed downloading  to"

# The per-ordinal failure marker logged from sdc_sync._run_partner_mode's
# exception boundary.  The traceback follows on subsequent lines until the
# next [INFO] / [ERROR] marker.
SDC_ORDINAL_ERROR_RE = re.compile(
    r"-- Ordinal \d+ \(M\d+\) for ([0-9a-f]{32}): SDC sync failed; skipping ordinal\."
)

# Substring patterns that indicate the failure is transient and a bare retry
# (re-running sdc-sync against the same staged sdc.json) is likely to succeed.
# Anything not matching one of these patterns is treated as STRUCTURAL —
# either a code bug, malformed SDC fragment, or Commons-side permanent state
# (deleted entity, permission denial) — and excluded from the retry CSV
# because re-running would just reproduce the same failure.
#
# Match strings are taken verbatim from observed traceback / API-error text:
#
#   * MaxlagTimeoutError                 — replica lag exhausted pywikibot retry budget
#   * ServerError                        — HTTP 5xx from MediaWiki / Wikibase
#   * ReadTimeoutError / ReadTimeout     — botocore / requests
#   * ConnectTimeoutError                — botocore
#   * EndpointConnectionError            — botocore
#   * ChunkedEncodingError               — requests partial-response
#   * ProtocolError                      — urllib3 connection drops
#   * ConnectionError                    — requests / urllib3
#   * internal_api_error_DBQueryError    — MediaWiki API replica/DB blip
#   * internal_api_error_DBConnectionError
#   * editconflict                       — concurrent writer race on the entity
#   * failed-save                        — Wikibase save retry storm
#   * readonly                           — MediaWiki database in read-only mode
#   * ratelimited                        — API rate limit reached
#   * maxlag                             — explicit maxlag rejection (lowercase form)
#   * SlowDown / RequestTimeout /        — S3 transient sidecar reads
#     ServiceUnavailable / InternalError
SDC_TRANSIENT_ERRORS = (
    "MaxlagTimeoutError",
    "ServerError",
    "ReadTimeoutError",
    "ReadTimeout",
    "ConnectTimeoutError",
    "EndpointConnectionError",
    "ChunkedEncodingError",
    "ProtocolError",
    "ConnectionError",
    "internal_api_error_DBQueryError",
    "internal_api_error_DBConnectionError",
    "editconflict",
    "failed-save",
    "readonly",
    "ratelimited",
    "maxlag",
    "SlowDown",
    "RequestTimeout",
    "ServiceUnavailable",
    "InternalError",
)

SDC_TRANSIENT_RE = re.compile("|".join(re.escape(e) for e in SDC_TRANSIENT_ERRORS))


def parse_upload_log(path: Path) -> tuple[set[str], set[str]]:
    """Return (transient_failure_ids, successfully_uploaded_ids) from an upload log.

    transient_failure_ids  — IDs that hit lock-contention, backend-storage, or
                             title/hash-drift errors; S3 assets are present but
                             the upload didn't land on Commons (or landed at the
                             wrong title).
    successfully_uploaded_ids — IDs for which at least one file reached Commons ("Uploaded to").
    """
    failures: set[str] = set()
    successes: set[str] = set()
    current_id: str | None = None
    with open(path, encoding="utf-8", errors="replace") as f:
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
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = DOWNLOAD_FAILED_RE.search(line)
            if m:
                current_id = m.group(1)
            elif current_id and "Failed downloading" in line:
                if EMPTY_URL_FAILURE not in line:
                    failed.add(current_id)
                current_id = None
    return failed


def parse_sdc_log(path: Path) -> set[str]:
    """Return DPLA IDs whose per-ordinal SDC sync failed with a transient error.

    The per-ordinal exception boundary in ``sdc_sync._run_partner_mode`` logs
    a single marker line followed by a Python traceback.  We collect the
    traceback text up to the next ``[INFO]`` / ``[ERROR]`` line and classify:

      retryable  — traceback matches ``SDC_TRANSIENT_RE``.  The DPLA ID is
                   added to the result set.
      structural — anything else.  Either a code bug, malformed SDC, or
                   permanent Commons-side state (deleted entity, permission
                   denial).  Re-running won't help; excluded from the CSV.

    A single DPLA item can have many ordinals; if ANY ordinal hit a
    retryable error, the whole item ID is included (sdc-sync's partner
    mode reads each item's sidecars and only writes diffs against
    Commons-side state, so re-syncing the whole item is safe and cheap —
    the already-clean ordinals produce zero writes).
    """
    retryable: set[str] = set()
    current_id: str | None = None
    current_traceback: list[str] = []

    def _flush() -> None:
        # Close the current traceback block: if it matched a transient
        # pattern, register the ID for retry.  Reset state regardless.
        nonlocal current_id, current_traceback
        if current_id and current_traceback:
            blob = "".join(current_traceback)
            if SDC_TRANSIENT_RE.search(blob):
                retryable.add(current_id)
        current_id = None
        current_traceback = []

    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            m = SDC_ORDINAL_ERROR_RE.search(line)
            if m:
                # New error block: flush any prior, start fresh.
                _flush()
                current_id = m.group(1)
                continue
            if current_id is None:
                continue
            # Inside an error block: collect traceback lines until the
            # next [INFO] / [ERROR] marker.  Anything starting with
            # "[INFO] " or "[ERROR] " ends the block.
            if line.startswith("[INFO] ") or line.startswith("[ERROR] "):
                _flush()
                # If this terminating line is itself a new ordinal error,
                # re-classify it as the start of the next block.
                m2 = SDC_ORDINAL_ERROR_RE.search(line)
                if m2:
                    current_id = m2.group(1)
                continue
            current_traceback.append(line)
        # End-of-file: flush whatever is pending.
        _flush()
    return retryable


def collect_partner_ids(
    partner: str, cutoff: datetime
) -> tuple[set[str], set[str], set[str]]:
    """Scan logs for *partner* and return ``(upload, download, sdc)`` retry sets.

    Upload logs are processed oldest-first so each run's outcome can supersede
    the previous one.  For each run an ID is classified as:

      "retry" — had any transient error (even alongside per-file successes);
                at least one file may be missing from Commons.
      "done"  — clean upload (no transient errors, at least one success);
                confirmed fully on Commons for this run.

    An ID that fails in an early run but uploads cleanly in a later run ends up
    as "done" and is excluded.  An ID with partial success in a single run
    (some files fail, some succeed) stays "retry".

    SDC logs are processed similarly: each per-ordinal error is classified as
    transient (matches ``SDC_TRANSIENT_RE``) or structural.  Only transient
    failures land in the retry set.  Items that succeed in a later run aren't
    excluded — the per-item SDC sync is idempotent, so re-running them is a
    no-op that produces zero writes; the simpler "any transient error in the
    window" rule is cheaper than tracking per-run outcomes.
    """
    log_dir = BASE_DIR / partner / "logs"
    # id → "retry" | "done"; later log files (by mtime) overwrite earlier ones.
    outcomes: dict[str, str] = {}
    download_failures: set[str] = set()
    sdc_failures: set[str] = set()

    for log_file in sorted(
        log_dir.glob("*-upload.log"), key=lambda f: f.stat().st_mtime
    ):
        if datetime.fromtimestamp(log_file.stat().st_mtime, tz=timezone.utc) < cutoff:
            continue
        file_failures, file_successes = parse_upload_log(log_file)
        for dpla_id in file_failures:
            outcomes[dpla_id] = "retry"
        for dpla_id in file_successes - file_failures:
            outcomes[dpla_id] = "done"

    for log_file in sorted(log_dir.glob("*-download.log")):
        if datetime.fromtimestamp(log_file.stat().st_mtime, tz=timezone.utc) < cutoff:
            continue
        download_failures.update(parse_download_log(log_file))

    for log_file in sorted(log_dir.glob("*-sdc.log")):
        if datetime.fromtimestamp(log_file.stat().st_mtime, tz=timezone.utc) < cutoff:
            continue
        sdc_failures.update(parse_sdc_log(log_file))

    upload_failures = {dpla_id for dpla_id, o in outcomes.items() if o == "retry"}
    fully_uploaded = {dpla_id for dpla_id, o in outcomes.items() if o == "done"}
    download_failures -= fully_uploaded
    # Avoid scheduling the same ID for both upload-side retry types.  An ID
    # that needs an upload retry will also re-run sdc-sync downstream when the
    # uploader catches up, so don't double-list it in the sdc-retry CSV
    # either — that would force the sdc-sync step to run twice on the same
    # ID.  Same rule for download: the combined retry pipeline already
    # chains uploader after downloader and sdc-sync after uploader.
    upload_failures -= download_failures
    sdc_failures -= upload_failures
    sdc_failures -= download_failures
    return upload_failures, download_failures, sdc_failures


def write_ids(path: Path, ids: set[str]) -> None:
    # Write-side counterpart to ingest_wikimedia.common.load_ids.
    with open(path, "w", encoding="utf-8", newline="") as f:
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
      <partner>-sdc-retry.csv      — run sdc-sync only (transient SDC failures)
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

        upload_ids, download_ids, sdc_ids = collect_partner_ids(p, cutoff)
        for suffix, ids in (
            ("upload", upload_ids),
            ("download", download_ids),
            ("sdc", sdc_ids),
        ):
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
