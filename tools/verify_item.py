#!/usr/bin/env python3
"""
End-to-end verification that every S3 file for a DPLA item has its exact
sha1 landed at its intended Commons title.

For each ordinal in the item's file_list this script:
  1. reads the S3 object's sha1 + size + content-type,
  2. uses compute_ordinal_exts_and_page_labels() — the same helper the
     uploader uses — to determine the Commons page_label the bot would
     have assigned to that ordinal (gap-squashing logic and all),
  3. queries the Commons MediaWiki API for the file at the resulting
     intended title and compares its sha1 to S3.

Outcomes per ordinal:
  CORRECT  — Commons file at the intended title has sha1 == S3 sha1
  MISMATCH — file exists at the intended title with a different sha1
  REDIRECT — intended title is a redirect (S3 sha1 isn't at the right name)
  MISSING  — intended title has no page on Commons
  SKIPPED  — the uploader would not have uploaded this ordinal (0-byte
             stub, download-only mime, missing from S3, etc.). Reported
             with a reason but does not count as a failure.

Exits 0 only when every uploadable ordinal is CORRECT. Anything else
exits 1 so the script can be wired into a deploy-verification loop.

Usage:
    python3 tools/verify_item.py <dpla_id> <partner>

Output:
    Summary printed to stdout, full per-ordinal detail written to
    /tmp/verify_<dpla_id>.json for follow-up.
"""

import datetime
import email.utils
import json
import logging
import mimetypes
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import boto3
from botocore.exceptions import ClientError

from ingest_wikimedia.common import get_dict, get_list
from ingest_wikimedia.dpla import DC_TITLE_FIELD_NAME, SOURCE_RESOURCE_FIELD_NAME
from ingest_wikimedia.s3 import S3_BUCKET, S3Client
from ingest_wikimedia.tools_context import ToolsContext
from ingest_wikimedia.wikimedia import (
    check_content_type,
    compute_ordinal_exts_and_page_labels,
    get_page_title,
    is_download_only,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

COMMONS_API = "https://commons.wikimedia.org/w/api.php"
# Commons accepts up to 50 titles per query for unauthenticated requests;
# matching that cap minimizes round-trips without triggering API limits.
BATCH = 50
# Fallback when a 429/503 response carries no Retry-After header. 30s is
# Commons' usual maxlag retry window; long enough to clear most throttles.
DEFAULT_RETRY_AFTER_SECS = 30
# Inter-request courtesy delay between Commons API calls. Keeps sustained
# request rate well under the unauthenticated ceiling so we never trigger
# the per-IP limiter on long verification runs.
INTER_REQUEST_SLEEP_SECS = 0.4


def _parse_retry_after(value: str | None) -> int:
    """Parse a Retry-After header (delay-seconds or HTTP-date, RFC 7231)."""
    if not value:
        return DEFAULT_RETRY_AFTER_SECS
    try:
        return int(value)
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(value)
        now = datetime.datetime.now(tz=when.tzinfo)
        return max(0, int((when - now).total_seconds()))
    except (TypeError, ValueError):
        return DEFAULT_RETRY_AFTER_SECS


def commons_api(params: dict) -> dict:
    """POST to the Commons API with maxlag + retries on 429/503."""
    params = {**params, "format": "json", "formatversion": "2", "maxlag": "5"}
    body = urllib.parse.urlencode(params).encode("utf-8")
    req = urllib.request.Request(
        COMMONS_API,
        data=body,
        headers={
            "User-Agent": "dpla-bot-verify/1.0 (dominic@dp.la)",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    for attempt in range(8):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                time.sleep(INTER_REQUEST_SLEEP_SECS)
                data = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 503):
                wait = _parse_retry_after(e.headers.get("Retry-After"))
                logging.warning(f"rate-limited; sleeping {wait}s")
                time.sleep(wait)
                continue
            raise
        if "error" in data:
            raise RuntimeError(f"Commons API error: {data['error']}")
        return data
    raise RuntimeError("Commons API retries exhausted")


def classify_ordinal(head: dict) -> tuple[str | None, str, str]:
    """Inspect an S3 object's HEAD metadata and decide whether the uploader
    would upload it. Returns (skip_reason_or_None, sha1, content_type).

    Mirrors the runtime checks process_file() performs. When skip_reason is
    None, the uploader would proceed and upload; otherwise the ordinal is
    skipped and no Commons title should be checked.
    """
    sha1 = head.get("Metadata", {}).get("sha1", "")
    ct = head.get("ContentType", "")
    size = int(head.get("ContentLength", 0))
    if size == 0:
        return "zero_byte_stub", sha1, ct
    if not check_content_type(ct):
        return f"bad_content_type:{ct}", sha1, ct
    if is_download_only(ct):
        return "download_only", sha1, ct
    if ct in ("application/octet-stream", "binary/octet-stream"):
        # process_file does runtime libmagic re-detection; we can't predict
        # its outcome without downloading the file.
        return "octet_stream_needs_redetect", sha1, ct
    ext = mimetypes.guess_extension(ct)
    if not ext or ext == ".bin":
        return f"unresolvable_ext:{ct}", sha1, ct
    return None, sha1, ct


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit("Usage: verify_item.py <dpla_id> <partner>")
    dpla_id, partner = sys.argv[1], sys.argv[2]
    logging.info(f"Verifying {dpla_id} (partner={partner})…")

    ctx = ToolsContext.init(partner)
    item_metadata = ctx.get_dpla().get_item_metadata(dpla_id)
    if not item_metadata:
        sys.exit(f"DPLA item {dpla_id} not found")
    # Use the same title selection as Uploader.process_item(): the FIRST
    # value of sourceResource.title, not the joined extract_strings(...)
    # representation. The uploader feeds titles[0] to get_page_title, so
    # the verifier must do the same to reconstruct the exact filename.
    titles = get_list(
        get_dict(item_metadata, SOURCE_RESOURCE_FIELD_NAME),
        DC_TITLE_FIELD_NAME,
    )
    title_string = titles[0] if titles else ""
    logging.info(f"  item title: {title_string}")

    s3_client_obj = ctx.get_s3_client()
    files = s3_client_obj.get_file_list(partner, dpla_id)
    num_files = len(files)
    if num_files == 0:
        sys.exit(f"No files listed in file_list.txt for {dpla_id}")
    logging.info(f"  total files in source list: {num_files}")

    # Use the same helper the uploader uses to compute per-ordinal page labels
    # so the intended Commons titles match exactly what the bot uploads to,
    # including the gap-squashing per-extension counter logic.
    _, page_labels = compute_ordinal_exts_and_page_labels(
        s3_client_obj, dpla_id, partner, num_files
    )

    s3 = boto3.client("s3")
    correct = 0
    mismatches: list[dict] = []
    redirects: list[dict] = []
    missing: list[dict] = []
    skipped: list[dict] = []  # uploader-skipped ordinals (stub, video, etc.)
    title_to_ord: dict[str, tuple[int, str]] = {}

    for ord_num in range(1, num_files + 1):
        s3_path = S3Client.get_media_s3_path(dpla_id, ord_num, partner)
        try:
            head = s3.head_object(Bucket=S3_BUCKET, Key=s3_path)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey"):
                skipped.append({"ordinal": ord_num, "reason": "missing_in_s3"})
                continue
            raise

        skip_reason, sha1, ct = classify_ordinal(head)
        if skip_reason:
            skipped.append({"ordinal": ord_num, "reason": skip_reason})
            continue

        ext = mimetypes.guess_extension(ct)
        page_label = page_labels.get(ord_num, "")
        title = get_page_title(title_string, dpla_id, ext, page_label or None)
        title_to_ord[f"File:{title}"] = (ord_num, sha1)

    logging.info(
        f"  ordinals to verify: {len(title_to_ord)}  "
        f"skipped by uploader: {len(skipped)}"
    )

    titles = list(title_to_ord.keys())
    for i in range(0, len(titles), BATCH):
        batch = titles[i : i + BATCH]
        data = commons_api(
            {
                "action": "query",
                "titles": "|".join(batch),
                "prop": "imageinfo|info",
                "iiprop": "sha1",
            }
        )
        pages = {p["title"]: p for p in data["query"]["pages"]}
        norm = {n["from"]: n["to"] for n in data["query"].get("normalized", [])}

        for sent in batch:
            (ord_num, s3_sha1) = title_to_ord[sent]
            page = pages.get(norm.get(sent, sent))
            if not page or page.get("missing"):
                missing.append({"ordinal": ord_num, "title": sent})
                continue
            if page.get("redirect"):
                redirects.append({"ordinal": ord_num, "title": sent})
                continue
            info = page.get("imageinfo") or []
            c_sha1 = info[0].get("sha1", "") if info else ""
            if c_sha1 == s3_sha1:
                correct += 1
            else:
                mismatches.append(
                    {
                        "ordinal": ord_num,
                        "title": sent,
                        "s3_sha1": s3_sha1,
                        "commons_sha1": c_sha1,
                    }
                )

        logging.info(
            f"  scanned {min(i + BATCH, len(titles))}/{len(titles)}  "
            f"correct={correct} mismatch={len(mismatches)} "
            f"redirect={len(redirects)} missing={len(missing)}"
        )

    all_correct = (
        correct == len(title_to_ord)
        and not mismatches
        and not redirects
        and not missing
    )
    print("\n" + "=" * 70)
    print(f"VERIFICATION REPORT: {dpla_id}")
    print(f"  item title:        {title_string}")
    print(f"  total files in source list: {num_files}")
    print(f"  ordinals checked:  {len(title_to_ord)}")
    print(f"  CORRECT (S3 sha1 == Commons sha1 at intended title): {correct}")
    print(f"  MISMATCH (different content):                        {len(mismatches)}")
    print(f"  REDIRECT at intended title:                          {len(redirects)}")
    print(f"  MISSING on Commons:                                  {len(missing)}")
    print(f"  SKIPPED by uploader (stub/video/etc):                {len(skipped)}")
    print(f"  {'PASS ✓' if all_correct else 'FAIL ✗'}")
    print("=" * 70)

    for label, items in (
        ("Mismatches (S3 sha1 ≠ Commons sha1)", mismatches),
        ("Redirects at intended title (should be real files)", redirects),
        ("Missing on Commons (should be uploaded)", missing),
    ):
        if items:
            print(f"\n{label}:")
            for it in items[:40]:
                s3s = it.get("s3_sha1", "")
                cs = it.get("commons_sha1", "")
                detail = f"  S3 {s3s[:12]}…  Commons {cs[:12]}…" if s3s else ""
                print(f"  ord {it['ordinal']:4d}  {it['title']}{detail}")
            if len(items) > 40:
                print(f"  …and {len(items) - 40} more")

    if skipped:
        print("\nSkipped by uploader (informational, not failures):")
        by_reason: dict[str, list[int]] = {}
        for s in skipped:
            by_reason.setdefault(s["reason"], []).append(s["ordinal"])
        for reason, ords in sorted(by_reason.items()):
            preview = ", ".join(str(o) for o in ords[:10])
            more = f" …and {len(ords) - 10} more" if len(ords) > 10 else ""
            print(f"  {reason}: ords [{preview}{more}]")

    out_path = f"/tmp/verify_{dpla_id}.json"
    with open(out_path, "w") as f:
        json.dump(
            {
                "dpla_id": dpla_id,
                "partner": partner,
                "title": title_string,
                "num_files": num_files,
                "ordinals_checked": len(title_to_ord),
                "correct": correct,
                "mismatches": mismatches,
                "redirects": redirects,
                "missing": missing,
                "skipped": skipped,
                "pass": all_correct,
            },
            f,
            indent=2,
        )
    print(f"\nFull report: {out_path}")

    sys.exit(0 if all_correct else 1)


if __name__ == "__main__":
    main()
