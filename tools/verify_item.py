#!/usr/bin/env python3
"""
End-to-end verification that every S3 file for a DPLA item has its exact
sha1 landed at its intended Commons title.

For each ordinal under the item's S3 prefix this script:
  1. reads the S3 object's sha1 from the CHECKSUM metadata,
  2. reconstructs the intended Commons title via get_page_title() using
     the DPLA item's source title and the S3 object's MIME-derived
     extension,
  3. queries the Commons MediaWiki API for the file at that intended
     title and compares its sha1 to S3.

Outcomes per ordinal:
  CORRECT  — Commons file at the intended title has sha1 == S3 sha1
  MISMATCH — file exists at the intended title with a different sha1
  REDIRECT — intended title is a redirect (S3 sha1 isn't at the right name)
  MISSING  — intended title has no page on Commons

Exits 0 only when every ordinal is CORRECT. Anything else exits 1 so the
script can be wired into a CI gate or a deploy verification loop.

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
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

import boto3

from ingest_wikimedia.s3 import S3_BUCKET
from ingest_wikimedia.tools_context import ToolsContext
from ingest_wikimedia.wikimedia import extract_strings, get_page_title

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

COMMONS_API = "https://commons.wikimedia.org/w/api.php"
BATCH = 50
DC_TITLE_FIELD = "title"  # under sourceResource
DEFAULT_RETRY_AFTER_SECS = 30


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


def s3_path_prefix(dpla_id: str, partner: str) -> str:
    return (
        f"{partner}/images/{dpla_id[0]}/{dpla_id[1]}/"
        f"{dpla_id[2]}/{dpla_id[3]}/{dpla_id}/"
    )


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
                time.sleep(0.4)
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


def list_s3_ordinals(dpla_id: str, partner: str) -> dict[int, tuple[str, str, str]]:
    """Return {ordinal: (key, sha1, content_type)} for every media file under
    the item's S3 prefix."""
    s3 = boto3.client("s3")
    prefix = s3_path_prefix(dpla_id, partner)
    out: dict[int, tuple[str, str, str]] = {}
    name_re = re.compile(r"^(\d+)_" + re.escape(dpla_id) + r"$")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            m = name_re.match(key.rsplit("/", 1)[-1])
            if not m:
                continue
            head = s3.head_object(Bucket=S3_BUCKET, Key=key)
            sha1 = head.get("Metadata", {}).get("sha1", "")
            ct = head.get("ContentType", "")
            out[int(m.group(1))] = (key, sha1, ct)
    return out


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit("Usage: verify_item.py <dpla_id> <partner>")
    dpla_id, partner = sys.argv[1], sys.argv[2]
    logging.info(f"Verifying {dpla_id} (partner={partner})…")

    # DPLA item title (needed to reconstruct the intended Commons titles)
    ctx = ToolsContext.init(partner)
    item_metadata = ctx.get_dpla().get_item_metadata(dpla_id)
    if not item_metadata:
        sys.exit(f"DPLA item {dpla_id} not found")
    title_string = extract_strings(
        item_metadata.get("sourceResource", {}), DC_TITLE_FIELD
    )
    logging.info(f"  item title: {title_string}")

    s3_files = list_s3_ordinals(dpla_id, partner)
    if not s3_files:
        sys.exit(f"No S3 ordinals under prefix {s3_path_prefix(dpla_id, partner)}")
    ordinals = sorted(s3_files)
    multipage = len(ordinals) > 1
    logging.info(
        f"  S3: {len(ordinals)} ordinals, range {ordinals[0]}..{ordinals[-1]}; "
        f"{'multi-page' if multipage else 'single-page'}"
    )

    def expected_title(ord_num: int, content_type: str) -> str:
        ext = mimetypes.guess_extension(content_type) or ".jpg"
        if ext == ".bin":
            ext = ".jpg"
        return get_page_title(
            title_string, dpla_id, ext, ord_num if multipage else None
        )

    correct = 0
    mismatches: list[dict] = []
    redirects: list[dict] = []
    missing: list[dict] = []

    for i in range(0, len(ordinals), BATCH):
        batch = ordinals[i : i + BATCH]
        title_to_ord: dict[str, tuple[int, str]] = {}
        for o in batch:
            _, sha1, ct = s3_files[o]
            title_to_ord[f"File:{expected_title(o, ct)}"] = (o, sha1)

        data = commons_api(
            {
                "action": "query",
                "titles": "|".join(title_to_ord),
                "prop": "imageinfo|info",
                "iiprop": "sha1",
            }
        )
        pages = {p["title"]: p for p in data["query"]["pages"]}
        norm = {n["from"]: n["to"] for n in data["query"].get("normalized", [])}

        for sent, (o, s3_sha1) in title_to_ord.items():
            page = pages.get(norm.get(sent, sent))
            if not page or page.get("missing"):
                missing.append({"ordinal": o, "title": sent})
                continue
            if page.get("redirect"):
                redirects.append({"ordinal": o, "title": sent})
                continue
            info = page.get("imageinfo") or []
            c_sha1 = info[0].get("sha1", "") if info else ""
            if c_sha1 == s3_sha1:
                correct += 1
            else:
                mismatches.append(
                    {
                        "ordinal": o,
                        "title": sent,
                        "s3_sha1": s3_sha1,
                        "commons_sha1": c_sha1,
                    }
                )

        logging.info(
            f"  scanned {min(i + BATCH, len(ordinals))}/{len(ordinals)}  "
            f"correct={correct} mismatch={len(mismatches)} "
            f"redirect={len(redirects)} missing={len(missing)}"
        )

    total = len(ordinals)
    all_correct = correct == total
    print("\n" + "=" * 70)
    print(f"VERIFICATION REPORT: {dpla_id}")
    print(f"  item title:  {title_string}")
    print(f"  S3 ordinals: {total}")
    print(f"  CORRECT (S3 sha1 == Commons sha1 at intended title): {correct}")
    print(f"  MISMATCH (different content):                        {len(mismatches)}")
    print(f"  REDIRECT at intended title:                          {len(redirects)}")
    print(f"  MISSING on Commons:                                  {len(missing)}")
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
                print(f"  page {it['ordinal']:4d}  {it['title']}{detail}")
            if len(items) > 40:
                print(f"  …and {len(items) - 40} more")

    out_path = f"/tmp/verify_{dpla_id}.json"
    with open(out_path, "w") as f:
        json.dump(
            {
                "dpla_id": dpla_id,
                "partner": partner,
                "title": title_string,
                "total_ordinals": total,
                "correct": correct,
                "mismatches": mismatches,
                "redirects": redirects,
                "missing": missing,
                "pass": all_correct,
            },
            f,
            indent=2,
        )
    print(f"\nFull report: {out_path}")

    sys.exit(0 if all_correct else 1)


if __name__ == "__main__":
    main()
