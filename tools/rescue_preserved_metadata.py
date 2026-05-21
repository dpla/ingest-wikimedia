#!/usr/bin/env python3
"""
Rescue PD-USGov / Image-extracted / Category metadata from file pages
that DPLA bot's title-drift correction edits overwrote *before* PR #202
introduced ``merge_preserved_wikitext``.

The bug: pre-PR-#202, ``_move_to_correct_title`` and
``_resolve_redirect_overwrite`` wrote a freshly-generated ``{{Artwork}}``
block to the page, blowing away any pre-existing license tags, parent
``{{Image extracted}}`` links, and ``[[Category:...]]`` membership added
by humans or earlier bot runs. PR #202 fixed forward behavior but did
nothing for the ~1.5-3k pages already stripped.

For each affected page this script:
  1. Finds the EARLIEST title-drift overwrite in the page's history.
  2. Walks back to the most recent non-redirect predecessor revision
     (page moves transfer history, so this picks up the original
     pre-move content; redirect-overwrite cases skip the redirect
     revision).
  3. Extracts ``extract_preserved_metadata(...)`` from that revision.
  4. Diffs against the current page text — finds items that exist in
     the pre-strip revision but not the current page.
  5. If anything is missing, appends the missing items to the current
     text in the same order ``merge_preserved_wikitext`` uses and saves.

Idempotent: pages already showing all preserved items are skipped.

Usage:
    python3 tools/rescue_preserved_metadata.py [--dry-run] [--max N]
                                              [--since YYYY-MM-DD]
"""

import argparse
import logging
import sys
import time

sys.path.insert(0, "/home/ec2-user/ingest-wikimedia")

import pywikibot  # noqa: E402

from ingest_wikimedia.wikimedia import (  # noqa: E402
    extract_preserved_metadata,
    get_page,
    get_site,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Edit-summary substrings that identify a title-drift TEXT overwrite (not
# the move log event, which doesn't change wikitext). Match both the
# pre-PR-#202 form ``(DPLA ID xxx)`` and the post-PR-#202 form with the
# ``[[dpla:xxx|xxx]]`` link — only the prefix matters here.
TITLE_DRIFT_OVERWRITE_PATTERNS = (
    "Update description after title drift correction",
    "Replacing redirect with DPLA metadata for title drift correction",
)

RESCUE_EDIT_SUMMARY = (
    "Restore metadata stripped by earlier title-drift correction "
    "(rescued from pre-strip revision)"
)

FILE_NAMESPACE = 6


def find_affected_pages(site, since=None, max_pages=None):
    """Iterate DPLA bot's File-namespace contribs and yield unique
    (page_title, earliest_drift_revid) for any contrib whose comment
    matches a title-drift overwrite pattern.

    ``since`` is a YYYY-MM-DD string; only contribs with timestamp >=
    this are considered (Commons API expects ISO 8601).
    """
    bot = pywikibot.User(site, "DPLA bot")
    earliest: dict[str, int] = {}
    total_seen = 0

    kwargs = {"namespaces": [FILE_NAMESPACE]}
    if since:
        kwargs["start"] = pywikibot.Timestamp.fromISOformat(f"{since}T00:00:00Z")

    for entry in bot.contributions(**kwargs):
        page, revid, ts, comment = entry
        total_seen += 1
        if total_seen % 5000 == 0:
            logging.info(
                f"  scanned {total_seen} contribs; "
                f"{len(earliest)} candidate pages so far"
            )

        if not any(p in (comment or "") for p in TITLE_DRIFT_OVERWRITE_PATTERNS):
            continue

        title = page.title()
        # Keep the OLDEST drift edit per page (smallest revid). The
        # pre-strip state is the predecessor of THAT edit.
        if title not in earliest or revid < earliest[title]:
            earliest[title] = revid

        if max_pages and len(earliest) >= max_pages:
            break

    logging.info(
        f"Scanned {total_seen} bot contribs; "
        f"found {len(earliest)} unique pages with title-drift overwrites"
    )
    return earliest


def _is_redirect_text(text: str) -> bool:
    return text.lstrip().lower().startswith("#redirect")


def get_pre_strip_text(site, page_title, drift_revid, max_walk=10):
    """Walk back from drift_revid through older revisions and return the
    text of the most recent non-redirect predecessor.

    Page moves transfer revision history to the new title, so for a
    move-then-strip sequence the predecessor IS the original rich
    content (the move itself doesn't change text). For a
    redirect-overwrite sequence the immediate predecessor IS the
    redirect markup — skip it and walk back to the last content
    revision.

    Returns (text, revid) or (None, None) if no non-redirect predecessor
    found within max_walk steps.
    """
    request = pywikibot.data.api.Request(
        site=site,
        parameters={
            "action": "query",
            "prop": "revisions",
            "titles": page_title,
            "rvprop": "content|ids|timestamp|comment|user",
            "rvslots": "main",
            "rvstartid": drift_revid,
            "rvdir": "older",
            "rvlimit": max_walk + 1,
        },
    )
    data = request.submit()
    for _, p in data.get("query", {}).get("pages", {}).items():
        revs = p.get("revisions", []) or []
        # First entry is drift_revid itself; subsequent entries are
        # older revisions (newest-to-oldest).
        for rev in revs[1:]:
            text = rev.get("slots", {}).get("main", {}).get("*", "")
            if text and not _is_redirect_text(text):
                return text, rev.get("revid")
    return None, None


def compute_rescue(pre_strip_text, current_text):
    """Return (missing_pd, missing_ie, missing_cat) — items in
    pre_strip_text but absent from current_text. Empty lists mean
    nothing to rescue."""
    pre_pd, pre_ie, pre_cat = extract_preserved_metadata(pre_strip_text)
    cur_pd, cur_ie, cur_cat = extract_preserved_metadata(current_text)

    cur_pd_set, cur_ie_set, cur_cat_set = set(cur_pd), set(cur_ie), set(cur_cat)
    missing_pd = [x for x in pre_pd if x not in cur_pd_set]
    missing_ie = [x for x in pre_ie if x not in cur_ie_set]
    missing_cat = [x for x in pre_cat if x not in cur_cat_set]
    return missing_pd, missing_ie, missing_cat


def build_rescued_text(current_text, missing_pd, missing_ie, missing_cat):
    """Append missing items to current_text in the same order
    ``merge_preserved_wikitext`` uses."""
    parts = [current_text.rstrip()]
    for group in (missing_pd, missing_ie, missing_cat):
        if group:
            parts.append("")
            parts.extend(group)
    return "\n".join(parts) + "\n"


def main():
    parser = argparse.ArgumentParser(
        description="Retroactive metadata rescue for title-drift overwrites."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Inspect without saving (default: writes to Commons).",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        help="Process at most N pages (after discovery). Useful for testing.",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Only consider bot contribs newer than this YYYY-MM-DD date.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="Seconds to sleep between save operations (default: 0.5).",
    )
    args = parser.parse_args()

    logging.info("Logging in to Commons…")
    site = get_site()
    logging.info(f"Logged in as: {site.user()}")
    logging.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE (will save)'}")
    if args.since:
        logging.info(f"Filtering bot contribs since: {args.since}")

    earliest = find_affected_pages(site, since=args.since, max_pages=None)

    processed = 0
    rescued = 0
    nothing_missing = 0
    no_predecessor = 0
    page_missing = 0
    errors = 0

    for page_title, drift_revid in earliest.items():
        if args.max and processed >= args.max:
            break
        processed += 1

        pre_text, pre_revid = get_pre_strip_text(site, page_title, drift_revid)
        if pre_text is None:
            logging.warning(
                f"  no non-redirect predecessor for {page_title} "
                f"(drift revid {drift_revid})"
            )
            no_predecessor += 1
            continue

        page = get_page(site, page_title)
        if not page.exists():
            logging.warning(f"  {page_title} no longer exists")
            page_missing += 1
            continue

        current_text = page.text or ""
        missing_pd, missing_ie, missing_cat = compute_rescue(pre_text, current_text)

        if not (missing_pd or missing_ie or missing_cat):
            nothing_missing += 1
            continue

        rescued += 1
        logging.info(
            f"  RESCUE {page_title}: "
            f"+{len(missing_pd)} PD-USGov, "
            f"+{len(missing_ie)} Image-extracted, "
            f"+{len(missing_cat)} Category "
            f"(pre-strip rev {pre_revid})"
        )

        if args.dry_run:
            continue

        new_text = build_rescued_text(current_text, missing_pd, missing_ie, missing_cat)
        try:
            page.text = new_text
            page.save(summary=RESCUE_EDIT_SUMMARY, minor=False)
            time.sleep(args.sleep)
        except Exception as e:
            logging.warning(f"  save failed for {page_title}: {e}")
            errors += 1

    logging.info(
        f"\nDONE: processed={processed}  rescued={rescued}  "
        f"nothing_missing={nothing_missing}  "
        f"no_predecessor={no_predecessor}  "
        f"page_missing={page_missing}  errors={errors}"
    )
    sys.exit(0 if errors == 0 else 1)


if __name__ == "__main__":
    main()
