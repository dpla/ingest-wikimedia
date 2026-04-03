"""
Retroactively fix files uploaded to Wikimedia Commons that landed in the
"unknown institution" category because their institution had no category page yet.

Algorithm: drain the unknown-institution category institution by institution.
For each iteration, take one file from the category, determine its institution,
create the category infrastructure if needed, then touch all Commons file pages
for that institution via search — which triggers the Wikidata Infobox template
to re-evaluate and re-categorize them. Repeat until the category is empty.
"""

import logging
import re
import time

import click
import pywikibot

from ingest_wikimedia.categories import CategoryEnsurer
from ingest_wikimedia.logs import setup_logging
from ingest_wikimedia.wikimedia import get_site, get_wikidata_site

_MAX_BATCH_SIZE = 50  # cap on members fetched per iteration to avoid runaway growth

UNKNOWN_INSTITUTION_CATEGORY = (
    "Category:Media contributed by the Digital Public Library of America"
    " with unknown institution"
)

# Matches: {{ Institution | wikidata = Q12345 ... }}
_INSTITUTION_QID_RE = re.compile(
    r"\{\{\s*Institution\s*\|\s*wikidata\s*=\s*(Q\d+)", re.IGNORECASE
)

# Matches: {{ DPLA | ... | hub = Q12345 | ... }} (multiline-safe)
_HUB_QID_RE = re.compile(
    r"\{\{\s*DPLA\b[^}]*?\|\s*hub\s*=\s*(Q\d+)", re.IGNORECASE | re.DOTALL
)


def _extract_institution_qid(wikitext: str) -> str | None:
    match = _INSTITUTION_QID_RE.search(wikitext)
    return match.group(1) if match else None


def _extract_hub_qid(wikitext: str) -> str | None:
    match = _HUB_QID_RE.search(wikitext)
    return match.group(1) if match else None


@click.command()
@click.option("--dry-run", is_flag=True)
@click.option("--verbose", is_flag=True)
def main(dry_run: bool, verbose: bool) -> None:
    setup_logging("fix-unknown-categories", "fix", logging.INFO)
    start_time = time.time()

    if dry_run:
        logging.warning("---=== DRY RUN ===---")

    commons_site = get_site()
    wikidata_site = get_wikidata_site()
    category_ensurer = CategoryEnsurer(commons_site, wikidata_site, dry_run=dry_run)
    repo = wikidata_site.data_repository()

    unknown_cat = pywikibot.Category(commons_site, UNKNOWN_INSTITUTION_CATEGORY)

    institutions_processed = 0
    files_touched = 0
    # Tracks files we cannot parse, so we don't loop on them forever
    cannot_process: set[str] = set()

    while True:
        # Fetch enough members to skip any we already know are unprocessable,
        # capped to avoid runaway API usage if many files are unparseable.
        batch_size = min(len(cannot_process) + 1, _MAX_BATCH_SIZE)
        members = list(unknown_cat.members(total=batch_size, namespaces=[6]))

        if not members:
            logging.info("Category is empty. Done.")
            break

        file_page = next((p for p in members if p.title() not in cannot_process), None)
        if file_page is None:
            logging.warning(
                f"All {len(members)} visible files could not be processed. Stopping."
            )
            break

        title = file_page.title()
        wikitext = file_page.text
        institution_qid = _extract_institution_qid(wikitext)
        hub_institution_qid = _extract_hub_qid(wikitext)

        if not institution_qid or not hub_institution_qid:
            logging.warning(f"Could not extract Q-IDs from '{title}' — skipping.")
            cannot_process.add(title)
            continue

        institution_item = pywikibot.ItemPage(repo, institution_qid)
        institution_item.get()
        institution_name = institution_item.labels.get("en", institution_qid)

        logging.info(f"Processing institution: {institution_name} ({institution_qid})")

        try:
            category_ensurer.ensure(
                institution_qid, institution_name, hub_institution_qid
            )
        except Exception as e:
            logging.error(
                f"Failed to ensure category for {institution_name} ({institution_qid})",
                exc_info=e,
            )
            cannot_process.add(title)
            continue

        institutions_processed += 1

        # Touch all Commons file pages for this institution so the Wikidata Infobox
        # template re-evaluates and moves them out of the unknown-institution category.
        count = 0
        for page in commons_site.search(
            f'insource:"Institution" insource:"wikidata = {institution_qid}"',
            namespaces=[6],
        ):
            if verbose:
                logging.info(
                    f"  {'Would touch' if dry_run else 'Touching'}: {page.title()}"
                )
            if not dry_run:
                try:
                    page.touch()
                except Exception as e:
                    logging.warning(f"Failed to touch '{page.title()}'", exc_info=e)
            count += 1

        files_touched += count
        logging.info(
            f"{'Would touch' if dry_run else 'Touched'} {count} files "
            f"for {institution_name}"
        )

    elapsed = time.time() - start_time
    logging.info(
        f"Done. Institutions processed: {institutions_processed}, "
        f"files {'would touch' if dry_run else 'touched'}: {files_touched}, "
        f"elapsed: {elapsed:.1f}s"
    )


if __name__ == "__main__":
    main()
