"""
Query Elasticsearch directly for wiki-eligible DPLA IDs for a given partner hub.

Eligibility criteria applied:
  1. Hub        — provider.name matches the given partner
  2. Rights     — rightsCategory = "Unlimited Re-Use"
  3. Asset      — has mediaMaster, iiifManifest, or an isShownAt URL from which
                  a IIIF manifest can be formulaically derived (e.g. CONTENTdm)
  4. Institution — dataProvider.name matches an eligible name string from
                   institutions_v2.json, where eligible means the hub has a
                   Wikidata ID AND the institution has a Wikidata ID AND
                   (hub.upload=True OR institution.upload=True).
                   The hub Wikidata requirement ensures the hub-level Commons
                   category can be created during upload.
                   Name strings are used (not Wikidata URIs) so that two name variants
                   mapping to the same Wikidata ID can have independent upload flags.
  5. Block list  — ID not present in dpla-id-banlist.txt

institutions_v2.json is fetched fresh from the GitHub main branch on each run
so that recent eligibility changes are always reflected.

For each eligible item, the full ES source document is written to S3 as
dpla-map.json so the downloader can skip redundant DPLA API calls entirely.
CONTENTdm items have their IIIF manifest URL derived from isShownAt and patched
into the document before it is written.

Output: one DPLA ID per line to stdout.  Redirect to produce the IDs CSV
consumed by the downloader and uploader:

    get-ids-es pa > pa/pa.csv
"""

import sys
from concurrent.futures import ThreadPoolExecutor

import click
import requests

from ingest_wikimedia.banlist import Banlist
from ingest_wikimedia.dpla import DPLA, DPLA_PARTNERS, INSTITUTIONS_URL
from ingest_wikimedia.iiif import IIIF
from ingest_wikimedia.s3 import S3Client
from ingest_wikimedia.slack import notify_phase_start
from ingest_wikimedia.staging import make_s3_stage_context, stage_item_to_s3

ES_URL = "http://search-prod1.internal.dp.la:9200/dpla_alias/_search"
PAGE_SIZE = 500
S3_WRITE_WORKERS = 10

IIIF_MANIFEST_FIELD = "iiifManifest"
MEDIA_MASTER_FIELD = "mediaMaster"
IS_SHOWN_AT_FIELD = "isShownAt"

# isShownAt URL patterns from which a IIIF manifest can be formulaically
# derived, expressed as ES wildcard values.  Extend this list as new DAMs
# with predictable IIIF paths are identified.
IIIF_DERIVABLE_ISSHOWNAT_PATTERNS = [
    "*/cdm/ref/collection/*/id/*",  # CONTENTdm
]


def load_eligible_dp_names(partner: str) -> list[str]:
    """
    Fetch institutions_v2.json from GitHub and return the list of
    dataProvider name strings that are eligible for upload for the given hub.

    An institution is eligible when:
      - the hub has a non-empty Wikidata ID (required for hub Commons category), AND
      - the institution has a non-empty Wikidata ID, AND
      - hub.upload=True  (the entire hub is open; every institution counts)
        OR institution.upload=True  (this specific institution is approved)

    Name strings are used rather than Wikidata URIs so that two provider name
    variants mapping to the same Wikidata ID can carry independent upload flags.
    """
    hub_name = DPLA_PARTNERS[partner]
    resp = requests.get(INSTITUTIONS_URL, timeout=15)
    resp.raise_for_status()
    institutions = resp.json()

    hub = institutions.get(hub_name)
    if not hub:
        raise ValueError(f"Hub '{hub_name}' not found in institutions_v2.json")

    hub_upload = hub.get("upload", False)
    hub_wikidata = hub.get("Wikidata", "")

    eligible = []
    for inst_name, inst_info in hub.get("institutions", {}).items():
        wikidata_id = inst_info.get("Wikidata", "")
        inst_upload = inst_info.get("upload", False)
        if hub_wikidata and wikidata_id and (hub_upload or inst_upload):
            eligible.append(inst_name)

    return eligible


def build_query(
    provider_name: str,
    eligible_dp_names: list[str],
    collection: str | None = None,
    search_after: list | None = None,
) -> dict:
    """Build the Elasticsearch boolean query for a single page."""
    asset_should = [
        {"exists": {"field": "mediaMaster"}},
        {"exists": {"field": "iiifManifest"}},
        *[
            {"wildcard": {"isShownAt": pattern}}
            for pattern in IIIF_DERIVABLE_ISSHOWNAT_PATTERNS
        ],
    ]

    filters: list[dict] = [
        {"term": {"provider.name.not_analyzed": provider_name}},
        {"term": {"rightsCategory": "Unlimited Re-Use"}},
        {"terms": {"dataProvider.name.not_analyzed": eligible_dp_names}},
        {
            "bool": {
                "should": asset_should,
                "minimum_should_match": 1,
            }
        },
    ]

    if collection is not None:
        filters.append(
            {"term": {"sourceResource.collection.title.not_analyzed": collection}}
        )

    query: dict = {
        "size": PAGE_SIZE,
        "sort": ["id", "_doc"],
        "query": {"bool": {"filter": filters}},
    }

    if search_after:
        query["search_after"] = search_after

    return query


@click.command()
@click.argument("partner")
@click.option(
    "--institution",
    default=None,
    help="Restrict to a single institution name (must be upload-eligible).",
)
@click.option(
    "--collection",
    default=None,
    help="Restrict to items in a specific collection title (requires --institution).",
)
def main(partner: str, institution: str | None, collection: str | None) -> None:
    """Print wiki-eligible DPLA IDs for PARTNER to stdout, one per line.

    Also stages each item's full metadata to S3 (dpla-map.json) so the
    downloader can skip DPLA API calls entirely.
    """
    if collection is not None:
        collection = collection.strip()
        if not collection:
            print("--collection cannot be empty.", file=sys.stderr)
            sys.exit(1)
        if institution is None:
            print(
                "--collection requires --institution to be specified.", file=sys.stderr
            )
            sys.exit(1)

    try:
        DPLA.check_partner(partner)
    except ValueError as e:
        raise click.BadParameter(str(e)) from e

    notify_phase_start(partner, "id-generation")
    provider_name = DPLA_PARTNERS[partner]

    eligible_dp_names = load_eligible_dp_names(partner)
    if not eligible_dp_names:
        print(
            f"No eligible institutions found for {partner} in institutions_v2.json",
            file=sys.stderr,
        )
        sys.exit(0)

    if institution is not None:
        if institution not in eligible_dp_names:
            print(
                f"Institution '{institution}' is not upload-eligible for {partner}.",
                file=sys.stderr,
            )
            sys.exit(1)
        eligible_dp_names = [institution]

    banlist = Banlist()
    s3_client = S3Client()
    search_after = None

    s3_sem, failed, _on_s3_done = make_s3_stage_context(S3_WRITE_WORKERS)

    with ThreadPoolExecutor(max_workers=S3_WRITE_WORKERS) as executor:
        while True:
            query = build_query(
                provider_name, eligible_dp_names, collection, search_after
            )
            response = requests.post(
                ES_URL,
                json=query,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            response.raise_for_status()
            page = response.json()
            hits = page["hits"]["hits"]

            if not hits:
                break

            for hit in hits:
                source = hit["_source"]
                dpla_id = source["id"]

                if banlist.is_banned(dpla_id):
                    continue

                # Derive ContentDM IIIF manifest URL if the record has neither
                # mediaMaster nor iiifManifest but has a derivable isShownAt.
                if not source.get(IIIF_MANIFEST_FIELD) and not source.get(
                    MEDIA_MASTER_FIELD
                ):
                    is_shown_at = source.get(IS_SHOWN_AT_FIELD, "")
                    iiif_url = IIIF.contentdm_iiif_url(is_shown_at)
                    if iiif_url:
                        source[IIIF_MANIFEST_FIELD] = iiif_url

                # Mark as staged by get-ids-es so the downloader can
                # distinguish fresh ES-sourced objects from legacy API ones.
                source["_staged_by_get_ids_es"] = True

                s3_sem.acquire()
                future = executor.submit(
                    stage_item_to_s3, s3_client, partner, dpla_id, source
                )
                future.add_done_callback(_on_s3_done(dpla_id))

                print(dpla_id)

            search_after = hits[-1]["sort"]

    if failed[0]:
        print(f"Error: {failed[0]} S3 writes failed", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
