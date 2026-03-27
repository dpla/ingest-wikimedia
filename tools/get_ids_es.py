"""
Query Elasticsearch directly for wiki-eligible DPLA IDs for a given partner hub.

Eligibility criteria applied:
  1. Hub        — provider.name matches the given partner
  2. Rights     — rightsCategory = "Unlimited Re-Use"
  3. Asset      — has mediaMaster, iiifManifest, or an isShownAt URL from which
                  a IIIF manifest can be formulaically derived (e.g. CONTENTdm)
  4. Institution — dataProvider has a Wikidata ID AND
                   (hub.upload=True OR institution.upload=True) per institutions_v2.json
  5. Block list  — ID not present in dpla-id-banlist.txt

institutions_v2.json is fetched fresh from the GitHub main branch on each run
so that recent eligibility changes are always reflected.

Output: one DPLA ID per line to stdout.  Redirect to produce the IDs CSV
consumed by the downloader and uploader:

    get-ids-es pa > pa/pa.csv
"""

import sys

import click
import requests

from ingest_wikimedia.banlist import Banlist
from ingest_wikimedia.dpla import DPLA, DPLA_PARTNERS, INSTITUTIONS_URL

ES_URL = "http://search-prod1.internal.dp.la:9200/dpla_alias/_search"
WIKIDATA_BASE_URI = "http://www.wikidata.org/entity/"
PAGE_SIZE = 500

# isShownAt URL patterns from which a IIIF manifest can be formulaically
# derived, expressed as ES wildcard values.  Extend this list as new DAMs
# with predictable IIIF paths are identified.
IIIF_DERIVABLE_ISSHOWNAT_PATTERNS = [
    "*/cdm/ref/collection/*/id/*",  # CONTENTdm
]


def load_eligible_dp_uris(partner: str) -> list[str]:
    """
    Fetch institutions_v2.json from GitHub and return the list of
    dataProvider Wikidata URIs that are eligible for upload for the given hub.

    An institution is eligible when:
      - it has a non-empty Wikidata ID, AND
      - hub.upload=True  (the entire hub is open; every institution counts)
        OR institution.upload=True  (this specific institution is approved)
    """
    hub_name = DPLA_PARTNERS[partner]
    resp = requests.get(INSTITUTIONS_URL, timeout=15)
    resp.raise_for_status()
    institutions = resp.json()

    hub = institutions.get(hub_name)
    if not hub:
        raise ValueError(f"Hub '{hub_name}' not found in institutions_v2.json")

    hub_upload = hub.get("upload", False)

    eligible = []
    for inst_info in hub.get("institutions", {}).values():
        wikidata_id = inst_info.get("Wikidata", "")
        inst_upload = inst_info.get("upload", False)
        if wikidata_id and (hub_upload or inst_upload):
            eligible.append(f"{WIKIDATA_BASE_URI}{wikidata_id}")

    return eligible


def build_query(
    provider_name: str,
    eligible_dp_uris: list[str],
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

    query: dict = {
        "_source": ["id"],
        "size": PAGE_SIZE,
        "sort": ["id", "_doc"],
        "query": {
            "bool": {
                "filter": [
                    {"term": {"provider.name.not_analyzed": provider_name}},
                    {"term": {"rightsCategory": "Unlimited Re-Use"}},
                    {"terms": {"dataProvider.exactMatch": eligible_dp_uris}},
                    {
                        "bool": {
                            "should": asset_should,
                            "minimum_should_match": 1,
                        }
                    },
                ]
            }
        },
    }

    if search_after:
        query["search_after"] = search_after

    return query


@click.command()
@click.argument("partner")
def main(partner: str) -> None:
    """Print wiki-eligible DPLA IDs for PARTNER to stdout, one per line."""
    try:
        DPLA.check_partner(partner)
    except ValueError as e:
        raise click.BadParameter(str(e)) from e

    provider_name = DPLA_PARTNERS[partner]

    eligible_dp_uris = load_eligible_dp_uris(partner)
    if not eligible_dp_uris:
        print(
            f"No eligible institutions found for {partner} in institutions_v2.json",
            file=sys.stderr,
        )
        sys.exit(0)

    banlist = Banlist()
    search_after = None

    while True:
        query = build_query(provider_name, eligible_dp_uris, search_after)
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
            dpla_id = hit["_source"]["id"]
            if not banlist.is_banned(dpla_id):
                print(dpla_id)

        search_after = hits[-1]["sort"]


if __name__ == "__main__":
    main()
