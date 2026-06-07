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

After pagination completes, this tool also writes a per-item sdc.json
containing the ready-to-POST Wikibase claim list (P760, P1476, P195, P170,
P571, P4272, P921, P10358, P9126, P7482, P217, plus the NARA-only P1225,
P7228, P6224). Subject reconciliation against Wikidata is batched across
the whole hub via wikidata.reconci.link so the sync phase can be a pure
diff+POST step against pre-computed claims.

Output: one DPLA ID per line to stdout.  Redirect to produce the IDs CSV
consumed by the downloader and uploader:

    get-ids-es pa > pa/pa.csv
"""

import datetime
import json
import logging
import os
import sys
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor

import click
import requests

from ingest_wikimedia.banlist import Banlist
from ingest_wikimedia.dpla import DPLA, INSTITUTIONS_URL
from ingest_wikimedia.partners import PARTNER_HUBS
from ingest_wikimedia.es import check_es_response, post_es
from ingest_wikimedia.iiif import IIIF
from ingest_wikimedia.s3 import APPLICATION_JSON, SDC_FILENAME, S3Client
from ingest_wikimedia.sdc import (
    normalize_rights_uri,
    build_claims_for_doc,
    collect_subject_queries,
    reconcile_subjects,
)
from ingest_wikimedia.slack import notify_phase_start
from ingest_wikimedia.staging import make_s3_stage_context, stage_item_to_s3

PAGE_SIZE = 500
S3_WRITE_WORKERS = 10

IIIF_MANIFEST_FIELD = "iiifManifest"
MEDIA_MASTER_FIELD = "mediaMaster"
IS_SHOWN_AT_FIELD = "isShownAt"

# SDC pre-compute inputs. subjects.json is the DPLA-subject → Wikidata-Q-ID
# map used to populate P921 alongside the string-form P4272. rights.json is
# the small SDC-specific copyright mapping vendored in this repo.
SUBJECTS_URL = (
    "https://raw.githubusercontent.com/dpla/ingestion3/develop/"
    "src/main/resources/subjects.json"
)
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# isShownAt URL patterns from which a IIIF manifest can be formulaically
# derived, expressed as ES wildcard values.  Extend this list as new DAMs
# with predictable IIIF paths are identified.
IIIF_DERIVABLE_ISSHOWNAT_PATTERNS = [
    "*/cdm/ref/collection/*/id/*",  # CONTENTdm
]


def fetch_institutions_v2() -> dict:
    """Fetch the full institutions_v2.json document used for hub/institution
    eligibility and (in the SDC pre-compute pass) Wikidata-ID resolution."""
    resp = requests.get(INSTITUTIONS_URL, timeout=15)
    resp.raise_for_status()
    return resp.json()


def fetch_subjects_json() -> dict:
    """Fetch the DPLA-subject → Wikidata-ID map used to populate P921.

    Sourced from dpla/ingestion3 alongside institutions_v2.json; fetched
    fresh per run so upstream changes land in the next sync without a
    redeploy.
    """
    resp = requests.get(SUBJECTS_URL, timeout=30)
    resp.raise_for_status()
    return resp.json()


def load_rights_json() -> dict:
    """Load rights.json from the repo root and normalize its keys for
    scheme-and-slash-insensitive lookup."""
    with open(os.path.join(_REPO_ROOT, "rights.json")) as f:
        raw = json.load(f)
    return {normalize_rights_uri(k): v for k, v in raw.items()}


def load_eligible_dp_names(institutions: dict, partner: str) -> list[str]:
    """
    Return the list of dataProvider name strings eligible for upload for the
    given hub, given an already-loaded institutions_v2.json document.

    An institution is eligible when:
      - the hub has a non-empty Wikidata ID (required for hub Commons category), AND
      - the institution has a non-empty Wikidata ID, AND
      - hub.upload=True  (the entire hub is open; every institution counts)
        OR institution.upload=True  (this specific institution is approved)

    Name strings are used rather than Wikidata URIs so that two provider name
    variants mapping to the same Wikidata ID can carry independent upload flags.
    """
    hub_name = PARTNER_HUBS[partner]

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


def stage_sdc_to_s3(
    s3_client: S3Client, partner: str, dpla_id: str, sdc_payload: dict
) -> None:
    """Write the per-item sdc.json sidecar to the partner's item prefix.

    Raises on failure so the caller's ThreadPoolExecutor can observe it
    via future.exception() in the done callback (same pattern as
    stage_item_to_s3).
    """
    s3_client.write_item_file(
        partner, dpla_id, json.dumps(sdc_payload), SDC_FILENAME, APPLICATION_JSON
    )


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
    "institutions",
    multiple=True,
    help=(
        "Restrict to a specific institution name (must be upload-eligible)."
        " May be passed multiple times to combine several institutions into"
        " one ID-generation run — used by the launch script when a single"
        " Wikidata QID resolves to multiple institutions under one hub."
    ),
)
@click.option(
    "--collection",
    default=None,
    help=(
        "Restrict to items in a specific collection title. Requires exactly"
        " one --institution (collection scoping is per-institution)."
    ),
)
def main(partner: str, institutions: tuple[str, ...], collection: str | None) -> None:
    """Print wiki-eligible DPLA IDs for PARTNER to stdout, one per line.

    Also stages each item's full metadata to S3 (dpla-map.json) so the
    downloader can skip DPLA API calls entirely.

    Multiple ``--institution`` flags are ORed together in the
    Elasticsearch ``dataProvider`` filter, so the output covers items
    belonging to any of the listed institutions in one combined run.
    No ``--institution`` flags means "all eligible institutions for
    this hub" (the existing hub-level behaviour).
    """
    if collection is not None:
        collection = collection.strip()
        if not collection:
            print("--collection cannot be empty.", file=sys.stderr)
            sys.exit(1)
        if len(institutions) != 1:
            print(
                "--collection requires exactly one --institution to be specified"
                " (collection scoping is per-institution).",
                file=sys.stderr,
            )
            sys.exit(1)

    try:
        DPLA.check_partner(partner)
    except ValueError as e:
        raise click.BadParameter(str(e)) from e

    notify_phase_start(partner, "id-generation")
    provider_name = PARTNER_HUBS[partner]

    # Load institutions_v2.json once and reuse it for both eligibility
    # filtering and the SDC pre-compute pass.
    institutions_json = fetch_institutions_v2()
    eligible_dp_names = load_eligible_dp_names(institutions_json, partner)
    if not eligible_dp_names:
        print(
            f"No eligible institutions found for {partner} in institutions_v2.json",
            file=sys.stderr,
        )
        sys.exit(0)

    if institutions:
        ineligible = [name for name in institutions if name not in eligible_dp_names]
        if ineligible:
            print(
                f"Institution(s) not upload-eligible for {partner}: {ineligible}.",
                file=sys.stderr,
            )
            sys.exit(1)
        eligible_dp_names = list(institutions)

    # SDC pre-compute inputs.
    rights = load_rights_json()
    subject_ids = fetch_subjects_json()
    retrieval_date = datetime.date.today()

    banlist = Banlist()
    s3_client = S3Client()
    search_after = None

    s3_sem, failed, _on_s3_done = make_s3_stage_context(S3_WRITE_WORKERS)

    # Phase 1 — paginate ES, stage dpla-map.json, remember each item's DPLA
    # ID for the SDC pass below, and collect NARA exactMatch subject queries
    # for batched Wikidata reconciliation.
    #
    # We deliberately do NOT buffer the full ES `_source` document in memory:
    # for a hub with 100K items that would be ~1 GB resident. Phase 3 re-reads
    # each dpla-map.json from S3 instead — cheap and bounded by S3 throughput,
    # not by hub size in RAM.
    #
    # subject_queries is a set because the reconci.link call itself dedupes
    # internally but pre-deduping bounds phase-1 memory by unique subjects
    # rather than total occurrences across the hub.
    dpla_ids: list[str] = []
    subject_queries: set[tuple[str, str]] = set()

    with ThreadPoolExecutor(max_workers=S3_WRITE_WORKERS) as executor:
        while True:
            query = build_query(
                provider_name, eligible_dp_names, collection, search_after
            )
            response = post_es(query)
            response.raise_for_status()
            page = response.json()
            check_es_response(page)
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

                dpla_ids.append(dpla_id)
                subject_queries.update(collect_subject_queries(source))

                print(dpla_id)

            search_after = hits[-1]["sort"]

    if failed[0]:
        print(f"Error: {failed[0]} dpla-map.json writes failed", file=sys.stderr)
        raise SystemExit(1)

    # Phase 2 — batched Wikidata reconciliation for NARA exactMatch subjects.
    # Best-effort: chunks that fail are logged and skipped (their items' P921
    # entries are simply omitted; the string-form P4272 still lands).
    print(
        f"Reconciling {len(subject_queries)} unique subject queries...",
        file=sys.stderr,
    )
    subjects_lookup = reconcile_subjects(subject_queries)
    print(
        f"Resolved {len(subjects_lookup)} subjects to Wikidata IDs.",
        file=sys.stderr,
    )

    # Phase 3 — build per-item sdc.json (ready-to-POST claim list) and stage
    # to S3 alongside dpla-map.json. Each item's source doc is re-read from
    # the dpla-map.json we just staged in phase 1 (rather than buffered in
    # memory) so peak memory stays O(unique-subjects) rather than scaling
    # with hub size. Items the SDC builder can't parse (e.g. provider /
    # institution missing from institutions_v2.json) are skipped silently;
    # their dpla-map.json is still in place for downloader/uploader and a
    # future re-run will pick them up.
    sdc_sem, sdc_failed, _on_sdc_done = make_s3_stage_context(S3_WRITE_WORKERS)
    sdc_skipped = 0

    with ThreadPoolExecutor(max_workers=S3_WRITE_WORKERS) as executor:
        for dpla_id in dpla_ids:
            raw = s3_client.get_item_metadata(partner, dpla_id)
            if raw is None:
                # Phase 1 already exited on any S3 write failure, so a miss
                # here is unexpected — log and continue rather than abort.
                logging.warning(
                    "dpla-map.json missing for %s in phase 3; skipping sdc.json",
                    dpla_id,
                )
                sdc_skipped += 1
                continue
            try:
                source = json.loads(raw)
            except json.JSONDecodeError as e:
                logging.warning(
                    "dpla-map.json for %s failed to parse in phase 3 (%s); "
                    "skipping sdc.json",
                    dpla_id,
                    e,
                )
                sdc_skipped += 1
                continue
            try:
                sdc_payload = build_claims_for_doc(
                    source,
                    dpla_id,
                    institutions_json,
                    rights,
                    subject_ids,
                    subjects_lookup,
                    retrieval_date,
                )
            except ET.ParseError as e:
                # parse_nara_access_level surfaces malformed NARA
                # originalRecord XML here instead of silently writing a
                # partial sdc.json missing P7228/P6224. One bad item
                # must not abort staging for the whole NARA hub.
                logging.warning(
                    "build_claims_for_doc for %s raised ET.ParseError (%s); "
                    "skipping sdc.json for this item",
                    dpla_id,
                    e,
                )
                sdc_skipped += 1
                continue
            if sdc_payload is None:
                sdc_skipped += 1
                continue
            sdc_sem.acquire()
            future = executor.submit(
                stage_sdc_to_s3, s3_client, partner, dpla_id, sdc_payload
            )
            future.add_done_callback(_on_sdc_done(dpla_id))

    if sdc_skipped:
        print(
            f"Skipped sdc.json for {sdc_skipped} items "
            "(unparseable provider/institution).",
            file=sys.stderr,
        )
    if sdc_failed[0]:
        print(f"Error: {sdc_failed[0]} sdc.json writes failed", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
