"""
Query Elasticsearch for wiki-eligible NARA DPLA IDs, applying NARA-specific
priority filters to produce a manageable subset of the 18M+ NARA items.

Three query strategies, run in order:
  1. Language   — all non-English language values, batched LANGUAGES_PER_QUERY at a time
  2. Format     — all formats with < FORMAT_COUNT_LIMIT items, batched FORMATS_PER_QUERY at a time
  3. Collection — all collections with < COLLECTION_COUNT_LIMIT items (after exclusions)

The count limits are intentional: they ensure small, high-priority collections are
fully processed before the enormous ones, since a complete run of all 18M+ NARA
items is not feasible. The parameters are defined as module-level constants so they
can be updated as priorities change.

For each eligible item, the full ES source document is written to S3 as dpla-map.json
so the downloader can skip DPLA API calls entirely.

Output: one DPLA ID per line to stdout. Redirect to produce the IDs CSV:
    get-ids-nara > nara/nara.csv
"""

import json
import logging
import sys
import threading
from concurrent.futures import Future, ThreadPoolExecutor

import click
import requests

from ingest_wikimedia.banlist import Banlist
from ingest_wikimedia.s3 import S3Client
from ingest_wikimedia.slack import notify_phase_start

ES_URL = "http://search-prod1.internal.dp.la:9200/dpla_alias/_search"
PAGE_SIZE = 500
S3_WRITE_WORKERS = 4
# Max S3 writes queued + in-flight at once — prevents the executor queue from
# growing unboundedly and holding thousands of ES source documents in memory.
_S3_QUEUE_DEPTH = S3_WRITE_WORKERS * 4
PARTNER = "nara"
NARA_PROVIDER = "National Archives and Records Administration"

# --- Tunable priority thresholds ---
# Collections or formats exceeding these counts are deferred to future runs,
# ensuring smaller, higher-priority sets are exhausted first.
FORMAT_COUNT_LIMIT = 12_000
COLLECTION_COUNT_LIMIT = 50_000
LANGUAGES_PER_QUERY = 10
FORMATS_PER_QUERY = 6
COLLECTIONS_PER_QUERY = 50

# Collections containing any of these substrings are excluded entirely.
COLLECTION_SUBSTRING_EXCLUSIONS: tuple[str, ...] = (
    "Personnel",
    "Military Files",
    "Correspondence Files",
)

# Collections matching any of these exact titles are excluded entirely.
COLLECTION_EXACT_EXCLUSIONS: frozenset[str] = frozenset(
    {
        "Naval Records Collection of the Office of Naval Records and Library",
        "War Department Collection of Confederate Records",
        "War Department Collection of Revolutionary War Records",
    }
)

# Items with any collection title starting with "Records of" belong to a major
# agency record group and are excluded from the collection strategy entirely.
# This matches the original DPLA API NOT "Records of*" filter behaviour.
_RECORDS_OF_EXCLUSION: dict = {
    "bool": {
        "must_not": [
            {"prefix": {"sourceResource.collection.title.not_analyzed": "Records of"}}
        ]
    }
}


def _fetch_buckets(field: str, extra_filters: list[dict] | None = None) -> list[dict]:
    """Return all ES aggregation buckets for `field` under NARA with Unlimited Re-Use.

    Uses composite aggregation to safely paginate through all values regardless of
    cardinality — avoids the 503s that a large fixed-size terms aggregation causes
    on high-cardinality fields like sourceResource.collection.title.not_analyzed.

    Each returned bucket has the shape {"key": <field_value>, "doc_count": N}.
    """
    buckets: list[dict] = []
    after_key: dict | None = None

    while True:
        composite_agg: dict = {
            "size": 1000,
            "sources": [{"key": {"terms": {"field": field}}}],
        }
        if after_key is not None:
            composite_agg["after"] = after_key

        query = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"provider.name.not_analyzed": NARA_PROVIDER}},
                        {"term": {"rightsCategory": "Unlimited Re-Use"}},
                        *(extra_filters or []),
                    ]
                }
            },
            "aggs": {"values": {"composite": composite_agg}},
        }
        response = requests.post(
            ES_URL,
            json=query,
            headers={"Content-Type": "application/json"},
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        _check_es_response(data)

        page = data["aggregations"]["values"]
        # Composite agg buckets: {"key": {"key": <value>}, "doc_count": N}
        # Normalize to {"key": <value>, "doc_count": N} to match the callers' expectations.
        for b in page["buckets"]:
            buckets.append({"key": b["key"]["key"], "doc_count": b["doc_count"]})

        after_key = page.get("after_key")
        if not after_key:
            break

    return buckets


def _check_es_response(data: dict) -> None:
    if data.get("timed_out"):
        raise RuntimeError("Elasticsearch query timed out — results may be incomplete")
    shards = data.get("_shards", {})
    if shards.get("failed", 0) > 0:
        raise RuntimeError(
            f"Elasticsearch query had {shards['failed']} shard failure(s)"
        )


def _paginate(extra_filter: dict):
    """Yield all ES hits for NARA items with Unlimited Re-Use and mediaMaster, filtered by extra_filter."""
    search_after = None
    while True:
        query: dict = {
            "size": PAGE_SIZE,
            "sort": ["id", "_doc"],
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"provider.name.not_analyzed": NARA_PROVIDER}},
                        {"term": {"rightsCategory": "Unlimited Re-Use"}},
                        {"exists": {"field": "mediaMaster"}},
                        extra_filter,
                    ]
                }
            },
        }
        if search_after is not None:
            query["search_after"] = search_after
        response = requests.post(
            ES_URL,
            json=query,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        _check_es_response(data)
        hits = data["hits"]["hits"]
        if not hits:
            break
        yield from hits
        search_after = hits[-1]["sort"]


def build_language_queries() -> list[list[str]]:
    """Return batches of non-English language values for NARA."""
    buckets = _fetch_buckets("sourceResource.language.name")
    langs = [b["key"] for b in buckets if b["key"] != "English"]
    return [
        langs[i : i + LANGUAGES_PER_QUERY]
        for i in range(0, len(langs), LANGUAGES_PER_QUERY)
    ]


def build_format_queries() -> list[list[str]]:
    """Return batches of format values with doc_count < FORMAT_COUNT_LIMIT."""
    buckets = _fetch_buckets("sourceResource.format")
    formats = [b["key"] for b in buckets if b["doc_count"] < FORMAT_COUNT_LIMIT]
    return [
        formats[i : i + FORMATS_PER_QUERY]
        for i in range(0, len(formats), FORMATS_PER_QUERY)
    ]


def build_collection_queries() -> list[str]:
    """Return collection titles that pass all exclusion filters and size threshold.

    Only considers items with no "Records of..." collection title — matching the
    original NOT "Records of*" filter from the DPLA API version of this script.
    """
    buckets = _fetch_buckets(
        "sourceResource.collection.title.not_analyzed",
        extra_filters=[_RECORDS_OF_EXCLUSION],
    )
    titles = []
    for b in buckets:
        title = b["key"]
        if b["doc_count"] >= COLLECTION_COUNT_LIMIT:
            continue
        if any(excl in title for excl in COLLECTION_SUBSTRING_EXCLUSIONS):
            continue
        if title in COLLECTION_EXACT_EXCLUSIONS:
            continue
        titles.append(title)
    return titles


def _stage_to_s3(s3_client: S3Client, dpla_id: str, source: dict) -> None:
    s3_client.write_item_metadata(PARTNER, dpla_id, json.dumps(source))


@click.command()
def main() -> None:
    """Print wiki-eligible NARA DPLA IDs to stdout, one per line.

    Also stages each item's full metadata to S3 (dpla-map.json) so the
    downloader can skip DPLA API calls entirely.
    """
    notify_phase_start(PARTNER, "id-generation")

    banlist = Banlist()
    s3_client = S3Client()
    seen_ids: set[str] = set()

    print("Fetching language query batches...", file=sys.stderr)
    lang_batches = build_language_queries()
    print(f"  {len(lang_batches)} language batches", file=sys.stderr)

    print("Fetching format query batches...", file=sys.stderr)
    format_batches = build_format_queries()
    print(f"  {len(format_batches)} format batches", file=sys.stderr)

    print("Fetching eligible collections...", file=sys.stderr)
    collection_titles = build_collection_queries()
    print(f"  {len(collection_titles)} collections", file=sys.stderr)

    queries: list[tuple[str, dict]] = []
    for batch in lang_batches:
        queries.append(
            (
                f"languages: {batch[0]}...",
                {"terms": {"sourceResource.language.name": batch}},
            )
        )
    for batch in format_batches:
        queries.append(
            (
                f"formats: {batch[0]}...",
                {"terms": {"sourceResource.format": batch}},
            )
        )
    for i in range(0, len(collection_titles), COLLECTIONS_PER_QUERY):
        batch = collection_titles[i : i + COLLECTIONS_PER_QUERY]
        queries.append(
            (
                f"collections: {batch[0][:50]}{'...' if len(batch[0]) > 50 else ''}",
                {
                    "bool": {
                        "filter": [
                            {
                                "terms": {
                                    "sourceResource.collection.title.not_analyzed": batch
                                }
                            }
                        ],
                        "must_not": [
                            {
                                "prefix": {
                                    "sourceResource.collection.title.not_analyzed": "Records of"
                                }
                            }
                        ],
                    }
                },
            )
        )

    s3_sem = threading.BoundedSemaphore(_S3_QUEUE_DEPTH)
    failed = [0]

    def _on_s3_done(dpla_id: str):
        def callback(future: Future) -> None:
            s3_sem.release()
            exc = future.exception()
            if exc:
                failed[0] += 1
                logging.warning(f"S3 write failed for {dpla_id}: {exc}")

        return callback

    with ThreadPoolExecutor(max_workers=S3_WRITE_WORKERS) as executor:
        for label, extra_filter in queries:
            print(f"Querying {label}", file=sys.stderr)
            for hit in _paginate(extra_filter):
                source = hit["_source"]
                dpla_id = source["id"]

                if dpla_id in seen_ids or banlist.is_banned(dpla_id):
                    continue

                seen_ids.add(dpla_id)
                source["_staged_by_get_ids_es"] = True

                s3_sem.acquire()
                future = executor.submit(_stage_to_s3, s3_client, dpla_id, source)
                future.add_done_callback(_on_s3_done(dpla_id))

                print(dpla_id)

    if failed[0]:
        print(f"Warning: {failed[0]} S3 writes failed", file=sys.stderr)


if __name__ == "__main__":
    main()
