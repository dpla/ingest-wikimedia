"""Resolve DPLA item IDs: check upload eligibility and stage metadata to S3.

Used by wikimedia_launch.py to handle single-item upload targets.  All IDs are
resolved in a single Elasticsearch request.  For each ID, applies the same
upload-eligibility criteria as get-ids-es (rights, media presence, hub known,
institution has Wikidata ID and upload=True per institutions_v2.json), stages
the full item metadata to S3 as dpla-map.json, and writes one status line to
stdout for the launch script to parse.

Output format (one line per ID):
  {id} HUB={canonical}       item is eligible; metadata has been staged to S3
  {id} NOT_FOUND              no document found in the ES index for this ID
  {id} INELIGIBLE:{reason}   item exists but fails an eligibility check
  {id} ERROR:{message}        unexpected error during processing
"""

import json

import click

from ingest_wikimedia.banlist import Banlist
from ingest_wikimedia.es import check_es_response, post_es
from ingest_wikimedia.iiif import IIIF
from ingest_wikimedia.partners import check_item_eligibility, resolve_slug
from ingest_wikimedia.s3 import S3Client

_IIIF_MANIFEST_FIELD = "iiifManifest"
_MEDIA_MASTER_FIELD = "mediaMaster"
_IS_SHOWN_AT_FIELD = "isShownAt"


@click.command()
@click.argument("dpla_ids", nargs=-1, required=True)
@click.option(
    "--maintain",
    is_flag=True,
    help=(
        "Maintain mode: include institutions regardless of their upload flag"
        " (Wikidata ID still required), so IDs + sdc.json are staged for"
        " already-uploaded files of institutions no longer authorized for"
        " new uploads. New-upload prevention is enforced by the uploader's"
        " ``--no-create`` fence, not by this eligibility check. Mirrors the"
        " same flag on get-ids-es."
    ),
)
def main(dpla_ids: tuple[str, ...], maintain: bool) -> None:
    """Resolve DPLA_IDS, check eligibility, and stage metadata to S3."""
    banlist = Banlist()
    s3_client = S3Client()

    # Single batched ES query for all IDs — avoids N round-trips.
    resp = post_es(
        {
            "query": {"terms": {"id": list(dpla_ids)}},
            "size": len(dpla_ids),
        }
    )
    resp.raise_for_status()
    data = resp.json()
    check_es_response(data)
    hits = data.get("hits", {}).get("hits", [])
    found: dict[str, dict] = {hit["_source"]["id"]: hit["_source"] for hit in hits}

    for dpla_id in dpla_ids:
        try:
            source = found.get(dpla_id)
            if source is None:
                print(f"{dpla_id} NOT_FOUND", flush=True)
                continue
            _process_one(dpla_id, source, banlist, s3_client, maintain=maintain)
        except Exception as e:
            print(f"{dpla_id} ERROR:{e}", flush=True)


def _process_one(
    dpla_id: str,
    source: dict,
    banlist: Banlist,
    s3_client: S3Client,
    *,
    maintain: bool = False,
) -> None:
    if banlist.is_banned(dpla_id):
        print(f"{dpla_id} INELIGIBLE:on banlist", flush=True)
        return

    rights = source.get("rightsCategory", "")
    if rights != "Unlimited Re-Use":
        print(f"{dpla_id} INELIGIBLE:rights={rights!r}", flush=True)
        return

    has_media = bool(
        source.get(_MEDIA_MASTER_FIELD)
        or source.get(_IIIF_MANIFEST_FIELD)
        or IIIF.contentdm_iiif_url(source.get(_IS_SHOWN_AT_FIELD, ""))
    )
    if not has_media:
        print(f"{dpla_id} INELIGIBLE:no media", flush=True)
        return

    provider_name = (source.get("provider") or {}).get("name", "")
    canonical = resolve_slug(provider_name)
    if not canonical:
        print(f"{dpla_id} INELIGIBLE:unknown hub {provider_name!r}", flush=True)
        return

    # Check institution-level eligibility per institutions_v2.json. Both
    # profiles (upload / maintain) require the two Wikidata IDs; maintain
    # relaxes the upload-flag requirement. ``check_item_eligibility``
    # returns a specific reason so callers (and the operator) can tell
    # which gate blocked the item — the historical conflated "missing
    # Wikidata ID or upload flag" message hid that distinction and
    # steered operators wrong on maintain-eligible items.
    dp_name = (source.get("dataProvider") or {}).get("name", "")
    eligible, reason = check_item_eligibility(canonical, dp_name, maintain=maintain)
    if not eligible:
        print(f"{dpla_id} INELIGIBLE:{reason}", flush=True)
        return

    # Derive CONTENTdm IIIF manifest URL if needed (mirrors get-ids-es behaviour).
    if not source.get(_IIIF_MANIFEST_FIELD) and not source.get(_MEDIA_MASTER_FIELD):
        iiif_url = IIIF.contentdm_iiif_url(source.get(_IS_SHOWN_AT_FIELD, ""))
        if iiif_url:
            source[_IIIF_MANIFEST_FIELD] = iiif_url

    # Set the same staging flag as get-ids-es so the downloader recognises the
    # S3 object as a fresh ES-sourced document and skips the legacy API fallback.
    source["_staged_by_get_ids_es"] = True
    s3_client.write_item_metadata(canonical, dpla_id, json.dumps(source))

    print(f"{dpla_id} HUB={canonical}", flush=True)


if __name__ == "__main__":
    main()
