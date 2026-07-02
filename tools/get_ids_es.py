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

import json
import logging
import sys
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor

import click

from ingest_wikimedia.banlist import Banlist
from ingest_wikimedia.common import get_dict, get_list
from ingest_wikimedia.dpla import (
    DC_TITLE_FIELD_NAME,
    DPLA,
    SOURCE_RESOURCE_FIELD_NAME,
)
from ingest_wikimedia.partners import PARTNER_HUBS
from ingest_wikimedia.es import check_es_response, post_es
from ingest_wikimedia.iiif import IIIF
from ingest_wikimedia.s3 import S3Client
from ingest_wikimedia.sdc import (
    build_claims_for_doc,
    collect_subject_queries,
    fetch_institutions_v2,
    fetch_subjects_json,
    load_rights_json,
    reconcile_subjects,
)
from ingest_wikimedia.slack import notify_phase_start
from ingest_wikimedia.staging import (
    make_s3_stage_context,
    stage_item_to_s3,
    stage_sdc_to_s3,
)
from ingest_wikimedia.wikimedia import get_page_title

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

# Characters stripped from the start of a title before case-folded sort.
# Without this, ASCII-sort puts every quote-wrapped title (`"YOU BET I'M ..."`)
# and every paren-wrapped title (`(Title Index ...)`) ahead of every alphabetic
# title — which buries the "this is alphabetical" intuition the sort is meant
# to provide. Strip leading punctuation so the first letter dominates ordering.
_TITLE_SORT_STRIP = " \"'(<[-_."


def _title_sort_key(source: dict, dpla_id: str) -> str:
    """Return a sort key approximating Commons file-name alphabetical order.

    Built from the same ``get_page_title`` the uploader uses, so the sort
    order matches the actual Commons title prefix exactly (including all
    its character normalisations: ``:`` → ``-``, ``[`` → ``(``, etc.).

    Multi-ordinal items stay grouped because all their ordinals share the
    same title prefix and only the ``(page N)`` suffix differs — and the
    uploader iterates ordinals 1..N in numeric order within each item.

    Items with no ``sourceResource.title`` cluster at the prefix Commons
    would actually generate for them (which is just the DPLA-ID-only
    suffix), keeping their position predictable rather than scattering.
    Ties on the title prefix fall back to ``dpla_id`` for stable order.
    """
    # Same title selection the uploader uses (titles[0] of
    # sourceResource.title), so the sort prefix matches the actual
    # Commons file name exactly.
    titles = get_list(get_dict(source, SOURCE_RESOURCE_FIELD_NAME), DC_TITLE_FIELD_NAME)
    title = titles[0] if titles else ""
    # ``get_list`` only validates the OUTER container — non-string list
    # elements (e.g. ``["title", 42]``) still pass through. Coerce to "" so
    # ``get_page_title``'s slice (``item_title[:181]``) doesn't crash on
    # malformed records.
    if not isinstance(title, str):
        title = ""
    full = get_page_title(title, dpla_id, "", page=None)
    # Strip the ' - DPLA - <id>' suffix so the human-readable prefix dominates.
    prefix = full.rsplit(" - DPLA - ", 1)[0]
    return prefix.casefold().lstrip(_TITLE_SORT_STRIP) + "\x00" + dpla_id


def load_eligible_dp_names(
    institutions: dict, partner: str, maintain: bool = False
) -> list[str]:
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

    ``maintain`` (maintain mode) drops ONLY the ``upload`` requirement: every
    QID-bearing institution under the hub is included regardless of its upload
    flag. This is what lets the pipeline regenerate sdc.json / dpla-map.json for
    institutions no longer authorized for *new* uploads, so their already-on-
    Commons files can still be maintained in place. The Wikidata-ID requirement
    is kept (it gates Commons categorisation and the P195 institution claim);
    new-upload prevention is enforced downstream by the uploader's no-create
    fence, never by this worklist.
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
        upload_ok = maintain or hub_upload or inst_upload
        if hub_wikidata and wikidata_id and upload_ok:
            eligible.append(inst_name)

    return eligible


def build_query(
    provider_name: str,
    eligible_dp_names: list[str],
    collection: str | None = None,
    search_after: list | None = None,
    skip_media_filter: bool = False,
) -> dict:
    """Build the Elasticsearch boolean query for a single page.

    The ``provider`` + ``dataProvider`` filters always apply: they scope the
    scan to this hub and to institutions that resolve to a Wikidata QID
    (``eligible_dp_names`` comes from :func:`load_eligible_dp_names`, which
    requires a QID). A dataProvider without a QID is correctly excluded — it
    can't get a P195 institution claim or a Commons category.

    ``skip_media_filter`` drops the two *upload-readiness* item filters —
    ``rightsCategory == "Unlimited Re-Use"`` and the asset-presence check
    (``mediaMaster`` / ``iiifManifest`` / IIIF-derivable ``isShownAt``). Those
    gate whether an item's media can be FETCHED. It is set only by **lite
    maintain** (``sdc-sync --cat``), which downloads nothing and operates on the
    files already on Commons — so applying them would wrongly skip
    already-uploaded items whose current index doc no longer carries fetchable
    media or a free rights category (e.g. DLG items that no longer populate
    ``mediaMaster``). The DEFAULT (hash) maintain path DOWNLOADS media, so it
    keeps these filters (``skip_media_filter=False``) — only downloadable,
    free-rights items can be hash-reconciled. Distinct from ``--maintain``,
    which relaxes the *institution* upload gate (see ``load_eligible_dp_names``)
    and applies to BOTH maintain routes.
    """
    filters: list[dict] = [
        {"term": {"provider.name.not_analyzed": provider_name}},
        {"terms": {"dataProvider.name.not_analyzed": eligible_dp_names}},
    ]

    if not skip_media_filter:
        asset_should = [
            {"exists": {"field": "mediaMaster"}},
            {"exists": {"field": "iiifManifest"}},
            *[
                {"wildcard": {"isShownAt": pattern}}
                for pattern in IIIF_DERIVABLE_ISSHOWNAT_PATTERNS
            ],
        ]
        filters.append({"term": {"rightsCategory": "Unlimited Re-Use"}})
        filters.append({"bool": {"should": asset_should, "minimum_should_match": 1}})

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
        "Restrict to items in a specific collection title. Combine with"
        " --institution to scope the collection to specific institution(s);"
        " omit --institution to match the collection across every"
        " upload-eligible institution in the hub (some collections span"
        " multiple institutions)."
    ),
)
@click.option(
    "--single-id",
    "single_id",
    default=None,
    help=(
        "Re-stage dpla-map.json + sdc.json for exactly ONE DPLA ID, then"
        " print that ID to stdout. Skips the hub-level eligibility scan;"
        " the operator is presumed to have already verified eligibility"
        " (e.g. via the launch script's ``resolve-dpla-ids`` pre-check)."
        " Mutually exclusive with ``--institution`` and ``--collection``."
        " Used by the launch script's single-item path so a"
        " ``/wikimedia-upload <dpla-id>`` run picks up the latest mapping"
        " code in ``build_claims_for_doc`` — without this, the sdc-sync"
        " step diffs against whatever sdc.json the partner's last full"
        " run produced, silently missing any subsequent mapping changes."
        " Eligibility is a SNAPSHOT taken at ``resolve-dpla-ids`` time;"
        " if institutions_v2.json drifts (e.g. ``upload`` flag flipped"
        " off) between submit and Phase 3, ``--single-id`` does NOT"
        " re-check — the operator's submission is treated as authoritative."
    ),
)
@click.option(
    "--maintain",
    is_flag=True,
    help=(
        "Maintain mode: include institutions regardless of their upload flag"
        " (QID still required), so IDs + sdc.json are generated for already-"
        "uploaded files of institutions no longer authorized for new uploads."
        " New-upload prevention is enforced by the uploader's --no-create"
        " fence, not by this worklist. Relaxes only the INSTITUTION gate; item"
        " media/rights filters still apply unless --skip-media-filter is given."
    ),
)
@click.option(
    "--skip-media-filter",
    is_flag=True,
    help=(
        "Drop the per-item upload-readiness filters (rightsCategory +"
        " mediaMaster/iiifManifest/IIIF-derivable isShownAt). Used only by LITE"
        " maintain (sdc-sync --cat), which downloads nothing and maintains the"
        " files already on Commons regardless of current media/rights. The"
        " default (hash) maintain path downloads media, so it keeps these"
        " filters (omit this flag)."
    ),
)
def main(
    partner: str,
    institutions: tuple[str, ...],
    collection: str | None,
    single_id: str | None,
    maintain: bool,
    skip_media_filter: bool,
) -> None:
    """Print wiki-eligible DPLA IDs for PARTNER to stdout, one per line.

    Also stages each item's full metadata to S3 (dpla-map.json) so the
    downloader can skip DPLA API calls entirely.

    Multiple ``--institution`` flags are ORed together in the
    Elasticsearch ``dataProvider`` filter, so the output covers items
    belonging to any of the listed institutions in one combined run.
    No ``--institution`` flags means "all eligible institutions for
    this hub" (the existing hub-level behaviour).

    ``--single-id`` switches to a per-ID branch: skip the hub-wide ES
    scan, fetch the one document by ID, run the same Phase 1 → 2 → 3
    pipeline on a list of one, and emit the single ID. Used by the
    launch script's ``/wikimedia-upload <dpla-id>`` path so single-item
    runs pick up the latest mapping code.
    """
    if single_id is not None and (institutions or collection):
        print(
            "--single-id cannot be combined with --institution or --collection"
            " (single-id mode targets exactly one document by ID).",
            file=sys.stderr,
        )
        sys.exit(1)

    if collection is not None:
        collection = collection.strip()
        if not collection:
            print("--collection cannot be empty.", file=sys.stderr)
            sys.exit(1)
        # No institution-count requirement: a collection can be scoped to one
        # or more --institution flags, or left hub-wide (no --institution), in
        # which case it is matched across every upload-eligible institution in
        # the hub. Some DPLA collections span multiple institutions.

    try:
        # Maintain mode operates on hubs whose upload flag is off
        # (the whole point — reconcile already-on-Commons files for
        # de-opted institutions), so the eligibility precheck must
        # honor ``--maintain``. Mirrors the same flag honoring
        # ``load_eligible_dp_names`` already does for the
        # institution-level gate below.
        DPLA.check_partner(partner, maintain=maintain)
    except ValueError as e:
        raise click.BadParameter(str(e)) from e

    notify_phase_start(partner, "id-generation")
    provider_name = PARTNER_HUBS[partner]

    # Load institutions_v2.json once and reuse it for both eligibility
    # filtering and the SDC pre-compute pass.
    institutions_json = fetch_institutions_v2()
    eligible_dp_names = load_eligible_dp_names(
        institutions_json, partner, maintain=maintain
    )
    if not eligible_dp_names:
        print(
            f"No eligible institutions found for {partner} in institutions_v2.json",
            file=sys.stderr,
        )
        sys.exit(0)

    if institutions:
        ineligible = [name for name in institutions if name not in eligible_dp_names]
        if ineligible:
            # In maintain mode the gate is QID presence, not the upload flag.
            reason = "lack a Wikidata ID" if maintain else "not upload-eligible"
            print(
                f"Institution(s) {reason} for {partner}: {ineligible}.",
                file=sys.stderr,
            )
            sys.exit(1)
        eligible_dp_names = list(institutions)

    # SDC pre-compute inputs.
    rights = load_rights_json()
    subject_ids = fetch_subjects_json()

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
    # Parallel list to dpla_ids: sort key per ID, used only to order the
    # final IDs CSV by Commons-title alphabetical order. Populated during
    # phase 1 while ``source`` is still in scope; we'd otherwise have to
    # re-read every dpla-map.json from S3 just to recover the title.
    sort_keys: list[str] = []
    subject_queries: set[tuple[str, str]] = set()

    # Single-ID mode: pre-check the banlist BEFORE the ES round-trip so the
    # operator gets a distinct ``banlisted`` error rather than the generic
    # ``no document from ES`` message — the two failures need different
    # remediation (banlist edit vs. ES indexing investigation).
    if single_id is not None and banlist.is_banned(single_id):
        print(
            f"Error: --single-id {single_id} is on the banlist; remove it"
            " from dpla-id-banlist.txt to proceed.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    with ThreadPoolExecutor(max_workers=S3_WRITE_WORKERS) as executor:
        while True:
            if single_id is not None:
                # Single-ID branch: bypass the hub-eligibility filter entirely
                # and look the document up by its DPLA ID. The operator already
                # vouched for eligibility upstream (resolve-dpla-ids runs
                # before the tmux session is even launched), so applying the
                # rightsCategory / institution-upload-flag gates here would
                # only cause confusing failures when those gates drift.
                # ``size: 2`` (not 1) so the defensive ``len(hits) != 1``
                # check below is actually REACHABLE. With size=1 ES caps
                # the response and the >1 defense never fires, which
                # defeats its purpose against stale-replica duplicates.
                query = {"query": {"term": {"id": single_id}}, "size": 2}
            else:
                query = build_query(
                    provider_name,
                    eligible_dp_names,
                    collection,
                    search_after,
                    skip_media_filter=skip_media_filter,
                )
            response = post_es(query)
            response.raise_for_status()
            page = response.json()
            check_es_response(page)
            hits = page["hits"]["hits"]

            if not hits:
                break

            if single_id is not None:
                # Defense-in-depth against ES returning more than one hit
                # (shouldn't with a properly unique ``id`` field but stale-
                # replica or reindex-cutover scenarios have surfaced
                # duplicates in the past — see the ``size: 2`` request
                # above) and against ID normalization on the ES side (e.g. if
                # the field analyser ever changed). Bail loudly rather than
                # silently staging the wrong document — single-id mode skips
                # the hub-eligibility filter, so we have no other guardrail.
                if len(hits) != 1:
                    print(
                        f"Error: --single-id {single_id} returned"
                        f" {len(hits)} hits from ES; expected exactly 1.",
                        file=sys.stderr,
                    )
                    raise SystemExit(1)
                returned_id = hits[0]["_source"].get("id")
                if returned_id != single_id:
                    print(
                        f"Error: --single-id {single_id} returned a document"
                        f" with id={returned_id!r}; refusing to stage under a"
                        " mismatched key.",
                        file=sys.stderr,
                    )
                    raise SystemExit(1)

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
                sort_keys.append(_title_sort_key(source, dpla_id))
                subject_queries.update(collect_subject_queries(source))

            if single_id is not None:
                # No pagination — single-id mode is a one-shot lookup.
                break
            search_after = hits[-1]["sort"]

    if single_id is not None and not dpla_ids:
        # ES had no document for this ID. (The banlist case is short-
        # circuited above with its own distinct error, and the per-hit
        # ID-mismatch / >1-hit cases also raise with their own messages,
        # so reaching this branch means ES genuinely returned an empty
        # hits array.) Non-zero exit so the launch script's tmux chain
        # short-circuits before downloader/uploader/sdc-sync run against
        # an empty CSV.
        print(
            f"Error: --single-id {single_id} returned no document from ES.",
            file=sys.stderr,
        )
        raise SystemExit(1)

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
                )
            except (ET.ParseError, ValueError) as e:
                # ET.ParseError: parse_nara_access_level on malformed
                # NARA originalRecord XML. ValueError:
                # ingest_date_from_doc on missing / unparseable
                # ingestDate. Both are per-item data-integrity signals,
                # not conditions to abort the whole hub for — skip the
                # sdc.json for this item and keep going.
                logging.warning(
                    "build_claims_for_doc for %s raised %s (%s); "
                    "skipping sdc.json for this item",
                    dpla_id,
                    type(e).__name__,
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
            f"Skipped sdc.json for {sdc_skipped} items"
            " (missing/malformed dpla-map.json, unmappable source, or"
            " missing/invalid ingestDate).",
            file=sys.stderr,
        )
    if sdc_failed[0]:
        print(f"Error: {sdc_failed[0]} sdc.json writes failed", file=sys.stderr)
        raise SystemExit(1)

    # Emit the IDs CSV sorted by Commons title prefix so downloader,
    # uploader, and sdc-sync all process items in human-readable alphabetic
    # order. Sorting is done at the very end (after staging completes) so a
    # mid-run crash never produces a partial-but-misleading CSV; the launch
    # script chains phases with ``&&`` so the CSV is only consumed when this
    # tool exits cleanly anyway.
    for _, dpla_id in sorted(zip(sort_keys, dpla_ids)):
        print(dpla_id)


if __name__ == "__main__":
    main()
