"""SDC (Structured Data on Commons) claim construction for DPLA items.

Given a DPLA item's ES `_source` document (the same shape the api.dp.la
/v2/items endpoint returns inside `docs[0]`), `build_claims_for_doc()`
produces the ready-to-POST Wikibase claim list — the exact `claims["claims"]`
array `sdc-sync` writes via `wbsetclaims`. This is what `get-ids-es` stages
to S3 as `sdc.json` so the sync phase becomes a pure diff+POST step.

All claim-building primitives in this module are pure: no Commons API
calls, no `check()`-style queries against existing state. Subject
reconciliation against Wikidata happens via the explicit
`reconcile_subjects()` helper, which is called once per hub (batched
across all NARA `exactMatch` subjects) by `get-ids-es` so the per-item
build step receives a fully-resolved lookup table.

Module structure:

  Constants
    Q-IDs for hardcoded entities and NARA-only access/level maps.

  Pure helpers
    normalize_rights_uri()         — canonicalize a DPLA rights URI
    _item_value(qid)                — wikibase-entityid datavalue
    formattedclaim(...)             — claim shape with P459 qualifier and
                                      P854/P123/P813 reference

  Per-item parsing
    parse_dpla_doc(doc, dpla_id, hubs, subject_ids, subjects_lookup)
        Returns the 13-element tuple of normalized intermediate values
        consumed by `build_claims_for_doc`. `subjects_lookup` is a
        pre-resolved `{(name, naid): qid}` map produced by
        `reconcile_subjects` — when None the caller can still call
        `collect_subject_queries` + `reconcile_subjects` itself to fill it.

  Reconciliation
    collect_subject_queries(doc)    — yield (name, naid) pairs to resolve
    reconcile_subjects(queries)     — batched call to wikidata.reconci.link
                                      returning {(name, naid): qid}

  Top-level builder
    build_claims_for_doc(doc, dpla_id, hubs, rights, subject_ids,
                        subjects_lookup, retrieval_date)
        Returns `{"claims": [...]}` ready to POST to wbsetclaims.
"""

from __future__ import annotations

import datetime
import json
import logging
from collections.abc import Iterable
from typing import Any

import requests
from bs4 import BeautifulSoup

# Hardcoded Wikibase entities used across the SDC mapping. Centralized here
# so any change has a single edit site.
Q_HEURISTIC = "Q61848113"
Q_DPLA = "Q2944483"
Q_SOURCE_CATALOG = "Q74228490"
Q_PUBLIC_DOMAIN = "Q19652"
Q_COPYRIGHTED = "Q50423863"
Q_CC0_PD_SOMEWHERE = "Q88088423"
Q_PD_MARK_RAW = "Q6938433"
Q_SMITHSONIAN = "Q518155"
Q_PUBLISHER = "Q393351"
Q_AGGREGATOR = "Q108296843"
Q_CONTRIBUTING_INSTITUTION = "Q108296919"
Q_NARA_ITEM = "Q11723795"
Q_NARA_FILE_UNIT = "Q59221146"

# NARA-only mappings.
NARA_ACCESS_CODES = {
    "10031403": "Q66739888",
    "10031402": "Q24238356",
    "10031399": "Q66739729",
    "10031400": "Q66739849",
    "10031401": "Q66739875",
}
NARA_LEVELS = {
    "item": Q_NARA_ITEM,
    "itemAv": Q_NARA_ITEM,
    "fileUnit": Q_NARA_FILE_UNIT,
}
NARA_PROVIDER_NAME = "National Archives and Records Administration"
NARA_CATALOG_PREFIX = "https://catalog.archives.gov/id/"

PD_MARK_URI_CANONICAL = "http://creativecommons.org/publicdomain/mark/1.0"

RECONCI_ENDPOINT = "https://wikidata.reconci.link/en/api"
RECONCI_BATCH_SIZE = 10
TEXT_VALUE_LIMIT = 1499  # matches sdc-sync's longstanding truncation cap
# Maximum length for a raw P217 (inventory number) value before we skip it
# entirely. sdc-sync's original guard was `len(local_id) >= 1501`, so anything
# strictly longer than 1500 is dropped.
LOCAL_ID_MAX_LENGTH = 1500

logger = logging.getLogger(__name__)


def normalize_rights_uri(uri: str) -> str:
    """Canonicalize a DPLA rights URI for lookup against rights.json.

    DPLA emits rights URIs in a few minor variants (http vs https, with
    or without a trailing slash). Keying on a single canonical form
    means we don't silently miss licenses just because of scheme/slash
    drift on either side.
    """
    if not uri:
        return uri
    return uri.replace("https://", "http://").rstrip("/")


def _item_value(qid: str) -> dict:
    """Build a wikibase-entityid datavalue payload for a Q-ID."""
    return {"entity-type": "item", "numeric-id": int(qid.replace("Q", ""))}


def _qualifier_item_snak(prop: str, qid: str) -> dict:
    """Build a single qualifier snak for an item-typed value."""
    return {
        "snaktype": "value",
        "property": prop,
        "datavalue": {"value": _item_value(qid), "type": "wikibase-entityid"},
    }


def _qualifier_string_snak(prop: str, value: str, datatype: str | None = None) -> dict:
    """Build a single qualifier snak for a string-typed value."""
    snak = {
        "snaktype": "value",
        "property": prop,
        "datavalue": {"value": value, "type": "string"},
    }
    if datatype is not None:
        snak["datatype"] = datatype
    return snak


def formattedclaim(
    prop: str,
    value: Any,
    value_type: str,
    dpla_id: str,
    retrieval_date: datetime.date | None = None,
) -> dict:
    """Build the canonical Wikibase claim envelope.

    Every DPLA-published claim carries:
      * a P459 (determination method) qualifier set to Q61848113 (heuristic)
      * a reference triple — P854 (DPLA item URL), P123 (publisher = DPLA),
        P813 (retrieved on `retrieval_date`).

    When called with `value == "somevalue"` the mainsnak is emitted as a
    snaktype=somevalue node instead of a value node — the caller adds the
    type-specific qualifier (P2093 for creators, P1932 for dates).

    `retrieval_date` defaults to today's date when omitted; callers that
    want a deterministic, persisted-to-S3 claim list (get-ids-es) should
    pass the run date explicitly so the same `sdc.json` content is
    reproducible from the same input doc.
    """
    if retrieval_date is None:
        retrieval_date = datetime.date.today()

    claim = {
        "mainsnak": {
            "snaktype": "value",
            "property": prop,
            "datavalue": {"value": value, "type": value_type},
        },
        "type": "statement",
        "rank": "normal",
        "qualifiers": {
            "P459": [
                {
                    "snaktype": "value",
                    "property": "P459",
                    "datavalue": {
                        "value": _item_value(Q_HEURISTIC),
                        "type": "wikibase-entityid",
                    },
                    "datatype": "wikibase-item",
                }
            ]
        },
        "references": [
            {
                "snaks": {
                    "P854": [
                        {
                            "snaktype": "value",
                            "property": "P854",
                            "datavalue": {
                                "value": f"https://dp.la/item/{dpla_id}",
                                "type": "string",
                            },
                        }
                    ],
                    "P123": [
                        {
                            "snaktype": "value",
                            "property": "P123",
                            "datavalue": {
                                "value": _item_value(Q_DPLA),
                                "type": "wikibase-entityid",
                            },
                        }
                    ],
                    "P813": [
                        {
                            "snaktype": "value",
                            "property": "P813",
                            "datavalue": {
                                "value": {
                                    "time": "+"
                                    + retrieval_date.isoformat()
                                    + "T00:00:00Z",
                                    "timezone": 0,
                                    "before": 0,
                                    "after": 0,
                                    "precision": 11,
                                    "calendarmodel": "http://www.wikidata.org/entity/Q1985727",
                                },
                                "type": "time",
                            },
                        }
                    ],
                }
            }
        ],
    }

    if value == "somevalue":
        claim["mainsnak"].pop("datavalue")
        claim["mainsnak"]["snaktype"] = "somevalue"

    return claim


def collect_subject_queries(doc: dict) -> list[tuple[str, str]]:
    """Return (name, naid) pairs for every NARA `exactMatch` subject in `doc`.

    A subject's NAID is harvested from its first `exactMatch` URL (stripping
    the NARA catalog prefix). Subjects without an `exactMatch` are
    irrelevant here — they map via the local `subjects.json` lookup in
    `parse_dpla_doc`, no HTTP needed.
    """
    queries: list[tuple[str, str]] = []
    for subject in doc.get("sourceResource", {}).get("subject", []) or []:
        if not isinstance(subject, dict):
            continue
        exact_match = subject.get("exactMatch")
        if not exact_match:
            continue
        first = exact_match[0] if isinstance(exact_match, list) else exact_match
        if not isinstance(first, str):
            continue
        naid = first.replace(NARA_CATALOG_PREFIX, "")
        name = str(subject.get("name") or "")
        queries.append((name, naid))
    return queries


def reconcile_subjects(
    queries: Iterable[tuple[str, str]],
    *,
    batch_size: int = RECONCI_BATCH_SIZE,
    timeout: int = 15,
) -> dict[tuple[str, str], str]:
    """Resolve (name, naid) subject pairs to Wikidata Q-IDs in batches.

    Uses wikidata.reconci.link's bulk endpoint with up to `batch_size`
    queries per HTTP call. Failures are best-effort: a chunk that errors
    out is logged and skipped so a single transient blip doesn't kill the
    whole hub's pre-compute pass — the items whose subjects couldn't be
    resolved still get their string-form P4272 statement; only the
    parallel P921 (depicts) for that subject is omitted.

    Returns `{(name, naid): qid}` for matched subjects. Subjects that
    weren't matched (no result, blank result id) are omitted.
    """
    deduped: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for pair in queries:
        if pair not in seen:
            seen.add(pair)
            deduped.append(pair)

    resolved: dict[tuple[str, str], str] = {}
    for start in range(0, len(deduped), batch_size):
        chunk = deduped[start : start + batch_size]
        payload = {
            f"q{i}": {
                "query": name,
                "limit": 5,
                "properties": [{"pid": "P1225", "v": naid}],
                "type_strict": "should",
            }
            for i, (name, naid) in enumerate(chunk)
        }
        try:
            response = requests.get(
                RECONCI_ENDPOINT,
                params={"queries": json.dumps(payload)},
                timeout=timeout,
            )
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError) as e:
            logger.warning(
                "Subject reconciliation chunk failed (%d queries): %s; "
                "subjects in this chunk will have no P921 entry.",
                len(chunk),
                e,
            )
            continue
        for i, pair in enumerate(chunk):
            result = data.get(f"q{i}", {}).get("result") or []
            if not result:
                continue
            qid = result[0].get("id")
            if qid:
                resolved[pair] = qid
    return resolved


def _resolve_subjects(
    doc: dict,
    subject_ids: dict,
    subjects_lookup: dict[tuple[str, str], str] | None,
) -> list[tuple[str, str]]:
    """Build the `[(name, qid_or_empty), ...]` list `parse_dpla_doc` returns.

    Three resolution sources, in order:
      1. The local `subjects.json` map (`subject_ids`) keyed by subject name.
      2. The pre-resolved `subjects_lookup` from `reconcile_subjects` for
         NARA `exactMatch` subjects.
      3. Fallback to `(name, "")` — string-form P4272 will still be emitted,
         but no parallel P921 entry.
    """
    subjects: list = []
    for subject in doc.get("sourceResource", {}).get("subject", []) or []:
        if not isinstance(subject, dict):
            continue
        name = subject.get("name")
        added = False
        if name in subject_ids:
            for subjqid in subject_ids[name]["id"]:
                if not any(subjqid in pair for pair in subjects):
                    subjects.append((str(name), subjqid))
                    added = True
                if not any(name in pair for pair in subjects):
                    subjects.append((str(name or ""), ""))
                    added = True
            # Fall through to the `if not added` fallback below so a
            # subject_ids entry with an empty "id" list still yields a
            # string-form P4272 statement — matches sdc-sync's existing
            # behavior before this code moved into a shared module.
        elif subject.get("exactMatch"):
            exact_match = subject["exactMatch"]
            first = exact_match[0] if isinstance(exact_match, list) else exact_match
            naid = ""
            if isinstance(first, str):
                naid = first.replace(NARA_CATALOG_PREFIX, "")
            subj_name = str(name or "")
            qid = ""
            if subjects_lookup is not None:
                qid = subjects_lookup.get((subj_name, naid), "")
            subjects.append((subj_name, qid))
            added = True
        if not added:
            subjects.append((str(name or ""), ""))
    return [tuple(pair) for pair in subjects]


def parse_dpla_doc(
    doc: dict,
    dpla_id: str,
    hubs: dict,
    subject_ids: dict,
    subjects_lookup: dict[tuple[str, str], str] | None = None,
) -> tuple | None:
    """Extract the 13-element tuple of normalized values from a DPLA doc.

    Tuple order (preserved for `sdc-sync.process_one()` compatibility):
        (url, descs, dates, titles, hub, local_ids, institution, rs,
         creators, subjects, naids, access, level)

    `subjects_lookup` is the pre-resolved `{(name, naid): qid}` map
    produced by `reconcile_subjects`. When omitted, NARA-exactMatch
    subjects are left unresolved (the parallel P921 entry won't be
    emitted for them); callers that want inline reconciliation should
    call `collect_subject_queries` + `reconcile_subjects` first and pass
    the result.
    """
    try:
        hub = hubs[doc["provider"]["name"]]["Wikidata"]
        institution = hubs[doc["provider"]["name"]]["institutions"][
            doc["dataProvider"]["name"]
        ]["Wikidata"]
    except (KeyError, TypeError):
        return None

    titles = doc["sourceResource"]["title"]
    if isinstance(titles, str):
        titles = [titles]
    rs = doc["rights"]
    url = doc["isShownAt"]

    try:
        dates = [
            displaydate["displayDate"] for displaydate in doc["sourceResource"]["date"]
        ]
    except Exception:
        dates = []

    try:
        local_ids = doc["sourceResource"]["identifier"]
        if isinstance(local_ids, str):
            local_ids = [local_ids]
    except Exception:
        local_ids = []

    try:
        descs = doc["sourceResource"]["description"]
        if isinstance(descs, str):
            descs = [descs]
    except Exception:
        descs = []

    try:
        subjects = _resolve_subjects(doc, subject_ids, subjects_lookup)
    except Exception:
        subjects = []

    try:
        creators = doc["sourceResource"]["creator"]
        if isinstance(creators, str):
            creators = [creators]
    except Exception:
        creators = []

    if doc["provider"]["name"] == NARA_PROVIDER_NAME:
        naids = doc["sourceResource"]["identifier"]
        if isinstance(naids, str):
            naids = [naids]
        access = ""
        level = ""
        try:
            xml = BeautifulSoup(doc["originalRecord"]["stringValue"], "xml")
        except Exception as e:
            logger.warning("Skipping NARA XML parse for %s: %s", dpla_id, e)
            xml = None
        if xml is not None:
            try:
                access_naid = str(
                    xml.find("accessRestriction").find("status").find("naId").text
                )
                access = NARA_ACCESS_CODES.get(access_naid, "")
            except Exception:
                access = ""
            for lvl_key in NARA_LEVELS:
                if xml.find(lvl_key):
                    level = NARA_LEVELS[lvl_key]
        local_ids = []
    else:
        naids = []
        access = ""
        level = ""

    return (
        url,
        descs,
        dates,
        titles,
        hub,
        local_ids,
        institution,
        rs,
        creators,
        subjects,
        naids,
        access,
        level,
    )


def _truncate(value: str) -> str:
    """Match sdc-sync's longstanding `[:1499].rstrip()` normalization."""
    return value[:TEXT_VALUE_LIMIT].rstrip() if value else ""


def _build_rights_claims(
    rs: str,
    rights: dict,
    dpla_id: str,
    retrieval_date: datetime.date,
) -> list[dict]:
    """Build the P275/P6426/P6216 claim cluster for an item's rights URI.

    Mirrors `sdc-sync.add_rs()` minus the Commons-check + appending side
    effects. Order of precedence:
      * If the rights URI maps via rights.json, emit the mapped P275 or
        P6426 claim and a parallel P6216 (status) derived from it.
      * Public Domain Mark gets a P6216=Q19652 status directly.
    """
    out: list[dict] = []
    rs_key = normalize_rights_uri(rs)
    rights_entry = rights.get(rs_key)
    status_prop: str | None = None
    status_qid: str | None = None

    if rights_entry:
        prop = next(iter(rights_entry))
        qid = rights_entry[prop]
        out.append(
            formattedclaim(
                prop, _item_value(qid), "wikibase-entityid", dpla_id, retrieval_date
            )
        )
        # Derive a P6216 (copyright status) companion claim.
        if prop == "P275" and qid != Q_PD_MARK_RAW:
            status_prop, status_qid = "P6216", Q_COPYRIGHTED
        elif prop == "P6426":
            status_prop, status_qid = "P6216", Q_PUBLIC_DOMAIN
        if qid == Q_PD_MARK_RAW:
            status_prop, status_qid = "P6216", Q_CC0_PD_SOMEWHERE

    if rs_key == PD_MARK_URI_CANONICAL:
        status_prop, status_qid = "P6216", Q_PUBLIC_DOMAIN

    if status_prop is not None and status_qid is not None:
        out.append(
            formattedclaim(
                status_prop,
                _item_value(status_qid),
                "wikibase-entityid",
                dpla_id,
                retrieval_date,
            )
        )
    return out


def _build_contributed_claims(
    hub: str,
    institution: str,
    dpla_id: str,
    retrieval_date: datetime.date,
) -> list[dict]:
    """Build the three-statement P9126 chain (DPLA + hub + institution).

    Each statement gets a P3831 (object of statement) qualifier identifying
    the role: publisher for DPLA, aggregator for the hub, contributing
    institution for the institution. The Smithsonian-hub case promotes
    the hub to fill both hub and institution slots.
    """
    out: list[dict] = []

    def _with_role(prop_qid: str, role_qid: str) -> dict:
        claim = formattedclaim(
            "P9126",
            _item_value(prop_qid),
            "wikibase-entityid",
            dpla_id,
            retrieval_date,
        )
        claim["qualifiers"]["P3831"] = [_qualifier_item_snak("P3831", role_qid)]
        return claim

    # The role qualifiers below intentionally MATCH what sdc-sync.add_contributed
    # has been writing to Commons. Smithsonian's path distinguishes
    # aggregator/contributing-institution; the non-Smithsonian path uses
    # publisher/aggregator for hub/institution respectively. Changing these
    # values without a coordinated Commons-side rewrite would make every
    # existing P9126 statement look "unexpected" to dpla_claims() and trigger
    # a remove+re-add on every re-sync.
    out.append(_with_role(Q_DPLA, Q_PUBLISHER))
    if hub == Q_SMITHSONIAN:
        out.append(_with_role(Q_SMITHSONIAN, Q_AGGREGATOR))
        out.append(_with_role(institution, Q_CONTRIBUTING_INSTITUTION))
    else:
        out.append(_with_role(hub, Q_PUBLISHER))
        out.append(_with_role(institution, Q_AGGREGATOR))
    return out


def _build_source_claim(
    hub: str,
    url: str,
    dpla_id: str,
    retrieval_date: datetime.date,
) -> dict:
    """Build the P7482 (described at) claim with P973 + P137 qualifiers."""
    claim = formattedclaim(
        "P7482",
        _item_value(Q_SOURCE_CATALOG),
        "wikibase-entityid",
        dpla_id,
        retrieval_date,
    )
    claim["qualifiers"]["P973"] = [_qualifier_string_snak("P973", url, datatype="url")]
    claim["qualifiers"]["P137"] = [_qualifier_item_snak("P137", hub)]
    return claim


def _build_local_id_claim(
    local_id: str,
    institution: str,
    dpla_id: str,
    retrieval_date: datetime.date,
) -> dict:
    """Build a P217 (inventory number) claim with P195 (collection) qualifier."""
    claim = formattedclaim("P217", local_id, "string", dpla_id, retrieval_date)
    claim["qualifiers"]["P195"] = [_qualifier_item_snak("P195", institution)]
    return claim


def _build_creator_claim(
    creator: str,
    dpla_id: str,
    retrieval_date: datetime.date,
) -> dict | None:
    normalized = _truncate(creator)
    if not normalized:
        return None
    claim = formattedclaim(
        "P170", "somevalue", "wikibase-entityid", dpla_id, retrieval_date
    )
    claim["qualifiers"]["P2093"] = [_qualifier_string_snak("P2093", normalized)]
    return claim


def _build_date_claim(
    date: str,
    dpla_id: str,
    retrieval_date: datetime.date,
) -> dict | None:
    normalized = _truncate(date)
    if not normalized:
        return None
    claim = formattedclaim("P571", "somevalue", "time", dpla_id, retrieval_date)
    claim["qualifiers"]["P1932"] = [_qualifier_string_snak("P1932", normalized)]
    return claim


def _build_monolingual_claim(
    prop: str,
    text: str,
    dpla_id: str,
    retrieval_date: datetime.date,
) -> dict | None:
    normalized = _truncate(text)
    if not normalized:
        return None
    return formattedclaim(
        prop,
        {"text": normalized, "language": "en"},
        "monolingualtext",
        dpla_id,
        retrieval_date,
    )


def build_claims_for_doc(
    doc: dict,
    dpla_id: str,
    hubs: dict,
    rights: dict,
    subject_ids: dict,
    subjects_lookup: dict[tuple[str, str], str] | None,
    retrieval_date: datetime.date,
) -> dict[str, list[dict]] | None:
    """Build the complete ready-to-POST SDC claim list for a DPLA item.

    Returns `{"claims": [...]}` matching the wbsetclaims POST shape. Returns
    `None` when the doc can't be parsed (e.g. provider/dataProvider not in
    the hubs map) — callers should skip the item and not stage an sdc.json
    for it.

    `subjects_lookup` is the pre-resolved reconciliation table; pass `None`
    for inline-only behavior (the parallel P921 statements for NARA
    `exactMatch` subjects are simply omitted in that case).

    `retrieval_date` is stamped into every claim's P813 reference. For
    deterministic, persisted-to-S3 output (get-ids-es), pass the run date.
    """
    parsed = parse_dpla_doc(doc, dpla_id, hubs, subject_ids, subjects_lookup)
    if parsed is None:
        return None
    (
        url,
        descs,
        dates,
        titles,
        hub,
        local_ids,
        institution,
        rs,
        creators,
        subjects,
        naids,
        access,
        level,
    ) = parsed

    claims: list[dict] = []

    # Rights cluster (P275/P6426/P6216).
    claims.extend(_build_rights_claims(rs, rights, dpla_id, retrieval_date))

    # P760 — DPLA ID.
    claims.append(formattedclaim("P760", dpla_id, "string", dpla_id, retrieval_date))

    # P1476 — title (one statement per title).
    for title in titles:
        c = _build_monolingual_claim("P1476", title, dpla_id, retrieval_date)
        if c is not None:
            claims.append(c)

    # P195 — collection (one statement; Smithsonian uses hub-as-institution).
    if institution:
        coll_qid = Q_SMITHSONIAN if hub == Q_SMITHSONIAN else institution
        claims.append(
            formattedclaim(
                "P195",
                _item_value(coll_qid),
                "wikibase-entityid",
                dpla_id,
                retrieval_date,
            )
        )

    # P170 — creator (one statement per creator, somevalue + P2093).
    for creator in creators:
        c = _build_creator_claim(creator, dpla_id, retrieval_date)
        if c is not None:
            claims.append(c)

    # P571 — date (one statement per date, somevalue + P1932).
    for date in dates:
        c = _build_date_claim(date, dpla_id, retrieval_date)
        if c is not None:
            claims.append(c)

    # P4272 (subject string) and P921 (subject entity).
    for name, subjqid in subjects:
        if name:
            claims.append(
                formattedclaim("P4272", name, "string", dpla_id, retrieval_date)
            )
        if subjqid:
            claims.append(
                formattedclaim(
                    "P921",
                    _item_value(subjqid),
                    "wikibase-entityid",
                    dpla_id,
                    retrieval_date,
                )
            )

    # P10358 — description.
    for desc in descs:
        c = _build_monolingual_claim("P10358", desc, dpla_id, retrieval_date)
        if c is not None:
            claims.append(c)

    # P9126 — maintained by chain.
    claims.extend(_build_contributed_claims(hub, institution, dpla_id, retrieval_date))

    # P7482 — described at source catalog.
    claims.append(_build_source_claim(hub, url, dpla_id, retrieval_date))

    # P217 — local identifier (non-NARA; per-value, with P195 qualifier).
    for local_id in local_ids:
        if not local_id or len(local_id) > LOCAL_ID_MAX_LENGTH:
            continue
        claims.append(
            _build_local_id_claim(local_id, institution, dpla_id, retrieval_date)
        )

    # NARA-only fields.
    for naid in naids:
        if naid:
            claims.append(
                formattedclaim("P1225", naid, "string", dpla_id, retrieval_date)
            )
    if access:
        claims.append(
            formattedclaim(
                "P7228",
                _item_value(access),
                "wikibase-entityid",
                dpla_id,
                retrieval_date,
            )
        )
    if level:
        claims.append(
            formattedclaim(
                "P6224",
                _item_value(level),
                "wikibase-entityid",
                dpla_id,
                retrieval_date,
            )
        )

    return {"claims": claims}
