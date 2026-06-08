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
import os
import re
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from typing import Any

import requests

from ingest_wikimedia.dpla import INSTITUTIONS_URL

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

# https://www.wikidata.org/wiki/Q5727902 — the canonical value for the
# ``sourcing circumstances`` (P1480) qualifier when a date is approximate
# (circa / c. / ca. / ~ / brackets / trailing ?). Per
# https://www.wikidata.org/wiki/Help:Dates#Inexact_dates, this is the
# qualifier convention for inexact dates — distinct from year/decade
# precision, which conveys "we know the year but not the day" rather
# than "the year itself is approximate".
Q_CIRCA = "Q5727902"

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


def parse_nara_access_level(string_value: str) -> tuple[str, str]:
    """Extract ``(access_qid, level_qid)`` from a NARA item's
    ``originalRecord["stringValue"]`` XML payload.

    Returns ``""`` for either field when it isn't present in the
    record — these are legitimate empty cases (the NARA item just
    doesn't carry that specific descriptor).

    Raises :class:`xml.etree.ElementTree.ParseError` on malformed
    XML. Callers MUST let this propagate to the per-item error
    boundary rather than catching-and-defaulting: returning empty
    strings for a *parse* failure (vs. a missing-field) lets the
    uploader write a sdc.json without P7228/P6224, and the next
    sdc-sync reconciler run would then strip those claims off
    Commons files where they were correctly written by a prior
    healthy run.

    The previous implementation used ``BeautifulSoup(..., "xml")``,
    which silently degraded to "no access / no level" whenever
    lxml wasn't installed on the host — exactly the silent-failure
    pattern this signature deliberately rules out. Stdlib's
    ElementTree is always available, removing the dependency.

    Namespace-tolerant via XPath ``{*}`` wildcards: NARA's
    ``xmlns="http://description.das.nara.gov/"`` is matched without
    requiring a specific prefix or URI.
    """
    root = ET.fromstring(string_value)

    access = ""
    naid_elem = root.find(".//{*}accessRestriction/{*}status/{*}naId")
    if naid_elem is not None and naid_elem.text:
        access = NARA_ACCESS_CODES.get(naid_elem.text.strip(), "")

    # Level comes from the root element's local name — NARA records use
    # <item>, <itemAv>, or <fileUnit> as the root and never nest them.
    # The prior BeautifulSoup code iterated NARA_LEVELS with "last match
    # wins", which had a latent bug: an <item> root with a stray
    # descendant <fileUnit> got classified as fileUnit. Pin to the
    # root tag instead — descendant matches are not consulted.
    root_local = root.tag.rsplit("}", 1)[-1] if "}" in root.tag else root.tag
    level = NARA_LEVELS.get(root_local, "")

    return access, level


PD_MARK_URI_CANONICAL = "http://creativecommons.org/publicdomain/mark/1.0"

RECONCI_ENDPOINT = "https://wikidata.reconci.link/en/api"
RECONCI_BATCH_SIZE = 10
TEXT_VALUE_LIMIT = 1499  # matches sdc-sync's longstanding truncation cap
# Maximum length for a raw P217 (inventory number) value before we skip it
# entirely. sdc-sync's original guard was `len(local_id) >= 1501`, so anything
# strictly longer than 1500 is dropped.
LOCAL_ID_MAX_LENGTH = 1500

# DPLA subject → Wikidata Q-ID lookup table; sourced from dpla/ingestion3
# alongside institutions_v2.json. Fetched fresh per run so upstream changes
# land in the next sync without a redeploy.
SUBJECTS_URL = (
    "https://raw.githubusercontent.com/dpla/ingestion3/develop/"
    "src/main/resources/subjects.json"
)

# Locate rights.json by walking up from this module's directory. sdc.py
# lives at <repo>/ingest_wikimedia/sdc.py, so two dirname's above gives the
# repo root.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger = logging.getLogger(__name__)


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
            # Use .get("id", []) so a subjects.json entry missing the
            # expected "id" array doesn't crash the whole sync — degrades
            # to "no P921 entry for this subject", same as no Q-ID match.
            for subjqid in subject_ids[name].get("id", []):
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

    # The shape-tolerant blocks below treat missing or mis-typed fields as
    # "absent" — same contract as the original sdc-sync. Narrowed from a bare
    # except to (KeyError, IndexError, TypeError) so a real bug (e.g.
    # AttributeError introduced by an upstream refactor) propagates instead
    # of being silently swallowed. Each fallback is logged so silent fallback
    # is still observable in the worker logs.

    try:
        dates = [
            displaydate["displayDate"] for displaydate in doc["sourceResource"]["date"]
        ]
    except (KeyError, IndexError, TypeError) as e:
        logger.debug("No usable dates for %s: %s", dpla_id, e)
        dates = []

    try:
        local_ids = doc["sourceResource"]["identifier"]
        if isinstance(local_ids, str):
            local_ids = [local_ids]
    except (KeyError, TypeError) as e:
        logger.debug("No usable identifiers for %s: %s", dpla_id, e)
        local_ids = []

    try:
        descs = doc["sourceResource"]["description"]
        if isinstance(descs, str):
            descs = [descs]
    except (KeyError, TypeError) as e:
        logger.debug("No usable description for %s: %s", dpla_id, e)
        descs = []

    try:
        subjects = _resolve_subjects(doc, subject_ids, subjects_lookup)
    except (KeyError, IndexError, TypeError) as e:
        logger.debug("Subject resolution failed for %s: %s", dpla_id, e)
        subjects = []

    try:
        creators = doc["sourceResource"]["creator"]
        if isinstance(creators, str):
            creators = [creators]
    except (KeyError, TypeError) as e:
        logger.debug("No usable creators for %s: %s", dpla_id, e)
        creators = []

    if doc["provider"]["name"] == NARA_PROVIDER_NAME:
        naids = doc["sourceResource"]["identifier"]
        if isinstance(naids, str):
            naids = [naids]
        # Malformed NARA XML raises ET.ParseError here — intentionally
        # propagated to the per-item boundary (writing a partial sdc.json
        # lets the reconciler later strip valid P7228/P6224 claims).
        access, level = parse_nara_access_level(doc["originalRecord"]["stringValue"])
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


_GREGORIAN_CALENDAR = "http://www.wikidata.org/entity/Q1985727"

# Wikibase time-datavalue precision codes used here:
#   8  = decade  (e.g. "1940s"        → +1940-01-01)
#   9  = year   (e.g. "1945"          → +1945-01-01)
#   10 = month  (e.g. "1945-06"       → +1945-06-01)
#   11 = day    (e.g. "1945-06-07"    → +1945-06-07)
_PRECISION_DECADE = 8
_PRECISION_YEAR = 9
_PRECISION_MONTH = 10
_PRECISION_DAY = 11

# DPLA decorators stripped before pattern-matching. Repeat iteratively so
# nested forms like "[1945?]" collapse cleanly.
_DATE_PREFIX = re.compile(
    r"^\s*(?:circa|c\.|ca\.|approximately|approx\.|~|\[)\s*", re.IGNORECASE
)
_DATE_SUFFIX = re.compile(r"\s*(?:\]|\?)\s*$")

_ISO_DATE = re.compile(r"^(\d{1,4})-(\d{1,2})-(\d{1,2})$")
_YEAR_MONTH = re.compile(r"^(\d{1,4})-(\d{1,2})$")
_DECADE = re.compile(r"^(\d{1,4})s$")
_YEAR_ONLY = re.compile(r"^(\d{1,4})$")


def _wikibase_time(time_str: str, precision: int) -> dict:
    """Construct the canonical ``value`` payload for a Wikibase time
    datavalue. ``timezone``, ``before``, ``after``, and ``calendarmodel``
    are pinned to constants — DPLA only emits proleptic Gregorian dates
    with no explicit uncertainty bounds, and downstream consumers
    (notably the reconciler's comparable key) treat any drift in those
    fields as "different claim, rewrite it" — which would churn millions
    of statements on every re-sync.
    """
    return {
        "time": time_str,
        "precision": precision,
        "before": 0,
        "after": 0,
        "timezone": 0,
        "calendarmodel": _GREGORIAN_CALENDAR,
    }


def _strip_date_decorators(s: str) -> tuple[str, bool]:
    """Iteratively strip leading ``circa``/``c.``/``ca.``/
    ``approximately``/``~``/``[`` and trailing ``]``/``?`` so nested
    patterns (``[1945?]``, ``circa [1945]``) collapse cleanly.

    Returns the stripped string AND a flag recording whether any
    decorator was actually removed — the flag drives the P1480
    qualifier the builder stamps to mark the date as approximate."""
    prev = None
    stripped = False
    while prev != s:
        prev = s
        new = _DATE_PREFIX.sub("", s)
        new = _DATE_SUFFIX.sub("", new)
        if new != s:
            stripped = True
        s = new
    return s.strip(), stripped


def parse_dpla_date(date_string: str) -> dict | None:
    """Parse a DPLA display-date string into a structured representation,
    or ``None`` when the input is too messy to commit to a Wikibase
    time value.

    On success returns a dict::

        {
            "value": <Wikibase time-datavalue value dict>,
            "approximate": bool,
        }

    ``approximate`` is True when the source string carried a
    ``circa``/``c.``/``ca.``/``approximately``/``~``/``[…]``/``?``
    decorator. The caller stamps a ``P1480 = Q5727902`` (sourcing
    circumstances = circa) qualifier on the claim in that case, per
    https://www.wikidata.org/wiki/Help:Dates#Inexact_dates — distinct
    from year/decade precision (which conveys "we know the year but
    not the day", a different shape of uncertainty than "the year
    itself is approximate").

    Recognised single-date shapes (after decorator stripping):

      * ``YYYY-MM-DD``   → precision 11 (day)
      * ``YYYY-MM``      → precision 10 (month)
      * ``YYYYs``        → precision 8 (decade), accepted only when the
                            year is decade-aligned (e.g. ``1940s``, not
                            ``1945s``)
      * ``YYYY``         → precision 9 (year)

    Returns None for ranges (``1945-1950``), BC dates (``500 BC``), free
    prose (``During the Gilded Age``), era markers (``1945 AD``),
    parenthesised forms (``(1945)``), month names (``January 1945``),
    "before"/"after"/"between" prefixes, or any other shape that
    leaves unrecognised text in the residue after decorator stripping
    — the builder falls back to ``somevalue + P1932 stated-as`` for
    those, so the original DPLA string is preserved verbatim
    regardless of parse outcome.

    Conservative-fallback contract: every regex above is anchored
    (``^…$``), so ANY non-date text adjacent to a recognised pattern
    forces a fallback. Decorator-stripping only removes the specific
    markers we know how to encode as a P1480 qualifier; anything else
    survives and breaks the regex match. There is no path where the
    parser silently drops meaningful text and produces a false
    precision.

    Year 0 returns None: proleptic Gregorian has no year 0, and
    ``datetime.date(0, …)`` would crash the calendar-arithmetic
    validation below.

    The ``value`` payload must round-trip through
    ``tools.sdc_sync._time_comparable`` to produce the canonical key the
    reconciler matches Commons statements against. If the
    Wikibase-time-datavalue shape changes here, mirror the change there.
    """
    if not date_string:
        return None
    s, approximate = _strip_date_decorators(date_string.strip())
    if not s:
        return None

    def _ok(value: dict) -> dict:
        return {"value": value, "approximate": approximate}

    m = _ISO_DATE.match(s)
    if m:
        y, mo, d = int(m[1]), int(m[2]), int(m[3])
        if y == 0:
            return None
        try:
            datetime.date(y, mo, d)
        except ValueError:
            pass
        else:
            return _ok(
                _wikibase_time(f"+{y:04d}-{mo:02d}-{d:02d}T00:00:00Z", _PRECISION_DAY)
            )

    m = _YEAR_MONTH.match(s)
    if m:
        y, mo = int(m[1]), int(m[2])
        if y != 0 and 1 <= mo <= 12:
            return _ok(
                _wikibase_time(f"+{y:04d}-{mo:02d}-01T00:00:00Z", _PRECISION_MONTH)
            )

    m = _DECADE.match(s)
    if m:
        y = int(m[1])
        # Accept only decade-aligned years so "1945s" (almost certainly a
        # typo for "1945") doesn't quietly become decade-precision.
        if y != 0 and y % 10 == 0:
            return _ok(_wikibase_time(f"+{y:04d}-01-01T00:00:00Z", _PRECISION_DECADE))

    m = _YEAR_ONLY.match(s)
    if m:
        y = int(m[1])
        if y != 0:
            return _ok(_wikibase_time(f"+{y:04d}-01-01T00:00:00Z", _PRECISION_YEAR))

    return None


def _build_date_claim(
    date: str,
    dpla_id: str,
    retrieval_date: datetime.date,
) -> dict | None:
    """Build a P571 (inception) claim from a DPLA display-date string.

    When the string parses to a structured time value, emit a value-typed
    P571 with a ``time`` datavalue at appropriate precision. When it
    doesn't, fall back to ``somevalue`` with the raw string in a P1932
    (stated as) qualifier — preserves today's behavior for any date the
    parser can't commit to.

    The P1932 qualifier is ALWAYS stamped, even on the value-typed
    branch, so the original DPLA-supplied display string is recoverable
    by readers (and so the template module can keep rendering whatever
    text DPLA chose, rather than re-formatting from the structured
    value).

    When the source carried a ``circa``/``c.``/``ca.``/brackets/``?``
    decorator (parser ``approximate`` flag), the claim also gets a
    ``P1480 = Q5727902`` (sourcing circumstances = circa) qualifier,
    per https://www.wikidata.org/wiki/Help:Dates#Inexact_dates. The
    structured year-precision time + P1480 qualifier together carry the
    "approximate" semantic into Wikidata-shaped queries; the P1932
    qualifier still preserves the verbatim source string for display.

    Idempotency note: the reconciler treats the OLD somevalue claim and
    the NEW value-typed claim as different (their comparable keys
    differ — the somevalue's P1932 string vs. the value-typed time
    canonical key), so a re-sync after this change cleanly migrates the
    Commons statement from somevalue to value-typed in one cycle
    (old removed, new added).
    """
    normalized = _truncate(date)
    if not normalized:
        return None
    parsed = parse_dpla_date(normalized)
    if parsed is not None:
        claim = formattedclaim("P571", parsed["value"], "time", dpla_id, retrieval_date)
        if parsed["approximate"]:
            claim["qualifiers"]["P1480"] = [_qualifier_item_snak("P1480", Q_CIRCA)]
    else:
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
