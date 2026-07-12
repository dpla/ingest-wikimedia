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
                        subjects_lookup)
        Returns `{"claims": [...], "ingest_date": "YYYY-MM-DD"}` ready to
        POST to wbsetclaims. The ingest date is derived internally from
        ``doc["ingestDate"]`` (see the "P813" section below).

## P813 (retrieved on) reference date — pinned to the DPLA ingest date

Every DPLA-authored claim carries a ``P813`` (retrieved on) snak in
its reference block, indicating when the DPLA data was fetched. This
codebase pins that date to the **DPLA item's ``ingestDate``** — the
timestamp DPLA's ingestion pipeline recorded when it harvested this
item's metadata from the source hub — rather than to the sync run's
``datetime.date.today()``.

Rationale: ``ingestDate`` changes only when DPLA re-ingests the item
(a genuine new fact about the DPLA-side data), whereas ``today()``
changes with every sync run. Anchoring P813 to ``today()`` produced
noisy Commons edit diffs — every re-sync of a file rewrote every
P813 date across every one of the file's claims, drowning out the
substantive changes in the diff. Anchoring to ``ingestDate`` means a
sync that finds no factual delta produces no diff at all; a diff
that DOES appear reflects a real DPLA-side change.

See ``ingest_date_from_doc`` for extraction. Callers that build
claims (``get-ids-es``, ``get-ids-nara``, ``sdc-sync``'s reference
refresh) should derive ``retrieval_date`` per-item from the doc, not
from ``today()``.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import re
import unicodedata
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from typing import Any

import requests

from ingest_wikimedia.partners import load_institutions, load_subjects

# Hardcoded Wikibase entities used across the SDC mapping. Centralized here
# so any change has a single edit site.
Q_HEURISTIC = "Q61848113"
Q_DPLA = "Q2944483"
Q_SOURCE_CATALOG = "Q74228490"
Q_PUBLIC_DOMAIN = "Q19652"
Q_COPYRIGHTED = "Q50423863"
Q_CC0_PD_SOMEWHERE = "Q88088423"
Q_PD_MARK_RAW = "Q6938433"
# DPLA distinguishes two kinds of hub, which take different SDC partnership
# shapes (see ``_build_contributed_claims``):
#   * Content hub — a large institution that IS the data provider (NARA,
#     Smithsonian, ...). It is itself a repository and sits in P195; its
#     "data providers" are internal departments, not independent orgs.
#   * Service hub — an aggregating intermediary; its institutions are
#     distinct organizations that sit in P195.
# institutions_v2.json carries no hub-type flag, so content-hub membership
# is enumerated here.
Q_NARA = "Q518155"  # National Archives and Records Administration
Q_SMITHSONIAN = "Q131626"  # Smithsonian Institution
CONTENT_HUB_QIDS = frozenset({Q_NARA, Q_SMITHSONIAN})

# object-has-role (P3831) qualifier values, named for their actual Wikidata
# roles. A service hub's hub is an ``aggregator`` (like DPLA itself); its
# institution is a ``repository``. A content hub is itself a ``repository``;
# its contributing department is a ``custodial unit``.
Q_ROLE_AGGREGATOR = "Q393351"
Q_ROLE_REPOSITORY = "Q108296843"
Q_ROLE_CONTRIBUTING = "Q108296919"
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
TEXT_VALUE_LIMIT = 1499  # matches sdc-sync's longstanding truncation cap;
# retained for snak values inside qualifiers (P2093 in P170, P1932 in P571),
# which can't be chunked at the qualifier level. Mainsnak values are chunked
# instead via _chunk_value below.

# Wikibase's hard cap for `string` and `monolingualtext` mainsnak values on
# Wikidata-class wikis (Commons inherits via the MediaInfo extension).
# Reference: https://www.wikidata.org/wiki/Help:Data_type — both datatypes
# listed at 1500. Mainsnak values exceeding this get split into multiple
# claims, each carrying a P1545 (series ordinal) qualifier so the Lua
# template on Commons can reassemble them on read.
WIKIBASE_STRING_LIMIT = 1500

# P1545 (series ordinal) is added by _chunk_and_emit_claims as a qualifier
# on every chunked-claim and is therefore a DPLA-authored qualifier wherever
# chunking is enabled. tools/sdc_sync.py's _DPLA_EXTRA_QUALIFIER_PROPS must
# include P1545 under each chunked property so _is_safe_to_amend_in_place
# treats it as part of the DPLA-owned envelope.
CHUNKABLE_PROPS = frozenset(
    {
        "P760",  # DPLA ID (typically short, normalized but rarely chunks)
        "P217",  # local identifier
        "P4272",  # subject string
        "P1225",  # NARA NAID
        "P1476",  # title (monolingualtext)
        "P10358",  # description (monolingualtext)
    }
)

# Character class for ASCII / Latin-1 control characters. Mirrors Wikibase's
# server-side `preg_replace('/\\p{Cc}+/u', ' ', $value)` normalization for
# string and monolingualtext datatypes (lib/includes/StringNormalizer.php).
# We apply the same transform locally so chunk boundary char counts match
# what Wikibase will actually store after the wbeditentity round-trip.
_CONTROL_CHAR_RUN = re.compile(r"[\x00-\x1F\x7F-\x9F]+")

# Locate rights.json by walking up from this module's directory. sdc.py
# lives at <repo>/ingest_wikimedia/sdc.py, so two dirname's above gives the
# repo root.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

logger = logging.getLogger(__name__)


def fetch_institutions_v2() -> dict:
    """Full institutions_v2.json (hub/institution eligibility + Wikidata IDs).

    Delegates to ``partners.load_institutions`` so it reads the launch-staged
    local copy when present instead of re-fetching from raw.githubusercontent.com
    (per-IP HTTP 429 under a multi-target batch).
    """
    return load_institutions()


def fetch_subjects_json() -> dict:
    """DPLA-subject → Wikidata-QID map used to populate P921.

    Delegates to ``partners.load_subjects`` (local-first, see there) instead of
    re-fetching from raw.githubusercontent.com per run.
    """
    return load_subjects()


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


def ingest_date_from_doc(doc: dict) -> datetime.date:
    """Return the DPLA ingestion date from an item document (either a
    live API response or a staged ``dpla-map.json`` sidecar) as a
    ``datetime.date``.

    The DPLA API emits ``ingestDate`` as an ISO 8601 timestamp string
    (e.g. ``"2026-06-23T15:50:29.874Z"``); this helper strips the time
    portion and returns the date part.

    Raises ``ValueError`` if the field is missing, non-string, or not
    parseable — every record that survives DPLA ingestion carries an
    ``ingestDate``, so its absence signals a corrupted record / ES bug
    / upstream data-integrity problem. Callers should catch per-item
    (same convention as the existing ``ET.ParseError`` skip in
    ``get-ids-es`` / ``get-ids-nara``), log the DPLA ID, and skip that
    item's ``sdc.json`` rather than fall back to a synthetic date that
    would mask the real issue.

    Used as the anchor for the P813 (retrieved on) reference date; see
    the module docstring for the rationale.
    """
    raw = doc.get("ingestDate") if isinstance(doc, dict) else None
    if not isinstance(raw, str) or len(raw) < 10:
        raise ValueError(
            f"DPLA item document missing or has invalid ingestDate: {raw!r} "
            "(every record ingested via DPLA carries a valid ingestDate; "
            "an item without one signals a corrupted record — do not "
            "silently synthesize a date)"
        )
    return datetime.date.fromisoformat(raw[:10])


def formattedclaim(
    prop: str,
    value: Any,
    value_type: str,
    dpla_id: str,
    retrieval_date: datetime.date,
) -> dict:
    """Build the canonical Wikibase claim envelope.

    Every DPLA-published claim carries:
      * a P459 (determination method) qualifier set to Q61848113 (heuristic)
      * a reference triple — P854 (DPLA item URL), P123 (publisher = DPLA),
        P813 (retrieved on ``retrieval_date``).

    ``retrieval_date`` is required; callers derive it per-item from the
    doc's ``ingestDate`` via :func:`ingest_date_from_doc`. See the
    module docstring for why P813 is pinned to the ingest date rather
    than the sync run's today().

    When called with `value == "somevalue"` the mainsnak is emitted as a
    snaktype=somevalue node instead of a value node — the caller adds the
    type-specific qualifier (P2093 for creators, P1932 for dates).
    """

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
    # ``rights`` is always present on regular runs (``rightsCategory ==
    # "Unlimited Re-Use"`` is in the ES filter), but maintain mode runs
    # with ``--skip-media-filter``, which drops that filter so docs
    # without a ``rights`` field reach SDC pre-compute. Default to empty
    # string — ``normalize_rights_uri("")`` is a no-op and the
    # ``rights.get("")`` lookup in ``_build_rights_claims`` returns
    # None, so no rights claim is appended. Without this, the first
    # de-opted item in a maintain run raises ``KeyError`` and aborts
    # the whole id-generation pass.
    rs = doc.get("rights", "")
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
    """Match sdc-sync's longstanding `[:1499].rstrip()` normalization.

    Used only for snak values inside qualifiers (e.g. P2093 in a P170
    creator statement). Mainsnak string/monolingualtext values go through
    :func:`_normalize_string_value` + :func:`_chunk_value` instead so long
    values are preserved across multiple claims rather than truncated.
    """
    return value[:TEXT_VALUE_LIMIT].rstrip() if value else ""


def _normalize_string_value(text: str, *, is_monolingualtext: bool = False) -> str:
    """Apply Wikibase's server-side string normalization locally.

    Mirrors `lib/includes/StringNormalizer.php` (Wikibase) so our in-memory
    value matches the bytes Wikibase will store after wbeditentity. Without
    this, the next sync would see drift on values containing newlines,
    leading/trailing whitespace, or invisible format characters (BOM,
    bidi marks) and trigger spurious re-writes; for chunked values, the
    drift compounds across every chunk.

    The transforms, applied in the order Wikibase applies them:

      1. ``trimBadChars`` — strip a fixed set of invisible/non-character
         codepoints that Wikibase rejects: BOM (``U+FEFF``), bidi marks
         (``U+200E``/``U+200F``), and non-characters (``U+FFFE``/
         ``U+FFFF``). Source CSV/JSON occasionally carries these when
         partner metadata is round-tripped through pasted-text fields.
      2. Collapse every run of ASCII / Latin-1 control characters
         (``\\x00`` – ``\\x1F``, ``\\x7F`` – ``\\x9F`` — includes ``\\n``,
         ``\\r``, ``\\t``) to a single ASCII space.
      3. Strip leading and trailing whitespace. Python's ``str.strip()``
         covers chars where ``isspace()`` is true, which matches
         Wikibase's ``\\p{Z}`` strip closely enough for DPLA's
         source-metadata corpus.
      4. For monolingualtext datatypes only: NFC-normalize via
         :func:`unicodedata.normalize`. Wikibase wires `cleanupToNFC` for
         the monolingualtext parser path.

    Wikibase does NOT collapse internal regular spaces; neither does this
    helper. ``<``, ``>``, ``&`` are stored verbatim — no HTML escaping at
    the snak layer.
    """
    if not text:
        return ""
    text = text.translate(_BAD_CHARS_TABLE)
    text = _CONTROL_CHAR_RUN.sub(" ", text)
    text = text.strip()
    if is_monolingualtext:
        text = unicodedata.normalize("NFC", text)
    return text


# Codepoints Wikibase rejects via its trimBadChars equivalent. BOM (U+FEFF)
# and bidi marks (U+200E/U+200F) appear mid-string when partner metadata
# comes through pasted-text fields; non-characters (U+FFFE/U+FFFF) are
# always invalid Unicode regardless. Without stripping these locally, a
# value with an embedded BOM would round-trip through Wikibase shorter
# than what we emit, and the next sync would see a mismatch and re-write.
_BAD_CHARS_TABLE = dict.fromkeys((0xFEFF, 0x200E, 0x200F, 0xFFFE, 0xFFFF))


def _chunk_value(text: str, limit: int = WIKIBASE_STRING_LIMIT) -> list[str]:
    """Split ``text`` into chunks no longer than ``limit`` characters each.

    Boundary search picks a position N (1 ≤ N ≤ ``limit``) where both
    ``text[N-1]`` and ``text[N]`` are non-whitespace. This guarantees neither
    side of the split has leading or trailing whitespace that Wikibase
    would strip on save, so concat-after-store reassembles bit-for-bit
    identical to the original.

    Pathological case: a whitespace run longer than ``limit`` characters
    that straddles the chunk window leaves no valid non-whitespace boundary
    within reach. We fall back to splitting at exactly ``limit`` and emit a
    warning — interior whitespace at that boundary will collapse on
    round-trip, but content outside the whitespace run survives intact.
    DPLA source metadata never contains runs this long in practice.

    Callers must apply :func:`_normalize_string_value` before chunking so
    input matches Wikibase's stored form.
    """
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    remaining = text
    while len(remaining) > limit:
        boundary = limit
        while boundary > 0 and (
            remaining[boundary - 1].isspace() or remaining[boundary].isspace()
        ):
            boundary -= 1
        if boundary == 0:
            logger.warning(
                "Chunk boundary fell inside a whitespace run longer than %d "
                "characters; interior whitespace at this boundary will "
                "collapse on Wikibase round-trip",
                limit,
            )
            boundary = limit
        chunks.append(remaining[:boundary])
        remaining = remaining[boundary:]
    if remaining:
        chunks.append(remaining)
    return chunks


def _next_series_letter(
    letters: dict[tuple[str, str], str], prop: str, language: str
) -> str:
    """Return the next series letter for a chunked value on (prop, language).

    First long value seen for a (prop, lang) gets ``"A"``; second ``"B"``;
    twenty-seventh ``"AA"``; then ``"AB"``, ``"AC"``... — Excel-style
    column-letter sequence so the series never silently advances past
    ``"Z"`` into non-alpha codepoints (``chr(ord('Z') + 1) == '['``,
    which Commons-side reassembly would mis-group). 26+ long values on
    one (prop, lang) is unlikely in practice, but the failure mode is
    silent data corruption — well worth the eight extra lines to make
    it impossible.

    The letter is paired with the chunk ordinal to form the P1545
    qualifier value (e.g. ``"A1"``, ``"A2"``, ``"AA1"``). The Lua
    template on Commons groups chunks by series letter and sorts by
    ordinal to reassemble each long value independently.

    Mutates ``letters`` in place — callers thread the same dict through
    one ``build_claims_for_doc`` invocation to maintain per-doc state.
    """
    current = letters.get((prop, language))
    new_letter = "A" if current is None else _advance_series_letter(current)
    letters[(prop, language)] = new_letter
    return new_letter


def _advance_series_letter(letter: str) -> str:
    """Advance an Excel-style column-letter sequence by one: ``A → B``,
    ``Z → AA``, ``AZ → BA``, ``ZZ → AAA``.

    Pure function. Used only by :func:`_next_series_letter` to safely
    increment past ``Z`` rather than falling off the end of the ASCII
    uppercase range.
    """
    chars = list(letter)
    i = len(chars) - 1
    while i >= 0:
        if chars[i] == "Z":
            chars[i] = "A"
            i -= 1
        else:
            chars[i] = chr(ord(chars[i]) + 1)
            return "".join(chars)
    # All Z's — extend the sequence by one position ("ZZ" → "AAA").
    return "A" + "".join(chars)


def _chunk_and_emit_claims(
    prop: str,
    text: str,
    value_type: str,
    dpla_id: str,
    retrieval_date: datetime.date,
    chunk_series_letters: dict[tuple[str, str], str],
    *,
    language: str = "",
) -> list[dict]:
    """Normalize ``text``, chunk if it exceeds ``WIKIBASE_STRING_LIMIT``,
    and return one claim per chunk.

    Single-chunk values (≤ limit after normalization) emit one claim with
    no P1545 qualifier — bytewise identical to the pre-chunking output
    apart from the normalization pass.

    Multi-chunk values emit one claim per chunk, each carrying a P1545
    qualifier with value ``f"{letter}{ordinal}"`` (e.g. ``"A1"``, ``"A2"``).
    The series letter advances per (prop, language) so a second long
    value on the same property + language gets the next letter, keeping
    the chunk groups separable for reassembly.

    ``value_type`` is ``"string"`` or ``"monolingualtext"``. For
    monolingualtext, ``language`` is required and is also used as part of
    the series-letter key so two long English titles get A/B series
    while a long French title gets its own A series.

    Mutates ``chunk_series_letters`` in place when a long value consumes
    a fresh series letter — callers thread the same dict through one
    ``build_claims_for_doc`` invocation.
    """
    is_monolingualtext = value_type == "monolingualtext"
    normalized = _normalize_string_value(text, is_monolingualtext=is_monolingualtext)
    if not normalized:
        return []
    chunks = _chunk_value(normalized)
    if len(chunks) == 1:
        value: Any = (
            {"text": normalized, "language": language}
            if is_monolingualtext
            else normalized
        )
        return [formattedclaim(prop, value, value_type, dpla_id, retrieval_date)]
    letter = _next_series_letter(chunk_series_letters, prop, language)
    out: list[dict] = []
    for ordinal, chunk in enumerate(chunks, start=1):
        chunk_value: Any = (
            {"text": chunk, "language": language} if is_monolingualtext else chunk
        )
        claim = formattedclaim(prop, chunk_value, value_type, dpla_id, retrieval_date)
        claim["qualifiers"]["P1545"] = [
            _qualifier_string_snak("P1545", f"{letter}{ordinal}")
        ]
        out.append(claim)
    return out


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
      * Public Domain Mark is a rights-statement declaration (not a
        copyright license), so it does NOT emit a P275 claim — see below.
    """
    out: list[dict] = []
    rs_key = normalize_rights_uri(rs)

    # PDM (Creative Commons Public Domain Mark) is not a copyright license —
    # it's an assertion by the source institution that the work is already in
    # the public domain. Emitting it as a P275 (copyright license) claim, as
    # rights.json would have us do, produces SDC that Module:License's branch
    # table can't reconcile: a subsequent community edit adding a second P6216
    # value trips the ``#cs>=2, #cl>=1`` branch that falls through to an empty
    # bundle, and no license template renders on the file page.
    #
    # Express the PD assertion structurally via a bare P6216=Q19652 (public
    # domain) statement instead. Module:DPLA reads this shape and emits
    # ``{{PD-US}}`` directly, matching what the source institution has
    # declared via PDM without asserting a specific reason (e.g. >95 years
    # old, government work) that our upstream data doesn't warrant. The
    # corresponding ``rights.json`` PDM entry has been removed so this branch
    # is the single source of truth.
    if rs_key == PD_MARK_URI_CANONICAL:
        out.append(
            formattedclaim(
                "P6216",
                _item_value(Q_PUBLIC_DOMAIN),
                "wikibase-entityid",
                dpla_id,
                retrieval_date,
            )
        )
        return out

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

    Each statement gets a P3831 (object has role) qualifier encoding DPLA's
    two hub models:

      * Content hub (NARA, Smithsonian, ...): the hub IS the providing
        institution — a ``repository`` that also sits in P195 — and the
        "institution" is an internal department, tagged as the
        ``contributing`` (custodial) unit. DPLA is the ``aggregator`` above.
      * Service hub (everything else): the hub is an aggregating
        intermediary (``aggregator``, like DPLA) and the institution is a
        distinct organization (``repository``) that also sits in P195.

    These role/P195 shapes match what is already on Commons; the read side
    (Module:DPLA) and ``dpla_claims()`` depend on them, so changing them
    without a coordinated Commons-side rewrite would make every existing
    P9126 statement look "unexpected" and trigger a remove+re-add on every
    re-sync.
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

    out.append(_with_role(Q_DPLA, Q_ROLE_AGGREGATOR))
    if hub in CONTENT_HUB_QIDS:
        out.append(_with_role(hub, Q_ROLE_REPOSITORY))
        out.append(_with_role(institution, Q_ROLE_CONTRIBUTING))
    else:
        out.append(_with_role(hub, Q_ROLE_AGGREGATOR))
        out.append(_with_role(institution, Q_ROLE_REPOSITORY))
    return out


def _build_source_claim(
    hub: str,
    url: str,
    dpla_id: str,
    retrieval_date: datetime.date,
    iiif_manifest_url: str | None = None,
) -> dict:
    """Build the P7482 (described at) claim with its qualifier bundle.

    Qualifiers, all DPLA-authored (the ``_is_safe_to_amend_in_place`` gate
    considers a P7482 statement still "ours" only when every qualifier is
    in this list; see ``_DPLA_EXTRA_QUALIFIER_PROPS`` in tools/sdc_sync.py):

      * ``P973``  — described-at URL (the partner catalog page for this
                    DPLA item, ``edm:isShownAt``)
      * ``P137``  — operator (the hub's Wikidata QID)
      * ``P6108`` — IIIF manifest URL (only when the source DPLA record
                    has ``iiifManifest``; per-DPLA-item, identical across
                    every ordinal)

    The per-ordinal ``P2699`` qualifier (direct file download URL) is NOT
    stamped here — it differs per Commons file and is materialized at
    sdc-sync write time from ``file-list.txt``. See
    ``process_one_from_sdc`` in tools/sdc_sync.py.
    """
    claim = formattedclaim(
        "P7482",
        _item_value(Q_SOURCE_CATALOG),
        "wikibase-entityid",
        dpla_id,
        retrieval_date,
    )
    claim["qualifiers"]["P973"] = [_qualifier_string_snak("P973", url, datatype="url")]
    claim["qualifiers"]["P137"] = [_qualifier_item_snak("P137", hub)]
    if iiif_manifest_url:
        claim["qualifiers"]["P6108"] = [
            _qualifier_string_snak("P6108", iiif_manifest_url, datatype="url")
        ]
    return claim


def _build_local_id_claim(
    local_id: str,
    institution: str,
    dpla_id: str,
    retrieval_date: datetime.date,
    chunk_series_letters: dict[tuple[str, str], str],
) -> list[dict]:
    """Build a P217 (inventory number) claim with P195 (collection) qualifier.

    Returns one claim per chunk when ``local_id`` exceeds the Wikibase
    string limit; the P195 collection qualifier is replicated on every
    chunk claim since each chunk is its own statement.
    """
    claims = _chunk_and_emit_claims(
        "P217",
        local_id,
        "string",
        dpla_id,
        retrieval_date,
        chunk_series_letters,
    )
    for claim in claims:
        claim["qualifiers"]["P195"] = [_qualifier_item_snak("P195", institution)]
    return claims


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

# English month-name lookup for the natural-language forms the DPLA API
# occasionally emits (and that community editors routinely type into
# ``{{DPLA metadata}}`` ``date`` overrides). Recognised spellings include
# every 3-letter abbreviation plus the two common 4-letter one
# (``Sept``). The lookup key is casefold-stripped-of-trailing-period so
# ``September``, ``sept``, ``Sept.``, ``SEPT`` all collapse to the same
# integer.
_MONTH_NAMES = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_MONTH_NAME_RE = (
    r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?"
    r"|aug(?:ust)?|sept?(?:ember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
)
# ``November 1902`` / ``Nov 1902`` / ``Sept. 1902`` → month + year precision.
_MONTH_YEAR = re.compile(rf"^{_MONTH_NAME_RE}\.?\s+(\d{{3,4}})$", re.IGNORECASE)
# ``November 19, 1902`` / ``Nov 19 1902`` / ``November 19 1902`` → day precision.
# Comma between day and year is optional; whitespace is required.
_MONTH_DAY_YEAR = re.compile(
    rf"^{_MONTH_NAME_RE}\.?\s+(\d{{1,2}}),?\s+(\d{{3,4}})$", re.IGNORECASE
)
# ``19 November 1902`` / ``19 Nov. 1902`` → day precision (British / scholarly form).
_DAY_MONTH_YEAR = re.compile(
    rf"^(\d{{1,2}})\s+{_MONTH_NAME_RE}\.?\s+(\d{{3,4}})$", re.IGNORECASE
)
# ``11/19/1902`` — US slash-form with day precision. Always interpreted
# as MM/DD/YYYY; when the day component is <= 12 this is genuinely
# ambiguous with DD/MM/YYYY and a non-US-formatted date will be silently
# mis-parsed rather than rejected. Acceptable trade-off in this
# codebase: ``parse_dpla_date``'s callers are (a) the reconciler's
# comparator — where a mis-parse is self-correcting, since both sides
# just fail to match and the override survives unstripped — and (b)
# ``_build_date_claim`` on unambiguously US-formatted DPLA sources.
# Requires a 3-4-digit trailing year so unrelated slash-shaped strings
# can't glue.
_US_SLASH_DATE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{3,4})$")

# Unicode punctuation categories used by ``casefold_for_compare``'s
# leading/trailing strip. ``\W`` in Python's re without ``re.ASCII`` covers
# most Unicode punctuation; the explicit character class here spells out
# the ASCII/Unicode punctuation that matters in practice so the intent is
# reviewable and the behaviour is predictable regardless of locale.
_TRIM_PUNCT_CHARS = (
    r" \t\r\n"
    r".,;:!?"
    r"\"'`‘’“”"  # curly quotes
    r"()\[\]{}<>"
    r"\-‐‑‒–—"  # hyphens/en-dash/em-dash
    r"…"  # ellipsis
    r"/\\|"
)
_LEADING_TRIM_RE = re.compile(rf"^[{_TRIM_PUNCT_CHARS}]+")
_TRAILING_TRIM_RE = re.compile(rf"[{_TRIM_PUNCT_CHARS}]+$")
_INTERNAL_WS_RE = re.compile(r"\s+")


# Canonical-params keys where a casefold + leading/trailing-punctuation
# strip is a safe equivalence-widening fallback. Excludes opaque
# identifier / URL / hub keys: a case change in a Q-ID, DPLA ID, URL, or
# hub identifier is a genuinely different value, not a display variant.
#
# ``date`` is deliberately EXCLUDED. ``casefold_for_compare`` trims ``[``,
# ``]``, ``(``, ``)``, ``?`` — the same characters ``_strip_date_decorators``
# treats as approximate/uncertain markers — so ``[1902]``, ``(1902)``, and
# ``1902?`` would all fold to ``1902`` and spuriously match a bare
# canonical ``1902``. That would silently strip an archival "supplied/
# uncertain date" override, losing the uncertainty annotation.
# :func:`dates_semantically_equal` is the ONLY comparator dates need —
# it already handles month-name/slash-form format variance AND respects
# the approximate-flag distinction the decorator markers encode.
#
# Imported by both :mod:`ingest_wikimedia.legacy_artwork` (pre-write
# migration equivalence) and :mod:`ingest_wikimedia.wikitext_normalize`
# (post-write template-arg strip) so the allowlist can't drift.
CASEFOLD_COMPARE_KEYS: frozenset[str] = frozenset(
    {
        "title",
        "description",
        "permission",
        "creator",
    }
)


def is_wikitext_junk_value(value: str) -> bool:
    """True when ``value`` looks like a wikitext-extraction artifact —
    1–2 characters after strip, all punctuation/whitespace (no
    alphanumerics). Almost always a markup / regex / human error in
    the source template rather than a genuine metadata contribution
    worth preserving.

    Motivating cases: ``| date = ;`` (stray semicolon left over from
    a template rewrite), ``| title = --``, ``| creator = .``. Values
    of 3+ characters, or values containing a letter or digit, are
    treated as potentially legitimate — a single-character title
    like ``A`` (film) or a date like ``1`` (unlikely but syntactically
    parseable) is not filtered here.
    """
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    if not (1 <= len(stripped) <= 2):
        return False
    return not any(c.isalnum() for c in stripped)


# MediaWiki character-escape magic words. When a literal ``|`` or ``=``
# would confuse the template parser (`|` separates params, `=`
# separates key from value), community editors escape it with the
# builtin magic-word form on the LEFT — which the parser expands back
# to the character on the RIGHT before rendering. Docs:
# https://www.mediawiki.org/wiki/Help:Magic_words
#
# When we read a value OUT of a template parameter position — either to
# store it as an SDC statement or to compare it against DPLA canonical
# — we must undo the escape or the value carries the meaningless magic-
# word text into a context where it doesn't mean anything (the SDC
# value ends up literally containing "{{!}}", and the comparator against
# DPLA's canonical "|" fails).
_WIKITEXT_MAGIC_WORD_TABLE: dict[str, str] = {
    "{{!}}": "|",
    "{{=}}": "=",
    "{{((}}": "{{",
    "{{))}}": "}}",
    # ``{{!(}}`` / ``{{)!}}`` are community templates (Template:!( and
    # Template:)!) that render as ``{|`` and ``|}`` — the MediaWiki
    # table-start and table-end markers. Used inside template params
    # to keep a nested wikitable's opening/closing tokens from being
    # consumed by the outer template's argument parser.
    "{{!(}}": "{|",
    "{{)!}}": "|}",
}
_WIKITEXT_MAGIC_WORD_RE = re.compile(
    "|".join(re.escape(k) for k in _WIKITEXT_MAGIC_WORD_TABLE)
)


def unescape_wikitext_magic_words(s: str) -> str:
    """Replace MediaWiki character-escape magic words with the literal
    characters they render as. Non-string / empty input returns as-is.

    Applied when moving a value from wikitext (where these escapes are
    required inside template parameters) to any other context (SDC
    storage, comparator keys, log messages) where the magic-word form
    is meaningless literal text.

    Single-pass — regex substitution walks the original string once,
    so the output of one un-escape can't be rescanned as input to
    another. Sequential ``str.replace()`` would over-un-escape
    ``{{((}}))}}`` (the community idiom for a literal ``{{}}``) all
    the way to ``}}`` because ``{{((}}`` → ``{{`` leaves ``{{))}}``
    on the tape, which the next pass would then match.
    """
    if not isinstance(s, str) or not s:
        return s
    return _WIKITEXT_MAGIC_WORD_RE.sub(
        lambda m: _WIKITEXT_MAGIC_WORD_TABLE[m.group(0)], s
    )


def casefold_for_compare(s: str) -> str:
    """Fold ``s`` to a comparable form for equivalence checks against
    another display string. Unescapes MediaWiki character-escape magic
    words (``{{!}}``, ``{{=}}``, …), strips leading/trailing whitespace
    and punctuation, collapses internal whitespace runs, and casefolds.

    Used ONLY as a comparator key — never to rewrite stored or rendered
    values. DPLA-authored SDC and rendered wikitext must continue to
    match byte-for-byte (case and punctuation intact); this helper only
    widens the tolerance of "is this DPLA claim + this community claim
    the same fact?" checks.

    Non-string / falsy input returns an empty string, so a caller
    comparing two folded keys treats both as equal only when both are
    empty-like — deliberate: two None-shaped claims should not
    accidentally dedup against each other.
    """
    if not isinstance(s, str) or not s:
        return ""
    s = unescape_wikitext_magic_words(s)
    s = _LEADING_TRIM_RE.sub("", s)
    s = _TRAILING_TRIM_RE.sub("", s)
    s = _INTERNAL_WS_RE.sub(" ", s)
    return s.casefold()


def dates_semantically_equal(a: str, b: str) -> bool:
    """True iff ``a`` and ``b`` are date display strings that parse
    (via :func:`parse_dpla_date`) to the same Wikibase time value,
    precision, and circa-flag.

    Returns False when either side fails to parse or the parses
    disagree — never widens a match beyond structured equivalence. Used
    by the wikitext-normalize equivalence check (for ``date =``
    overrides in ``{{DPLA metadata}}``) and by the legacy-Artwork
    migration planner's community-vs-canonical comparator.
    """
    if not a or not b:
        return False
    # A maintenance-bot or community edit may have reformatted a bare display
    # date ("1910?") into {{other date|...}} template markup
    # ("{{other date|~|1910}}"). Reduce that markup to its plain display form
    # first — parse_dpla_date only understands display strings, not raw
    # template wikitext — so the reformatted value reconciles against the
    # canonical date and is stripped, instead of being mistaken for a
    # community date override. (Ranges like {{other date|between|X|Y}} return
    # None from parse_other_date_template and pass through to the range
    # matcher unchanged.)
    a = parse_other_date_template(a) or parse_taken_on_template(a) or a
    b = parse_other_date_template(b) or parse_taken_on_template(b) or b
    pa = parse_dpla_date(a)
    pb = parse_dpla_date(b)
    if pa is None or pb is None:
        return False
    if bool(pa.get("approximate")) != bool(pb.get("approximate")):
        return False
    return pa.get("value") == pb.get("value")


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


def _day_precision_value(y: int, mo: int, d: int) -> dict | None:
    """Return a day-precision Wikibase time-datavalue value dict for
    ``(y, mo, d)``, or ``None`` if the tuple isn't a real Gregorian
    date. Encapsulates the year-zero guard + ``datetime.date`` calendar
    check + ``_wikibase_time`` construction shared by every day-
    precision branch of :func:`parse_dpla_date`.
    """
    if y == 0:
        return None
    try:
        datetime.date(y, mo, d)
    except ValueError:
        return None
    return _wikibase_time(f"+{y:04d}-{mo:02d}-{d:02d}T00:00:00Z", _PRECISION_DAY)


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

      * ``YYYY-MM-DD``            → precision 11 (day)
      * ``MM/DD/YYYY``            → precision 11 (US slash-form)
      * ``Month D, YYYY`` /
        ``Month D YYYY``          → precision 11
      * ``D Month YYYY``          → precision 11 (British / scholarly)
      * ``YYYY-MM``               → precision 10 (month)
      * ``Month YYYY``            → precision 10 (English month name)
      * ``YYYYs``                 → precision 8 (decade), accepted only
                                    when the year is decade-aligned
                                    (e.g. ``1940s``, not ``1945s``)
      * ``YYYY``                  → precision 9 (year)

    Month names accept every 3-letter abbreviation plus the common
    4-letter ``Sept``; case is folded and an optional trailing period is
    tolerated (``Nov``, ``Nov.``, ``November``, ``NOV``). Comma between
    day and year is optional in the ``Month D YYYY`` shapes.

    Returns None for ranges (``1945-1950``), BC dates (``500 BC``), free
    prose (``During the Gilded Age``), era markers (``1945 AD``),
    parenthesised forms (``(1945)``), "before"/"after"/"between"
    prefixes, or any other shape that leaves unrecognised text in the
    residue after decorator stripping — the builder falls back to
    ``somevalue + P1932 stated-as`` for those, so the original DPLA
    string is preserved verbatim regardless of parse outcome.

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
        v = _day_precision_value(int(m[1]), int(m[2]), int(m[3]))
        if v is not None:
            return _ok(v)

    m = _US_SLASH_DATE.match(s)
    if m:
        v = _day_precision_value(int(m[3]), int(m[1]), int(m[2]))
        if v is not None:
            return _ok(v)

    m = _MONTH_DAY_YEAR.match(s)
    if m:
        v = _day_precision_value(int(m[3]), _MONTH_NAMES[m[1].casefold()], int(m[2]))
        if v is not None:
            return _ok(v)

    m = _DAY_MONTH_YEAR.match(s)
    if m:
        v = _day_precision_value(int(m[3]), _MONTH_NAMES[m[2].casefold()], int(m[1]))
        if v is not None:
            return _ok(v)

    m = _YEAR_MONTH.match(s)
    if m:
        y, mo = int(m[1]), int(m[2])
        if y != 0 and 1 <= mo <= 12:
            return _ok(
                _wikibase_time(f"+{y:04d}-{mo:02d}-01T00:00:00Z", _PRECISION_MONTH)
            )

    m = _MONTH_YEAR.match(s)
    if m:
        mo = _MONTH_NAMES[m[1].casefold()]
        y = int(m[2])
        if y != 0:
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


# Year-range patterns. ``parse_dpla_date`` deliberately returns None for any
# multi-year range, so ranges always land as ``somevalue + P1932`` claims;
# this helper produces an equivalence key so two range-shaped claims (one
# from DPLA, one inferred-from-Wikitext) can be recognised as the same
# statement and the inferred dupe pruned. Requires 3- or 4-digit years on
# both sides so an ISO-month string like ``"1934-12"`` can't pretend to be
# a range — single-date parsing handles those.
_RANGE_DASH = re.compile(r"^(\d{3,4})\s*[-/–—]\s*(\d{3,4})$")
_RANGE_BETWEEN = re.compile(
    r"^between\s+(\d{3,4})\s+(?:and|to|-|–|—)\s+(\d{3,4})$",
    re.IGNORECASE,
)
_RANGE_FROM_TO = re.compile(
    r"^(?:from\s+)?(\d{3,4})\s+(?:to|–|—)\s+(\d{3,4})$",
    re.IGNORECASE,
)
# Raw-wikitext fallback: if ``_expand_wikitext_for_date_parse`` couldn't
# reach the API or the value was stored before expansion was wired up,
# the P1932 qualifier carries literal ``{{other date|between|X|Y}}``
# markup. Match it directly so the reconciler can still dedup.
_RANGE_OTHER_DATE_BETWEEN = re.compile(
    r"^\{\{\s*other[ _]date\s*\|\s*between\s*\|\s*(\d{3,4})\s*\|\s*(\d{3,4})\s*\}\}$",
    re.IGNORECASE,
)


def parse_date_range(date_string: str) -> tuple[int, int] | None:
    """Parse a year-range string into ``(start_year, end_year)`` with
    ``start <= end``, or return ``None`` for non-range / unparseable
    inputs. Used purely for equivalence checks between two range-shaped
    claims; never used to build structured Wikibase time values.

    Recognised shapes (post wikitext-expansion or raw):

      * ``YYYY - YYYY`` / ``YYYY-YYYY`` / ``YYYY–YYYY`` / ``YYYY/YYYY``
      * ``between YYYY and YYYY``
      * ``(from) YYYY to YYYY``
      * ``{{other date|between|YYYY|YYYY}}`` (raw wikitext fallback)

    Returns ``None`` for single dates, BC dates, prose, or anything else
    ``parse_dpla_date`` already handles — callers try the single-date
    parser first and fall back to this helper only on its None result.

    Year 0 returns None, matching ``parse_dpla_date``: proleptic
    Gregorian has no year 0 and any downstream key built from it would
    collide with the year-1 form.
    """
    if not date_string:
        return None
    s = date_string.strip()
    for rx in (_RANGE_DASH, _RANGE_BETWEEN, _RANGE_FROM_TO, _RANGE_OTHER_DATE_BETWEEN):
        m = rx.match(s)
        if m:
            a, b = int(m[1]), int(m[2])
            if a == 0 or b == 0:
                return None
            return (min(a, b), max(a, b))
    return None


# Raw ``{{other date|MODIFIER|args...}}`` wikitext. Legacy migration runs
# before the expand-then-store fix (PR #304) stored this markup verbatim
# in the P1932 stated-as qualifier instead of its rendered text, so a
# claim's comparable key never matched the DPLA-sourced equivalent (which
# carries the *rendered* string, e.g. "circa 1911"). This recogniser
# converts the markup to the plain display string ``parse_dpla_date``
# understands so the two dedup.
_OTHER_DATE_TEMPLATE_RE = re.compile(
    r"^\{\{\s*other[ _]date\s*\|(.+?)\}\}$", re.IGNORECASE | re.DOTALL
)
# Modifiers whose meaning ``parse_dpla_date`` can also represent, mapped
# to the display-string shape it parses. Anything outside this set
# (before/after/century/season/…) is intentionally NOT converted: the
# DPLA side can't represent those either, so they stay as raw-string
# comparables that only match on byte-identical text — conservative, so
# an unrecognised modifier can never cause a wrong dedup/removal.
#
# These are ``{{other date}}`` *modifier tokens* — a different vocabulary
# from ``parse_dpla_date``'s display-string decorators (``_DATE_PREFIX``).
# We normalise every spelling here to the canonical ``"circa "`` prefix
# before handing off, so this set doesn't need to stay in sync with what
# ``parse_dpla_date`` strips — it only needs to recognise the template's
# own circa spellings (incl. bare ``c`` / ``ca`` the template accepts).
_OTHER_DATE_CIRCA_MODIFIERS = frozenset({"~", "circa", "c", "c.", "ca", "ca."})


def parse_other_date_template(value: str) -> str | None:
    """Convert a raw ``{{other date|...}}`` wikitext value into the plain
    display string :func:`parse_dpla_date` understands, or ``None`` when
    the input isn't an ``{{other date}}`` template or uses a modifier
    DPLA can't represent.

    Handles only the modifiers with an exact ``parse_dpla_date``
    equivalent:

      * circa family (``~`` / ``circa`` / ``c`` / ``c.`` / ``ca`` /
        ``ca.``) + a single date  → ``"circa <date>"``
      * ``?`` (uncertain) + a single date                → ``"<date>?"``
      * ``s`` / ``decade`` + a year                       → ``"<year>s"``

    Ranges (``{{other date|between|X|Y}}``) are NOT handled here —
    :func:`parse_date_range` already matches that markup directly.

    Conservative by design: returns ``None`` (caller keeps the raw
    string, which only matches byte-identical text) rather than guessing
    at unsupported modifiers, so this can never widen a dedup into a
    wrong removal.
    """
    if not value:
        return None
    m = _OTHER_DATE_TEMPLATE_RE.match(value.strip())
    if not m:
        return None
    parts = [p.strip() for p in m.group(1).split("|")]
    modifier = parts[0].lower() if parts else ""
    rest = [p for p in parts[1:] if p]
    if not rest:
        return None
    date_arg = rest[0]
    if modifier in _OTHER_DATE_CIRCA_MODIFIERS:
        return f"circa {date_arg}"
    if modifier == "?":
        return f"{date_arg}?"
    if modifier in ("s", "decade"):
        return f"{date_arg}s"
    return None


# Commons "date taken" templates whose first positional argument is a plain
# date: {{Taken on|1921-09-21}}, {{Taken in|1921}}, {{Taken circa|1921}}.
# Reduce to the display string parse_dpla_date understands so a community
# {{Taken on|…}} reconciles against the canonical date instead of importing a
# duplicate P571. Named params (|location=…) are ignored. Conservative:
# returns None for anything else so it can never widen a wrong dedup.
_TAKEN_TEMPLATE_RE = re.compile(
    r"^\{\{\s*taken[\s_]+(on|in|circa)\s*\|(.+?)\}\}$",
    re.IGNORECASE | re.DOTALL,
)


def parse_taken_on_template(value: str) -> str | None:
    """Convert a ``{{Taken on|…}}`` / ``{{Taken in|…}}`` / ``{{Taken circa|…}}``
    value into the plain display string :func:`parse_dpla_date` understands, or
    ``None`` when the input isn't one of those templates."""
    if not value:
        return None
    m = _TAKEN_TEMPLATE_RE.match(value.strip())
    if not m:
        return None
    modifier = m.group(1).lower()
    date_arg = next(
        (a.strip() for a in m.group(2).split("|") if a.strip() and "=" not in a),
        "",
    )
    if not date_arg:
        return None
    return f"circa {date_arg}" if modifier == "circa" else date_arg


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
    chunk_series_letters: dict[tuple[str, str], str],
) -> list[dict]:
    """Build the monolingualtext claim(s) for a long-form text field.

    Returns multiple claims with P1545 series-ordinal qualifiers when the
    text exceeds the Wikibase string limit; the Lua template on Commons
    reassembles by series + ordinal.
    """
    return _chunk_and_emit_claims(
        prop,
        text,
        "monolingualtext",
        dpla_id,
        retrieval_date,
        chunk_series_letters,
        language="en",
    )


def build_claims_for_doc(
    doc: dict,
    dpla_id: str,
    hubs: dict,
    rights: dict,
    subject_ids: dict,
    subjects_lookup: dict[tuple[str, str], str] | None,
) -> dict[str, Any] | None:
    """Build the complete ready-to-POST SDC claim list for a DPLA item.

    Returns ``{"claims": [...], "ingest_date": "YYYY-MM-DD"}`` matching
    the wbsetclaims POST shape (the extra ``ingest_date`` envelope field
    is read back by ``sdc-sync`` to pin its per-file P813 refresh — see
    the module docstring). Returns ``None`` when the doc can't be
    parsed (e.g. provider / dataProvider not in the hubs map) — callers
    should skip the item and not stage an sdc.json for it.

    ``subjects_lookup`` is the pre-resolved reconciliation table; pass
    ``None`` for inline-only behavior (the parallel P921 statements for
    NARA ``exactMatch`` subjects are simply omitted in that case).

    The P813 (retrieved on) reference date is derived internally from
    the doc's ``ingestDate`` via :func:`ingest_date_from_doc`. Raises
    ``ValueError`` if the doc lacks a valid ``ingestDate`` — callers
    catch per-item (same convention as ``ET.ParseError`` in
    ``get-ids-es`` / ``get-ids-nara``), log, and skip that item.
    """
    retrieval_date = ingest_date_from_doc(doc)
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

    # Track series-letter assignments for long-value chunking. Keyed by
    # (prop, language); first long value seen for a key gets letter "A",
    # second "B", and so on. The chunk emitters mutate this dict as they
    # consume letters.
    chunk_series_letters: dict[tuple[str, str], str] = {}

    # Rights cluster (P275/P6426/P6216).
    claims.extend(_build_rights_claims(rs, rights, dpla_id, retrieval_date))

    # P760 — DPLA ID.
    claims.extend(
        _chunk_and_emit_claims(
            "P760", dpla_id, "string", dpla_id, retrieval_date, chunk_series_letters
        )
    )

    # P1476 — title (one statement per title; chunked if needed).
    for title in titles:
        claims.extend(
            _build_monolingual_claim(
                "P1476", title, dpla_id, retrieval_date, chunk_series_letters
            )
        )

    # P195 — collection (one statement). A content hub is itself the
    # collection/repository; a service hub's collection is its institution.
    if institution:
        coll_qid = hub if hub in CONTENT_HUB_QIDS else institution
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
            claims.extend(
                _chunk_and_emit_claims(
                    "P4272",
                    name,
                    "string",
                    dpla_id,
                    retrieval_date,
                    chunk_series_letters,
                )
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

    # P10358 — description (chunked if needed).
    for desc in descs:
        claims.extend(
            _build_monolingual_claim(
                "P10358", desc, dpla_id, retrieval_date, chunk_series_letters
            )
        )

    # P9126 — maintained by chain.
    claims.extend(_build_contributed_claims(hub, institution, dpla_id, retrieval_date))

    # P7482 — described at source catalog. P6108 (IIIF manifest URL) is
    # added as a qualifier here when the source carries a usable
    # iiifManifest; P2699 (per-ordinal download URL) is added downstream
    # by sdc-sync. Defensive URL validation: some hubs ship the literal
    # string "null", whitespace, or malformed fragments in iiifManifest;
    # stamping that as P6108 would require manual Commons cleanup later.
    iiif_manifest_raw = doc.get("iiifManifest") or ""
    iiif_manifest_url = (
        iiif_manifest_raw.strip() if isinstance(iiif_manifest_raw, str) else ""
    )
    if not iiif_manifest_url.startswith(("http://", "https://")):
        iiif_manifest_url = None
    claims.append(
        _build_source_claim(
            hub,
            url,
            dpla_id,
            retrieval_date,
            iiif_manifest_url=iiif_manifest_url,
        )
    )

    # P217 — local identifier (non-NARA; per-value, with P195 qualifier).
    # Values exceeding the Wikibase string limit are chunked rather than
    # dropped, preserving the source identifier across multiple P217
    # statements (each carrying P195 + a P1545 series-ordinal qualifier).
    for local_id in local_ids:
        if not local_id:
            continue
        claims.extend(
            _build_local_id_claim(
                local_id, institution, dpla_id, retrieval_date, chunk_series_letters
            )
        )

    # NARA-only fields.
    for naid in naids:
        if naid:
            claims.extend(
                _chunk_and_emit_claims(
                    "P1225",
                    naid,
                    "string",
                    dpla_id,
                    retrieval_date,
                    chunk_series_letters,
                )
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

    return {"claims": claims, "ingest_date": retrieval_date.isoformat()}
