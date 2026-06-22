"""Re-link engine for maintain mode.

Maintain mode keeps already-uploaded Commons files current (SDC sync, template
migration, title/hash/ID-drift correction) for institutions no longer
authorized for *new* uploads. The hard sub-problem is **DPLA ID drift**: a
file's embedded DPLA ID (in its filename / ``dpla_id=`` param / SDC ``P760``)
can go dead while the same item lives on under a *new* DPLA ID, so the
``dp.la/item/<id>`` links 404. We must re-link the orphaned Commons file to its
current DPLA record.

The durable anchor is the provider's own source URL (the legacy ``url=`` ↔ the
record's ``isShownAt``), which sampling shows is present on essentially every
legacy upload. This module resolves the current DPLA ID via a provider-agnostic
ladder — no per-institution URL parsing, validated against the live index
(``isShownAt`` is a ``keyword`` field, exact ``term`` queries work):

  1. **embedded** — the file's embedded DPLA ID still resolves in the index.
     No drift; use it. (The common case; callers pass it first.)
  2. **isShownAt** — exact ``term`` match on ``isShownAt`` over a few
     *normalized* variants (http/https, trailing slash). Catches both genuinely
     stable URLs and the very common trivial mismatch where the old upload
     recorded ``http://`` but the index stores ``https://``.
  3. **wildcard** — institution-scoped ``wildcard`` on the longest stable
     id-like token from the URL path. Catches real domain drift where the
     record-id token persists. Bounded by the institution scope; only accepted
     when it resolves to exactly one record.
  4. **unresolved** — anything left (ambiguous or no match) is reported for
     content-hash confirmation or human review. Never guessed, never deleted.

The ES-querying resolver takes an injectable ``query_fn`` (defaults to
:func:`ingest_wikimedia.es.post_es`) so the ladder logic is unit-tested without
a live cluster. The pure helpers below carry no ES dependency at all.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable
from urllib.parse import urlparse

from ingest_wikimedia.es import check_es_response, post_es
from ingest_wikimedia.partners import is_dpla_id

# Tokens shorter than this are too generic to anchor a wildcard match on
# (e.g. "id", "ref", "cdm"); below it we decline rather than risk a broad scan.
_MIN_TOKEN_LEN = 6


def normalize_url_candidates(url: str) -> list[str]:
    """Return exact-match candidate strings for ``url``, most-specific first.

    The old upload's recorded ``url=`` and the index's ``isShownAt`` are
    frequently the same record but differ in scheme (``http`` vs ``https``) or a
    trailing slash — trivial, extremely common mismatches that would defeat an
    exact ``term`` query. Emit the original plus those normalized variants
    (deduped, order-preserving) so the resolver can try each as an exact match.
    """
    url = (url or "").strip()
    if not url:
        return []
    bases: list[str] = [url]
    # http <-> https
    if url.startswith("http://"):
        bases.append("https://" + url[len("http://") :])
    elif url.startswith("https://"):
        bases.append("http://" + url[len("https://") :])
    # +/- a single trailing slash for each scheme variant
    out: list[str] = []
    for b in bases:
        for v in (b, b.rstrip("/") if b.endswith("/") else b + "/"):
            if v not in out:
                out.append(v)
    return out


def extract_stable_token(url: str) -> str | None:
    """Return the longest id-like token from ``url``'s path/query, or None.

    Provider-agnostic: split the path and query on common identifier
    delimiters and pick the longest token (ties broken toward a token
    containing a digit). For the providers seen across hubs this yields the
    record id — DigitalNC ``/record/100550`` -> ``100550``, DLG
    ``/id:arl_awc_awc337`` -> ``arl_awc_awc337``, Illinois ``/items/<uuid>`` ->
    the uuid, archive.org ``/details/<id>`` -> the id. It is only a *narrowing*
    signal for the scoped wildcard (anchor 3), never a sole re-link key, so an
    imperfect pick (e.g. a CONTENTdm collection slug) merely yields an ambiguous
    result that falls through to human review — never a wrong re-link.
    """
    parsed = urlparse(url or "")
    haystack = f"{parsed.path}?{parsed.query}" if parsed.query else parsed.path
    tokens = [t for t in re.split(r"[^A-Za-z0-9_-]+", haystack) if t]
    if not tokens:
        return None
    best = max(
        tokens,
        key=lambda t: (len(t), any(c.isdigit() for c in t)),
    )
    return best if len(best) >= _MIN_TOKEN_LEN else None


# --- ES query builders (pure) -----------------------------------------------


def _ids_query(dpla_id: str) -> dict:
    """Count query: does a record with this id exist? (id is the ES _id.)"""
    return {"size": 0, "query": {"ids": {"values": [dpla_id]}}}


def _isshownat_terms_query(urls: list[str]) -> dict:
    """Exact ``terms`` on the ``isShownAt`` keyword field over all normalized
    URL variants in ONE round-trip — a record matches if its ``isShownAt``
    equals any variant. ``size`` 2 so the ">1 record" ambiguity check still
    sees a second hit. (One query instead of one-per-variant matters at the
    per-file scale this runs at.)
    """
    return {
        "size": 2,
        "query": {"terms": {"isShownAt": urls}},
        "_source": ["isShownAt"],
    }


def _scoped_wildcard_query(token: str, scope_filter: dict) -> dict:
    """Institution-scoped ``wildcard`` on ``isShownAt`` for ``*token*``.

    ``scope_filter`` is an ES filter clause the caller builds to bound the scan
    to one institution (e.g. ``{"term": {"dataProvider.name.not_analyzed":
    "..."}}``) — required, because a leading-wildcard scan over the whole index
    is prohibitively expensive.
    """
    return {
        "size": 3,
        "query": {
            "bool": {
                "filter": [scope_filter],
                "must": [{"wildcard": {"isShownAt": f"*{token}*"}}],
            }
        },
        "_source": ["isShownAt"],
    }


# --- resolver ----------------------------------------------------------------


@dataclass
class ResolveResult:
    """Outcome of re-linking one orphaned file to a current DPLA record.

    ``anchor`` is the rung that resolved it ("embedded" / "isShownAt" /
    "wildcard") or "unresolved". ``dpla_id`` is the current id when resolved,
    else None. ``ambiguous`` flags a multi-hit (more than one current record
    matched) — never auto-applied; surfaced for content-hash/human review.
    """

    dpla_id: str | None
    anchor: str
    ambiguous: bool = False
    tried: list[str] = field(default_factory=list)


def _total(resp_json: dict) -> int:
    check_es_response(resp_json)
    total = resp_json.get("hits", {}).get("total", 0)
    return total.get("value", 0) if isinstance(total, dict) else total


def _hit_ids(resp_json: dict) -> list[str]:
    return [h.get("_id") for h in resp_json.get("hits", {}).get("hits", [])]


def resolve_current_dpla_id(
    *,
    embedded_id: str | None,
    recorded_url: str | None,
    scope_filter: dict | Callable[[], dict | None] | None,
    query_fn: Callable[[dict], object] = post_es,
) -> ResolveResult:
    """Resolve the *current* DPLA ID for one orphaned Commons file.

    Walks the validated ladder (embedded -> exact isShownAt -> scoped wildcard)
    and stops at the first unambiguous hit. ``query_fn`` runs an ES query and
    returns a ``requests``-style response exposing ``.json()`` — injected so the
    ladder is testable without a cluster.

    ``scope_filter`` bounds the wildcard rung to one institution. It may be a
    ready ES filter clause, or a zero-arg callable that returns one (or None) —
    the callable is invoked **only** if the ladder reaches the wildcard rung, so
    the per-file work of deriving the scope (e.g. reading the file's existing
    P195 institution) is skipped for the common embedded/isShownAt hits. Pass
    None to skip the wildcard rung entirely.
    """
    tried: list[str] = []

    # Anchor 1: embedded id still live in the index.
    if embedded_id and is_dpla_id(embedded_id):
        tried.append("embedded")
        if _total(query_fn(_ids_query(embedded_id)).json()) > 0:
            return ResolveResult(embedded_id, "embedded", tried=tried)

    # Anchor 2: exact isShownAt over all normalized URL variants, one query.
    candidates = normalize_url_candidates(recorded_url or "")
    if candidates:
        tried.append("isShownAt")
        data = query_fn(_isshownat_terms_query(candidates)).json()
        total = _total(data)
        if total == 1:
            return ResolveResult(_hit_ids(data)[0], "isShownAt", tried=tried)
        if total > 1:
            # A variant matches more than one record — don't guess.
            return ResolveResult(None, "unresolved", ambiguous=True, tried=tried)

    # Anchor 3: institution-scoped wildcard on the stable URL token.
    token = extract_stable_token(recorded_url or "")
    if token:
        # Resolve a lazy scope only now — deriving it (a P195 read on Commons)
        # is wasted work on the embedded/isShownAt hits handled above.
        scope = scope_filter() if callable(scope_filter) else scope_filter
        if scope:
            tried.append("wildcard")
            data = query_fn(_scoped_wildcard_query(token, scope)).json()
            total = _total(data)
            if total == 1:
                return ResolveResult(_hit_ids(data)[0], "wildcard", tried=tried)
            if total > 1:
                return ResolveResult(None, "unresolved", ambiguous=True, tried=tried)

    return ResolveResult(None, "unresolved", tried=tried)
