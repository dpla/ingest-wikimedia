"""Tests for ingest_wikimedia/maintain.py — the maintain-mode re-link engine.

Pure helpers (URL normalization, token extraction) are tested directly. The
anchor ladder is tested with a fake ``query_fn`` that routes on ES query shape,
so the rung-selection logic is exercised without a live cluster.
"""

from ingest_wikimedia.maintain import (
    ResolveResult,
    extract_stable_token,
    normalize_url_candidates,
    resolve_current_dpla_id,
)

NC = "https://lib.digitalnc.org/record/100550"
CURRENT_ID = "26f8310936c0321a8c611703751034ee"
DEAD_ID = "496453e05948633e07ab9cdeb8c99cd8"


# --- pure helpers ------------------------------------------------------------


def test_normalize_url_candidates_scheme_and_slash():
    out = normalize_url_candidates("http://x.org/record/1")
    assert out[0] == "http://x.org/record/1"  # original first
    assert "https://x.org/record/1" in out  # scheme flip (the common drift)
    assert "http://x.org/record/1/" in out  # trailing-slash variant
    # No duplicates.
    assert len(out) == len(set(out))


def test_normalize_url_candidates_empty():
    assert normalize_url_candidates("") == []
    assert normalize_url_candidates("   ") == []


def test_extract_stable_token_across_providers():
    assert extract_stable_token("https://lib.digitalnc.org/record/100550") == "100550"
    assert (
        extract_stable_token("http://dlg.galileo.usg.edu/id:arl_awc_awc337")
        == "arl_awc_awc337"
    )
    assert (
        extract_stable_token(
            "https://digital.library.illinois.edu/items/"
            "f9708790-cebe-0134-238a-0050569601ca-7"
        )
        == "f9708790-cebe-0134-238a-0050569601ca-7"
    )
    assert (
        extract_stable_token("https://archive.org/details/billcontinuingin00conf")
        == "billcontinuingin00conf"
    )


def test_extract_stable_token_too_short_declines():
    # Nothing long enough to safely anchor a wildcard on.
    assert extract_stable_token("https://x.org/a/b") is None
    assert extract_stable_token("") is None


# (DPLA-ID validation is reused from ingest_wikimedia.partners.is_dpla_id and
# tested there; the resolver just consumes it.)


# --- fake ES for the ladder --------------------------------------------------


class _Resp:
    def __init__(self, ids):
        self._ids = ids

    def json(self):
        return {
            "_shards": {"failed": 0},
            "hits": {
                "total": {"value": len(self._ids)},
                "hits": [{"_id": i} for i in self._ids],
            },
        }


class FakeES:
    """Routes a query dict to canned hits by shape."""

    def __init__(self, *, live_ids=(), isshownat=None, wildcard=None):
        self.live_ids = set(live_ids)
        self.isshownat = isshownat or {}
        self.wildcard = wildcard or {}
        self.calls = []

    def __call__(self, query):
        self.calls.append(query)
        q = query["query"]
        if "ids" in q:
            wanted = q["ids"]["values"][0]
            return _Resp([wanted] if wanted in self.live_ids else [])
        if "terms" in q and "isShownAt" in q["terms"]:
            ids: list[str] = []
            for u in q["terms"]["isShownAt"]:
                for i in self.isshownat.get(u, []):
                    if i not in ids:
                        ids.append(i)
            return _Resp(ids)
        if "bool" in q:
            patt = q["bool"]["must"][0]["wildcard"]["isShownAt"]  # "*token*"
            token = patt.strip("*")
            return _Resp(self.wildcard.get(token, []))
        raise AssertionError(f"unexpected query shape: {query}")


# --- ladder ------------------------------------------------------------------


def test_anchor1_embedded_id_live():
    es = FakeES(live_ids={CURRENT_ID})
    r = resolve_current_dpla_id(
        embedded_id=CURRENT_ID, recorded_url=NC, scope_filter=None, query_fn=es
    )
    assert r == ResolveResult(CURRENT_ID, "embedded", tried=["embedded"])
    # Resolved on rung 1 — never queried isShownAt.
    assert len(es.calls) == 1


def test_anchor2_exact_isshownat_when_embedded_dead():
    es = FakeES(live_ids=set(), isshownat={NC: [CURRENT_ID]})
    r = resolve_current_dpla_id(
        embedded_id=DEAD_ID, recorded_url=NC, scope_filter=None, query_fn=es
    )
    assert r.dpla_id == CURRENT_ID
    assert r.anchor == "isShownAt"


def test_anchor2_recovers_via_scheme_normalization():
    # Old upload recorded http://; index stores https:// — the common case.
    recorded_http = "http://lib.digitalnc.org/record/100550"
    stored_https = "https://lib.digitalnc.org/record/100550"
    es = FakeES(isshownat={stored_https: [CURRENT_ID]})
    r = resolve_current_dpla_id(
        embedded_id=None, recorded_url=recorded_http, scope_filter=None, query_fn=es
    )
    assert r.dpla_id == CURRENT_ID
    assert r.anchor == "isShownAt"


def test_anchor2_multi_hit_is_ambiguous_not_guessed():
    es = FakeES(isshownat={NC: [CURRENT_ID, "other"]})
    r = resolve_current_dpla_id(
        embedded_id=None, recorded_url=NC, scope_filter=None, query_fn=es
    )
    assert r.dpla_id is None
    assert r.anchor == "unresolved"
    assert r.ambiguous is True


def test_anchor3_scoped_wildcard_on_token():
    scope = {"term": {"dataProvider.name.not_analyzed": "Athens-Clarke County Library"}}
    ga = "http://dlg.galileo.usg.edu/id:arl_awc_awc337"
    es = FakeES(wildcard={"arl_awc_awc337": ["0ff4842c996e3abab8cff9b3f8f8b297"]})
    r = resolve_current_dpla_id(
        embedded_id=DEAD_ID, recorded_url=ga, scope_filter=scope, query_fn=es
    )
    assert r.dpla_id == "0ff4842c996e3abab8cff9b3f8f8b297"
    assert r.anchor == "wildcard"


def test_anchor3_skipped_without_scope_filter():
    ga = "http://dlg.galileo.usg.edu/id:arl_awc_awc337"
    es = FakeES(wildcard={"arl_awc_awc337": ["whatever"]})
    r = resolve_current_dpla_id(
        embedded_id=None, recorded_url=ga, scope_filter=None, query_fn=es
    )
    assert r.anchor == "unresolved"
    # Wildcard rung never ran (no scope to bound it).
    assert "wildcard" not in r.tried


def test_lazy_scope_callable_invoked_only_at_wildcard_rung():
    # An embedded hit resolves on rung 1 — the scope callable (a per-file P195
    # read in production) must not run.
    es = FakeES(live_ids={CURRENT_ID})
    calls = []

    def scope():
        calls.append(1)
        return {"term": {"dataProvider.name.not_analyzed": "X"}}

    r = resolve_current_dpla_id(
        embedded_id=CURRENT_ID, recorded_url=NC, scope_filter=scope, query_fn=es
    )
    assert r.anchor == "embedded"
    assert calls == []


def test_lazy_scope_callable_bounds_wildcard_when_reached():
    ga = "http://dlg.galileo.usg.edu/id:arl_awc_awc337"
    es = FakeES(wildcard={"arl_awc_awc337": ["0ff4842c996e3abab8cff9b3f8f8b297"]})
    calls = []

    def scope():
        calls.append(1)
        return {
            "term": {"dataProvider.name.not_analyzed": "Athens-Clarke County Library"}
        }

    r = resolve_current_dpla_id(
        embedded_id=DEAD_ID, recorded_url=ga, scope_filter=scope, query_fn=es
    )
    assert r.dpla_id == "0ff4842c996e3abab8cff9b3f8f8b297"
    assert r.anchor == "wildcard"
    assert calls == [1]  # invoked exactly once, at the wildcard rung


def test_lazy_scope_callable_returning_none_skips_wildcard():
    ga = "http://dlg.galileo.usg.edu/id:arl_awc_awc337"
    es = FakeES(wildcard={"arl_awc_awc337": ["whatever"]})
    r = resolve_current_dpla_id(
        embedded_id=DEAD_ID, recorded_url=ga, scope_filter=lambda: None, query_fn=es
    )
    assert r.anchor == "unresolved"
    assert "wildcard" not in r.tried


def test_all_anchors_miss_is_unresolved():
    es = FakeES()
    r = resolve_current_dpla_id(
        embedded_id=DEAD_ID,
        recorded_url=NC,
        scope_filter={"term": {"dataProvider.name.not_analyzed": "X"}},
        query_fn=es,
    )
    assert r.dpla_id is None
    assert r.anchor == "unresolved"
    assert r.ambiguous is False
