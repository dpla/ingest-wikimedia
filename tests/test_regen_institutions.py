"""Tests for tools/regen_institutions.py — the institutions_v2.json regenerator.

The ES fetch is exercised separately (composite pagination over a stubbed
`post_es`); the merge rules and serialisation are pure functions tested with
fixtures.
"""

import json
from unittest.mock import patch

from tools import regen_institutions as r


def _doc(**hubs):
    return dict(hubs)


def _hub(wikidata="", upload=False, institutions=None):
    return {"Wikidata": wikidata, "upload": upload, "institutions": institutions or {}}


def _inst(wikidata="", upload=False):
    return {"Wikidata": wikidata, "upload": upload}


# --- reconcile: institutions -------------------------------------------------


def test_new_institution_added_as_blank_opt_out():
    current = _doc(Alpha=_hub(institutions={"Old Inst": _inst("Q1", upload=True)}))
    index = {"Alpha": {"Old Inst", "New Inst"}}
    result, report = r.reconcile(current, index)
    assert result["Alpha"]["institutions"]["New Inst"] == {
        "Wikidata": "",
        "upload": False,
    }
    # Existing entry untouched (QID + opt-in preserved).
    assert result["Alpha"]["institutions"]["Old Inst"] == {
        "Wikidata": "Q1",
        "upload": True,
    }
    assert report["insts_added"] == {"Alpha": ["New Inst"]}


def test_dropped_institution_without_qid_is_removed():
    current = _doc(Alpha=_hub(institutions={"Gone": _inst("")}))
    index = {"Alpha": {"Still Here"}}
    result, report = r.reconcile(current, index)
    assert "Gone" not in result["Alpha"]["institutions"]
    assert "Still Here" in result["Alpha"]["institutions"]
    assert report["insts_removed"] == {"Alpha": ["Gone"]}


def test_dropped_institution_with_qid_is_kept():
    current = _doc(Alpha=_hub(institutions={"Drifted": _inst("Q42")}))
    index = {"Alpha": {"Other"}}  # 'Drifted' no longer in index
    result, report = r.reconcile(current, index)
    assert result["Alpha"]["institutions"]["Drifted"] == {
        "Wikidata": "Q42",
        "upload": False,
    }
    assert report["insts_dropped_kept"] == {"Alpha": ["Drifted"]}
    assert "Drifted" not in report.get("insts_removed", {}).get("Alpha", [])


# --- reconcile: hubs ---------------------------------------------------------


def test_new_hub_added():
    current = _doc()
    index = {"BrandNew": {"Inst A", "Inst B"}}
    result, report = r.reconcile(current, index)
    assert result["BrandNew"]["Wikidata"] == ""
    assert result["BrandNew"]["upload"] is False
    assert set(result["BrandNew"]["institutions"]) == {"Inst A", "Inst B"}
    assert report["hubs_added"] == ["BrandNew"]


def test_dropped_hub_without_any_qid_is_removed():
    current = _doc(Ghost=_hub(institutions={"X": _inst(""), "Y": _inst("")}))
    index = {"Alpha": {"Z"}}  # Ghost absent from index
    result, report = r.reconcile(current, index)
    assert "Ghost" not in result
    assert report["hubs_removed"] == ["Ghost"]


def test_dropped_hub_kept_when_hub_has_qid():
    current = _doc(Ghost=_hub(wikidata="Q100", institutions={"X": _inst("")}))
    index = {"Alpha": {"Z"}}
    result, report = r.reconcile(current, index)
    assert "Ghost" in result
    # Its QID-less institution still drops (no QID, not in index).
    assert result["Ghost"]["institutions"] == {}
    assert "Ghost" in report["hubs_dropped_kept"]


def test_dropped_hub_kept_when_an_institution_has_qid():
    current = _doc(Ghost=_hub(institutions={"Keep": _inst("Q7"), "Drop": _inst("")}))
    index = {"Alpha": {"Z"}}
    result, report = r.reconcile(current, index)
    assert "Ghost" in result
    assert "Keep" in result["Ghost"]["institutions"]
    assert "Drop" not in result["Ghost"]["institutions"]
    assert "Ghost" in report["hubs_dropped_kept"]


def test_hub_still_in_index_is_never_in_removed_or_dropped_kept():
    current = _doc(Alpha=_hub(institutions={"A": _inst("")}))
    index = {"Alpha": {"A"}}
    result, report = r.reconcile(current, index)
    assert "Alpha" in result
    assert report["hubs_removed"] == []
    assert report["hubs_dropped_kept"] == []


# --- serialisation -----------------------------------------------------------


def test_dump_json_shape_order_and_sorting():
    doc = _doc(
        Zeta=_hub(wikidata="Q9", upload=True, institutions={"Bee": _inst("Q2", True)}),
        Alpha=_hub(institutions={"Yak": _inst(""), "Ant": _inst("Q1")}),
    )
    out = r.dump_json(doc)
    parsed = json.loads(out)
    # Hubs and institutions plain-sorted.
    assert list(parsed) == ["Alpha", "Zeta"]
    assert list(parsed["Alpha"]["institutions"]) == ["Ant", "Yak"]
    # Fixed key order on both levels.
    assert list(parsed["Zeta"]) == ["Wikidata", "institutions", "upload"]
    assert list(parsed["Zeta"]["institutions"]["Bee"]) == ["Wikidata", "upload"]
    # 2-space indent + trailing newline.
    assert out.endswith("}\n")
    assert '\n  "Alpha"' in out


def test_dump_json_ascii_escapes_non_ascii():
    doc = _doc(Alpha=_hub(institutions={"Café": _inst("")}))
    out = r.dump_json(doc)
    assert "\\u00e9" in out  # é escaped, matching the committed file's style


# --- composite-aggregation pagination ---------------------------------------


def _agg_page(buckets, after_key):
    return {
        "aggregations": {"pairs": {"buckets": buckets, "after_key": after_key}},
        "_shards": {"failed": 0},
    }


def test_fetch_index_providers_paginates_and_groups():
    page1 = _agg_page(
        [
            {"key": {"provider": "Alpha", "dp": "Inst A"}},
            {"key": {"provider": "Alpha", "dp": "Inst B"}},
        ],
        after_key={"provider": "Alpha", "dp": "Inst B"},
    )
    page2 = _agg_page(
        [{"key": {"provider": "Beta", "dp": "Inst C"}}],
        after_key={"provider": "Beta", "dp": "Inst C"},
    )
    page3 = {"aggregations": {"pairs": {"buckets": []}}, "_shards": {"failed": 0}}

    responses = [page1, page2, page3]
    calls = []

    class _Resp:
        def __init__(self, data):
            self._data = data

        def raise_for_status(self):
            pass

        def json(self):
            return self._data

    def fake_post(query):
        calls.append(query)
        return _Resp(responses[len(calls) - 1])

    with patch.object(r, "post_es", side_effect=fake_post):
        out = r.fetch_index_providers(page_size=2)

    assert out == {"Alpha": {"Inst A", "Inst B"}, "Beta": {"Inst C"}}
    # Second call carried the after_key from page 1.
    assert calls[1]["aggs"]["pairs"]["composite"]["after"] == {
        "provider": "Alpha",
        "dp": "Inst B",
    }
