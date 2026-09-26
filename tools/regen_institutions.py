#!/usr/bin/env python3
"""Regenerate ``institutions_v2.json`` from the live DPLA index.

``institutions_v2.json`` (in dpla/ingestion3 at
``src/main/resources/wiki/institutions_v2.json``) is the upload-eligibility +
Wikidata-QID registry the Wikimedia pipeline reads. Its top-level keys are hub
(provider) display names; under each is an ``institutions`` map keyed by
dataProvider display name. Both levels carry ``upload`` (bool) and ``Wikidata``
(a QID string, ``""`` when unknown).

Institution names — and, rarely, hub names — appear, drift, and disappear with
every re-index. This tool reconciles the file against the current index:

  * **ADD** every (provider, dataProvider) pair present in the index but
    missing from the file, as ``{"Wikidata": "", "upload": false}``.
  * **REMOVE** entries no longer in the index — but ONLY when their
    ``Wikidata`` is empty. Any entry carrying a QID is preserved: a name may
    have only drifted slightly and the QID is worth recovering by hand.
  * **PRESERVE** every retained entry's existing ``upload`` and ``Wikidata``
    verbatim — this tool never flips an opt-in or drops a QID.
  * A **hub** is removed only when it is absent from the index AND its own
    ``Wikidata`` is empty AND no surviving institution under it carries a QID.

Unique dataProvider names are taken *per provider* (within each hub) via a
composite aggregation over ``provider.name.not_analyzed`` ×
``dataProvider.name.not_analyzed`` — composite, not ``terms``, so a large hub
(Digital Maryland alone has ~2,650 institutions) cannot silently truncate.

Output is written byte-faithful to the file's conventions (2-space indent,
ASCII-escaped, plain-sorted keys, key order ``Wikidata`` / ``institutions`` /
``upload``, trailing newline) so the resulting PR diff is purely semantic.

Must run where the internal DPLA ES is reachable (the wiki/ingest EC2 box),
since :mod:`ingest_wikimedia.es` points at ``search-prod1.internal.dp.la``.
"""

import argparse
import json
import sys
import urllib.request

from ingest_wikimedia.es import check_es_response, post_es
from ingest_wikimedia.partners import INSTITUTIONS_URL

# Composite-aggregation page size. Each page is one ES request returning this
# many (provider, dataProvider) pairs; ~9,300 pairs today ⇒ ~10 pages.
_PAGE_SIZE = 1000


def fetch_index_providers(page_size: int = _PAGE_SIZE) -> dict[str, set[str]]:
    """Return ``{provider_name: {dataProvider_name, …}}`` for every unique
    (provider, dataProvider) pair in the live index.

    Uses a composite aggregation paginated via ``after_key`` so the full set
    is enumerated exhaustively — a ``terms`` aggregation would silently cap at
    its ``size`` and drop the tail of a large hub.
    """
    out: dict[str, set[str]] = {}
    after: dict | None = None
    while True:
        composite: dict = {
            "size": page_size,
            "sources": [
                {"provider": {"terms": {"field": "provider.name.not_analyzed"}}},
                {"dp": {"terms": {"field": "dataProvider.name.not_analyzed"}}},
            ],
        }
        if after is not None:
            composite["after"] = after
        resp = post_es({"size": 0, "aggs": {"pairs": {"composite": composite}}})
        resp.raise_for_status()
        data = resp.json()
        check_es_response(data)
        agg = data["aggregations"]["pairs"]
        buckets = agg.get("buckets") or []
        if not buckets:
            break
        for bucket in buckets:
            provider = bucket["key"].get("provider")
            dataprovider = bucket["key"].get("dp")
            if provider is None or dataprovider is None:
                continue
            out.setdefault(provider, set()).add(dataprovider)
        after = agg.get("after_key")
        if after is None:
            break
    return out


def load_current(source: str) -> dict:
    """Load the current institutions_v2.json from a URL or a local path."""
    if source.startswith(("http://", "https://")):
        with urllib.request.urlopen(source, timeout=30) as resp:
            return json.loads(resp.read())
    with open(source, encoding="utf-8") as fh:
        return json.load(fh)


def _has_qid(obj: dict) -> bool:
    """True iff the entry carries a non-empty Wikidata value.

    Deliberately a presence check, NOT ``partners.is_wikidata_id`` (which
    enforces ``^Q\\d+$``). This gates the protect-from-removal rule, so the
    safe failure mode is to over-preserve: a malformed or hand-entered value
    is exactly the kind of thing worth keeping for human recovery, never
    silently deleting. Do not "tighten" this to the regex.
    """
    return bool((obj.get("Wikidata") or "").strip())


def reconcile(current: dict, index: dict[str, set[str]]) -> tuple[dict, dict]:
    """Merge the live ``index`` into the ``current`` document.

    Returns ``(new_document, report)``. ``report`` records every add, removal,
    and dropped-but-kept-for-QID entry for the human-readable summary.
    """
    report: dict = {
        "hubs_added": [],
        "hubs_removed": [],
        "hubs_dropped_kept": [],
        "insts_added": {},
        "insts_removed": {},
        "insts_dropped_kept": {},
    }
    result: dict = {}

    for hub in set(current) | set(index):
        es_insts = index.get(hub, set())
        cur = current.get(hub)
        is_new_hub = cur is None
        if is_new_hub:
            cur = {"Wikidata": "", "upload": False, "institutions": {}}
        cur_insts = cur.get("institutions") or {}

        new_insts: dict = {}
        for name, obj in cur_insts.items():
            if name in es_insts:
                new_insts[name] = obj  # still in the index — keep, untouched
            elif _has_qid(obj):
                new_insts[name] = obj  # dropped but protected by its QID
                report["insts_dropped_kept"].setdefault(hub, []).append(name)
            else:
                report["insts_removed"].setdefault(hub, []).append(name)
        for name in es_insts:
            if name not in new_insts:
                new_insts[name] = {"Wikidata": "", "upload": False}
                report["insts_added"].setdefault(hub, []).append(name)
        cur["institutions"] = new_insts

        hub_in_index = hub in index
        # Keep the hub if it's still aggregating items, or if a QID (its own or
        # any surviving institution's) makes it worth preserving for recovery.
        hub_protected = _has_qid(cur) or any(_has_qid(o) for o in new_insts.values())
        if hub_in_index or hub_protected:
            result[hub] = cur
            if is_new_hub:
                report["hubs_added"].append(hub)
            elif not hub_in_index:
                report["hubs_dropped_kept"].append(hub)
        else:
            report["hubs_removed"].append(hub)

    return result, report


def dump_json(doc: dict) -> str:
    """Serialise ``doc`` byte-faithfully to the file's conventions: 2-space
    indent, ASCII-escaped, plain-sorted hub and institution keys, fixed key
    order, trailing newline."""
    ordered: dict = {}
    for hub in sorted(doc):
        node = doc[hub]
        insts = node.get("institutions") or {}
        ordered[hub] = {
            "Wikidata": node.get("Wikidata", ""),
            "institutions": {
                name: {
                    "Wikidata": insts[name].get("Wikidata", ""),
                    "upload": bool(insts[name].get("upload", False)),
                }
                for name in sorted(insts)
            },
            "upload": bool(node.get("upload", False)),
        }
    return json.dumps(ordered, indent=2, ensure_ascii=True) + "\n"


def format_report(report: dict, current: dict, result: dict) -> str:
    """Render the change summary for an operator / PR description."""
    lines = ["== institutions_v2.json regeneration =="]
    n_add = sum(len(v) for v in report["insts_added"].values())
    n_rem = sum(len(v) for v in report["insts_removed"].values())
    n_kept = sum(len(v) for v in report["insts_dropped_kept"].values())
    lines.append(
        f"hubs: {len(current)} -> {len(result)} "
        f"(+{len(report['hubs_added'])} added, -{len(report['hubs_removed'])} removed)"
    )
    lines.append(
        f"institutions: +{n_add} added, -{n_rem} removed, "
        f"{n_kept} dropped-from-index but kept (carry a QID)"
    )

    def hub_list(items: list, header: str, sign: str) -> None:
        if items:
            lines.append("\n" + header)
            lines.extend(f"  {sign} {h}" for h in sorted(items))

    def per_hub_counts(by_hub: dict, header: str, sign: str) -> None:
        if by_hub:
            lines.append("\n" + header)
            lines.extend(f"  {hub}: {sign}{len(by_hub[hub])}" for hub in sorted(by_hub))

    hub_list(report["hubs_added"], "Hubs ADDED:", "+")
    hub_list(report["hubs_removed"], "Hubs REMOVED (absent from index, no QID):", "-")
    if report["insts_dropped_kept"]:
        lines.append("\nInstitutions dropped from index but KEPT (carry a QID):")
        for hub in sorted(report["insts_dropped_kept"]):
            for name in sorted(report["insts_dropped_kept"][hub]):
                qid = current[hub]["institutions"][name].get("Wikidata", "")
                lines.append(f"  ~ {hub} / {name} ({qid})")
    per_hub_counts(report["insts_added"], "Institutions ADDED (per hub):", "+")
    per_hub_counts(report["insts_removed"], "Institutions REMOVED (per hub):", "-")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--current",
        default=INSTITUTIONS_URL,
        help="Current institutions_v2.json (URL or local path). "
        "Defaults to the live file on dpla/ingestion3 main.",
    )
    parser.add_argument(
        "--out",
        default="-",
        help="Where to write the regenerated JSON ('-' for stdout).",
    )
    args = parser.parse_args()

    current = load_current(args.current)
    index = fetch_index_providers()
    result, report = reconcile(current, index)
    output = dump_json(result)

    if args.out == "-":
        sys.stdout.write(output)
    else:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(output)
    sys.stderr.write(format_report(report, current, result) + "\n")


if __name__ == "__main__":
    main()
