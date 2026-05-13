"""Partner hub lookup table and upload eligibility check for Wikimedia upload pipeline.

Stdlib-only — no third-party imports — so this module is safe to use in Lambda and
GitHub Actions without installing the full ingest_wikimedia package dependencies.
"""

import json
import re
import urllib.request

# Module-level cache so warm Lambda invocations skip repeated network fetches.
_institutions_cache: dict | None = None

INSTITUTIONS_URL = (
    "https://raw.githubusercontent.com/dpla/ingestion3"
    "/refs/heads/main/src/main/resources/wiki/institutions_v2.json"
)

# Wikidata QID pattern (e.g. Q12345).
_QID_RE = re.compile(r"^Q\d+$")

# All DPLA partner hubs: canonical slug → hub display name (as used in institutions_v2.json)
PARTNER_HUBS: dict[str, str] = {
    "artstor": "Artstor",
    "bhl": "Biodiversity Heritage Library",
    "bpl": "Digital Commonwealth",
    "cdl": "California Digital Library",
    "community-webs": "Community Webs",
    "ct": "Connecticut Digital Archive",
    "david-rumsey": "David Rumsey",
    "dc": "District Digital",
    "digitalnc": "North Carolina Digital Heritage Center",
    "florida": "Sunshine State Digital Network",
    "georgia": "Digital Library of Georgia",
    "getty": "J. Paul Getty Trust",
    "gpo": "United States Government Publishing Office (GPO)",
    "harvard": "Harvard Library",
    "hathi": "HathiTrust",
    "heartland": "Heartland Hub",
    "ia": "Internet Archive",
    "il": "Illinois Digital Heritage Hub",
    "indiana": "Indiana Memory",
    "jh3": "Jewish Heritage and History Hub",
    "lc": "Library of Congress",
    "maine": "Digital Maine",
    "maryland": "Digital Maryland",
    "mi": "Michigan Service Hub",
    "minnesota": "Minnesota Digital Library",
    "mississippi": "Mississippi Digital Library",
    "mwdl": "Mountain West Digital Library",
    "nara": "National Archives and Records Administration",
    "njde": "NJ/DE Digital Collective",
    "northwest-heritage": "Northwest Digital Heritage",
    "nypl": "The New York Public Library",
    "ohio": "Ohio Digital Network",
    "oklahoma": "OKHub",
    "p2p": "Plains to Peaks Collective",
    "pa": "PA Digital",
    "scdl": "South Carolina Digital Library",
    "si": "Smithsonian Institution",
    "texas": "The Portal to Texas History",
    "tn": "Digital Library of Tennessee",
    "txdl": "Texas Digital Library",
    "virginias": "Digital Virginias",
    "vt": "Vermont Green Mountain Digital Archive",
    "washington": "University of Washington",
    "wisconsin": "Recollection Wisconsin",
}

# Alternate slugs that map to a canonical slug in PARTNER_HUBS
_SLUG_ALIASES: dict[str, str] = {
    "nwdh": "northwest-heritage",
    "ppc": "p2p",
    "smithsonian": "si",
    "ms": "mississippi",
    "jhn": "jh3",
    "rw": "wisconsin",
    "in": "indiana",
    "oh": "ohio",
    "odn": "ohio",
    "mn": "minnesota",
    "idhh": "il",
    "hh": "heartland",
    "ga": "georgia",
    "dlg": "georgia",
    "fl": "florida",
    "ssdn": "florida",
    "ma": "bpl",
    "mass": "bpl",
}

# Reverse lookup: lowercase hub display name → canonical slug
_SLUG_BY_HUB_NAME: dict[str, str] = {
    name.lower(): slug for slug, name in PARTNER_HUBS.items()
}


def resolve_slug(slug: str) -> str | None:
    """Return canonical partner slug, or None if not recognised."""
    s = slug.strip().lower()
    if s in PARTNER_HUBS:
        return s
    alias = _SLUG_ALIASES.get(s)
    if alias is not None:
        return alias
    return _SLUG_BY_HUB_NAME.get(s)


def _get_institutions(timeout: int = 5) -> dict:
    """Fetch institutions_v2.json from GitHub, caching after the first call."""
    global _institutions_cache
    if _institutions_cache is None:
        with urllib.request.urlopen(INSTITUTIONS_URL, timeout=timeout) as resp:
            _institutions_cache = json.loads(resp.read())
    return _institutions_cache


def is_upload_eligible(canonical_slug: str, timeout: int = 5) -> bool:
    """Return True if institutions_v2.json marks this hub (or any child) upload=True."""
    hub_name = PARTNER_HUBS.get(canonical_slug)
    if not hub_name:
        return False
    hub = _get_institutions(timeout).get(hub_name, {})
    return hub.get("upload", False) or any(
        inst.get("upload", False) for inst in hub.get("institutions", {}).values()
    )


def is_wikidata_id(s: str) -> bool:
    """Return True if s is a Wikidata QID (e.g. 'Q12345')."""
    return bool(_QID_RE.match(s))


def canonical_matches_session_component(
    canonical: str, component: str, timeout: int = 5
) -> bool:
    """Return True if a tmux session component represents the given canonical hub.

    Components are either the canonical slug (full-hub sessions) or a
    hyphenated-lowercase institution name (institution-level sessions).
    """
    if component == canonical:
        return True
    hub_name = PARTNER_HUBS.get(canonical)
    if not hub_name:
        return False
    hub_data = _get_institutions(timeout).get(hub_name, {})
    return any(
        inst.lower().replace(" ", "-") == component
        for inst in hub_data.get("institutions", {})
    )


def resolve_wikidata_id(qid: str, timeout: int = 5) -> list[tuple[str, str | None]]:
    """Return (canonical_slug, institution_or_None) pairs matching a Wikidata QID.

    Searches hub-level and institution-level Wikidata fields in institutions_v2.json.
    Returns an empty list if no match. Multiple matches are possible when the same
    QID appears under different hubs or institutions in the JSON.
    """
    institutions = _get_institutions(timeout)
    results: list[tuple[str, str | None]] = []
    for hub_name, hub_data in institutions.items():
        canonical = _SLUG_BY_HUB_NAME.get(hub_name.lower())
        if canonical is None:
            continue
        if hub_data.get("Wikidata") == qid:
            results.append((canonical, None))
        else:
            for inst_name, inst_data in hub_data.get("institutions", {}).items():
                if inst_data.get("Wikidata") == qid:
                    results.append((canonical, inst_name))
    return results
