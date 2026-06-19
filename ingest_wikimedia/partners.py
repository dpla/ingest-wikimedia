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

# DPLA item ID: 32-character hex string (MD5 hash). Case-insensitive so that
# IDs pasted in uppercase or mixed-case are recognised correctly.
_DPLA_ID_RE = re.compile(r"^[0-9a-f]{32}$", re.IGNORECASE)

# Pattern that strips any character not allowed in a tmux-safe session-label
# slug.  Anything outside [a-z0-9-] is removed; whitespace is converted to
# hyphens first.  See slugify_session_label_component().
_SLUG_STRIP_RE = re.compile(r"[^a-z0-9-]")

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

# Partners whose EC2 directory name differs from their canonical slug.
PARTNER_DIR: dict[str, str] = {
    "si": "smithsonian",
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


def is_institution_upload_eligible(
    canonical_slug: str, institution_name: str, timeout: int = 5
) -> bool:
    """Return True if a specific institution is upload-eligible.

    An institution is eligible if its parent hub has upload=True (hub-level
    eligibility cascades to all child institutions), or if the institution
    itself has upload=True.
    """
    hub_name = PARTNER_HUBS.get(canonical_slug)
    if not hub_name:
        return False
    hub = _get_institutions(timeout).get(hub_name, {})
    if hub.get("upload", False):
        return True
    inst_data = hub.get("institutions", {}).get(institution_name, {})
    return inst_data.get("upload", False)


def is_item_upload_eligible(
    canonical_slug: str, institution_name: str, timeout: int = 5
) -> bool:
    """Return True if an item from this institution is fully eligible for upload.

    Mirrors the per-institution check performed by get-ids-es: requires that
    the hub has a Wikidata ID (needed for the hub-level Commons category), the
    institution has a Wikidata ID (needed for the institution-level Commons
    category), and either the hub or the institution has upload=True.
    """
    hub_name = PARTNER_HUBS.get(canonical_slug)
    if not hub_name:
        return False
    hub = _get_institutions(timeout).get(hub_name, {})
    if not hub.get("Wikidata", ""):
        return False
    inst_data = hub.get("institutions", {}).get(institution_name, {})
    if not inst_data.get("Wikidata", ""):
        return False
    return hub.get("upload", False) or inst_data.get("upload", False)


def slugify_session_label_component(name: str) -> str:
    """Normalize a display name to the tmux-safe slug used in session labels.

    Session-label components are lowercase alphanumeric + hyphens.  Whitespace
    becomes hyphens; everything else (apostrophes, ampersands, commas, slashes,
    accents, etc.) is stripped.

    Both wikimedia_launch (which produces session names) and wikimedia_kill
    (which matches them) MUST use this same function — otherwise an institution
    name like ``AT&T Archives`` would launch as ``att-archives`` but never
    match when ``kill`` derives its target slug from a different rule.
    """
    return _SLUG_STRIP_RE.sub("", name.lower().replace(" ", "-"))


def is_wikidata_id(s: str) -> bool:
    """Return True if s is a Wikidata QID (e.g. 'Q12345')."""
    return bool(_QID_RE.match(s))


def is_dpla_id(s: str) -> bool:
    """Return True if s is a 32-hex-char DPLA item ID."""
    return bool(_DPLA_ID_RE.match(s))


def parse_session_labels(suffix: str) -> list[str]:
    """Parse the part of a tmux session name after 'wikimedia-' into target labels.

    Session names concatenate target labels with '+' as separator, but institution-level
    labels themselves contain '+' (e.g. 'nara+herbert-hoover-library'). This function
    uses PARTNER_HUBS keys as hub boundaries to unambiguously reconstruct labels:
    a token that immediately follows a known hub slug and is not itself a known hub slug
    is treated as that hub's institution slug.

    Examples:
        'bpl'                              → ['bpl']
        'nara+herbert-hoover-library'      → ['nara+herbert-hoover-library']
        'bpl+nara+herbert-hoover-library'  → ['bpl', 'nara+herbert-hoover-library']
        'bpl+nara'                         → ['bpl', 'nara']
    """
    parts = suffix.split("+")
    labels: list[str] = []
    i = 0
    while i < len(parts):
        part = parts[i]
        if part in PARTNER_HUBS:
            # Consume all consecutive non-hub tokens as the institution suffix.
            # Institution names can contain '+' (e.g. "LGBTQ+ Archives"), producing
            # multiple tokens here. Greedily take everything up to the next hub slug.
            j = i + 1
            while j < len(parts) and parts[j] not in PARTNER_HUBS:
                j += 1
            if j > i + 1:
                labels.append("+".join(parts[i:j]))
                i = j
            else:
                labels.append(part)
                i += 1
        else:
            i += 1  # orphaned token (malformed session name); skip
    return labels


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

    Searches hub-level and institution-level Wikidata fields in
    institutions_v2.json. Returns an empty list if no match. Multiple
    matches are possible when the same QID appears under different
    hubs or institutions in the JSON.

    Filters out non-upload-eligible matches using the same rule as
    :func:`is_item_upload_eligible`:

    * Hub-level match (institution=None) is kept only if the hub
      itself has ``upload: True``.
    * Institution-level match is kept only if the hub OR the
      institution has ``upload: True``.

    Without this filter, a QID like ``Q131454`` (Library of Congress)
    that appears in the JSON as an *institution* under many hubs
    (where it's listed but not opted in for upload) AND as a hub
    (the ``lc`` slug, which is also not upload-eligible) would route
    the launcher onto the broader ``lc`` hub scope and produce the
    misleading ``Skipped targets: 'lc'`` error. Filtering here keeps
    eligibility on the QID resolution side, where the data shape is
    available, instead of leaking that knowledge into every caller.
    """
    institutions = _get_institutions(timeout)
    results: list[tuple[str, str | None]] = []
    for hub_name, hub_data in institutions.items():
        canonical = _SLUG_BY_HUB_NAME.get(hub_name.lower())
        if canonical is None:
            continue
        hub_upload = bool(hub_data.get("upload"))
        if hub_data.get("Wikidata") == qid and hub_upload:
            results.append((canonical, None))
        # Don't ``continue`` on a hub-level QID match — if the same
        # QID is also used by an institution within this hub, that
        # match needs to be considered for institution-level
        # eligibility (hub.upload=false + inst.upload=true is a valid
        # combination).
        for inst_name, inst_data in hub_data.get("institutions", {}).items():
            if inst_data.get("Wikidata") != qid:
                continue
            if hub_upload or bool(inst_data.get("upload")):
                results.append((canonical, inst_name))
    return results
