import logging
import mimetypes
import re
import typing
from collections import Counter
from string import Template

import pywikibot
from botocore.exceptions import ClientError
from pywikibot import FilePage

from pywikibot.site import APISite, BaseSite

from pywikibot.tools.chars import replace_invisible


from .common import CHECKSUM, get_list, get_str, get_dict
from .s3 import S3_BUCKET, S3Client
from .dpla import (
    WIKIDATA_FIELD_NAME,
    EDM_RIGHTS_FIELD_NAME,
    SOURCE_RESOURCE_FIELD_NAME,
    DC_CREATOR_FIELD_NAME,
    DC_TITLE_FIELD_NAME,
    DC_DESCRIPTION_FIELD_NAME,
    DC_DATE_FIELD_NAME,
    EDM_TIMESPAN_DISPLAY_DATE,
    EDM_IS_SHOWN_AT,
    DC_IDENTIFIER_FIELD_NAME,
    DC_LANGUAGE_FIELD_NAME,
)


def get_permissions_template(rights_uri: str) -> str:
    """Looks up the right wikitext template call for the rights_uri."""
    if rights_uri.startswith(RS_NKC_URL_BASE):
        return RS_NKC_TEMPLATE
    if rights_uri.startswith(RS_NOC_URL_BASE):
        return NOC_US_TEMPLATE
    if rights_uri.startswith(CC_PD_URL_BASE):
        return PD_US_TEMPLATE
    if rights_uri.startswith(CC_ZERO_URL_BASE):
        return CC_ZERO_TEMPLATE
    if rights_uri.startswith(CC_BY_URL_BASE):
        return license_to_markup_code(rights_uri)
    if rights_uri.startswith(CC_BY_SA_URL_BASE):
        return license_to_markup_code(rights_uri)
    return ""


def check_content_type(content_type: str) -> bool:
    return content_type not in INVALID_CONTENT_TYPES


def is_download_only(content_type: str) -> bool:
    """Returns True for types staged to S3 but not uploaded to Commons (e.g. video)."""
    return content_type in DOWNLOAD_ONLY_CONTENT_TYPES


def _break_query_string_pattern(title: str) -> str:
    """Break the `&...=` query-string blacklist pattern only when both chars
    are present.

    The bot's older "always replace `&` with `+` and `=` with `-`" behavior
    was over-eager: titles containing only one of the two are not rejected
    by Commons (the blacklist rule the lesson cites needs both, in order,
    to look like a URL query string). Over-replacing forced drift-correction
    renames of perfectly good Commons titles like
    `... suffragiis & orationibus. - DPLA - …` into the uglier
    `... suffragiis + orationibus. - DPLA - …` form.

    Only when both characters appear in the same title do we substitute, so
    legitimate use of either character alone is preserved.
    """
    if "&" in title and "=" in title:
        return title.replace("&", "+").replace("=", "-")
    return title


def get_page_title(
    item_title: str, dpla_identifier: str, suffix: str, page=None
) -> str:
    """Build a Commons file title from a DPLA item title + identifier.

    Applies the same character normalisations Commons enforces at upload
    time so that the title returned here matches what Commons stores once
    the upload completes.  Equality of constructed vs. stored title is
    load-bearing for every downstream check — skip-if-already-there,
    hash-drift detection, expected_item_titles sibling protection,
    orphan probes, tag-as-duplicate targets, SDC sidecar bookkeeping —
    so any character Commons normalises but we don't would silently
    bypass those checks (see PR #261's audit).
    """
    escaped_title = (
        # MediaWiki's `stripIllegalFilenameChars` strips `:` from File-
        # namespace titles unconditionally (filesystem path-separator
        # concern), replacing with `-`.  Apply the same rule here.  An
        # earlier version of this code only broke a leading
        # `<namespace>:` prefix and left mid-title colons intact on the
        # assumption that Commons "accepts mid-title colons fine".  That
        # was wrong: titles like `Delegation: "Wooden Lance"` are
        # silently stored as `Delegation- "Wooden Lance"`.  The mismatch
        # broke every downstream title-equality check for items whose
        # source title contained `:` — 5 NARA items in May 2026 ended
        # up as orphan duplicates because the uploader treated the
        # colon-form title as the canonical one while Commons stored
        # the dash form.
        _break_query_string_pattern(item_title[:181])
        .replace(":", "-")
        # `/` must also be substituted.  An earlier reading of MediaWiki's
        # rules (PR #223) removed this substitution because
        # `action=query&titles=File:Test/Sub.jpg` returned "missing"
        # rather than "invalid", which was taken as proof that Commons
        # accepts `/` in file titles.  That reading conflated two
        # different layers: `action=query` exercises the title PARSER
        # (which accepts most characters including `/`), whereas
        # UPLOAD/MOVE operations run `UploadBase::isValidName` →
        # `Title::makeTitleSafe(NS_FILE, …)` which DOES reject titles
        # whose `/` triggers subpage parsing.  4,114 NARA items in the
        # May 2026 Nixon/LBJ upload sessions hit
        # `imageinvalidfilename` during Case 3 drift-correction moves
        # because the move target contained `/` from a date like
        # `1/5/1966`.  Substituting up front matches what Commons
        # actually stores (the older code's `1-5-1966` form, as visible
        # in 2025-vintage Commons titles) and eliminates the cascade.
        .replace("/", "-")
        # MediaWiki's `stripIllegalFilenameChars` also strips `\` and
        # any character outside Title::legalChars().  Of those, `<` and
        # `>` are the only ones the existing replace-chain DIDN'T
        # already cover.  Mirror the strip here so our constructed
        # title matches what Commons stores — same class of fix as the
        # `:` and `/` rules, audited together.
        .replace("\\", "-")  # MediaWiki: stripped from file titles
        .replace("<", "-")  # MediaWiki: not in Title::legalChars()
        .replace(">", "-")  # MediaWiki: not in Title::legalChars()
        .replace("''", '"')  # titleblacklist: double-apostrophe rule → double-quote
        .replace("[", "(")  # MediaWiki: forbidden in page names
        .replace("]", ")")  # MediaWiki: forbidden in page names
        .replace("{", "(")  # MediaWiki: forbidden in page names
        .replace("}", ")")  # MediaWiki: forbidden in page names
        .replace("#", "-")  # MediaWiki: forbidden (URL fragment separator)
        .replace(
            "|", "-"
        )  # wikitext table/link syntax; breaks Commons extension detection
        .replace(
            "\ufffd", "\u2019"
        )  # Unicode replacement char → right single quote (corrupted metadata)
    )

    escaped_visible_title = replace_invisible(escaped_title)

    # Add pagination to page title if needed
    if page:
        return (
            f"{escaped_visible_title} - DPLA - {dpla_identifier} (page {page}){suffix}"
        )
    else:
        return f"{escaped_visible_title} - DPLA - {dpla_identifier}{suffix}"


def compute_ordinal_exts_and_page_labels(
    s3_client: S3Client,
    dpla_id: str,
    partner: str,
    num_files: int,
) -> tuple[dict[int, str], dict[int, str]]:
    """Compute per-ordinal extension and page-label for a multi-file DPLA item.

    Mirrors the uploader's pre-scan + per-extension counter logic so any code
    that needs to predict the Commons title an S3 ordinal will land at can
    reconstruct it without duplicating the (subtle) accounting. Producer
    (uploader) and consumer (verifier) MUST share this helper so they never
    diverge — see lessons.md "Don't normalize platform-dependent values on
    one side of a producer/consumer pair".

    Returns:
        ordinal_exts: {ordinal: extension} for ordinals the uploader has
            classified during pre-scan.
              - real ext (e.g. ".jpg") means a normal uploadable file.
              - "" means a stub or octet-stream placeholder; process_file
                may still upload it after content-type re-detection but
                without a (page N) suffix.
              - ordinals absent from the dict are download-only files
                (e.g. videos) — staged to S3 but never uploaded.
        page_labels: {ordinal: page_label_string} for every ordinal in
            1..num_files. Pass to get_page_title(page=...). Empty string
            means no (page N) suffix on the Commons title.
    """
    ordinal_exts: dict[int, str] = {}
    if num_files > 1:
        for i in range(1, num_files + 1):
            s3_path = s3_client.get_media_s3_path(dpla_id, i, partner)
            try:
                s3_obj = s3_client.get_s3().Object(S3_BUCKET, s3_path)
                mime = s3_obj.content_type
            except ClientError as e:
                # Only treat "object not found" as a stub placeholder.
                # Anything else (AccessDenied, InternalError, throttling) must
                # surface so we never silently corrupt the page-label
                # assignment on a transient S3 failure.
                if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
                    ordinal_exts[i] = ""
                    continue
                raise
            if is_download_only(mime):
                continue
            if mime in ("application/octet-stream", "binary/octet-stream"):
                ordinal_exts[i] = ""
                continue
            ext = mimetypes.guess_extension(mime)
            ordinal_exts[i] = ext if ext and ext != MIME_UNKNOWN_EXT else ""

    ext_counts: Counter[str] = Counter(ordinal_exts.values())
    ext_seen: Counter[str] = Counter()
    page_labels: dict[int, str] = {}
    for ordinal in range(1, num_files + 1):
        ext = ordinal_exts.get(ordinal, "")
        if ext_counts[ext] > 1:
            ext_seen[ext] += 1
            page_labels[ordinal] = str(ext_seen[ext])
        else:
            page_labels[ordinal] = ""

    return ordinal_exts, page_labels


def collect_duplicate_source_sha1s(
    s3_client: S3Client,
    dpla_id: str,
    partner: str,
    num_files: int,
) -> set[str]:
    """Return SHA1s that appear at TWO OR MORE positions in this item's S3
    asset list.

    A SHA1 in this set means the source data has the same file content
    listed at multiple ordinals — both/all positions are legitimate per
    the source and should land at their own Commons titles.  Without
    this knowledge, _resolve_hash_drift would see SHA1 X already at
    (page A), be asked to upload it to (page B), and conclude that
    (page A) is drift to be moved over (page B).  That's wrong: both
    positions should exist as separate Commons pages.

    SHA1 metadata read failures (transient S3 errors, stub ordinals)
    are tolerated — those ordinals are simply absent from the count,
    which conservatively keeps the SHA1 out of the duplicate set.  We
    DO log the skipped ordinal: under-counting silently would cause
    _resolve_hash_drift to incorrectly treat a legitimate sibling as
    drift and move/redirect it, so visibility matters for diagnosis.
    """
    counts: Counter[str] = Counter()
    for i in range(1, num_files + 1):
        s3_path = s3_client.get_media_s3_path(dpla_id, i, partner)
        try:
            s3_obj = s3_client.get_s3().Object(S3_BUCKET, s3_path)
            sha1 = (s3_obj.metadata or {}).get(CHECKSUM)
        except Exception as ex:
            logging.warning(
                f"collect_duplicate_source_sha1s: skipped ordinal {i} for "
                f"{dpla_id} (path={s3_path}): {ex}; duplicate detection may "
                f"under-count this item"
            )
            continue
        if sha1:
            counts[sha1] += 1
    return {sha1 for sha1, n in counts.items() if n > 1}


def license_to_markup_code(rights_uri: str) -> str:
    match_result = re.match(CC_URL_REGEX, rights_uri)
    if not match_result:
        return ""
    else:
        port = match_result.group(1).replace("/", "-")[:-1]
        return f"Cc-{port}"


def get_permissions(
    rights_uri: str, permissions_template_name: str, data_provider_wiki_q: str
) -> str:
    """Builds the wikitext for the commons item permissions."""
    if rights_uri.startswith(RIGHTS_STATEMENTS_URL_BASE):
        return f"{permissions_template_name} | {data_provider_wiki_q}"
    else:
        return permissions_template_name


def escape_wiki_strings(unescaped_string: str) -> str:
    """Removes specific character sequences from string to be safe for wikitext."""
    for reserved_string in RESERVED_WIKITEXT_STRINGS:
        unescaped_string = unescaped_string.replace(reserved_string, "")
    return unescaped_string


def join(strs: list[str]) -> str:
    """Convenience method for joining lists of strings."""
    return VALUE_JOIN_DELIMITER.join(strs)


def extract_strings(data: dict, field_name: str) -> str:
    """Convenience method for building a string
    out of escaped strings from a dict field"""
    return join([escape_wiki_strings(value) for value in get_list(data, field_name)])


def extract_strings_dict(data: dict, field_name1: str, field_name2: str) -> str:
    """Convenience method for building a string
    out of escaped strings from a dict field from an inner dict"""
    return join(
        [
            escape_wiki_strings(get_str(value, field_name2))
            for value in get_list(data, field_name1)
        ]
    )


# Commons-style language wrappers use ISO 639-1 codes (``{{es|...}}``,
# not ``{{spa|...}}``). DPLA's ``sourceResource.language.name`` is the
# English language name; this map covers the languages most commonly
# present in DPLA records' language facets. Anything outside the map is
# silently ignored — its values just stay strip-ineligible (the file's
# wikitext keeps the wrapper), which is the safe default.
_LANGUAGE_NAME_TO_ISO_639_1 = {
    "english": "en",
    "spanish": "es",
    "french": "fr",
    "german": "de",
    "italian": "it",
    "portuguese": "pt",
    "dutch": "nl",
    "russian": "ru",
    "polish": "pl",
    "swedish": "sv",
    "norwegian": "no",
    "danish": "da",
    "finnish": "fi",
    "czech": "cs",
    "hungarian": "hu",
    "greek": "el",
    "turkish": "tr",
    "arabic": "ar",
    "hebrew": "he",
    "chinese": "zh",
    "japanese": "ja",
    "korean": "ko",
    "vietnamese": "vi",
    "thai": "th",
    "hindi": "hi",
    "latin": "la",
    "welsh": "cy",
    "irish": "ga",
    "ukrainian": "uk",
    "romanian": "ro",
    "bulgarian": "bg",
    "serbian": "sr",
    "croatian": "hr",
    "slovak": "sk",
    "slovenian": "sl",
    "lithuanian": "lt",
    "latvian": "lv",
    "estonian": "et",
    "icelandic": "is",
    "catalan": "ca",
}


def _extract_unwrap_languages(item_metadata: dict) -> set[str]:
    """Return the ISO 639-1 language codes the comparator may safely
    unwrap for this item.

    Always includes ``en`` (the canonical wikitext is English by
    convention, and the legacy uploader emitted English strings even
    for non-English-language items). Any additional codes come from
    the item's ``sourceResource.language`` field — mapped from the
    DPLA-supplied English-name to its 639-1 code via
    :data:`_LANGUAGE_NAME_TO_ISO_639_1`.

    Resilient to all shapes ``sourceResource.language`` can take in
    practice — a missing field, a single dict, a list of dicts, or any
    of those carrying ``name`` strings of mixed casing. The DPLA
    ``iso639_3`` field is *not* consulted because in practice it
    contains either the English language name or a 639-3 code
    depending on the hub's mapper, so it isn't a reliable source for
    639-1 codes; the ``name`` field is consistent.
    """
    languages: set[str] = {"en"}
    source_resource = get_dict(item_metadata, SOURCE_RESOURCE_FIELD_NAME)
    raw = source_resource.get(DC_LANGUAGE_FIELD_NAME)
    if raw is None:
        return languages
    entries = raw if isinstance(raw, list) else [raw]
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not isinstance(name, str):
            continue
        code = _LANGUAGE_NAME_TO_ISO_639_1.get(name.strip().casefold())
        if code:
            languages.add(code)
    return languages


def dpla_metadata_params(
    dpla_id: str, item_metadata: dict, provider: dict, data_provider: dict
) -> dict:
    """Compute the canonical `{{DPLA metadata}}` parameter dict for an item.

    Single source of truth for the values the uploader writes into the
    template wikitext. ``get_wiki_text`` formats this dict into the
    rendered template; ``wikitext_normalize`` reads the same dict and
    compares each value against what's already in the file's wikitext to
    decide which params are redundant against SDC (the parameter is then
    safe to strip because the SDC-backed render produces the same display).

    Both callers must derive their expectations from this helper so the
    two flows can never drift — the comparator side computing one value
    while the writer side emits another would silently fail to strip
    redundant params or, worse, strip params that don't actually match
    the rendered output.

    Every parameter is a flat string. The previous ``source`` and
    ``institution`` sub-template dicts have been collapsed into flat
    ``hub`` / ``institution`` (Q-IDs) / ``url`` / ``dpla_id`` /
    ``local_id`` scalars, and ``creator`` is now a plain creator-name
    string. Module:DPLA's yellow box reads each flat param directly
    via the same parametric helpers that drive the SDC-backed blue
    box; no nested ``{{DPLA|...}}`` / ``{{Institution|...}}`` /
    ``{{InFi|...}}`` sub-templates are emitted in the wikitext, which
    eliminates the table-syntax-in-cell rendering bug those sub-
    templates caused when their wikitext-table output landed inside
    Module:DPLA's HTML ``<td>``.

    The non-rendered ``languages`` key carries the per-item allowlist
    of ISO 639-1 codes the comparator may safely unwrap. Always
    contains ``en``, plus any DPLA-supplied ``sourceResource.language``
    entries the helper recognises. The writer side ignores this entry;
    only :mod:`ingest_wikimedia.wikitext_normalize` reads it.
    """
    data_provider_wiki_q = escape_wiki_strings(
        get_str(data_provider, WIKIDATA_FIELD_NAME)
    )
    provider_wiki_q = escape_wiki_strings(get_str(provider, WIKIDATA_FIELD_NAME))
    rights_uri = get_str(item_metadata, EDM_RIGHTS_FIELD_NAME)
    permissions = get_permissions(
        rights_uri, get_permissions_template(rights_uri), data_provider_wiki_q
    )
    # ``get_permissions_template`` returns the empty string for any
    # ``edm:rights`` URI not in its allowlist; ``get_permissions`` can
    # then surface either ``""`` or ``" | <qid>"`` (for unmapped
    # RIGHTS_STATEMENTS URLs that still pick up the data-provider
    # suffix). Wrapping either in ``{{...}}`` produces malformed wikitext
    # (``{{}}`` / ``{{ | Q...}}``), so render the param as empty in that
    # case — the row becomes ``| permission =`` instead of carrying a
    # broken template invocation. Pre-empts the same shape leaking into
    # the comparator side: ``_value_matches("", "")`` is trivially True,
    # so a file whose wikitext already carries a blank ``| permission =``
    # row stays consistent.
    source_resource = get_dict(item_metadata, SOURCE_RESOURCE_FIELD_NAME)
    permissions_clean = permissions.strip() if permissions else ""
    permission_value = f"{{{{{permissions_clean}}}}}" if permissions_clean else ""
    return {
        "title": extract_strings(source_resource, DC_TITLE_FIELD_NAME),
        "description": extract_strings(source_resource, DC_DESCRIPTION_FIELD_NAME),
        "date": extract_strings_dict(
            source_resource, DC_DATE_FIELD_NAME, EDM_TIMESPAN_DISPLAY_DATE
        ),
        "permission": permission_value,
        "creator": extract_strings(source_resource, DC_CREATOR_FIELD_NAME),
        # Flat source-row params — Module:DPLA's yellow box reads each
        # directly. ``institution`` and ``hub`` are Wikidata Q-IDs;
        # the institution Q-ID doubles as the Institution row driver
        # (Module:DPLA expands ``{{Institution|wikidata=<inst>}}`` on
        # its side, not in the wikitext).
        "hub": provider_wiki_q,
        "institution": data_provider_wiki_q,
        "url": escape_wiki_strings(get_str(item_metadata, EDM_IS_SHOWN_AT)),
        "dpla_id": dpla_id,
        "local_id": extract_strings(source_resource, DC_IDENTIFIER_FIELD_NAME),
        "languages": _extract_unwrap_languages(item_metadata),
    }


def get_wiki_text(
    dpla_id: str, item_metadata: dict, provider: dict, data_provider: dict
) -> str:
    """Turns DPLA item info into a wikitext document.

    Emits the flat-param ``{{DPLA metadata}}`` shape Module:DPLA
    expects post the dual-path rewrite: every value is a plain
    string (Q-ID, URL, or text), no nested sub-templates whose
    wikitext-table output would leak raw ``{|`` markup inside
    Module:DPLA's HTML ``<td>``. ``creator`` is conditionally
    emitted — the row is suppressed entirely when DPLA has no
    creator string to avoid a blank ``creator =`` row.
    """
    params = dpla_metadata_params(dpla_id, item_metadata, provider, data_provider)

    # Template literal is left-justified — the leading whitespace of
    # the Python source indentation would otherwise carry through into
    # the rendered wikitext. The wiki parser ignores leading whitespace
    # on template-param lines (so the previous indented form rendered
    # the same), but anything reading the page source — editors using
    # the wikitext editor, future scripts diffing the wikitext, the
    # ``wikitext_normalize`` comparator — sees the noise.
    if params["creator"]:
        creator_row = "\n| creator = $creator"
    else:
        creator_row = ""

    template_string = (
        "== {{int:filedesc}} ==\n"
        "{{DPLA metadata" + creator_row + "\n| title = $title"
        "\n| description = $description"
        "\n| date = $date_string"
        "\n| permission = $permission"
        "\n| hub = $hub"
        "\n| institution = $institution"
        "\n| url = $url"
        "\n| dpla_id = $dpla_id"
        "\n| local_id = $local_id"
        "\n}}"
    )

    return Template(template_string).substitute(
        title=params["title"],
        description=params["description"],
        date_string=params["date"],
        permission=params["permission"],
        creator=params["creator"],
        hub=params["hub"],
        institution=params["institution"],
        url=params["url"],
        dpla_id=params["dpla_id"],
        local_id=params["local_id"],
    )


def wikimedia_url(title: str) -> str:
    """Return the URL for the Wikimedia page"""
    return f"{COMMONS_URL_PREFIX}{title.replace(' ', '_')}"


def get_page(site: BaseSite, title: str) -> FilePage:
    """
    Get the pywikibot object representing the page on Commons.
    """
    try:
        return pywikibot.FilePage(site, title=title)
    except pywikibot.exceptions.InvalidTitleError as e:
        raise ValueError(f"Invalid title {title}: {str(e)}") from e
    except Exception as e:
        raise RuntimeError(f"Unable to create page {title}: {str(e)}") from e


def get_site() -> BaseSite:
    """Returns the Site object for Wikimedia Commons."""
    site = pywikibot.Site(COMMONS_SITE_NAME)
    site.login()
    return site


def get_wikidata_site() -> BaseSite:
    """Returns the Site object for Wikidata."""
    site = pywikibot.Site("wikidata", "wikidata")
    site.login()
    return site


COMMONSDELINKER_PAGE = "User:CommonsDelinker/commands/filemovers"
_COMMONSDELINKER_REASON = (
    "[[COM:FR|File renamed]]: [[COM:FR#FR4|Criterion 4]] "
    "(harmonize the names of a set of images)"
)

# MediaWiki's stored comment field is capped at 500 bytes
# (CommentStore::COMMENT_CHARACTER_LIMIT). Comments longer than this are
# truncated mid-string on save, which produces unreadable summaries like
# "...(DPLA ID [[dpla:093..." with the link unterminated.
MAX_COMMENT_BYTES = 500

# For a move, MediaWiki composes the stored comment as
#   "$user moved page [[File:$old]] to [[File:$new]]: $reason"
# Fixed-overhead pieces:
#   " moved page [[File:"  = 19
#   "]] to [[File:"        = 13
#   "]]: "                 = 4
# Total non-variable overhead, excluding the username and filenames: 36 bytes.
_MOVE_AUTO_PREFIX_OVERHEAD = 36


def build_title_drift_move_reason(
    old_filename: str, new_filename: str, dpla_id: str, username: str
) -> str:
    """Return the longest title-drift move reason that fits MediaWiki's
    500-byte comment limit, given the filenames and bot username that will
    be auto-prefixed.

    MediaWiki truncates the composed comment at MAX_COMMENT_BYTES bytes; for
    long filenames the default reason (which includes a [[dpla:...]]
    interwiki link) can push the comment over the limit and lose its closing
    brackets. We degrade by first dropping the link wrapper, then the
    descriptive text, keeping the DPLA ID visible at every step so the
    comment remains traceable.
    """
    prefix_len = (
        len(username.encode("utf-8"))
        + _MOVE_AUTO_PREFIX_OVERHEAD
        + len(old_filename.encode("utf-8"))
        + len(new_filename.encode("utf-8"))
    )
    budget = MAX_COMMENT_BYTES - prefix_len
    candidates = (
        f"Title drift correction: updating to current DPLA title "
        f"(DPLA ID [[dpla:{dpla_id}|{dpla_id}]])",
        f"Title drift correction: updating to current DPLA title (DPLA ID {dpla_id})",
        f"Title drift correction (DPLA ID {dpla_id})",
        f"Title drift correction ({dpla_id})",
        "Title drift correction",
    )
    for reason in candidates:
        if len(reason.encode("utf-8")) <= budget:
            return reason
    # Filenames so long that even the shortest reason won't fit — return
    # it anyway; MediaWiki will still truncate, but we've done our best.
    return candidates[-1]


def post_commonsdelinker_request(
    site: BaseSite, old_filename: str, new_filename: str
) -> None:
    """Append a universal-replace request to CommonsDelinker's filemovers page.

    Both filenames should be bare (without the 'File:' namespace prefix).
    Each call makes one edit, matching the one-request-per-edit convention
    used by other editors on that page.

    Uses the MediaWiki `appendtext` API parameter rather than the naive
    read-modify-write (`page.text = page.text + template; page.save()`).
    The naive form races itself on this page in particular: a single
    uploader run can post hundreds of requests in rapid succession, and
    the GET that pywikibot issues to populate `page.text` is load-balanced
    across MediaWiki replica databases that may be lagged behind the
    primary by the time we POST. The resulting basetimestamp is older
    than the primary's current revision (the one OUR previous successful
    edit just produced), and the primary rejects with `editconflict` —
    a self-inflicted conflict despite our bot being the only editor.

    `appendtext` sidesteps both halves of that race:

      * No GET — pywikibot skips reading `page.text`, so there is no
        ~230 KB-and-growing payload pulled into the process on every
        call (a major memory accumulator that contributed to a recent
        OOM kill of the uploader).
      * No basetimestamp — MediaWiki concatenates atomically on the
        primary; conflicting concurrent edits can no longer cause
        spurious rejections.
    """
    page = pywikibot.Page(site, COMMONSDELINKER_PAGE)
    template = (
        f"{{{{universal replace"
        f"|{old_filename}"
        f"|{new_filename}"
        f"|reason={_COMMONSDELINKER_REASON}}}}}"
    )
    summary = f"universal replace: [[File:{old_filename}]] → [[File:{new_filename}]]"
    # Leading newline so the new request lands on its own line regardless
    # of whether the existing page ends with one.
    site.editpage(page, summary=summary, minor=False, appendtext="\n" + template)


def wiki_file_exists(site: BaseSite, sha1: str) -> bool:
    """Calls the find by hash api on commons to see if the file already exists."""

    api_site = typing.cast(APISite, site)
    for _ in api_site.allimages(sha1=sha1):
        return True
    return False


def find_file_by_hash(
    site: BaseSite, sha1: str, preferred_title: str | None = None
) -> FilePage | None:
    """Return the Commons FilePage with the given SHA1, or None.

    If preferred_title is given and a file with that title (without namespace)
    shares the hash, it is returned immediately. Otherwise the first result
    returned by the API (alphabetical) is returned. This handles the rare case
    where multiple files share a SHA1 and we want the one at the correct title.
    """
    api_site = typing.cast(APISite, site)
    first: FilePage | None = None
    for img in api_site.allimages(sha1=sha1):
        if preferred_title and img.title(with_ns=False) == preferred_title:
            return img
        if first is None:
            first = img
    return first


_DPLA_ID_RE = re.compile(r"- DPLA - ([0-9a-f]{32})")
# Anchored to the DPLA filename suffix: "... - DPLA - <id> (page N)<.ext>$".
# Prevents false matches when "(page N)" appears in the descriptive title text
# (e.g. titles where source brackets were normalised to parens by get_page_title).
_PAGE_ORDINAL_RE = re.compile(r"- DPLA - [0-9a-f]{32} \(page (\d+)\)(?=\.[^.]+$)")


def extract_dpla_id_from_commons_title(title: str) -> str | None:
    """Extract the DPLA ID from a Commons filename, or None if not present."""
    m = _DPLA_ID_RE.search(title)
    return m.group(1) if m else None


def extract_page_ordinal_from_commons_title(title: str) -> int | None:
    """Extract the `(page N)` ordinal from a Commons filename.

    Returns None for single-page items (no page suffix) and for any title
    that doesn't follow the DPLA `... (page N)<ext>` format.
    """
    m = _PAGE_ORDINAL_RE.search(title)
    return int(m.group(1)) if m else None


def is_same_item_redirect_relic(
    intended_title: str, target_title: str, dpla_id: str
) -> bool:
    """True if a redirect from intended_title → target_title is a relic of a
    prior bad move within the same multi-page DPLA item.

    Both titles must carry the same DPLA ID matching the current item, and
    both must have parseable but different page ordinals. When this is True
    the uploader must NOT call _resolve_redirect_move — doing so would just
    shuffle content between two valid ordinals of the same item, and the
    same shuffle would be reversed when the iteration later reaches the
    other ordinal, producing an oscillation that destroys uploaded content.
    """
    intended_dpla_id = extract_dpla_id_from_commons_title(intended_title)
    target_dpla_id = extract_dpla_id_from_commons_title(target_title)
    if intended_dpla_id != dpla_id or target_dpla_id != dpla_id:
        return False
    intended_page = extract_page_ordinal_from_commons_title(intended_title)
    target_page = extract_page_ordinal_from_commons_title(target_title)
    return (
        intended_page is not None
        and target_page is not None
        and intended_page != target_page
    )


# Patterns for metadata we preserve from the original page wikitext when
# rewriting a file description after a title-drift move.
#
# - PD-USGov family: {{PD-USGov}}, {{PD-USGov-Military-Army}}, {{PD-USGov-NARA}},
#   with optional |params. Allowed name chars match Commons template-name rules.
# - Image extracted: {{Image extracted|<params>}} — links the file back to the
#   parent page it was extracted from. Anchored anywhere in the text (including
#   inside |other versions= of an {{Information}} template).
# - Category links: [[Category:Name]] or [[Category:Name|sort key]].
# - Assessment-class templates: status that Commons editors confer on a
#   file outside of its bare description — Media/Picture of the day on a
#   specific date, Featured/Quality/Valued image marks, or the bundled
#   {{Assessments}} wrapper. These represent real curatorial work and must
#   survive a metadata-rescue rewrite. Pattern is case-insensitive to handle
#   the casing variants Commons editors use in practice ({{media of the day}}
#   vs {{Media of the day}}). Optional `|params` covers the dated forms
#   like {{Media of the day|2023|2|18}}.
_PD_USGOV_RE = re.compile(r"\{\{PD-USGov(?:-[A-Za-z0-9_-]+)?(?:\|[^{}]*)?\}\}")
_IMAGE_EXTRACTED_RE = re.compile(r"\{\{Image extracted\|[^{}]*\}\}", re.IGNORECASE)
_CATEGORY_RE = re.compile(r"\[\[Category:[^\]\n]+\]\]")
_ASSESSMENT_TEMPLATE_RE = re.compile(
    r"\{\{\s*(?:"
    r"Media of the day"
    r"|Picture of the day"
    r"|Featured picture"
    r"|Quality image"
    r"|Valued image"
    r"|Assessments"
    r")\s*(?:\|[^{}]*)?\}\}",
    re.IGNORECASE,
)


def merge_preserved_wikitext(existing_text: str, new_wikitext: str) -> str:
    """Append preserved metadata from existing_text to new_wikitext.

    Used when the uploader rewrites a file description after a title-drift
    move or redirect-overwrite. The new {{DPLA metadata}} wikitext is
    authoritative for the file's description, but page-level metadata that
    pre-existed — PD-USGov license tags, Image-extracted parent links,
    category membership, and assessment-class templates — must survive the
    rewrite.

    Result order (matches Commons page-structure convention):
        1. new_wikitext (the freshly generated {{DPLA metadata}} block)
        2. preserved Assessment block (=={{Assessment}}== header + any
           {{Media of the day|...}}, {{Picture of the day|...}},
           {{Featured picture}}, {{Quality image}}, {{Valued image}},
           or {{Assessments|...}} templates from the original)
        3. preserved {{PD-USGov...}} templates (license, above categories)
        4. preserved {{Image extracted|...}} templates (above categories)
        5. preserved [[Category:...]] links

    The Assessment header is emitted unconditionally when any assessment
    template is preserved — Commons convention is that these templates
    live under that header for proper categorisation, and an MOTD-archive
    scraper expecting that wrapper would miss a bare template.

    Duplicates within each preserved group are collapsed.

    TODO (Goal 2 follow-up): the title-drift rescue currently
    overwrites the metadata template wholesale, discarding any
    community-contributed template params an editor added between
    the original upload and the rescue. The same provenance-aware
    migration logic from :mod:`ingest_wikimedia.legacy_artwork`
    would let us preserve community values as SDC imports first,
    then overwrite. Out of scope for the flat-shape uploader PR;
    flagged here so the integration point doesn't get lost.
    """
    parts: list[str] = [new_wikitext.rstrip()]
    assessment = list(dict.fromkeys(_ASSESSMENT_TEMPLATE_RE.findall(existing_text)))
    if assessment:
        parts.append("")
        parts.append("=={{Assessment}}==")
        parts.extend(assessment)
    for pattern in (_PD_USGOV_RE, _IMAGE_EXTRACTED_RE, _CATEGORY_RE):
        group = list(dict.fromkeys(pattern.findall(existing_text)))
        if group:
            parts.append("")
            parts.extend(group)
    return "\n".join(parts) + "\n"


# Matches a real {{Duplicate}} template invocation. `\s*` on both sides of
# `duplicate` accepts the whitespace/newlines wikitext allows inside the
# braces — `{{ Duplicate|…}}`, `{{Duplicate |…}}` and `{{Duplicate\n|…}}`
# are all valid invocations Commons editors do use. The trailing
# `(?:\||\}\})` is what prevents `{{DuplicateImageFinder|…}}` from
# false-matching: we only count it as a duplicate template when the next
# non-whitespace token is the parameter pipe or the template's closing
# braces. IGNORECASE handles both `Duplicate` and the `{{duplicate}}`
# variant some Commons editors use.
_DUPLICATE_TAG_RE = re.compile(r"\{\{\s*duplicate\s*(?:\||\}\})", re.IGNORECASE)


def tag_as_duplicate(
    site: BaseSite,
    file_page: FilePage,
    correct_filename: str,
    reason: str,
) -> None:
    """Prepend {{Duplicate}} to file_page, flagging it for speedy deletion.

    correct_filename should be bare (no 'File:' prefix).
    reason is the free-text reason shown in the template.

    Idempotent: if the page already carries a {{Duplicate}} (or
    {{duplicate}}) template, this is a no-op. Two distinct uploader code
    paths can both identify the same file as a duplicate of the same
    target — the per-asset hash-drift correction during upload, and the
    post-item trailing-orphan sweep that runs after the asset loop —
    and unconditionally prepending each time produces a stack of
    redundant tags on the page (see uploader regression where one file
    got tagged twice within three seconds with the same correct title).

    Uses the MediaWiki `prependtext` API parameter so the existing page
    text doesn't need to be fetched, modified, and re-saved. The
    idempotency check above does cost one GET to read `file_page.text`
    that the pure-prependtext path avoided, but skipping a redundant
    write is worth the single read.
    """
    if _DUPLICATE_TAG_RE.search(file_page.text or ""):
        logging.info(
            f"Skipping duplicate tag on [[File:{file_page.title(with_ns=False)}]] — "
            f"already tagged as duplicate."
        )
        return
    tag = f"{{{{Duplicate|{correct_filename}|{reason}}}}}"
    summary = f"Tagging as duplicate: correct title is [[File:{correct_filename}]]"
    # Trailing newline so the tag sits on its own line above whatever
    # wikitext currently starts the page.
    site.editpage(file_page, summary=summary, minor=False, prependtext=tag + "\n")


INVALID_CONTENT_TYPES = frozenset(
    [
        "text/html",
        "application/json",
        "application/xml",
        "text/xml",
        "text/plain",
        "application/msword",
        "application/octet-stream",
        # Rich-text and Office XML formats — not accepted by Commons
        "application/rtf",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/vnd.ms-excel",
        "application/vnd.ms-powerpoint",
    ]
)
# Video/audio types downloaded to S3 for future conversion but never uploaded directly.
# Commons does not accept these container formats; .ogv/.webm conversion is needed first.
DOWNLOAD_ONLY_CONTENT_TYPES = frozenset(
    [
        "video/mp4",
        "video/x-msvideo",  # .avi
        "video/quicktime",  # .mov
        "audio/x-ms-wma",  # .wma
        "video/x-ms-wmv",  # .wmv
    ]
)
# mimetypes.guess_extension() returns this sentinel for MIME types it cannot map to a
# known file extension. Commons rejects files with this extension unconditionally.
MIME_UNKNOWN_EXT = ".bin"
COMMONS_URL_PREFIX = "https://commons.wikimedia.org/wiki/File:"
ERROR_FILEEXISTS = "fileexists-shared-forbidden"
ERROR_MIME = "filetype-badmime"
ERROR_BANNED = "filetype-banned"
ERROR_DUPLICATE = "duplicate"
ERROR_NOCHANGE = "no-change"
ERROR_BACKEND_FAIL = "backend-fail-internal"
COMMONS_SITE_NAME = "commons"
WMC_UPLOAD_CHUNK_SIZE = 20_000_000  # 20 MB
VALUE_JOIN_DELIMITER = "; "
RESERVED_WIKITEXT_STRINGS = ["|", "=", "[[", "]]", "{{", "}}", "''"]
IGNORE_WIKIMEDIA_WARNINGS = [
    # Target filename has a bad prefix {msg}.
    "bad-prefix",
    # Target filename is invalid.
    "badfilename",
    # The file is a duplicate of a deleted file {msg}.
    "duplicate-archive",
    # The upload is an exact duplicate of older version(s) of this file
    "duplicate-version",
    # File {msg} is empty.
    "empty-file",
    # File [Page] {msg} already exists
    "exists",
    # File exists with different extension as {msg}.
    "exists-normalized",
    # File {msg} type is unwanted type.
    "filetype-unwanted-type",
    # Target filename exists but with a different file {msg}
    "page-exists",
    # The file {msg} was previously deleted.
    "was-deleted",
    # Not ignored:
    # Uploaded file is a duplicate of {msg}
    # 'duplicate',
    # The upload is an exact duplicate of the current version  of this file
    # 'no-change',
]
FIND_BY_HASH_URL_PREFIX: str = (
    "https://commons.wikimedia.org/w/api.php?action=query&format=json"
    "&list=allimages&aisha1="
)
FIND_BY_HASH_QUERY_FIELD_NAME = "query"
FIND_BY_HASH_ALLIMAGES_FIELD_NAME = "allimages"

RIGHTS_STATEMENTS_URL_BASE = "http://rightsstatements.org"
RS_NKC_URL_BASE = RIGHTS_STATEMENTS_URL_BASE + "/vocab/NKC/"
RS_NOC_URL_BASE = RIGHTS_STATEMENTS_URL_BASE + "/vocab/NoC-US/"
CC_URL_BASE = "http://creativecommons.org"
CC_PD_URL_BASE = CC_URL_BASE + "/publicdomain/mark/"
CC_ZERO_URL_BASE = CC_URL_BASE + "/publicdomain/zero/"
CC_BY_URL_BASE = CC_URL_BASE + "/licenses/by/"
CC_BY_SA_URL_BASE = CC_URL_BASE + "/licenses/by-sa/"
CC_ZERO_TEMPLATE = "cc-zero"
RS_NKC_TEMPLATE = "NKC"
NOC_US_TEMPLATE = "NoC-US"
PD_US_TEMPLATE = "PD-US"
CC_URL_REGEX = "^http://creativecommons.org/licenses/(.*)"
