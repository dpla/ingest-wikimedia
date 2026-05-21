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


from .common import get_list, get_str, get_dict
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


def get_page_title(
    item_title: str, dpla_identifier: str, suffix: str, page=None
) -> str:
    """
    Makes a proper Wikimedia page title from the DPLA identifier and
    the title of the image.
    """
    escaped_title = (
        item_title[:181]
        .replace("''", '"')  # titleblacklist: double-apostrophe rule → double-quote
        .replace("&", "+")  # titleblacklist: query-string pattern (&...=)
        .replace("=", "-")  # titleblacklist: query-string pattern (&...=)
        .replace("[", "(")
        .replace("]", ")")
        .replace("{", "(")
        .replace("}", ")")
        .replace("/", "-")
        .replace(":", "-")
        .replace("#", "-")
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


def get_wiki_text(
    dpla_id: str, item_metadata: dict, provider: dict, data_provider: dict
) -> str:
    """Turns DPLA item info into a wikitext document."""
    data_provider_wiki_q = escape_wiki_strings(
        get_str(data_provider, WIKIDATA_FIELD_NAME)
    )
    provider_wiki_q = escape_wiki_strings(get_str(provider, WIKIDATA_FIELD_NAME))
    rights_uri = get_str(item_metadata, EDM_RIGHTS_FIELD_NAME)
    permissions = get_permissions(
        rights_uri, get_permissions_template(rights_uri), data_provider_wiki_q
    )
    source_resource = get_dict(item_metadata, SOURCE_RESOURCE_FIELD_NAME)
    creator_string = extract_strings(source_resource, DC_CREATOR_FIELD_NAME)
    title_string = extract_strings(source_resource, DC_TITLE_FIELD_NAME)
    description_string = extract_strings(source_resource, DC_DESCRIPTION_FIELD_NAME)
    date_string = extract_strings_dict(
        source_resource, DC_DATE_FIELD_NAME, EDM_TIMESPAN_DISPLAY_DATE
    )
    is_shown_at = escape_wiki_strings(get_str(item_metadata, EDM_IS_SHOWN_AT))
    local_id = extract_strings(source_resource, DC_IDENTIFIER_FIELD_NAME)

    if creator_string:
        creator_template = """
        | Other fields 1 = {{ InFi | Creator | $creator | id=fileinfotpl_aut}}"""
    else:
        creator_template = ""

    template_string = (
        """== {{int:filedesc}} ==
     {{ Artwork"""
        + creator_template
        + """
        | title = $title
        | description = $description
        | date = $date_string
        | permission = {{$permissions}}
        | source = {{ DPLA
            | $data_provider
            | hub = $provider
            | url = $is_shown_at
            | dpla_id = $dpla_id
            | local_id = $local_id
        }}
        | Institution = {{ Institution | wikidata = $data_provider }}
     }}"""
    )

    return Template(template_string).substitute(
        creator=creator_string,
        title=title_string,
        description=description_string,
        date_string=date_string,
        permissions=permissions,
        data_provider=data_provider_wiki_q,
        provider=provider_wiki_q,
        is_shown_at=is_shown_at,
        dpla_id=dpla_id,
        local_id=local_id,
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


def post_commonsdelinker_request(
    site: BaseSite, old_filename: str, new_filename: str
) -> None:
    """Append a universal-replace request to CommonsDelinker's filemovers page.

    Both filenames should be bare (without the 'File:' namespace prefix).
    Each call makes one edit, matching the one-request-per-edit convention
    used by other editors on that page.
    """
    page = pywikibot.Page(site, COMMONSDELINKER_PAGE)
    template = (
        f"{{{{universal replace"
        f"|{old_filename}"
        f"|{new_filename}"
        f"|reason={_COMMONSDELINKER_REASON}}}}}"
    )
    summary = f"universal replace: [[File:{old_filename}]] → [[File:{new_filename}]]"
    page.text = page.text.rstrip("\n") + "\n" + template
    page.save(summary=summary, minor=False)


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
_PD_USGOV_RE = re.compile(r"\{\{PD-USGov(?:-[A-Za-z0-9_-]+)?(?:\|[^{}]*)?\}\}")
_IMAGE_EXTRACTED_RE = re.compile(r"\{\{Image extracted\|[^{}]*\}\}", re.IGNORECASE)
_CATEGORY_RE = re.compile(r"\[\[Category:[^\]\n]+\]\]")


def merge_preserved_wikitext(existing_text: str, new_artwork: str) -> str:
    """Append preserved metadata from existing_text to new_artwork.

    Used when the uploader rewrites a file description after a title-drift
    move or redirect-overwrite. The new {{Artwork}} wikitext is authoritative
    for the file's description, but page-level metadata that pre-existed —
    PD-USGov license tags, Image-extracted parent links, and category
    membership — must survive the rewrite.

    Result order:
        1. new_artwork (the freshly generated {{Artwork}} block)
        2. preserved {{PD-USGov...}} templates (license, above categories)
        3. preserved {{Image extracted|...}} templates (above categories)
        4. preserved [[Category:...]] links

    Duplicates within each preserved group are collapsed.
    """
    parts: list[str] = [new_artwork.rstrip()]
    for pattern in (_PD_USGOV_RE, _IMAGE_EXTRACTED_RE, _CATEGORY_RE):
        group = list(dict.fromkeys(pattern.findall(existing_text)))
        if group:
            parts.append("")
            parts.extend(group)
    return "\n".join(parts) + "\n"


def tag_as_duplicate(
    site: BaseSite,
    file_page: FilePage,
    correct_filename: str,
    reason: str,
) -> None:
    """Prepend {{Duplicate}} to file_page, flagging it for speedy deletion.

    correct_filename should be bare (no 'File:' prefix).
    reason is the free-text reason shown in the template.
    """
    tag = f"{{{{Duplicate|{correct_filename}|{reason}}}}}"
    summary = f"Tagging as duplicate: correct title is [[File:{correct_filename}]]"
    file_page.text = tag + "\n" + file_page.text
    file_page.save(summary=summary, minor=False)


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
