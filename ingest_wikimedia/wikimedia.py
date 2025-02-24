import re
from string import Template

import pywikibot
from pywikibot import FilePage

from pywikibot.tools.chars import replace_invisible

from .common import get_list, get_str, get_dict
from .metadata import (
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
from .web import get_http_session


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


def get_page_title(
    item_title: str, dpla_identifier: str, suffix: str, page=None
) -> str:
    """
    Makes a proper Wikimedia page title from the DPLA identifier and
    the title of the image.
    """
    escaped_title = (
        item_title[:181]
        .replace("[", "(")
        .replace("]", ")")
        .replace("{", "(")
        .replace("}", ")")
        .replace("/", "-")
        .replace(":", "-")
        .replace("#", "-")
    )

    escaped_visible_title = replace_invisible(escaped_title)

    # Add pagination to page title if needed
    if page:
        return (
            f"{escaped_visible_title} - DPLA - {dpla_identifier} (page {page}){suffix}"
        )
    return f"{escaped_visible_title} - DPLA - {dpla_identifier}{suffix}"


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


def get_page(site: pywikibot.Site, title: str) -> FilePage:
    """
    Get the pywikibot object representing the page on Commons.
    """
    try:
        return pywikibot.FilePage(site, title=title)
    except pywikibot.exceptions.InvalidTitleError as e:
        raise Exception(f"Invalid title {title}: {str(e)}") from e
    except Exception as e:
        raise Exception(f"Unable to create page {title}: {str(e)}") from e


def get_site() -> pywikibot.Site:
    """Returns the Site object for wikimedia commons."""
    site = pywikibot.Site(COMMONS_SITE_NAME)
    site.login()
    return site


def wiki_file_exists(sha1: str) -> bool:
    """Calls the find by hash api on commons to see if the file already exists."""
    response = get_http_session().get(FIND_BY_HASH_URL_PREFIX + sha1)
    sha1_response = response.json()
    if "error" in sha1_response:
        raise Exception(
            f"Received bad response from find by hash endpoint.\n{str(sha1_response)}"
        )

    all_images = get_list(
        get_dict(sha1_response, FIND_BY_HASH_QUERY_FIELD_NAME),
        FIND_BY_HASH_ALLIMAGES_FIELD_NAME,
    )
    return len(all_images) > 0


INVALID_CONTENT_TYPES = [
    "text/html",
    "application/json",
    "application/xml",
    "text/plain",
]
COMMONS_URL_PREFIX = "https://commons.wikimedia.org/wiki/File:"
ERROR_FILEEXISTS = "fileexists-shared-forbidden"
ERROR_MIME = "filetype-badmime"
ERROR_BANNED = "filetype-banned"
ERROR_DUPLICATE = "duplicate"
ERROR_NOCHANGE = "no-change"
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
