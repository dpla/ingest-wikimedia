import json
import logging
import re
from operator import itemgetter
from typing import TypeVar, Callable
from urllib.parse import urlparse
from pathlib import Path

import validators

from .common import null_safe, get_str, get_list, get_dict
from .s3 import write_iiif_manifest
from .tracker import Tracker, Result
from .web import get_http_session, HTTP_REQUEST_HEADERS

__dpla_id_banlist: set[str] | None = None

T = TypeVar("T")

BANLIST_FILE_NAME = "dpla-id-banlist.txt"


def setup_dpla_id_banlist() -> None:
    """
    Loads a list of banned dpla ids from disk
    """
    global __dpla_id_banlist
    banlist_path = Path(__file__).parent.parent / BANLIST_FILE_NAME
    with open(banlist_path, "r") as file:
        __dpla_id_banlist = set([line.rstrip() for line in file])


def check_partner(partner: str) -> None:
    """
    Blows up if we're working on a partner we shouldn't.
    """
    if partner not in DPLA_PARTNERS.keys():
        raise ValueError("Unrecognized partner.")


def get_item_metadata(dpla_id: str, api_key: str) -> dict:
    """
    Retrieves a DPLA MAP record from the DPLA API for an item.
    """
    url = DPLA_API_URL_BASE + dpla_id
    headers = {AUTHORIZATION_HEADER: api_key}
    response = get_http_session().get(url, headers=headers)
    response.raise_for_status()
    response_json = response.json()
    docs = get_list(response_json, DPLA_API_DOCS)
    return docs[0] if docs else {}


def check_record_partner(partner: str, item_metadata: dict) -> bool:
    partner_long_name = DPLA_PARTNERS.get(partner, "")
    record_partner_long_name = get_str(
        get_dict(item_metadata, PROVIDER_FIELD_NAME), EDM_AGENT_NAME
    )
    return partner_long_name == record_partner_long_name


def is_wiki_eligible(
    dpla_id: str, item_metadata: dict, provider: dict, data_provider: dict
) -> bool:
    """
    Enforces a number of criteria for ensuring this is an item we should upload.
    """

    global __dpla_id_banlist
    if __dpla_id_banlist is None:
        setup_dpla_id_banlist()

    def value_ok(value: T, test: Callable[[T], bool], error_msg: str):
        result = test(value)
        if not result:
            logging.info(error_msg)
        return result

    def non_empty_str(x: str) -> bool:
        return x != ""

    def not_false(x: bool) -> bool:
        return x

    def rights_category_ok(x: str) -> bool:
        return x == UNLIMITED_RE_USE

    provider_wikidata_id_ok = value_ok(
        value=get_str(provider, WIKIDATA_FIELD_NAME),
        test=non_empty_str,
        error_msg="Missing wikidata id for provider.",
    )

    data_provider_wikidata_id_ok = value_ok(
        value=get_str(data_provider, WIKIDATA_FIELD_NAME),
        test=non_empty_str,
        error_msg="Missing wikidata id for dataProvider.",
    )

    wikidata_ids_ok = data_provider_wikidata_id_ok and provider_wikidata_id_ok

    provider_ok = value_ok(
        value=(
            null_safe(provider, UPLOAD_FIELD_NAME, False)
            or null_safe(data_provider, UPLOAD_FIELD_NAME, False)
        ),
        test=not_false,
        error_msg="Bad provider.",
    )

    rights_category_ok = value_ok(
        value=get_str(item_metadata, RIGHTS_CATEGORY_FIELD_NAME),
        test=rights_category_ok,
        error_msg="Bad rights category.",
    )

    is_shown_at = get_str(item_metadata, EDM_IS_SHOWN_AT)
    media_master = len(get_list(item_metadata, MEDIA_MASTER_FIELD_NAME)) > 0
    iiif_manifest = get_str(item_metadata, IIIF_MANIFEST_FIELD_NAME)

    if not iiif_manifest and not media_master:
        iiif_url = contentdm_iiif_url(is_shown_at)
        if iiif_url is not None:
            response = get_http_session().head(iiif_url, allow_redirects=True)
            if response.status_code < 400:
                item_metadata[IIIF_MANIFEST_FIELD_NAME] = iiif_url
                iiif_manifest = iiif_url

    asset_ok = value_ok(
        value=(media_master or iiif_manifest),
        test=not_false,
        error_msg="Bad asset.",
    )

    dpla_id_ok = value_ok(
        value=dpla_id not in __dpla_id_banlist,
        test=not_false,
        error_msg="DPLA ID in banlist.",
    )

    return (
        rights_category_ok
        and asset_ok
        and provider_ok
        and dpla_id_ok
        and wikidata_ids_ok
    )


def get_provider_and_data_provider(
    item_metadata: dict, providers_json: dict
) -> tuple[dict, dict]:
    """
    Loads metadata about the provider and data provider from the providers json file.
    """
    provider_name = get_str(
        get_dict(item_metadata, PROVIDER_FIELD_NAME), EDM_AGENT_NAME
    )
    data_provider_name = get_str(
        get_dict(item_metadata, DATA_PROVIDER_FIELD_NAME), EDM_AGENT_NAME
    )
    provider = get_dict(providers_json, provider_name)
    data_provider = get_dict(
        get_dict(provider, INSTITUTIONS_FIELD_NAME), data_provider_name
    )
    return provider, data_provider


def get_providers_data() -> dict:
    """Loads the institutions file from ingestion3 in GitHub."""
    return get_http_session().get(INSTITUTIONS_URL).json()


def provider_str(provider: dict) -> str:
    """
    Creates a human-readable string out of the provider record.
    """
    if provider is None:
        return "Provider: None"
    else:
        return (
            f"Provider: {provider.get(WIKIDATA_FIELD_NAME, '')}, "
            f"{provider.get(UPLOAD_FIELD_NAME, '')}"
        )


def extract_urls(
    partner,
    dpla_id,
    item_metadata: dict,
) -> list[str]:
    """
    Tries to find some way to get a list of file urls out of the item. Writes the IIIF
    manifest to S3 if there is one.
    """
    if MEDIA_MASTER_FIELD_NAME in item_metadata:
        return get_list(item_metadata, MEDIA_MASTER_FIELD_NAME)

    elif IIIF_MANIFEST_FIELD_NAME in item_metadata:
        manifest_url = get_str(item_metadata, IIIF_MANIFEST_FIELD_NAME)
        manifest = get_iiif_manifest(manifest_url)
        write_iiif_manifest(partner, dpla_id, json.dumps(manifest))
        return get_iiif_urls(manifest)

    else:
        Tracker().increment(Result.NO_MEDIA)
        raise NotImplementedError(
            f"No {MEDIA_MASTER_FIELD_NAME} or {IIIF_MANIFEST_FIELD_NAME}"
        )


def iiif_v2_urls(iiif: dict) -> list[str]:
    """
    Extracts image URLs from a v2 IIIF manifest and returns them as a list
    """
    urls = []
    sequences = get_list(iiif, IIIF_SEQUENCES)
    for sequence in sequences:
        canvases = get_list(sequence, IIIF_CANVASES)
        for canvas in canvases:
            for image in get_list(canvas, IIIF_IMAGES):
                resource = get_dict(image, IIIF_RESOURCE)
                service = get_dict(resource, IIIF_SERVICE)
                context = get_str(service, JSON_LD_AT_CONTEXT)
                url = get_str(service, JSON_LD_AT_ID)
                if url and context == IIIF_IMAGE_API_V2:
                    big_url = maximize_iiif_url(url, IIIF_V2_FULL_RES_JPG_SUFFIX)
                    urls.append(big_url)
                elif url:
                    # guessing the v3 syntax will work even if we don't know it's v3.
                    big_url = maximize_iiif_url(url, IIIF_V3_FULL_RES_JPG_SUFFIX)
                    urls.append(big_url)
                else:
                    # we give up, but need to hold space so the numbering is right
                    urls.append("")

    return urls


def iiif_v3_urls(iiif: dict) -> list[str]:
    """
    Extracts image URLs from a v3 IIIF manifest and returns them as a list
    Servers specify urls in multiple ways.
    """
    urls = []
    for item in get_list(iiif, IIIF_ITEMS):
        try:
            url = get_str(
                get_dict(item[IIIF_ITEMS][0][IIIF_ITEMS][0], IIIF_BODY), IIIF_ID
            )
            new_url = ""
            if url:
                # This might later need to sniff the profile level of the image API
                # like we did in iiif_v2_urls()
                new_url = maximize_iiif_url(url, IIIF_V3_FULL_RES_JPG_SUFFIX)
            # This always adds something to the list.
            # If we didn't get a URL, it's just an empty string.
            # This prevents getting the page order wrong if we don't
            # figure out the URL one time and fix it later.
            urls.append(new_url)

        except (IndexError, TypeError, KeyError) as e:
            logging.warning("Unable to parse IIIF manifest.", exc_info=e)
            Tracker().increment(Result.BAD_IIIF_MANIFEST)
            return []
    return urls


def maximize_iiif_url(url: str, suffix: str) -> str:
    """
    This attempts to get whatever putative IIIF Image API URL and convert it to a
    request for the largest payload the server will deliver. Many times, URLs are
    supplied with arbitrary dimensions as default, which would result in Commons
    uploads that aren't the full quality.

    Unfortunately, many IIIF Image API servers are not deployed per the spec, which
    requires that there only be one "prefix" path segment in the URL, max. So I'm
    very pedantically parsing the URLs with inflexible regexes that are tailored to
    each case of one, two or three prefixes. If someone pops up with a IIIF endpoint
    four layers deep, it'll require more regexes and so forth.

    Also, sometimes the URLs point to just the identifier rather than info.json or a
    full Image API request, so I'm handling that too.

    Someone more crafty than I am with regexes might be able to do a cleaner job of
    this, but this does work.

    The oss Python implementation of IIIF URL handling I ran into looked like it wasn't
    going to handle these cases, so I've had to DIY to get more partners in.
    """

    def no_prefix(inner_match: re.Match) -> str:
        scheme, server, identifier = itemgetter(
            SCHEME_GROUP,
            SERVER_GROUP,
            IDENTIFIER_GROUP,
        )(inner_match.groupdict())
        return f"{scheme}://{server}/{identifier}{suffix}"

    def one_prefix(inner_match: re.Match) -> str:
        scheme, server, identifier, prefix1 = itemgetter(
            SCHEME_GROUP, SERVER_GROUP, IDENTIFIER_GROUP, PREFIX1_GROUP
        )(inner_match.groupdict())
        return f"{scheme}://{server}/{prefix1}/{identifier}{suffix}"

    def two_prefixes(inner_match: re.Match) -> str:
        scheme, server, identifier, prefix1, prefix2 = itemgetter(
            SCHEME_GROUP, SERVER_GROUP, IDENTIFIER_GROUP, PREFIX1_GROUP, PREFIX2_GROUP
        )(inner_match.groupdict())
        return f"{scheme}://{server}/{prefix1}/{prefix2}/{identifier}{suffix}"

    def three_prefixes(inner_match: re.Match) -> str:
        scheme, server, identifier, prefix1, prefix2, prefix3 = itemgetter(
            SCHEME_GROUP,
            SERVER_GROUP,
            IDENTIFIER_GROUP,
            PREFIX1_GROUP,
            PREFIX2_GROUP,
            PREFIX3_GROUP,
        )(inner_match.groupdict())
        return f"{scheme}://{server}/{prefix1}/{prefix2}/{prefix3}/{identifier}{suffix}"

    if match := IMAGE_API_UP_THROUGH_IDENTIFIER_REGEX_NO_PREFIX.match(url):
        return no_prefix(match)

    elif match := IMAGE_API_UP_THROUGH_IDENTIFIER_W_PREFIX_REGEX.match(url):
        return one_prefix(match)

    elif match := IMAGE_API_UP_THROUGH_IDENTIFIER_W_DOUBLE_PREFIX_REGEX.match(url):
        return two_prefixes(match)

    elif match := IMAGE_API_UP_THROUGH_IDENTIFIER_W_TRIPLE_PREFIX_REGEX.match(url):
        return three_prefixes(match)

    elif match := FULL_IMAGE_API_URL_REGEX_NO_PREFIX.match(url):
        return no_prefix(match)

    elif match := FULL_IMAGE_API_URL_W_PREFIX_REGEX.match(url):
        return one_prefix(match)

    elif match := FULL_IMAGE_API_URL_W_DOUBLE_PREFIX_REGEX.match(url):
        return two_prefixes(match)

    elif match := FULL_IMAGE_API_URL_W_TRIPLE_PREFIX_REGEX.match(url):
        return three_prefixes(match)

    elif match := IMAGE_API_UP_THROUGH_IDENTIFIER_REGEX_ONE_PREFIX_ID_W_SLASHES.match(
        url
    ):
        return one_prefix(match)
    else:
        # try just whacking a max-res suffix on:
        test_url = url + suffix
        # test to make sure that has something at the end of it
        http_session = get_http_session()
        head_response = http_session.head(test_url)
        if (
            head_response.ok
            and head_response.headers[HEADER_CONTENT_TYPE] == CONTENT_TYPE_JPEG
        ):
            return test_url

    logging.warning(f"Couldn't maximize IIIF URL: {url}")
    Tracker().increment(Result.BAD_IMAGE_API)
    return ""  # we give up


def get_iiif_urls(manifest: dict) -> list[str]:
    """
    Extracts image URLs from IIIF manifest and returns them as a list
    Currently only supports IIIF v2 and v3
    """
    # v2 or v3?
    match manifest.get(JSON_LD_AT_CONTEXT, None):
        case None:
            raise ValueError("No IIIF version specified.")
        case x if x == IIIF_PRESENTATION_API_MANIFEST_V3:
            return iiif_v3_urls(manifest)
        case x if x == IIIF_PRESENTATION_API_MANIFEST_V2:
            return iiif_v2_urls(manifest)
        case x if type(x) is list and IIIF_PRESENTATION_API_MANIFEST_V3 in x:
            return iiif_v3_urls(manifest)
        case x if type(x) is list and IIIF_PRESENTATION_API_MANIFEST_V2 in x:
            return iiif_v2_urls(manifest)
        case x:
            raise ValueError(f"Unimplemented IIIF version: {x}")


def get_iiif_manifest(url: str) -> dict | None:
    """
    Gets the IIIF manifest from the given url.
    """
    if not validators.url(url):
        logging.warning(f"Invalid IIIF manifest url: {url}")
        return None

    try:
        request = get_http_session().get(url, headers=HTTP_REQUEST_HEADERS)
        request.raise_for_status()
        return request.json()

    except Exception:
        logging.info(f"Unable to read IIIF manifest at {url}")
        return None


def contentdm_iiif_url(is_shown_at: str) -> str | None:
    """
    Creates a IIIF presentation API manifest URL from the
    link to the object in ContentDM

    We want to go from
    https://www.ohiomemory.org/cdm/ref/collection/p16007coll33/id/126923
    to
    https://www.ohiomemory.org/iiif/info/p16007coll33/126923/manifest.json

    """
    parsed_url = urlparse(is_shown_at)
    match_result = re.match(CONTENT_DM_ISSHOWNAT_REGEX, parsed_url.path)
    if not match_result:
        return None
    else:
        return (
            parsed_url.scheme
            + "://"
            + parsed_url.netloc
            + CONTENTDM_IIIF_INFO
            + match_result.group(1)
            + "/"
            + match_result.group(2)
            + CONTENTDM_IIIF_MANIFEST_JSON
        )


SCHEME_GROUP = "scheme"
SCHEME_REGEX = r"^(?P<scheme>http|https)://"
SERVER_GROUP = "server"
SERVER_REGEX = r"(?P<server>[^/]+)/"

PREFIX1_GROUP = "prefix1"
PREFIX1_REGEX = r"(?P<prefix1>[^/]+)/"
PREFIX1_IIIF_REGEX = r"(?P<prefix1>iiif)/"

PREFIX2_GROUP = "prefix2"
PREFIX2_REGEX = r"(?P<prefix2>[^/]+)/"
PREFIX2_IIIF_REGEX = r"(?P<prefix2>iiif)/"

PREFIX3_GROUP = "prefix3"
PREFIX3_REGEX = r"(?P<prefix3>[^/]+)/"
PREFIX3_IIIF_REGEX = r"(?P<prefix3>iiif)/"

IDENTIFIER_GROUP = "identifier"
IDENTIFIER_REGEX_OPTIONAL_SLASH = r"(?P<identifier>[^/]+)/?"
IDENTIFIER_REGEX_REQUIRED_SLASH = r"(?P<identifier>[^/]+)/"
IDENTIFIER_REGEX_REST_OF_STRING = r"(?P<identifier>.*)$"
REGION_GROUP = "region"
REGION_REGEX = r"(?P<region>[^/]+)/"
SIZE_GROUP = "size"
SIZE_REGEX = r"(?P<size>[^/]+)/"
ROTATION_GROUP = "rotation"
ROTATION_REGEX = r"(?P<rotation>[^/]+)/"
QUALITY_GROUP = "quality"
FORMAT_GROUP = "format"
QUALITY_FORMAT_REGEX = r"(?P<quality>[^./]+)\.(?P<format>.*)"
STRING_END_REGEX = r"$"

IMAGE_API_UP_THROUGH_IDENTIFIER_W_PREFIX_REGEX = re.compile(
    SCHEME_REGEX
    + SERVER_REGEX
    + PREFIX1_IIIF_REGEX
    + IDENTIFIER_REGEX_OPTIONAL_SLASH
    + STRING_END_REGEX
)

IMAGE_API_UP_THROUGH_IDENTIFIER_W_DOUBLE_PREFIX_REGEX = re.compile(
    SCHEME_REGEX
    + SERVER_REGEX
    + PREFIX1_REGEX
    + PREFIX2_IIIF_REGEX
    + IDENTIFIER_REGEX_OPTIONAL_SLASH
    + STRING_END_REGEX
)

IMAGE_API_UP_THROUGH_IDENTIFIER_W_TRIPLE_PREFIX_REGEX = re.compile(
    SCHEME_REGEX
    + SERVER_REGEX
    + PREFIX1_REGEX
    + PREFIX2_REGEX
    + PREFIX3_IIIF_REGEX
    + IDENTIFIER_REGEX_OPTIONAL_SLASH
    + STRING_END_REGEX
)

IMAGE_API_UP_THROUGH_IDENTIFIER_REGEX_NO_PREFIX = re.compile(
    SCHEME_REGEX + SERVER_REGEX + IDENTIFIER_REGEX_OPTIONAL_SLASH + STRING_END_REGEX
)

IMAGE_API_UP_THROUGH_IDENTIFIER_REGEX_ONE_PREFIX_ID_W_SLASHES = re.compile(
    SCHEME_REGEX + SERVER_REGEX + PREFIX1_IIIF_REGEX + IDENTIFIER_REGEX_REST_OF_STRING
)

# {scheme}://{server}{/prefix}/{identifier}/{region}/{size}/{rotation}/{quality}.{format}
FULL_IMAGE_API_URL_W_PREFIX_REGEX = re.compile(
    SCHEME_REGEX
    + SERVER_REGEX
    + PREFIX1_IIIF_REGEX
    + IDENTIFIER_REGEX_REQUIRED_SLASH
    + REGION_REGEX
    + SIZE_REGEX
    + ROTATION_REGEX
    + QUALITY_FORMAT_REGEX
    + STRING_END_REGEX
)

FULL_IMAGE_API_URL_W_DOUBLE_PREFIX_REGEX = re.compile(
    SCHEME_REGEX
    + SERVER_REGEX
    + PREFIX1_REGEX
    + PREFIX2_IIIF_REGEX
    + IDENTIFIER_REGEX_REQUIRED_SLASH
    + REGION_REGEX
    + SIZE_REGEX
    + ROTATION_REGEX
    + QUALITY_FORMAT_REGEX
    + STRING_END_REGEX
)

FULL_IMAGE_API_URL_W_TRIPLE_PREFIX_REGEX = re.compile(
    SCHEME_REGEX
    + SERVER_REGEX
    + PREFIX1_REGEX
    + PREFIX2_REGEX
    + PREFIX3_IIIF_REGEX
    + IDENTIFIER_REGEX_REQUIRED_SLASH
    + REGION_REGEX
    + SIZE_REGEX
    + ROTATION_REGEX
    + QUALITY_FORMAT_REGEX
    + STRING_END_REGEX
)

FULL_IMAGE_API_URL_REGEX_NO_PREFIX = re.compile(
    SCHEME_REGEX
    + SERVER_REGEX
    + IDENTIFIER_REGEX_REQUIRED_SLASH
    + REGION_REGEX
    + SIZE_REGEX
    + ROTATION_REGEX
    + QUALITY_FORMAT_REGEX
    + STRING_END_REGEX
)

HEADER_CONTENT_TYPE = "Content-Type"
CONTENT_TYPE_JPEG = "image/jpeg"

DPLA_API_URL_BASE = "https://api.dp.la/v2/items/"
DPLA_API_DOCS = "docs"
INSTITUTIONS_URL = (
    "https://raw.githubusercontent.com/dpla/ingestion3"
    "/refs/heads/main/src/main/resources/wiki/institutions_v2.json"
)
UPLOAD_FIELD_NAME = "upload"
INSTITUTIONS_FIELD_NAME = "institutions"
SOURCE_RESOURCE_FIELD_NAME = "sourceResource"
MEDIA_MASTER_FIELD_NAME = "mediaMaster"
IIIF_MANIFEST_FIELD_NAME = "iiifManifest"
PROVIDER_FIELD_NAME = "provider"
DATA_PROVIDER_FIELD_NAME = "dataProvider"
EXACT_MATCH_FIELD_NAME = "exactMatch"
EDM_AGENT_NAME = "name"
EDM_IS_SHOWN_AT = "isShownAt"
RIGHTS_CATEGORY_FIELD_NAME = "rightsCategory"
EDM_RIGHTS_FIELD_NAME = "rights"
EDM_TIMESPAN_DISPLAY_DATE = "displayDate"
UNLIMITED_RE_USE = "Unlimited Re-Use"
DC_CREATOR_FIELD_NAME = "creator"
DC_DATE_FIELD_NAME = "date"
DC_DESCRIPTION_FIELD_NAME = "description"
DC_TITLE_FIELD_NAME = "title"
DC_IDENTIFIER_FIELD_NAME = "identifier"
WIKIDATA_FIELD_NAME = "Wikidata"
AUTHORIZATION_HEADER = "Authorization"
JSON_LD_AT_CONTEXT = "@context"
JSON_LD_AT_ID = "@id"
IIIF_DEFAULT_JPG_SUFFIX = "default.jpg"
IIIF_ID = "id"
IIIF_BODY = "body"
IIIF_ITEMS = "items"
IIIF_RESOURCE = "resource"
IIIF_IMAGES = "images"
IIIF_CANVASES = "canvases"
IIIF_SEQUENCES = "sequences"
IIIF_SERVICE = "service"
IIIF_V2_FULL_RES_JPG_SUFFIX = "/full/full/0/default.jpg"
IIIF_V3_FULL_RES_JPG_SUFFIX = "/full/max/0/default.jpg"
IIIF_PRESENTATION_API_MANIFEST_V2 = "http://iiif.io/api/presentation/2/context.json"
IIIF_PRESENTATION_API_MANIFEST_V3 = "http://iiif.io/api/presentation/3/context.json"
IIIF_IMAGE_API_V2 = "http://iiif.io/api/image/2/context.json"
IIIF_IMAGE_API_V3 = "http://iiif.io/api/image/3/context.json"
CONTENTDM_IIIF_MANIFEST_JSON = "/manifest.json"
CONTENTDM_IIIF_INFO = "/iiif/info/"
CONTENT_DM_ISSHOWNAT_REGEX = r"^/cdm/ref/collection/(.*)/id/(.*)$"
DPLA_PARTNERS = {
    "bpl": "Digital Commonwealth",
    "georgia": "Digital Library of Georgia",
    # hold off on illinois "il": "Illinois Digital Heritage Hub",
    "indiana": "Indiana Memory",
    "nara": "National Archives and Records Administration",
    "northwest-heritage": "Northwest Digital Heritage",
    "ohio": "Ohio Digital Network",
    "p2p": "Plains to Peaks Collective",
    "pa": "PA Digital",
    "texas": "The Portal to Texas History",
    "minnesota": "Minnesota Digital Library",
    "mwdl": "Mountain West Digital Library",
    "heartland": "Heartland Hub",
}
