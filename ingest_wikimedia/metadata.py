import json
import logging
import re
from operator import itemgetter
from urllib.parse import urlparse

import validators

from .common import null_safe, get_str, get_list, get_dict
from .s3 import write_iiif_manifest
from .tracker import Tracker, Result
from .web import get_http_session, HTTP_REQUEST_HEADERS


def check_partner(partner: str) -> None:
    """
    Blows up if we're working on a partner we shouldn't.
    """
    if partner not in DPLA_PARTNERS.keys():
        raise Exception("Unrecognized partner.")


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


def is_wiki_eligible(item_metadata: dict, provider: dict, data_provider: dict) -> bool:
    """
    Enforces a number of criteria for ensuring this is an item we should upload.
    """

    provider_ok = null_safe(provider, UPLOAD_FIELD_NAME, False) or null_safe(
        data_provider, UPLOAD_FIELD_NAME, False
    )

    rights_category_ok = (
        get_str(item_metadata, RIGHTS_CATEGORY_FIELD_NAME) == UNLIMITED_RE_USE
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
                iiif_manifest = True

    asset_ok = (media_master is not None) or (iiif_manifest is not None)

    # todo create banlist. item based? sha based? local id based? all three?
    # todo don't re-upload if deleted

    id_ok = True

    return rights_category_ok and asset_ok and provider_ok and id_ok


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
            f"Provider: {provider.get(WIKIDATA_FIELD_NAME, "")}, "
            f"{provider.get(UPLOAD_FIELD_NAME, "")}"
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
    sequence = sequences[0:1] if len(sequences) == 1 else None
    canvases = get_list(sequence[0], IIIF_CANVASES)

    for canvas in canvases:
        for image in get_list(canvas, IIIF_IMAGES):
            resource = get_dict(image, IIIF_RESOURCE)
            service = get_dict(resource, IIIF_SERVICE)
            url = get_str(service, JSON_LD_AT_ID)
            if url:
                urls.append(maximize_iiif_url(url))
            else:
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
                new_url = maximize_iiif_url(url)
            # This always adds something to the list.
            # If we didn't get a URL, it's just an empty string.
            # This prevents getting the page order wrong if we don't
            # figure out the URL one time and fix it later.
            urls.append(new_url)

        except (IndexError, TypeError, KeyError) as e:
            logging.warning("Unable to parse IIIF manifest.", e)
            Tracker().increment(Result.BAD_IIIF_MANIFEST)
            return []
    return urls


def maximize_iiif_url(url: str) -> str:
    m = None

    if match := FULL_IMAGE_API_URL_REGEX.match(url):
        m = match.groupdict()

    elif match := IMAGE_API_UP_THROUGH_IDENTIFIER_REGEX.match(url):
        m = match.groupdict()

    if m is not None:
        scheme, server, prefix, identifier = itemgetter(
            "scheme",
            "server",
            "prefix",
            "identifier",
        )(m)

        return f"{scheme}://{server}/{prefix}/{identifier}/full/max/0/default.jpg"

    if match := FULL_IMAGE_API_URL_REGEX_NO_PREFIX.match(url):
        m = match.groupdict()

    elif match := IMAGE_API_UP_THROUGH_IDENTIFIER_REGEX.match(url):
        m = match.groupdict()

    if m is not None:
        scheme, server, prefix, identifier = itemgetter(
            "scheme",
            "server",
            "identifier",
        )(m)

        return f"{scheme}://{server}/{identifier}/full/max/0/default.jpg"

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
            raise Exception("No IIIF version specified.")
        case x if x == IIIF_PRESENTATION_API_MANIFEST_V3:
            return iiif_v3_urls(manifest)
        case x if x == IIIF_PRESENTATION_API_MANIFEST_V2:
            return iiif_v2_urls(manifest)
        case x if type(x) is list and IIIF_PRESENTATION_API_MANIFEST_V3 in x:
            return iiif_v3_urls(manifest)
        case x if type(x) is list and IIIF_PRESENTATION_API_MANIFEST_V2 in x:
            return iiif_v2_urls(manifest)
        case x:
            raise Exception(f"Unimplemented IIIF version: {x}")


def get_iiif_manifest(url: str) -> dict:
    """
    Gets the IIIF manifest from the given url.
    """
    if not validators.url(url):
        raise Exception(f"Invalid url {url}")
    try:
        request = get_http_session().get(url, headers=HTTP_REQUEST_HEADERS)
        request.raise_for_status()
        return request.json()

    except Exception as ex:
        # todo maybe this should return None?
        raise Exception(f"Error getting IIIF manifest at {url}") from ex


def contentdm_iiif_url(is_shown_at: str) -> str | None:
    """
    Creates a IIIF presentation API manifest URL from the
    link to the object in ContentDM

    We want to go from
    http://www.ohiomemory.org/cdm/ref/collection/p16007coll33/id/126923
    to
    http://www.ohiomemory.org/iiif/info/p16007coll33/126923/manifest.json

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


# {scheme}://{server}{/prefix}/{identifier}/
IMAGE_API_UP_THROUGH_IDENTIFIER_REGEX = re.compile(
    r"^(?P<scheme>http|https)://(?P<server>[^/]+)/(?P<prefix>[^/]+)/"
    r"(?P<identifier>[^/]+)/?$"
)

IMAGE_API_UP_THROUGH_IDENTIFIER_REGEX_NO_PREFIX = re.compile(
    r"^(?P<scheme>http|https)://(?P<server>[^/]+)/(?P<identifier>[^/]+)/?$"
)


# {scheme}://{server}{/prefix}/{identifier}/{region}/{size}/{rotation}/{quality}.{format}
FULL_IMAGE_API_URL_REGEX = re.compile(
    r"^(?P<scheme>http|https)://(?P<server>[^/]+)/(?P<prefix>[^/]+)/"
    r"(?P<identifier>[^/]+)/(?P<region>[^/]+)/(?P<size>[^/]+)/"
    r"(?P<rotation>[^/]+)/(?P<quality>[^.]+).(?P<format>.*)$"
)

FULL_IMAGE_API_URL_REGEX_NO_PREFIX = re.compile(
    r"^(?P<scheme>http|https)://(?P<server>[^/]+)/(?P<prefix>[^/]+)/"
    r"(?P<identifier>[^/]+)/(?P<region>[^/]+)/(?P<size>[^/]+)/"
    r"(?P<rotation>[^/]+)/(?P<quality>[^.]+).(?P<format>.*)$"
)


DPLA_API_URL_BASE = "https://api.dp.la/v2/items/"
DPLA_API_DOCS = "docs"
INSTITUTIONS_URL = (
    "https://raw.githubusercontent.com/dpla/ingestion3"
    "/refs/heads/develop/src/main/resources/wiki/institutions_v2.json"
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
EDM_TIMESPAN_PREF_LABEL = "prefLabel"
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
IIIF_FULL_RES_JPG_SUFFIX = "/full/max/0/default.jpg"
IIIF_PRESENTATION_API_MANIFEST_V2 = "http://iiif.io/api/presentation/2/context.json"
IIIF_PRESENTATION_API_MANIFEST_V3 = "http://iiif.io/api/presentation/3/context.json"
IIIF_IMAGE_API_V2 = "http://iiif.io/api/image/2/context.json"
IIIF_IMAGE_API_V3 = "http://iiif.io/api/image/3/context.json"
CONTENTDM_IIIF_MANIFEST_JSON = "/manifest.json"
CONTENTDM_IIIF_INFO = "/iiif/info/"
CONTENT_DM_ISSHOWNAT_REGEX = r"^/cdm/ref/collection/(.*)/id/(.*)$"  # todo
DPLA_PARTNERS = {
    "bpl": "Digital Commonwealth",
    "georgia": "Digital Library of Georgia",
    "il": "Illinois Digital Heritage Hub",
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
