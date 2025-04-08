import json
import logging
import re
from operator import itemgetter
from urllib.parse import urlparse

import requests
import validators
from requests import Session

from ingest_wikimedia.common import get_list, get_dict, get_str
from ingest_wikimedia.tracker import Tracker, Result
from ingest_wikimedia.web import HTTP_REQUEST_HEADERS


class IIIF:
    def __init__(self, tracker: Tracker, http_session: Session):
        self.tracker = tracker
        self.http_session = http_session

    def iiif_v2_urls(self, iiif: dict) -> list[str]:
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
                        big_url = self.maximize_iiif_url(
                            url, IIIF_V2_FULL_RES_JPG_SUFFIX
                        )
                        urls.append(big_url)
                    elif url:
                        # guessing the v3 syntax will work even if we don't know it's v3.
                        big_url = self.maximize_iiif_url(
                            url, IIIF_V3_FULL_RES_JPG_SUFFIX
                        )
                        urls.append(big_url)
                    else:
                        # we give up, but need to hold space so the numbering is right
                        urls.append("")

        return urls

    def iiif_v3_urls(self, iiif: dict) -> list[str]:
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
                    new_url = self.maximize_iiif_url(url, IIIF_V3_FULL_RES_JPG_SUFFIX)
                # This always adds something to the list.
                # If we didn't get a URL, it's just an empty string.
                # This prevents getting the page order wrong if we don't
                # figure out the URL one time and fix it later.
                urls.append(new_url)

            except (IndexError, TypeError, KeyError) as e:
                logging.warning("Unable to parse IIIF manifest.", exc_info=e)
                self.tracker.increment(Result.BAD_IIIF_MANIFEST)
                return []
        return urls

    def maximize_iiif_url(self, url: str, suffix: str) -> str:
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
                SCHEME_GROUP,
                SERVER_GROUP,
                IDENTIFIER_GROUP,
                PREFIX1_GROUP,
                PREFIX2_GROUP,
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

        elif (
            match
            := IMAGE_API_UP_THROUGH_IDENTIFIER_REGEX_ONE_PREFIX_ID_W_SLASHES.match(url)
        ):
            return one_prefix(match)
        else:
            # try just whacking a max-res suffix on:
            test_url = url + suffix
            # test to make sure that has something at the end of it

            head_response = self.http_session.head(test_url)
            if (
                head_response.ok
                and head_response.headers[HEADER_CONTENT_TYPE] == CONTENT_TYPE_JPEG
            ):
                return test_url

        logging.warning(f"Couldn't maximize IIIF URL: {url}")
        self.tracker.increment(Result.BAD_IMAGE_API)
        return ""  # we give up

    def get_iiif_urls(self, manifest: dict) -> list[str]:
        """
        Extracts image URLs from IIIF manifest and returns them as a list
        Currently only supports IIIF v2 and v3
        """
        # v2 or v3?
        match manifest.get(JSON_LD_AT_CONTEXT, None):
            case None:
                raise ValueError("No IIIF version specified.")
            case x if x == IIIF_PRESENTATION_API_MANIFEST_V3:
                return self.iiif_v3_urls(manifest)
            case x if x == IIIF_PRESENTATION_API_MANIFEST_V2:
                return self.iiif_v2_urls(manifest)
            case x if type(x) is list and IIIF_PRESENTATION_API_MANIFEST_V3 in x:
                return self.iiif_v3_urls(manifest)
            case x if type(x) is list and IIIF_PRESENTATION_API_MANIFEST_V2 in x:
                return self.iiif_v2_urls(manifest)
            case x:
                raise ValueError(f"Unimplemented IIIF version: {x}")

    def get_iiif_manifest(self, url: str) -> dict | None:
        """
        Gets the IIIF manifest from the given url.
        """
        if not validators.url(url):
            logging.warning(f"Invalid IIIF manifest url: {url}")
            return None

        try:
            request = self.http_session.get(url, headers=HTTP_REQUEST_HEADERS)
            request.raise_for_status()
            return request.json()

        except (requests.RequestException, json.JSONDecodeError):
            logging.info(f"Unable to read IIIF manifest at {url}")
            return None

    @staticmethod
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

JSON_LD_AT_CONTEXT = "@context"
JSON_LD_AT_ID = "@id"

HEADER_CONTENT_TYPE = "Content-Type"
CONTENT_TYPE_JPEG = "image/jpeg"
