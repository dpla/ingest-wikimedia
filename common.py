import csv
import logging
import os
import re
import sys
import tempfile
from datetime import datetime
from enum import Enum
from typing import IO
from urllib.parse import urlparse
from tqdm import tqdm

import boto3
import requests
import validators
from botocore.config import Config
from mypy_boto3_s3.service_resource import S3ServiceResource
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from botocore.exceptions import ClientError

from constants import (
    AUTHORIZATION_HEADER,
    CONTENT_DM_ISSHOWNAT_REGEX,
    CONTENTDM_IIIF_INFO,
    CONTENTDM_IIIF_MANIFEST_JSON,
    DATA_PROVIDER_FIELD_NAME,
    DPLA_API_DOCS,
    DPLA_API_URL_BASE,
    DPLA_PARTNERS,
    EDM_AGENT_NAME,
    HTTP_REQUEST_HEADERS,
    IIIF_BODY,
    IIIF_CANVASES,
    IIIF_DEFAULT_JPG_SUFFIX,
    IIIF_FULL_RES_JPG_SUFFIX,
    IIIF_ID,
    IIIF_IMAGES,
    IIIF_ITEMS,
    IIIF_MANIFEST_FIELD_NAME,
    IIIF_PRESENTATION_API_MANIFEST_V2,
    IIIF_PRESENTATION_API_MANIFEST_V3,
    IIIF_RESOURCE,
    IIIF_SEQUENCES,
    INSTITUTIONS_FIELD_NAME,
    INSTITUTIONS_URL,
    JSON_LD_AT_CONTEXT,
    JSON_LD_AT_ID,
    LOGS_DIR_BASE,
    MEDIA_MASTER_FIELD_NAME,
    PROVIDER_FIELD_NAME,
    RIGHTS_CATEGORY_FIELD_NAME,
    S3_RETRIES,
    UNLIMITED_RE_USE,
    UPLOAD_FIELD_NAME,
    WIKIDATA_FIELD_NAME,
    S3_BUCKET,
    EDM_IS_SHOWN_AT,
)

__http_session: requests.Session | None = None
__temp_dir: tempfile.TemporaryDirectory | None = None


def load_ids(ids_file: IO) -> list[str]:
    dpla_ids = []
    csv_reader = csv.reader(ids_file)
    for row in csv_reader:
        dpla_ids.append(row[0])
    return dpla_ids


def get_http_session() -> requests.Session:
    global __http_session
    if __http_session is not None:
        return __http_session
    retry_strategy = Retry(
        connect=3,
        read=3,
        redirect=5,
        status=5,
        other=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        respect_retry_after_header=True,
        raise_on_status=True,
        raise_on_redirect=True,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    __http_session = requests.Session()
    __http_session.mount("https://", adapter)
    __http_session.mount("http://", adapter)
    return __http_session


def null_safe[T](data: dict, field_name: str, identity_element: T) -> T:
    if data is not None:
        return data.get(field_name, identity_element)
    else:
        return identity_element


def get_list(data: dict, field_name: str) -> list:
    """Null safe shortcut for getting an array from a dict."""
    return null_safe(data, field_name, [])


def get_str(data: dict, field_name: str) -> str:
    """Null safe shortcut for getting a string from a dict."""
    return null_safe(data, field_name, "")


def get_dict(data: dict, field_name: str) -> dict:
    """Null safe shortcut for getting a dict from a dict."""
    return null_safe(data, field_name, {})


def check_partner(partner: str) -> None:
    if partner not in DPLA_PARTNERS:
        sys.exit("Unrecognized partner.")


def get_item_metadata(dpla_id: str, api_key: str) -> dict:
    url = DPLA_API_URL_BASE + dpla_id
    headers = {AUTHORIZATION_HEADER: api_key}
    response = get_http_session().get(url, headers=headers)
    response_json = response.json()
    return response_json.get(DPLA_API_DOCS)[0]


def extract_urls(item_metadata: dict) -> list[str]:
    if MEDIA_MASTER_FIELD_NAME in item_metadata:
        return get_list(item_metadata, MEDIA_MASTER_FIELD_NAME)

    elif IIIF_MANIFEST_FIELD_NAME in item_metadata:
        return get_iiif_urls(get_str(item_metadata, IIIF_MANIFEST_FIELD_NAME))

    else:
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
            url = get_str(resource, JSON_LD_AT_ID)
            if url:
                urls.append(url)
    return urls


def iiif_v3_urls(iiif: dict) -> list[str]:
    """
    Extracts image URLs from a v3 IIIF manifest and returns them as a list
    """
    urls = []
    for item in get_list(iiif, IIIF_ITEMS):
        try:
            url = get_str(
                get_dict(item[IIIF_ITEMS][0][IIIF_ITEMS][0], IIIF_BODY), IIIF_ID
            )
            # This is a hack to get around that v3 presumes the user supplies the
            # resolution in the URL
            if url:
                # This condition may not be necessary but I'm leaving it in for now
                # TODO does this end up giving us smaller resources than we want?
                if url.endswith(IIIF_DEFAULT_JPG_SUFFIX):
                    urls.append(url)
                else:
                    urls.append(url + IIIF_FULL_RES_JPG_SUFFIX)
        except (IndexError, TypeError, KeyError) as e:
            logging.warning("Unable to parse IIIF manifest.", e)
            return []
    return urls


def get_iiif_urls(iiif_presentation_api_url: str) -> list[str]:
    """
    Extracts image URLs from IIIF manifest and returns them as a list
    Currently only supports IIIF v2 and v3
    """
    manifest = _get_iiif_manifest(iiif_presentation_api_url)
    # v2 or v3?
    if get_str(manifest, JSON_LD_AT_CONTEXT) == IIIF_PRESENTATION_API_MANIFEST_V3:
        return iiif_v3_urls(manifest)
    elif get_str(manifest, JSON_LD_AT_CONTEXT) == IIIF_PRESENTATION_API_MANIFEST_V2:
        return iiif_v2_urls(manifest)
    else:
        raise Exception("Unimplemented IIIF version")


def _get_iiif_manifest(url: str) -> dict:
    """
    :return: parsed JSON
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


def get_s3_path(dpla_id: str, ordinal: int, partner: str) -> str:
    return (
        f"{partner}/images/{dpla_id[0]}/{dpla_id[1]}/"
        f"{dpla_id[2]}/{dpla_id[3]}/{dpla_id}/{ordinal}_{dpla_id}"
    ).strip()


def s3_file_exists(path: str, s3: S3ServiceResource):
    try:
        s3.Object(S3_BUCKET, path).load()
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            # The object does not exist.
            return False
        else:
            # Something else has gone wrong.
            raise


def setup_temp_dir() -> None:
    global __temp_dir
    if __temp_dir is None:
        __temp_dir = tempfile.TemporaryDirectory(
            "tmp", "wiki", dir=".", ignore_cleanup_errors=True, delete=False
        )


def cleanup_temp_dir() -> None:
    global __temp_dir
    if __temp_dir is not None:
        __temp_dir.cleanup()


def get_temp_file():
    global __temp_dir
    if __temp_dir is None:
        raise Exception("Temp dir not initialized.")
    return tempfile.NamedTemporaryFile(delete=False, dir=__temp_dir.name)


def clean_up_tmp_file(temp_file) -> None:
    try:
        if temp_file:
            os.unlink(temp_file.name)
    except Exception as e:
        logging.warning("Temp file unlink failed.", exc_info=e)


def get_s3() -> S3ServiceResource:
    config = Config(
        signature_version="s3v4",
        max_pool_connections=25,
        retries={"max_attempts": S3_RETRIES},
    )

    return boto3.resource("s3", config=config)


class TqdmLoggingHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


def setup_logging(partner: str, event_type: str, level: int = logging.INFO) -> None:
    os.makedirs(LOGS_DIR_BASE, exist_ok=True)
    time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_file_name = f"{time_str}-{partner}-{event_type}.log"
    filename = f"{LOGS_DIR_BASE}/{log_file_name}"
    logging.basicConfig(
        level=level,
        datefmt="%H:%M:%S",
        handlers=[
            TqdmLoggingHandler(),
            logging.FileHandler(filename=filename, mode="w"),
        ],
        format="[%(levelname)s] " "%(asctime)s: " "%(message)s",
    )
    logging.info(f"Logging to {filename}.")
    for d in logging.Logger.manager.loggerDict:
        if d.startswith("pywiki"):
            logging.getLogger(d).setLevel(logging.ERROR)


Result = Enum("Result", ["DOWNLOADED", "FAILED", "SKIPPED", "UPLOADED", "BYTES"])


class Tracker:
    def __init__(self):
        self.data = {}

    def increment(self, status: Result, amount=1) -> None:
        if status not in self.data:
            self.data[status] = 0
        self.data[status] = self.data[status] + amount

    def count(self, status: Result) -> int:
        if status not in self.data:
            return 0
        else:
            return self.data[status]

    def __str__(self) -> str:
        result = "COUNTS:\n"
        for key in self.data:
            value = self.data[key]
            result += f"{key.name}: {value}\n"
        return result


def is_wiki_eligible(item_metadata: dict, provider: dict, data_provider: dict) -> bool:
    provider_ok = null_safe(provider, UPLOAD_FIELD_NAME, False) or null_safe(
        data_provider, UPLOAD_FIELD_NAME, False
    )

    rights_category_ok = (
        get_str(item_metadata, RIGHTS_CATEGORY_FIELD_NAME) == UNLIMITED_RE_USE
    )

    is_shown_at = get_str(item_metadata, EDM_IS_SHOWN_AT)
    media_master = len(get_list(item_metadata, MEDIA_MASTER_FIELD_NAME)) > 0
    iiif_manifest = null_safe(item_metadata, IIIF_MANIFEST_FIELD_NAME, False)

    if not iiif_manifest and not media_master:
        iiif_url = contentdm_iiif_url(is_shown_at)
        if iiif_url is not None:
            response = get_http_session().head(iiif_url, allow_redirects=True)
            if response.status_code < 400:
                item_metadata[IIIF_MANIFEST_FIELD_NAME] = iiif_url
                iiif_manifest = True

    asset_ok = media_master or iiif_manifest

    # todo create banlist. item based? sha based? local id based? all three?
    # todo don't reupload if deleted

    id_ok = True

    logging.info(
        f"Rights: {rights_category_ok}, Asset: {asset_ok}, Provider: {provider_ok}, ID: {id_ok}"
    )

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
    if provider is None:
        return "Provider: None"
    else:
        return (
            f"Provider: {provider.get(WIKIDATA_FIELD_NAME, "")}, "
            f"{provider.get(UPLOAD_FIELD_NAME, "")}"
        )
