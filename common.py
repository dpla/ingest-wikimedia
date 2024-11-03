import logging
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime
from enum import Enum

import boto3
import requests
import validators
from botocore.config import Config
from mypy_boto3_s3 import S3ServiceResource
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

from constants import (
    DPLA_API_URL_BASE,
    TMP_DIR_BASE,
    S3_RETRIES,
    LOGS_DIR_BASE,
    UPLOAD_FIELD_NAME,
    RIGHTS_CATEGORY_FIELD_NAME,
    UNLIMITED_RE_USE,
    IIIF_MANIFEST_FIELD_NAME,
    PROVIDER_FIELD_NAME,
    EDM_AGENT_NAME,
    DATA_PROVIDER_FIELD_NAME,
    INSTITUTIONS_FIELD_NAME,
    INSTITUTIONS_URL,
    DPLA_PARTNERS,
    MEDIA_MASTER_FIELD_NAME,
    WIKIDATA_FIELD_NAME,
    CONTENT_DM_ISSHOWNAT_REGEX,
    HTTP_REQUEST_HEADERS,
    AUTHORIZATION_HEADER,
    DPLA_API_DOCS,
    JSON_LD_AT_ID,
    IIIF_DEFAULT_JPG_SUFFIX,
    IIIF_ID,
    IIIF_BODY,
    IIIF_ITEMS,
    IIIF_FULL_RES_JPG_SUFFIX,
    IIIF_PRESENTATION_API_MANIFEST_V2,
    IIIF_PRESENTATION_API_MANIFEST_V3,
    CONTENTDM_IIIF_MANIFEST_JSON,
    CONTENTDM_IIIF_INFO,
    IIIF_RESOURCE,
    IIIF_IMAGES,
    IIIF_CANVASES,
    IIIF_SEQUENCES,
    JSON_LD_AT_CONTEXT,
)

__http_session = None


def get_http_session() -> requests.Session:
    global __http_session
    if __http_session is not None:
        return __http_session
    retry_strategy = Retry(
        total=10,
        backoff_factor=1,
        redirect=5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        raise_on_status=True,
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
            logging.info(f"IMAGE: {image}")
            logging.info(f"RESOURCE: {resource}")
            logging.info(f"URL: {url}")
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
        return request.json()

    except Exception as ex:
        raise Exception(f"Error getting IIIF manifest at {url}") from ex


def contentdm_iiif_url(is_shown_at: str) -> str:
    """
    Creates a IIIF presentation API manifest URL from the
    link to the object in ContentDM
    """
    match_result = re.match(CONTENT_DM_ISSHOWNAT_REGEX, is_shown_at)
    if not match_result:
        return ""
    else:
        return (
            match_result.group(1)
            + CONTENTDM_IIIF_INFO
            + match_result.group(2)
            + "/"
            + match_result.group(3)
            + CONTENTDM_IIIF_MANIFEST_JSON
        )


def get_s3_path(dpla_id: str, ordinal: int, partner: str) -> str:
    return (
        f"{partner}/images/{dpla_id[0]}/{dpla_id[1]}/"
        f"{dpla_id[2]}/{dpla_id[3]}/{dpla_id}/{ordinal}_{dpla_id}"
    ).strip()


def setup_temp_dir() -> None:
    if not os.path.isdir(TMP_DIR_BASE):
        os.mkdir(TMP_DIR_BASE)


def cleanup_temp_dir() -> None:
    shutil.rmtree(TMP_DIR_BASE)


def get_temp_file():
    return tempfile.NamedTemporaryFile(delete=False, dir=TMP_DIR_BASE)


def get_s3() -> S3ServiceResource:
    config = Config(
        signature_version="s3v4",
        max_pool_connections=25,
        retries={"max_attempts": S3_RETRIES},
    )

    return boto3.resource("s3", config=config)


def setup_logging(partner: str, event_type: str, level: int = logging.INFO) -> None:
    os.makedirs(LOGS_DIR_BASE, exist_ok=True)
    time_str = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_file_name = f"{time_str}-{partner}-{event_type}.log"
    filename = f"{LOGS_DIR_BASE}/{log_file_name}"
    logging.basicConfig(
        level=level,
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(filename=filename, mode="w"),
        ],
        format="[%(levelname)s] " "%(asctime)s: " "%(message)s",
    )
    logging.info(f"Logging to {filename}.")
    for d in logging.Logger.manager.loggerDict:
        if d.startswith("pywiki"):
            logging.getLogger(d).setLevel(logging.ERROR)


def clean_up_tmp_file(temp_file) -> None:
    try:
        if temp_file:
            os.unlink(temp_file.name)
    except Exception as e:
        logging.warning("Temp file unlink failed.", e)


Result = Enum("Result", ["DOWNLOADED", "FAILED", "SKIPPED", "UPLOADED", "BYTES"])


class Tracker:
    def __init__(self):
        self.data = {}

    def increment(self, status: Result, amount=1) -> None:
        if status not in self.data:
            self.data[status] = 0
        self.data[status] = self.data[status] + amount

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

    asset_ok = (len(get_list(item_metadata, MEDIA_MASTER_FIELD_NAME)) > 0) or null_safe(
        item_metadata, IIIF_MANIFEST_FIELD_NAME, False
    )

    # todo create banlist. item based? sha based? local id based? all three?
    # todo don't reupload if deleted

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
    """Loads the institutions file from ingestion3 in github."""
    return get_http_session().get(INSTITUTIONS_URL).json()


def provider_str(provider: dict) -> str:
    if provider is None:
        return "Provider: None"
    else:
        return (
            f"Provider: {provider.get(WIKIDATA_FIELD_NAME, "")}, "
            f"{provider.get(UPLOAD_FIELD_NAME, "")}"
        )
