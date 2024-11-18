import threading

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3ServiceResource

from .common import CHECKSUM
from .local import get_bytes_hash

IIIF_JSON = "iiif.json"
FILE_LIST_TXT = "file-list.txt"
TEXT_PLAIN = "text/plain"
DPLA_MAP_FILENAME = "dpla-map.json"
APPLICATION_JSON = "application/json"
S3_RETRIES = 3
S3_BUCKET = "dpla-mdpdb"  # TODO change for prod
S3_KEY_METADATA = "Metadata"

# S3 resources are not thread safe, so make one per thread
__thread_local = threading.local()
__thread_local.s3 = None


def get_s3() -> S3ServiceResource:
    """
    Gives you the S3ServiceResource for the thread you're on,
    and makes it if it hasn't yet.
    """
    if __thread_local.s3 is not None:
        return __thread_local.s3

    config = Config(
        signature_version="s3v4",
        max_pool_connections=25,
        retries={"max_attempts": S3_RETRIES},
    )
    s3 = boto3.resource("s3", config=config)
    __thread_local.s3 = s3
    return s3


def get_item_s3_path(dpla_id: str, filename: str, partner: str) -> str:
    """
    Calculates the S3 path for a file related to an item in S3.
    """
    return (
        f"{partner}/images/{dpla_id[0]}/{dpla_id[1]}/"
        f"{dpla_id[2]}/{dpla_id[3]}/{dpla_id}/{filename}"
    )


def get_media_s3_path(dpla_id: str, ordinal: int, partner: str) -> str:
    """
    Calculates the D3 path for an individual media file.
    """
    return (
        f"{partner}/images/{dpla_id[0]}/{dpla_id[1]}/"
        f"{dpla_id[2]}/{dpla_id[3]}/{dpla_id}/{ordinal}_{dpla_id}"
    ).strip()


def s3_file_exists(path: str):
    """
    Checks to see if something already exists in S3 for a path.
    """
    try:
        s3 = get_s3()
        s3.Object(S3_BUCKET, path).load()
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            # The object does not exist.
            return False
        else:
            # Something else has gone wrong.
            raise


def write_item_metadata(partner: str, dpla_id: str, item_metadata: str) -> None:
    """
    Writes the metadata file for the item in S3.
    """
    write_item_file(partner, dpla_id, item_metadata, DPLA_MAP_FILENAME, TEXT_PLAIN)


def write_file_list(partner: str, dpla_id: str, file_urls: list[str]) -> None:
    """
    Writes the list of media files for the item in S3.
    """
    data = "\n".join(file_urls)
    write_item_file(partner, dpla_id, data, FILE_LIST_TXT, TEXT_PLAIN)


def write_iiif_manifest(partner: str, dpla_id: str, manifest: str) -> None:
    """
    Writes the IIIF manifest for an item in S3.
    """
    write_item_file(partner, dpla_id, manifest, IIIF_JSON, APPLICATION_JSON)


def write_item_file(
    partner: str,
    dpla_id: str,
    data: str,
    filename: str,
    content_type: str,
) -> None:
    """
    Writes a file for an item to the appropriate place in S3.
    """
    s3 = get_s3()
    s3_path = get_item_s3_path(dpla_id, filename, partner)
    s3_object = s3.Object(S3_BUCKET, s3_path)
    sha1 = get_bytes_hash(data)
    s3_object.put(ContentType=content_type, Metadata={CHECKSUM: sha1}, Body=data)
