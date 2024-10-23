import csv
import hashlib
import os
import tempfile
import traceback

import click
import magic
import requests
from mypy_boto3_s3.service_resource import S3ServiceResource

from common import (
    get_item_metadata,
    extract_urls,
    get_s3_path,
    get_temp_file,
    setup_temp_dir,
    cleanup_temp_dir,
    get_s3,
)
from constants import (
    S3_BUCKET,
    CHECKSUM_KEY,
    INVALID_CONTENT_TYPES,
)


def download_media(
    partner: str, dpla_id: str, ordinal: int, media_url: str, s3: S3ServiceResource
) -> None:
    temp_file = None
    try:
        destination_path = get_s3_path(dpla_id, ordinal, partner)
        temp_file = download_file_to_temp_path(media_url)
        content_type = get_content_type(temp_file)
        sha1 = get_file_hash(temp_file)
        upload_temp_file(content_type, destination_path, media_url, s3, sha1, temp_file)

    finally:
        if temp_file:
            temp_file.close()
            os.unlink(temp_file.name)


def upload_temp_file(
    content_type: str,
    destination_path: str,
    media_url: str,
    s3: S3ServiceResource,
    sha1: str,
    temp_file: tempfile.NamedTemporaryFile,
):
    try:
        with open(temp_file.name, "rb") as file:
            obj = s3.Object(S3_BUCKET, destination_path)
            obj_metadata = obj.metadata
            if obj_metadata and obj_metadata.get(CHECKSUM_KEY, None) == sha1:
                # Already uploaded, move on.
                return
            obj.upload_fileobj(
                Fileobj=file,
                ExtraArgs={
                    "ContentType": content_type,
                    "Metadata": {CHECKSUM_KEY: sha1},
                },
            )
    except Exception as e:
        raise Exception(
            f"Error uploading {media_url} to s3://{S3_BUCKET}/{destination_path}"
        ) from e


def get_file_hash(temp_file):
    return hashlib.file_digest(temp_file, CHECKSUM_KEY).hexdigest()


def get_content_type(temp_file: tempfile.NamedTemporaryFile):
    content_type = magic.from_file(temp_file.name, mime=True)
    if content_type in INVALID_CONTENT_TYPES:
        raise Exception(f"Invalid content-type: {content_type}")
    return content_type


def download_file_to_temp_path(media_url: str) -> tempfile.NamedTemporaryFile:
    temp_file = get_temp_file()
    try:
        response = requests.get(media_url, timeout=30)
        with open(temp_file.name, "wb") as f:
            f.write(response.content)

    except Exception as e:
        raise Exception(f"Failed saving {media_url} to local") from e
    return temp_file


@click.command()
@click.argument("ids_file")
@click.argument("partner")
@click.argument("api_key")
def main(ids_file: str, partner: str, api_key: str):

    setup_temp_dir()
    s3 = get_s3()

    with open(ids_file) as csv_file:
        csv_reader = csv.reader(csv_file)
        for row in csv_reader:
            dpla_id = row[0]
            item_metadata = get_item_metadata(dpla_id, api_key)
            media_urls = extract_urls(item_metadata)
            count = 0
            for media_url in media_urls:
                count += 1
                # hack to fix bad nara data
                if media_url.startswith("https/"):
                    media_url = media_url.replace("https/", "https:/")
                try:
                    print(f"Downloading {partner} {dpla_id} {count} from {media_url}")
                    download_media(partner, dpla_id, count, media_url, s3)
                except Exception:
                    traceback.print_exc()

    cleanup_temp_dir()


if __name__ == "__main__":
    main()
