import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3ServiceResource
from .common import CHECKSUM
from .localfs import LocalFS

IIIF_JSON = "iiif.json"
FILE_LIST_TXT = "file-list.txt"
TEXT_PLAIN = "text/plain"
DPLA_MAP_FILENAME = "dpla-map.json"
APPLICATION_JSON = "application/json"
S3_RETRIES = 3
S3_BUCKET = "dpla-wikimedia"
S3_KEY_METADATA = "Metadata"


class S3Client:
    """
    A wrapper around the S3 client to make it easier to mock in tests.
    """

    def __init__(self):
        config = Config(
            signature_version="s3v4",
            max_pool_connections=25,
            retries={"max_attempts": S3_RETRIES},
        )

        self.s3 = boto3.resource("s3", config=config)

    def get_s3(self) -> S3ServiceResource:
        """
        Gives you the S3ServiceResource for the thread you're on,
        and makes it if it hasn't yet.
        """
        return self.s3

    @staticmethod
    def get_item_s3_path(dpla_id: str, filename: str, partner: str) -> str:
        """
        Calculates the S3 path for a file related to an item in S3.
        """
        return (
            f"{partner}/images/{dpla_id[0]}/{dpla_id[1]}/"
            f"{dpla_id[2]}/{dpla_id[3]}/{dpla_id}/{filename}"
        )

    @staticmethod
    def get_media_s3_path(dpla_id: str, ordinal: int, partner: str) -> str:
        """
        Calculates the D3 path for an individual media file.
        """
        return (
            f"{partner}/images/{dpla_id[0]}/{dpla_id[1]}/"
            f"{dpla_id[2]}/{dpla_id[3]}/{dpla_id}/{ordinal}_{dpla_id}"
        ).strip()

    def s3_file_exists(self, path: str):
        """
        Checks to see if something already exists in S3 for a path.
        """
        try:
            self.s3.Object(S3_BUCKET, path).load()
            return True
        except ClientError as e:
            if (
                "Error" in e.response
                and "Code" in e.response["Error"]
                and e.response["Error"]["Code"] == "404"
            ):
                # The object does not exist.
                return False
            else:
                # Something else has gone wrong.
                raise

    def write_item_metadata(
        self, partner: str, dpla_id: str, item_metadata: str
    ) -> None:
        """
        Writes the metadata file for the item in S3.
        """
        self.write_item_file(
            partner, dpla_id, item_metadata, DPLA_MAP_FILENAME, APPLICATION_JSON
        )

    def get_item_metadata(self, partner: str, dpla_id: str) -> str | None:
        """
        Reads the metadata file back from s3.
        """
        return self.get_item_file(partner, dpla_id, DPLA_MAP_FILENAME)

    def write_file_list(self, partner: str, dpla_id: str, file_urls: list[str]) -> None:
        """
        Writes the list of media files for the item in S3.
        """
        data = "\n".join(file_urls)
        self.write_item_file(partner, dpla_id, data, FILE_LIST_TXT, TEXT_PLAIN)

    def get_file_list(self, partner: str, dpla_id: str) -> list[str]:
        result = self.get_item_file(partner, dpla_id, FILE_LIST_TXT)
        if result is None:
            return []
        else:
            return result.split("\n")

    def write_iiif_manifest(self, partner: str, dpla_id: str, manifest: str) -> None:
        """
        Writes the IIIF manifest for an item in S3.
        """
        self.write_item_file(partner, dpla_id, manifest, IIIF_JSON, APPLICATION_JSON)

    def write_item_file(
        self,
        partner: str,
        dpla_id: str,
        data: str,
        filename: str,
        content_type: str,
    ) -> None:
        """
        Writes a file for an item to the appropriate place in S3.
        """

        s3_path = self.get_item_s3_path(dpla_id, filename, partner)
        s3_object = self.s3.Object(S3_BUCKET, s3_path)
        sha1 = LocalFS.get_bytes_hash(data)
        s3_object.put(ContentType=content_type, Metadata={CHECKSUM: sha1}, Body=data)

    def get_item_file(self, partner, dpla_id, file_name) -> str | None:
        s3_path = self.get_item_s3_path(dpla_id, file_name, partner)

        try:
            s3_object = self.s3.Object(S3_BUCKET, s3_path).get()

        except ClientError as e:
            if (
                "Error" in e.response
                and "Code" in e.response["Error"]
                and e.response["Error"]["Code"] == "404"
            ):
                # The object does not exist.
                return None
            else:
                # Something else has gone wrong.
                raise

        return s3_object["Body"].read().decode("utf-8")
