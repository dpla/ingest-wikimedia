import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3ServiceResource


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


def get_s3() -> S3ServiceResource:
    config = Config(
        signature_version="s3v4",
        max_pool_connections=25,
        retries={"max_attempts": S3_RETRIES},
    )

    return boto3.resource("s3", config=config)


S3_RETRIES = 3
S3_BUCKET = "dpla-mdpdb"  # TODO change for prod
S3_KEY_CHECKSUM = "sha1"
S3_KEY_METADATA = "Metadata"
S3_KEY_CONTENT_TYPE = "ContentType"
