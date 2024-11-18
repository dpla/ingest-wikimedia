import hashlib
import logging
import os
import tempfile
import magic

from s3 import S3_KEY_CHECKSUM
from wikimedia import INVALID_CONTENT_TYPES

__temp_dir: tempfile.TemporaryDirectory | None = None


def setup_temp_dir() -> None:
    """
    Sets up a temporary dir for this process. Not thread-safe.
    """
    global __temp_dir
    if __temp_dir is None:
        __temp_dir = tempfile.TemporaryDirectory(
            "tmp", "wiki-tmp-", dir="", ignore_cleanup_errors=True, delete=False
        )


def cleanup_temp_dir() -> None:
    """
    Cleans up the temp dir for this process. Not thread-safe.
    """
    global __temp_dir
    if __temp_dir is not None:
        __temp_dir.cleanup()


def get_temp_file() -> tempfile.NamedTemporaryFile:
    """
    Creates a new temporary file. Caller responsible for clean up.
    """
    global __temp_dir
    if __temp_dir is None:
        raise Exception("Temp dir not initialized.")
    return tempfile.NamedTemporaryFile(delete=False, dir=__temp_dir.name)


def clean_up_tmp_file(temp_file: tempfile.NamedTemporaryFile) -> None:
    """
    Cleans up a temporary file.
    """
    try:
        if temp_file:
            os.unlink(temp_file.name)
    except Exception as e:
        logging.warning("Temp file unlink failed.", exc_info=e)


def get_file_hash(file: str) -> str:
    """
    Gets the SHA-1 hash for a file at the given path. We're using SHA-1 because that's
    what Wikimedia Commons uses for uniqueness.
    """
    with open(file, "rb") as f:
        # noinspection PyTypeChecker
        return hashlib.file_digest(f, S3_KEY_CHECKSUM).hexdigest()


def get_bytes_hash(data: str) -> str:
    """
    Gets the SHA-1 hash for a string, used for metadata files. We're using SHA-1
    because that's what Wikimedia Commons uses for uniqueness.
    """
    return hashlib.sha1(data.encode("utf-8"), usedforsecurity=False).hexdigest()


def get_content_type(file: str) -> str:
    """
    Tries to detect the mime type of a download.
    """
    content_type = magic.from_file(file, mime=True)
    if content_type in INVALID_CONTENT_TYPES:
        raise Exception(f"Invalid content-type: {content_type}")
    return content_type
