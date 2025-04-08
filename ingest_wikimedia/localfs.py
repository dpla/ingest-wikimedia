import hashlib
import logging
import os
import tempfile
import magic

from .common import CHECKSUM


class LocalFS:
    __temp_dir: tempfile.TemporaryDirectory | None = None

    def setup_temp_dir(self) -> None:
        """
        Sets up a temporary dir for this process. Not thread-safe.
        """

        if self.__temp_dir is None:
            self.__temp_dir = tempfile.TemporaryDirectory(
                "tmp", "wiki-tmp-", dir="", ignore_cleanup_errors=True, delete=False
            )

    def cleanup_temp_dir(self) -> None:
        """
        Cleans up the temp dir for this process. Not thread-safe.
        """

        if self.__temp_dir is not None:
            self.__temp_dir.cleanup()

    def get_temp_file(self) -> tempfile.NamedTemporaryFile:  # pyright: ignore [reportGeneralTypeIssues]
        """
        Creates a new temporary file. Caller responsible for clean up.
        """
        if self.__temp_dir is None:
            raise RuntimeError("Temp dir not initialized.")
        return tempfile.NamedTemporaryFile(delete=False, dir=self.__temp_dir.name)

    @staticmethod
    def clean_up_tmp_file(temp_file: tempfile.NamedTemporaryFile) -> None:  # pyright: ignore [reportGeneralTypeIssues]
        """
        Cleans up a temporary file.
        """
        try:
            if temp_file:
                temp_file.close()
                os.unlink(temp_file.name)
        except Exception as e:
            logging.warning("Temp file cleanup failed.", exc_info=e)

    @staticmethod
    def get_file_hash(file: str) -> str:
        """
        Gets the SHA-1 hash for a file at the given path. We're using SHA-1 because that's
        what Wikimedia Commons uses for uniqueness.
        """
        with open(file, "rb") as f:
            # noinspection PyTypeChecker
            return hashlib.file_digest(f, CHECKSUM).hexdigest()

    @staticmethod
    def get_bytes_hash(data: str) -> str:
        """
        Gets the SHA-1 hash for a string, used for metadata files. We're using SHA-1
        because that's what Wikimedia Commons uses for uniqueness.
        """
        return hashlib.sha1(data.encode("utf-8"), usedforsecurity=False).hexdigest()

    @staticmethod
    def get_content_type(file: str) -> str:
        """
        Tries to detect the mime type of a downloaded file.
        """
        content_type = magic.from_file(file, mime=True)
        return content_type
