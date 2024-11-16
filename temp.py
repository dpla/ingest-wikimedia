import logging
import os
import tempfile

__temp_dir: tempfile.TemporaryDirectory | None = None


def setup_temp_dir() -> None:
    global __temp_dir
    if __temp_dir is None:
        __temp_dir = tempfile.TemporaryDirectory(
            "tmp", "wiki-tmp-", dir="", ignore_cleanup_errors=True, delete=False
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
