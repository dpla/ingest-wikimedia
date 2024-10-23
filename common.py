import logging
import os
import shutil
import tempfile
from datetime import datetime
from enum import Enum

import boto3
import requests
from botocore.config import Config

# from mypy.typeshed.stdlib.tempfile import NamedTemporaryFile
from mypy_boto3_s3 import S3ServiceResource

from constants import (
    API_URL_BASE,
    MEDIA_MASTER_FIELD_NAME,
    TMP_DIR_BASE,
    S3_RETRIES,
    LOGS_DIR_BASE,
)


def get_item_metadata(dpla_id: str, api_key: str) -> dict:
    url = API_URL_BASE + dpla_id
    headers = {"Authorization": api_key}
    response = requests.get(url, headers=headers)
    response_json = response.json()
    return response_json.get("docs")[0]


def extract_urls(item_metadata: dict) -> list[str]:
    if MEDIA_MASTER_FIELD_NAME in item_metadata:
        return item_metadata.get(MEDIA_MASTER_FIELD_NAME, [])
    else:
        raise NotImplementedError("No mediaMaster")


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
