
import boto3
import logging
import requests
import tempfile
from pathlib import Path
from urllib.parse import urlparse
from botocore.exceptions import ClientError

import awswrangler as wr
import getopt
import json
import logging
import pandas as pd
import requests


class Utils:
    def __init__(self):
        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

    def create_path(self, path):
        prefix = b's3' if isinstance(path, bytes) else "s3"
        if not path.startswith(prefix) and not Path(path).exists():
            Path(path).mkdir(parents=True)

    def get_df_s3(self, path, columns):
        return wr.s3 \
            .read_parquet(path=path) \
            .rename(columns=columns)

    def get_df_local(self, path, columns):
        path = path if isinstance(path, bytes) else path.encode('utf-8')
        print("Type is instance of " + type(path))
        return pd.read_parquet(path, engine='fastparquet')\
            .rename(columns=columns)

    def get_df(self, path, columns):
        # {"_1": "id", "_2": "wiki_markup", "_3": "iiif", "_4": "media_master", "_5": "title"} ingest df columns
        prefix = b's3' if isinstance(path, bytes) else "s3"
        return self.get_df_s3(path, columns) if path.startswith(prefix) else self.get_df_local(path, columns)

    def get_iiif_urls(self, iiif):
        request = requests.get(iiif)
        data = request.content
        jsonData = json.loads(data)
        # More than one sequence, return empty list and log some kind of message
        sequences = jsonData['sequences']
        if len(sequences) > 1:
            return list()
        elif len(sequences) == 1:
            print(f"Got IIIF at {iiif}")
            print(f"{sequences[0]}")
            return list()  # ['canvases']['images']['resource']['@id']

    def get_parquet_files(self, path):
        return wr.s3.list_objects(path, suffix=".parquet") if path.startswith("s3") else self.get_local_parquet(path)

    def get_local_parquet(self, path):
        posix_files = Path(path).glob('*.parquet')
        files_str = list()
        for p in posix_files:
            files_str.append(f"{p.parent}/{p.name}".encode())
        return files_str

    def sizeof_fmt(self, num, suffix='B'):
        for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Yi', suffix)

    def write_parquet(self, path, data, columns):
        logging.info(f"Saving {path}")
        df_out = pd.DataFrame(data, columns=columns)
        df_out.to_parquet(path)
