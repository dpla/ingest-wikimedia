import pandas as pd
import awswrangler as wr
import json
import logging
import requests
from pathlib import Path


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
        # path = path if isinstance(path, bytes) else path.encode('utf-8')
        # print(f"Type of {path} == {type(path)}")
        return pd.read_parquet(path, engine='fastparquet')\
            .rename(columns=columns)

    def get_df(self, path, columns):
        # {"_1": "id", "_2": "wiki_markup", "_3": "iiif", "_4": "media_master", "_5": "title"} ingest df columns
        # prefix = b's3' if isinstance(path, bytes) else "s3"
        # return self.get_df_s3(path, columns) if path.startswith(prefix) else self.get_df_local(path, columns)
        return self.get_df_local(path, columns)

    def get_iiif_urls(self, iiif):
        # sequences \ [array] — [0, default]  \ canvases \ [array] \ images \ [array, 0 default] \ resource \ @id
        canvases = None
        sequences = None
        try:
            request = requests.get(iiif)
            data = request.content
            iiif_manifest = json.loads(data)
            sequences = iiif_manifest['sequences']
        except ConnectionError as ce:
            logging.error(f"Unable to request {iiif}: {ce}")
            return list()
        except Exception as e:
            logging.error(f"Unknown error requesting {iiif}: {e}")
            return list()

        if len(sequences) > 1:
            # More than one sequence, return empty list and log some kind of message
            logging.info(f"Got more than one IIIF sequence. Unsure of meaning. {iiif}")
            return list()
        elif len(sequences) == 1:
            canvases = sequences[0]['canvases']

        if canvases is None:
            logging.info(f"No sequences or canvases in IIIF manifest: {iiif}")
            return list()

        images_urls = list()
        for canvas in canvases:
            image_url = canvas['images'][0]['resource']['@id']
            # if missing file extension add it to URL to be requested
            image_url = image_url if '.' in image_url[image_url.rfind('/'):] else f"{image_url}.jpg"
            images_urls.append(image_url)

        return images_urls

    def get_parquet_files(self, path):
        return wr.s3.list_objects(path, suffix=".parquet") if path.startswith("s3") else self.get_local_parquet(path)

    def get_local_parquet(self, path):
        return Path(path).glob('*.parquet')
        #
        # files_str = list()
        # for p in posix_files:
        #     files_str.append(f"{p.parent}/{p.name}".encode())
        # return files_str

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
