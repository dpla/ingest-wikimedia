import logging
import time

import click

from ingest_wikimedia.logs import setup_logging
from ingest_wikimedia.metadata import check_partner
from ingest_wikimedia.s3 import get_s3
from ingest_wikimedia.common import CHECKSUM
from ingest_wikimedia.local import (
    get_temp_file,
    clean_up_tmp_file,
    setup_temp_dir,
    cleanup_temp_dir,
    get_file_hash,
    get_content_type,
)
from tqdm import tqdm


@click.command()
@click.argument("partner")
def main(partner: str):
    start_time = time.time()
    setup_logging(partner, "sign", logging.INFO)
    check_partner(partner)
    logging.info(f"Starting signing for {partner}")
    s3 = get_s3()
    setup_temp_dir()
    bucket = s3.Bucket("dpla-wikimedia")

    try:
        for object_summary in tqdm(
            bucket.objects.filter(Prefix=f"{partner}/images/").all(),
            "Signing files",
            unit="File",
        ):
            temp_file = get_temp_file()
            temp_file_name = temp_file.name
            try:
                tqdm.write(object_summary.key)
                obj = object_summary.Object()
                sha1 = obj.metadata.get(CHECKSUM, "")
                if sha1 != "":
                    pass

                with tqdm(
                    total=obj.content_length,
                    leave=False,
                    desc="S3 Download",
                    unit="B",
                    unit_scale=1024,
                    unit_divisor=True,
                    delay=2,
                ) as t:
                    obj.download_file(
                        temp_file_name,
                        Callback=lambda bytes_xfer: t.update(bytes_xfer),
                    )
                sha1 = get_file_hash(temp_file_name)
                content_type = get_content_type(temp_file_name)
                tqdm.write(f"{obj.key} {content_type} {sha1}")
                obj.metadata.update({CHECKSUM: sha1})
                obj.copy_from(
                    CopySource={"Bucket": bucket.name, "Key": obj.key},
                    ContentType=content_type,
                    Metadata=obj.metadata,
                    MetadataDirective="REPLACE",
                )
            except Exception as e:
                tqdm.write(str(e))
            finally:
                clean_up_tmp_file(temp_file)
    finally:
        logging.info(f"{time.time() - start_time} seconds.")
        cleanup_temp_dir()


if __name__ == "__main__":
    main()
