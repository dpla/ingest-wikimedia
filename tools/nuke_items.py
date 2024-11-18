import logging
import os
import time
from typing import IO

import click
from tqdm import tqdm

from ingest_wikimedia.common import load_ids
from ingest_wikimedia.logs import setup_logging
from ingest_wikimedia.metadata import check_partner
from ingest_wikimedia.s3 import get_item_s3_path, S3_BUCKET


@click.command()
@click.argument("ids_file", type=click.File("r"))
@click.argument("partner")
@click.option("--dry-run", is_flag=True)
def main(ids_file: IO, partner: str, dry_run: bool):
    start_time = time.time()
    check_partner(partner)
    setup_logging(partner, "nuke-items", logging.INFO)
    logging.info(f"Nuking items for {partner}")
    dpla_ids = load_ids(ids_file)
    for dpla_id in tqdm(dpla_ids, desc="Nuking Items", unit="Item"):
        logging.info(f"DPLA ID: {dpla_id}")
        s3_path = get_item_s3_path(dpla_id, "", partner)
        command = f"aws s3 rm s3://{S3_BUCKET}/{s3_path} --recursive --dryrun"
        if dry_run:
            command = command + " --dryrun"
        os.system(command)

    logging.info(f"{time.time() - start_time} seconds.")


if __name__ == "__main__":
    main()
