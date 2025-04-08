import logging
import subprocess
import time
from typing import IO

import click
from tqdm import tqdm

from ingest_wikimedia.common import load_ids
from ingest_wikimedia.logs import setup_logging

from ingest_wikimedia.s3 import S3_BUCKET
from ingest_wikimedia.tools_context import ToolsContext


@click.command()
@click.argument("ids_file", type=click.File("r"))
@click.argument("partner")
@click.option("--dry-run", is_flag=True)
def main(ids_file: IO, partner: str, dry_run: bool):
    start_time = time.time()
    tools_context = ToolsContext.init()
    s3 = tools_context.get_s3_client()
    dpla = tools_context.get_dpla()
    dpla.check_partner(partner)
    setup_logging(partner, "nuke-items", logging.INFO)
    logging.info(f"Nuking items for {partner}")
    dpla_ids = load_ids(ids_file)
    for dpla_id in tqdm(dpla_ids, desc="Nuking Items", unit="Item", ncols=100):
        logging.info(f"DPLA ID: {dpla_id}")
        s3_path = s3.get_item_s3_path(dpla_id, "", partner)
        command = f"aws s3 rm s3://{S3_BUCKET}/{s3_path} --recursive"
        if dry_run:
            command = command + " --dryrun"
        subprocess.run(command, shell=True, check=True)

    logging.info(f"{time.time() - start_time} seconds.")


if __name__ == "__main__":
    main()
