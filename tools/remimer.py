import click
import time
import logging

from tqdm import tqdm


from ingest_wikimedia.s3 import S3_BUCKET
from ingest_wikimedia.tools_context import ToolsContext

from ingest_wikimedia.logs import setup_logging


@click.command()
@click.argument("partner")
@click.option("--dry-run", is_flag=True)
def main(partner: str, dry_run: bool) -> None:
    start_time = time.time()
    tools_context = ToolsContext.init(partner)
    dpla = tools_context.get_dpla()
    tracker = tools_context.get_tracker()
    local_fs = tools_context.get_local_fs()
    local_fs.setup_temp_dir()

    dpla.check_partner(partner)

    try:
        setup_logging(partner, "remimer", logging.INFO)
        if dry_run:
            logging.warning("---=== DRY RUN ===---")

        s3 = tools_context.get_s3_client().get_s3()
        bucket = s3.Bucket(S3_BUCKET)

        for object_summary in tqdm(
            bucket.objects.filter(
                Prefix=f"{partner}/images/",
            ),
            desc="Processing Items",
            unit="Item",
            ncols=100,
        ):
            key = object_summary.key
            if not key.endswith(".txt") and not key.endswith(".json"):
                obj = object_summary.Object()
                if (
                    obj.content_type == "binary/octet-stream"
                    or obj.content_type == "application/octet-stream"
                ):
                    with local_fs.get_temp_file() as data:
                        obj.download_fileobj(data)
                        content_type = local_fs.get_content_type(data.name)
                        logging.info(f"Updating {key} to {content_type}")
                        if not dry_run:
                            s3.meta.client.copy_object(
                                Bucket=S3_BUCKET,
                                Key=key,
                                ContentType=content_type,
                                MetadataDirective="REPLACE",
                                CopySource=S3_BUCKET + "/" + key,
                            )

    finally:
        logging.info("\n" + str(tracker))
        logging.info(f"{time.time() - start_time} seconds.")
        local_fs.cleanup_temp_dir()


if __name__ == "__main__":
    main()
