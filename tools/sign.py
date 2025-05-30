import logging
import time

import click

from ingest_wikimedia.logs import setup_logging
from ingest_wikimedia.common import CHECKSUM
from tqdm import tqdm

from ingest_wikimedia.tools_context import ToolsContext


@click.command()
@click.argument("partner")
def main(partner: str):
    start_time = time.time()
    tools_context = ToolsContext.init()
    dpla = tools_context.get_dpla()
    setup_logging(partner, "sign", logging.INFO)
    dpla.check_partner(partner)
    logging.info(f"Starting signing for {partner}")
    s3 = tools_context.get_s3_client().get_s3()
    local_fs = tools_context.get_local_fs()
    local_fs.setup_temp_dir()
    bucket = s3.Bucket("dpla-wikimedia")

    try:
        for object_summary in tqdm(
            bucket.objects.filter(Prefix=f"{partner}/images/").all(),
            "Signing files",
            unit="File",
            ncols=100,
        ):
            temp_file = local_fs.get_temp_file()
            temp_file_name = temp_file.name
            try:
                tqdm.write(object_summary.key)
                obj = object_summary.Object()
                sha1 = obj.metadata.get(CHECKSUM, "")
                if sha1 != "":
                    continue

                with tqdm(
                    total=obj.content_length,
                    leave=False,
                    desc="S3 Download",
                    unit="B",
                    unit_divisor=1024,
                    unit_scale=True,
                    delay=2,
                    ncols=100,
                ) as progress_bar:
                    # this is the first time I feel like I wrote something just to make
                    # SonarQube happy.
                    def make_progress_callback(progress):
                        def update_progress(bytes_xfer):
                            progress.update(bytes_xfer)

                        return update_progress

                    callback = make_progress_callback(progress_bar)
                    obj.download_file(
                        temp_file_name,
                        Callback=callback,
                    )
                sha1 = local_fs.get_file_hash(temp_file_name)
                content_type = local_fs.get_content_type(temp_file_name)
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
                local_fs.clean_up_tmp_file(temp_file)
    finally:
        logging.info(f"{time.time() - start_time} seconds.")
        local_fs.cleanup_temp_dir()


if __name__ == "__main__":
    main()
