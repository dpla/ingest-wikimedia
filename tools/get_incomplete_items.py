import click

from ingest_wikimedia.s3 import get_s3, S3_BUCKET


def get_incomplete_items(prefix) -> list[str]:
    """Gets a list of incomplete items from S3."""
    s3 = get_s3()
    bucket = s3.Bucket(S3_BUCKET)
    incomplete_items = []

    for object_summary in bucket.objects.filter(Prefix=f"{prefix}/images/"):
        key = object_summary.key
        if key.endswith("file-list.txt"):
            dpla_id = key.split("/")[-2]
            folder = "/".join(key.split("/")[0:-1])
            file_list = object_summary.get()["Body"].read().decode("utf-8")
            file_count = len(file_list.split("\n"))
            media_files = []
            for object_summary2 in bucket.objects.filter(Prefix=folder):
                key = object_summary2.key
                if (
                    key.endswith("file-list.txt")
                    or key.endswith("dpla-map.json")
                    or key.endswith("iiif.json")
                ):
                    continue
                media_files.append(key)

            if len(media_files) != file_count:
                incomplete_items.append(dpla_id)

    return incomplete_items


@click.command()
@click.argument("partner")
def main(partner: str):
    for item_id in get_incomplete_items(partner):
        print(item_id)


if __name__ == "__main__":
    main()
