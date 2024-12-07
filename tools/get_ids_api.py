import sys

import click
import requests

from ingest_wikimedia.metadata import DPLA_PARTNERS, check_partner


def run_query(url):
    page = 0
    while True:
        page += 1
        page_url = url + "&page=" + str(page)
        print(page_url, file=sys.stderr)
        response = requests.get(page_url)
        response.raise_for_status()
        data = response.json()
        if not data.get("docs", None):
            break
        for doc in data.get("docs"):
            dpla_id = doc.get("id")
            print(dpla_id)


@click.command()
@click.argument("partner")
@click.argument("api_key")
@click.option("--no-shard", is_flag=True)
@click.option("--add-query")
def main(partner: str, api_key: str, no_shard: bool, add_query: str):
    check_partner(partner)
    partner_string = DPLA_PARTNERS.get(partner).replace(" ", "+")

    api_query_base = (
        f"https://api.dp.la/v2/items?api_key={api_key}"
        f"&provider.name={partner_string}"
        "&rightsCategory=Unlimited+Re-Use"
        "&fields=id"
        "&page_size=500"
    )

    if add_query:
        api_query_base += "&" + add_query

    if not no_shard:
        shards = [hex(i)[2:].zfill(2) for i in range(256)]
        for shard in shards:
            url = f"{api_query_base}&id={shard}*"
            run_query(url)
    else:
        run_query(api_query_base)


if __name__ == "__main__":
    main()
