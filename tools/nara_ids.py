import urllib
from typing import IO

import click
import urllib.parse

from ingest_wikimedia.web import get_http_session


def build_collections_params(api_key: str) -> list[str]:
    request_url = (
        "https://api.dp.la/v2/items"
        "?provider.name=%22National%20Archives%20and%20Records%20Administration%22"
        f"&api_key={api_key}"
        "&page_size=0"
        "&facet_size=50000"
        '&sourceResource.collection.title=NOT%20"Records%20of*"%20NOT%20"Naval%20Records%20Collection%20of%20the%20Office%20of%20Naval%20Records%20and%20Library"%20NOT%20"War%20Department%20Collection%20of%20Confederate%20Records"'
        "&facets=sourceResource.collection.title"
    )
    collection_facet_response = get_http_session().get(request_url).json()
    return [
        "exact_field_match=true&sourceResource.collection.title="
        + urllib.parse.quote('"' + collection["term"] + '"', safe="")
        for collection in collection_facet_response["facets"][
            "sourceResource.collection.title"
        ]["terms"]
        if (collection["count"] < 50000)
        and ("Personnel" not in collection["term"])
        and ("Military Files" not in collection["term"])
        and ("Correspondence Files" not in collection["term"])
        and (
            "War Department Collection of Revolutionary War Records"
            not in collection["term"]
        )
    ]


def build_languages_params(api_key: str) -> list[str]:
    request_url = (
        "https://api.dp.la/v2/items"
        "?provider.name=%22National%20Archives%20and%20Records%20Administration%22"
        f"&api_key={api_key}"
        "&page_size=0"
        "&facets=sourceResource.language.name"
        "&facet_size=50000"
    )

    lang_facet_response = get_http_session().get(request_url).json()

    lang_values = [
        '"' + lang["term"] + '"'
        for lang in lang_facet_response["facets"]["sourceResource.language.name"][
            "terms"
        ]
        if lang["term"] != "English"
    ]

    return [
        "sourceResource.language.name=" + "+OR+".join(lang_values[i : i + 10])
        for i in range(0, len(lang_values), 10)
    ]


def build_formats_params(api_key: str) -> list[str]:
    request_url = (
        "https://api.dp.la/v2/items"
        "?provider.name=%22National%20Archives%20and%20Records%20Administration%22"
        f"&api_key={api_key}"
        "&page_size=0"
        "&facets=sourceResource.format"
        "&facet_size=50000"
    )
    format_facet_response = get_http_session().get(request_url).json()

    format_values = [
        '"' + facet["term"] + '"'
        for facet in format_facet_response["facets"]["sourceResource.format"]["terms"]
        if facet["count"] < 12000
    ]

    return [
        "sourceResource.format=" + "+OR+".join(format_values[i : i + 6])
        for i in range(0, len(format_values), 6)
    ]


@click.command()
@click.argument("api_key")
@click.argument("output", type=click.File("w"))
def main(api_key: str, output: IO):
    queries = []
    queries.extend(build_languages_params(api_key))
    queries.extend(build_formats_params(api_key))
    queries.extend(build_collections_params(api_key))

    for query in queries:
        print("  Checking parameters: " + query)
        has_results = True
        page = 0
        count = 0
        base_request_url = (
            "https://api.dp.la/v2/items"
            "?provider.name=%22National%20Archives%20and%20Records%20Administration%22"
            "&page_size=5000"
            f"&api_key={api_key}"
            "&rightsCategory=%22Unlimited+Re-Use%22"
            "&fields=id&" + query
        )
        while has_results:
            page += 1
            request_url = base_request_url + "&page=" + str(page)
            try:
                res = get_http_session().get(request_url).json()

                for item in res["docs"]:
                    output.write(item["id"])
                    output.write("\n")
                    count += 1
                if res["count"] < (res["limit"] + res["start"]):
                    has_results = False
            except Exception as e:
                raise RuntimeError("Error in request: " + request_url) from e

        print("  -- " + str(count) + " added!")
        output.flush()


if __name__ == "__main__":
    main()
