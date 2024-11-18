import json

import requests


def get_data_from_page(page):
    if "hits" in page and "hits" in page["hits"] and len(page["hits"]["hits"]) > 0:
        results = []
        for obj in page["hits"]["hits"]:
            try:
                results.append(obj["_id"])
            except KeyError:
                print("Key failure: ", obj)
                exit(1)
        return results
    else:
        return []


def get_search_after(page: dict):
    return page["hits"]["hits"][-1]["sort"]


template_query = {
    "_source": ["id"],
    "size": 200,
    "sort": ["id", "_doc"],
}

search_after = None
response = None

while True:
    if search_after:
        template_query["search_after"] = search_after

    response = requests.post(
        "http://search-prod1.internal.dp.la:9200/dpla_alias/_search",
        headers={"Content-Type": "application/json"},
        data=json.dumps(template_query),
    )
    page_json = response.json()
    page_data = get_data_from_page(page_json)
    for dpla_id in page_data:
        print(dpla_id)
    if len(page_data) == 0:
        break
    search_after = get_search_after(response.json())
