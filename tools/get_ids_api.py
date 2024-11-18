import requests

api_query_base = (
    "https://api.dp.la/v2/items?api_key=f584083b954d0fb03cb2b882b100cd51"
    "&provider.name=Minnesota+Digital+Library"
    "&dataProvider.name=Hennepin+County+Library"
    "&rightsCategory=Unlimited+Re-Use"
    "&fields=id"
    "&page_size=500"
)

count = None
page = 1

# shards = [
# "Bethel University",
# "Bethel University Digital Herbarium",
# "Bethel University Digital Library",
# "Blue Earth County Historical Society",
# "Carleton College",
# "Hennepin County Library, James K. Hosmer Special Collections Library",
# "Minitex",
# "Nicollet County Historical Society",
# "Norwegian-American Historical Association",
# "Saint Paul Public Library",
# "St. Cloud State University",
# "Stillwater Public Library",
# "Weavers Guild of Minnesota",
# "University Archives and Southern Minnesota Historical Center, Memorial Library, Minnesota State University, Mankato",
# "Hennepin County Library",
# "Carleton College Archives",
# "The History Center, Archives of Bethel University and Converge Worldwide - BGC",
# ]
shards = [hex(i)[2:].zfill(2) for i in range(256)]

for shard in shards:
    shard = shard.replace(" ", "+")
    page = 0
    while True:
        page += 1
        url = f"{api_query_base}&id={shard}*&page={page}"

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if not data.get("docs", None):
            break
        for doc in data.get("docs"):
            dpla_id = doc.get("id")
            print(dpla_id)

exit(0)
