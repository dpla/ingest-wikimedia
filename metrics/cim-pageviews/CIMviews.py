# CIMviews.py
# Updates Commons Impact Metrics (CIM) pageview data on Wikimedia Commons.
#
# Two modes, controlled by --mode:
#
#   data (default): For each category tracked by {{views from category}},
#     fetches monthly pageview data from the CIM API and writes/updates a
#     tabular data page (Data:Views/<category>.tab) and chart page
#     (Data:Views/<category>.chart) on Wikimedia Commons.
#
#   categories: Manages the CIM category request workflow. Checks
#     subcategories of "Category requested for Commons Impact Metrics" and
#     removes those that are now in the CIM allow list (fulfilled). Also
#     adds "Category requested for Commons Impact Metrics" to any tracked
#     category not yet in the allow list.
#
# Usage:
#   python CIMviews.py                          # data mode, all categories
#   python CIMviews.py --cat "Category:Foo"     # data mode, single category
#   python CIMviews.py --mode categories        # category request management

import json, requests, pywikibot, datetime, argparse

# --cat: run data mode on a single category instead of the full list
# --mode: select workflow (data or categories)
parse = argparse.ArgumentParser()
parse.add_argument('--cat', dest='cat', metavar='CATEGORY',
                    action='store')
parse.add_argument('--mode', dest='mode', choices=['data', 'categories'], default='data')
arg = parse.parse_args()

# Connect and authenticate to Wikimedia Commons
site = pywikibot.Site()
site.login()

# Counters for the summary report
updated = 0   # data pages written/updated
none = 0      # categories in allow list but data not yet generated
exists = 0    # categories already up-to-date, skipped
fulfilled = 0  # category requests removed (now in allow list)
already = 0   # categories already pending a CIM request
requested = 0  # new CIM category requests added

# Template for tabular data pages (Data:Views/<category>.tab)
# Schema defines the two columns: timestamp (YYYY-MM) and pageview count
datapage = {}
datapage['license'] = 'CC0-1.0'
datapage['schema'] = { 'fields': [ { 'name': 'timestamp', 'type': 'string', 'title': { 'en': 'Month' } }, { 'name': 'pageviews', 'type': 'number', 'title': { 'en': 'Pageviews' } } ] }

# Template for chart pages (Data:Views/<category>.chart)
# References the corresponding .tab page as its data source
chartpage = { 'license': 'CC0-1.0', 'version': 1, "type": 'area', 'xAxis': { 'title': { 'en': 'Month' }, 'type': 'date' }, 'yAxis': { 'title': { 'en': 'Views' } } }

# Required by the Wikimedia API; requests without a User-Agent return 403
HEADERS = {'User-Agent': 'DPLA-Bot/1.0 (https://dp.la; tech@dp.la) python-requests'}

# Canonical allow list of categories approved for Commons Impact Metrics,
# fetched live from the Wikimedia data-engineering Airflow DAGs repo
ALLOW_LIST_URL = 'https://gitlab.wikimedia.org/api/v4/projects/repos%2Fdata-engineering%2Fairflow-dags/repository/files/main%2Fdags%2Fcommons%2Fcommons_category_allow_list.tsv/raw?ref=main'

# Fetch the allow list and normalise entries to match pywikibot category titles
# (underscores to spaces, percent-encoded characters decoded)
cimlist = []
print('\n****\n')
allow_list_response = requests.get(ALLOW_LIST_URL, headers=HEADERS, timeout=30)
for line in allow_list_response.text.splitlines():
    if line.strip():
        cimlist.append('Category:' + line.strip().replace('_',' ').replace('%26', '&').replace('%27','\''))

# The category used to signal that a category has been requested for CIM
cimcat = pywikibot.Category(site, 'Category requested for Commons Impact Metrics')

# --- CATEGORIES MODE: fulfillment check ---
# Remove "Category requested for Commons Impact Metrics" from any category
# that has since been added to the allow list (i.e. the request was fulfilled)
if arg.mode == 'categories':
    cimreqs = cimcat.subcategories()
    for cimreq in cimreqs:
        category = pywikibot.Page(site, cimreq.title())
        cimreq = cimreq.title()
        if cimreq in cimlist:
            print(cimreq)
            category.change_category(cimcat, None, 'Remove category: [[Category:Category requested for Commons Impact Metrics]]')
            print('Removed request for ' + cimreq)
            fulfilled += 1


def get_data(cat):
    """Fetch monthly pageview data for a category from the CIM API.

    Returns a list of [YYYY-MM, count] pairs. Raises KeyError if the
    category has no data yet (API response contains no 'items' key).
    """
    load = json.loads(requests.get('https://wikimedia.org/api/rest_v1/metrics/commons-analytics/pageviews-per-category-monthly/' + cat + '/deep/all-wikis/00000101/99991231', headers=HEADERS, timeout=30).text)

    outputs = []
    for month in load['items']:
        outputs.append([ month['timestamp'][0:7], month['pageview-count'] ])

    return outputs


# Build the list of categories to process.
# --cat overrides to a single category; otherwise use all subcategories of
# "Category with page views table" (excluding the maintenance subcategory).
if arg.cat:
    cats = [arg.cat, ]

else:
    print('\nFinding categories to work on... (' + str(datetime.datetime.now()) + ')')
    cats = []
    update_cat = pywikibot.Category(site, 'Category with page views table')

    for u in update_cat.subcategories():
        cats.append(u.title())
    cats.remove('Category:Category page views need to be updated')
    print('{{views from category}} categories retrieved! (' + str(datetime.datetime.now()) + ')')

for cat in cats:
    print('\n' + cat)

    # --- CATEGORIES MODE: request management ---
    # For categories not yet in the allow list, add a CIM request if one
    # isn't already pending. Categories in the allow list are skipped silently.
    if arg.mode == 'categories':
        category = pywikibot.Page(site, cat)
        if cat not in cimlist:
            if cimcat not in category.categories():
                category.text += '\n[[Category:Category requested for Commons Impact Metrics]]'
                category.save(summary='Adding category: [[Category:Category requested for Commons Impact Metrics]]')
                print('   Category requested for Commons Impact Metrics!')
                requested += 1
            else:
                print('   Category already requested for Commons Impact Metrics.')
                already += 1
        continue

    # --- DATA MODE ---
    # Set per-category metadata fields on the data page template
    datapage['sources'] = 'Copied from [https://wikimedia.org/api/rest_v1/metrics/commons-analytics/pageviews-per-category-monthly/' + cat.replace(' ','_').replace('Category:','') + '/deep/all-wikis/00000101/99991231 Commons Impact Metrics].'
    datapage['description'] = {'en': 'Data from commons-analytics/pageviews-per-category-monthly endpoint for ' + cat.replace(' ','_')}

    pagename = pywikibot.Page(site, cat.replace('Category:','Data:Views/') + '.tab')
    chartpagename = pywikibot.Page(site, cat.replace('Category:','Data:Views/') + '.chart')
    current = False

    # Check if the data page already contains last month's data; if so, skip
    if pagename.exists():
        now = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime("%Y-%m")
        if now in pagename.text:

            current = True
            print('   Data already up-to-date!')
            exists += 1

    if not current:
        category = pywikibot.Page(site, cat)
        try:
            # Encode the category name for the API URL (spaces to underscores,
            # ampersands percent-encoded, "Category:" prefix stripped)
            datapage['data'] = get_data(cat.replace(' ','_').replace('&', '%26').replace('Category:',''))
        except KeyError:
            # API returned a response but with no 'items' — either the category
            # isn't in CIM yet, or data is still being generated
            if cat not in cimlist:
                print('   Not found in Commons Impact Metrics.')
            else:
                print('   Data still generating in Commons Impact Metrics.')
                none += 1
            continue
        pagename.text = json.dumps(datapage, indent=4)
        pagename.save('Adding tabular data for category page views from Commons Impact Metrics.')
        updated += 1

        # Touch the category page to trigger template re-renders
        try:
            category.touch()
        except:
            pass

    # Create the chart page if the data page exists but the chart page doesn't
    if pagename.exists() and not chartpagename.exists():
        print(chartpagename)
        print(cat.replace('Category:','Views/') + '.tab')
        chartpage['source'] = cat.replace('Category:','Views/') + '.tab'
        print(chartpage)
        chartpagename.text = json.dumps(chartpage, indent=4, ensure_ascii=False)
        print(chartpagename.text)
        chartpagename.save('   Creating chart page for category page views from Commons Impact Metrics.')

print("""
****
| -- Total categories currently in Commons Impact Metrics: """ + str(len(cimlist)) + """
| -- Categories with updated data: """ + str(updated) + """
| -- Categories with no data detected: """ + str(none) + """
| -- Categories already up to date: """ + str(exists) + """
| -- Category requests fulfilled: """ + str(fulfilled) + """
| -- Categories already requested to be added: """ + str(already) + """
| -- Categories newly requested to be added: """ + str(requested) + """
| -- Total categories currently utilizing this tool: """ + str(len(cats)) + """
****
""")
