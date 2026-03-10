import json, requests, pywikibot, datetime, argparse

parse = argparse.ArgumentParser()
parse.add_argument('--cat', dest='cat', metavar='CATEGORY',
                    action='store')
arg = parse.parse_args()

site = pywikibot.Site()
site.login()

updated = 0
none = 0
exists = 0
fulfilled = 0
already = 0
requested = 0

datapage = {}
datapage['license'] = 'CC0-1.0'
datapage['schema'] = { 'fields': [ { 'name': 'timestamp', 'type': 'string', 'title': { 'en': 'Month' } }, { 'name': 'pageviews', 'type': 'number', 'title': { 'en': 'Pageviews' } } ] }

chartpage = { 'license': 'CC0-1.0', 'version': 1, "type": 'area', 'xAxis': { 'title': { 'en': 'Month' }, 'type': 'date' }, 'yAxis': { 'title': { 'en': 'Views' } } }

HEADERS = {'User-Agent': 'DPLA-Bot/1.0 (https://dp.la; tech@dp.la) python-requests'}

ALLOW_LIST_URL = 'https://gitlab.wikimedia.org/api/v4/projects/repos%2Fdata-engineering%2Fairflow-dags/repository/files/main%2Fdags%2Fcommons%2Fcommons_category_allow_list.tsv/raw?ref=main'

cimlist = []
print('\n****\n')
allow_list_response = requests.get(ALLOW_LIST_URL, headers=HEADERS, timeout=30)
for line in allow_list_response.text.splitlines():
    if line.strip():
        cimlist.append('Category:' + line.strip().replace('_',' ').replace('%26', '&').replace('%27','\''))
cimcat = pywikibot.Category(site, 'Category requested for Commons Impact Metrics')
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
    load = json.loads(requests.get('https://wikimedia.org/api/rest_v1/metrics/commons-analytics/pageviews-per-category-monthly/' + cat + '/deep/all-wikis/00000101/99991231', headers=HEADERS, timeout=30).text)

    outputs = []
    for month in load['items']:
        outputs.append([ month['timestamp'][0:7], month['pageview-count'] ])

    return outputs

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

    datapage['sources'] = 'Copied from [https://wikimedia.org/api/rest_v1/metrics/commons-analytics/pageviews-per-category-monthly/' + cat.replace(' ','_').replace('Category:','') + '/deep/all-wikis/00000101/99991231 Commons Impact Metrics].'
    datapage['description'] = {'en': 'Data from commons-analytics/pageviews-per-category-monthly endpoint for ' + cat.replace(' ','_')}

    pagename = pywikibot.Page(site, cat.replace('Category:','Data:Views/') + '.tab')
    chartpagename = pywikibot.Page(site, cat.replace('Category:','Data:Views/') + '.chart')
    current = False

    if pagename.exists():
        now = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime("%Y-%m")
        if now in pagename.text:

            current = True
            print('   Data already up-to-date!')
            exists += 1

    if not current:
        category = pywikibot.Page(site, cat)
        try:
            datapage['data'] = get_data(cat.replace(' ','_').replace('&', '%26').replace('Category:',''))
        except KeyError:
            if cat not in cimlist:
                print('   Not found in Commons Impact Metrics.')
                if cimcat not in category.categories():
                    category.text += '\n[[Category:Category requested for Commons Impact Metrics]]'
                    category.save(summary='Adding category: [[Category:Category requested for Commons Impact Metrics]]')
                    print('   Category requested for Commons Impact Metrics!')
                    requested += 1
                else:
                    print('   Category already requested for Commons Impact Metrics.')
                    already += 1
            else:
                print('   Data still generating in Commons Impact Metrics.')
                none += 1
            continue
        pagename.text = json.dumps(datapage, indent=4)
        pagename.save('Adding tabular data for category page views from Commons Impact Metrics.')
        updated += 1

        try:
            category.touch()
        except:
            pass

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
