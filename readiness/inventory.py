import datetime
import json
import os
import re
import time

import requests
from urllib.parse import urlparse, quote, quote_plus

API_KEY = os.environ.get('DPLA_API_KEY', 'YOUR_DPLA_API_KEY_HERE')
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(SCRIPT_DIR, 'inventory_data.json')
README_FILE = os.path.join(SCRIPT_DIR, 'README.md')

RETRY_LIMIT = 10
RETRY_SLEEP = 60  # seconds between retries on failure

contentdm_re = re.compile(
    r'^https?://[^/]+/(?:digital|cdm(?:/ref)?)/collection/[^/]+/id/\d+(?:/.*)?$',
    re.IGNORECASE,
)


def fetch(url):
    for attempt in range(RETRY_LIMIT):
        try:
            r = requests.get(url, params={'api_key': API_KEY}, timeout=30)
            if r.status_code == 200:
                return json.loads(r.text)
            if r.status_code == 400:
                print("  HTTP 400 (bad request, skipping — will not retry)")
                return None
            print(f"  HTTP {r.status_code}, retrying in {RETRY_SLEEP}s... (attempt {attempt + 1}/{RETRY_LIMIT})")
        except Exception as e:
            print(f"  Error: {e}, retrying in {RETRY_SLEEP}s... (attempt {attempt + 1}/{RETRY_LIMIT})")
        time.sleep(RETRY_SLEEP)
    raise Exception(f"Failed after {RETRY_LIMIT} attempts: {url}")


def facet_to_dict(result):
    """Converts a dataProvider facet result into a {name: count} dict."""
    return {term['term']: term['count'] for term in result['facets']['dataProvider']['terms']}


def dpla_search_url(name, hub):
    q = '"'
    return f"https://dp.la/search?provider={quote_plus(q + name + q)}&partner={quote_plus(q + hub + q)}"


def readiness_stmt(name, unlim, old_other, total, label="domain", parenthetical=None, hub=None):
    pct = f"{round(unlim / total * 100)}%" if total > 0 and unlim > 0 else "0%"
    total_fmt = f"{total:,}"
    suffix = "across all collections hosted on this domain" if label == "domain" else "already"

    if unlim >= 5000:
        level = 'very high'
    elif unlim >= 1000:
        level = 'high'
    elif unlim >= 100:
        level = 'moderate'
    elif unlim > 0:
        level = 'low'
    else:
        level = 'very low'

    if label == "domain":
        name_md = f"'[**{name}**](https://{name})'"
        paren_md = (f" *(e.g. '[{parenthetical}]({dpla_search_url(parenthetical, hub)})')*"
                    if parenthetical and hub else "")
    else:
        safe_name = name.replace('|', r'\|').replace('[', r'\[').replace(']', r'\]')
        name_md = f"'[**{safe_name}**]({dpla_search_url(name, hub)})'" if hub else f"'**{safe_name}**'"
        paren_md = f" *([{parenthetical}](https://{parenthetical}))*" if parenthetical else ""

    if unlim > 0:
        pd_items = "item" if unlim == 1 else "items"
        stmt = (f"{name_md}{paren_md} has a **{level}** readiness score because it has "
                f"**{unlim:,}** eligible public domain {pd_items} (**{pct}** of **{total_fmt}** total items) "
                f"{suffix} in CONTENTdm or providing a media location")
        if old_other > 0:
            add_items = "item" if old_other == 1 else "items"
            stmt += (f", and **{old_other:,}** additional {add_items} over 120 years old in other rights "
                     f"categories that could become eligible with rights review")
        stmt += "."
    elif old_other > 0:
        add_items = "item" if old_other == 1 else "items"
        stmt = (f"{name_md}{paren_md} has a **very low** readiness score with no public domain items, "
                f"but has **{old_other:,}** {add_items} over 120 years old in other rights categories "
                f"that could become eligible with rights review.")
    else:
        stmt = (f"{name_md}{paren_md} has a **very low** readiness score with no public domain items "
                f"and no items dated over 120 years old.")
    return stmt


def update_readme(hub, output_filename=None, note=None):
    """Add a new row to the Inventories table in README.md if not already present,
    inserting it at the correct alphabetical position.

    Pass output_filename for a normal inventory link, or note for a plain-text
    second column (e.g. hubs skipped because upload=True hub-wide)."""
    if not os.path.exists(README_FILE):
        print(f"  WARNING: README.md not found at {README_FILE}")
        return

    with open(README_FILE, 'r') as f:
        content = f.read()

    q = '"'
    hub_url = f"https://dp.la/search?partner={quote_plus(q + hub + q)}"

    if hub_url in content:
        return  # Already present

    col2 = f"[{output_filename}]({output_filename})" if output_filename else (note or "already participating hub-wide")
    new_row = f"| [{hub}]({hub_url}) | {col2} |"

    lines = content.split('\n')
    table_row_indices = []
    in_inventories_table = False

    for i, line in enumerate(lines):
        if '| Hub |' in line:
            in_inventories_table = True
        if in_inventories_table and line.startswith('|'):
            table_row_indices.append(i)
        elif in_inventories_table and not line.startswith('|'):
            in_inventories_table = False

    if not table_row_indices:
        print("  WARNING: Could not find Inventories table in README.md")
        return

    # Find the alphabetically correct insertion point among data rows
    # (skip the header row and separator row, which are the first two entries)
    data_row_indices = table_row_indices[2:]
    insert_after = table_row_indices[1]  # default: after separator, before first data row
    for idx in data_row_indices:
        existing_hub = re.search(r'\[([^\]]+)\]', lines[idx])
        if existing_hub and existing_hub.group(1).lower() < hub.lower():
            insert_after = idx

    lines.insert(insert_after + 1, new_row)
    with open(README_FILE, 'w') as f:
        f.write('\n'.join(lines))
    print(f"  Updated README.md: added row for {hub}")


def process_hub(hub, pipelinejson, all_data, cutoff_year):
    print(f"\n{'=' * 60}")
    print(f"Processing hub: {hub}")
    print(f"{'=' * 60}")

    # Check hub-level pipeline skip
    hub_pipeline_entry = pipelinejson.get(hub, {})
    if hub_pipeline_entry.get('upload') is True:
        print(f"  Skipping {hub}: hub-level upload=True (already a full pipeline partner)")
        update_readme(hub, note="already participating hub-wide")
        return

    hub_encoded = hub.replace(' ', '%20')
    hub_file_slug = hub.lower().replace(' ', '_').replace('/', '_')
    hub_md_slug = hub.lower().replace(' ', '-').replace('/', '-')
    output_file = os.path.join(SCRIPT_DIR, f'{hub_file_slug}_inventory.md')

    # Load or initialize cache for this hub
    if all_data.get(hub, {}).get('_no_eligible'):
        print("  Cached: no eligible institutions. Skipping.")
        update_readme(hub, note="no eligible items")
        return
    if hub in all_data:
        data = all_data[hub]
        if '_rights' not in data:
            data['_rights'] = {}
        if 'contentdm' not in data:
            data['contentdm'] = {}
        if '_domains' not in data:
            data['_domains'] = {}
        if 'iiif_manifest' not in data:
            data['iiif_manifest'] = {}
        if 'media_master' not in data:
            data['media_master'] = {}
        print(f"  Loaded cache: {len(data.get('_rights', {}))} rights queries, "
              f"{len(data.get('contentdm', {}))} media checks, "
              f"{len(data.get('_domains', {}))} domain checks.")
    else:
        data = {'_rights': {}, 'contentdm': {}, 'iiif_manifest': {}, 'media_master': {}, '_domains': {}}
        print(f"  No cache found for {hub}, starting fresh.")

    BASE = (f'https://api.dp.la/v2/items?provider=%22{hub_encoded}%22'
            f'&page_size=0&facets=dataProvider&facet_size=2000')

    # Phase 1: Four global facet queries — one per rights category of interest.
    # These return exact per-institution counts with no fuzzy-match conflation.
    rights_queries = {
        'unlim':      BASE + '&rightsCategory=%22Unlimited%20Re-Use%22',
        'old_condit': BASE + f'&rightsCategory=%22Re-use%20With%20Conditions%22&sourceResource.date.before={cutoff_year}',
        'old_nomod':  BASE + f'&rightsCategory=%22Re-use%2C%20No%20Modification%22&sourceResource.date.before={cutoff_year}',
        'old_unspec': BASE + f'&rightsCategory=%22Unspecified%20Rights%20Status%22&sourceResource.date.before={cutoff_year}',
    }

    for key, url in rights_queries.items():
        if key in data['_rights']:
            print(f"  Cached rights data: {key} ({len(data['_rights'][key])} institutions)")
        else:
            print(f"  Fetching global rights data: {key}...")
            data['_rights'][key] = facet_to_dict(fetch(url))
            all_data[hub] = data
            with open(DATA_FILE, 'w') as f:
                json.dump(all_data, f, indent=2)
            print(f"    Done. {len(data['_rights'][key])} institutions found.")

    hub_institutions_pipeline = hub_pipeline_entry.get('institutions', {})

    # Phase 2: Institutions list
    print("  Fetching institutions list...")
    institutionsjson = fetch(
        f'https://api.dp.la/v2/items?facets=dataProvider&provider=%22{hub_encoded}%22'
        f'&page_size=0&facet_size=2000'
    )
    institutions = institutionsjson['facets']['dataProvider']['terms']
    total = len(institutions)

    def is_fully_cached(name):
        """An institution is fully cached if contentdm=True (already eligible, no need to
        check iiif/media), or if all three media fields have been checked."""
        if name not in data['contentdm'] or name not in data['_domains']:
            return False
        if data['contentdm'][name]:
            return True  # CONTENTdm=True → eligible regardless; skip re-fetch
        return name in data['iiif_manifest'] and name in data['media_master']

    need_sample = sum(1 for inst in institutions if not is_fully_cached(inst['term']))
    print(f"  Found {total} institutions. {total - need_sample} fully cached, {need_sample} need sample doc fetch.\n")

    # Phase 3: Per-institution media platform detection and domain extraction.
    # Checks CONTENTdm URL pattern, iiifManifest field, and mediaMaster field.
    # Uses fuzzy dataProvider= which is fine — we only need yes/no platform detection,
    # and hyphen-variant pairs are always the same library system using the same platform.
    for i, institution in enumerate(institutions):
        name = institution['term']
        if is_fully_cached(name):
            print(f"  [{i + 1}/{total}] Cached: {name}")
            continue

        print(f"  [{i + 1}/{total}] Fetching sample doc: {name}")
        encoded = '"' + quote(name, safe=' ') + '"'
        sample = fetch(
            f'https://api.dp.la/v2/items?dataProvider={encoded}'
            f'&provider=%22{hub_encoded}%22&page_size=1'
        )

        if sample and sample['docs']:
            doc = sample['docs'][0]
            isShownAt = doc.get('isShownAt', '')
            data['contentdm'][name] = bool(contentdm_re.match(isShownAt))
            data['iiif_manifest'][name] = 'iiifManifest' in doc
            data['media_master'][name] = 'mediaMaster' in doc
            data['_domains'][name] = urlparse(isShownAt).netloc
        else:
            data['contentdm'][name] = False
            data['iiif_manifest'][name] = False
            data['media_master'][name] = False
            data['_domains'][name] = None

        all_data[hub] = data
        with open(DATA_FILE, 'w') as f:
            json.dump(all_data, f, indent=2)

    # If no eligible institutions found, skip output and cache the result
    def inst_is_eligible(name):
        return (data['contentdm'].get(name, False)
                or data['iiif_manifest'].get(name, False)
                or data['media_master'].get(name, False))

    any_eligible = any(inst_is_eligible(inst['term']) for inst in institutions)
    if not any_eligible:
        print(f"\n  No eligible institutions found for {hub}. Skipping output.")
        all_data[hub] = {'_no_eligible': True}
        with open(DATA_FILE, 'w') as f:
            json.dump(all_data, f, indent=2)
        update_readme(hub, note="no eligible items")
        return

    print("\n  All data collected. Generating output...\n")

    # Phase 4: Build per-institution records
    institutions_data = []
    for institution in institutions:
        name = institution['term']
        unlim     = data['_rights']['unlim'].get(name, 0)
        old_other = (data['_rights']['old_condit'].get(name, 0) +
                     data['_rights']['old_nomod'].get(name, 0) +
                     data['_rights']['old_unspec'].get(name, 0))
        is_contentdm = data['contentdm'].get(name, False)
        has_iiif = data['iiif_manifest'].get(name, False)
        has_media = data['media_master'].get(name, False)
        pipeline_entry = hub_institutions_pipeline.get(name)
        is_partner = pipeline_entry is not None and pipeline_entry.get('upload') is True

        institutions_data.append({
            'name': name,
            'is_eligible': is_contentdm or has_iiif or has_media,
            'is_partner': is_partner,
            'domain': data['_domains'].get(name),
            'total': institution['count'],
            'unlim': unlim,
            'old_other': old_other,
        })

    eligible = [i for i in institutions_data if not i['is_partner'] and i['is_eligible']]
    eligible.sort(key=lambda i: (-i['unlim'], -i['old_other']))

    # Phase 5: Group by domain
    domain_map = {}
    for inst in institutions_data:
        domain = inst['domain']
        if not domain:
            continue
        if domain not in domain_map:
            domain_map[domain] = {
                'domain': domain,
                'is_eligible': False,
                'all_partners': True,   # flipped to False as soon as one non-partner is found
                'total': 0,
                'unlim': 0,
                'old_other': 0,
                'largest_inst': '',
                'largest_inst_total': 0,
            }
        domain_map[domain]['unlim']     += inst['unlim']
        domain_map[domain]['old_other'] += inst['old_other']
        domain_map[domain]['total']     += inst['total']
        if inst['is_eligible']:
            domain_map[domain]['is_eligible'] = True
        if not inst['is_partner']:
            domain_map[domain]['all_partners'] = False
        if inst['total'] > domain_map[domain]['largest_inst_total']:
            domain_map[domain]['largest_inst'] = inst['name']
            domain_map[domain]['largest_inst_total'] = inst['total']

    eligible_domains = [d for d in domain_map.values() if not d['all_partners'] and d['is_eligible']]
    eligible_domains.sort(key=lambda d: (-d['unlim'], -d['old_other']))

    # Phase 6: Write consolidated Markdown report
    is_new_hub = not os.path.exists(output_file)
    output_basename = os.path.basename(output_file)
    top_link = f"[go to top](#wikimedia-readiness-for-{hub_md_slug})"

    with open(output_file, 'w') as f:
        f.write(f"# Wikimedia Readiness for {hub}\n\n")
        f.write("- [By institution](#by-institution)\n")
        f.write("- [By domain](#by-domain)\n\n")
        f.write("## By institution\n\n")
        f.write(f"- {top_link}\n\n")
        for rank, inst in enumerate(eligible, 1):
            stmt = readiness_stmt(inst['name'], inst['unlim'], inst['old_other'], inst['total'],
                                  label="institution", parenthetical=inst['domain'], hub=hub)
            f.write(str(rank) + ". " + stmt + "\n")
        f.write("\n## By domain\n\n")
        f.write(f"- {top_link}\n\n")
        for rank, dom in enumerate(eligible_domains, 1):
            stmt = readiness_stmt(dom['domain'], dom['unlim'], dom['old_other'], dom['total'],
                                  label="domain", parenthetical=dom['largest_inst'], hub=hub)
            f.write(str(rank) + ". " + stmt + "\n")

    print(f"  Report written to {output_basename}: {len(eligible)} institutions, {len(eligible_domains)} domains")

    # Update README if this hub's output file is being created for the first time
    if is_new_hub:
        update_readme(hub, output_filename=output_basename)


# ── Main ──────────────────────────────────────────────────────────────────────

print("Fetching hub list from DPLA API...")
hub_list_result = fetch(
    'https://api.dp.la/v2/items?facets=provider.name&page_size=0&facet_size=100'
)
hubs = [term['term'] for term in hub_list_result['facets']['provider.name']['terms']]
print(f"Found {len(hubs)} hubs.\n")

print("Fetching pipeline JSON...")
pipelinejson = fetch(
    'https://raw.githubusercontent.com/dpla/ingestion3/refs/heads/main/src/main/resources/wiki/institutions_v2.json'
)

# Load shared cache file, keyed by hub name
if os.path.exists(DATA_FILE):
    with open(DATA_FILE, 'r') as f:
        all_data = json.load(f)
else:
    all_data = {}

cutoff_year = datetime.date.today().year - 120

for hub in hubs:
    process_hub(hub, pipelinejson, all_data, cutoff_year)

print("\nDone.")
