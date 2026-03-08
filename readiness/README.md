# DPLA Hub Wikimedia Readiness Inventories

This directory contains upload readiness assessments for DPLA hubs, evaluating their collections for eligibility to upload to [Wikimedia Commons](https://commons.wikimedia.org/) via the [DPLA–Wikimedia partnership](https://dp.la/news/dpla-and-wikimedia/).

## Inventories

| Hub | Inventory |
|-----|------|
| [Indiana Memory](https://dp.la/search?partner=%22Indiana+Memory%22) | [indiana_memory_inventory.md](indiana_memory_inventory.md) |
| [Heartland Hub](https://dp.la/search?partner=%22Heartland+Hub%22) | [heartland_hub_inventory.md](heartland_hub_inventory.md) |
| [California Digital Library](https://dp.la/search?partner=%22California+Digital+Library%22) | [california_digital_library_inventory.md](california_digital_library_inventory.md) |
| [Digital Commonwealth](https://dp.la/search?partner=%22Digital+Commonwealth%22) | [digital_commonwealth_inventory.md](digital_commonwealth_inventory.md) |
| [Minnesota Digital Library](https://dp.la/search?partner=%22Minnesota+Digital+Library%22) | [minnesota_digital_library_inventory.md](minnesota_digital_library_inventory.md) |
| [Plains to Peaks Collective](https://dp.la/search?partner=%22Plains+to+Peaks+Collective%22) | [plains_to_peaks_collective_inventory.md](plains_to_peaks_collective_inventory.md) |
| [Northwest Digital Heritage](https://dp.la/search?partner=%22Northwest+Digital+Heritage%22) | [northwest_digital_heritage_inventory.md](northwest_digital_heritage_inventory.md) |
| [Digital Library of Georgia](https://dp.la/search?partner=%22Digital+Library+of+Georgia%22) | [digital_library_of_georgia_inventory.md](digital_library_of_georgia_inventory.md) |
| [OKHub](https://dp.la/search?partner=%22OKHub%22) | [okhub_inventory.md](okhub_inventory.md) |
| [Michigan Service Hub](https://dp.la/search?partner=%22Michigan+Service+Hub%22) | [michigan_service_hub_inventory.md](michigan_service_hub_inventory.md) |
| [North Carolina Digital Heritage Center](https://dp.la/search?partner=%22North+Carolina+Digital+Heritage+Center%22) | [north_carolina_digital_heritage_center_inventory.md](north_carolina_digital_heritage_center_inventory.md) |
| [Recollection Wisconsin](https://dp.la/search?partner=%22Recollection+Wisconsin%22) | [recollection_wisconsin_inventory.md](recollection_wisconsin_inventory.md) |
| [PA Digital](https://dp.la/search?partner=%22PA+Digital%22) | [pa_digital_inventory.md](pa_digital_inventory.md) |
| [Ohio Digital Network](https://dp.la/search?partner=%22Ohio+Digital+Network%22) | [ohio_digital_network_inventory.md](ohio_digital_network_inventory.md) |
| [Empire State Digital Network](https://dp.la/search?partner=%22Empire+State+Digital+Network%22) | [empire_state_digital_network_inventory.md](empire_state_digital_network_inventory.md) |
| [NJ/DE Digital Collective](https://dp.la/search?partner=%22NJ%2FDE+Digital+Collective%22) | [nj_de_digital_collective_inventory.md](nj_de_digital_collective_inventory.md) |
| [Sunshine State Digital Network](https://dp.la/search?partner=%22Sunshine+State+Digital+Network%22) | [sunshine_state_digital_network_inventory.md](sunshine_state_digital_network_inventory.md) |
| [South Carolina Digital Library](https://dp.la/search?partner=%22South+Carolina+Digital+Library%22) | [south_carolina_digital_library_inventory.md](south_carolina_digital_library_inventory.md) |
| [Digital Library of Tennessee](https://dp.la/search?partner=%22Digital+Library+of+Tennessee%22) | [digital_library_of_tennessee_inventory.md](digital_library_of_tennessee_inventory.md) |

## How readiness is assessed

Each inventory ranks institutions and domains by their estimated readiness to have items uploaded to Wikimedia Commons. Readiness is determined by counting how many items a collection has that are both on CONTENTdm and marked with an **Unlimited Re-Use** rights status in the DPLA — meaning they are cleared as public domain with no upload restrictions.

### Readiness levels

| Level | Public domain item count |
|-------|--------------------------|
| Very high | 5,000 or more |
| High | 1,000 – 4,999 |
| Moderate | 100 – 999 |
| Low | 1 – 99 |
| Very low | 0 |

Institutions with a very low score but with items **over 120 years old** in other rights categories are also flagged. These items may be public domain by age but have not yet been formally reviewed or relicensed, so they represent a potential pool of eligible material pending a rights review.

### Sorting

Institutions and domains are sorted by:

1. Number of public domain (Unlimited Re-Use) items — descending
2. Number of items over 120 years old in other rights categories — descending (as a tiebreaker)

## What is filtered out

Two categories of institutions are excluded from the ranked lists:

**Non-CONTENTdm institutions** — The current DPLA–Wikimedia upload pipeline only supports items hosted on [CONTENTdm](https://www.oclc.org/en/contentdm.html). Institutions whose items are served from other platforms are excluded for now, as they cannot be uploaded through the existing tooling regardless of their rights status.

**Existing pipeline partners** — Institutions that are already active upload partners (i.e., listed with `"upload": true` in the [DPLA pipeline configuration](https://github.com/dpla/ingestion3/blob/main/src/main/resources/wiki/institutions_v2.json)) are excluded, since their collections are already being handled.

## File structure

Each inventory file contains two sections:

**By institution** — Each entry represents a single DPLA data provider as it appears in the API. The institution name links to its DPLA search results. Where available, the CONTENTdm domain is shown in parentheses and links to the host site.

**By domain** — Institutions are grouped by their CONTENTdm domain and their counts are aggregated. This is useful for understanding the total opportunity across a library system that may contribute under multiple provider names, and for planning outreach at the platform/domain level rather than the individual institution level. Each domain entry notes the largest contributing institution as a representative example.

## Running the inventory script

[`inventory.py`](inventory.py) is a self-contained Python script that automatically surveys all DPLA hubs and produces Wikimedia readiness inventories for hubs that use CONTENTdm and have not yet fully joined the upload pipeline.

### Usage

```bash
export DPLA_API_KEY=your_key_here
python readiness/inventory.py
```

The only configuration required is a valid DPLA API key, available at [dp.la/info/developers/codex/](https://dp.la/info/developers/codex/).

### What it does

1. Fetches all DPLA hub names from the DPLA API
2. Fetches the [DPLA ingestion pipeline configuration](https://github.com/dpla/ingestion3/blob/main/src/main/resources/wiki/institutions_v2.json) to identify hubs and institutions already in the upload pipeline
3. For each hub:
   - Skips the hub entirely if `"upload": true` at the hub level (already a full pipeline partner)
   - Queries per-rights-category item counts for all institutions in the hub
   - Fetches a sample item per institution to detect CONTENTdm hosting and extract the domain
   - Skips hubs with no CONTENTdm institutions (no output file is created and no data is cached)
   - Writes a consolidated Markdown inventory file (`{hub_slug}_inventory.md`) to this directory
   - Automatically adds a new row to the Inventories table in this README when a hub is first processed
4. Caches all API responses in `inventory_data.json` to allow interrupted runs to resume without re-fetching

## Cache file

`inventory_data.json` is a local cache file generated and read by `inventory.py`. It is excluded from version control via `.gitignore` because it can grow large and is fully reproducible by re-running the script.

### Structure

```json
{
  "Hub Name": {
    "_rights": {
      "unlim":      { "Institution Name": 1234, ... },
      "old_condit": { "Institution Name": 56,   ... },
      "old_nomod":  { "Institution Name": 78,   ... },
      "old_unspec": { "Institution Name": 90,   ... }
    },
    "contentdm": {
      "Institution Name": true,
      ...
    },
    "_domains": {
      "Institution Name": "digital.example.org",
      ...
    }
  }
}
```

### Keys

- **`_rights`** — Item counts per institution, broken down by rights category. `unlim` = Unlimited Re-Use (public domain eligible). The `old_*` keys count items over 120 years old in other rights categories (conditional, no-modification, and unspecified), which are flagged as candidates for rights review.
- **`contentdm`** — Boolean per institution: whether its sample item URL matched the CONTENTdm URL pattern.
- **`_domains`** — The hostname extracted from the sample item URL for each institution.
