# DPLA Hub Wikimedia Readiness Inventories

This directory contains upload readiness assessments for DPLA hubs, evaluating their collections for eligibility to upload to [Wikimedia Commons](https://commons.wikimedia.org/) via the [DPLA–Wikimedia partnership](https://dp.la/news/dpla-and-wikimedia/).

## Inventories

| Hub | File |
|-----|------|
| [Indiana Memory](https://dp.la/search?partner=%22Indiana+Memory%22) | [indiana_inventory.md](indiana_inventory.md) |
| [Heartland Hub](https://dp.la/search?partner=%22Heartland+Hub%22) | [heartland_inventory.md](heartland_inventory.md) |

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
