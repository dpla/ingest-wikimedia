# Metrics

A separate, scheduled workflow tracks monthly pageview metrics for DPLA-uploaded files on Wikimedia Commons. This is independent of the ingest pipeline — it does not upload, does not move files, and does not call any of the four pipeline phases.

## Overview

```text
[scheduled cron / manual trigger]
        │
        ▼
┌─────────────────────────────────┐
│  GitHub Actions                 │
│  .github/workflows/cim-...yml   │
└────────────────┬────────────────┘
                 │
                 ▼
        ┌────────────────┐
        │  CIMviews.py   │ (pywikibot bot, user "DPLA bot")
        └────────┬───────┘
                 │
        ┌────────┴────────┐
        ▼                 ▼
 Wikimedia REST     Commons "Data:" pages
 (pageview API)     Data:Views/<category>.tab
                    Data:Views/<category>.chart
                              │
                              ▼
                  GitHub-Pages site (metrics/)
                  reads Data: pages client-side,
                  renders Google Charts
```

## The bot: `metrics/cim-pageviews/CIMviews.py`

A pywikibot script with two modes selected via `--mode`:

### `--mode data` (default)

For every Commons category that transcludes `{{views from category}}`:

1. Walks `Category:Category with page views table` to enumerate target categories (removing the maintenance subcategory `Category:Category page views need to be updated`).
2. Calls the Wikimedia Commons-Impact-Metrics REST endpoint:
   ```
   https://wikimedia.org/api/rest_v1/metrics/commons-analytics/pageviews-per-category-monthly/<category>/deep/all-wikis/00000101/99991231
   ```
   This returns monthly pageview counts for every wiki that ever displayed a file in the category.
3. Writes two pages on Commons per category:
   - **`Data:Views/<category>.tab`** — a `jsondata` page with a declared schema (two columns: timestamp + pageviews) and one row per month.
   - **`Data:Views/<category>.chart`** — a chart definition referencing the `.tab` page.
4. Calls `category.touch()` on the source category so dependent templates re-render with the new data.

### `--mode categories`

Walks `Category:Category requested for Commons Impact Metrics`. For categories now in the Wikimedia "category allow list" (fetched live from the Airflow DAGs repo at `gitlab.wikimedia.org`), removes the request as fulfilled. For categories not yet in the allow list, adds the request via the standard `{{Commons Impact Metrics request}}` template — unless a request is already pending.

In effect: `data` mode publishes the numbers; `categories` mode manages the queue of categories that *should* have numbers published.

## The workflow: `.github/workflows/cim-pageviews.yml`

Triggers:

```yaml
on:
  schedule:
    - cron: "0 8 * * *"        # daily 08:00 UTC → categories mode
    - cron: "0 8 1,7 * *"      # 1st and 7th of every month → data mode
  workflow_dispatch:
    inputs:
      mode: { default: data, type: choice, options: [data, categories] }
```

Two jobs (`categories`, `data`), each gated by the matching trigger:

- **`categories`** runs on the daily 08:00 cron (and on manual dispatch with `mode=categories`). 60-minute timeout.
- **`data`** runs on the monthly 1st/7th cron (and on manual dispatch with `mode=data`). 120-minute timeout.

Both jobs do the same setup:

1. Checkout the repo.
2. Install Python via `actions/setup-python`.
3. Install `uv`; run `uv sync --no-dev`.
4. Build `metrics/cim-pageviews/user-password.py` at runtime via a HEREDOC, embedding `BotPassword('PARTNER_UPLOADS', '<secret>')` where `<secret>` is `secrets.PYWIKIBOT_PASSWORD`. (The committed `user-config.py` references this file but doesn't contain the secret.)
5. Run `python metrics/cim-pageviews/CIMviews.py --mode <mode>` with `PYWIKIBOT_DIR=metrics/cim-pageviews`.

Permissions: `contents: read`. The bot's edits to Commons happen via pywikibot's own auth flow, not GitHub's permissions.

## The site: `metrics/`

`metrics/index.html` is the landing page of a small Jekyll site published via GitHub Pages on this repo. Layout: `jekyll-theme-cayman`, with `_includes/head-custom.html` wiring favicon links from `assets/favicons/`.

The page does no server-side work; everything is client-side JS in `metrics/metrics.js`:

1. Loads Google Charts via `https://www.gstatic.com/charts/loader.js`.
2. Fetches the Wikimedia category-allow-list TSV from GitLab (`https://gitlab.wikimedia.org/api/v4/projects/repos%2Fdata-engineering%2Fairflow-dags/repository/files/main%2Fdags%2Fcommons%2Fcommons_category_allow_list.tsv/raw?ref=main`).
3. Populates a `<datalist>` autocomplete from that TSV.
4. URL modes:
   - `?show=all` — every category in the allow list with pageview data.
   - `?show=dpla` — only categories under the DPLA umbrella (walks Commons' category hierarchy from `Category:Media contributed by the Digital Public Library of America` → hubs → contributing institutions).
   - `?show=<category>` — a single named category.
   - `?hub=<name>` — every contributing institution within one DPLA hub.
5. For each rendered category, fetches the `Data:Views/<category>.tab` JSON page from Commons via the standard MediaWiki API and renders a Google Chart from the monthly series.

A pre-paint inline script (`metrics/index.html` line 11) sets a `.filter-view` class on `<html>` when the URL has a `show` or `hub` parameter — this lets CSS hide the input form without a flash-of-unstyled-content.

There is no server-side state. The bot publishes `Data:` pages on Commons; the GitHub Pages site fetches them client-side. The two systems are decoupled — a change to either doesn't break the other as long as the `Data:Views/<category>.tab` JSON schema stays stable.

## Why this is separate from the ingest pipeline

The metrics system is operationally and architecturally distinct:

- **Different trigger.** Cron / manual dispatch, never Slack.
- **Different EC2 footprint.** None. CIMviews runs entirely inside the GitHub Actions runner; no SSM, no tmux.
- **Different secrets.** Uses `PYWIKIBOT_PASSWORD` (the DPLA bot's pywikibot bot-password), not the ingest pipeline's AWS / Slack tokens.
- **Different cadence.** Daily (categories) and monthly (data), versus on-demand for the ingest pipeline.
- **Different output channel.** Commons `Data:` pages, not S3.
- **Decoupled audience.** Operators don't need to be aware of the metrics system to run the ingest pipeline; metrics consumers (Commons editors looking at pageview data) don't need to know about ingest.

The only connection is the bot identity (`User:DPLA bot`), and `dpla-id-banlist.txt` / `rights.json` are not consulted by the metrics workflow.

## Operations

- **Adding a category** — edit `Category:Category requested for Commons Impact Metrics` on Commons with the `{{Commons Impact Metrics request}}` template. The daily `categories` job will surface it and the next monthly `data` job will publish numbers for it.
- **Removing a category** — remove `{{views from category}}` from its category page; the next `data` job will stop touching it. The Commons `Data:Views/<category>.tab` and `.chart` pages can be deleted manually if desired.
- **Manual catch-up after a data outage** — trigger the workflow manually with `mode=data` (admin-only via GitHub Actions UI). The 120-minute timeout is sized for the full category set.
- **Bot password rotation** — rotate `PYWIKIBOT_PASSWORD` in the repo's secret store. No code change needed.
