// Module-level promise: resolves when the Google Charts library is fully loaded.
// Starting the load here (at script parse time, before DOMContentLoaded) gives
// the library maximum time to initialize, avoiding a race condition where the
// Wikimedia API responds before google.visualization is available.
const chartsReady = google.charts.load('current', { packages: ['corechart', 'line'] });

document.addEventListener('DOMContentLoaded', function () {

    // URL parameter contract:
    //   ?show=all        → render all categories as collapsible panels
    //   ?show=dpla       → render only DPLA-institution categories
    //   ?show=<category> → render and auto-open a single specific category
    //   (no parameter)   → show the search/browse forms
    const id = new URLSearchParams(window.location.search).get('show') ?? 'none';

    // Fetch the live allow-list of Wikimedia Commons categories (one category per line)
    // directly from the canonical source via the GitLab REST API.
    const ALLOW_LIST_URL = 'https://gitlab.wikimedia.org/api/v4/projects/repos%2Fdata-engineering%2Fairflow-dags/repository/files/main%2Fdags%2Fcommons%2Fcommons_category_allow_list.tsv/raw?ref=main';

    fetch(ALLOW_LIST_URL)
        .then(response => {
            if (!response.ok) throw new Error(`GitLab API returned HTTP ${response.status}`);
            return response.text();
        })
        .then(data => {
            const allLines = data.split('\n').filter(line => line.trim() !== '');

            // Always populate the autocomplete datalist with every available
            // category so the search form works regardless of the current view.
            const datalist = document.getElementById('showOptions');
            allLines.forEach(item => {
                const option = document.createElement('option');
                option.value = item;
                datalist.appendChild(option);
            });

            const form         = document.getElementById('showForm');
            const input        = document.getElementById('showInput');
            const errorMessage = document.getElementById('errorMessage');
            const showNow      = document.getElementById('show');
            const showDpla     = document.getElementById('showDpla');

            // "Show all" redirects to ?show=all, which re-renders all categories.
            showNow.addEventListener('submit', function (event) {
                event.preventDefault();
                const url = new URL(window.location.href);
                url.searchParams.set('show', 'all');
                window.location.href = url.toString();
            });

            // "Show DPLA institutions only" redirects to ?show=dpla.
            showDpla.addEventListener('submit', function (event) {
                event.preventDefault();
                const url = new URL(window.location.href);
                url.searchParams.set('show', 'dpla');
                window.location.href = url.toString();
            });

            // "Submit" validates the typed category then redirects to ?show=<category>.
            form.addEventListener('submit', function (event) {
                event.preventDefault();
                const value = input.value.trim();

                if (!allLines.some(item => item.toLowerCase() === value.toLowerCase())) {
                    errorMessage.textContent = 'Please select a valid item from the list.';
                    errorMessage.style.display = 'block';
                    return;
                }

                errorMessage.style.display = 'none';
                const url = new URL(window.location.href);
                url.searchParams.set('show', value);
                window.location.href = url.toString();
            });

            // When any display mode is active, hide all three forms and show the dashboard.
            if (id !== 'none') {
                form.style.display      = 'none';
                showNow.style.display   = 'none';
                showDpla.style.display  = 'none';
                document.getElementById('sections-container').style.display = 'block';
            }

            const container = document.getElementById('sections-container');

            // Builds the back button and a collapsible panel for each category in `lines`.
            // Pass autoOpen=true only for the single-category view: the panel opens
            // immediately and data is fetched without waiting for a user click.
            function buildPanels(lines, autoOpen) {
                // "Back" strips the ?show param and returns to the search forms.
                // Attached once here rather than inside the loop to avoid duplicate listeners.
                const back = document.createElement('button');
                back.className   = 'back';
                back.textContent = 'BACK';
                back.style.display = 'block';
                back.addEventListener('click', function () {
                    window.location.href = window.location.href.split(/[?#]/)[0];
                });
                container.appendChild(back);

                lines.forEach(line => {
                    const category = line.trim();

                    // Collapsible header button — shows the human-readable category name.
                    // For "Media contributed by [the] X" categories, strip the common
                    // prefix so only the institution name is displayed. Lowercase "the"
                    // after the prefix is also stripped; uppercase "The" is kept as part
                    // of the institution name.
                    const button = document.createElement('button');
                    button.className   = 'collapsible';
                    button.textContent = categoryDisplayName(category);
                    container.appendChild(button);

                    // Chart panel: Google Charts renders a line chart here.
                    const chartDiv = document.createElement('div');
                    chartDiv.className = 'chart_div';
                    container.appendChild(chartDiv);

                    // Text panel: shows the lifetime total and a monthly breakdown list.
                    // max-height is driven by inline style rather than class alone because
                    // CSS transitions on max-height require a concrete pixel value to
                    // animate from 0 to open; setting it to null collapses back to the
                    // CSS default of 0.
                    const content = document.createElement('div');
                    content.className = 'content';
                    content.innerHTML = '<p>Loading...</p>';
                    container.appendChild(content);

                    // For a single specific category, auto-open and load data immediately.
                    if (autoOpen) {
                        button.classList.add('active');
                        content.classList.add('open');
                        chartDiv.classList.add('open');
                        content.style.maxHeight = '800px';
                        chartDiv.style.maxHeight = '800px';
                        // Guard on chartsReady here: the Wikimedia API may respond before
                        // google.visualization has finished loading on a cold page load.
                        chartsReady.then(() => fetchData(content, category, chartDiv));
                    }

                    // Toggle panel open/closed on click; lazy-load data on first open.
                    // By the time a user can physically click a button, Google Charts
                    // has had ample time to load, so no chartsReady guard is needed here.
                    button.addEventListener('click', function () {
                        this.classList.toggle('active');
                        content.classList.toggle('open');
                        chartDiv.classList.toggle('open');

                        if (content.style.maxHeight) {
                            // Closing: clear inline max-height so CSS default (0) takes over.
                            content.style.maxHeight = null;
                            chartDiv.style.maxHeight = null;
                        } else {
                            // Opening: set a concrete max-height so the CSS transition animates.
                            content.style.maxHeight = '800px';
                            chartDiv.style.maxHeight = '800px';
                            if (!content.dataset.loaded) {
                                fetchData(content, category, chartDiv);
                            }
                        }
                    });
                });
            }

            if (id === 'dpla') {
                // Show a loading message while the Wikimedia Commons category tree is fetched.
                // fetchDplaCategories makes ~20-50 parallel API calls (one per institution
                // subcategory) to build the two-level category set, then intersects it with
                // the allow list. Expect 300-400 matching categories.
                container.innerHTML = '<p>Loading DPLA institutions…</p>';
                fetchDplaCategories()
                    .then(dplaSet => {
                        // Intersect with the allow list, then keep only the
                        // "Media contributed by" categories (the root and its
                        // subcategory tree). Other DPLA-adjacent categories are excluded.
                        const lines = allLines.filter(l => {
                            const name = l.trim();
                            return dplaSet.has(name) && name.startsWith('Media_contributed_by_');
                        });
                        container.innerHTML = '';
                        buildPanels(lines, false);
                    })
                    .catch(err => {
                        container.innerHTML = '<p>Error loading DPLA category data.</p>';
                        console.error('Error fetching DPLA categories:', err);
                    });
            } else {
                // For 'all' or no param, render every category.
                // For a specific category name, render only that one and auto-open it.
                const lines = (id === 'all' || id === 'none') ? allLines : [id];
                buildPanels(lines, id !== 'all' && id !== 'none');
            }
        })
        .catch(error => {
            console.error('Error fetching allow list:', error);
            document.getElementById('sections-container').innerHTML = '<p>Error loading categories.</p>';
        });
});

/**
 * Fetches monthly pageview data for a Wikimedia Commons category from the
 * Wikimedia Analytics REST API, then renders a Google Charts line chart and
 * a monthly breakdown list into the given panel elements.
 *
 * API endpoint docs:
 *   https://wikimedia.org/api/rest_v1/#/Commons%20Analytics%20Data/get_metrics_commons_analytics_pageviews_per_category_monthly__category__depth__access__start__end_
 *
 * @param {HTMLElement} content   - Text panel element (receives total + list)
 * @param {string}      category  - Raw category name (underscores, may be URL-encoded)
 * @param {HTMLElement} chartDiv  - Chart container element
 */
function fetchData(content, category, chartDiv) {
    // Normalize any spaces to underscores and percent-encode for the API path.
    const encodedCategory = encodeURIComponent(category.replace(/\s+/g, '_'));

    fetch(`https://wikimedia.org/api/rest_v1/metrics/commons-analytics/pageviews-per-category-monthly/${encodedCategory}/deep/all-wikis/00000101/99991231`)
        .then(response => response.json())
        .then(apiData => {
            if (apiData.items && apiData.items.length > 0) {
                // API timestamps are in YYYYMM00 format; extract YYYY-MM for display.
                // Each row is [displayMonth, viewCount], e.g. ["2023-04", 1234].
                const pageviews = apiData.items.map(item => [
                    item['timestamp'].substring(0, 4) + '-' + item['timestamp'].substring(5, 7),
                    item['pageview-count']
                ]);

                // Sum all monthly counts for the lifetime total.
                const total = pageviews.reduce((sum, [, count]) => sum + count, 0);

                // Build a Google Charts DataTable and draw the line chart.
                const chartData = new google.visualization.DataTable();
                chartData.addColumn('string', 'Timestamp');
                chartData.addColumn('number', 'Views');
                chartData.addRows(pageviews);

                new google.visualization.LineChart(chartDiv).draw(chartData, {
                    hAxis: { title: 'Time' },
                    vAxis: { title: 'Views' }
                });

                // Build the monthly list for the text panel.
                const listItems = pageviews
                    .map(([month, count]) => `<li>${month}: ${count.toLocaleString()} views</li>`)
                    .join('');
                content.innerHTML = `<p><strong>Total: ${total.toLocaleString()}</strong></p><ul>${listItems}</ul>`;
            } else {
                content.innerHTML = '<p>No data available.</p>';
                chartDiv.style.display = 'none';
            }

            content.dataset.loaded = true; // Prevent redundant fetches on re-open.
        })
        .catch(error => {
            content.innerHTML = '<p>Error loading data.</p>';
            chartDiv.innerHTML = '';
            console.error('Error fetching data:', error);
        });
}

/**
 * Builds the set of Wikimedia Commons category names associated with DPLA
 * institutions by walking two levels of the subcategory tree under the root
 * DPLA category. Both level-1 and level-2 subcategories are included.
 *
 * Category names are normalized to underscore form to match the allow-list
 * (e.g. "Foo Bar" → "Foo_Bar").
 *
 * @returns {Promise<Set<string>>} Set of normalized category names
 */
async function fetchDplaCategories() {
    const COMMONS_API = 'https://commons.wikimedia.org/w/api.php';
    const DPLA_ROOT   = 'Category:Media contributed by the Digital Public Library of America';

    // Level 1: direct subcategories of the DPLA root (typically ~20-50 institutions).
    const level1 = await fetchSubcategories(COMMONS_API, DPLA_ROOT);

    // Level 2: subcategories of each level-1 entry, fetched in parallel.
    // These are the individual collection/partner categories (~300-400 total).
    const level2Arrays = await Promise.all(
        level1.map(cat => fetchSubcategories(COMMONS_API, cat))
    );

    // Combine both levels plus the root itself; strip the "Category:" prefix and
    // normalize spaces to underscores so names match the allow-list format.
    const categorySet = new Set();
    [DPLA_ROOT, ...level1, ...level2Arrays.flat()].forEach(title => {
        categorySet.add(title.replace(/^Category:/, '').replaceAll(' ', '_'));
    });

    return categorySet;
}

/**
 * Fetches all subcategory titles (cmtype=subcat) of the given category from
 * the Wikimedia Commons API, following continuation tokens to retrieve all pages.
 *
 * @param {string} apiUrl        - Wikimedia Commons API base URL
 * @param {string} categoryTitle - Full category title including "Category:" prefix
 * @returns {Promise<string[]>}  - Array of subcategory title strings
 */
async function fetchSubcategories(apiUrl, categoryTitle) {
    const results = [];
    let cmcontinue = null;

    do {
        const params = new URLSearchParams({
            action:   'query',
            list:     'categorymembers',
            cmtitle:  categoryTitle,
            cmtype:   'subcat',   // namespace 14 only — skips files and pages
            cmlimit:  'max',      // up to 500 per request
            format:   'json',
            origin:   '*',        // required for cross-origin browser requests
        });
        if (cmcontinue) params.set('cmcontinue', cmcontinue);

        const response = await fetch(`${apiUrl}?${params}`);
        if (!response.ok) throw new Error(`Commons API returned HTTP ${response.status} for "${categoryTitle}"`);
        const data = await response.json();

        if (data.query?.categorymembers) {
            results.push(...data.query.categorymembers.map(m => m.title));
        }

        // The API returns a `continue` object when there are more results to fetch.
        cmcontinue = data.continue?.cmcontinue ?? null;
    } while (cmcontinue);

    return results;
}

/**
 * Returns the human-readable display name for a category.
 *
 * For "Media contributed by [the] X" categories, strips the common prefix so
 * only the institution name is shown. The word "the" (lowercase only) immediately
 * after the prefix is also stripped; uppercase "The" is treated as part of the
 * institution name and kept.
 *
 * Examples:
 *   "Media_contributed_by_the_Foo_Library" → "Foo Library"
 *   "Media_contributed_by_The_Foo_Library" → "The Foo Library"
 *   "Media_contributed_by_the_Digital_Public_Library_of_America" → "Digital Public Library of America"
 *   "Some_Other_Category"                  → "Some Other Category"
 *
 * @param {string} category - Raw category name (underscores, may be URL-encoded)
 * @returns {string}
 */
function categoryDisplayName(category) {
    let name = decodeURI(category).replaceAll('_', ' ');
    const prefix = 'Media contributed by ';
    if (name.startsWith(prefix)) {
        name = name.slice(prefix.length);
        // Strip lowercase "the " but preserve uppercase "The" as part of the name.
        if (name.startsWith('the ')) {
            name = name.slice(4);
        }
    }
    return name;
}
