// Module-level promise: resolves when the Google Charts library is fully loaded.
// Starting the load here (at script parse time, before DOMContentLoaded) gives
// the library maximum time to initialize, avoiding a race condition where the
// Wikimedia API responds before google.visualization is available.
const chartsReady = google.charts.load('current', { packages: ['corechart', 'line'] });

document.addEventListener('DOMContentLoaded', function () {

    // URL parameter contract:
    //   ?show=all        → render all categories as collapsible panels
    //   ?show=<category> → render and auto-open a single specific category
    //   (no parameter)   → show the search/browse forms
    const id = new URLSearchParams(window.location.search).get('show') ?? 'none';

    // Fetch the allow-list of Wikimedia Commons categories (one category per line).
    fetch('https://raw.githubusercontent.com/dpla/ingest-wikimedia/refs/heads/main/metrics/commons_category_allow_list.tsv')
        .then(response => response.text())
        .then(data => {
            const allLines = data.split('\n').filter(line => line.trim() !== '');

            // For 'all' or no param, show every category.
            // For a specific category, the render list contains only that entry.
            const lines = (id === 'all' || id === 'none') ? allLines : [id];

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

            // "Show all" redirects to ?show=all, which re-renders all categories.
            showNow.addEventListener('submit', function (event) {
                event.preventDefault();
                const url = new URL(window.location.href);
                url.searchParams.set('show', 'all');
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

            // When a specific category or all categories are selected,
            // hide the search forms and reveal the dashboard panels.
            if (id !== 'none') {
                form.style.display    = 'none';
                showNow.style.display = 'none';
                document.getElementById('sections-container').style.display = 'block';
            }

            const container = document.getElementById('sections-container');

            // "Back" strips the ?show param and returns to the search forms.
            // The listener is attached once here, outside the forEach loop,
            // to avoid registering a duplicate listener for each category.
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
                const button = document.createElement('button');
                button.className   = 'collapsible';
                button.textContent = decodeURI(category).replaceAll('_', ' ');
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
                if (id !== 'all' && id !== 'none') {
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
        })
        .catch(error => {
            console.error('Error fetching TSV file:', error);
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
