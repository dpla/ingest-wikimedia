document.addEventListener('DOMContentLoaded', function() {    
    google.charts.load('current', {packages: ['corechart', 'line']});

    fetch('https://raw.githubusercontent.com/dpla/ingest-wikimedia/refs/heads/main/metrics/commons_category_allow_list.tsv')
        .then(response => response.text())
        .then(data => {
                
            params = window.location.search.substring(1).split("&");
        	console.log(params)
        	if (params[0].split("=")[0] == 'show') {
        		var id = params[0].split("=")[1]
            }
        	else {
        		var id = 'none'
            	}
            console.log(id)
            
                if ((id == 'all') || (id == 'none')) {
                    var lines = data.split('\n').filter(line => line.trim() !== "");
                }
                if ((id != 'all') && (id != 'none')) {
                    var lines = [id]
                }
                console.log(lines);

            const datalist = document.getElementById("showOptions");
            lines.forEach(item => {
              const option = document.createElement("option");
              option.value = item;
              datalist.appendChild(option);
            });
            
            const form = document.getElementById("showForm");
            const input = document.getElementById("showInput");
            const errorMessage = document.getElementById("errorMessage");
            const sectionsContainer = document.getElementById("sections-container");
            const showNow = document.getElementById("show");
            
            // Utility: case-insensitive exact match
            function isValidItem(value) {
              return lines.some(item => item.toLowerCase() === value.toLowerCase());
            }
            showNow.addEventListener("submit", function (event) {
                event.preventDefault();

              // Hide the form
              form.style.display = "none";
              showNow.style.display = "none";
            
              // Show the sections container
              sectionsContainer.style.display = "block";
            
              // Update the `show` URL parameter
              const url = new URL(window.location.href);
              url.searchParams.set("show", "all");
            
              // Redirect to updated URL
              window.location.href = url.toString();
            });
            form.addEventListener("submit", function (event) {
                event.preventDefault();
            
              const value = input.value.trim();
            
              if (!isValidItem(value)) {
                errorMessage.textContent = "Please select a valid item from the list.";
                errorMessage.style.display = "block";
                return;
              }
            
              errorMessage.style.display = "none";

              // Hide the form
              form.style.display = "none";
              showNow.style.display = "none";
            
              // Show the sections container
              sectionsContainer.style.display = "block";
            
              // Update the `show` URL parameter
              const url = new URL(window.location.href);
              url.searchParams.set("show", value);
            
              // Redirect to updated URL
              window.location.href = url.toString();
            });
                
                var container = document.getElementById('sections-container');
    
                // Create collapsible button
                var back = document.createElement('button');

                back.className = 'back';
                back.textContent = 'BACK';
                back.style.display = "block";
                container.appendChild(back);
                
                lines.forEach(line => {
                    var category = line.trim();
    
                    // Create collapsible button
                    var button = document.createElement('button');
                    
                    button.className = 'collapsible';
                    button.textContent = decodeURI(category).replaceAll('_', ' ');
                    container.appendChild(button);
    
                    // Create content div
                    var content = document.createElement('div');
                    var chart_div = document.createElement('div');
                    content.className = 'content';
                    chart_div.className = 'chart_div';
                    content.innerHTML = '<p>Loading...</p>';
                    container.appendChild(chart_div);
                    container.appendChild(content);
    
                    if ((id != 'all') && (id !='none')) {
                        
                        form.style.display = "none";
                        showNow.style.display = "none";
                        
                        container.style.display = "block";
                        button.classList.toggle("active");
                        content.classList.toggle("open");
                        chart_div.classList.toggle("open");
                        if (content.style.maxHeight) {
                            chart_div.style.maxHeight = null;
                            content.style.maxHeight = null;
                        } else {
                            content.style.maxHeight = "800px";
                            chart_div.style.maxHeight = "800px";
                            if (!content.dataset.loaded) {
                                fetchData(content, decodeURI(category).replace(" ", "_"), chart_div);
                                
                            }
                        }
                    }

                    if (id == 'all') {
                        
                        form.style.display = "none";
                        showNow.style.display = "none";
                        
                        container.style.display = "block";
                    }
                    
                    // Add event listener for the toggle button
                    button.addEventListener("click", function activate(){
                        this.classList.toggle("active");
                        content.classList.toggle("open");
                        chart_div.classList.toggle("open");
                        if (content.style.maxHeight) {
                            chart_div.style.maxHeight = null;
                            content.style.maxHeight = null;
                        } else {
                            content.style.maxHeight = "800px";
                            chart_div.style.maxHeight = "800px";
                            if (!content.dataset.loaded) {
                                fetchData(content, decodeURI(category).replace(" ", "_"), chart_div);
                                
                            }
                        }
                    });
                    // Add event listener for the back button
                    back.addEventListener("click", function activate(){
                      // Update the `show` URL parameter
                      const url = new URL(window.location.href.split(/[?#]/)[0]);
                      // url.searchParams.set("show", value);
            
                      // Redirect to updated URL
                      window.location.href = url.toString();
                    });
                });
            })
        .catch(error => {
            console.error('Error fetching TSV file:', error);
            var container = document.getElementById('sections-container');
            container.innerHTML = "<p>Error loading categories.</p>";
        });
    });

function fetchData(content, category, chart_div) {
    // Replace spaces with underscores and escape category name
    var encodedCategory = encodeURIComponent(category.replace(/\s+/g, '_'));

    fetch(`https://wikimedia.org/api/rest_v1/metrics/commons-analytics/pageviews-per-category-monthly/${encodedCategory}/deep/all-wikis/00000101/99991231`)
        .then(response => response.json())
        .then(data => {
            if (data.items && data.items.length > 0) {
                var pageviews = data.items.map(item => [item['timestamp'].substring(0, 4) + '-' + item['timestamp'].substring(5, 7), item['pageview-count']]);
                var listItems = pageviews.map((count, index) => `<li>${count[0]}: ${count[1].toLocaleString()} views</li>`).join('');

                  var data = new google.visualization.DataTable();
                  data.addColumn('string', 'Timestamp');
                  data.addColumn('number', 'Views');
                  // for (i = 0; i < [pageviews].length; i++) {
                  data.addRows([pageviews][0]);
                  // }
                  var total = 0
                  for (i = 0; i < pageviews.length; i++) {
                      total = total + pageviews[i][1];
                  }
            
                  var options = {
                    hAxis: {
                      title: 'Time'
                    },
                    vAxis: {
                      title: 'Views'
                    }
                  };
            
                  var chart = new google.visualization.LineChart(chart_div);
            
                  chart.draw(data, options);

                content.innerHTML = `<p><strong>Total: ${total.toLocaleString()}</strong></p><ul>${listItems}</ul>`;
            } else {
                content.innerHTML = "<p>No data available.</p>";
                chart_div.style.display = "none";
            }
            content.dataset.loaded = true; // Mark as loaded
        })
        .catch(error => {
            content.innerHTML = "<p>Error loading data.</p>";
            chart_div.innerHTML = "";
            console.error('Error fetching data:', error);
    });
}
