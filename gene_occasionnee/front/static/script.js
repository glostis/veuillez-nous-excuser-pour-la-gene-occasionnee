function generateTableHTML(stats, title) {
  let html = `<h3>${title}</h3>`;
  html += '<div class="table-container"><table>';
  html += '<thead><tr><th>Ligne</th><th>Durée</th><th>Retard moyen</th><th>Retards</th><th>Trajets</th><th>À l\'heure</th><th>≤5 min</th><th>≤15 min</th><th>≤45 min</th><th>>45 min</th><th>Inconnu</th></tr></thead><tbody>';

  stats.forEach((stat) => {
    // Format the line information: C13 16:30 - 17:28
    let lineDisplay = stat.line.split(" ")[0]; // Get line number
    if (stat.arrival_time) {
      lineDisplay += ` ${stat.departure_time} - ${stat.arrival_time}`;
    } else {
      lineDisplay += ` ${stat.departure_time}`;
    }

    // Format duration for separate column
    let durationDisplay = '';
    if (stat.scheduled_duration) {
      if (stat.scheduled_duration < 60) {
        durationDisplay = `${stat.scheduled_duration} min`;
      } else {
        const hours = Math.floor(stat.scheduled_duration / 60);
        const minutes = stat.scheduled_duration % 60;
        durationDisplay = `${hours}h${minutes.toString().padStart(2, "0")}`;
      }
    }

    html += `<tr>
                      <td>${lineDisplay}</td>
                      <td>${durationDisplay}</td>
                      <td>${stat.average_delay_minutes.toFixed(1)} min</td>
                      <td>${generateInlineChartHTML(stat)}</td>
                      <td>${stat.total_trains}</td>
                      <td>${stat.on_time_percentage.toFixed(1)}%</td>
                      <td>${stat.delay_5min_percentage.toFixed(1)}%</td>
                      <td>${stat.delay_15min_percentage.toFixed(1)}%</td>
                      <td>${stat.delay_45min_percentage.toFixed(1)}%</td>
                      <td>${stat.delay_over_45min_percentage.toFixed(1)}%</td>
                      <td>${stat.delay_unknown_percentage.toFixed(1)}%</td>
                  </tr>`;
  });

  html += "</tbody></table></div>";

  // Add legend below the table
  html += generateChartLegendHTML();

  return html;
}

function generateChartLegendHTML() {
  return (
    '<div class="chart-legend" style="margin-top: 10px; font-size: 12px;">' +
    "<strong>Légende:</strong> " +
    '<span><span class="legend-color-box legend-on-time"></span>À l\'heure</span> | ' +
    '<span><span class="legend-color-box legend-delay-5min"></span>≤5 min</span> | ' +
    '<span><span class="legend-color-box legend-delay-15min"></span>≤15 min</span> | ' +
    '<span><span class="legend-color-box legend-delay-45min"></span>≤45 min</span> | ' +
    '<span><span class="legend-color-box legend-delay-over-45min"></span>>45 min</span> | ' +
    '<span><span class="legend-color-box legend-delay-unknown"></span>Inconnu</span>' +
    "</div>"
  );
}

function generateGlobalChartHTML(stats) {
  const total = stats.total_trains;
  if (total === 0) return "";

  let html =
    '<div style="width: 100%; height: 30px; position: relative; border: 1px solid var(--color-border); border-radius: 3px; overflow: hidden; margin-bottom: 5px;">';

  // Calculate widths to ensure total is exactly 100%
  const segments = [];
  if (stats.on_time > 0) segments.push({count: stats.on_time, type: 'on-time', label: 'À l\'heure'});
  if (stats.delay_5min > 0) segments.push({count: stats.delay_5min, type: 'delay-5min', label: '≤5 min'});
  if (stats.delay_15min > 0) segments.push({count: stats.delay_15min, type: 'delay-15min', label: '≤15 min'});
  if (stats.delay_45min > 0) segments.push({count: stats.delay_45min, type: 'delay-45min', label: '≤45 min'});
  if (stats.delay_over_45min > 0) segments.push({count: stats.delay_over_45min, type: 'delay-over-45min', label: '>45 min'});
  if (stats.delay_unknown > 0) segments.push({count: stats.delay_unknown, type: 'delay-unknown', label: 'Inconnu'});

  // Calculate widths ensuring total is 100%
  let usedWidth = 0;
  for (let i = 0; i < segments.length; i++) {
    const isLast = i === segments.length - 1;
    let width;
    if (isLast) {
      // Last segment gets remaining width
      width = 100 - usedWidth;
    } else {
      // Round other segments to 1 decimal place
      width = parseFloat(((segments[i].count / total) * 100).toFixed(1));
      usedWidth += width;
    }

    const percentage = (segments[i].count / total) * 100;
    html += `<div data-chart-segment="${segments[i].type}" style="width: ${width.toFixed(1)}%; height: 100%; float: left;" title="${segments[i].label}: ${segments[i].count} (${percentage.toFixed(1)}%)"></div>`;
  }

  html += "</div>";

  // Add labels below the chart
  html +=
    '<div style="display: flex; justify-content: space-between; font-size: 12px; margin-top: 5px;">';
  html += '<span style="text-align: left;">À l\'heure</span>';
  html += '<span style="text-align: right;">>45 min</span>';
  html += "</div>";

  // Add legend below the chart
  html += generateChartLegendHTML();

  return html;
}

function generateInlineChartHTML(stat) {
  const total = stat.total_trains;
  if (total === 0) return "";

  let html =
    '<div style="width: 150px; height: 15px; position: relative; border: 1px solid var(--color-border); border-radius: 3px; overflow: hidden;">';

  // Calculate widths to ensure total is exactly 100%
  const segments = [];
  if (stat.on_time > 0) segments.push({count: stat.on_time, type: 'on-time', label: 'À l\'heure'});
  if (stat.delay_5min > 0) segments.push({count: stat.delay_5min, type: 'delay-5min', label: '≤5 min'});
  if (stat.delay_15min > 0) segments.push({count: stat.delay_15min, type: 'delay-15min', label: '≤15 min'});
  if (stat.delay_45min > 0) segments.push({count: stat.delay_45min, type: 'delay-45min', label: '≤45 min'});
  if (stat.delay_over_45min > 0) segments.push({count: stat.delay_over_45min, type: 'delay-over-45min', label: '>45 min'});
  if (stat.delay_unknown > 0) segments.push({count: stat.delay_unknown, type: 'delay-unknown', label: 'Inconnu'});

  // Calculate widths ensuring total is 100%
  let usedWidth = 0;
  for (let i = 0; i < segments.length; i++) {
    const isLast = i === segments.length - 1;
    let width;
    if (isLast) {
      // Last segment gets remaining width
      width = 100 - usedWidth;
    } else {
      // Round other segments to 1 decimal place
      width = parseFloat(((segments[i].count / total) * 100).toFixed(1));
      usedWidth += width;
    }

    const percentage = (segments[i].count / total) * 100;
    html += `<div data-chart-segment="${segments[i].type}" style="width: ${width.toFixed(1)}%; height: 100%; float: left;" title="${segments[i].label}: ${segments[i].count} (${percentage.toFixed(1)}%)"></div>`;
  }

  html += "</div>";
  return html;
}

async function loadTimestamp() {
  try {
    const response = await fetch("/api/latest-timestamp");
    const timestamp = await response.json();

    let html =
      "<p><strong>Dernière mise à jour des données :</strong> le " +
      timestamp.updated_at +
      "</p>";

    if (timestamp.is_outdated) {
      document.getElementById("timestamp").style.color = "red";
    } else {
      document.getElementById("timestamp").style.color = "#666";
    }

    document.getElementById("timestamp").innerHTML = html;
  } catch (error) {
    console.error(
      "Erreur lors du chargement de la date de dernière mise à jour :",
      error,
    );
  }
}

function detectDarkReader() {
  // Check for Dark Reader HTML attributes
  return (
    document.documentElement.hasAttribute("data-darkreader-scheme") ||
    document.documentElement.hasAttribute("data-darkreader-mode")
  );
}

// Check for Dark Reader and show warning
function checkDarkReader() {
  if (detectDarkReader()) {
    document.getElementById("dark-reader-warning").classList.add("show");

    document
      .getElementById("disable-dark-reader-btn")
      .addEventListener("click", function () {
        alert(
          "Pour désactiver Dark Reader pour ce site:\n1. Cliquez sur l'icône Dark Reader dans votre barre d'outils\n2. Sélectionnez \"Désactiver pour ce site\"\n3. Actualisez la page",
        );
      });
  }
}

function setupSystemThemeListener() {
  const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)");

  function handleThemeChange(e) {
    if (e.matches) {
      document.body.classList.add("dark-theme");
    } else {
      document.body.classList.remove("dark-theme");
    }
  }

  handleThemeChange(mediaQuery);

  mediaQuery.addEventListener("change", handleThemeChange);
}

// Global variables for date filtering
let currentStartDate = null;
let currentEndDate = null;

async function loadDateRange() {
  try {
    const response = await fetch("/api/date-range");
    const dateRange = await response.json();

    if (dateRange.min_date && dateRange.max_date) {
      // Set default dates to full range
      currentStartDate = dateRange.min_date;
      currentEndDate = dateRange.max_date;

      // Set input values and constraints directly
      document.getElementById("start-date").value = dateRange.min_date;
      document.getElementById("end-date").value = dateRange.max_date;
      document.getElementById("start-date").max = dateRange.max_date;
      document.getElementById("end-date").max = dateRange.max_date;
      document.getElementById("start-date").min = dateRange.min_date;
      document.getElementById("end-date").min = dateRange.min_date;
    }
  } catch (error) {
    console.error("Erreur lors du chargement de la plage de dates:", error);
  }
}

function buildUrlWithDates(url, startDate, endDate) {
  let urlWithParams = url;
  const separator = url.includes("?") ? "&" : "?";
  if (startDate && endDate) {
    urlWithParams += `${separator}start_date=${startDate}&end_date=${endDate}`;
  } else if (startDate) {
    urlWithParams += `${separator}start_date=${startDate}`;
  } else if (endDate) {
    urlWithParams += `${separator}end_date=${endDate}`;
  }
  return urlWithParams;
}

async function loadStats() {
  try {
    let url = "/api/stats";
    if (currentStartDate || currentEndDate) {
      url = buildUrlWithDates(url, currentStartDate, currentEndDate);
    }

    const response = await fetch(url);
    const stats = await response.json();

    let html = "<h2>Statistiques globales</h2>";
    html += '<div class="stats-container">';
    html += `<div class="stat-box">
                    <h3>Nombre de trajets</h3>
                    <div class="stat-value">${stats.total_trains}</div>
                </div>`;
    html += `<div class="stat-box">
                    <h3>À l'heure</h3>
                    <div class="stat-value">${stats.on_time} (${stats.on_time_percentage.toFixed(1)}%)</div>
                </div>`;
    html += `<div class="stat-box">
                    <h3>≤5 min de retard</h3>
                    <div class="stat-value">${stats.delay_5min} (${stats.delay_5min_percentage.toFixed(1)}%)</div>
                </div>`;
    html += `<div class="stat-box">
                    <h3>≤15 min de retard</h3>
                    <div class="stat-value">${stats.delay_15min} (${stats.delay_15min_percentage.toFixed(1)}%)</div>
                </div>`;
    html += `<div class="stat-box">
                    <h3>≤45 min de retard</h3>
                    <div class="stat-value">${stats.delay_45min} (${stats.delay_45min_percentage.toFixed(1)}%)</div>
                </div>`;
    html += `<div class="stat-box">
                    <h3>>45 min de retard</h3>
                    <div class="stat-value">${stats.delay_over_45min} (${stats.delay_over_45min_percentage.toFixed(1)}%)</div>
                </div>`;
    html += `<div class="stat-box">
                    <h3>Inconnu</h3>
                    <div class="stat-value">${stats.delay_unknown} (${stats.delay_unknown_percentage.toFixed(1)}%)</div>
                </div>`;
    html += "</div>";

    // Add horizontal stacked bar chart for global statistics
    html += '<div style="margin-top: 20px; width: fit-content;">';
    html += "<h4>Répartition des retards</h4>";
    html += generateGlobalChartHTML(stats);
    html += "</div>";

    // Add timeline chart
    html += '<div style="margin-top: 30px;">';
    html += "<h4>Évolution des retards dans le temps</h4>";
    html += '<div id="timeline-chart" style="margin-top: 10px;">';
    html += await generateTimelineChartHTML();
    html += '</div>';
    html += "</div>";

    document.getElementById("stats").innerHTML = html;
  } catch (error) {
    console.error("Erreur lors du chargement des statistiques:", error);
  }
}

async function loadLineStats() {
  try {
    let url = "/api/stats?split_by_line=true";
    if (currentStartDate || currentEndDate) {
      url = buildUrlWithDates(url, currentStartDate, currentEndDate);
    }

    const response = await fetch(url);
    const lineStats = await response.json();

    const compiegneToParis = lineStats.filter(
      (stat) => stat.direction === "Compiègne → Paris Nord",
    );
    const parisToCompiegne = lineStats.filter(
      (stat) => stat.direction === "Paris Nord → Compiègne",
    );

    let html = "<h2>Statistiques par ligne</h2>";

    html += generateTableHTML(compiegneToParis, "Compiègne vers Paris Nord");
    html += "<br>";
    html += generateTableHTML(parisToCompiegne, "Paris Nord vers Compiègne");

    document.getElementById("line-stats").innerHTML = html;
  } catch (error) {
    console.error(
      "Erreur lors du chargement des statistiques par ligne:",
      error,
    );
  }
}

async function generateTimelineChartHTML() {
  try {
    let url = "/api/timeline";
    if (currentStartDate || currentEndDate) {
      url = buildUrlWithDates(url, currentStartDate, currentEndDate);
    }

    const response = await fetch(url);
    const timelineData = await response.json();

    if (timelineData.length === 0) {
      return "<p>Aucune donnée disponible pour la frise chronologique.</p>";
    }

    // Determine time unit based on timespan
    const startDate = new Date(timelineData[0].date);
    const endDate = new Date(timelineData[timelineData.length - 1].date);
    const daysSpan = (endDate - startDate) / (1000 * 60 * 60 * 24);
    const useWeekly = daysSpan > 30; // Use weekly if span > 1 month

    // Group data by time unit
    const groupedData = {};
    timelineData.forEach((entry) => {
      const date = new Date(entry.date);
      let groupKey;

      if (useWeekly) {
        // Find the Monday of the current week
        const dayOfWeek = date.getDay(); // 0=Sunday, 1=Monday, ..., 6=Saturday
        const daysToMonday = dayOfWeek === 0 ? 6 : dayOfWeek - 1;
        const monday = new Date(date);
        monday.setDate(date.getDate() - daysToMonday);

        // Use Monday's date as the group key (YYYY-MM-DD)
        groupKey = monday.toISOString().split("T")[0];
      } else {
        // Group by day (YYYY-MM-DD)
        groupKey = entry.date;
      }

      if (!groupedData[groupKey]) {
        // For weekly grouping, we'll fix the label later
        const displayDate = useWeekly
          ? new Date(groupKey)
          : new Date(entry.date);
        const displayKey = useWeekly ? groupKey : entry.date;

        groupedData[groupKey] = {
          date: displayKey,
          total_trains: 0,
          on_time: 0,
          delay_5min: 0,
          delay_15min: 0,
          delay_45min: 0,
          delay_over_45min: 0,
          delay_unknown: 0,
        };
      }

      // Sum up the counts
      groupedData[groupKey].total_trains += entry.total_trains;
      groupedData[groupKey].on_time += entry.on_time;
      groupedData[groupKey].delay_5min += entry.delay_5min;
      groupedData[groupKey].delay_15min += entry.delay_15min;
      groupedData[groupKey].delay_45min += entry.delay_45min;
      groupedData[groupKey].delay_over_45min += entry.delay_over_45min;
      groupedData[groupKey].delay_unknown += entry.delay_unknown;
    });

    // Convert to array and sort
    const groupedArray = Object.values(groupedData);
    groupedArray.sort((a, b) => a.date.localeCompare(b.date));

    // Generate HTML for timeline chart
    let html =
      '<div style="display: flex; overflow-x: auto; gap: 5px; margin-bottom: 10px;">';

    groupedArray.forEach((period) => {
      const total = period.total_trains;
      if (total === 0) return;

      // Format date label
      let dateLabel;
      if (useWeekly) {
        const monday = new Date(period.date);
        const sunday = new Date(monday);
        sunday.setDate(monday.getDate() + 6); // 6 days after Monday = Sunday

        const startDay = monday.getDate();
        const endDay = sunday.getDate();
        const startMonth = monday.getMonth() + 1;
        const endMonth = sunday.getMonth() + 1;

        // Handle month transitions
        if (startMonth === endMonth) {
          // Same month - format as "start-end/month"
          dateLabel = `${startDay}-${endDay}/${startMonth.toString().padStart(2, "0")}`;
        } else {
          // Month transition - format as "start/month-end/month"
          dateLabel = `${startDay}/${startMonth.toString().padStart(2, "0")}-${endDay}/${endMonth.toString().padStart(2, "0")}`;
        }
      } else {
        const dateParts = period.date.split("-");
        dateLabel = `${dateParts[2]}/${dateParts[1].padStart(2, "0")}`;
      }

      html += '<div style="min-width: 40px; text-align: center;">';

      // Show total number of trips at the top
      html += `<div style="font-size: 10px; margin-bottom: 2px;">${total}</div>`;

      // Vertical stacked bar chart (reversed direction)
      html +=
        '<div style="width: 30px; height: 100px; position: relative; border: 1px solid var(--color-border); border-radius: 3px; overflow: hidden; margin: 0 auto;">';

      // Calculate heights for each segment
      const segmentHeight = 100 / total;

      // On time (bottom segment - reversed)
      if (period.on_time > 0) {
        const height = period.on_time * segmentHeight;
        html += `<div data-chart-segment="on-time" style="width: 100%; height: ${height}px; position: absolute; bottom: 0;" title="À l'heure: ${period.on_time}"></div>`;
      }

      // 0-5 min
      if (period.delay_5min > 0) {
        const height = period.delay_5min * segmentHeight;
        const bottom = period.on_time * segmentHeight;
        html += `<div data-chart-segment="delay-5min" style="width: 100%; height: ${height}px; position: absolute; bottom: ${bottom}px;" title="≤5 min: ${period.delay_5min}"></div>`;
      }

      // 5-15 min
      if (period.delay_15min > 0) {
        const height = period.delay_15min * segmentHeight;
        const bottom = (period.on_time + period.delay_5min) * segmentHeight;
        html += `<div data-chart-segment="delay-15min" style="width: 100%; height: ${height}px; position: absolute; bottom: ${bottom}px;" title="≤15 min: ${period.delay_15min}"></div>`;
      }

      // 15-45 min
      if (period.delay_45min > 0) {
        const height = period.delay_45min * segmentHeight;
        const bottom =
          (period.on_time + period.delay_5min + period.delay_15min) *
          segmentHeight;
        html += `<div data-chart-segment="delay-45min" style="width: 100%; height: ${height}px; position: absolute; bottom: ${bottom}px;" title="≤45 min: ${period.delay_45min}"></div>`;
      }

      // Over 45 min
      if (period.delay_over_45min > 0) {
        const height = period.delay_over_45min * segmentHeight;
        const bottom =
          (period.on_time +
            period.delay_5min +
            period.delay_15min +
            period.delay_45min) *
          segmentHeight;
        html += `<div data-chart-segment="delay-over-45min" style="width: 100%; height: ${height}px; position: absolute; bottom: ${bottom}px;" title=">45 min: ${period.delay_over_45min}"></div>`;
      }

      // Unknown
      if (period.delay_unknown > 0) {
        const height = period.delay_unknown * segmentHeight;
        const bottom =
          (period.on_time +
            period.delay_5min +
            period.delay_15min +
            period.delay_45min +
            period.delay_over_45min) *
          segmentHeight;
        html += `<div data-chart-segment="delay-unknown" style="width: 100%; height: ${height}px; position: absolute; bottom: ${bottom}px;" title="Inconnu: ${period.delay_unknown}"></div>`;
      }

      html += "</div>";
      html += `<div style="font-size: 10px; margin-top: 2px;">${dateLabel}</div>`;
      html += "</div>";
    });

    html += "</div>";
    html += generateChartLegendHTML();

    return html;
  } catch (error) {
    console.error("Erreur lors du chargement de la frise chronologique :", error);
    return "<p>Erreur lors du chargement de la frise chronologique.</p>";
  }
}

function setupDateFilter() {
  // Auto-apply when date inputs change
  document.getElementById("start-date").addEventListener("change", applyDateFilter);
  document.getElementById("end-date").addEventListener("change", applyDateFilter);

  // Preset buttons
  document.getElementById("preset-week").addEventListener("click", () => setPresetDateRange("week"));
  document.getElementById("preset-month").addEventListener("click", () => setPresetDateRange("month"));
  document.getElementById("preset-all").addEventListener("click", () => setPresetDateRange("all"));
}

function applyDateFilter() {
  const startDate = document.getElementById("start-date").value;
  const endDate = document.getElementById("end-date").value;

  // Validate dates
  if (startDate && endDate && startDate > endDate) {
    alert("La date de début ne peut pas être postérieure à la date de fin.");
    // Reset to previous valid values
    document.getElementById("start-date").value = currentStartDate || "";
    document.getElementById("end-date").value = currentEndDate || "";
    return;
  }

  // Update global variables
  currentStartDate = startDate || null;
  currentEndDate = endDate || null;

  // Reload all data with new date filter
  loadStats();
  loadLineStats();
}

async function setPresetDateRange(preset) {
  const response = await fetch("/api/date-range");
  const dateRange = await response.json();

  const today = new Date();
  const endDate = today.toISOString().split("T")[0];
  let startDate;

  switch (preset) {
    case "week":
      // Last 7 days
      const weekAgo = new Date(today);
      weekAgo.setDate(today.getDate() - 7);
      startDate = weekAgo.toISOString().split("T")[0];
      break;
    case "month":
      // Last 30 days
      const monthAgo = new Date(today);
      monthAgo.setDate(today.getDate() - 30);
      startDate = monthAgo.toISOString().split("T")[0];
      break;
    case "all":
      // Full range
      startDate = dateRange.min_date;
      break;
  }

  // Ensure dates are within available range
  const finalStartDate = startDate > dateRange.max_date ? dateRange.max_date :
                         startDate < dateRange.min_date ? dateRange.min_date : startDate;
  const finalEndDate = endDate > dateRange.max_date ? dateRange.max_date :
                       endDate < dateRange.min_date ? dateRange.min_date : endDate;

  // Update inputs and global variables
  document.getElementById("start-date").value = finalStartDate;
  document.getElementById("end-date").value = finalEndDate;
  currentStartDate = finalStartDate;
  currentEndDate = finalEndDate;

  // Reload data
  loadStats();
  loadLineStats();
}

// Function to refresh all data
async function refreshAllData() {
  await loadStats();
  await loadLineStats();
  await loadTimestamp();
}

// Set up periodic refresh every 2 minutes (120,000 milliseconds)
function setupPeriodicRefresh() {
  // Initial refresh
  refreshAllData();

  // Set interval for periodic refresh
  const refreshInterval = setInterval(refreshAllData, 120000);

  // Store interval ID so it can be cleared if needed
  window.refreshIntervalId = refreshInterval;
}

window.onload = function () {
  loadTimestamp();

  loadDateRange();
  setupDateFilter();

  loadStats();
  loadLineStats();

  checkDarkReader();
  setupSystemThemeListener();

  // Start periodic refresh
  setupPeriodicRefresh();
};
