function generateTripHTML(trip) {
  return `
    <tr class="live-trip-row">
      <td class="live-trip-line">${trip.line}</td>
      <td class="live-trip-departure">
        ${formatTripTime(trip.departure_time_scheduled, trip.departure_time_real, trip.departure_delay_minutes)}
      </td>
      <td class="live-trip-arrival">
        ${formatTripTime(trip.arrival_time_scheduled, trip.arrival_time_real, trip.arrival_delay_minutes)}
      </td>
      <td class="live-trip-delay">
        ${formatDelayInfo(trip.departure_delay_minutes, trip.arrival_delay_minutes)}
      </td>
    </tr>
  `;
}

async function loadLiveData() {
  try {
    const response = await fetch('/api/live');
    const data = await response.json();

    // Separate trips by direction
    const compiegneToParis = [];
    const parisToCompiegne = [];

    data.forEach(trip => {
      if (trip.direction === 'Compiègne → Paris Nord') {
        compiegneToParis.push(trip);
      } else if (trip.direction === 'Paris Nord → Compiègne') {
        parisToCompiegne.push(trip);
      }
    });

    // Sort by departure time
    compiegneToParis.sort((a, b) => a.departure_time_scheduled.localeCompare(b.departure_time_scheduled));
    parisToCompiegne.sort((a, b) => a.departure_time_scheduled.localeCompare(b.departure_time_scheduled));

    // Generate HTML
    const compiegneHTML = compiegneToParis.map(generateTripHTML).join('');
    const parisHTML = parisToCompiegne.map(generateTripHTML).join('');

    document.getElementById('compiegne-to-paris-trips').innerHTML = compiegneHTML || '<p>Aucun train trouvé pour cette direction aujourd\'hui.</p>';
    document.getElementById('paris-to-compiegne-trips').innerHTML = parisHTML || '<p>Aucun train trouvé pour cette direction aujourd\'hui.</p>';

    // Add legend below each table
    const compiegneTable = document.getElementById('compiegne-to-paris-trips').closest('.live-direction-table');
    if (compiegneTable && !compiegneTable.querySelector('.chart-legend')) {
      const legendHTML = generateLegend();
      compiegneTable.insertAdjacentHTML('beforeend', legendHTML);
    }

    const parisTable = document.getElementById('paris-to-compiegne-trips').closest('.live-direction-table');
    if (parisTable && !parisTable.querySelector('.chart-legend')) {
      const legendHTML = generateLegend();
      parisTable.insertAdjacentHTML('beforeend', legendHTML);
    }

  } catch (error) {
    console.error('Erreur lors du chargement des données en direct :', error);
    document.getElementById('compiegne-to-paris-trips').innerHTML = '<p>Erreur lors du chargement des données. Veuillez rafraîchir la page.</p>';
    document.getElementById('paris-to-compiegne-trips').innerHTML = '<p>Erreur lors du chargement des données. Veuillez rafraîchir la page.</p>';
  }
}

// Set up periodic refresh for live data
function setupLivePage() {
  // Initial refresh
  loadLiveData();
  loadTimestamp();

  // Set interval for periodic refresh (every 2 minutes)
  const refreshInterval = setInterval(() => {
    loadLiveData();
    loadTimestamp();
  }, 120000);

  // Store interval ID so it can be cleared if needed
  window.refreshIntervalId = refreshInterval;
}

window.onload = function () {
  checkDarkReader();
  setupSystemThemeListener();
  setupLivePage();
};
