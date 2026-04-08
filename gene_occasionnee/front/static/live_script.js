// Functions for delay classification and display
function getDelayClass(delayMinutes) {
  if (delayMinutes === null || delayMinutes === undefined) {
    return 'delay-unknown';
  } else if (delayMinutes <= 0) {
    return 'on-time';
  } else if (delayMinutes <= 5) {
    return 'delay-5min';
  } else if (delayMinutes <= 15) {
    return 'delay-15min';
  } else if (delayMinutes <= 45) {
    return 'delay-45min';
  } else {
    return 'delay-over-45min';
  }
}

function getDelayDot(delayMinutes) {
  const delayClass = getDelayClass(delayMinutes);
  return `<span class="delay-dot ${delayClass}"></span>`;
}

function formatTime(timeString) {
  if (!timeString) return 'N/A';
  // Convert from "HH:MM:SS" to "HH:MM"
  return timeString.split(':').slice(0, 2).join(':');
}

function formatDelayInfo(delayMinutes) {
  if (delayMinutes === null || delayMinutes === undefined) {
    return '';
  } else if (delayMinutes <= 0) {
    return 'À l\'heure';
  } else if (delayMinutes < 60) {
    return `${delayMinutes} min`;
  } else {
    const hours = Math.floor(delayMinutes / 60);
    const minutes = delayMinutes % 60;
    return `${hours}h${minutes.toString().padStart(2, '0')}`;
  }
}

function formatTripTime(scheduledTime, realTime, delayMinutes) {
  const scheduledFormatted = formatTime(scheduledTime);

  if (!realTime) {
    // No realtime data - just show scheduled time
    return scheduledFormatted;
  }

  const realFormatted = formatTime(realTime);
  const hasDelay = delayMinutes > 0;

  if (hasDelay) {
    // Show strikethrough scheduled time and real time
    return `${getDelayDot(delayMinutes)} <span style="text-decoration: line-through;">${scheduledFormatted}</span> ${realFormatted}`;
  } else {
    // On time - show scheduled time with dot
    return `${getDelayDot(delayMinutes)} ${scheduledFormatted}`;
  }
}

function generateTripHTML(trip) {
  const isDelayed = trip.arrival_delay_minutes > 0 || trip.departure_delay_minutes > 0;
  const rowClass = isDelayed ? 'live-trip-row live-trip-delayed' : 'live-trip-row';

  return `
    <div class="${rowClass}">
      <div class="live-trip-info">
        <strong>${trip.line}</strong> ${trip.trip_headsign}
      </div>
      <div class="live-trip-times">
        <div class="live-trip-time">
          ${formatTripTime(trip.departure_time_scheduled, trip.departure_time_real, trip.departure_delay_minutes)}
        </div>
        <div class="live-trip-time">
          ${formatTripTime(trip.arrival_time_scheduled, trip.arrival_time_real, trip.arrival_delay_minutes)}
        </div>
        <div class="live-trip-delay-info">
          ${formatDelayInfo(trip.arrival_delay_minutes || trip.departure_delay_minutes)}
        </div>
      </div>
    </div>
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

    // Update last updated timestamp
    const now = new Date();
    document.getElementById('last-updated').textContent = `Dernière mise à jour : ${now.toLocaleString('fr-FR')}`;

  } catch (error) {
    console.error('Erreur lors du chargement des données en direct :', error);
    document.getElementById('compiegne-to-paris-trips').innerHTML = '<p>Erreur lors du chargement des données. Veuillez rafraîchir la page.</p>';
    document.getElementById('paris-to-compiegne-trips').innerHTML = '<p>Erreur lors du chargement des données. Veuillez rafraîchir la page.</p>';
  }
}

// Check for Dark Reader
function detectDarkReader() {
  return (
    document.documentElement.hasAttribute('data-darkreader-scheme') ||
    document.documentElement.hasAttribute('data-darkreader-mode')
  );
}

function checkDarkReader() {
  if (detectDarkReader()) {
    document.getElementById('dark-reader-warning').classList.add('show');

    document
      .getElementById('disable-dark-reader-btn')
      .addEventListener('click', function () {
        alert(
          'Pour désactiver Dark Reader pour ce site:\n1. Cliquez sur l\'icône Dark Reader dans votre barre d\'outils\n2. Sélectionnez "Désactiver pour ce site"\n3. Actualisez la page',
        );
      });
  }
}

// Setup system theme listener
function setupSystemThemeListener() {
  const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

  function handleThemeChange(e) {
    if (e.matches) {
      document.body.classList.add('dark-theme');
    } else {
      document.body.classList.remove('dark-theme');
    }
  }

  handleThemeChange(mediaQuery);

  mediaQuery.addEventListener('change', handleThemeChange);
}

// Load timestamp
async function loadTimestamp() {
  try {
    const response = await fetch('/api/latest-timestamp');
    const timestamp = await response.json();
    document.getElementById('update-timestamp').textContent = timestamp.updated_at;
  } catch (error) {
    console.error('Erreur lors du chargement de la date de dernière mise à jour :', error);
  }
}

// Set up periodic refresh
function setupPeriodicRefresh() {
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
  setupPeriodicRefresh();
};