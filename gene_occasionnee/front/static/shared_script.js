// Functions for delay classification and display
function getDelayClass(delayMinutes, scheduleRelationship) {
  if (scheduleRelationship === 'CANCELLED') {
    return 'delay-skipped';
  } else if (delayMinutes === null || delayMinutes === undefined) {
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

function getDelayDot(delayMinutes, scheduleRelationship) {
  const delayClass = getDelayClass(delayMinutes, scheduleRelationship);
  return `<span class="delay-dot ${delayClass}"></span>`;
}

function formatTime(timeString) {
  if (!timeString) return 'N/A';
  // Convert from "HH:MM:SS" to "HH:MM"
  return timeString.split(':').slice(0, 2).join(':');
}

function formatDelayInfo(departureDelay, arrivalDelay, departureScheduleRelationship, arrivalScheduleRelationship) {
  // Check if the trip is skipped
  const isSkipped = departureScheduleRelationship === 'CANCELLED' || arrivalScheduleRelationship === 'CANCELLED';
  if (isSkipped) {
    return 'Supprimé';
  }

  if (departureDelay === null && arrivalDelay === null) {
    return '';
  } else if (departureDelay === arrivalDelay) {
    // Same delay for both
    if (departureDelay === null || departureDelay === undefined) {
      return '';
    } else if (departureDelay <= 0) {
      return 'À l\'heure';
    } else if (departureDelay < 60) {
      return `${departureDelay} min`;
    } else {
      const hours = Math.floor(departureDelay / 60);
      const minutes = departureDelay % 60;
      return `${hours}h${minutes.toString().padStart(2, '0')}`;
    }
  } else {
    // Different delays for departure and arrival
    const parts = [];
    if (departureDelay !== null && departureDelay !== undefined) {
      if (departureDelay > 0) {
        parts.push(`Départ: ${formatSingleDelay(departureDelay)}`);
      } else {
        parts.push('Départ: À l\'heure');
      }
    }
    if (arrivalDelay !== null && arrivalDelay !== undefined) {
      if (arrivalDelay > 0) {
        parts.push(`Arrivée: ${formatSingleDelay(arrivalDelay)}`);
      } else {
        parts.push('Arrivée: À l\'heure');
      }
    }
    return parts.join(', ');
  }
}

function formatSingleDelay(delayMinutes) {
  if (delayMinutes < 60) {
    return `${delayMinutes} min`;
  } else {
    const hours = Math.floor(delayMinutes / 60);
    const minutes = delayMinutes % 60;
    return `${hours}h${minutes.toString().padStart(2, '0')}`;
  }
}

function formatDurationValue(minutes) {
  // Shared function to format duration value (used by both live and statistics pages)
  if (minutes < 60) {
    return `${minutes} min`;
  } else {
    const hours = Math.floor(minutes / 60);
    const minutesPart = minutes % 60;
    return `${hours}h${minutesPart.toString().padStart(2, '0')}`;
  }
}

function formatDuration(scheduledMinutes, realMinutes) {
  const scheduledFormatted = formatDurationValue(scheduledMinutes);

  if (!realMinutes) {
    // No realtime data - just show scheduled duration
    return scheduledFormatted;
  }

  const realFormatted = formatDurationValue(realMinutes);
  const hasDelay = realMinutes > scheduledMinutes;

  if (hasDelay) {
    // Show strikethrough scheduled duration and real duration
    return `<span style="text-decoration: line-through;">${scheduledFormatted}</span> ${realFormatted}`;
  } else {
    // On time - show scheduled duration
    return scheduledFormatted;
  }
}

function formatTripTime(scheduledTime, realTime, delayMinutes, scheduleRelationship) {
  const scheduledFormatted = formatTime(scheduledTime);

  if (scheduleRelationship === 'CANCELLED') {
    // Skipped trip - show scheduled time with skipped dot and strikethrough
    return `${getDelayDot(null, 'CANCELLED')} <span style="text-decoration: line-through;">${scheduledFormatted}</span> Supprimé`;
  }

  if (!realTime) {
    // No realtime data - show scheduled time with unknown dot
    return `${getDelayDot(null, scheduleRelationship)} ${scheduledFormatted}`;
  }

  const realFormatted = formatTime(realTime);
  const hasDelay = delayMinutes > 0;

  if (hasDelay) {
    // Show strikethrough scheduled time and real time
    return `${getDelayDot(delayMinutes, scheduleRelationship)} <span style="text-decoration: line-through;">${scheduledFormatted}</span> ${realFormatted}`;
  } else {
    // On time - show scheduled time with dot
    return `${getDelayDot(delayMinutes, scheduleRelationship)} ${scheduledFormatted}`;
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

    html += "<i>Les données sont mises à jour toutes les 2 minutes</i>";

    document.getElementById("timestamp").innerHTML = html;
  } catch (error) {
    console.error(
      "Erreur lors du chargement de la date de dernière mise à jour :",
      error,
    );
  }
}

// Set up periodic refresh
function setupPeriodicRefresh(refreshFunction, interval = 120000) {
  // Initial refresh
  if (refreshFunction) {
    refreshFunction();
  }

  // Set interval for periodic refresh (every 2 minutes by default)
  const refreshInterval = setInterval(() => {
    if (refreshFunction) {
      refreshFunction();
    }
  }, interval);

  // Store interval ID so it can be cleared if needed
  window.refreshIntervalId = refreshInterval;
}

// Chart legend generation
function generateLegend() {
  return (
    '<div class="chart-legend" style="margin-top: 10px; font-size: 12px;">' +
    "<strong>Légende:</strong> " +
    '<span><span class="delay-dot on-time"></span>À l\'heure</span> | ' +
    '<span><span class="delay-dot delay-5min"></span>≤5 min</span> | ' +
    '<span><span class="delay-dot delay-15min"></span>≤15 min</span> | ' +
    '<span><span class="delay-dot delay-45min"></span>≤45 min</span> | ' +
    '<span><span class="delay-dot delay-over-45min"></span>>45 min</span> | ' +
    '<span><span class="delay-dot delay-skipped"></span>Supprimé</span> | ' +
    '<span><span class="delay-dot delay-unknown"></span>Inconnu</span>' +
    "</div>"
  );
}
