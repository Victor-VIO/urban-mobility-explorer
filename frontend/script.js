// API BASE URL
const API_URL = "http://localhost:5000/api";

// Global variables to store charts
let hourlyChart, dailyChart, speedChart, durationChart;

// Helper function to show/hide loading overlay
function showLoading() {
  document.getElementById("loading-overlay").classList.remove("hidden");
}

function hideLoading() {
  document.getElementById("loading-overlay").classList.add("hidden");
}

// Fetch data from backend
async function fetchData(endpoint) {
  try {
    const response = await fetch(`${API_URL}${endpoint}`);
    const data = await response.json();
    return data;
  } catch (error) {
    console.error("Error fetching data:", error);
    alert("Error connecting to backend. Make sure the server is running!");
    return null;
  }
}

// Load overall statistics
async function loadStats() {
  const data = await fetchData("/stats");
  if (data && data.success) {
    const stats = data.stats;
    document.getElementById("total-trips").textContent =
      stats.total_trips.toLocaleString();
    document.getElementById(
      "avg-duration"
    ).textContent = `${stats.avg_duration_minutes.toFixed(1)} min`;
    document.getElementById(
      "avg-speed"
    ).textContent = `${stats.avg_speed_kmh.toFixed(1)} km/h`;
    document.getElementById("common-passengers").textContent =
      stats.most_common_passenger_count;
  }
}

// Load and display busiest hours chart
async function loadHourlyChart() {
  const data = await fetchData("/analytics/busiest-hours");
  if (data && data.success) {
    const hours = data.hours;

    // Prepare data for chart
    const labels = hours.map((h) => {
      const hour = h.pickup_hour;
      if (hour === 0) return "12 AM";
      if (hour < 12) return `${hour} AM`;
      if (hour === 12) return "12 PM";
      return `${hour - 12} PM`;
    });
    const counts = hours.map((h) => h.trip_count);

    // Create chart
    const ctx = document.getElementById("hourly-chart");

    if (hourlyChart) {
      hourlyChart.destroy(); // Destroy old chart if exists
    }

    hourlyChart = new Chart(ctx, {
      type: "bar",
      data: {
        labels: labels,
        datasets: [
          {
            label: "Number of Trips",
            data: counts,
            backgroundColor: "rgba(102, 126, 234, 0.6)",
            borderColor: "rgba(102, 126, 234, 1)",
            borderWidth: 2,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
          legend: {
            display: false,
          },
        },
        scales: {
          y: {
            beginAtZero: true,
            title: {
              display: true,
              text: "Number of Trips",
            },
          },
          x: {
            title: {
              display: true,
              text: "Hour of Day",
            },
          },
        },
      },
    });
  }
}

// Load day of week chart
async function loadDailyChart() {
  const data = await fetchData("/analytics/day-of-week");
  if (data && data.success) {
    const stats = data.stats;

    const labels = stats.map((s) => s.day_name);
    const counts = stats.map((s) => s.trip_count);

    const ctx = document.getElementById("daily-chart");

    if (dailyChart) {
      dailyChart.destroy();
    }

    dailyChart = new Chart(ctx, {
      type: "line",
      data: {
        labels: labels,
        datasets: [
          {
            label: "Number of Trips",
            data: counts,
            backgroundColor: "rgba(118, 75, 162, 0.2)",
            borderColor: "rgba(118, 75, 162, 1)",
            borderWidth: 3,
            fill: true,
            tension: 0.4,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
          legend: {
            display: false,
          },
        },
        scales: {
          y: {
            beginAtZero: true,
            title: {
              display: true,
              text: "Number of Trips",
            },
          },
        },
      },
    });
  }
}

// Load speed by hour chart
async function loadSpeedChart() {
  const data = await fetchData("/analytics/speed-by-hour");
  if (data && data.success) {
    const speeds = data.speeds;

    const labels = speeds.map((s) => {
      const hour = s.pickup_hour;
      if (hour === 0) return "12 AM";
      if (hour < 12) return `${hour} AM`;
      if (hour === 12) return "12 PM";
      return `${hour - 12} PM`;
    });
    const avgSpeeds = speeds.map((s) => s.avg_speed.toFixed(2));

    const ctx = document.getElementById("speed-chart");

    if (speedChart) {
      speedChart.destroy();
    }

    speedChart = new Chart(ctx, {
      type: "line",
      data: {
        labels: labels,
        datasets: [
          {
            label: "Average Speed (km/h)",
            data: avgSpeeds,
            backgroundColor: "rgba(255, 99, 132, 0.2)",
            borderColor: "rgba(255, 99, 132, 1)",
            borderWidth: 3,
            fill: true,
            tension: 0.4,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
          legend: {
            display: false,
          },
        },
        scales: {
          y: {
            beginAtZero: true,
            title: {
              display: true,
              text: "Average Speed (km/h)",
            },
          },
          x: {
            title: {
              display: true,
              text: "Hour of Day",
            },
          },
        },
      },
    });
  }
}

// Load duration distribution chart
async function loadDurationChart() {
  const data = await fetchData("/analytics/duration-distribution");
  if (data && data.success) {
    const distribution = data.distribution;

    const labels = distribution.map((d) => d.duration_range);
    const counts = distribution.map((d) => d.trip_count);

    const ctx = document.getElementById("duration-chart");

    if (durationChart) {
      durationChart.destroy();
    }

    durationChart = new Chart(ctx, {
      type: "pie",
      data: {
        labels: labels,
        datasets: [
          {
            data: counts,
            backgroundColor: [
              "rgba(102, 126, 234, 0.8)",
              "rgba(118, 75, 162, 0.8)",
              "rgba(255, 99, 132, 0.8)",
              "rgba(54, 162, 235, 0.8)",
              "rgba(255, 206, 86, 0.8)",
            ],
            borderWidth: 2,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        plugins: {
          legend: {
            position: "bottom",
          },
        },
      },
    });
  }
}

// Load trips data into table
async function loadTrips(filters = {}) {
  // Build query string from filters
  let queryString = "?limit=500";
  if (filters.hour) queryString += `&hour=${filters.hour}`;
  if (filters.day) queryString += `&day=${filters.day}`;
  if (filters.category) queryString += `&category=${filters.category}`;

  const data = await fetchData(`/trips${queryString}`);
  if (data && data.success) {
    const trips = data.trips;
    const tbody = document.getElementById("trips-tbody");
    tbody.innerHTML = ""; // Clear existing rows

    // Update count
    document.getElementById("trips-count").textContent = trips.length;

    // Add rows
    trips.forEach((trip) => {
      const row = document.createElement("tr");

      // Format datetime
      const pickupTime = new Date(trip.pickup_datetime).toLocaleString();
      const durationMin = (trip.trip_duration / 60).toFixed(1);

      row.innerHTML = `
                <td>${trip.trip_id}</td>
                <td>${pickupTime}</td>
                <td>${durationMin}</td>
                <td>${trip.trip_speed_kmh.toFixed(1)}</td>
                <td>${trip.passenger_count}</td>
                <td><span class="category-badge">${
                  trip.distance_category
                }</span></td>
            `;

      tbody.appendChild(row);
    });
  }
}

// Generate insights from data
async function generateInsights() {
  // Insight 1: Peak hours
  const hourlyData = await fetchData("/analytics/busiest-hours");
  if (hourlyData && hourlyData.success) {
    const hours = hourlyData.hours;
    const topHour = hours[0];
    const hourLabel =
      topHour.pickup_hour === 0
        ? "12 AM"
        : topHour.pickup_hour < 12
        ? `${topHour.pickup_hour} AM`
        : topHour.pickup_hour === 12
        ? "12 PM"
        : `${topHour.pickup_hour - 12} PM`;

    document.getElementById(
      "insight-peak-hours"
    ).textContent = `The busiest hour is ${hourLabel} with ${topHour.trip_count.toLocaleString()} trips. This represents peak demand time in NYC, likely due to commuting patterns or nightlife activity.`;
  }

  // Insight 2: Speed patterns
  const speedData = await fetchData("/analytics/speed-by-hour");
  if (speedData && speedData.success) {
    const speeds = speedData.speeds;

    // Find fastest and slowest hours
    let fastest = speeds[0];
    let slowest = speeds[0];

    for (let i = 1; i < speeds.length; i++) {
      if (speeds[i].avg_speed > fastest.avg_speed) fastest = speeds[i];
      if (speeds[i].avg_speed < slowest.avg_speed) slowest = speeds[i];
    }

    const fastestLabel =
      fastest.pickup_hour === 0
        ? "12 AM"
        : fastest.pickup_hour < 12
        ? `${fastest.pickup_hour} AM`
        : fastest.pickup_hour === 12
        ? "12 PM"
        : `${fastest.pickup_hour - 12} PM`;

    const slowestLabel =
      slowest.pickup_hour === 0
        ? "12 AM"
        : slowest.pickup_hour < 12
        ? `${slowest.pickup_hour} AM`
        : slowest.pickup_hour === 12
        ? "12 PM"
        : `${slowest.pickup_hour - 12} PM`;

    document.getElementById(
      "insight-speed"
    ).textContent = `Fastest trips occur at ${fastestLabel} (${fastest.avg_speed.toFixed(
      1
    )} km/h) with less traffic. Slowest at ${slowestLabel} (${slowest.avg_speed.toFixed(
      1
    )} km/h), indicating heavy congestion during rush hours.`;
  }

  // Insight 3: Weekly trends
  const weekData = await fetchData("/analytics/day-of-week");
  if (weekData && weekData.success) {
    const stats = weekData.stats;

    let busiest = stats[0];
    let quietest = stats[0];

    for (let i = 1; i < stats.length; i++) {
      if (stats[i].trip_count > busiest.trip_count) busiest = stats[i];
      if (stats[i].trip_count < quietest.trip_count) quietest = stats[i];
    }

    document.getElementById("insight-weekly").textContent = `${
      busiest.day_name
    } is the busiest day with ${busiest.trip_count.toLocaleString()} trips, while ${
      quietest.day_name
    } is quietest with ${quietest.trip_count.toLocaleString()} trips. This shows clear weekday vs weekend patterns in urban mobility.`;
  }
}

// Filter event handlers
document.getElementById("apply-filters").addEventListener("click", () => {
  const filters = {
    hour: document.getElementById("hour-filter").value,
    day: document.getElementById("day-filter").value,
    category: document.getElementById("category-filter").value,
  };

  showLoading();
  loadTrips(filters).then(() => hideLoading());
});

document.getElementById("reset-filters").addEventListener("click", () => {
  document.getElementById("hour-filter").value = "";
  document.getElementById("day-filter").value = "";
  document.getElementById("category-filter").value = "";

  showLoading();
  loadTrips().then(() => hideLoading());
});

// Initialize dashboard on page load
async function initDashboard() {
  showLoading();

  try {
    // Load all data in parallel for faster loading
    await Promise.all([
      loadStats(),
      loadHourlyChart(),
      loadDailyChart(),
      loadSpeedChart(),
      loadDurationChart(),
      loadTrips(),
      generateInsights(),
    ]);

    console.log("Dashboard loaded successfully!");
  } catch (error) {
    console.error("Error loading dashboard:", error);
    alert(
      "Error loading dashboard data. Please check the console for details."
    );
  } finally {
    hideLoading();
  }
}

// Start the dashboard when page loads
window.addEventListener("DOMContentLoaded", () => {
  console.log("Initializing NYC Taxi Trip Explorer...");
  initDashboard();
});
