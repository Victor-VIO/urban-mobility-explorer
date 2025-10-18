const API_BASE_URL = 'http://localhost:8000';
    let currentPage = 1;
    const itemsPerPage = 50;
    let currentFilters = {};
    let isLoading = false;

    document.addEventListener('DOMContentLoaded', () => {
        loadStatistics();
        loadCharts();
        loadTrips();
    });

    async function loadStatistics() {
        try {
        const response = await fetch(`${API_BASE_URL}/api/stats`);
        const data = await response.json();

        document.getElementById('totalTrips').textContent = data.total_trips.toLocaleString();
        document.getElementById('avgDuration').innerHTML = `${data.avg_duration_minutes.toFixed(2)}<span class="stat-unit">min</span>`;
        document.getElementById('avgDistance').innerHTML = `${data.avg_distance_miles.toFixed(2)}<span class="stat-unit">mi</span>`;
        document.getElementById('avgSpeed').innerHTML = `${data.avg_speed_mph.toFixed(2)}<span class="stat-unit">mph</span>`;
        } catch (error) {
        console.error('Error loading statistics:', error);
        showError('Failed to load statistics');
        }
    }

    async function loadCharts() {
        if (isLoading) return;
        isLoading = true;

    try {
        // Load both charts in parallel
        await Promise.all([
            loadTimeChart(),
            loadSpeedChart()
        ]);
            } catch (error) {
        console.error('Error loading charts:', error);
        } finally {
        isLoading = false;
        }
    }

    async function loadTimeChart() {
        const container = document.getElementById('timeChart');
        container.innerHTML = '<div class="loading">Loading</div>';

    try {
        const response = await fetch(`${API_BASE_URL}/api/patterns/time`);
        const data = await response.json();
        createBarChart('timeChart', data.by_time_of_day, 'time_of_day', 'trip_count');
    } catch (error) {
        container.innerHTML = '<div style="text-align:center;color:#999;padding:40px;">Failed to load</div>';
    }
}

    async function loadSpeedChart() {
        const container = document.getElementById('speedChart');
        container.innerHTML = '<div class="loading">Loading</div>';

    try {
        const response = await fetch(`${API_BASE_URL}/api/patterns/speed`);
        const data = await response.json();
        createBarChart('speedChart', data.speed_distribution, 'speed_category', 'trip_count');
    } catch (error) {
        container.innerHTML = '<div style="text-align:center;color:#999;padding:40px;">Failed to load</div>';
    }
    }

    function createBarChart(containerId, data, labelKey, valueKey) {
        const container = document.getElementById(containerId);

        if (!data || data.length === 0) {
        container.innerHTML = '<div style="text-align:center;color:#999;padding:40px;">No data available</div>';
        return;
    }

        container.innerHTML = '';
        const maxValue = Math.max(...data.map(d => d[valueKey]));

    data.forEach(item => {
        const bar = document.createElement('div');
        bar.className = 'bar';
        const heightPercent = maxValue > 0 ? (item[valueKey] / maxValue) * 100 : 0;
        bar.style.height = `${Math.max(heightPercent, 1)}%`;

        const label = document.createElement('div');
        label.className = 'bar-label';
        label.textContent = formatLabel(item[labelKey]);
        const value = document.createElement('div');
        value.className = 'bar-value';
        value.textContent = item[valueKey].toLocaleString();

        bar.appendChild(value);
        bar.appendChild(label);
        container.appendChild(bar);
    });
    }

    function formatLabel(label) {
        return label.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
    }

    async function loadTrips() {
        if (isLoading) return;
        isLoading = true;

        const tableContainer = document.getElementById('tableContainer');
        const prevBtn = document.getElementById('prevBtn');
        const nextBtn = document.getElementById('nextBtn');
    
        tableContainer.innerHTML = '<div class="loading">Loading trips data</div>';
        prevBtn.disabled = true;
        nextBtn.disabled = true;

    try {
        const offset = (currentPage - 1) * itemsPerPage;
        const params = new URLSearchParams({
            limit: itemsPerPage,
            offset: offset,
            ...currentFilters
    });

        const response = await fetch(`${API_BASE_URL}/api/trips?${params}`);
        const trips = await response.json();

        if (trips.length === 0) {
            tableContainer.innerHTML = '<div class="loading">No trips found</div>';
            prevBtn.disabled = currentPage <= 1;
            nextBtn.disabled = true;
            return;
    }

        const table = document.createElement('table');
        table.innerHTML = `
            <thead>
            <tr>
                <th>Trip ID</th>
                <th>Pickup Time</th>
                <th>Duration</th>
                <th>Distance</th>
                <th>Speed</th>
                <th>Passengers</th>
                <th>Time of Day</th>
                <th>Speed Category</th>
            </tr>
            </thead>
            <tbody>
            ${trips.map(trip => `
            <tr>
                <td>${trip.id}</td>
                <td>${formatDateTime(trip.pickup_datetime)}</td>
                <td>${trip.trip_duration_minutes.toFixed(2)} min</td>
                <td>${trip.trip_distance_miles.toFixed(2)} mi</td>
                <td>${trip.avg_speed_mph.toFixed(2)} mph</td>
                <td>${trip.passenger_count}</td>
                <td><span class="time-badge time-${trip.time_of_day || 'unknown'}">${formatLabel(trip.time_of_day || 'Unknown')}</span></td>
                <td><span class="speed-badge speed-${trip.speed_category || 'unknown'}">${formatLabel(trip.speed_category || 'Unknown')}</span></td>
            </tr>
            `).join('')}
            </tbody>
        `;

        tableContainer.innerHTML = '';
        tableContainer.appendChild(table);
        document.getElementById('currentPage').textContent = currentPage;
        
        prevBtn.disabled = currentPage <= 1;
        nextBtn.disabled = trips.length < itemsPerPage;
    } catch (error) {
        console.error('Error loading trips:', error);
        tableContainer.innerHTML = '<div class="error">Failed to load trips data</div>';
        prevBtn.disabled = false;
        nextBtn.disabled = false;
    } finally {
        isLoading = false;
    }
    }

    function formatDateTime(dateString) {
        const date = new Date(dateString);
        return date.toLocaleString('en-US', {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
    }
    async function applyFilters() {
        if (isLoading) return;

        const applyBtn = document.getElementById('applyBtn');
        applyBtn.disabled = true;
        applyBtn.textContent = 'Applying...';

        currentFilters = {};

        const timeOfDay = document.getElementById('timeOfDay').value;
        if (timeOfDay) currentFilters.time_of_day = timeOfDay;

        const speedCategory = document.getElementById('speedCategory').value;
        if (speedCategory) currentFilters.speed_category = speedCategory;

        const minDistance = document.getElementById('minDistance').value;
        if (minDistance) currentFilters.min_distance = minDistance;

        const maxDistance = document.getElementById('maxDistance').value;
        if (maxDistance) currentFilters.max_distance = maxDistance;

        currentPage = 1;
    
        try {
        await loadTrips();
    } finally {
        applyBtn.disabled = false;
        applyBtn.textContent = 'Apply Filters';
    }
    }

    function resetFilters() {
        document.getElementById('timeOfDay').value = '';
        document.getElementById('speedCategory').value = '';
        document.getElementById('minDistance').value = '';
        document.getElementById('maxDistance').value = '';
        currentFilters = {};
        currentPage = 1;
        loadTrips();
    }

    function nextPage() {
        if (!isLoading) {
        currentPage++;
        loadTrips();
    }
    }

    function previousPage() {
        if (currentPage > 1 && !isLoading) {
        currentPage--;
        loadTrips();
    }
    }

    function showError(message) {
        const errorDiv = document.createElement('div');
        errorDiv.className = 'error';
        errorDiv.textContent = message;
        document.querySelector('.container').insertBefore(errorDiv, document.querySelector('.stats-grid'));
        setTimeout(() => errorDiv.remove(), 5000);
    }