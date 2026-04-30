document.addEventListener('DOMContentLoaded', () => {
    const API_BASE_URL = 'http://localhost:8000';
    const GAINESVILLE_CENTER = [29.6516, -82.3248];

    const map = L.map('map', {
        zoomControl: false
    }).setView(GAINESVILLE_CENTER, 13);

    L.control.zoom({ position: 'bottomleft' }).addTo(map);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
        maxZoom: 19
    }).addTo(map);

    const dom = {
        apiStatus: document.getElementById('api-status'),
        mapSummary: document.getElementById('map-summary'),
        refreshPredictions: document.getElementById('refresh-predictions'),
        loadPredictions: document.getElementById('load-predictions'),
        clearSelection: document.getElementById('clear-selection'),
        predictionDate: document.getElementById('prediction-date'),
        riskFilter: document.getElementById('risk-filter'),
        predictionStatus: document.getElementById('prediction-status'),
        predictionCount: document.getElementById('prediction-count'),
        statTotal: document.getElementById('stat-total'),
        statHigh: document.getElementById('stat-high'),
        statMedium: document.getElementById('stat-medium'),
        selectedRisk: document.getElementById('selected-risk'),
        advicePanel: document.getElementById('advice-panel')
    };

    let selectedGridId = null;
    const predictionLayer = L.geoJSON(null, {
        style: stylePredictionFeature,
        onEachFeature: bindPredictionFeature
    }).addTo(map);

    initializeDates();
    bindEvents();
    checkBackendHealth();
    loadPredictions();

    function initializeDates() {
        const today = new Date();
        dom.predictionDate.value = formatInputDate(today);
    }

    function bindEvents() {
        dom.refreshPredictions.addEventListener('click', loadPredictions);
        dom.loadPredictions.addEventListener('click', loadPredictions);
        dom.clearSelection.addEventListener('click', clearSelectedZone);
    }

    async function checkBackendHealth() {
        try {
            await fetchJson(`${API_BASE_URL}/`);
            dom.apiStatus.textContent = 'Backend online';
            dom.apiStatus.className = 'online';
        } catch (error) {
            dom.apiStatus.textContent = 'Backend offline';
            dom.apiStatus.className = 'offline';
        }
    }

    async function loadPredictions() {
        selectedGridId = null;
        updateAdviceEmptyState('No zone selected');
        setStatus(dom.predictionStatus, 'Loading predictions...', 'loading');
        predictionLayer.clearLayers();
        resetPredictionStats();

        const params = new URLSearchParams({
            date: dom.predictionDate.value || 'today',
            min_risk_level: dom.riskFilter.value
        });

        try {
            const geojson = await fetchJson(`${API_BASE_URL}/crimes/predict?${params.toString()}`);
            const features = Array.isArray(geojson.features) ? geojson.features : [];

            if (features.length === 0) {
                setStatus(dom.predictionStatus, 'No prediction zones found.', 'empty');
                dom.mapSummary.textContent = 'No prediction zones found';
                return;
            }

            predictionLayer.addData(geojson);
            updatePredictionStats(features);
            fitLayer(predictionLayer);
            setStatus(dom.predictionStatus, 'Prediction layer loaded.', 'success');
        } catch (error) {
            setStatus(dom.predictionStatus, error.message, 'error');
            dom.mapSummary.textContent = 'Backend connection error';
        }
    }

    function bindPredictionFeature(feature, layer) {
        const properties = feature.properties || {};
        const popupHtml = `
            <strong>${escapeHtml(properties.grid_id || 'Grid cell')}</strong>
            <div>Risk: ${escapeHtml(properties.risk_level || 'unknown')}</div>
            <div>Score: ${formatScore(properties.risk_score)}</div>
            <div>Type: ${escapeHtml(properties.dominant_crime_type || 'unknown')}</div>
        `;

        layer.bindPopup(popupHtml);
        layer.on('click', () => selectPredictionFeature(feature, layer));
    }

    async function selectPredictionFeature(feature, layer) {
        const properties = feature.properties || {};
        selectedGridId = properties.grid_id;
        predictionLayer.setStyle(stylePredictionFeature);
        layer.setStyle(selectedPredictionStyle());
        layer.bringToFront();

        dom.selectedRisk.textContent = properties.risk_level || 'Selected';
        dom.selectedRisk.className = `pill risk-${properties.risk_level || 'selected'}`;
        renderSelectedPredictionLoading(properties);

        if (!selectedGridId) {
            renderAdviceError('Selected zone is missing a grid ID.');
            return;
        }

        const params = new URLSearchParams({ grid_id: selectedGridId });
        if (properties.prediction_window) {
            params.set('prediction_window', properties.prediction_window);
        }

        try {
            const advice = await fetchJson(`${API_BASE_URL}/predict/advice?${params.toString()}`);
            renderAdvice(advice);
        } catch (error) {
            renderAdviceError(error.message);
        }
    }

    function renderSelectedPredictionLoading(properties) {
        dom.advicePanel.innerHTML = `
            <div class="zone-summary">
                <div>
                    <span class="label">Grid</span>
                    <strong>${escapeHtml(properties.grid_id || 'Unknown')}</strong>
                </div>
                <div>
                    <span class="label">Score</span>
                    <strong>${formatScore(properties.risk_score)}</strong>
                </div>
                <div>
                    <span class="label">Risk</span>
                    <strong>${escapeHtml(properties.risk_level || 'unknown')}</strong>
                </div>
            </div>
            <div class="loading-block">Loading advice...</div>
        `;
    }

    function renderAdvice(advice) {
        const facts = advice.facts || {};
        const temporal = facts.temporal_patterns || {};
        const crimeBreakdown = Array.isArray(facts.crime_breakdown) ? facts.crime_breakdown : [];
        const nearbyPlaces = Array.isArray(facts.nearby_places) ? facts.nearby_places : [];

        dom.selectedRisk.textContent = advice.risk_level || 'Selected';
        dom.selectedRisk.className = `pill risk-${advice.risk_level || 'selected'}`;

        dom.advicePanel.innerHTML = `
            ${advice.llm_error ? `<div class="notice warning">Fallback response used</div>` : ''}
            <div class="zone-summary">
                <div>
                    <span class="label">Grid</span>
                    <strong>${escapeHtml(advice.grid_id || selectedGridId || 'Unknown')}</strong>
                </div>
                <div>
                    <span class="label">Score</span>
                    <strong>${formatScore(advice.risk_score)}</strong>
                </div>
                <div>
                    <span class="label">Type</span>
                    <strong>${escapeHtml(advice.dominant_crime_type || 'unknown')}</strong>
                </div>
            </div>

            <div class="advice-block">
                <h3>Explanation</h3>
                <p>${escapeHtml(advice.explanation || 'No explanation returned.')}</p>
            </div>

            ${renderListBlock('Why Risk Is Elevated', advice.why_risky)}
            ${renderListBlock('Safety Advice', advice.safety_advice)}

            <dl class="fact-grid">
                <div>
                    <dt>Total Incidents</dt>
                    <dd>${formatInteger(facts.total_incidents)}</dd>
                </div>
                <div>
                    <dt>Recent 30 Days</dt>
                    <dd>${formatInteger(advice.recent_30_day_count)}</dd>
                </div>
                <div>
                    <dt>Peak Day</dt>
                    <dd>${escapeHtml(temporal.peak_day || 'n/a')}</dd>
                </div>
                <div>
                    <dt>Nearby Places</dt>
                    <dd>${nearbyPlaces.length}</dd>
                </div>
            </dl>

            ${renderCrimeBreakdown(crimeBreakdown)}

            <p class="disclaimer">${escapeHtml(advice.disclaimer || '')}</p>
        `;
    }

    function renderCrimeBreakdown(items) {
        if (!items.length) {
            return '';
        }

        const rows = items.slice(0, 5).map(item => `
            <tr>
                <td>${escapeHtml(item.type || item.crime_type || 'unknown')}</td>
                <td>${formatInteger(item.count)}</td>
            </tr>
        `).join('');

        return `
            <div class="advice-block">
                <h3>Crime Breakdown</h3>
                <table class="mini-table">
                    <tbody>${rows}</tbody>
                </table>
            </div>
        `;
    }

    function renderListBlock(title, items) {
        const safeItems = Array.isArray(items) ? items.filter(Boolean) : [];
        if (!safeItems.length) {
            return '';
        }

        const listItems = safeItems.map(item => `<li>${escapeHtml(item)}</li>`).join('');
        return `
            <div class="advice-block">
                <h3>${escapeHtml(title)}</h3>
                <ul>${listItems}</ul>
            </div>
        `;
    }

    function renderAdviceError(message) {
        dom.advicePanel.innerHTML = `
            <div class="notice error">${escapeHtml(message)}</div>
        `;
    }

    function clearSelectedZone() {
        selectedGridId = null;
        predictionLayer.setStyle(stylePredictionFeature);
        dom.selectedRisk.textContent = 'None';
        dom.selectedRisk.className = 'pill muted';
        updateAdviceEmptyState('No zone selected');
    }

    function updateAdviceEmptyState(message) {
        dom.advicePanel.innerHTML = `<div class="empty-state">${escapeHtml(message)}</div>`;
    }

    function stylePredictionFeature(feature) {
        const properties = feature.properties || {};
        const level = String(properties.risk_level || 'low').toLowerCase();
        const selected = properties.grid_id && properties.grid_id === selectedGridId;
        const palette = {
            high: { color: '#b91c1c', fillColor: '#ef4444', fillOpacity: 0.45 },
            medium: { color: '#c2410c', fillColor: '#f97316', fillOpacity: 0.36 },
            low: { color: '#a16207', fillColor: '#facc15', fillOpacity: 0.24 }
        };
        const base = palette[level] || palette.low;

        return selected ? selectedPredictionStyle() : {
            ...base,
            weight: 1.4,
            opacity: 0.95
        };
    }

    function selectedPredictionStyle() {
        return {
            color: '#111827',
            fillColor: '#ef4444',
            fillOpacity: 0.58,
            weight: 3,
            opacity: 1
        };
    }

    function updatePredictionStats(features) {
        const stats = features.reduce((acc, feature) => {
            const level = String(feature.properties?.risk_level || 'low').toLowerCase();
            acc.total += 1;
            if (level === 'high') acc.high += 1;
            if (level === 'medium') acc.medium += 1;
            return acc;
        }, { total: 0, high: 0, medium: 0 });

        dom.predictionCount.textContent = `${stats.total} zones`;
        dom.statTotal.textContent = stats.total;
        dom.statHigh.textContent = stats.high;
        dom.statMedium.textContent = stats.medium;
        dom.mapSummary.textContent = `${stats.total} predicted zones: ${stats.high} high, ${stats.medium} medium`;
    }

    function resetPredictionStats() {
        dom.predictionCount.textContent = '0 zones';
        dom.statTotal.textContent = '0';
        dom.statHigh.textContent = '0';
        dom.statMedium.textContent = '0';
        dom.mapSummary.textContent = 'Loading prediction layer';
    }

    function fitLayer(layer) {
        const bounds = layer.getBounds();
        if (bounds.isValid()) {
            map.fitBounds(bounds, { padding: [24, 24], maxZoom: 14 });
        }
    }

    async function fetchJson(url) {
        const response = await fetch(url);
        let payload = null;

        try {
            payload = await response.json();
        } catch (error) {
            payload = null;
        }

        if (!response.ok) {
            const message = payload?.detail || `Request failed with status ${response.status}`;
            throw new Error(message);
        }

        return payload;
    }

    function setStatus(element, message, kind) {
        element.textContent = message;
        element.className = `status-text ${kind}`;
    }

    function formatInputDate(date) {
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
    }

    function formatScore(value) {
        const number = Number(value);
        if (!Number.isFinite(number)) return 'n/a';
        return number.toFixed(3).replace(/0+$/, '').replace(/\.$/, '');
    }

    function formatInteger(value) {
        const number = Number(value);
        if (!Number.isFinite(number)) return '0';
        return number.toLocaleString();
    }

    function escapeHtml(value) {
        return String(value ?? '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }
});
