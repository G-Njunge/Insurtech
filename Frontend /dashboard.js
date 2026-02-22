let densityChart;
let revenueChart;
let map;
let mapLayer;
let geojsonData;
let currentHour = 17;
const zoneMetricsById = new Map();

document.addEventListener("DOMContentLoaded", () => {
  const b = document.body;
  if (b && b.classList.contains("page-dashboard")) {
    initializeDashboard();
  }
});

function initializeDashboard() {
const hourSlider = document.getElementById("hourSlider");
const hourLabel = document.getElementById("hourLabel");
if (hourSlider && hourLabel) {
hourSlider.addEventListener("input", () => {
const value = parseInt(hourSlider.value, 10);
currentHour = value;
hourLabel.textContent = formatHourLabel(value);
loadMapForHour(value);
loadTopZones(value);
});
hourLabel.textContent = formatHourLabel(currentHour);
}
loadOverview();
loadHourlyDensitySeries();
initializeMap();
loadRevenueMetrics();
loadTopZones(currentHour);
}

function formatHourLabel(hour) {
const padded = hour.toString().padStart(2, "0");
return padded + ":00";
}

async function loadOverview() {
try {
const res = await fetch("/api/overview");
if (!res.ok) {
return;
}
const data = await res.json();
setText("kpi-total-trips", formatNumber(data.total_trips));
setText("kpi-high-risk-zones", formatNumber(data.high_risk_zones_count));
setText("kpi-peak-hour", formatHourLabel(Number(data.peak_exposure_hour || 0)));
setText("kpi-revenue-volatility", formatDecimal(data.revenue_volatility_score));
} catch (e) {
}
}

async function loadHourlyDensitySeries() {
try {
const res = await fetch("/api/hourly_density");
if (!res.ok) {
return;
}
const data = await res.json();
const hours = data.map(d => d.hour);
const counts = data.map(d => d.total_trips);
renderDensityChart(hours, counts);
} catch (e) {
}
}

function renderDensityChart(hours, counts) {
const ctx = document.getElementById("densityChart");
if (!ctx) {
return;
}
if (densityChart) {
densityChart.destroy();
}
densityChart = new Chart(ctx, {
type: "line",
data: {
labels: hours.map(formatHourLabel),
datasets: [
{
label: "Trips",
data: counts,
borderColor: "#38bdf8",
backgroundColor: "rgba(56, 189, 248, 0.1)",
tension: 0.35,
fill: true,
pointRadius: 0,
borderWidth: 2
}
]
},
options: {
responsive: true,
maintainAspectRatio: false,
plugins: {
legend: {
display: false
},
tooltip: {
mode: "index",
intersect: false
}
},
scales: {
x: {
grid: {
color: "rgba(31, 41, 55, 0.8)"
},
ticks: {
color: "#9ca3af",
maxRotation: 0
}
},
y: {
grid: {
color: "rgba(31, 41, 55, 0.8)"
},
ticks: {
color: "#9ca3af",
callback: value => formatCompact(value)
}
}
}
}
});
}

async function initializeMap() {
const mapElement = document.getElementById("map");
const fallback = document.getElementById("mapFallback");
if (!mapElement) {
return;
}
try {
map = L.map(mapElement, {
zoomSnap: 0.5,
worldCopyJump: false
}).setView([40.72, -73.97], 11.4);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
maxZoom: 18,
attribution: ""
}).addTo(map);
try {
const res = await fetch("zones.geojson");
if (res.ok) {
geojsonData = await res.json();
if (fallback) {
fallback.classList.add("hidden");
}
} else if (fallback) {
fallback.classList.remove("hidden");
}
} catch (e) {
if (fallback) {
fallback.classList.remove("hidden");
}
}
loadMapForHour(currentHour);
} catch (e) {
if (fallback) {
fallback.classList.remove("hidden");
}
}
}

async function loadMapForHour(hour) {
// Map was removed, nothing to do
return;
}

function colorForRisk(score) {
const value = Number(score || 0);
if (value <= 0) {
return "#0f172a";
}
if (value < 0.4) {
return "#22c55e";
}
if (value < 0.7) {
return "#eab308";
}
return "#ef4444";
}

function renderTopZonesTableFromMap() {
const tbody = document.getElementById("topZonesBody");
if (!tbody) {
return;
}
if (zoneMetricsById.size === 0) {
return;
}
const rows = [];
zoneMetricsById.forEach(value => {
rows.push(value);
});
let sorted = [];
while (rows.length > 0 && sorted.length < 5) {
let bestIndex = 0;
let bestValue = rows[0];
for (let i = 1; i < rows.length; i++) {
const current = rows[i];
if (Number(current.risk_score || 0) > Number(bestValue.risk_score || 0)) {
bestIndex = i;
bestValue = current;
}
}
sorted.push(bestValue);
rows.splice(bestIndex, 1);
}
tbody.innerHTML = "";
for (let i = 0; i < sorted.length; i++) {
const item = sorted[i];
const tr = document.createElement("tr");
tr.dataset.zoneId = String(item.zone_id);
tr.addEventListener("click", () => {
handleZoneSelection(String(item.zone_id));
});
const zoneCell = document.createElement("td");
zoneCell.textContent = item.zone_name || item.zone_id;
const boroughCell = document.createElement("td");
boroughCell.textContent = item.borough || "";
const riskCell = document.createElement("td");
riskCell.textContent = formatDecimal(item.risk_score);
const tripsCell = document.createElement("td");
tripsCell.textContent = formatNumber(item.trip_count || 0);
const exposureCell = document.createElement("td");
exposureCell.textContent = formatDecimal(item.exposure_score);
tr.appendChild(zoneCell);
tr.appendChild(boroughCell);
tr.appendChild(riskCell);
tr.appendChild(tripsCell);
tr.appendChild(exposureCell);
tbody.appendChild(tr);
}
}

async function loadTopZones(hour) {
try {
const tbody = document.getElementById("topZonesBody");
const res = await fetch("/api/top_zones?hour=" + hour);
if (!res.ok) {
return;
}
const data = await res.json();
if (!tbody) {
return;
}
tbody.innerHTML = "";
for (let i = 0; i < data.length; i++) {
const item = data[i];
const tr = document.createElement("tr");
tr.dataset.zoneId = String(item.zone_id);
tr.addEventListener("click", () => {
handleZoneSelection(String(item.zone_id));
});
const zoneCell = document.createElement("td");
zoneCell.textContent = item.zone_name || item.zone_id;
const boroughCell = document.createElement("td");
boroughCell.textContent = item.borough || "";
const riskCell = document.createElement("td");
riskCell.textContent = formatDecimal(item.risk_score);
const tripsCell = document.createElement("td");
tripsCell.textContent = formatNumber(item.trip_count || 0);
const exposureCell = document.createElement("td");
exposureCell.textContent = formatDecimal(item.exposure_score);
tr.appendChild(zoneCell);
tr.appendChild(boroughCell);
tr.appendChild(riskCell);
tr.appendChild(tripsCell);
tr.appendChild(exposureCell);
tbody.appendChild(tr);
}
} catch (e) {
}
}

async function handleZoneSelection(zoneId) {
const idValue = String(zoneId);
try {
const res = await fetch("/api/zone/" + encodeURIComponent(idValue) + "?hour=" + currentHour);
if (!res.ok) {
return;
}
const data = await res.json();
setText("detail-zone-name", data.zone_name || idValue);
setText("detail-zone-borough", data.borough || "");
setText("detail-trips-per-hour", formatNumber(data.trips_per_hour || 0));
setText("detail-avg-duration", formatDecimal(data.avg_trip_duration));
setText("detail-exposure-index", formatDecimal(data.exposure_index));
setText("detail-revenue-stability", formatDecimal(data.revenue_stability));
setText("detail-risk-score", formatDecimal(data.risk_score));
} catch (e) {
}
}

async function loadRevenueMetrics() {
try {
const res = await fetch("/api/revenue");
if (!res.ok) {
return;
}
const data = await res.json();
const labels = [];
const volatility = [];
const stability = [];
for (let i = 0; i < data.length; i++) {
const item = data[i];
labels.push(item.zone_name || item.zone_id);
volatility.push(Number(item.revenue_volatility || 0));
stability.push(Number(item.stability_score || 0));
}
renderRevenueChart(labels, volatility, stability);
} catch (e) {
}
}

function renderRevenueChart(labels, volatility, stability) {
const ctx = document.getElementById("revenueChart");
if (!ctx) {
return;
}
if (revenueChart) {
revenueChart.destroy();
}
revenueChart = new Chart(ctx, {
type: "bar",
data: {
labels,
datasets: [
{
label: "Volatility",
data: volatility,
backgroundColor: "rgba(248, 113, 113, 0.8)"
},
{
label: "Stability",
data: stability,
backgroundColor: "rgba(52, 211, 153, 0.9)"
}
]
},
options: {
responsive: true,
maintainAspectRatio: false,
plugins: {
legend: {
labels: {
color: "#9ca3af",
font: {
size: 10
}
}
}
},
scales: {
x: {
grid: {
display: false
},
ticks: {
color: "#9ca3af",
maxRotation: 45,
minRotation: 0,
autoSkip: true,
maxTicksLimit: 8
}
},
y: {
grid: {
color: "rgba(31, 41, 55, 0.8)"
},
ticks: {
color: "#9ca3af",
callback: value => formatCompact(value)
}
}
}
}
});
}

function setText(id, value) {
const el = document.getElementById(id);
if (!el) {
return;
}
el.textContent = value;
}

function formatNumber(value) {
const n = Number(value || 0);
if (isNaN(n)) {
return "0";
}
if (n >= 1000000) {
return (n / 1000000).toFixed(1) + "M";
}
if (n >= 1000) {
return (n / 1000).toFixed(1) + "K";
}
return n.toString();
}

function formatDecimal(value) {
const n = Number(value || 0);
if (isNaN(n)) {
return "0.00";
}
return n.toFixed(2);
}

function formatCompact(value) {
const n = Number(value || 0);
if (isNaN(n)) {
return "";
}
if (n >= 1000000) {
return (n / 1000000).toFixed(1) + "M";
}
if (n >= 1000) {
return (n / 1000).toFixed(1) + "K";
}
return n.toString();
}