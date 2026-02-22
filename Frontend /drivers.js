// Handles the driver risk calculator page

document.addEventListener("DOMContentLoaded", () => {
  const form = document.getElementById("driverForm");
  if (!form) return;
  form.addEventListener("submit", handleSubmit);
});

async function handleSubmit(e) {
  e.preventDefault();

  const btn = document.getElementById("submitBtn");
  const errBox = document.getElementById("formError");
  const results = document.getElementById("resultsSection");

  const driverId = parseInt(document.getElementById("driverId").value, 10);

  // Clear any old errors and hide results
  errBox.classList.add("hidden");
  errBox.textContent = "";
  results.classList.add("hidden");
  btn.textContent = "Calculating…";
  btn.disabled = true;

  try {
    const res = await fetch("/api/driver-risk", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ driver_id: driverId })
    });

    if (!res.ok) {
      let msg = "Something went wrong (" + res.status + ")";
      try { const d = await res.json(); msg = d.error || msg; } catch (_) {}
      showError(errBox, msg);
      return;
    }

    const data = await res.json();
    renderResults(data);
    results.classList.remove("hidden");
    results.scrollIntoView({ behavior: "smooth", block: "start" });
  } catch (err) {
    showError(errBox, "Could not reach the server. Is it running? (" + err.message + ")");
  } finally {
    btn.textContent = "Calculate Risk";
    btn.disabled = false;
  }
}

function showError(box, msg) {
  box.textContent = msg;
  box.classList.remove("hidden");
}

function renderResults(data) {
  // Show the message at the top
  document.getElementById("personalizedMsg").textContent =
    data.personalized_message || "";

  // Set the risk badge color and text
  const risk = data.risk_assessment || {};
  const score = Number(risk.composite_risk_score) || 0;
  const level = risk.risk_level || "Unknown";

  const badge = document.getElementById("riskBadge");
  badge.textContent = level;
  badge.style.borderColor = colorForLevel(level);
  badge.style.color = colorForLevel(level);

  const driverName = data.driver ? data.driver.name : "Driver";
  document.getElementById("resultTitle").textContent =
    "Risk Report for " + driverName;

  // Draw the score gauge ring
  const gaugeValue = document.getElementById("gaugeValue");
  const gaugeRing = document.getElementById("gaugeRing");
  gaugeValue.textContent = score.toFixed(1);
  const deg = Math.round((score / 80) * 360);
  gaugeRing.style.background =
    "conic-gradient(" +
    colorForLevel(level) +
    " " +
    deg +
    "deg, #1f2937 " +
    deg +
    "deg)";

  document.getElementById("riskLevel").textContent = level + " Risk";
  document.getElementById("riskLevel").style.color = colorForLevel(level);

  // Fill in the driver's zone, hours, and trip count
  const prof = data.operating_profile || {};
  const zones = (prof.zones || []).map(z => z.zone_name).join(", ") || "—";
  const hours =
    (prof.hours || []).map(h => String(h).padStart(2, "0") + ":00").join(", ") ||
    "—";
  document.getElementById("profZones").textContent = zones;
  document.getElementById("profHours").textContent = hours;
  document.getElementById("profTrips").textContent =
    prof.total_trips_analyzed != null ? prof.total_trips_analyzed : "—";

  const calc = data.calculation_logic || {};
  document.getElementById("profMethod").textContent =
    calc.methodology || "Based on your areas and work hours";

  // Show the explanation paragraph
  document.getElementById("explanationText").textContent =
    data.explanation || "";
}

function colorForLevel(level) {
  switch ((level || "").toLowerCase()) {
    case "low":
      return "#22c55e";
    case "medium":
      return "#eab308";
    case "high":
      return "#f97316";
    case "very high":
      return "#ef4444";
    default:
      return "#9ca3af";
  }
}