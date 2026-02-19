
-- =============================================
-- 1. zones - Central table for geographical zones
-- =============================================
CREATE TABLE zones (
    zone_id INTEGER PRIMARY KEY,
    borough VARCHAR(100),
    zone_name VARCHAR(255),
    service_zone VARCHAR(50)
);

-- =============================================
-- 2. zone_hour_metrics - Hourly metrics per zone
-- =============================================
CREATE TABLE zone_hour_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    zone_id INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    trip_count INTEGER,
    average_trip_duration REAL,
    exposure_score REAL,
    FOREIGN KEY (zone_id) REFERENCES zones(zone_id) ON DELETE CASCADE
);

CREATE INDEX idx_zone_hour_metrics_zone_id ON zone_hour_metrics(zone_id);
CREATE INDEX idx_zone_hour_metrics_hour ON zone_hour_metrics(hour);

-- =============================================
-- 3. zone_risk_scores - Risk scores per zone per hour
-- =============================================
CREATE TABLE zone_risk_scores (
    zone_id INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    risk_score REAL,
    PRIMARY KEY (zone_id, hour),
    FOREIGN KEY (zone_id) REFERENCES zones(zone_id) ON DELETE CASCADE
);

CREATE INDEX idx_zone_risk_scores_zone_id ON zone_risk_scores(zone_id);

-- =============================================
-- 4. zone_revenue_metrics - Revenue metrics per zone (one-to-one)
-- =============================================
CREATE TABLE zone_revenue_metrics (
    zone_id INTEGER PRIMARY KEY,
    average_revenue REAL,
    revenue_volatility REAL,
    stability_score REAL,
    FOREIGN KEY (zone_id) REFERENCES zones(zone_id) ON DELETE CASCADE
);
