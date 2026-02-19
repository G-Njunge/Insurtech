-- InsurTech Taxi Risk Model - Base Schema
-- SQLite-compatible

-- Raw taxi trips (TLC-style)
CREATE TABLE taxi_trips (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vendor_id INTEGER,
    tpep_pickup_datetime TEXT NOT NULL,
    tpep_dropoff_datetime TEXT NOT NULL,
    passenger_count INTEGER,
    trip_distance REAL,
    ratecode_id INTEGER,
    store_and_fwd_flag TEXT,
    pulocation_id INTEGER NOT NULL,
    dolocation_id INTEGER,
    payment_type INTEGER,
    fare_amount REAL,
    extra REAL,
    mta_tax REAL,
    tip_amount REAL,
    tolls_amount REAL,
    improvement_surcharge REAL,
    total_amount REAL,
    congestion_surcharge REAL
);

CREATE INDEX idx_taxi_trips_pulocation ON taxi_trips(pulocation_id);
CREATE INDEX idx_taxi_trips_pickup_datetime ON taxi_trips(tpep_pickup_datetime);
CREATE INDEX idx_taxi_trips_zone_hour ON taxi_trips(pulocation_id, tpep_pickup_datetime);

-- Per-zone revenue metrics (volatility, avg revenue)
CREATE TABLE zone_revenue_metrics (
    zone_id INTEGER PRIMARY KEY,
    revenue_volatility REAL,
    avg_revenue REAL,
    total_trips INTEGER
);

-- Precomputed zone-hour metrics (trip density, exposure, congestion, risk)
CREATE TABLE zone_hourly_metrics (
    zone_id INTEGER NOT NULL,
    hour INTEGER NOT NULL CHECK (hour >= 0 AND hour <= 23),
    trip_count INTEGER NOT NULL DEFAULT 0,
    exposure_index REAL,
    avg_trip_duration_min REAL,
    congestion_index REAL,
    risk_score REAL,
    PRIMARY KEY (zone_id, hour)
);

CREATE INDEX IF NOT EXISTS idx_zone_hourly_hour ON zone_hourly_metrics(hour);
