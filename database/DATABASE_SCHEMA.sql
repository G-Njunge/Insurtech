-- Insurtech Database Schema
-- Database: nyc_taxi_temp (MySQL 8+)


-- 1. Zone (50 rows)
-- All the NYC taxi zones from the TLC lookup data
CREATE TABLE zone (
    zone_id       INT            NOT NULL AUTO_INCREMENT,
    zone_name     VARCHAR(100)   NOT NULL UNIQUE,
    borough       VARCHAR(50)    DEFAULT NULL,
    service_zone  VARCHAR(50)    DEFAULT NULL,
    PRIMARY KEY (zone_id)
) ENGINE=InnoDB;


-- 2. Location (50 rows)
-- Each pickup or dropoff spot, linked to a zone
CREATE TABLE location (
    loc_id        INT            NOT NULL,
    borough       VARCHAR(50)    DEFAULT NULL,
    zone_name     VARCHAR(100)   DEFAULT NULL,
    service_zone  VARCHAR(50)    DEFAULT NULL,
    zone_id       INT            DEFAULT NULL,
    PRIMARY KEY (loc_id)
) ENGINE=InnoDB;


-- 3. Trip (1705 rows)
-- Actual taxi trip records from NYC yellow cab data
CREATE TABLE trip (
    trip_id                       INT            NOT NULL AUTO_INCREMENT,
    vendor_id                     INT            DEFAULT NULL,
    pickup_time                   DATETIME       NOT NULL,
    dropoff_time                  DATETIME       NOT NULL,
    passenger_count               INT            DEFAULT NULL,
    trip_distance                 DECIMAL(10,2)  DEFAULT NULL,
    pickup_location_id            INT            DEFAULT NULL,
    dropoff_location_id           INT            DEFAULT NULL,
    fare_amount                   DECIMAL(10,2)  DEFAULT NULL,
    total_amount                  DECIMAL(10,2)  DEFAULT NULL,
    original_pickup_location_id   INT            DEFAULT NULL,
    original_dropoff_location_id  INT            DEFAULT NULL,
    original_pickup_time          DATETIME       DEFAULT NULL,
    original_dropoff_time         DATETIME       DEFAULT NULL,
    PRIMARY KEY (trip_id),
    INDEX idx_pickup_time  (pickup_time),
    INDEX idx_pickup_loc   (pickup_location_id),
    INDEX idx_dropoff_loc  (dropoff_location_id)
) ENGINE=InnoDB;


-- 4. User (93 rows)
-- Made-up driver profiles built from unique vendor + pickup location combos
CREATE TABLE user (
    user_id    INT            NOT NULL,
    user_name  VARCHAR(100)   NOT NULL,
    PRIMARY KEY (user_id)
) ENGINE=InnoDB;


-- 5. Driver Operations (748 rows)
-- Shows how many trips each driver made in each zone at each hour
CREATE TABLE driver_operations (
    id               INT            NOT NULL AUTO_INCREMENT,
    driver_id        INT            NOT NULL,
    zone_id          INT            NOT NULL,
    hour             INT            NOT NULL,
    trips_in_period  INT            DEFAULT 0,
    avg_risk_in_zone DECIMAL(10,4)  DEFAULT 0.0000,
    PRIMARY KEY (id),
    INDEX idx_driver (driver_id),
    FOREIGN KEY (driver_id) REFERENCES user(user_id) ON DELETE CASCADE
) ENGINE=InnoDB;


-- 6. Overview Metrics (1 row)
-- One row with the big picture numbers for the dashboard
CREATE TABLE overview_metrics (
    id                      INT            NOT NULL,
    total_trips             INT            DEFAULT NULL,
    high_risk_zones         INT            DEFAULT NULL,
    peak_exposure_hour      INT            DEFAULT NULL,
    avg_revenue_volatility  DECIMAL(5,2)   DEFAULT NULL,
    last_updated            TIMESTAMP      DEFAULT CURRENT_TIMESTAMP
                                           ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB;


-- 7. Zone Hourly Metrics (1200 rows)
-- The main analytics table with one row per zone per hour
-- Has trip counts, exposure, congestion, fare swings, and risk scores
CREATE TABLE zone_hourly_metrics (
    zone_id             INT            NOT NULL,
    hour                INT            NOT NULL,
    trip_count          INT            DEFAULT 0,
    exposure_index      DECIMAL(10,2)  DEFAULT NULL,
    avg_trip_duration   DECIMAL(10,2)  DEFAULT NULL,
    congestion_index    DECIMAL(10,2)  DEFAULT NULL,
    revenue_volatility  DECIMAL(10,2)  DEFAULT NULL,
    risk_score          DECIMAL(10,2)  DEFAULT NULL,
    zone_name           VARCHAR(100)   DEFAULT NULL,
    PRIMARY KEY (zone_id, hour),
    INDEX idx_hour (hour),
    INDEX idx_zone (zone_id),
    INDEX idx_risk (risk_score),
    FOREIGN KEY (zone_id) REFERENCES zone(zone_id) ON DELETE CASCADE
) ENGINE=InnoDB;




