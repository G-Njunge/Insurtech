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

