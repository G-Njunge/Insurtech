# Insurtech
Team task sheet - https://docs.google.com/spreadsheets/d/1Xokvr5Wbj971KtAKTu1TXAzik2ZTFbefU900CTHZPxE/edit?gid=0#gid=0

Video demo upload :
https://youtu.be/0uDv7UfWONs
A risk analytics platform built on NYC yellow taxi trip data. The system loads raw trip records, computes zone-level risk metrics (exposure, congestion, revenue volatility), and serves them through a Flask API. A static frontend provides a dashboard for exploring risk by zone and hour, and a demo page where individual drivers can look up their composite risk score.

Built by Group 7.


## Table of Contents

- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Data Processing and Cleaning](#data-processing-and-cleaning)
- [Database Setup](#database-setup)
- [Python Environment Setup](#python-environment-setup)
- [Loading Data](#loading-data)
- [Starting the Server](#starting-the-server)
- [Using the Application](#using-the-application)
- [API Endpoints](#api-endpoints)
- [Database Schema](#database-schema)
- [Risk Scoring Methodology](#risk-scoring-methodology)


## Project Structure

```
trials/
    data/
        cleaned_yellow_trips.csv      Raw trip data (1705 records)
        locations.csv                  TLC location lookup (50 locations)
    database/
        DATABASE_SCHEMA.sql            Full schema reference for all 9 tables
        load_data.py                   Step 1: Creates base tables and loads CSVs
        populate_precomputed_tables.py Step 2: Computes all risk/exposure metrics
        seed_drivers.py                Step 3: Creates driver profiles and operations
    dsa/
        app.py                         Flask backend (API + static file serving)
        database_config.py             MySQL connection configuration
    frontend/
        index.html                     Home / landing page
        dashboard.html                 Risk dashboard with KPIs and charts
        dashboard.js                   Dashboard logic (API calls, charts, tables)
        drivers.html                   Driver risk calculator (demo page)
        drivers.js                     Driver risk form and result rendering
        styles.css                     All styles for every page
        image/                         Static images
    README.md
```


## Prerequisites

Before you begin, make sure you have the following installed:

- Python 3.10 or higher
- MySQL 8.0 or higher
- pip (Python package manager)
- Git (to clone the repository)


## Database Setup

1. Open a MySQL shell as root:

```
mysql -u root -p
```

2. Create the database:

```sql
CREATE DATABASE nyc_taxi_temp;
```

3. Create a dedicated application user:

```sql
CREATE USER 'trials_user'@'localhost' IDENTIFIED WITH mysql_native_password BY 'trials_pass';
GRANT ALL PRIVILEGES ON nyc_taxi_temp.* TO 'trials_user'@'localhost';
FLUSH PRIVILEGES;
```

4. Exit the MySQL shell:

```sql
EXIT;
```

If you want to use different credentials, edit the values in `dsa/database_config.py`:

```python
host="127.0.0.1"
user="trials_user"
password="trials_pass"
database="nyc_taxi_temp"
```


## Data Processing and Cleaning

Before loading data, the raw trip records go through a comprehensive cleaning pipeline (`database/data_cleaning.py`) that:
- Validates data against domain constraints (valid zones, realistic distances/fares, reasonable passenger counts)
- Handles missing values (excludes critical fields, imputes non-critical ones with median)
- Detects and removes exact duplicates (using composite key: pickup_time, dropoff_time, locations, fare)
- Normalizes all formats (ISO 8601 timestamps, 2-decimal precision for amounts, proper data types)
- Logs every exclusion with reasoning (JSON log + human-readable report)

**Quality checks performed:**
- Location IDs: 1-263 (valid NYC zones)
- Trip distance: 0.1-100 miles
- Fare amount: $2.50-$500
- Passenger count: 1-6
- Trip duration: 1 minute to 8 hours
- No future timestamps

**To run the cleaning pipeline:**
```bash
python database/data_cleaning.py
```

This generates:
- `yellow_trips_cleaned.csv` - Cleaned data ready for database load
- `data_cleaning_log.json` - Detailed log of all exclusions (machine-readable)
- `data_cleaning_report.txt` - Human-readable summary with statistics

**Expected result:** ~97% data retention (55 records excluded from ~1,705 due to data quality issues)


## Python Environment Setup

1. Clone the repository and navigate into it:

```
git clone <repository-url>
cd trials
```

2. (Optional but recommended) Create a virtual environment:

```
python -m venv venv
```

Activate it:

- Windows: `venv\Scripts\activate`
- macOS/Linux: `source venv/bin/activate`

3. Install the required Python packages:

```
pip install flask mysql-connector-python pandas numpy
```


## Database Setup

1. Open a MySQL shell as root:

```
mysql -u root -p
```

2. Create the database:

```sql
CREATE DATABASE nyc_taxi_temp;
```

3. Create a dedicated application user:

```sql
CREATE USER 'trials_user'@'localhost' IDENTIFIED WITH mysql_native_password BY 'trials_pass';
GRANT ALL PRIVILEGES ON nyc_taxi_temp.* TO 'trials_user'@'localhost';
FLUSH PRIVILEGES;
```

4. Exit the MySQL shell:

```sql
EXIT;
```

If you want to use different credentials, edit the values in `api/database_config.py`:

```python
host="127.0.0.1"
user="trials_user"
password="trials_pass"
database="nyc_taxi_temp"
```


## Loading Data

Run the following scripts in order from the project root directory to populate the database.

**Step 0 (Optional but Recommended): Clean the raw data**

```
python database/data_cleaning.py
```

This optional step runs the data cleaning pipeline before database load. It generates a detailed cleaning report showing how many records were excluded and why. The script:
- Validates all data quality thresholds (distance, fare, location, passenger count, etc.)
- Detects and removes duplicates and anomalies
- Normalizes timestamps and numeric fields
- Produces: `data/data_cleaning_log.json` (detailed log) and `data/data_cleaning_report.txt` (summary)

This step is useful for demonstrating the data cleaning process described in the rubric, though the pre-cleaned CSV already has high data quality.

**Step 1: Create base tables and load data**

```
python database/load_data.py
```

This reads `data/locations.csv` and `data/cleaned_yellow_trips.csv`, creates the zone, location, and trip tables, and inserts all records. Expected output: 50 zones, 50 locations, 1,705 trips.

**Step 2: Compute precomputed metric tables**

```
python database/populate_precomputed_tables.py
```

This creates and populates zone_hourly_metrics, zone_hourly_risk, zone_hourly_details, and overview_metrics. It computes trip density, exposure index, congestion index, revenue volatility, and composite risk scores for every zone-hour combination (1,200 records: 50 zones × 24 hours).

**Step 3: Seed driver profiles**

```
python database/seed_drivers.py
```

This analyzes the trip table to find unique (vendor_id, pickup_location_id) combinations, creates 93 driver profiles in the user table with realistic names, and builds 748 driver_operations records linking each driver to their zones, hours, trip counts, and risk levels.

After all three steps, the database will contain 9 tables with approximately 5,047 records.


## Starting the Server

Start the Flask backend:

```
python dsa/app.py
```

The server starts on http://127.0.0.1:5000. It serves both the API endpoints and the frontend pages. There is no separate frontend server needed.

If port 5000 is already in use, stop the existing process first or change the port in the last line of `dsa/app.py`.


## Using the Application

Open a browser and go to http://127.0.0.1:5000. You will see the landing page.

### Home Page

The landing page with a link to the risk dashboard.

### Dashboard (dashboard.html)

Displays four KPI cards at the top:
- Total Trips: number of trip records in the database
- High-Risk Zones: zones with risk score above 50
- Peak Exposure Hour: the hour with the most trip activity
- Revenue Volatility Score: average fare volatility across zones

Below the KPIs is an hourly trip density chart and a top risk zones table. Use the hour slider to filter the risk zones table by hour (0 to 23). Click any zone row to see detailed metrics for that zone.

### Driver Risk Demo (drivers.html)

Enter a Driver ID (1 to 93) and click "Calculate Risk" to see:
- A personalized risk assessment message
- A visual risk gauge (scale 10 to 80)
- Operating profile: zones, active hours, trips analyzed
- A detailed explanation of how the score was calculated

The composite risk score is computed as: 10 + (weighted average zone risk x 70), capped between 10 and 80.


## API Endpoints

All endpoints return JSON.

### GET /api/overview

Returns a summary of the full dataset.

Response:
```json
{
  "total_trips": 1705,
  "high_risk_zones_count": 12,
  "peak_exposure_hour": 8,
  "avg_revenue_volatility": 9.45
}
```

### GET /api/zone/<zone_id>

Returns hourly detail for a specific zone.

Example: GET /api/zone/1

Response includes zone_name, hour, trip_count, avg_trip_duration, exposure_index, revenue_volatility, stability_score, and risk_score for each hour.

### GET /api/top_zones?hour=H

Returns the top 10 riskiest zones for a given hour (0-23).

Example: GET /api/top_zones?hour=8

Response:
```json
{
  "hour": 8,
  "zones": [
    {
      "zone_id": 5,
      "zone_name": "Midtown",
      "borough": "Manhattan",
      "risk_score": 72.5,
      "trip_count": 45,
      "exposure_index": 85.0
    }
  ]
}
```

### POST /api/driver-risk

Calculates the composite risk score for a driver.

Request body:
```json
{
  "driver_id": 1
}
```

Response includes the driver name, composite risk score (10-80), risk level (Low/Medium/High/Very High), operating zones and hours, trip count, and a personalized message explaining the assessment.


## Database Schema

The full schema with all column definitions, data types, and foreign keys is documented in `database/DATABASE_SCHEMA.sql`.

Summary of tables:

| Table | Rows | Purpose |
|-------|------|---------|
| zone | 50 | NYC taxi zone definitions |
| location | 50 | Pickup/dropoff location lookup |
| trip | 1705 | Raw trip records from TLC data |
| user | 93 | Synthesized driver profiles |
| driver_operations | 748 | Per-driver zone/hour aggregates |
| overview_metrics | 1 | Single-row dashboard summary |
| zone_hourly_metrics | 1200 | Core analytics (50 zones x 24 hours) |
| zone_hourly_risk | 1200 | Lightweight risk lookup by zone/hour |
| zone_hourly_details | 1200 | Extended zone detail for drilldowns |

Key relationships:
- zone to location (location.zone_id references zone.zone_id)
- location to trip (trip.pickup_location_id and trip.dropoff_location_id reference location.loc_id)
- user to driver_operations (driver_operations.driver_id references user.user_id)
- zone to zone_hourly_metrics, zone_hourly_risk, zone_hourly_details (all keyed by zone_id and hour)


## Risk Scoring Methodology

All metrics are precomputed per zone per hour.

1. Trip Density: COUNT of trips per zone per hour.

2. Exposure Index: (zone trip count / max trip count for that hour) x 100. Normalized to a 0-100 scale so zones are comparable within the same hour.

3. Average Trip Duration: AVG of TIMESTAMPDIFF(MINUTE, pickup_time, dropoff_time) per zone per hour.

4. Congestion Index: (avg_trip_duration x exposure_index) / 100. High traffic duration combined with high exposure indicates congestion.

5. Revenue Volatility: STDDEV of fare_amount per zone. High standard deviation means unstable earnings.

6. Composite Risk Score: (0.4 x exposure_index) + (0.3 x normalized_congestion) + (0.3 x normalized_volatility). Congestion and volatility are normalized to 0-100 using their respective maximums before combining.

7. Driver Risk Score: For each driver, a weighted average of avg_risk_in_zone across all their operating zone-hour combinations (weighted by trips_in_period). The final score is mapped to a 10-80 scale: score = 10 + (weighted_avg_risk x 70).

Risk levels:
- Below 25: Low
- 25 to 44: Medium
- 45 to 64: High
- 65 and above: Very High