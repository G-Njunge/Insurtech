# InsurTech Taxi Risk Model – Database & Implementation

This project implements **trip density**, **hourly exposure index**, **congestion intensity**, **revenue volatility**, and a **custom risk score** per zone per hour, with a SQLite database and Python scripts that are ready to run.

## Quick Start

1. **Create the database**
   ```bash
   python scripts/init_db.py
   ```

2. **Load trip data** (CSV or TSV with header; columns like `VendorID`, `tpep_pickup_datetime`, `PULocationID`, `total_amount`, etc.)
   ```bash
   python scripts/load_taxi_data.py data/taxi_trips_sample.tsv
   ```
   Or use your full file:
   ```bash
   python scripts/load_taxi_data.py path/to/your_taxi_trips.csv
   ```

3. **Compute zone-hourly metrics and risk score**
   ```bash
   python scripts/compute_zone_metrics.py
   ```

4. **Query risk heatmap by hour** (example)
   ```bash
   python scripts/query_risk_by_hour.py 0
   ```

## Entity Relationship Diagram

See **[docs/ERD.md](docs/ERD.md)** for the full ERD (Mermaid) and table descriptions.

- **taxi_trips**: Raw trip records; `pulocation_id` = pickup zone.
- **zone_revenue_metrics**: Per-zone revenue volatility (stddev) and average revenue.
- **zone_hourly_metrics**: Precomputed per zone per hour: `trip_count`, `exposure_index`, `avg_trip_duration_min`, `congestion_index`, `risk_score`.

## Data Flow

1. **Trip density** – Count of trips per `pulocation_id` per hour.
2. **Exposure index** – `(zone_trip_count / max_trip_count_for_that_hour) * 100` (0–100).
3. **Congestion index** – `avg_trip_duration_min * (exposure_index/100)` per zone-hour.
4. **Revenue volatility** – `STDDEV(total_amount)` per zone (stored in `zone_revenue_metrics`).
5. **Risk score** – In Python: `0.4*exposure + 0.3*congestion_normalized + 0.3*volatility_normalized`, all normalized to 0–100.

## File Layout

```
revised insurtech/
├── config.py
├── data/
│   ├── insurtech_taxi.db    # Created by init_db.py
│   ├── taxi_trips_sample.tsv
│   └── taxi_trips.csv       # Optional: your full data
├── docs/
│   └── ERD.md
├── schema/
│   └── 01_schema.sql
├── scripts/
│   ├── init_db.py
│   ├── load_taxi_data.py
│   ├── compute_zone_metrics.py
│   └── query_risk_by_hour.py
├── requirements.txt
└── README.md
```

## Input Data Format

Your file should have a header row and columns such as:

- `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`
- `PULocationID` (pickup zone), `DOLocationID`
- `total_amount`, and other fare fields.

Both comma- and tab-separated files are supported; the loader detects the delimiter.
