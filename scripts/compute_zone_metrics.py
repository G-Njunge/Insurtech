"""
Compute zone-hourly metrics and risk score (DSA: algorithmic part in Python).

Flow:
  1. Trip density: trips per zone per hour
  2. Exposure index: (zone_trip_count / max_trip_count_for_hour) * 100
  3. Avg trip duration per zone per hour -> congestion_index = avg_duration * exposure_index
  4. Zone revenue: stddev(total_amount), avg(total_amount) per zone
  5. Risk score: 0.4*exposure + 0.3*congestion_norm + 0.3*volatility_norm (all 0-100)
"""
import os
import sqlite3
from collections import defaultdict
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DB_PATH = os.path.join(DATA_DIR, "insurtech_taxi.db")

# Risk score weights
WEIGHT_EXPOSURE = 0.4
WEIGHT_CONGESTION = 0.3
WEIGHT_VOLATILITY = 0.3


def _parse_dt(s):
    if s is None:
        return None
    s = str(s).strip()
    for fmt in ("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def _duration_minutes(pickup_str, dropoff_str):
    pu = _parse_dt(pickup_str)
    do = _parse_dt(dropoff_str)
    if pu is None or do is None:
        return None
    delta = do - pu
    return max(0.0, delta.total_seconds() / 60.0)


def _normalize_to_100(values_dict, key_func):
    """Normalize values to 0-100 scale: (x / max) * 100. max must be > 0."""
    if not values_dict:
        return values_dict
    all_vals = [key_func(v) for v in values_dict.values()]
    max_val = max(all_vals)
    if max_val <= 0:
        return {k: 0.0 for k in values_dict}
    return {k: (key_func(v) / max_val) * 100.0 for k, v in values_dict.items()}


def compute_zone_revenue_metrics(conn):
    """Per-zone: revenue_volatility (stddev), avg_revenue, total_trips."""
    cur = conn.execute(
        "SELECT pulocation_id, total_amount FROM taxi_trips WHERE total_amount IS NOT NULL"
    )
    zone_amounts = defaultdict(list)
    for zone_id, amount in cur:
        try:
            zone_amounts[zone_id].append(float(amount))
        except (TypeError, ValueError):
            pass

    rows = []
    for zone_id, amounts in zone_amounts.items():
        n = len(amounts)
        if n == 0:
            continue
        avg = sum(amounts) / n
        variance = sum((x - avg) ** 2 for x in amounts) / n
        stddev = variance ** 0.5
        rows.append((zone_id, stddev, avg, n))

    conn.executemany(
        "INSERT OR REPLACE INTO zone_revenue_metrics (zone_id, revenue_volatility, avg_revenue, total_trips) VALUES (?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    return len(rows)


def compute_zone_hourly_metrics(conn):
    """Build zone_hourly_metrics: trip_count, exposure_index, avg_trip_duration_min, congestion_index, risk_score."""
    cur = conn.execute(
        "SELECT tpep_pickup_datetime, tpep_dropoff_datetime, pulocation_id, total_amount FROM taxi_trips"
    )

    # (zone_id, hour) -> list of trip durations
    zone_hour_durations = defaultdict(list)
    # (zone_id, hour) -> count (we can derive from durations length)
    for row in cur:
        pickup_str, dropoff_str, zone_id, _ = row
        pu = _parse_dt(pickup_str)
        if pu is None:
            continue
        hour = pu.hour
        dur = _duration_minutes(pickup_str, dropoff_str)
        if dur is not None:
            zone_hour_durations[(zone_id, hour)].append(dur)
        else:
            zone_hour_durations[(zone_id, hour)].append(0.0)

    # Trip density: (zone_id, hour) -> trip_count
    trip_count = {(z, h): len(durs) for (z, h), durs in zone_hour_durations.items()}

    # Max trip count per hour (for exposure)
    max_per_hour = {}
    for (z, h), cnt in trip_count.items():
        max_per_hour[h] = max(max_per_hour.get(h, 0), cnt)

    # Exposure index per (zone_id, hour): (trip_count / max_for_hour) * 100
    exposure_index = {}
    for (z, h), cnt in trip_count.items():
        m = max_per_hour.get(h, 1)
        exposure_index[(z, h)] = (cnt / m) * 100.0 if m > 0 else 0.0

    # Avg duration per (zone_id, hour)
    avg_duration = {}
    for (z, h), durs in zone_hour_durations.items():
        avg_duration[(z, h)] = sum(durs) / len(durs) if durs else 0.0

    # Congestion index = avg_trip_duration * exposure_index (raw; we normalize for risk)
    congestion_index = {}
    for (z, h) in zone_hour_durations:
        congestion_index[(z, h)] = avg_duration[(z, h)] * (exposure_index.get((z, h), 0) / 100.0)

    # Zone revenue metrics (already in DB)
    cur = conn.execute(
        "SELECT zone_id, revenue_volatility FROM zone_revenue_metrics"
    )
    zone_volatility = dict(cur.fetchall())

    # Normalize congestion to 0-100 over all zone-hours
    cong_max = max(congestion_index.values()) if congestion_index else 1.0
    cong_norm = {k: (v / cong_max) * 100.0 if cong_max > 0 else 0.0 for k, v in congestion_index.items()}

    # Volatility per zone: normalize to 0-100
    vol_max = max(zone_volatility.values()) if zone_volatility else 1.0
    vol_norm_by_zone = {z: (v / vol_max) * 100.0 if vol_max > 0 else 0.0 for z, v in zone_volatility.items()}

    # Risk score: 0.4*exposure + 0.3*congestion_norm + 0.3*volatility_norm
    conn.execute("DELETE FROM zone_hourly_metrics")
    rows = []
    for (zone_id, hour) in sorted(trip_count.keys()):
        exp = exposure_index.get((zone_id, hour), 0.0)
        cong_n = cong_norm.get((zone_id, hour), 0.0)
        vol_n = vol_norm_by_zone.get(zone_id, 0.0)
        risk = WEIGHT_EXPOSURE * exp + WEIGHT_CONGESTION * cong_n + WEIGHT_VOLATILITY * vol_n
        cnt = trip_count[(zone_id, hour)]
        avg_dur = avg_duration.get((zone_id, hour), 0.0)
        cong_raw = congestion_index.get((zone_id, hour), 0.0)
        rows.append((zone_id, hour, cnt, exp, avg_dur, cong_raw, round(risk, 4)))

    conn.executemany(
        "INSERT INTO zone_hourly_metrics (zone_id, hour, trip_count, exposure_index, avg_trip_duration_min, congestion_index, risk_score) VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    return len(rows)


def main():
    if not os.path.isfile(DB_PATH):
        raise FileNotFoundError(f"Database not found. Run init_db.py first: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    try:
        n_rev = compute_zone_revenue_metrics(conn)
        print(f"Computed zone_revenue_metrics for {n_rev} zones.")
        n_hourly = compute_zone_hourly_metrics(conn)
        print(f"Computed zone_hourly_metrics: {n_hourly} zone-hour rows.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
