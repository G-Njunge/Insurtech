"""Query zone risk heatmap for a given hour. Usage: python query_risk_by_hour.py [hour]"""
import os
import sqlite3
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DB_PATH = os.path.join(PROJECT_ROOT, "data", "insurtech_taxi.db")


def main():
    hour = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    if hour < 0 or hour > 23:
        print("Hour must be 0-23")
        sys.exit(1)
    if not os.path.isfile(DB_PATH):
        print(f"DB not found: {DB_PATH}. Run init_db.py and load data first.")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT zone_id, trip_count, exposure_index, avg_trip_duration_min, congestion_index, risk_score FROM zone_hourly_metrics WHERE hour = ? ORDER BY risk_score DESC",
        (hour,),
    )
    rows = cur.fetchall()
    conn.close()

    print(f"Risk heatmap for hour {hour} (top 20 zones by risk_score):")
    print("-" * 80)
    for r in rows[:20]:
        print(f"  zone_id={r['zone_id']:4}  trips={r['trip_count']:4}  exposure={r['exposure_index']:.1f}  avg_dur={r['avg_trip_duration_min']:.1f}m  congestion={r['congestion_index']:.2f}  risk_score={r['risk_score']:.2f}")
    print(f"\nTotal zone-hour rows for hour {hour}: {len(rows)}")


if __name__ == "__main__":
    main()
