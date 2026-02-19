"""Run full pipeline: init DB, load data, compute zone metrics. Optional: path to trip file."""
import os
import subprocess
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DB_PATH = os.path.join(DATA_DIR, "insurtech_taxi.db")
SAMPLE = os.path.join(DATA_DIR, "taxi_trips_sample.tsv")
DEFAULT_FILE = os.path.join(DATA_DIR, "taxi_trips.csv")


def run(script_name, *args):
    cmd = [sys.executable, os.path.join(PROJECT_ROOT, "scripts", script_name)] + list(args)
    subprocess.check_call(cmd, cwd=PROJECT_ROOT)


def main():
    trip_file = sys.argv[1] if len(sys.argv) > 1 else None
    if not trip_file:
        if os.path.isfile(SAMPLE):
            trip_file = SAMPLE
        elif os.path.isfile(DEFAULT_FILE):
            trip_file = DEFAULT_FILE
        else:
            print("Usage: python run_pipeline.py [path/to/taxi_trips.csv|.tsv]")
            print("Or place taxi_trips.csv or taxi_trips_sample.tsv in data/")
            sys.exit(1)

    os.makedirs(DATA_DIR, exist_ok=True)
    run("init_db.py")
    run("load_taxi_data.py", trip_file)
    run("compute_zone_metrics.py")
    print("Pipeline done. Query risk by hour: python scripts/query_risk_by_hour.py 0")


if __name__ == "__main__":
    main()
