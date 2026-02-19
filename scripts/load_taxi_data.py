"""Load taxi trip CSV/TSV into taxi_trips table."""
import os
import sqlite3
import csv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
DB_PATH = os.path.join(DATA_DIR, "insurtech_taxi.db")
DEFAULT_FILE = os.path.join(DATA_DIR, "taxi_trips.csv")

# Map file header names to DB columns (file can have VendorID or vendor_id etc.)
COLUMN_MAP = {
    "VendorID": "vendor_id",
    "vendor_id": "vendor_id",
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "ratecode_id",
    "ratecode_id": "ratecode_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pulocation_id",
    "pulocation_id": "pulocation_id",
    "DOLocationID": "dolocation_id",
    "dolocation_id": "dolocation_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
}

DB_COLUMNS = [
    "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "ratecode_id", "store_and_fwd_flag",
    "pulocation_id", "dolocation_id", "payment_type", "fare_amount", "extra",
    "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
    "total_amount", "congestion_surcharge",
]


def _float_or_none(s):
    if s is None or (isinstance(s, str) and s.strip() == ""):
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _int_or_none(s):
    if s is None or (isinstance(s, str) and s.strip() == ""):
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def _str_or_none(s):
    if s is None:
        return None
    t = str(s).strip()
    return t if t else None


def detect_delimiter(filepath):
    with open(filepath, "r", encoding="utf-8", newline="") as f:
        sample = f.read(4096)
    if "\t" in sample and sample.count("\t") > sample.count(","):
        return "\t"
    return ","


def load_file(filepath: str = None, clear_first: bool = True):
    filepath = filepath or DEFAULT_FILE
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"Trip file not found: {filepath}")

    delim = detect_delimiter(filepath)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    if clear_first:
        cur.execute("DELETE FROM taxi_trips")
        conn.commit()

    placeholders = ",".join(["?" for _ in DB_COLUMNS])
    insert_sql = f"INSERT INTO taxi_trips ({','.join(DB_COLUMNS)}) VALUES ({placeholders})"

    with open(filepath, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        file_headers = reader.fieldnames or []
        # For each DB column, which file header to use (first that maps to it)
        header_to_db = {h.strip(): COLUMN_MAP.get(h.strip(), h.strip()) for h in file_headers}
        db_to_header = {}
        for fh, db_col in header_to_db.items():
            if db_col not in db_to_header:
                db_to_header[db_col] = fh

        rows = []
        for row in reader:
            values = []
            for db_col in DB_COLUMNS:
                fh = db_to_header.get(db_col)
                val = row.get(fh) if fh else None

                if db_col in ("fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
                              "improvement_surcharge", "total_amount", "congestion_surcharge", "trip_distance"):
                    values.append(_float_or_none(val))
                elif db_col in ("vendor_id", "passenger_count", "ratecode_id", "pulocation_id", "dolocation_id", "payment_type"):
                    values.append(_int_or_none(val))
                else:
                    values.append(_str_or_none(val))

            if values[7] is not None:  # pulocation_id required
                rows.append(tuple(values))

            if len(rows) >= 5000:
                cur.executemany(insert_sql, rows)
                conn.commit()
                rows = []

        if rows:
            cur.executemany(insert_sql, rows)
            conn.commit()

    count = cur.execute("SELECT COUNT(*) FROM taxi_trips").fetchone()[0]
    conn.close()
    print(f"Loaded {count} rows from {filepath}")
    return count


if __name__ == "__main__":
    import sys
    path = sys.argv[1] if len(sys.argv) > 1 else None
    load_file(path)
