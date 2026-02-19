"""Configuration for DB and data paths."""
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(BASE_DIR, "data", "insurtech_taxi.db")

# Default input file (CSV or TSV with header)
DEFAULT_TRIP_FILE = os.path.join(DATA_DIR, "taxi_trips.csv")
