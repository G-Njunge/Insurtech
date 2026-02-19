"""
Database Loader for Zones and Zone Metrics
Loads data into the database following the ERD schema.
Uses SQLite by default. Requires schema to be created first via schema.sql
"""

import sqlite3
import os
from pathlib import Path


# Default database file path
DEFAULT_DB_PATH = Path(__file__).parent / "zones_metrics.db"


def get_connection(db_path: str | Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Create and return a database connection."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Enable dict-like access to rows
    return conn


def init_schema(conn: sqlite3.Connection, schema_path: str | Path | None = None) -> None:
    """
    Initialize database schema by executing schema.sql.
    """
    if schema_path is None:
        schema_path = Path(__file__).parent / "schema.sql"

    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    conn.executescript(schema_sql)
    conn.commit()
    print("Schema initialized successfully.")


def load_zones(conn: sqlite3.Connection, zones: list[tuple]) -> None:
    """
    Load zones into the zones table.
    Each tuple: (zone_id, borough, zone_name, service_zone)
    """
    conn.executemany(
        """
        INSERT OR REPLACE INTO zones (zone_id, borough, zone_name, service_zone)
        VALUES (?, ?, ?, ?)
        """,
        zones,
    )
    conn.commit()
    print(f"Loaded {len(zones)} zones.")


def load_zone_hour_metrics(
    conn: sqlite3.Connection,
    metrics: list[tuple],
) -> None:
    """
    Load hourly metrics into zone_hour_metrics.
    Each tuple: (zone_id, hour, trip_count, average_trip_duration, exposure_score)
    """
    conn.executemany(
        """
        INSERT INTO zone_hour_metrics (zone_id, hour, trip_count, average_trip_duration, exposure_score)
        VALUES (?, ?, ?, ?, ?)
        """,
        metrics,
    )
    conn.commit()
    print(f"Loaded {len(metrics)} zone hour metrics.")


def load_zone_risk_scores(conn: sqlite3.Connection, scores: list[tuple]) -> None:
    """
    Load risk scores into zone_risk_scores.
    Each tuple: (zone_id, hour, risk_score)
    """
    conn.executemany(
        """
        INSERT OR REPLACE INTO zone_risk_scores (zone_id, hour, risk_score)
        VALUES (?, ?, ?)
        """,
        scores,
    )
    conn.commit()
    print(f"Loaded {len(scores)} zone risk scores.")


def load_zone_revenue_metrics(conn: sqlite3.Connection, metrics: list[tuple]) -> None:
    """
    Load revenue metrics into zone_revenue_metrics.
    Each tuple: (zone_id, average_revenue, revenue_volatility, stability_score)
    """
    conn.executemany(
        """
        INSERT OR REPLACE INTO zone_revenue_metrics (zone_id, average_revenue, revenue_volatility, stability_score)
        VALUES (?, ?, ?, ?)
        """,
        metrics,
    )
    conn.commit()
    print(f"Loaded {len(metrics)} zone revenue metrics.")


def load_from_csv(
    conn: sqlite3.Connection,
    zones_csv: str | Path | None = None,
    hour_metrics_csv: str | Path | None = None,
    risk_scores_csv: str | Path | None = None,
    revenue_metrics_csv: str | Path | None = None,
) -> None:
    """
    Load data from CSV files. Expects headers in first row.
    CSV columns must match the expected order for each table.
    """
    import csv

    base_path = Path(__file__).parent

    if zones_csv:
        path = Path(zones_csv) if not isinstance(zones_csv, Path) else zones_csv
        if not path.is_absolute():
            path = base_path / path
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            zones = [
                (int(r[0]), r[1], r[2], r[3])
                for r in reader
                if len(r) >= 4
            ]
            load_zones(conn, zones)

    if hour_metrics_csv:
        path = Path(hour_metrics_csv) if not isinstance(hour_metrics_csv, Path) else hour_metrics_csv
        if not path.is_absolute():
            path = base_path / path
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            metrics = [
                (int(r[0]), int(r[1]), int(r[2]), float(r[3]) if r[3] else None, float(r[4]) if r[4] else None)
                for r in reader
                if len(r) >= 5
            ]
            load_zone_hour_metrics(conn, metrics)

    if risk_scores_csv:
        path = Path(risk_scores_csv) if not isinstance(risk_scores_csv, Path) else risk_scores_csv
        if not path.is_absolute():
            path = base_path / path
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            scores = [
                (int(r[0]), int(r[1]), float(r[2]) if r[2] else None)
                for r in reader
                if len(r) >= 3
            ]
            load_zone_risk_scores(conn, scores)

    if revenue_metrics_csv:
        path = Path(revenue_metrics_csv) if not isinstance(revenue_metrics_csv, Path) else revenue_metrics_csv
        if not path.is_absolute():
            path = base_path / path
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)
            metrics = [
                (int(r[0]), float(r[1]) if r[1] else None, float(r[2]) if r[2] else None, float(r[3]) if r[3] else None)
                for r in reader
                if len(r) >= 4
            ]
            load_zone_revenue_metrics(conn, metrics)


def load_sample_data(conn: sqlite3.Connection) -> None:
    """Load sample data for testing the schema."""
    zones = [
        (1, "Manhattan", "East Harlem North", "Boro Zone"),
        (2, "Manhattan", "East Harlem South", "Boro Zone"),
        (3, "Brooklyn", "Bushwick North", "Boro Zone"),
    ]
    load_zones(conn, zones)

    hour_metrics = [
        (1, 9, 150, 12.5, 0.72),
        (1, 10, 180, 11.2, 0.68),
        (2, 9, 95, 14.1, 0.55),
        (3, 18, 200, 10.3, 0.81),
    ]
    load_zone_hour_metrics(conn, hour_metrics)

    risk_scores = [
        (1, 9, 0.65),
        (1, 10, 0.62),
        (2, 9, 0.48),
        (3, 18, 0.73),
    ]
    load_zone_risk_scores(conn, risk_scores)

    revenue_metrics = [
        (1, 1250.50, 0.15, 0.85),
        (2, 980.30, 0.22, 0.72),
        (3, 1100.00, 0.18, 0.78),
    ]
    load_zone_revenue_metrics(conn, revenue_metrics)


def main(
    db_path: str | Path = DEFAULT_DB_PATH,
    init_schema_flag: bool = True,
    use_sample_data: bool = True,
    schema_path: str | Path | None = None,
) -> None:
    """
    Main entry point: initialize schema and load data.
    """
    conn = get_connection(db_path)

    try:
        if init_schema_flag:
            init_schema(conn, schema_path)

        if use_sample_data:
            load_sample_data(conn)

        print("Database setup complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load zones and metrics into the database")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Database file path")
    parser.add_argument("--no-init", action="store_true", help="Skip schema initialization")
    parser.add_argument("--no-sample", action="store_true", help="Skip loading sample data")
    args = parser.parse_args()

    main(
        db_path=args.db,
        init_schema_flag=not args.no_init,
        use_sample_data=not args.no_sample,
    )
