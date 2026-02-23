# Reads CSV files from the data folder and loads them into the database

import sys
import os
import csv

# Figure out where this file is so we can find other project files
DATABASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DATABASE_DIR)
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'dsa'))
from database_config import get_connection

LOCATION_CSV = os.path.join(PROJECT_ROOT, 'data', 'locations.csv')
TRIP_CSV = os.path.join(PROJECT_ROOT, 'data', 'cleaned_yellow_trips.csv')


def create_tables(conn):
    # Wipe the old tables and make fresh ones
    cur = conn.cursor()

    cur.execute("SET FOREIGN_KEY_CHECKS=0")
    for t in ('trip', 'location', 'zone'):
        cur.execute(f"DROP TABLE IF EXISTS `{t}`")
    cur.execute("SET FOREIGN_KEY_CHECKS=1")
    conn.commit()

    cur.execute("""
        CREATE TABLE zone (
            zone_id       INT          NOT NULL AUTO_INCREMENT,
            zone_name     VARCHAR(100) NOT NULL UNIQUE,
            borough       VARCHAR(50)  DEFAULT NULL,
            service_zone  VARCHAR(50)  DEFAULT NULL,
            PRIMARY KEY (zone_id)
        ) ENGINE=InnoDB;
    """)

 cur.execute("""
        CREATE TABLE location (
            loc_id        INT          NOT NULL,
            borough       VARCHAR(50)  DEFAULT NULL,
            zone_name     VARCHAR(100) DEFAULT NULL,
            service_zone  VARCHAR(50)  DEFAULT NULL,
            zone_id       INT          DEFAULT NULL,
            PRIMARY KEY (loc_id)
        ) ENGINE=InnoDB;
    """)

cur.execute("""
        CREATE TABLE trip (
            trip_id              INT           NOT NULL AUTO_INCREMENT,
            vendor_id            INT           DEFAULT NULL,
            pickup_time          DATETIME      NOT NULL,
            dropoff_time         DATETIME      NOT NULL,
            passenger_count      INT           DEFAULT NULL,
            trip_distance        DECIMAL(10,2) DEFAULT NULL,
            pickup_location_id   INT           DEFAULT NULL,
            dropoff_location_id  INT           DEFAULT NULL,
            fare_amount          DECIMAL(10,2) DEFAULT NULL,
            total_amount         DECIMAL(10,2) DEFAULT NULL,
            PRIMARY KEY (trip_id),
            INDEX idx_pickup_time (pickup_time),
            INDEX idx_pickup_loc  (pickup_location_id),
            INDEX idx_dropoff_loc (dropoff_location_id)
        ) ENGINE=InnoDB;
    """)

   conn.commit()
    cur.close()
    print("[1/3] Tables created (zone, location, trip)")

def load_locations(conn):
    # Read locations.csv and fill the zone and location tables
    cur = conn.cursor()

    with open(LOCATION_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Get each unique zone (no duplicates)
    zones_seen = {}
    for r in rows:
        zname = r['Zone']
        if zname not in zones_seen:
            borough = r['Borough']
            service = r['service_zone']
            zones_seen[zname] = (borough, service)

    for zname, (borough, service) in zones_seen.items():
        cur.execute(
            "INSERT INTO zone (zone_name, borough, service_zone) VALUES (%s, %s, %s)",
            (zname, borough, service)
        )
    conn.commit()

    # Make a quick lookup so we can match zone names to their IDs
    cur.execute("SELECT zone_id, zone_name FROM zone")
    zone_map = {name: zid for zid, name in cur.fetchall()}

    # Now load all the locations
    for r in rows:
        loc_id = int(r['LocationID'])
        borough = r['Borough']
        zone_name = r['Zone']
        service = r['service_zone']
        zone_id = zone_map.get(zone_name)

 cur.execute(
            "INSERT INTO location (loc_id, borough, zone_name, service_zone, zone_id) "
            "VALUES (%s, %s, %s, %s, %s)",
            (loc_id, borough, zone_name, service, zone_id)
        )

    conn.commit()
    cur.close()
    print(f"[2/3] Loaded {len(zones_seen)} zones and {len(rows)} locations from locations.csv")


def load_trips(conn, batch_size=500):
    # Read the trips CSV and load them into the trip table in batches
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO trip
            (vendor_id, pickup_time, dropoff_time, passenger_count, trip_distance,
             pickup_location_id, dropoff_location_id, fare_amount, total_amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

  batch = []
    total = 0
    skipped = 0

    with open(TRIP_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for i, r in enumerate(reader, 1):
            try:
                batch.append((
                    int(r['VendorID']),
                    r['tpep_pickup_datetime'],
                    r['tpep_dropoff_datetime'],
                    int(r['passenger_count']) if r['passenger_count'] else None,
                    float(r['trip_distance']) if r['trip_distance'] else None,
                    int(r['PULocationID']),
                    int(r['DOLocationID']),
                    float(r['fare_amount']) if r['fare_amount'] else None,
                    float(r['total_amount']) if r['total_amount'] else None,
                ))
 except (ValueError, KeyError) as e:
                skipped += 1
                if skipped <= 5:
                    print(f"  Warning: row {i} skipped – {e}")
                continue

            if len(batch) >= batch_size:
                cur.executemany(insert_sql, batch)
                conn.commit()
                total += len(batch)
                batch = []
                print(f"  {total} trips inserted...")

   if batch:
        cur.executemany(insert_sql, batch)
        conn.commit()
        total += len(batch)

    cur.close()
    msg = f"[3/3] Loaded {total} trips from cleaned_yellow_trips.csv"
    if skipped:
        msg += f"  ({skipped} rows skipped)"
    print(msg)


def main():
    print("Insurtech Data Loader")
    print()

    # Make sure the CSV files are there before we start
    for label, path in [('Location CSV', LOCATION_CSV), ('Trip CSV', TRIP_CSV)]:
        if not os.path.exists(path):
            print(f"ERROR: {label} not found at {path}")
            return
        

