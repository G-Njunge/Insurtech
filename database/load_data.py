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


