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



