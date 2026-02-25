import os
import sys
import mysql.connector

# Figure out where this file is so we can find other project files
DSA_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DSA_DIR)
if DSA_DIR not in sys.path:
    sys.path.append(DSA_DIR)

# Connect to the MySQL database and return the connection
def get_connection():
    try:
        conn = mysql.connector.connect(
            host="127.0.0.1",
            user="trials_user",
            password="trials_pass",
            database="nyc_taxi_temp",
            auth_plugin="mysql_native_password"
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None



