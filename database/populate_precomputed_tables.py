# Crunches the trip data into ready-made tables so the API can respond immmediately

import sys
import os

# Figure out where this file is so we can find other project files
DATABASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DATABASE_DIR)
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'api'))

from database_config import get_connection
import mysql.connector


def populate_zone_hourly_metrics(conn):
    # Count trips per zone per hour, then figure out exposure, duration, congestion, volatility, and risk
    cursor = conn.cursor()
    
    print("\nComputing zone_hourly_metrics (the main table)...")
    
    try:
        # Make the table if it doesn't exist, then empty it out
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS zone_hourly_metrics (
                zone_id INT NOT NULL,
                hour INT NOT NULL CHECK (hour >= 0 AND hour <= 23),
                trip_count INT DEFAULT 0,
                exposure_index DECIMAL(10, 2),
                avg_trip_duration DECIMAL(10, 2),
                congestion_index DECIMAL(10, 2),
                revenue_volatility DECIMAL(10, 2),
                risk_score DECIMAL(10, 2),
                zone_name VARCHAR(100),
                PRIMARY KEY (zone_id, hour),
                FOREIGN KEY (zone_id) REFERENCES Zone(zone_id) ON DELETE CASCADE,
                INDEX idx_hour (hour),
                INDEX idx_zone (zone_id),
                INDEX idx_risk (risk_score)
            ) ENGINE=InnoDB;
        """)
        
        cursor.execute("TRUNCATE TABLE zone_hourly_metrics;")
        
        # Count trips per zone per hour and calculate how busy each zone is (exposure)
        # Exposure = this zone's trips divided by the busiest zone's trips, times 100
        query_density_exposure = """
        INSERT INTO zone_hourly_metrics (zone_id, hour, trip_count, exposure_index, zone_name)
        WITH hourly_counts AS (
            SELECT
                z.zone_id,
                HOUR(t.pickup_time) AS hour,
                COUNT(*) AS trip_count,
                z.zone_name
            FROM Trip t
            JOIN Location l ON t.pickup_location_id = l.loc_id
            JOIN Zone z ON l.zone_id = z.zone_id
            GROUP BY z.zone_id, HOUR(t.pickup_time), z.zone_name
        ),
        hourly_max AS (
            SELECT
                hour,
                MAX(trip_count) AS max_count
            FROM hourly_counts
            GROUP BY hour
        )
        SELECT
            hc.zone_id,
            hc.hour,
            hc.trip_count,
            ROUND((hc.trip_count / hm.max_count) * 100, 2) AS exposure_index,
            hc.zone_name
        FROM hourly_counts hc
        JOIN hourly_max hm ON hc.hour = hm.hour
        ORDER BY hc.zone_id, hc.hour;
        """
        # Quick check: how many zone-hour groups do we have?
        try:
            cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT z.zone_id, HOUR(t.pickup_time) AS hour
                FROM Trip t
                JOIN Location l ON t.pickup_location_id = l.loc_id
                JOIN Zone z ON l.zone_id = z.zone_id
                GROUP BY z.zone_id, HOUR(t.pickup_time)
            ) AS groups;
            """)
            groups_cnt = cursor.fetchone()[0]
            print(f"   • Found {groups_cnt} non-empty (zone,hour) groups before insert")

            cursor.execute("SELECT HOUR(pickup_time) AS hour, COUNT(*) AS trips FROM Trip GROUP BY hour ORDER BY hour;")
            hour_dist = cursor.fetchall()
            print("   • Trip distribution by HOUR(pickup_time):")
            for hr, cnt in hour_dist:
                print(f"     - hour {hr}: {cnt} trips")
        except Exception:
            # Not a big deal if this check fails, keep going
            pass

        cursor.execute(query_density_exposure)
        conn.commit()
        print(f"Done: Trip counts and exposure ({cursor.rowcount} records)")
        
    except mysql.connector.Error as e:
        print(f"Error in density/exposure: {e}")
        conn.rollback()
    finally:
        cursor.close()


def compute_trip_duration(conn):
    # Work out how long each trip took on average, per zone per hour
    cursor = conn.cursor()
    
    print("\nWorking out average trip times...")
    
    try:
        query = """
        UPDATE zone_hourly_metrics zhm
        SET avg_trip_duration = (
            SELECT ROUND(AVG(TIMESTAMPDIFF(MINUTE, t.pickup_time, t.dropoff_time)), 2)
            FROM Trip t
            JOIN Location l ON t.pickup_location_id = l.loc_id
            WHERE l.zone_id = zhm.zone_id
            AND HOUR(t.pickup_time) = zhm.hour
        )
        WHERE zhm.trip_count > 0;
        """
        
        cursor.execute(query)
        conn.commit()
        print(f"Done: Trip durations ({cursor.rowcount} records)")
        
    except mysql.connector.Error as e:
        print(f"Error computing duration: {e}")
        conn.rollback()
    finally:
        cursor.close()


def compute_congestion_index(conn):
    # Measures how jammed up a zone is (long trips + busy zone = bad traffic)
    cursor = conn.cursor()
    
    print("\nWorking out congestion levels...")
    
    try:
        query = """
        UPDATE zone_hourly_metrics
        SET congestion_index = ROUND(
            (COALESCE(avg_trip_duration, 0) * COALESCE(exposure_index, 0)) / 100,
            2
        )
        WHERE congestion_index IS NULL;
        """
        
        cursor.execute(query)
        conn.commit()
        print(f"Done: Congestion levels ({cursor.rowcount} records)")
        
    except mysql.connector.Error as e:
        print(f"Error computing congestion: {e}")
        conn.rollback()
    finally:
        cursor.close()


def compute_revenue_volatility(conn):
    # Check how much fares jump around per zone (big swings = unpredictable earnings)
    cursor = conn.cursor()
    
    print("\nChecking fare swings per zone...")
    
    try:
        query = """
        UPDATE zone_hourly_metrics zhm
        SET revenue_volatility = (
            SELECT ROUND(COALESCE(STDDEV_POP(t.fare_amount), 0), 2)
            FROM Trip t
            JOIN Location l ON t.pickup_location_id = l.loc_id
            WHERE l.zone_id = zhm.zone_id
        )
        WHERE revenue_volatility IS NULL;
        """
        
        cursor.execute(query)
        conn.commit()
        print(f"Done: Fare volatility ({cursor.rowcount} records)")
        
    except mysql.connector.Error as e:
        print(f"Error computing volatility: {e}")
        conn.rollback()
    finally:
        cursor.close()


def compute_risk_score(conn):
    # Mix everything together into one risk score per zone per hour
    # 40% exposure + 30% congestion + 30% fare swings, all scaled 0 to 100
    cursor = conn.cursor()
    
    print("\nCalculating risk scores...")
    
    try:
        # Find the biggest values so we can scale everything to 0-100
        query_max = """
        SELECT
            COALESCE(MAX(congestion_index), 1) as max_congestion,
            COALESCE(MAX(revenue_volatility), 1) as max_volatility
        FROM zone_hourly_metrics
        WHERE congestion_index > 0 OR revenue_volatility > 0;
        """
        
        cursor.execute(query_max)
        result = cursor.fetchone()
        max_congestion = result[0] if result[0] else 1
        max_volatility = result[1] if result[1] else 1
        
        # Now combine the three parts into one risk number
        query_risk = f"""
        UPDATE zone_hourly_metrics
        SET risk_score = ROUND(
            (0.4 * COALESCE(exposure_index, 0)) +
            (0.3 * (COALESCE(congestion_index, 0) / {max_congestion} * 100)) +
            (0.3 * (COALESCE(revenue_volatility, 0) / {max_volatility} * 100)),
            2
        )
        WHERE risk_score IS NULL;
        """
        
        cursor.execute(query_risk)
        conn.commit()
        print(f"Done: Risk scores ({cursor.rowcount} records)")
        print(f"   Normalization factors: congestion_max={max_congestion}, volatility_max={max_volatility}")
        
    except mysql.connector.Error as e:
        print(f"Error computing risk: {e}")
        conn.rollback()
    finally:
        cursor.close()


def populate_zone_hourly_risk(conn):
    # Copy risk data into a separate table used by the top zones API
    cursor = conn.cursor()
    
    print("\nFilling zone_hourly_risk table...")
    
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS zone_hourly_risk (
                zone_id INT NOT NULL,
                hour INT NOT NULL CHECK (hour >= 0 AND hour <= 23),
                risk_score DECIMAL(10, 2),
                trip_count INT DEFAULT 0,
                zone_name VARCHAR(100),
                PRIMARY KEY (zone_id, hour),
                FOREIGN KEY (zone_id) REFERENCES Zone(zone_id) ON DELETE CASCADE,
                INDEX idx_hour (hour)
            ) ENGINE=InnoDB;
        """)

        cursor.execute("TRUNCATE TABLE zone_hourly_risk;")
        
        query = """
        INSERT INTO zone_hourly_risk (zone_id, hour, risk_score, trip_count, zone_name)
        SELECT zone_id, hour, risk_score, trip_count, zone_name
        FROM zone_hourly_metrics
        ORDER BY zone_id, hour;
        """
        
        cursor.execute(query)
        conn.commit()
        print(f"Done: zone_hourly_risk ({cursor.rowcount} rows)")
        
    except mysql.connector.Error as e:
        print(f"Error populating zone_hourly_risk: {e}")
        conn.rollback()
    finally:
        cursor.close()


def populate_overview_metrics(conn):
    # Calculate the big picture numbers for the dashboard overview
    cursor = conn.cursor(dictionary=True)
    
    print("\nFilling overview_metrics table...")
    
    try:
        # Clear old data
        cursor.execute("DELETE FROM overview_metrics WHERE id = 1;")
        
        # How many trips total
        cursor.execute("SELECT COUNT(*) as cnt FROM Trip;")
        total_trips = cursor.fetchone()['cnt']
        
        # How many zones have a risk score of 50 or higher
        cursor.execute("""
            SELECT COUNT(DISTINCT zone_id) as cnt
            FROM zone_hourly_risk
            WHERE risk_score >= 50;
        """)
        high_risk_zones = cursor.fetchone()['cnt'] or 0
        
        # Which hour has the most trips
        cursor.execute("""
            SELECT hour FROM zone_hourly_risk
            ORDER BY trip_count DESC
            LIMIT 1;
        """)
        peak_result = cursor.fetchone()
        peak_exposure_hour = peak_result['hour'] if peak_result else 0
        
        # How much do fares bounce around on average
        cursor.execute("SELECT ROUND(AVG(revenue_volatility), 2) as avg_rv FROM zone_hourly_metrics WHERE revenue_volatility > 0;")
        rv_result = cursor.fetchone()
        avg_revenue_volatility = rv_result['avg_rv'] if rv_result and rv_result['avg_rv'] else 0
        
        # Save it all in one row
        cursor.execute("""
            INSERT INTO overview_metrics 
            (id, total_trips, high_risk_zones, peak_exposure_hour, avg_revenue_volatility)
            VALUES (%s, %s, %s, %s, %s)
        """, (1, total_trips, high_risk_zones, peak_exposure_hour, avg_revenue_volatility))
        
        conn.commit()
        print(f"Done: overview_metrics:")
        print(f"   - Total trips: {total_trips}")
        print(f"   - High risk zones: {high_risk_zones}")
        print(f"   - Peak hour: {peak_exposure_hour}:00")
        
    except mysql.connector.Error as e:
        print(f"Error populating overview_metrics: {e}")
        conn.rollback()
    finally:
        cursor.close()


def populate_zone_hourly_details(conn):
    # Build the detailed breakdown table used when you click on a zone
    cursor = conn.cursor()
    
    print("\nFilling zone_hourly_details table...")
    
    try:
        cursor.execute("TRUNCATE TABLE zone_hourly_details;")
        
        query = """
        INSERT INTO zone_hourly_details 
        (zone_id, hour, zone_name, trip_count, avg_trip_duration, 
         exposure_index, revenue_volatility, stability_score, risk_score)
        SELECT 
            zhm.zone_id,
            zhm.hour,
            zhm.zone_name,
            zhm.trip_count,
            zhm.avg_trip_duration,
            zhm.exposure_index,
            zhm.revenue_volatility,
            ROUND(100 - COALESCE(zhm.risk_score, 0), 2) as stability_score,
            zhm.risk_score
        FROM zone_hourly_metrics zhm
        ORDER BY zhm.zone_id, zhm.hour;
        """
        
        cursor.execute(query)
        conn.commit()
        print(f"Done: zone_hourly_details ({cursor.rowcount} rows)")
        
    except mysql.connector.Error as e:
        print(f"Error populating zone_hourly_details: {e}")
        conn.rollback()
    finally:
        cursor.close()


def fill_missing_hours(conn):
    # Some zones don't have data for every hour, so fill the gaps with zeros
    cursor = conn.cursor()
    
    print("\nFilling in missing hours (0 through 23) for every zone...")
    
    try:
        # Get every zone we have
        cursor.execute("SELECT DISTINCT zone_id, zone_name FROM Zone;")
        zones = cursor.fetchall()
        
        for zone_id, zone_name in zones:
            # Add hours 0-23 for any that are missing in the main metrics table
            query = """
            INSERT INTO zone_hourly_metrics 
            (zone_id, hour, trip_count, exposure_index, avg_trip_duration, 
             congestion_index, revenue_volatility, risk_score, zone_name)
            SELECT %s, h.hour, 0, 0, 0, 0, 0, 0, %s
            FROM (
                SELECT 0 as hour UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 
                UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 
                UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11
                UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15
                UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19
                UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23
            ) h
            WHERE NOT EXISTS (
                SELECT 1 FROM zone_hourly_metrics 
                WHERE zone_id = %s AND hour = h.hour
            )
            ON DUPLICATE KEY UPDATE trip_count = trip_count;
            """
            
            cursor.execute(query, (zone_id, zone_name, zone_id))
        
        conn.commit()
        print(f"Done: Filled missing hours in zone_hourly_metrics")
        
        # Fill missing hours in zone_hourly_risk
        query_risk = """
        INSERT INTO zone_hourly_risk (zone_id, hour, risk_score, trip_count, zone_name)
        SELECT z.zone_id, z.hour, 0, 0, z.zone_name
        FROM zone_hourly_metrics z
        WHERE NOT EXISTS (
            SELECT 1 FROM zone_hourly_risk 
            WHERE zone_id = z.zone_id AND hour = z.hour
        );
        """
        
        cursor.execute(query_risk)
        conn.commit()
        print(f"Done: Filled missing hours in zone_hourly_risk")
        
        # Do the same for the details table
        query_details = """
        INSERT INTO zone_hourly_details 
        (zone_id, hour, zone_name, trip_count, avg_trip_duration, 
         exposure_index, revenue_volatility, stability_score, risk_score)
        SELECT z.zone_id, z.hour, z.zone_name, 0, 0, 0, 0, 100, 0
        FROM zone_hourly_metrics z
        WHERE NOT EXISTS (
            SELECT 1 FROM zone_hourly_details 
            WHERE zone_id = z.zone_id AND hour = z.hour
        );
        """

        cursor.execute(query_details)
        conn.commit()
        print(f"Done: Filled missing hours in zone_hourly_details")
        
    except mysql.connector.Error as e:
        print(f"Error filling missing hours: {e}")
        conn.rollback()
    finally:
        cursor.close()


def main():
    print("Insurtech - Building precomputed tables")
    print()
    
    conn = get_connection()
    if not conn:
        print("Could not connect to database")
        sys.exit(1)
    
    try:
        # Build the main metrics table first
        populate_zone_hourly_metrics(conn)
        
        # Then add each metric one by one
        compute_trip_duration(conn)
        compute_congestion_index(conn)
        compute_revenue_volatility(conn)
        compute_risk_score(conn)
        
        # Copy data into the tables each API endpoint reads from
        populate_zone_hourly_risk(conn)
        populate_overview_metrics(conn)
        populate_zone_hourly_details(conn)
        
        # Make sure every zone has all 24 hours filled in
        fill_missing_hours(conn)
        
        print("\nAll done! Precomputed tables are ready.")
        print("The API endpoints will now load instantly.")
        
    except Exception as e:
        print(f"\nSomething went wrong: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()