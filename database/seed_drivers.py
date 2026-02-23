# Creates drivers from the trip data and builds their work history
import sys, os

# Figure out where this file is so we can find other project files
DATABASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DATABASE_DIR)
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'dsa'))

from database_config import get_connection


# List of first and last names we pick from to name our drivers
FIRST_NAMES = [
    "Alice", "Brian", "Catherine", "Daniel", "Eve", "Faith", "George",
    "Hannah", "Isaac", "Joyce", "Kevin", "Lilian", "Martin", "Nancy",
    "Oscar", "Patricia", "Quentin", "Rose", "Samuel", "Teresa",
    "Uriel", "Violet", "Wesley", "Xena", "Yusuf", "Zena",
    "Amani", "Baraka", "Charity", "Dennis", "Esther", "Felix",
    "Grace", "Henry", "Irene", "James", "Kendi", "Lydia",
    "Moses", "Nadia", "Oliver", "Pauline", "Rashid", "Sarah",
    "Thomas", "Uma", "Victor", "Wambui", "Xavier", "Zuri",
]
LAST_NAMES = [
    "Mwangi", "Ochieng", "Njeri", "Kimani", "Wanjiku", "Akinyi",
    "Mutua", "Chebet", "Wanyama", "Nyambura", "Otieno", "Kamau",
    "Kipchoge", "Maina", "Odhiambo", "Wairimu", "Kibet", "Mugo",
    "Ndungu", "Korir", "Wafula", "Karanja", "Mogaka", "Rotich",
    "Njenga", "Onyango", "Kiptoo", "Mwenda", "Gitau", "Cheruiyot",
    "Njoroge", "Sang", "Biwott", "Langat", "Ouma", "Wekesa",
    "Simiyu", "Ngetich", "Tanui", "Kemboi", "Rono", "Lagat",
    "Cherono", "Kigen", "Barno", "Koech", "Yego", "Chepng'etich",
    "Limo", "Tarus",
]


def seed():
    conn = get_connection()
    if not conn:
        print("DB connection failed")
        return
    cur = conn.cursor()

    try:
        # Start fresh by deleting old tables and making new ones
        cur.execute("DROP TABLE IF EXISTS driver_operations;")
        cur.execute("DROP TABLE IF EXISTS user;")
        conn.commit()

        cur.execute("""
            CREATE TABLE user (
                user_id INT PRIMARY KEY,
                user_name VARCHAR(100) NOT NULL
            ) ENGINE=InnoDB;
        """)

        cur.execute("""
            CREATE TABLE driver_operations (
                id INT AUTO_INCREMENT PRIMARY KEY,
                driver_id INT NOT NULL,
                zone_id INT NOT NULL,
                hour INT NOT NULL,
                trips_in_period INT DEFAULT 0,
                avg_risk_in_zone DECIMAL(10,4) DEFAULT 0,
                FOREIGN KEY (driver_id) REFERENCES user(user_id) ON DELETE CASCADE,
                INDEX idx_driver (driver_id)
            ) ENGINE=InnoDB;
        """)
        conn.commit()

        # Find every unique vendor + pickup spot combo to treat as a separate driver
        cur.execute("""
            SELECT DISTINCT t.vendor_id, t.pickup_location_id
            FROM Trip t
            WHERE t.vendor_id IS NOT NULL
              AND t.pickup_location_id IS NOT NULL
            ORDER BY t.vendor_id, t.pickup_location_id;
        """)
        combos = cur.fetchall()
        print(f"  Found {len(combos)} unique (vendor, pickup_location) combos")

        # Give each combo a driver number and a random name
        combo_to_driver = {}
        users = []
        for idx, (vid, ploc) in enumerate(combos):
            driver_id = idx + 1
            first = FIRST_NAMES[idx % len(FIRST_NAMES)]
            last = LAST_NAMES[idx % len(LAST_NAMES)]
            name = f"{first} {last}"
            combo_to_driver[(vid, ploc)] = driver_id
            users.append((driver_id, name))

        cur.executemany("INSERT INTO user (user_id, user_name) VALUES (%s, %s);", users)
        conn.commit()
        print(f"  Inserted {len(users)} drivers")

        # Build the work history for each driver
        # Group their trips by zone and hour so we know where and when they drive
        cur.execute("""
            SELECT
                t.vendor_id,
                t.pickup_location_id,
                l.zone_id,
                HOUR(t.pickup_time)            AS hour,
                COUNT(*)                       AS trips_in_period,
                COALESCE(MAX(zhm.risk_score) / 100.0, 0) AS avg_risk
            FROM Trip t
            JOIN Location l ON t.pickup_location_id = l.loc_id
            LEFT JOIN zone_hourly_metrics zhm
                ON l.zone_id = zhm.zone_id
               AND HOUR(t.pickup_time) = zhm.hour
            WHERE l.zone_id IS NOT NULL
              AND t.vendor_id IS NOT NULL
            GROUP BY t.vendor_id, t.pickup_location_id, l.zone_id, HOUR(t.pickup_time)
            ORDER BY t.vendor_id, t.pickup_location_id, l.zone_id, HOUR(t.pickup_time);
        """)
        rows = cur.fetchall()

        ops = []
        for r in rows:
            vid, ploc = int(r[0]), int(r[1])
            driver_id = combo_to_driver.get((vid, ploc))
            if driver_id is None:
                continue
            zone_id = int(r[2])
            hour = int(r[3])
            trips = int(r[4])
            avg_risk = float(r[5])
            ops.append((driver_id, zone_id, hour, trips, avg_risk))

        cur.executemany("""
            INSERT INTO driver_operations (driver_id, zone_id, hour, trips_in_period, avg_risk_in_zone)
            VALUES (%s, %s, %s, %s, %s);
        """, ops)
        conn.commit()

        total_trips = sum(o[3] for o in ops)
        print(f"  Inserted {len(ops)} driver_operations records from {total_trips} trips")

        # Show a quick summary of some drivers
        show_ids = [u[0] for u in users[:10]] + [u[0] for u in users[-5:]]
        for did, name in [(uid, un) for uid, un in users if uid in show_ids]:
            cur.execute("""
                SELECT COUNT(*), SUM(trips_in_period), ROUND(AVG(avg_risk_in_zone),4)
                FROM driver_operations WHERE driver_id=%s;
            """, (did,))
            cnt, trips, avg_r = cur.fetchone()
            cnt = cnt or 0
            trips = trips or 0
            avg_r = avg_r or 0
            print(f"   Driver {did:3d} ({name:20s}): {int(cnt):3d} combos, {int(trips):4d} trips, avg_risk={float(avg_r):.4f}")

        if len(users) > 15:
            print(f"   ... and {len(users) - 15} more drivers")

        print(f"\nSeed complete. {len(ops)} records across {len(users)} drivers (IDs 1-{len(users)}).")

    except Exception as e:
        print("Error:", e)
        import traceback; traceback.print_exc()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    seed()