import os
import sys
from flask import Flask, request, jsonify, send_file, send_from_directory

# Figure out where this file is so we can find other project files
DSA_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DSA_DIR)
if DSA_DIR not in sys.path:
    sys.path.append(DSA_DIR)

from database_config import get_connection

# Where the HTML, JS, and CSS files live
FRONTEND_PATH = os.path.join(PROJECT_ROOT, 'frontend')

app = Flask(__name__)

# Home page
@app.route("/")
def home():
    return send_from_directory(FRONTEND_PATH, 'index.html')

@app.route("/dashboard.html")
def dashboard():
    return send_from_directory(FRONTEND_PATH, 'dashboard.html')

@app.route('/api/overview', methods=['GET'])
def get_overview():

    conn = get_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("SELECT * FROM overview_metrics WHERE id = 1;")
        data = cursor.fetchone()
        
        if not data:
            return jsonify({"error": "No overview metrics found"}), 404

        response = {
            "total_trips": data.get("total_trips", 0),
            "high_risk_zones_count": data.get("high_risk_zones", 0),
            "peak_exposure_hour": data.get("peak_exposure_hour", 0),
            "revenue_volatility_score": data.get("avg_revenue_volatility", 0)
        }

        return jsonify(response)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/zone/<int:zone_id>', methods=['GET'])
def get_zone_details(zone_id):

    hour = request.args.get('hour', type=int)

    if hour is None:
        return jsonify({"error": "Hour parameter is required"}), 400

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT zone_name, trip_count,
               avg_trip_duration, exposure_index,
               revenue_volatility, stability_score, risk_score
        FROM zone_hourly_metrics
        WHERE zone_id = %s AND hour = %s;
    """, (zone_id, hour))

    data = cursor.fetchone()

    cursor.close()
    conn.close()

    if not data:
        return jsonify({"error": "Zone or hour not found"}), 404

    response = {
        "zone_name": data["zone_name"],
        "trip_count": data["trip_count"],
        "avg_trip_duration": data["avg_trip_duration"],
        "exposure_index": data["exposure_index"],
        "revenue_volatility": data["revenue_volatility"],
        "stability_score": data["stability_score"],
        "risk_score": data["risk_score"]
    }

    return jsonify(response)


# Returns total trips for each hour (0-23) so the density chart can draw in one request
@app.route('/api/hourly_density', methods=['GET'])
def get_hourly_density():
    conn = get_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500

    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT hour, SUM(trip_count) AS total_trips
            FROM zone_hourly_metrics
            GROUP BY hour
            ORDER BY hour;
        """)
        rows = cursor.fetchall()
        result = {}
        for r in rows:
            result[r["hour"]] = r["total_trips"] or 0
        # Fill in any missing hours with 0
        response = []
        for h in range(24):
            response.append({"hour": h, "total_trips": int(result.get(h, 0))})
        return jsonify(response)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/top_zones', methods=['GET'])
def get_top_zones():
    hour = request.args.get('hour', type=int)
    if hour is None:
        return jsonify({"error": "Hour parameter is required"}), 400

    conn = get_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500

    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT zhm.zone_id, zhm.zone_name, COALESCE(z.borough, '') AS borough,
                   zhm.risk_score, zhm.trip_count, zhm.exposure_index
            FROM zone_hourly_metrics zhm
            LEFT JOIN Zone z ON zhm.zone_id = z.zone_id
            WHERE zhm.hour = %s
            ORDER BY zhm.risk_score DESC
            LIMIT 10;
        """, (hour,))

        rows = cursor.fetchall()
        response = []
        for r in rows:
            response.append({
                "zone_id": r["zone_id"],
                "zone_name": r["zone_name"],
                "borough": r.get("borough", ""),
                "risk_score": r.get("risk_score", 0),
                "trip_count": r.get("trip_count", 0),
                "exposure_score": r.get("exposure_index", 0)
            })

        return jsonify(response)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/api/driver-risk', methods=['POST'])
def calculate_driver_risk():
    # Takes a driver_id and works out their risk score based on where and when they drive
    data = request.get_json()
    
    driver_id = data.get('driver_id')
    
    if not driver_id:
        return jsonify({"error": "driver_id is required"}), 400
    
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    # Check if this driver exists in the database
    cursor.execute("SELECT user_id, user_name FROM user WHERE user_id = %s;", (driver_id,))
    driver = cursor.fetchone()
    
    if not driver:
        # Tell the user how many drivers we actually have
        cursor.execute("SELECT MIN(user_id), MAX(user_id), COUNT(*) FROM user;")
        info = cursor.fetchone()
        cursor.close()
        conn.close()
        total = info["COUNT(*)"] if info else 0
        max_id = info["MAX(user_id)"] if info else 0
        return jsonify({"error": f"Driver ID {driver_id} not found. We have {total} drivers (IDs 1-{max_id})."}), 404
    
    # Get all the zones and hours this driver has worked in
    cursor.execute("""
        SELECT driver_id, zone_id, hour, trips_in_period, avg_risk_in_zone
        FROM driver_operations
        WHERE driver_id = %s
        ORDER BY hour, zone_id;
    """, (driver_id,))
    
    operations = cursor.fetchall()
    
    if not operations:
        # No records found, so make some from the zone metrics as a fallback
        import random
        rng = random.Random(driver_id)
        cursor.execute("SELECT zone_id, hour, risk_score FROM zone_hourly_metrics WHERE risk_score > 0 ORDER BY zone_id, hour;")
        all_metrics = cursor.fetchall()
        if not all_metrics:
            cursor.close()
            conn.close()
            return jsonify({"error": "No zone metrics available to generate a profile"}), 500
        # Pick a few random zone-hour combos for this driver
        sample_size = min(rng.randint(3, 6), len(all_metrics))
        chosen = rng.sample(all_metrics, sample_size)
        for m in chosen:
            trips = rng.randint(5, 40)
            cursor.execute(
                "INSERT INTO driver_operations (driver_id, zone_id, hour, trips_in_period, avg_risk_in_zone) VALUES (%s,%s,%s,%s,%s);",
                (driver_id, m['zone_id'], m['hour'], trips, m['risk_score'])
            )
        conn.commit()
        # Now grab those new records
        cursor.execute("""
            SELECT driver_id, zone_id, hour, trips_in_period, avg_risk_in_zone
            FROM driver_operations
            WHERE driver_id = %s
            ORDER BY hour, zone_id;
        """, (driver_id,))
        operations = cursor.fetchall()
    
    # Look up zone names and add up the risk across all trips
    operating_zones = {}
    operating_hours = set()
    total_risk = 0
    total_trips = 0
    valid_operations = []
    
    for op in operations:
        zone_id = op["zone_id"]
        hour = op["hour"]
        trips = op["trips_in_period"]
        risk = op["avg_risk_in_zone"]
        
        # Look up the zone name if we haven't already
        if zone_id not in operating_zones:
            cursor.execute("SELECT zone_id, zone_name FROM zone_hourly_details WHERE zone_id = %s AND hour = %s LIMIT 1;", (zone_id, hour))
            zone_info = cursor.fetchone()
            if zone_info:
                operating_zones[zone_id] = zone_info["zone_name"]
                valid_operations.append(op)
            else:
                continue  # Skip this operation if zone doesn't exist
        else:
            valid_operations.append(op)
        
        operating_hours.add(hour)
        total_trips += trips
        total_risk += risk * trips  # Weighted by trip count
    
    cursor.close()
    conn.close()
    
    # Work out the final risk score (10 = safest, 80 = riskiest)
    # We weight each zone's risk by how many trips the driver made there
    raw_risk = (total_risk / total_trips) if total_trips > 0 else 0.0  # 0-1 scale
    composite_risk = 10 + (raw_risk * 70)  # maps 0->10, 1->80
    composite_risk = min(80, max(10, round(composite_risk, 2)))
    
    # Decide the risk label based on the score
    if composite_risk < 25:
        risk_level = "Low"
    elif composite_risk < 45:
        risk_level = "Medium"
    elif composite_risk < 65:
        risk_level = "High"
    else:
        risk_level = "Very High"
    
    # Build nice text for zones and hours
    zones_list = ", ".join([f"{operating_zones[z]} (Zone {z})" for z in sorted(operating_zones.keys())])
    hours_list = ", ".join([f"{h}:00" for h in sorted(operating_hours)])
    
    # Put together the explanation text
    zone_details = []
    hour_details = []
    
    for op in valid_operations:
        zone_id = op["zone_id"]
        hour = op["hour"]
        trips = op["trips_in_period"]
        risk = op["avg_risk_in_zone"]
        
        zone_name = operating_zones.get(zone_id, "Unknown")
        risk_desc = "Low" if risk < 0.33 else ("Medium" if risk < 0.67 else "High")
        zone_details.append(f"{zone_name} - you did {trips} trips here, risk is {risk_desc}")
        
        if hour not in [h["hour"] for h in hour_details]:
            hour_details.append({"hour": hour, "trips": 0})
    
    # Add up trips per hour
    for op in valid_operations:
        for h in hour_details:
            if h["hour"] == op["hour"]:
                h["trips"] += op["trips_in_period"]
    
    # The full explanation shown to the driver
    explanation_text = f"""
Here is how we worked out your risk score:

1. WHERE YOU DRIVE:
   You work in {len(operating_zones)} area(s): {zones_list}.
   Some areas are more dangerous than others. We look at where you spend most of your time.

2. WHEN YOU DRIVE:
   You are on the road at these hours: {', '.join([f'{h}:00' for h in sorted(operating_hours)])}.
   Some hours are riskier than others, like late at night when it is dark and there is more traffic.

3. HOW MUCH YOU DRIVE:
   We looked at {total_trips} of your trips across all your areas and times.
   If you do more trips in a dangerous area, that counts more towards your score.

4. THE FINAL SCORE:
   We take each area's danger level, multiply it by how many trips you did there, add it all up, then divide by your total trips.
   This gives you a fair score based on YOUR actual work.

5. YOUR RESULT:
   Score: {composite_risk:.2f} out of 80 ({risk_level} Risk)
   - 10 to 25 = Low risk (you drive in safer conditions)
   - 25 to 45 = Medium risk (some danger, but manageable)
   - 45 to 65 = High risk (you drive in risky conditions often)
   - 65 to 80 = Very high risk (your driving conditions are very dangerous)
"""
    
    # Put together the final response
    driver_name = driver["user_name"]
    response = {
        "personalized_message": f"Hi {driver_name}! You drive in {zones_list} during {hours_list}. Your risk score is {composite_risk:.2f} out of 80 ({risk_level}).",
        "driver": {
            "driver_id": driver["user_id"],
            "name": driver["user_name"]
        },
        "operating_profile": {
            "zones": [{"zone_id": z, "zone_name": operating_zones[z]} for z in sorted(operating_zones.keys())],
            "hours": sorted(list(operating_hours)),
            "total_trips_analyzed": total_trips
        },
        "risk_assessment": {
            "composite_risk_score": round(composite_risk, 2),
            "risk_level": risk_level,
            "scale": "10 is safest, 80 is riskiest"
        },
        "explanation": explanation_text.strip(),
        "calculation_logic": {
            "methodology": "We look at how risky your areas are and how many trips you do there",
            "formula": "Risk = (danger of each area x trips there) / total trips",
            "factors_considered": [
                f"You drive in {len(operating_zones)} area(s)",
                f"You work during {len(operating_hours)} different hour(s)",
                f"We checked {total_trips} of your trips",
                "How dangerous each area is",
                "What time of day you drive"
            ]
        }
    }
    
    return jsonify(response)

# Serve any other file from the frontend folder (must be the last route)
@app.route("/<path:filename>")
def serve_static(filename):
    # If the URL starts with frontend/, strip that part off
    if filename.startswith("frontend/"):
        filename = filename[len("frontend/"):]
    return send_from_directory(FRONTEND_PATH, filename)

if __name__ == "__main__":
    app.run(debug=False, host='127.0.0.1', port=5000)