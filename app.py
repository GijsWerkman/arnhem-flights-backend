from flask import Flask, jsonify
from flask_cors import CORS
from datetime import datetime, timezone
import threading
import time
import math
import requests

from db import get_conn

# Arnhem bubble config
ARNHEM_LAT = 51.9851
ARNHEM_LON = 5.8987
BUBBLE_RADIUS_KM = 5.0
ADSB_URL = "https://opendata.adsb.fi/api/v3/lat/51.9851/lon/5.8987/dist/3"

app = Flask(__name__)
CORS(app)

collector_started = False  # ensures background thread runs only once


# -------------------------------------------------------------------
# Database initialization
# -------------------------------------------------------------------
def init_db():
    conn = get_conn()
    cur = conn.cursor()
    with open("schema.sql") as f:
        cur.execute(f.read())
    conn.commit()
    cur.close()
    conn.close()


# -------------------------------------------------------------------
# Query helper
# -------------------------------------------------------------------
def query(sql):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# -------------------------------------------------------------------
# Collector logic
# -------------------------------------------------------------------
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2 +
        math.cos(phi1) * math.cos(phi2) *
        math.sin(dlambda / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def save_positions(ac_list):
    conn = get_conn()
    cur = conn.cursor()
    now = int(datetime.now(timezone.utc).timestamp())

    for ac in ac_list:
        lat = ac.get("lat")
        lon = ac.get("lon")
        if lat is None or lon is None:
            continue

        # restrict to 5 km bubble
        if haversine_km(ARNHEM_LAT, ARNHEM_LON, lat, lon) > BUBBLE_RADIUS_KM:
            continue

        cur.execute(
            """
            INSERT INTO positions (icao, callsign, ts, lat, lon, alt_ft, gs_kts)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                ac.get("icao"),
                (ac.get("flight") or "").strip(),
                now,
                lat,
                lon,
                ac.get("alt_baro"),
                ac.get("gs"),
            )
        )

    conn.commit()
    cur.close()
    conn.close()


def collector_loop():
    print("Collector thread started")
    init_db()

    while True:
        try:
            r = requests.get(ADSB_URL, timeout=10)
            r.raise_for_status()
            ac = r.json().get("ac", [])
            save_positions(ac)
            print("Saved batch at", datetime.utcnow())
        except Exception as e:
            print("Collector error:", e)

        time.sleep(10)


# -------------------------------------------------------------------
# Start collector once on first request
# -------------------------------------------------------------------
@app.before_request
def start_collector_once():
    global collector_started
    if not collector_started:
        print("Launching collector thread...")
        t = threading.Thread(target=collector_loop, daemon=True)
        t.start()
        collector_started = True


# -------------------------------------------------------------------
# API Endpoints
# -------------------------------------------------------------------

# UNIQUE-LAST 10 (option B)
@app.get("/api/last10")
def last10():
    rows = query("""
        WITH unique_latest AS (
            SELECT DISTINCT ON (callsign)
                callsign, ts, gs_kts, alt_ft
            FROM positions
            WHERE callsign IS NOT NULL AND callsign <> ''
            ORDER BY callsign, ts DESC
        )
        SELECT *
        FROM unique_latest
        ORDER BY ts DESC
        LIMIT 10;
    """)

    return jsonify([
        {
            "ts": datetime.fromtimestamp(r["ts"], tz=timezone.utc).isoformat(),
            "callsign": r["callsign"],
            "gs_kts": r["gs_kts"],
            "alt_ft": r["alt_ft"]
        }
        for r in rows
    ])


@app.get("/api/daily_counts")
def daily_counts():
    rows = query("""
        SELECT to_char(to_timestamp(ts), 'YYYY-MM-DD') AS day,
               COUNT(*) AS flights
        FROM positions
        GROUP BY 1
        ORDER BY 1;
    """)
    return jsonify(rows)


@app.get("/api/stats")
def stats():
    conn = get_conn()
    cur = conn.cursor()

    # global stats
    cur.execute("""
        SELECT COUNT(*) AS total, MIN(ts) AS first_ts, MAX(ts) AS last_ts
        FROM positions;
    """)
    s = cur.fetchone()

    total = s["total"]
    first_ts = s["first_ts"]
    last_ts = s["last_ts"]

    # per-day stats
    cur.execute("""
        SELECT to_char(to_timestamp(ts), 'YYYY-MM-DD') AS day,
               COUNT(*) AS flights
        FROM positions
        GROUP BY 1
        ORDER BY 1;
    """)
    days_raw = cur.fetchall()

    # compute median & max
    days = len(days_raw)
    if days > 0:
        counts = sorted([d["flights"] for d in days_raw])
        mid = days // 2
        if days % 2 == 1:
            median = counts[mid]
        else:
            median = (counts[mid - 1] + counts[mid]) / 2
        max_flights = max(counts)
        max_day = days_raw[counts.index(max_flights)]["day"]
    else:
        median = 0
        max_flights = 0
        max_day = None

    cur.close()
    conn.close()

    def iso(ts):
        if ts is None:
            return None
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    return jsonify({
        "total_flights": total,
        "first_ts": iso(first_ts),
        "last_ts": iso(last_ts),
        "days": days,
        "median_per_day": median,
        "max_per_day": max_flights,
        "max_per_day_date": max_day
    })


@app.get("/api/hourly_heatmap")
def hourly_heatmap():
    rows = query("""
        SELECT
          EXTRACT(DOW FROM to_timestamp(ts))::INT AS dow,
          EXTRACT(HOUR FROM to_timestamp(ts))::INT AS hour,
          COUNT(*) AS flights
        FROM positions
        GROUP BY 1,2
        ORDER BY 1,2;
    """)
    return jsonify(rows)


@app.get("/api/top_callsigns")
def top_callsigns():
    rows = query("""
        SELECT callsign, COUNT(*) AS flights
        FROM positions
        WHERE callsign IS NOT NULL AND callsign <> ''
        GROUP BY callsign
        ORDER BY flights DESC
        LIMIT 10;
    """)
    return jsonify(rows)


@app.get("/api/tracks")
def tracks():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        WITH latest AS (
          SELECT callsign, MAX(ts) AS last_ts
          FROM positions
          WHERE callsign IS NOT NULL AND callsign <> ''
          GROUP BY callsign
          ORDER BY last_ts DESC
          LIMIT 10
        )
        SELECT p.callsign, p.ts, p.lat, p.lon, p.alt_ft
        FROM positions p
        JOIN latest l USING (callsign)
        ORDER BY l.last_ts DESC, p.ts ASC;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    tracks = {}
    for r in rows:
        cs = r["callsign"]
        if cs not in tracks:
            tracks[cs] = []
        tracks[cs].append({
            "ts": datetime.fromtimestamp(r["ts"], tz=timezone.utc).isoformat(),
            "lat": r["lat"],
            "lon": r["lon"],
            "alt_ft": r["alt_ft"],
        })

    return jsonify([
        {"callsign": cs, "points": pts}
        for cs, pts in tracks.items()
    ])


@app.get("/")
def home():
    return "Arnhem Flight API running"


# -------------------------------------------------------------------
# Run only when local (Render uses gunicorn)
# -------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
