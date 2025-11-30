from flask import Flask, jsonify
from flask_cors import CORS
from datetime import datetime, timezone
import threading
import time
import math
import requests

from db import get_conn

# Arnhem config
ARNHEM_LAT = 51.9851
ARNHEM_LON = 5.8987

# Statistieken-bubbel (km)
BUBBLE_RADIUS_KM = 7.5

# Opslag-bereik voor tracks (km) – optie C
TRACK_RADIUS_KM = 20.0

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
    """
    Sla ALLE metingen op binnen TRACK_RADIUS_KM (20 km),
    zodat routes op de kaart volledig zichtbaar zijn.
    """
    conn = get_conn()
    cur = conn.cursor()
    now = int(datetime.now(timezone.utc).timestamp())

    for ac in ac_list:
        lat = ac.get("lat")
        lon = ac.get("lon")
        if lat is None or lon is None:
            continue

        # restrict to 20 km opslag-bereik
        if haversine_km(ARNHEM_LAT, ARNHEM_LON, lat, lon) > TRACK_RADIUS_KM:
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
# Helper: SQL-fragment voor bubbel-filter (7.5 km, Haversine in PostgreSQL)
#
# LET OP: dit wordt als string letterlijk in de queries geplakt.
# -------------------------------------------------------------------
BUBBLE_SQL = f"""
  (6371 * 2 * ASIN(
     SQRT(
       POWER(SIN(RADIANS(lat - {ARNHEM_LAT})/2), 2) +
       COS(RADIANS({ARNHEM_LAT})) * COS(RADIANS(lat)) *
       POWER(SIN(RADIANS(lon - {ARNHEM_LON})/2), 2)
     )
   )) <= {BUBBLE_RADIUS_KM}
"""


# -------------------------------------------------------------------
# Helper: definitie "unieke vlucht" (rollend 60-minutenvenster)
#
# Per callsign:
# - Sorteer metingen op tijd (oud -> nieuw)
# - Als ts - prev_ts > 3600 seconden OF er is geen prev_ts -> start nieuwe vlucht
# - flight_seq = cumulatieve som van deze "is_new_flight"-flags
#
# Unieke vlucht = (callsign, flight_seq)
#
# Voor ALLE statistieken gebruiken we alleen metingen BINNEN de 7.5 km bubbel.
# -------------------------------------------------------------------


# -------------------------------------------------------------------
# /api/last10  – laatste 10 unieke vluchten (op basis van bubbel-metingen)
# -------------------------------------------------------------------
@app.get("/api/last10")
def last10():
    rows = query(f"""
        WITH ordered AS (
          SELECT
            callsign,
            ts,
            gs_kts,
            alt_ft,
            LAG(ts) OVER (PARTITION BY callsign ORDER BY ts) AS prev_ts
          FROM positions
          WHERE callsign IS NOT NULL
            AND callsign <> ''
            AND {BUBBLE_SQL}
        ),
        flagged AS (
          SELECT
            callsign,
            ts,
            gs_kts,
            alt_ft,
            CASE
              WHEN prev_ts IS NULL THEN 1
              WHEN ts - prev_ts > 3600 THEN 1
              ELSE 0
            END AS is_new_flight
          FROM ordered
        ),
        segmented AS (
          SELECT
            callsign,
            ts,
            gs_kts,
            alt_ft,
            SUM(is_new_flight) OVER (PARTITION BY callsign ORDER BY ts) AS flight_seq
          FROM flagged
        ),
        flights AS (
          -- per vlucht (callsign, flight_seq) de laatste meting in de bubbel
          SELECT DISTINCT ON (callsign, flight_seq)
            callsign,
            ts,
            gs_kts,
            alt_ft
          FROM segmented
          ORDER BY callsign, flight_seq, ts DESC
        )
        SELECT callsign, ts, gs_kts, alt_ft
        FROM flights
        ORDER BY ts DESC
        LIMIT 10;
    """)

    return jsonify([
        {
            "ts": datetime.fromtimestamp(r["ts"], tz=timezone.utc).isoformat(),
            "callsign": r["callsign"],
            "gs_kts": r["gs_kts"],
            "alt_ft": r["alt_ft"],
        }
        for r in rows
    ])


# -------------------------------------------------------------------
# /api/daily_counts – aantal unieke vluchten per dag (bubbel)
# -------------------------------------------------------------------
@app.get("/api/daily_counts")
def daily_counts():
    rows = query(f"""
        WITH ordered AS (
          SELECT
            callsign,
            ts,
            gs_kts,
            alt_ft,
            LAG(ts) OVER (PARTITION BY callsign ORDER BY ts) AS prev_ts
          FROM positions
          WHERE callsign IS NOT NULL
            AND callsign <> ''
            AND {BUBBLE_SQL}
        ),
        flagged AS (
          SELECT
            callsign,
            ts,
            gs_kts,
            alt_ft,
            CASE
              WHEN prev_ts IS NULL THEN 1
              WHEN ts - prev_ts > 3600 THEN 1
              ELSE 0
            END AS is_new_flight
          FROM ordered
        ),
        segmented AS (
          SELECT
            callsign,
            ts,
            gs_kts,
            alt_ft,
            SUM(is_new_flight) OVER (PARTITION BY callsign ORDER BY ts) AS flight_seq
          FROM flagged
        ),
        flights AS (
          SELECT DISTINCT ON (callsign, flight_seq)
            callsign,
            ts
          FROM segmented
          ORDER BY callsign, flight_seq, ts DESC
        )
        SELECT
          to_char(to_timestamp(ts), 'YYYY-MM-DD') AS day,
          COUNT(*) AS flights
        FROM flights
        GROUP BY day
        ORDER BY day;
    """)
    return jsonify(rows)


# -------------------------------------------------------------------
# /api/stats – gebaseerd op unieke vluchten in de bubbel
# -------------------------------------------------------------------
@app.get("/api/stats")
def stats():
    conn = get_conn()
    cur = conn.cursor()

    # Eerste en laatste ruwe meting (meetperiode, alle data binnen 20 km)
    cur.execute("""
        SELECT MIN(ts) AS first_ts, MAX(ts) AS last_ts
        FROM positions;
    """)
    row0 = cur.fetchone()
    first_ts = row0["first_ts"]
    last_ts = row0["last_ts"]

    # Unieke vluchten in de bubbel
    cur.execute(f"""
        WITH ordered AS (
          SELECT
            callsign,
            ts,
            gs_kts,
            alt_ft,
            LAG(ts) OVER (PARTITION BY callsign ORDER BY ts) AS prev_ts
          FROM positions
          WHERE callsign IS NOT NULL
            AND callsign <> ''
            AND {BUBBLE_SQL}
        ),
        flagged AS (
          SELECT
            callsign,
            ts,
            gs_kts,
            alt_ft,
            CASE
              WHEN prev_ts IS NULL THEN 1
              WHEN ts - prev_ts > 3600 THEN 1
              ELSE 0
            END AS is_new_flight
          FROM ordered
        ),
        segmented AS (
          SELECT
            callsign,
            ts,
            gs_kts,
            alt_ft,
            SUM(is_new_flight) OVER (PARTITION BY callsign ORDER BY ts) AS flight_seq
          FROM flagged
        ),
        flights AS (
          SELECT DISTINCT ON (callsign, flight_seq)
            callsign,
            ts
          FROM segmented
          ORDER BY callsign, flight_seq, ts DESC
        )
        SELECT
          COUNT(*) AS total,
          ARRAY_AGG(
            JSON_BUILD_OBJECT(
              'day', to_char(to_timestamp(ts), 'YYYY-MM-DD'),
              'ts', ts
            )
            ORDER BY ts
          ) AS daily_detail
        FROM flights;
    """)
    row1 = cur.fetchone()
    total = row1["total"]
    daily_detail = row1["daily_detail"] or []

    cur.close()
    conn.close()

    # daily_detail is een array van {day, ts}; reduceer naar counts per dag
    day_counts = {}
    for item in daily_detail:
        day = item["day"]
        day_counts[day] = day_counts.get(day, 0) + 1

    days = len(day_counts)
    median = 0
    max_flights = 0
    max_day = None

    if days > 0:
        counts = list(day_counts.values())
        sorted_counts = sorted(counts)
        mid = days // 2
        if days % 2 == 1:
            median = sorted_counts[mid]
        else:
            median = (sorted_counts[mid - 1] + sorted_counts[mid]) / 2

        max_flights = max(counts)
        for d, c in day_counts.items():
            if c == max_flights:
                max_day = d
                break

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


# -------------------------------------------------------------------
# /api/hourly_heatmap – unieke vluchten per weekday × uur (bubbel)
# -------------------------------------------------------------------
@app.get("/api/hourly_heatmap")
def hourly_heatmap():
    rows = query(f"""
        WITH ordered AS (
          SELECT
            callsign,
            ts,
            gs_kts,
            alt_ft,
            LAG(ts) OVER (PARTITION BY callsign ORDER BY ts) AS prev_ts
          FROM positions
          WHERE callsign IS NOT NULL
            AND callsign <> ''
            AND {BUBBLE_SQL}
        ),
        flagged AS (
          SELECT
            callsign,
            ts,
            gs_kts,
            alt_ft,
            CASE
              WHEN prev_ts IS NULL THEN 1
              WHEN ts - prev_ts > 3600 THEN 1
              ELSE 0
            END AS is_new_flight
          FROM ordered
        ),
        segmented AS (
          SELECT
            callsign,
            ts,
            gs_kts,
            alt_ft,
            SUM(is_new_flight) OVER (PARTITION BY callsign ORDER BY ts) AS flight_seq
          FROM flagged
        ),
        flights AS (
          SELECT DISTINCT ON (callsign, flight_seq)
            callsign,
            ts
          FROM segmented
          ORDER BY callsign, flight_seq, ts DESC
        )
        SELECT
          EXTRACT(DOW FROM to_timestamp(ts))::INT AS dow,
          EXTRACT(HOUR FROM to_timestamp(ts))::INT AS hour,
          COUNT(*) AS flights
        FROM flights
        GROUP BY dow, hour
        ORDER BY dow, hour;
    """)
    return jsonify(rows)


# -------------------------------------------------------------------
# /api/top_callsigns – aantal unieke vluchten per callsign (bubbel)
# -------------------------------------------------------------------
@app.get("/api/top_callsigns")
def top_callsigns():
    rows = query(f"""
        WITH ordered AS (
          SELECT
            callsign,
            ts,
            gs_kts,
            alt_ft,
            LAG(ts) OVER (PARTITION BY callsign ORDER BY ts) AS prev_ts
          FROM positions
          WHERE callsign IS NOT NULL
            AND callsign <> ''
            AND {BUBBLE_SQL}
        ),
        flagged AS (
          SELECT
            callsign,
            ts,
            gs_kts,
            alt_ft,
            CASE
              WHEN prev_ts IS NULL THEN 1
              WHEN ts - prev_ts > 3600 THEN 1
              ELSE 0
            END AS is_new_flight
          FROM ordered
        ),
        segmented AS (
          SELECT
            callsign,
            ts,
            gs_kts,
            alt_ft,
            SUM(is_new_flight) OVER (PARTITION BY callsign ORDER BY ts) AS flight_seq
          FROM flagged
        ),
        flights AS (
          SELECT DISTINCT ON (callsign, flight_seq)
            callsign,
            ts
          FROM segmented
          ORDER BY callsign, flight_seq, ts DESC
        )
        SELECT
          callsign,
          COUNT(*) AS flights
        FROM flights
        GROUP BY callsign
        ORDER BY flights DESC
        LIMIT 10;
    """)
    return jsonify(rows)


# -------------------------------------------------------------------
# /api/tracks – routes van de 10 meest recente vluchten (volledige track)
#
# Hier gebruiken we ALLE opgeslagen data (20 km),
# zodat de lijnen op de kaart niet worden afgekapt bij 7.5 km.
# -------------------------------------------------------------------
@app.get("/api/tracks")
def tracks():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        WITH ordered AS (
          SELECT
            callsign,
            ts,
            lat,
            lon,
            alt_ft,
            LAG(ts) OVER (PARTITION BY callsign ORDER BY ts) AS prev_ts
          FROM positions
          WHERE callsign IS NOT NULL
            AND callsign <> ''
        ),
        flagged AS (
          SELECT
            callsign,
            ts,
            lat,
            lon,
            alt_ft,
            CASE
              WHEN prev_ts IS NULL THEN 1
              WHEN ts - prev_ts > 3600 THEN 1
              ELSE 0
            END AS is_new_flight
          FROM ordered
        ),
        segmented AS (
          SELECT
            callsign,
            ts,
            lat,
            lon,
            alt_ft,
            SUM(is_new_flight) OVER (PARTITION BY callsign ORDER BY ts) AS flight_seq
          FROM flagged
        ),
        flights AS (
          -- per vlucht: laatste tijdstip
          SELECT DISTINCT ON (callsign, flight_seq)
            callsign,
            flight_seq,
            ts AS last_ts
          FROM segmented
          ORDER BY callsign, flight_seq, ts DESC
        ),
        latest10 AS (
          SELECT *
          FROM flights
          ORDER BY last_ts DESC
          LIMIT 10
        )
        SELECT
          s.callsign,
          s.ts,
          s.lat,
          s.lon,
          s.alt_ft
        FROM segmented s
        JOIN latest10 lf
          ON s.callsign = lf.callsign
         AND s.flight_seq = lf.flight_seq
        ORDER BY lf.last_ts DESC, s.ts ASC;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    grouped = {}
    for r in rows:
        cs = r["callsign"]
        grouped.setdefault(cs, []).append({
            "ts": datetime.fromtimestamp(r["ts"], tz=timezone.utc).isoformat(),
            "lat": r["lat"],
            "lon": r["lon"],
            "alt_ft": r["alt_ft"],
        })

    return jsonify([
        {"callsign": cs, "points": pts}
        for cs, pts in grouped.items()
    ])


@app.get("/")
def home():
    return "Arnhem Flight API running"


# -------------------------------------------------------------------
# Local run (Render uses gunicorn app:app)
# -------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
