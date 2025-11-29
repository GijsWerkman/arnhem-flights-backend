from flask import Flask, jsonify
from datetime import datetime, timezone
import threading
import time
import math
import requests

from db import get_conn

# Arnhem bubble
ARNHEM_LAT = 51.9851
ARNHEM_LON = 5.8987
BUBBLE_RADIUS_KM = 5.0
ADSB_URL = "https://opendata.adsb.fi/api/v3/lat/51.9851/lon/5.8987/dist/3"

app = Flask(__name__)

collector_started = False   # <-- important global flag


# ---------------------------------
# Database init
# ---------------------------------
def init_db():
    conn = get_conn()
    cur = conn.cursor()
    with open("schema.sql") as f:
        cur.execute(f.read())
    conn.commit()
    cur.close()
    conn.close()


# ---------------------------------
# Query helper
# ---------------------------------
def query(sql):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# ---------------------------------
# Collector logic
# ---------------------------------
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi/2)**2 +
        math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
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

        if haversine_km(ARNHEM_LAT, ARNHEM_LON, lat, lon) > BUBBLE_RADIUS_KM:
            continue

        cur.execute("""
            INSERT INTO positions (icao, callsign, ts, lat, lon, alt_ft, gs_kts)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            ac.get("icao"),
            (ac.get("flight") or "").strip(),
            now,
