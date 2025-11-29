import time
import math
import requests
import sqlite3
import os
from datetime import datetime, timezone

ARNHEM_LAT = 51.9851
ARNHEM_LON = 5.8987
BUBBLE_RADIUS_KM = 5.0
ADSB_URL = "https://opendata.adsb.fi/api/v3/lat/51.9851/lon/5.8987/dist/3"
DB_PATH = "/data/flights.db"

if not os.path.exists("/data/flights.db"):
    import sqlite3
    conn = sqlite3.connect("/data/flights.db")
    with open("schema.sql") as f:
        conn.executescript(f.read())
    conn.close()

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def save(ac_list):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    now = int(datetime.now(timezone.utc).timestamp())

    for ac in ac_list:
        lat = ac.get("lat")
        lon = ac.get("lon")
        if lat is None or lon is None:
            continue

        if haversine_km(ARNHEM_LAT, ARNHEM_LON, lat, lon) > BUBBLE_RADIUS_KM:
            continue

        icao = ac.get("icao")
        callsign = (ac.get("flight") or "").strip()
        alt_ft = ac.get("alt_baro")
        gs_kts = ac.get("gs")

        cur.execute("""
            INSERT INTO positions (icao, callsign, ts, lat, lon, alt_ft, gs_kts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (icao, callsign, now, lat, lon, alt_ft, gs_kts))

    conn.commit()
    conn.close()

def main():
    while True:
        try:
            r = requests.get(ADSB_URL, timeout=10)
            r.raise_for_status()
            data = r.json()
            save(data.get("ac", []))
            print("Saved batch at", datetime.utcnow())
        except Exception as e:
            print("Error:", e)

        time.sleep(10)

if __name__ == "__main__":
    main()
