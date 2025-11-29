import os
import time
import math
import requests
from datetime import datetime, timezone
from db import get_conn

ARNHEM_LAT = 51.9851
ARNHEM_LON = 5.8987
BUBBLE_RADIUS_KM = 5.0
ADSB_URL = "https://opendata.adsb.fi/api/v3/lat/51.9851/lon/5.8987/dist/3"

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    import math
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    with open("schema.sql") as f:
        cur.execute(f.read())
    conn.commit()
    cur.close()
    conn.close()

def save(ac_list):
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
            lat,
            lon,
            ac.get("alt_baro"),
            ac.get("gs"),
        ))

    conn.commit()
    cur.close()
    conn.close()

def main():
    # Make sure tables exist
    init_db()

    while True:
        try:
            r = requests.get(ADSB_URL, timeout=10)
            r.raise_for_status()
            data = r.json()
            save(data.get("ac", []))
            print("Saved batch at", datetime.utcnow())
        except Exception as e:
            print("Error:", e)

        time.sleep(10)  # stay well under rate limits

if __name__ == "__main__":
    main()
