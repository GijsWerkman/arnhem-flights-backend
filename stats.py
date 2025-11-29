import sqlite3
from datetime import datetime, timezone

DB = "flights.db"

def last10():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT ts, callsign, gs_kts, alt_ft
        FROM positions
        ORDER BY ts DESC
        LIMIT 10;
    """)
    rows = cur.fetchall()
    conn.close()

    return [
        {
            "ts": datetime.fromtimestamp(r[0], tz=timezone.utc).isoformat(),
            "callsign": r[1],
            "gs_kts": r[2],
            "alt_ft": r[3],
        }
        for r in rows
    ]

def daily_counts():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT strftime('%Y-%m-%d', ts, 'unixepoch') AS day,
               COUNT(DISTINCT icao || '-' || strftime('%H', ts, 'unixepoch')) AS flights
        FROM positions
        GROUP BY day
        ORDER BY day;
    """)
    rows = cur.fetchall()
    conn.close()
    return [{"day": r[0], "flights": r[1]} for r in rows]
