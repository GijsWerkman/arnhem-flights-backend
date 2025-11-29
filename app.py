from flask import Flask, jsonify
from datetime import datetime, timezone
import sqlite3

DB = "flights.db"
app = Flask(__name__)

def query(sql):
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    return rows

@app.get("/api/last10")
def last10():
    rows = query("""
        SELECT ts, callsign, gs_kts, alt_ft
        FROM positions
        ORDER BY ts DESC
        LIMIT 10;
    """)

    return jsonify([
        {
            "ts": datetime.fromtimestamp(r[0], tz=timezone.utc).isoformat(),
            "callsign": r[1],
            "gs_kts": r[2],
            "alt_ft": r[3],
        }
        for r in rows
    ])

@app.get("/api/daily_counts")
def daily():
    rows = query("""
        SELECT strftime('%Y-%m-%d', ts, 'unixepoch'),
               COUNT(DISTINCT icao || '-' || strftime('%H', ts, 'unixepoch'))
        FROM positions
        GROUP BY 1
        ORDER BY 1;
    """)
    return jsonify([{"day": r[0], "flights": r[1]} for r in rows])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
