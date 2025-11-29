from flask import Flask, jsonify
from datetime import datetime, timezone
from db import get_conn

app = Flask(__name__)

def query(sql):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
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
            "ts": datetime.fromtimestamp(r["ts"], tz=timezone.utc).isoformat(),
            "callsign": r["callsign"],
            "gs_kts": r["gs_kts"],
            "alt_ft": r["alt_ft"],
        }
        for r in rows
    ])

@app.get("/api/daily_counts")
def daily():
    rows = query("""
        SELECT to_char(to_timestamp(ts), 'YYYY-MM-DD') AS day,
               COUNT(*) AS flights
        FROM positions
        GROUP BY 1
        ORDER BY 1;
    """)
    return jsonify(rows)

@app.get("/")
def home():
    return "Arnhem Flight API is running"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
