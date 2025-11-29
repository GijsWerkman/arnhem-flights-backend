CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    icao TEXT,
    callsign TEXT,
    ts INTEGER,
    lat REAL,
    lon REAL,
    alt_ft REAL,
    gs_kts REAL
);

CREATE INDEX IF NOT EXISTS idx_ts ON positions (ts);
CREATE INDEX IF NOT EXISTS idx_icao ON positions (icao);

sqlite3 flights.db < schema.sql
