CREATE TABLE IF NOT EXISTS positions (
    id SERIAL PRIMARY KEY,
    icao TEXT,
    callsign TEXT,
    ts BIGINT,        -- unix epoch seconds
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    alt_ft DOUBLE PRECISION,
    gs_kts DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_ts ON positions(ts);
CREATE INDEX IF NOT EXISTS idx_icao ON positions(icao);
