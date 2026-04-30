-- 1. Enable PostGIS to support geometry types and spatial queries.
CREATE EXTENSION IF NOT EXISTS postgis;

-- 2. Clean crime event facts loaded by the Spark processor.
CREATE TABLE IF NOT EXISTS historical_crimes (
    id                  TEXT PRIMARY KEY,
    incident_type       TEXT,
    description         TEXT,

    -- Timestamps
    offense_date        TIMESTAMP,
    report_date         TIMESTAMP,

    -- Spatial / Location Data
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    address             TEXT,
    city                TEXT,
    state               TEXT,

    -- Feature Engineering Columns
    offense_hour        INTEGER,
    offense_day_of_week TEXT,

    -- SRID 4326 = GPS lat/lon.
    geometry            GEOMETRY(POINT, 4326),

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Keep existing deployments migratable.
ALTER TABLE historical_crimes ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE historical_crimes ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE historical_crimes ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_crime_geometry ON historical_crimes USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_crime_offense_date ON historical_crimes (offense_date);
CREATE INDEX IF NOT EXISTS idx_crime_day_hour ON historical_crimes (offense_day_of_week, offense_hour);
CREATE INDEX IF NOT EXISTS idx_crime_incident_type ON historical_crimes (incident_type);

-- 3. Source-of-truth map grid used by PostGIS lookup and heatmap rendering.
CREATE TABLE IF NOT EXISTS grid_cells (
    grid_id             TEXT PRIMARY KEY,
    geom                GEOMETRY(POLYGON, 4326) NOT NULL,
    center_lat          DOUBLE PRECISION NOT NULL,
    center_lon          DOUBLE PRECISION NOT NULL,
    cell_size_meters    INTEGER NOT NULL,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_grid_cells_geom ON grid_cells USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_grid_cells_center ON grid_cells (center_lat, center_lon);

-- 4. Cached daily prediction layer served to the frontend as GeoJSON.
CREATE TABLE IF NOT EXISTS daily_grid_predictions (
    prediction_id          BIGSERIAL PRIMARY KEY,
    grid_id                TEXT NOT NULL REFERENCES grid_cells(grid_id) ON DELETE CASCADE,
    prediction_date        DATE NOT NULL,
    prediction_window      TEXT NOT NULL,
    day_of_week            TEXT,
    time_window_start      TIME,
    time_window_end        TIME,
    risk_score             DOUBLE PRECISION NOT NULL CHECK (risk_score >= 0.0 AND risk_score <= 1.0),
    risk_level             TEXT NOT NULL CHECK (risk_level IN ('low', 'medium', 'high')),
    dominant_crime_type    TEXT,
    historical_count       INTEGER DEFAULT 0,
    recent_30_day_count    INTEGER DEFAULT 0,
    nearby_poi_score       DOUBLE PRECISION DEFAULT 0.0,
    model_version          TEXT NOT NULL,
    data_snapshot_version  TEXT,
    generated_at           TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (grid_id, prediction_date, prediction_window, model_version)
);

CREATE INDEX IF NOT EXISTS idx_daily_predictions_grid_date
ON daily_grid_predictions (grid_id, prediction_date);

CREATE INDEX IF NOT EXISTS idx_daily_predictions_grid_date_window
ON daily_grid_predictions (grid_id, prediction_date, prediction_window);

CREATE INDEX IF NOT EXISTS idx_daily_predictions_date
ON daily_grid_predictions (prediction_date);

CREATE INDEX IF NOT EXISTS idx_daily_predictions_risk_level
ON daily_grid_predictions (risk_level);

-- 5. Optional app-level cache for LLM advice. The API can still work without it.
CREATE TABLE IF NOT EXISTS predictive_advice_cache (
    cache_key              TEXT PRIMARY KEY,
    grid_id                TEXT NOT NULL,
    prediction_window      TEXT NOT NULL,
    model_version          TEXT NOT NULL,
    data_snapshot_version  TEXT,
    response_json          JSONB NOT NULL,
    created_at             TIMESTAMPTZ DEFAULT NOW(),
    updated_at             TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_advice_cache_grid_window
ON predictive_advice_cache (grid_id, prediction_window);

-- Legacy table kept for compatibility with early experiments.
CREATE TABLE IF NOT EXISTS predictive_analysis (
    prediction_id       SERIAL PRIMARY KEY,
    grid_id             TEXT,
    prediction_time     TIMESTAMP,
    risk_score          DOUBLE PRECISION,
    model_version       TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_prediction_time ON predictive_analysis (prediction_time);
