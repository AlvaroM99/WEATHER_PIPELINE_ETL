-- ============================================================================
-- DIMENSIONAL TABLES INITIALIZATION
-- Data Warehouse Schema: dwh
-- ============================================================================

-- Create dwh schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS dwh;

-- ============================================================================
-- DIM_CITY - City Dimension
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.dim_city (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL UNIQUE,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    population INTEGER,
    is_coastal INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dwh.dim_city IS 'City dimension table for Spanish capitals';

-- ============================================================================
-- DIM_DATE - Date Dimension (2020-2030)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    id_calendar_day INTEGER PRIMARY KEY,
    dt_date DATE NOT NULL UNIQUE,
    id_year INTEGER NOT NULL,
    id_calendar_month INTEGER NOT NULL,
    id_month INTEGER NOT NULL,
    id_calendar_week INTEGER NOT NULL,
    id_week INTEGER NOT NULL,
    id_weekday INTEGER NOT NULL,
    id_quarter INTEGER NOT NULL,
    id_calendar_quarter INTEGER NOT NULL,
    id_calendar_semester INTEGER NOT NULL,
    nm_day VARCHAR(20),
    ds_calendar_day VARCHAR(50)
);

COMMENT ON TABLE dwh.dim_date IS 'Date dimension with calendar attributes from 2020 to 2030';

-- ============================================================================
-- DIM_WEEK - Week Dimension
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.dim_week (
    id_calendar_week INTEGER PRIMARY KEY,
    id_year INTEGER NOT NULL,
    id_week INTEGER NOT NULL,
    ds_calendar_week VARCHAR(50),
    ds_week_from_to VARCHAR(50),
    dt_monday_of_week DATE,
    dt_sunday_of_week DATE
);

COMMENT ON TABLE dwh.dim_week IS 'Week dimension derived from dim_date';

-- ============================================================================
-- DIM_MONTH - Month Dimension
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.dim_month (
    id_calendar_month INTEGER PRIMARY KEY,
    id_month INTEGER NOT NULL,
    id_year INTEGER NOT NULL,
    id_quarter INTEGER NOT NULL,
    id_calendar_quarter INTEGER NOT NULL,
    id_calendar_semester INTEGER NOT NULL,
    ds_calendar_month VARCHAR(50),
    dt_month_first_day DATE
);

COMMENT ON TABLE dwh.dim_month IS 'Month dimension derived from dim_date';

-- ============================================================================
-- DIM_SEASONS - Season Dimension (Northern/Southern Hemisphere)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.dim_seasons (
    season_id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    hemisphere VARCHAR(10) NOT NULL CHECK (hemisphere IN ('North', 'South')),
    season_type VARCHAR(20) DEFAULT 'Meteorological',
    start_month INTEGER NOT NULL CHECK (start_month BETWEEN 1 AND 12),
    end_month INTEGER NOT NULL CHECK (end_month BETWEEN 1 AND 12),
    precipitation_type VARCHAR(20),
    is_growing_season BOOLEAN DEFAULT false,
    color_code VARCHAR(10),
    description TEXT
);

COMMENT ON TABLE dwh.dim_seasons IS 'Season dimension for both hemispheres';

-- ============================================================================
-- DIM_LAYERS - Atmospheric and Soil Layers Dimension
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.dim_layers (
    layer_id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    medium VARCHAR(20) NOT NULL CHECK (medium IN ('Surface', 'Air', 'Soil')),
    vertical_level_m DECIMAL(10,2),
    pressure_hpa DECIMAL(10,2),
    is_standard_wmo BOOLEAN DEFAULT false,
    layer_category VARCHAR(50),
    description TEXT
);

COMMENT ON TABLE dwh.dim_layers IS 'Atmospheric and soil layers for weather measurements';

-- ============================================================================
-- DIM_SEVERITY - Weather Severity Levels
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.dim_severity (
    severity_id INTEGER PRIMARY KEY,
    severity_level VARCHAR(50) NOT NULL,
    color_code VARCHAR(10),
    description TEXT
);

COMMENT ON TABLE dwh.dim_severity IS 'Weather severity classification levels';

-- ============================================================================
-- CREATE INDEXES FOR PERFORMANCE
-- ============================================================================

-- dim_date indexes
CREATE INDEX IF NOT EXISTS idx_dim_date_year ON dwh.dim_date(id_year);
CREATE INDEX IF NOT EXISTS idx_dim_date_month ON dwh.dim_date(id_calendar_month);
CREATE INDEX IF NOT EXISTS idx_dim_date_week ON dwh.dim_date(id_calendar_week);

-- dim_city indexes
CREATE INDEX IF NOT EXISTS idx_dim_city_name ON dwh.dim_city(city_name);

-- dim_week indexes
CREATE INDEX IF NOT EXISTS idx_dim_week_year ON dwh.dim_week(id_year);

-- dim_month indexes
CREATE INDEX IF NOT EXISTS idx_dim_month_year ON dwh.dim_month(id_year);

-- dim_seasons indexes
CREATE INDEX IF NOT EXISTS idx_dim_seasons_hemisphere ON dwh.dim_seasons(hemisphere);
CREATE INDEX IF NOT EXISTS idx_dim_seasons_month ON dwh.dim_seasons(start_month, end_month);

-- dim_layers indexes
CREATE INDEX IF NOT EXISTS idx_dim_layers_medium ON dwh.dim_layers(medium);
CREATE INDEX IF NOT EXISTS idx_dim_layers_category ON dwh.dim_layers(layer_category);
