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
    city_code VARCHAR(10) NOT NULL UNIQUE,
    city_name VARCHAR(100) NOT NULL,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    country_code VARCHAR(2) NOT NULL DEFAULT 'ES',
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
CREATE INDEX IF NOT EXISTS idx_dim_city_code ON dwh.dim_city(city_code);
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

-- ============================================================================
-- DIM_AEMET_STATIONS - AEMET Meteorological Stations Dimension
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.dim_aemet_stations (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(10) NOT NULL UNIQUE,
    station_name VARCHAR(100),
    province VARCHAR(50),
    altitude DECIMAL(7,2),
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6),
    synop_code VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE dwh.dim_aemet_stations IS 'AEMET meteorological stations dimension (Spanish Meteorological Agency)';

-- dim_aemet_stations indexes
CREATE INDEX IF NOT EXISTS idx_dim_aemet_stations_id ON dwh.dim_aemet_stations(station_id);
CREATE INDEX IF NOT EXISTS idx_dim_aemet_stations_province ON dwh.dim_aemet_stations(province);
CREATE INDEX IF NOT EXISTS idx_dim_aemet_stations_coords ON dwh.dim_aemet_stations(latitude, longitude);


-- ============================================================================
-- POPULATE STATIC DIMENSIONS
-- ============================================================================

-- ============================================================================
-- DIM_DATE: Generate all dates from 2020-01-01 to 2030-12-31
-- ============================================================================
INSERT INTO dwh.dim_date (
    id_calendar_day, dt_date, id_year, id_calendar_month, id_month,
    id_calendar_week, id_week, id_weekday, id_quarter, id_calendar_quarter,
    id_calendar_semester, nm_day, ds_calendar_day
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER                       AS id_calendar_day,
    d                                                       AS dt_date,
    EXTRACT(YEAR FROM d)::INTEGER                           AS id_year,
    TO_CHAR(d, 'YYYYMM')::INTEGER                          AS id_calendar_month,
    EXTRACT(MONTH FROM d)::INTEGER                          AS id_month,
    -- ISO week: year of the ISO week * 100 + ISO week number
    (EXTRACT(ISOYEAR FROM d)::INTEGER * 100
        + EXTRACT(WEEK FROM d)::INTEGER)                    AS id_calendar_week,
    EXTRACT(WEEK FROM d)::INTEGER                           AS id_week,
    EXTRACT(ISODOW FROM d)::INTEGER                         AS id_weekday,
    EXTRACT(QUARTER FROM d)::INTEGER                        AS id_quarter,
    (EXTRACT(YEAR FROM d)::INTEGER * 10
        + EXTRACT(QUARTER FROM d)::INTEGER)                 AS id_calendar_quarter,
    (EXTRACT(YEAR FROM d)::INTEGER * 10
        + CASE WHEN EXTRACT(MONTH FROM d) <= 6
               THEN 1 ELSE 2 END)                          AS id_calendar_semester,
    TO_CHAR(d, 'FMDay')                                     AS nm_day,
    TO_CHAR(d, 'FMDay, DD FMMonth YYYY')                    AS ds_calendar_day
FROM generate_series('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day') AS d
ON CONFLICT (id_calendar_day) DO NOTHING;


-- ============================================================================
-- DIM_WEEK: Derive from dim_date (one row per ISO week)
-- ============================================================================
INSERT INTO dwh.dim_week (
    id_calendar_week, id_year, id_week,
    ds_calendar_week, ds_week_from_to, dt_monday_of_week, dt_sunday_of_week
)
SELECT DISTINCT ON (id_calendar_week)
    id_calendar_week,
    EXTRACT(ISOYEAR FROM dt_date)::INTEGER                  AS id_year,
    id_week,
    'W' || LPAD(id_week::TEXT, 2, '0') || ' '
        || EXTRACT(ISOYEAR FROM dt_date)::INTEGER           AS ds_calendar_week,
    TO_CHAR(dt_date - (EXTRACT(ISODOW FROM dt_date)::INT - 1) * INTERVAL '1 day', 'DD/MM')
        || ' - '
        || TO_CHAR(dt_date + (7 - EXTRACT(ISODOW FROM dt_date)::INT) * INTERVAL '1 day', 'DD/MM')
                                                            AS ds_week_from_to,
    (dt_date - (EXTRACT(ISODOW FROM dt_date)::INT - 1) * INTERVAL '1 day')::DATE
                                                            AS dt_monday_of_week,
    (dt_date + (7 - EXTRACT(ISODOW FROM dt_date)::INT) * INTERVAL '1 day')::DATE
                                                            AS dt_sunday_of_week
FROM dwh.dim_date
ORDER BY id_calendar_week, dt_date
ON CONFLICT (id_calendar_week) DO NOTHING;


-- ============================================================================
-- DIM_MONTH: Derive from dim_date (one row per calendar month)
-- ============================================================================
INSERT INTO dwh.dim_month (
    id_calendar_month, id_month, id_year,
    id_quarter, id_calendar_quarter, id_calendar_semester,
    ds_calendar_month, dt_month_first_day
)
SELECT DISTINCT ON (id_calendar_month)
    id_calendar_month,
    id_month,
    id_year,
    id_quarter,
    id_calendar_quarter,
    id_calendar_semester,
    TO_CHAR(dt_date, 'FMMonth YYYY')                        AS ds_calendar_month,
    DATE_TRUNC('month', dt_date)::DATE                      AS dt_month_first_day
FROM dwh.dim_date
ORDER BY id_calendar_month, dt_date
ON CONFLICT (id_calendar_month) DO NOTHING;


-- ============================================================================
-- DIM_SEASONS: Meteorological seasons for both hemispheres
-- ============================================================================
INSERT INTO dwh.dim_seasons (
    season_id, name, hemisphere, season_type,
    start_month, end_month, precipitation_type,
    is_growing_season, color_code, description
) VALUES
    -- Northern hemisphere (meteorological definition)
    (1, 'Winter',  'North', 'Meteorological', 12, 2, 'Snow/Rain', false, '#4FC3F7',
        'December-February: coldest months in the Northern Hemisphere'),
    (2, 'Spring',  'North', 'Meteorological',  3, 5, 'Rain',      true,  '#81C784',
        'March-May: warming temperatures with frequent rain'),
    (3, 'Summer',  'North', 'Meteorological',  6, 8, 'Convective', true, '#FFD54F',
        'June-August: warmest months, thunderstorm season'),
    (4, 'Autumn',  'North', 'Meteorological',  9, 11, 'Rain',     false, '#FF8A65',
        'September-November: cooling temperatures, increasing rainfall'),
    -- Southern hemisphere (meteorological definition)
    (5, 'Winter',  'South', 'Meteorological',  6, 8, 'Snow/Rain', false, '#4FC3F7',
        'June-August: coldest months in the Southern Hemisphere'),
    (6, 'Spring',  'South', 'Meteorological',  9, 11, 'Rain',     true,  '#81C784',
        'September-November: warming temperatures'),
    (7, 'Summer',  'South', 'Meteorological', 12, 2, 'Convective', true, '#FFD54F',
        'December-February: warmest months'),
    (8, 'Autumn',  'South', 'Meteorological',  3, 5, 'Rain',      false, '#FF8A65',
        'March-May: cooling temperatures')
ON CONFLICT (season_id) DO NOTHING;


-- ============================================================================
-- DIM_LAYERS: Standard atmospheric and soil measurement layers
-- ============================================================================
INSERT INTO dwh.dim_layers (
    layer_id, name, medium, vertical_level_m, pressure_hpa,
    is_standard_wmo, layer_category, description
) VALUES
    -- Surface measurements
    (1,  'Surface (2m)',       'Surface',  2.00,    NULL, true,  'Near-surface',
        'Standard WMO screen-level: temperature, humidity, dew point at 2m AGL'),
    (2,  'Surface (10m)',      'Surface', 10.00,    NULL, true,  'Near-surface',
        'Standard WMO wind measurement height: wind speed and direction at 10m AGL'),
    -- Atmospheric layers
    (3,  'Boundary Layer',     'Air',    1000.00,   NULL, false, 'Troposphere',
        'Planetary boundary layer (~1 km): turbulent mixing, diurnal cycle'),
    (4,  'Lower Troposphere',  'Air',    3000.00, 700.00, true,  'Troposphere',
        '700 hPa level (~3 km): cloud base, low-level wind patterns'),
    (5,  'Mid Troposphere',    'Air',    5500.00, 500.00, true,  'Troposphere',
        '500 hPa level (~5.5 km): synoptic-scale weather patterns'),
    (6,  'Upper Troposphere',  'Air',    9000.00, 300.00, true,  'Troposphere',
        '300 hPa level (~9 km): jet stream, upper-level divergence'),
    (7,  'Tropopause',         'Air',   12000.00, 200.00, true,  'Tropopause',
        '200 hPa level (~12 km): tropopause boundary'),
    (8,  'Lower Stratosphere', 'Air',   20000.00, 50.00,  true,  'Stratosphere',
        '50 hPa level (~20 km): ozone layer, stratospheric dynamics'),
    -- Wind measurement at 80m (used by Open-Meteo for wind energy)
    (9,  'Wind (80m)',         'Air',      80.00,   NULL, false, 'Near-surface',
        'Wind turbine hub height: wind speed at 80m AGL for energy forecasting'),
    -- Soil layers
    (10, 'Topsoil (0-10cm)',   'Soil',     0.10,    NULL, false, 'Soil',
        'Shallow soil layer: soil temperature and moisture at 0-10 cm depth'),
    (11, 'Subsoil (10-40cm)',  'Soil',     0.40,    NULL, false, 'Soil',
        'Root zone: soil moisture at 10-40 cm depth'),
    (12, 'Deep Soil (40-100cm)', 'Soil',   1.00,    NULL, false, 'Soil',
        'Deep root zone: soil moisture at 40-100 cm depth')
ON CONFLICT (layer_id) DO NOTHING;


-- ============================================================================
-- DIM_SEVERITY: Weather severity classification (AEMET / Meteoalarm scale)
-- ============================================================================
INSERT INTO dwh.dim_severity (
    severity_id, severity_level, color_code, description
) VALUES
    (1, 'No Risk',    '#00E676', 'No significant weather hazards expected'),
    (2, 'Low',        '#FFEB3B', 'Minor weather hazards: be aware of conditions'),
    (3, 'Moderate',   '#FF9800', 'Moderate weather hazards: be prepared for disruptions'),
    (4, 'High',       '#F44336', 'Severe weather: significant risk to life and property'),
    (5, 'Extreme',    '#9C27B0', 'Extreme weather event: immediate danger, take shelter')
ON CONFLICT (severity_id) DO NOTHING;
