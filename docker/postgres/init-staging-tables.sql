-- ============================================================================
-- STAGING TABLES INITIALIZATION
-- Schema: staging
-- Purpose: Intermediate layer between Silver (Parquet) and Gold (DWH)
-- Data Quality validation happens here before promoting to Gold
-- ============================================================================

-- Create staging schema
CREATE SCHEMA IF NOT EXISTS staging;

-- Enable UUID extension for batch tracking
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- VALIDATION_LOG - Tracks data quality validation results
-- ============================================================================
CREATE TABLE IF NOT EXISTS staging.validation_log (
    id SERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    execution_date DATE NOT NULL,
    total_rows INTEGER DEFAULT 0,

    -- Duplicate metrics
    duplicate_count INTEGER DEFAULT 0,
    duplicate_pct DECIMAL(5,2) DEFAULT 0,

    -- Null metrics (critical columns)
    null_count INTEGER DEFAULT 0,
    null_pct DECIMAL(5,2) DEFAULT 0,
    null_details JSONB,

    -- Validation result
    validation_passed BOOLEAN DEFAULT FALSE,
    validation_errors JSONB,

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    promoted_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_validation_log_batch ON staging.validation_log(batch_id);
CREATE INDEX IF NOT EXISTS idx_validation_log_table ON staging.validation_log(table_name);
CREATE INDEX IF NOT EXISTS idx_validation_log_date ON staging.validation_log(execution_date);

COMMENT ON TABLE staging.validation_log IS 'Tracks data quality validation results for each staging load batch';

-- ============================================================================
-- STG_WEATHER_OBSERVATION - Staging for OpenWeather current observations
-- ============================================================================
CREATE TABLE IF NOT EXISTS staging.stg_weather_observation (
    id SERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,

    -- Business keys (no FK constraints in staging)
    city_name VARCHAR(100),
    city_id INTEGER,
    date_id INTEGER,
    observation_timestamp TIMESTAMP,

    -- Temperature (C)
    temperature DECIMAL(5,2),
    feels_like DECIMAL(5,2),
    temp_min DECIMAL(5,2),
    temp_max DECIMAL(5,2),

    -- Atmospheric
    pressure INTEGER,
    humidity INTEGER,
    visibility INTEGER,

    -- Wind
    wind_speed DECIMAL(5,2),
    wind_deg INTEGER,
    wind_gust DECIMAL(5,2),

    -- Clouds & Weather
    cloudiness INTEGER,
    weather_code INTEGER,
    weather_main VARCHAR(50),
    weather_description VARCHAR(100),

    -- Precipitation
    rain_1h DECIMAL(5,2),
    rain_3h DECIMAL(5,2),
    snow_1h DECIMAL(5,2),
    snow_3h DECIMAL(5,2),

    -- Source
    data_source VARCHAR(20) DEFAULT 'OpenWeather',

    -- Quality flags
    is_duplicate BOOLEAN DEFAULT FALSE,
    has_critical_nulls BOOLEAN DEFAULT FALSE,

    -- Metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_obs_batch ON staging.stg_weather_observation(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_obs_city ON staging.stg_weather_observation(city_name);

COMMENT ON TABLE staging.stg_weather_observation IS 'Staging table for OpenWeather current observations';

-- ============================================================================
-- STG_WEATHER_FORECAST - Staging for Open-Meteo daily forecasts
-- ============================================================================
CREATE TABLE IF NOT EXISTS staging.stg_weather_forecast (
    id SERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,

    -- Business keys
    city_name VARCHAR(100),
    city_id INTEGER,
    forecast_date DATE,
    forecast_date_id INTEGER,
    extraction_date_id INTEGER,

    -- Temperature
    temp_max DECIMAL(5,2),
    temp_min DECIMAL(5,2),
    apparent_temp_max DECIMAL(5,2),
    apparent_temp_min DECIMAL(5,2),

    -- Precipitation
    precipitation_sum DECIMAL(6,2),
    rain_sum DECIMAL(6,2),
    showers_sum DECIMAL(6,2),
    snowfall_sum DECIMAL(6,2),
    precipitation_hours INTEGER,

    -- Wind
    wind_speed_max DECIMAL(5,2),
    wind_gusts_max DECIMAL(5,2),
    wind_direction_dominant INTEGER,

    -- Solar
    sunrise TIME,
    sunset TIME,
    shortwave_radiation_sum DECIMAL(8,2),

    -- Weather Code
    weather_code INTEGER,

    -- Evapotranspiration
    et0_fao_evapotranspiration DECIMAL(6,2),

    -- Source
    data_source VARCHAR(20) DEFAULT 'Open-Meteo',

    -- Quality flags
    is_duplicate BOOLEAN DEFAULT FALSE,
    has_critical_nulls BOOLEAN DEFAULT FALSE,

    -- Metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_fcst_batch ON staging.stg_weather_forecast(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_fcst_city ON staging.stg_weather_forecast(city_name);

COMMENT ON TABLE staging.stg_weather_forecast IS 'Staging table for Open-Meteo daily forecasts';

-- ============================================================================
-- STG_WEATHER_FORECAST_HOURLY - Staging for Open-Meteo hourly forecasts
-- ============================================================================
CREATE TABLE IF NOT EXISTS staging.stg_weather_forecast_hourly (
    id SERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,

    -- Business keys
    city_name VARCHAR(100),
    city_id INTEGER,
    forecast_datetime TIMESTAMP,
    extraction_date_id INTEGER,

    -- Temperature at different heights
    temp_2m DECIMAL(5,2),
    temp_80m DECIMAL(5,2),
    apparent_temperature DECIMAL(5,2),

    -- Humidity & Dew Point
    relative_humidity_2m INTEGER,
    dew_point_2m DECIMAL(5,2),

    -- Precipitation
    precipitation_probability INTEGER,
    precipitation DECIMAL(6,2),
    rain DECIMAL(6,2),
    snowfall DECIMAL(6,2),

    -- Atmospheric Pressure
    pressure_msl DECIMAL(7,2),
    surface_pressure DECIMAL(7,2),

    -- Cloud Cover
    cloud_cover INTEGER,

    -- Visibility & UV
    visibility DECIMAL(8,2),
    uv_index DECIMAL(4,2),

    -- Wind
    wind_speed_10m DECIMAL(5,2),
    wind_direction_10m INTEGER,
    wind_gusts_10m DECIMAL(5,2),

    -- Weather Code
    weather_code INTEGER,

    -- Source
    data_source VARCHAR(20) DEFAULT 'Open-Meteo',

    -- Quality flags
    is_duplicate BOOLEAN DEFAULT FALSE,
    has_critical_nulls BOOLEAN DEFAULT FALSE,

    -- Metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_fcst_hr_batch ON staging.stg_weather_forecast_hourly(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_fcst_hr_city ON staging.stg_weather_forecast_hourly(city_name);

COMMENT ON TABLE staging.stg_weather_forecast_hourly IS 'Staging table for Open-Meteo hourly forecasts';

-- ============================================================================
-- STG_AIR_QUALITY - Staging for Open-Meteo air quality data
-- ============================================================================
CREATE TABLE IF NOT EXISTS staging.stg_air_quality (
    id SERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,

    -- Business keys
    city_name VARCHAR(100),
    city_id INTEGER,
    measurement_datetime TIMESTAMP,
    extraction_date_id INTEGER,

    -- Particulate Matter
    pm10 DECIMAL(8,2),
    pm2_5 DECIMAL(8,2),

    -- Gases
    carbon_monoxide DECIMAL(8,2),
    nitrogen_dioxide DECIMAL(8,2),
    sulphur_dioxide DECIMAL(8,2),
    ozone DECIMAL(8,2),

    -- Aerosols
    aerosol_optical_depth DECIMAL(6,4),
    dust DECIMAL(8,2),

    -- Source
    data_source VARCHAR(20) DEFAULT 'Open-Meteo',

    -- Quality flags
    is_duplicate BOOLEAN DEFAULT FALSE,
    has_critical_nulls BOOLEAN DEFAULT FALSE,

    -- Metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_aq_batch ON staging.stg_air_quality(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_aq_city ON staging.stg_air_quality(city_name);

COMMENT ON TABLE staging.stg_air_quality IS 'Staging table for Open-Meteo air quality measurements';

-- ============================================================================
-- STG_POLLEN - Staging for Open-Meteo pollen data
-- ============================================================================
CREATE TABLE IF NOT EXISTS staging.stg_pollen (
    id SERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,

    -- Business keys
    city_name VARCHAR(100),
    city_id INTEGER,
    measurement_datetime TIMESTAMP,
    extraction_date_id INTEGER,

    -- Pollen Counts
    alder_pollen INTEGER,
    birch_pollen INTEGER,
    grass_pollen INTEGER,
    mugwort_pollen INTEGER,
    olive_pollen INTEGER,
    ragweed_pollen INTEGER,

    -- Source
    data_source VARCHAR(20) DEFAULT 'Open-Meteo',

    -- Quality flags
    is_duplicate BOOLEAN DEFAULT FALSE,
    has_critical_nulls BOOLEAN DEFAULT FALSE,

    -- Metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_pollen_batch ON staging.stg_pollen(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_pollen_city ON staging.stg_pollen(city_name);

COMMENT ON TABLE staging.stg_pollen IS 'Staging table for Open-Meteo pollen measurements';

-- ============================================================================
-- STG_MARINE - Staging for Open-Meteo marine data (coastal cities)
-- ============================================================================
CREATE TABLE IF NOT EXISTS staging.stg_marine (
    id SERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,

    -- Business keys
    city_name VARCHAR(100),
    city_id INTEGER,
    forecast_date DATE,
    forecast_date_id INTEGER,
    extraction_date_id INTEGER,

    -- Wave data
    wave_height_max DECIMAL(5,2),
    wave_direction_dominant INTEGER,
    wave_period_max DECIMAL(5,2),
    wind_wave_height_max DECIMAL(5,2),
    swell_wave_height_max DECIMAL(5,2),

    -- Source
    data_source VARCHAR(20) DEFAULT 'Open-Meteo',

    -- Quality flags
    is_duplicate BOOLEAN DEFAULT FALSE,
    has_critical_nulls BOOLEAN DEFAULT FALSE,

    -- Metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_marine_batch ON staging.stg_marine(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_marine_city ON staging.stg_marine(city_name);

COMMENT ON TABLE staging.stg_marine IS 'Staging table for Open-Meteo marine forecasts (coastal cities)';

-- ============================================================================
-- STG_AEMET_DAILY_WEATHER - Staging for AEMET daily climatological data
-- ============================================================================
CREATE TABLE IF NOT EXISTS staging.stg_aemet_daily_weather (
    id SERIAL PRIMARY KEY,
    batch_id UUID NOT NULL,

    -- Business keys
    station_id_code VARCHAR(10),
    station_id INTEGER,
    date_value DATE,
    date_id INTEGER,
    extraction_date_id INTEGER,

    -- Temperature (C)
    temp_avg DECIMAL(5,2),
    temp_min DECIMAL(5,2),
    temp_max DECIMAL(5,2),

    -- Precipitation (mm)
    precipitation DECIMAL(6,2),

    -- Wind
    wind_speed_avg DECIMAL(5,2),
    wind_gust_max DECIMAL(5,2),
    wind_direction INTEGER,

    -- Solar
    sunshine_hours DECIMAL(4,2),

    -- Atmospheric Pressure (hPa)
    pressure_max DECIMAL(7,2),
    pressure_min DECIMAL(7,2),

    -- Humidity (%)
    humidity_avg DECIMAL(5,2),
    humidity_min DECIMAL(5,2),
    humidity_max DECIMAL(5,2),

    -- Source
    data_source VARCHAR(20) DEFAULT 'AEMET',

    -- Quality flags
    is_duplicate BOOLEAN DEFAULT FALSE,
    has_critical_nulls BOOLEAN DEFAULT FALSE,

    -- Metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_aemet_batch ON staging.stg_aemet_daily_weather(batch_id);
CREATE INDEX IF NOT EXISTS idx_stg_aemet_station ON staging.stg_aemet_daily_weather(station_id_code);

COMMENT ON TABLE staging.stg_aemet_daily_weather IS 'Staging table for AEMET daily climatological data';

-- ============================================================================
-- UTILITY FUNCTIONS
-- ============================================================================

-- Function to truncate all staging tables for a fresh load
CREATE OR REPLACE FUNCTION staging.truncate_staging_tables()
RETURNS void AS $$
BEGIN
    TRUNCATE TABLE staging.stg_weather_observation;
    TRUNCATE TABLE staging.stg_weather_forecast;
    TRUNCATE TABLE staging.stg_weather_forecast_hourly;
    TRUNCATE TABLE staging.stg_air_quality;
    TRUNCATE TABLE staging.stg_pollen;
    TRUNCATE TABLE staging.stg_marine;
    TRUNCATE TABLE staging.stg_aemet_daily_weather;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION staging.truncate_staging_tables() IS 'Truncates all staging tables for a fresh load';

-- Function to get validation summary for a batch
CREATE OR REPLACE FUNCTION staging.get_validation_summary(p_batch_id UUID)
RETURNS TABLE (
    table_name VARCHAR,
    total_rows INTEGER,
    duplicate_pct DECIMAL,
    null_pct DECIMAL,
    validation_passed BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        v.table_name,
        v.total_rows,
        v.duplicate_pct,
        v.null_pct,
        v.validation_passed
    FROM staging.validation_log v
    WHERE v.batch_id = p_batch_id
    ORDER BY v.table_name;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION staging.get_validation_summary(UUID) IS 'Returns validation summary for a specific batch';
