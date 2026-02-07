-- ============================================================================
-- FACT TABLES INITIALIZATION (Range Partitioned)
-- Data Warehouse Schema: dwh
-- Source: MinIO Silver Layer (Parquet files)
--
-- Partitioning strategy:
--   - 6 tables partitioned by extraction_date_id (YYYYMMDD integer)
--   - fct_weather_observation partitioned by date_id (no extraction_date_id)
--   - Monthly partitions from 2024-01 to 2027-12
--   - DEFAULT partition for out-of-range data
-- ============================================================================

-- ============================================================================
-- 1. PARENT TABLE DEFINITIONS
-- ============================================================================

-- FCT_WEATHER_OBSERVATION - Current Weather Observations (OpenWeather)
-- Partitioned by date_id (observation date ≈ extraction date)
CREATE TABLE IF NOT EXISTS dwh.fct_weather_observation (
    city_id INTEGER REFERENCES dwh.dim_city(city_id),
    date_id INTEGER NOT NULL,
    observation_timestamp TIMESTAMP NOT NULL,

    -- Temperature
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

    -- Metadata
    data_source VARCHAR(20) DEFAULT 'OpenWeather',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (city_id, date_id, observation_timestamp)
) PARTITION BY RANGE (date_id);

CREATE INDEX IF NOT EXISTS idx_fct_obs_city_date
    ON dwh.fct_weather_observation(city_id, date_id);

COMMENT ON TABLE dwh.fct_weather_observation
    IS 'Current weather observations from OpenWeather API — partitioned by date_id (monthly)';


-- FCT_WEATHER_FORECAST - Daily Weather Forecast (Open-Meteo)
CREATE TABLE IF NOT EXISTS dwh.fct_weather_forecast (
    city_id INTEGER REFERENCES dwh.dim_city(city_id),
    forecast_date_id INTEGER REFERENCES dwh.dim_date(id_calendar_day),
    extraction_date_id INTEGER NOT NULL,

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

    -- Metadata
    data_source VARCHAR(20) DEFAULT 'Open-Meteo',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (city_id, forecast_date_id, extraction_date_id)
) PARTITION BY RANGE (extraction_date_id);

CREATE INDEX IF NOT EXISTS idx_fct_fcst_city_date
    ON dwh.fct_weather_forecast(city_id, forecast_date_id);

COMMENT ON TABLE dwh.fct_weather_forecast
    IS 'Daily weather forecasts from Open-Meteo API — partitioned by extraction_date_id (monthly)';


-- FCT_WEATHER_FORECAST_HOURLY - Hourly Weather Forecast (Open-Meteo)
CREATE TABLE IF NOT EXISTS dwh.fct_weather_forecast_hourly (
    city_id INTEGER REFERENCES dwh.dim_city(city_id),
    forecast_datetime TIMESTAMP NOT NULL,
    extraction_date_id INTEGER NOT NULL,

    -- Temperature
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

    -- Metadata
    data_source VARCHAR(20) DEFAULT 'Open-Meteo',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (city_id, forecast_datetime, extraction_date_id)
) PARTITION BY RANGE (extraction_date_id);

CREATE INDEX IF NOT EXISTS idx_fct_fcst_hr_city_datetime
    ON dwh.fct_weather_forecast_hourly(city_id, forecast_datetime);

COMMENT ON TABLE dwh.fct_weather_forecast_hourly
    IS 'Hourly weather forecasts from Open-Meteo API — partitioned by extraction_date_id (monthly)';


-- FCT_AIR_QUALITY - Air Quality Measurements (Open-Meteo)
CREATE TABLE IF NOT EXISTS dwh.fct_air_quality (
    city_id INTEGER REFERENCES dwh.dim_city(city_id),
    measurement_datetime TIMESTAMP NOT NULL,
    extraction_date_id INTEGER NOT NULL,

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

    -- Metadata
    data_source VARCHAR(20) DEFAULT 'Open-Meteo',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (city_id, measurement_datetime, extraction_date_id)
) PARTITION BY RANGE (extraction_date_id);

CREATE INDEX IF NOT EXISTS idx_fct_aq_city_datetime
    ON dwh.fct_air_quality(city_id, measurement_datetime);

COMMENT ON TABLE dwh.fct_air_quality
    IS 'Air quality measurements from Open-Meteo API — partitioned by extraction_date_id (monthly)';


-- FCT_POLLEN - Pollen Levels (Open-Meteo)
CREATE TABLE IF NOT EXISTS dwh.fct_pollen (
    city_id INTEGER REFERENCES dwh.dim_city(city_id),
    measurement_datetime TIMESTAMP NOT NULL,
    extraction_date_id INTEGER NOT NULL,

    -- Pollen Counts
    alder_pollen INTEGER,
    birch_pollen INTEGER,
    grass_pollen INTEGER,
    mugwort_pollen INTEGER,
    olive_pollen INTEGER,
    ragweed_pollen INTEGER,

    -- Metadata
    data_source VARCHAR(20) DEFAULT 'Open-Meteo',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (city_id, measurement_datetime, extraction_date_id)
) PARTITION BY RANGE (extraction_date_id);

CREATE INDEX IF NOT EXISTS idx_fct_pollen_city_datetime
    ON dwh.fct_pollen(city_id, measurement_datetime);

COMMENT ON TABLE dwh.fct_pollen
    IS 'Pollen level measurements from Open-Meteo API — partitioned by extraction_date_id (monthly)';


-- FCT_MARINE - Marine Weather (Open-Meteo)
CREATE TABLE IF NOT EXISTS dwh.fct_marine (
    city_id INTEGER REFERENCES dwh.dim_city(city_id),
    forecast_date_id INTEGER,
    extraction_date_id INTEGER NOT NULL,

    -- Attributes
    wave_height_max DECIMAL(5,2),
    wave_direction_dominant INTEGER,
    wave_period_max DECIMAL(5,2),
    wind_wave_height_max DECIMAL(5,2),
    swell_wave_height_max DECIMAL(5,2),

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (city_id, forecast_date_id, extraction_date_id)
) PARTITION BY RANGE (extraction_date_id);

COMMENT ON TABLE dwh.fct_marine
    IS 'Marine weather forecasts from Open-Meteo API (Coastal Cities) — partitioned by extraction_date_id (monthly)';


-- FCT_AEMET_DAILY_WEATHER - AEMET Daily Climatological Data
CREATE TABLE IF NOT EXISTS dwh.fct_aemet_daily_weather (
    station_id INTEGER REFERENCES dwh.dim_aemet_stations(id),
    date_id INTEGER REFERENCES dwh.dim_date(id_calendar_day),
    extraction_date_id INTEGER NOT NULL,

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

    -- Metadata
    data_source VARCHAR(20) DEFAULT 'AEMET',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (station_id, date_id, extraction_date_id)
) PARTITION BY RANGE (extraction_date_id);

CREATE INDEX IF NOT EXISTS idx_fct_aemet_station_date
    ON dwh.fct_aemet_daily_weather(station_id, date_id);
CREATE INDEX IF NOT EXISTS idx_fct_aemet_date
    ON dwh.fct_aemet_daily_weather(date_id);
CREATE INDEX IF NOT EXISTS idx_fct_aemet_extraction
    ON dwh.fct_aemet_daily_weather(extraction_date_id);

COMMENT ON TABLE dwh.fct_aemet_daily_weather
    IS 'Daily climatological data from AEMET — partitioned by extraction_date_id (monthly)';


-- ============================================================================
-- 2. MONTHLY PARTITIONS (2024-01 .. 2027-12) + DEFAULT
-- ============================================================================
-- Partitions use YYYYMMDD integer boundaries:
--   FROM (20240101) TO (20240201) covers all January 2024 dates.
-- ============================================================================

DO $$
DECLARE
    tbl   TEXT;
    y     INT;
    m     INT;
    lo    INT;   -- lower bound (inclusive)
    hi    INT;   -- upper bound (exclusive)
    pname TEXT;
BEGIN
    -- All fact tables that are partitioned
    FOREACH tbl IN ARRAY ARRAY[
        'dwh.fct_weather_observation',
        'dwh.fct_weather_forecast',
        'dwh.fct_weather_forecast_hourly',
        'dwh.fct_air_quality',
        'dwh.fct_pollen',
        'dwh.fct_marine',
        'dwh.fct_aemet_daily_weather'
    ] LOOP
        -- Create monthly partitions: 2024-01 to 2027-12
        FOR y IN 2024..2027 LOOP
            FOR m IN 1..12 LOOP
                lo := y * 10000 + m * 100 + 1;

                IF m = 12 THEN
                    hi := (y + 1) * 10000 + 101;    -- next year, January 1st
                ELSE
                    hi := y * 10000 + (m + 1) * 100 + 1;
                END IF;

                -- Partition name: dwh.fct_xxx → fct_xxx_p202401
                pname := tbl || '_p' || y::TEXT || LPAD(m::TEXT, 2, '0');

                EXECUTE format(
                    'CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM (%s) TO (%s)',
                    pname, tbl, lo, hi
                );
            END LOOP;
        END LOOP;

        -- DEFAULT partition for out-of-range data
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %s PARTITION OF %s DEFAULT',
            tbl || '_default', tbl
        );
    END LOOP;
END $$;


-- ============================================================================
-- 3. UTILITY FUNCTION: Create new monthly partitions on demand
-- ============================================================================

CREATE OR REPLACE FUNCTION dwh.create_monthly_partition(
    p_table_name TEXT,    -- e.g. 'dwh.fct_weather_forecast'
    p_year       INT,
    p_month      INT
) RETURNS TEXT AS $$
DECLARE
    lo    INT;
    hi    INT;
    pname TEXT;
BEGIN
    lo := p_year * 10000 + p_month * 100 + 1;

    IF p_month = 12 THEN
        hi := (p_year + 1) * 10000 + 101;
    ELSE
        hi := p_year * 10000 + (p_month + 1) * 100 + 1;
    END IF;

    pname := p_table_name || '_p' || p_year::TEXT || LPAD(p_month::TEXT, 2, '0');

    -- Detach from default first (data will be re-routed)
    BEGIN
        EXECUTE format(
            'ALTER TABLE %s DETACH PARTITION %s',
            p_table_name, p_table_name || '_default'
        );
    EXCEPTION WHEN OTHERS THEN
        NULL; -- default partition may not exist
    END;

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM (%s) TO (%s)',
        pname, p_table_name, lo, hi
    );

    -- Re-attach default partition
    BEGIN
        EXECUTE format(
            'ALTER TABLE %s ATTACH PARTITION %s DEFAULT',
            p_table_name, p_table_name || '_default'
        );
    EXCEPTION WHEN OTHERS THEN
        NULL;
    END;

    RETURN pname;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION dwh.create_monthly_partition(TEXT, INT, INT)
    IS 'Create a new monthly partition for a fact table. Handles detach/re-attach of DEFAULT partition.';
