-- ============================================================================
-- FACT TABLES INITIALIZATION
-- Data Warehouse Schema: dwh
-- Source: MinIO Silver Layer (Parquet files)
-- ============================================================================

-- ============================================================================
-- FCT_WEATHER_OBSERVATION - Current Weather Observations (OpenWeather)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.fct_weather_observation (
    -- Foreign Keys to Dimensions
    city_id INTEGER REFERENCES dwh.dim_city(city_id),
    date_id INTEGER REFERENCES dwh.dim_date(id_calendar_day),
    
    -- Timestamp
    observation_timestamp TIMESTAMP NOT NULL,
    
    -- Temperature (°C)
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
    
    PRIMARY KEY (city_id, date_id, observation_timestamp, created_at)
);

CREATE INDEX IF NOT EXISTS idx_fct_obs_city_date ON dwh.fct_weather_observation(city_id, date_id);

COMMENT ON TABLE dwh.fct_weather_observation IS 'Current weather observations from OpenWeather API';

-- ============================================================================
-- FCT_WEATHER_FORECAST - Daily Weather Forecast (Open-Meteo)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.fct_weather_forecast (
    -- Foreign Keys
    city_id INTEGER REFERENCES dwh.dim_city(city_id),
    forecast_date_id INTEGER REFERENCES dwh.dim_date(id_calendar_day),
    extraction_date_id INTEGER REFERENCES dwh.dim_date(id_calendar_day),
    
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
    
    PRIMARY KEY (city_id, forecast_date_id, extraction_date_id, created_at)
);

CREATE INDEX IF NOT EXISTS idx_fct_fcst_city_date ON dwh.fct_weather_forecast(city_id, forecast_date_id);

COMMENT ON TABLE dwh.fct_weather_forecast IS 'Daily weather forecasts from Open-Meteo API';

-- ============================================================================
-- FCT_WEATHER_FORECAST_HOURLY - Hourly Weather Forecast (Open-Meteo)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.fct_weather_forecast_hourly (
    -- Foreign Keys
    city_id INTEGER REFERENCES dwh.dim_city(city_id),
    forecast_datetime TIMESTAMP NOT NULL,
    extraction_date_id INTEGER REFERENCES dwh.dim_date(id_calendar_day),
    
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
    
    -- Metadata
    data_source VARCHAR(20) DEFAULT 'Open-Meteo',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (city_id, forecast_datetime, extraction_date_id, created_at)
);

CREATE INDEX IF NOT EXISTS idx_fct_fcst_hr_city_datetime ON dwh.fct_weather_forecast_hourly(city_id, forecast_datetime);

COMMENT ON TABLE dwh.fct_weather_forecast_hourly IS 'Hourly weather forecasts from Open-Meteo API';

-- ============================================================================
-- FCT_AIR_QUALITY - Air Quality Measurements (Open-Meteo)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.fct_air_quality (
    -- Foreign Keys
    city_id INTEGER REFERENCES dwh.dim_city(city_id),
    measurement_datetime TIMESTAMP NOT NULL,
    extraction_date_id INTEGER REFERENCES dwh.dim_date(id_calendar_day),
    
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
    
    PRIMARY KEY (city_id, measurement_datetime, extraction_date_id, created_at)
);

CREATE INDEX IF NOT EXISTS idx_fct_aq_city_datetime ON dwh.fct_air_quality(city_id, measurement_datetime);

COMMENT ON TABLE dwh.fct_air_quality IS 'Air quality measurements from Open-Meteo API';

-- ============================================================================
-- FCT_POLLEN - Pollen Levels (Open-Meteo)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.fct_pollen (
    -- Foreign Keys
    city_id INTEGER REFERENCES dwh.dim_city(city_id),
    measurement_datetime TIMESTAMP NOT NULL,
    extraction_date_id INTEGER REFERENCES dwh.dim_date(id_calendar_day),
    
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
    
    PRIMARY KEY (city_id, measurement_datetime, extraction_date_id, created_at)
);

CREATE INDEX IF NOT EXISTS idx_fct_pollen_city_datetime ON dwh.fct_pollen(city_id, measurement_datetime);

COMMENT ON TABLE dwh.fct_pollen IS 'Pollen level measurements from Open-Meteo API';

-- ============================================================================
-- FCT_MARINE - Marine Weather (Open-Meteo)
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.fct_marine (
    -- Foreign Keys
    city_id INTEGER REFERENCES dwh.dim_city(city_id),
    forecast_date_id INTEGER,
    extraction_date_id INTEGER,
    
    -- Attributes
    wave_height_max DECIMAL(5,2),
    wave_direction_dominant INTEGER,
    wave_period_max DECIMAL(5,2),
    wind_wave_height_max DECIMAL(5,2),
    swell_wave_height_max DECIMAL(5,2),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (city_id, forecast_date_id, extraction_date_id, created_at)
);

COMMENT ON TABLE dwh.fct_marine IS 'Marine weather forecasts from Open-Meteo API (Coastal Cities)';

-- ============================================================================
-- FCT_AEMET_DAILY_WEATHER - AEMET Daily Climatological Data
-- Historical and current daily weather observations from AEMET stations
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.fct_aemet_daily_weather (
    -- Foreign Keys
    station_id INTEGER REFERENCES dwh.dim_aemet_stations(id),
    date_id INTEGER REFERENCES dwh.dim_date(id_calendar_day),
    extraction_date_id INTEGER REFERENCES dwh.dim_date(id_calendar_day),

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
);

CREATE INDEX IF NOT EXISTS idx_fct_aemet_station_date ON dwh.fct_aemet_daily_weather(station_id, date_id);
CREATE INDEX IF NOT EXISTS idx_fct_aemet_date ON dwh.fct_aemet_daily_weather(date_id);
CREATE INDEX IF NOT EXISTS idx_fct_aemet_extraction ON dwh.fct_aemet_daily_weather(extraction_date_id);

COMMENT ON TABLE dwh.fct_aemet_daily_weather IS 'Daily climatological data from AEMET (Spanish Meteorological Agency) - Historical 2020-2025 and current observations';
