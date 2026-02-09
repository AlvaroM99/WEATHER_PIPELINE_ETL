-- Initialize the weather database schema
-- This script runs automatically when PostgreSQL container starts

-- Create weather table to store current weather data
CREATE TABLE IF NOT EXISTS weather (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    country VARCHAR(10),
    latitude FLOAT,
    longitude FLOAT,
    temperature FLOAT NOT NULL,
    feels_like FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    pressure INTEGER,
    humidity INTEGER NOT NULL,
    weather_main VARCHAR(50),
    weather_description TEXT NOT NULL,
    wind_speed FLOAT,
    wind_deg INTEGER,
    clouds INTEGER,
    visibility INTEGER,
    date DATE NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(city, date)
);

-- Create index for faster queries
CREATE INDEX IF NOT EXISTS idx_weather_city ON weather(city);
CREATE INDEX IF NOT EXISTS idx_weather_date ON weather(date);
CREATE INDEX IF NOT EXISTS idx_weather_city_date ON weather(city, date);

-- Insert a sample comment
COMMENT ON TABLE weather IS 'Stores daily weather data from OpenWeatherMap API';

-- Table to track data lineage from lake to DWH
CREATE TABLE IF NOT EXISTS lake_metadata (
    id SERIAL PRIMARY KEY,
    bucket_name VARCHAR(100) NOT NULL,
    object_path TEXT NOT NULL,
    layer VARCHAR(20) NOT NULL,  -- 'bronze' or 'silver'
    data_source VARCHAR(50) NOT NULL,  -- 'openweather', 'openmeteo', 'aemet'
    record_count INTEGER,
    file_size_bytes BIGINT,
    status VARCHAR(20) DEFAULT 'success',
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(object_path)
);

-- Create indexes for faster queries on metadata
CREATE INDEX IF NOT EXISTS idx_lake_metadata_layer ON lake_metadata(layer);
CREATE INDEX IF NOT EXISTS idx_lake_metadata_data_source ON lake_metadata(data_source);
CREATE INDEX IF NOT EXISTS idx_lake_metadata_timestamp ON lake_metadata(load_timestamp);
CREATE INDEX IF NOT EXISTS idx_lake_metadata_status ON lake_metadata(status);

COMMENT ON TABLE lake_metadata IS 'Tracks data lineage and metadata for files in the data lake (Bronze and Silver layers)';

-- ============================================================================
-- Main Initialization Script
-- ============================================================================
-- This script initializes the weather data warehouse schema and tables
-- Execute order: init.sql → init-dimensional-tables.sql → init-fact-tables.sql → init-etl-audit.sql

\echo 'Starting database initialization...'

\echo 'Loading dimensional tables...'
\i /docker-entrypoint-initdb.d/init-dimensional-tables.sql

\echo 'Loading fact tables...'
\i /docker-entrypoint-initdb.d/init-fact-tables.sql

\echo 'Loading ETL audit tables...'
\i /docker-entrypoint-initdb.d/init-etl-audit.sql

\echo 'Database initialization complete!'
