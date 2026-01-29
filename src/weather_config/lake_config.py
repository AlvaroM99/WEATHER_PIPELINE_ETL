"""
Data Lake Configuration
Defines bucket structure and paths for medallion architecture
Supports multiple API data sources: OpenWeather, Open-Meteo, AEMET
"""

import os

# MinIO Connection
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "MinIO2024!Secure")

# ============================================================================
# BRONZE LAYER - Raw data from each API source
# ============================================================================
BRONZE_OPENWEATHER_BUCKET = "bronze-openweather"  # Current weather API
BRONZE_OPENMETEO_BUCKET = "bronze-openmeteo"  # Forecast, air quality, pollen, marine
BRONZE_AEMET_BUCKET = "bronze-aemet"  # Spanish meteorological service

# Legacy bucket name (keep for backward compatibility, maps to OpenWeather)
BRONZE_BUCKET = BRONZE_OPENWEATHER_BUCKET

# ============================================================================
# SILVER LAYER - Processed/transformed data
# ============================================================================
SILVER_OPENWEATHER_BUCKET = "silver-openweather"
SILVER_OPENMETEO_BUCKET = "silver-openmeteo"
SILVER_AEMET_BUCKET = "silver-aemet"

# Legacy bucket name (keep for backward compatibility)
SILVER_BUCKET = SILVER_OPENWEATHER_BUCKET

# ============================================================================
# PATH TEMPLATES - Organized by API source and data type
# ============================================================================

# OpenWeather (existing current weather API)
OPENWEATHER_CURRENT_PATH = "current/{date}/weather_{city}_{timestamp}.json"
BRONZE_PATH_TEMPLATE = OPENWEATHER_CURRENT_PATH  # Legacy compatibility
SILVER_PATH_TEMPLATE = "processed/{date}/weather_{date}.parquet"

# Open-Meteo (forecast and environmental data)
OPENMETEO_DAILY_PATH = "forecast/daily/{date}/weather_daily_{timestamp}.json"
OPENMETEO_HOURLY_PATH = "forecast/hourly/{date}/weather_hourly_{timestamp}.json"
OPENMETEO_AIR_QUALITY_PATH = "air_quality/{date}/air_quality_{timestamp}.json"
OPENMETEO_POLLEN_PATH = "pollen/{date}/pollen_{timestamp}.json"
OPENMETEO_MARINE_PATH = "marine/{date}/marine_{timestamp}.json"

# AEMET (Spanish meteorological service)
AEMET_PATH_TEMPLATE = "aemet/{endpoint}/{date}/{timestamp}.json"
