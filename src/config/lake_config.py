"""
Data Lake Configuration
Defines bucket structure and paths for medallion architecture
Supports multiple API data sources: OpenWeather, Open-Meteo, AEMET

Credentials are managed through SecretsManager which prioritizes
Airflow Connections when available, with fallback to environment variables.
"""

try:
    from src.config.secrets_manager import get_minio_credentials
except ImportError:
    from config.secrets_manager import get_minio_credentials  # type: ignore[no-redef]


def get_minio_connection() -> dict:
    """
    Get MinIO connection configuration.

    Returns dict with: endpoint, access_key, secret_key, secure
    Uses SecretsManager for credential retrieval.
    """
    creds = get_minio_credentials()
    return {
        "endpoint": creds.endpoint,
        "access_key": creds.access_key,
        "secret_key": creds.secret_key,
        "secure": creds.secure,
    }


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
AEMET_STATIONS_PATH = "stations/{date}/stations_{timestamp}.json"
AEMET_DAILY_CLIMATOLOGY_PATH = "climatology/daily/{date}/daily_{station}_{timestamp}.json"
AEMET_HISTORICAL_PATH = "historical/{year}/{station}/historical_{start}_{end}_{timestamp}.json"
