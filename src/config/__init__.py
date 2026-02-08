"""
Configuration Package

Centralized configuration management for the Weather Pipeline ETL.

Structure:
---------
config/
├── __init__.py          # This file
├── apis/                # API configurations (one per data source)
│   ├── aemet_config.py      # AEMET (Spanish meteorological service)
│   ├── openmeteo_config.py  # Open-Meteo (forecast, air quality, marine)
│   └── openweather_config.py # OpenWeatherMap (current weather)
├── database_config.py   # PostgreSQL connection settings
├── lake_config.py       # MinIO/Data Lake bucket and path settings
└── secrets_manager.py   # Credential retrieval (Airflow + env vars)

Usage:
------
    # API configurations
    from src.config.apis.aemet_config import DEFAULT_STATION_IDS, get_api_key
    from src.config.apis.openmeteo_config import DAILY_FORECAST_PARAMS
    from src.config.apis.openweather_config import get_api_key

    # Database configuration (via SecretsManager)
    from src.config.database_config import get_postgres_config

    # Data Lake configuration
    from src.config.lake_config import BRONZE_BUCKET, get_minio_connection
"""
