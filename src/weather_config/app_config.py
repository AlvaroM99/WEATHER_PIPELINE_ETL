"""
Application Configuration
Shared settings/constants for the weather pipeline

Credentials are managed through SecretsManager which prioritizes
Airflow Connections when available, with fallback to environment variables.
"""

import os
from typing import Optional

from weather_config.secrets_manager import (
    get_openweather_api_key,
    get_postgres_credentials,
)

# OpenWeatherMap API Key (via SecretsManager)
_api_key: Optional[str] = None


def get_api_key() -> Optional[str]:
    """Get OpenWeather API key with lazy loading."""
    global _api_key
    if _api_key is None:
        _api_key = get_openweather_api_key()
    return _api_key


# Legacy: Direct access for backward compatibility
API_KEY = os.getenv("OPENWEATHER_API_KEY")

# PostgreSQL Configuration (via SecretsManager)
_pg_creds = None


def _get_pg_creds():
    """Get PostgreSQL credentials with lazy loading."""
    global _pg_creds
    if _pg_creds is None:
        _pg_creds = get_postgres_credentials()
    return _pg_creds


# Legacy: Direct access for backward compatibility
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")


def get_postgres_config() -> dict:
    """
    Get PostgreSQL configuration dictionary.

    Returns dict with: host, port, database, user, password
    Uses SecretsManager for credential retrieval.
    """
    creds = _get_pg_creds()
    return {
        "host": creds.host,
        "port": creds.port,
        "database": creds.database,
        "user": creds.user,
        "password": creds.password,
    }


# Cities to fetch weather data for (major Spanish cities)
CITIES = [
    {"name": "Madrid", "lat": 40.4168, "lon": -3.7038},
    {"name": "Barcelona", "lat": 41.3851, "lon": 2.1734},
    {"name": "Valencia", "lat": 39.4699, "lon": -0.3763},
    {"name": "Sevilla", "lat": 37.3891, "lon": -5.9845},
    {"name": "Bilbao", "lat": 43.2630, "lon": -2.9350},
    {"name": "Málaga", "lat": 36.7213, "lon": -4.4214},
    {"name": "Zaragoza", "lat": 41.6488, "lon": -0.8891},
]
