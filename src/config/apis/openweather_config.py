"""
OpenWeatherMap API Configuration

Credentials are managed through SecretsManager which prioritizes
Airflow Connections when available, with fallback to environment variables.
"""

from typing import Optional

try:
    from src.config.secrets_manager import get_openweather_api_key
except ImportError:
    from config.secrets_manager import get_openweather_api_key  # type: ignore[no-redef]


def get_api_key() -> Optional[str]:
    """Get OpenWeather API key via SecretsManager."""
    return get_openweather_api_key()


# API Base URL
OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5"

# Request timeout (seconds)
REQUEST_TIMEOUT = 30
