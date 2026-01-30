"""
OpenWeatherMap API Configuration

Credentials are managed through SecretsManager which prioritizes
Airflow Connections when available, with fallback to environment variables.
"""

import os
from typing import Optional

try:
    from src.config.secrets_manager import get_openweather_api_key
except ImportError:
    from config.secrets_manager import get_openweather_api_key  # type: ignore[no-redef]

# API Key (via SecretsManager)
_api_key: Optional[str] = None


def get_api_key() -> Optional[str]:
    """Get OpenWeather API key with lazy loading."""
    global _api_key
    if _api_key is None:
        _api_key = get_openweather_api_key()
    return _api_key


# Legacy: Direct access for backward compatibility
API_KEY = os.getenv("OPENWEATHER_API_KEY")

# API Base URL
OPENWEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5"

# Request timeout (seconds)
REQUEST_TIMEOUT = 30
