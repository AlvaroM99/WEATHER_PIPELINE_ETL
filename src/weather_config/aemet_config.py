"""
AEMET API Configuration
Spanish State Meteorological Agency (Agencia Estatal de Meteorología)

Credentials are managed through SecretsManager which prioritizes
Airflow Connections when available, with fallback to environment variables.
"""

import os

try:
    from src.weather_config.secrets_manager import get_aemet_api_key
except ImportError:
    from weather_config.secrets_manager import get_aemet_api_key  # type: ignore[no-redef]

# API Key (via SecretsManager)
_api_key = None


def get_api_key():
    """Get AEMET API key with lazy loading."""
    global _api_key
    if _api_key is None:
        _api_key = get_aemet_api_key()
        if not _api_key:
            print("WARNING: AEMET_API_KEY not found in Airflow Connections or environment.")
    return _api_key


# Legacy: Direct access for backward compatibility
AEMET_API_KEY = os.getenv("AEMET_API_KEY")

# Base URL
AEMET_BASE_URL = "https://opendata.aemet.es/opendata/api"

# Common endpoints
ENDPOINTS = {
    "stations": "/valores/climatologicos/inventarioestaciones/todasestaciones",
    "daily_climatology": "/valores/climatologicos/diarios/datos/fechaini/{start}/fechafin/{end}/estacion/{station}",
    "current_observation": "/observacion/convencional/datos/estacion/{station}",
    "forecast_municipality": "/prediccion/especifica/municipio/diaria/{municipality_code}",
    "forecast_hourly": "/prediccion/especifica/municipio/horaria/{municipality_code}",
    "warnings": "/avisos/ultimoelaborado",
}

# Request timeout (seconds)
REQUEST_TIMEOUT = 30

# Retry configuration
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 0.5
