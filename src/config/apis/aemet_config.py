"""
AEMET API Configuration
Spanish State Meteorological Agency (Agencia Estatal de Meteorología)

Credentials are managed through SecretsManager which prioritizes
Airflow Connections when available, with fallback to environment variables.
"""

import logging
from typing import Optional

try:
    from src.config.secrets_manager import get_aemet_api_key
except ImportError:
    from config.secrets_manager import get_aemet_api_key  # type: ignore[no-redef]

logger = logging.getLogger(__name__)


def get_api_key() -> Optional[str]:
    """Get AEMET API key via SecretsManager."""
    key = get_aemet_api_key()
    if not key:
        logger.warning("AEMET_API_KEY not found in Airflow Connections or environment.")
    return key

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

# ============================================================================
# Default AEMET Stations (Spanish Capital Cities)
# ============================================================================

# Station IDs for extraction (subset used in daily extractions)
DEFAULT_STATION_IDS = [
    "3129",  # Madrid (Retiro)
    "0076",  # Barcelona (Fabra)
    "5530E",  # Sevilla (Aeropuerto)
    "8416",  # Valencia (Aeropuerto)
    "1024E",  # Bilbao (Aeropuerto)
    "6155A",  # Malaga (Aeropuerto)
    "1387",  # Zaragoza (Aeropuerto)
    "8178D",  # Alicante (Aeropuerto)
]

# Full station metadata for dimensional loading
# Format: (station_id, station_name, province, altitude, latitude, longitude)
DEFAULT_STATIONS = [
    ("3129", "MADRID, RETIRO", "MADRID", 667.0, 40.4115, -3.6784),
    ("0076", "BARCELONA, FABRA", "BARCELONA", 412.0, 41.4181, 2.1246),
    ("5530E", "SEVILLA, AEROPUERTO", "SEVILLA", 34.0, 37.4167, -5.8833),
    ("8416", "VALENCIA, AEROPUERTO", "VALENCIA", 69.0, 39.4833, -0.4833),
    ("1024E", "BILBAO, AEROPUERTO", "VIZCAYA", 42.0, 43.3000, -2.9167),
    ("6155A", "MALAGA, AEROPUERTO", "MALAGA", 5.0, 36.6667, -4.4833),
    ("1387", "ZARAGOZA, AEROPUERTO", "ZARAGOZA", 247.0, 41.6617, -1.0042),
    ("8178D", "ALICANTE, AEROPUERTO", "ALICANTE", 43.0, 38.2833, -0.5500),
    ("1111X", "SANTANDER, CMT", "CANTABRIA", 64.0, 43.4917, -3.7992),
    ("2539", "VALLADOLID", "VALLADOLID", 735.0, 41.6528, -4.7617),
    ("C447A", "PALMA DE MALLORCA", "ILLES BALEARS", 8.0, 39.5592, 2.7386),
    ("9434", "MURCIA, ALCANTARILLA", "MURCIA", 75.0, 37.9589, -1.2306),
    ("6001", "GRANADA, AEROPUERTO", "GRANADA", 567.0, 37.1867, -3.7772),
    ("9091O", "CORDOBA, AEROPUERTO", "CORDOBA", 90.0, 37.8417, -4.8500),
    ("9170", "TOLEDO", "TOLEDO", 515.0, 39.8817, -4.0489),
]
