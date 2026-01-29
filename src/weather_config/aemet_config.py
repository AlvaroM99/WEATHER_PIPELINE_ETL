"""
AEMET API Configuration
Spanish State Meteorological Agency (Agencia Estatal de Meteorología)
"""

import os

from dotenv import load_dotenv

load_dotenv()

# API Key (required)
AEMET_API_KEY = os.getenv("AEMET_API_KEY")
if not AEMET_API_KEY:
    print("WARNING: AEMET_API_KEY not found in environment variables.")

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
