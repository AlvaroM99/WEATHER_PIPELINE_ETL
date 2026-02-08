"""
API Configurations Package

Weather data API configurations for the ETL pipeline.

Available APIs:
--------------
    - aemet_config: Spanish State Meteorological Agency (AEMET)
    - openmeteo_config: Open-Meteo (forecast, air quality, pollen, marine)
    - openweather_config: OpenWeatherMap (current weather)

Usage:
------
    from src.config.apis.aemet_config import AEMET_BASE_URL, DEFAULT_STATION_IDS
    from src.config.apis.openmeteo_config import DAILY_FORECAST_PARAMS
    from src.config.apis.openweather_config import get_api_key
"""
