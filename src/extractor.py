"""
Unified Extractor
Consolidates all extraction logic into a single class.
"""
import logging
import requests
import time
import urllib3
from datetime import datetime
from abc import ABC

# Import configuration and utils
from src.weather_config.app_config import API_KEY
from src.weather_config.lake_config import (
    BRONZE_BUCKET, BRONZE_PATH_TEMPLATE,
    BRONZE_OPENMETEO_BUCKET
)
from src.weather_config.openmeteo_config import (
    OPENMETEO_FORECAST_URL, OPENMETEO_AIR_QUALITY_URL,
    OPENMETEO_MARINE_URL, OPENMETEO_POLLEN_URL,
    OPENMETEO_API_KEY,
    DAILY_FORECAST_PARAMS, HOURLY_FORECAST_PARAMS,
    AIR_QUALITY_PARAMS, MARINE_PARAMS, POLLEN_PARAMS,
    DEFAULT_TIMEZONE
)
from src.weather_utils.city_utils import get_capitals_dataframe, get_cities
from src.weather_utils.http_utils import get_retrying_session
from src.weather_utils.minio_client import MinIOClient

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class Extractor:
    """
    Unified Extractor Manager.
    Handles extraction for OpenWeatherMap and Open-Meteo services.
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.minio_client = MinIOClient()
        self.session = get_retrying_session()

    def log_start(self, msg: str):
        self.logger.info(f"🚀 START: {msg}")

    def log_end(self, msg: str):
        self.logger.info(f"🏁 END: {msg}")

    def log_error(self, msg: str, error: Exception = None):
        if error:
            self.logger.error(f"❌ ERROR: {msg} - {str(error)}")
        else:
            self.logger.error(f"❌ ERROR: {msg}")

    # ========================================================================
    # OpenWeatherMap Extraction
    # ========================================================================

    def extract_openweather(self, **context):
        """Extract current weather from OpenWeatherMap"""
        # Load cities from GitHub CSV
        cities = get_cities()
        self.log_start(f"OpenWeatherMap extraction for {len(cities)} cities")

        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        uploaded_objects = []

        for city in cities:
            try:
                url = "https://api.openweathermap.org/data/2.5/weather"
                params = {
                    'lat': city['lat'], 'lon': city['lon'],
                    'appid': API_KEY, 'units': 'metric'
                }
                
                self.logger.info(f"Fetching weather data for {city['name']}")
                response = self.session.get(url, params=params, timeout=10)
                response.raise_for_status()
                raw_data = response.json()
                
                raw_data['_metadata'] = {
                    'city_name': city['name'],
                    'extraction_timestamp': timestamp,
                    'execution_date': execution_date
                }
                
                object_path = BRONZE_PATH_TEMPLATE.format(
                    date=execution_date,
                    city=city['name'].lower(),
                    timestamp=timestamp
                )
                
                file_size = self.minio_client.upload_json(BRONZE_BUCKET, object_path, raw_data)
                
                uploaded_objects.append({
                    'object_path': object_path,
                    'city': city['name'],
                    'file_size': file_size
                })
                self.logger.info(f"✅ Stored raw data for {city['name']}")
                
            except Exception as e:
                self.log_error(f"Error processing {city['name']}", e)
                continue
        
        self.log_end(f"Stored {len(uploaded_objects)} OpenWeather files")
        
        if context.get('task_instance'):
            context['task_instance'].xcom_push(key='bronze_objects', value=uploaded_objects)
            context['task_instance'].xcom_push(key='execution_date', value=execution_date)
            
        return len(uploaded_objects)

    # ========================================================================
    # Open-Meteo Daily Extraction
    # ========================================================================

    def extract_openmeteo_daily(self, **context):
        """Extract 7-day daily forecast from Open-Meteo"""
        self.log_start("Open-Meteo DAILY forecast extraction")
        
        capitals_df = get_capitals_dataframe()
        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        uploaded_objects = []

        for _, city in capitals_df.iterrows():
            params = {
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "daily": ",".join(DAILY_FORECAST_PARAMS),
                "timezone": DEFAULT_TIMEZONE
            }
            if OPENMETEO_API_KEY: params["apikey"] = OPENMETEO_API_KEY

            try:
                self.logger.info(f"Fetching DAILY forecast for {city['municipio_nombre']}")
                response = self.session.get(OPENMETEO_FORECAST_URL, params=params, verify=False, timeout=60)
                response.raise_for_status()
                data = response.json()
                
                data["city_code"] = city["city_code"]
                data["municipio_nombre"] = city["municipio_nombre"]
                data["_metadata"] = {"extraction_timestamp": timestamp, "execution_date": execution_date}
                
                object_path = f"forecast/daily/{execution_date}/weather_daily_{city['municipio_nombre'].lower()}_{timestamp}.json"
                file_size = self.minio_client.upload_json(BRONZE_OPENMETEO_BUCKET, object_path, data)
                
                uploaded_objects.append({'object_path': object_path, 'city': city['municipio_nombre']})
                self.logger.info(f"✅ Stored DAILY forecast for {city['municipio_nombre']}")
                time.sleep(0.1)
                
            except Exception as e:
                self.log_error(f"Error fetching DAILY forecast for {city['municipio_nombre']}", e)
                continue

        self.log_end(f"Stored {len(uploaded_objects)} DAILY forecast files")
        if context.get('task_instance'):
            context['task_instance'].xcom_push(key='openmeteo_daily_objects', value=uploaded_objects)
            context['task_instance'].xcom_push(key='execution_date', value=execution_date)
        return len(uploaded_objects)

    # ========================================================================
    # Open-Meteo Hourly Extraction
    # ========================================================================

    def extract_openmeteo_hourly(self, **context):
        """Extract 7-day hourly forecast from Open-Meteo"""
        self.log_start("Open-Meteo HOURLY forecast extraction")
        
        capitals_df = get_capitals_dataframe()
        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        uploaded_objects = []

        for _, city in capitals_df.iterrows():
            params = {
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "hourly": ",".join(HOURLY_FORECAST_PARAMS),
                "timezone": DEFAULT_TIMEZONE
            }
            if OPENMETEO_API_KEY: params["apikey"] = OPENMETEO_API_KEY

            try:
                self.logger.info(f"Fetching HOURLY forecast for {city['municipio_nombre']}")
                response = self.session.get(OPENMETEO_FORECAST_URL, params=params, verify=False, timeout=60)
                response.raise_for_status()
                data = response.json()
                
                data["city_code"] = city["city_code"]
                data["municipio_nombre"] = city["municipio_nombre"]
                data["_metadata"] = {"extraction_timestamp": timestamp, "execution_date": execution_date}
                
                object_path = f"forecast/hourly/{execution_date}/weather_hourly_{city['municipio_nombre'].lower()}_{timestamp}.json"
                file_size = self.minio_client.upload_json(BRONZE_OPENMETEO_BUCKET, object_path, data)
                
                uploaded_objects.append({'object_path': object_path, 'city': city['municipio_nombre']})
                self.logger.info(f"✅ Stored HOURLY forecast for {city['municipio_nombre']}")
                time.sleep(0.1)
                
            except Exception as e:
                self.log_error(f"Error fetching HOURLY forecast for {city['municipio_nombre']}", e)
                continue

        self.log_end(f"Stored {len(uploaded_objects)} HOURLY forecast files")
        if context.get('task_instance'):
            context['task_instance'].xcom_push(key='openmeteo_hourly_objects', value=uploaded_objects)
        return len(uploaded_objects)

    # ========================================================================
    # Open-Meteo Air Quality Extraction
    # ========================================================================

    def extract_openmeteo_air_quality(self, **context):
        """Extract Air Quality data"""
        self.log_start("Open-Meteo AIR QUALITY extraction")
        
        capitals_df = get_capitals_dataframe()
        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        uploaded_objects = []

        for _, city in capitals_df.iterrows():
            params = {
                "latitude": city["latitude"], "longitude": city["longitude"],
                "hourly": ",".join(AIR_QUALITY_PARAMS), "timezone": DEFAULT_TIMEZONE
            }
            if OPENMETEO_API_KEY: params["apikey"] = OPENMETEO_API_KEY

            try:
                self.logger.info(f"Fetching AIR QUALITY for {city['municipio_nombre']}")
                response = self.session.get(OPENMETEO_AIR_QUALITY_URL, params=params, verify=False, timeout=60)
                if response.status_code != 200: continue
                
                data = response.json()
                data["city_code"] = city["city_code"]
                data["municipio_nombre"] = city["municipio_nombre"]
                data["_metadata"] = {"extraction_timestamp": timestamp, "execution_date": execution_date}
                
                object_path = f"air_quality/{execution_date}/air_quality_{city['municipio_nombre'].lower()}_{timestamp}.json"
                self.minio_client.upload_json(BRONZE_OPENMETEO_BUCKET, object_path, data)
                uploaded_objects.append({'object_path': object_path, 'city': city['municipio_nombre']})
                time.sleep(0.1)
                
            except Exception as e:
                self.log_error(f"Error fetching AIR QUALITY for {city['municipio_nombre']}", e)
                continue

        self.log_end(f"Stored {len(uploaded_objects)} AIR QUALITY files")
        if context.get('task_instance'):
            context['task_instance'].xcom_push(key='openmeteo_air_quality_objects', value=uploaded_objects)
        return len(uploaded_objects)

    # ========================================================================
    # Open-Meteo Pollen Extraction
    # ========================================================================

    def extract_openmeteo_pollen(self, **context):
        """Extract Pollen data"""
        self.log_start("Open-Meteo POLLEN extraction")
        
        capitals_df = get_capitals_dataframe()
        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        uploaded_objects = []

        for _, city in capitals_df.iterrows():
            params = {
                "latitude": city["latitude"], "longitude": city["longitude"],
                "hourly": ",".join(POLLEN_PARAMS), "timezone": DEFAULT_TIMEZONE
            }
            if OPENMETEO_API_KEY: params["apikey"] = OPENMETEO_API_KEY

            try:
                self.logger.info(f"Fetching POLLEN for {city['municipio_nombre']}")
                response = self.session.get(OPENMETEO_POLLEN_URL, params=params, verify=False, timeout=60)
                if response.status_code != 200: continue
                
                data = response.json()
                data["city_code"] = city["city_code"]
                data["municipio_nombre"] = city["municipio_nombre"]
                data["_metadata"] = {"extraction_timestamp": timestamp, "execution_date": execution_date}
                
                object_path = f"pollen/{execution_date}/pollen_{city['municipio_nombre'].lower()}_{timestamp}.json"
                self.minio_client.upload_json(BRONZE_OPENMETEO_BUCKET, object_path, data)
                uploaded_objects.append({'object_path': object_path, 'city': city['municipio_nombre']})
                time.sleep(0.1)
                
            except Exception as e:
                self.log_error(f"Error fetching POLLEN for {city['municipio_nombre']}", e)
                continue

        self.log_end(f"Stored {len(uploaded_objects)} POLLEN files")
        if context.get('task_instance'):
            context['task_instance'].xcom_push(key='openmeteo_pollen_objects', value=uploaded_objects)
        return len(uploaded_objects)

    # ========================================================================
    # Open-Meteo Marine Extraction
    # ========================================================================

    def extract_openmeteo_marine(self, **context):
        """Extract Marine data"""
        self.log_start("Open-Meteo MARINE extraction")
        
        capitals_df = get_capitals_dataframe()
        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        uploaded_objects = []

        for _, city in capitals_df.iterrows():
            # Only coastal cities logic could be added here
            params = {
                "latitude": city["latitude"], "longitude": city["longitude"],
                "hourly": ",".join(MARINE_PARAMS), "timezone": DEFAULT_TIMEZONE
            }
            if OPENMETEO_API_KEY: params["apikey"] = OPENMETEO_API_KEY

            try:
                self.logger.info(f"Fetching MARINE for {city['municipio_nombre']}")
                response = self.session.get(OPENMETEO_MARINE_URL, params=params, verify=False, timeout=60)
                if response.status_code != 200: continue
                
                data = response.json()
                data["city_code"] = city["city_code"]
                data["municipio_nombre"] = city["municipio_nombre"]
                data["_metadata"] = {"extraction_timestamp": timestamp, "execution_date": execution_date}
                
                object_path = f"marine/{execution_date}/marine_{city['municipio_nombre'].lower()}_{timestamp}.json"
                self.minio_client.upload_json(BRONZE_OPENMETEO_BUCKET, object_path, data)
                uploaded_objects.append({'object_path': object_path, 'city': city['municipio_nombre']})
                time.sleep(0.1)
                
            except Exception as e:
                self.log_error(f"Error fetching MARINE for {city['municipio_nombre']}", e)
                continue

        self.log_end(f"Stored {len(uploaded_objects)} MARINE files")
        if context.get('task_instance'):
            context['task_instance'].xcom_push(key='openmeteo_marine_objects', value=uploaded_objects)
        return len(uploaded_objects)
