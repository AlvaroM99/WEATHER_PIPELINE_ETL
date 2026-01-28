"""
Extract Open-Meteo Hourly Forecast Data
Fetches 7-day hourly weather forecast (42+ variables) for all configured cities
"""
import logging
import requests
import json
import time
from datetime import datetime
import urllib3
import os

from weather_etl.base import BaseExtractor
from weather_etl.utils.city_utils import get_capitals_dataframe
from weather_etl.utils.http_utils import get_retrying_session
from weather_etl.utils.minio_client import MinIOClient
from weather_etl.config.openmeteo_config import (
    OPENMETEO_FORECAST_URL, OPENMETEO_API_KEY, 
    HOURLY_FORECAST_PARAMS, DEFAULT_TIMEZONE
)
from weather_etl.config.lake_config import BRONZE_OPENMETEO_BUCKET

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class OpenMeteoHourlyExtractor(BaseExtractor):
    """
    Extractor for Open-Meteo Hourly Forecast API.
    """

    def extract(self, **context):
        """
        Extract 7-day hourly forecast from Open-Meteo API
        Stores data in MinIO bronze-openmeteo bucket
        """
        self.log_start("Starting Open-Meteo hourly forecast extraction (Bronze Layer)")
        
        capitals_df = get_capitals_dataframe()
        session = get_retrying_session()
        minio_client = MinIOClient()
        
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

            if OPENMETEO_API_KEY:
                params["apikey"] = OPENMETEO_API_KEY

            try:
                self.logger.info(f"Fetching hourly forecast for {city['municipio_nombre']}")
                response = session.get(OPENMETEO_FORECAST_URL, params=params, verify=False, timeout=60)
                response.raise_for_status()
                data = response.json()
                
                # Add metadata
                data["city_code"] = city["city_code"]
                data["municipio_nombre"] = city["municipio_nombre"]
                data["_metadata"] = {
                    "extraction_timestamp": timestamp,
                    "execution_date": execution_date
                }
                
                # Upload to MinIO
                object_path = f"forecast/hourly/{execution_date}/weather_hourly_{city['municipio_nombre'].lower()}_{timestamp}.json"
                
                file_size = minio_client.upload_json(BRONZE_OPENMETEO_BUCKET, object_path, data)
                
                uploaded_objects.append({
                    'object_path': object_path,
                    'city': city['municipio_nombre'],
                    'file_size': file_size
                })
                
                self.logger.info(f"✅ Stored hourly forecast for {city['municipio_nombre']}")
                
                time.sleep(0.1)
                
            except requests.exceptions.RequestException as e:
                self.log_error(f"Error fetching hourly forecast for {city['municipio_nombre']}", e)
                continue

        self.log_end(f"Stored {len(uploaded_objects)} hourly forecast files")
        
        if context.get('task_instance'):
            context['task_instance'].xcom_push(key='openmeteo_hourly_objects', value=uploaded_objects)
            context['task_instance'].xcom_push(key='execution_date', value=execution_date)
        
        return len(uploaded_objects)


def extract_openmeteo_hourly(**context):
    """
    Wrapper function for Airflow PythonOperator
    """
    extractor = OpenMeteoHourlyExtractor()
    return extractor.extract(**context)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_openmeteo_hourly()

