"""
Extract Open-Meteo Marine/Coastal Weather Data
Fetches marine data (waves, sea temp) for coastal cities only
"""
import logging
import requests
import time
from datetime import datetime
import os

from weather_etl.base import BaseExtractor
from weather_etl.utils.city_utils import get_coastal_cities
from weather_etl.utils.http_utils import get_retrying_session
from weather_etl.utils.minio_client import MinIOClient
from weather_etl.config.openmeteo_config import (
    OPENMETEO_MARINE_URL, OPENMETEO_API_KEY, 
    MARINE_PARAMS, DEFAULT_TIMEZONE
)
from weather_etl.config.lake_config import BRONZE_OPENMETEO_BUCKET

class OpenMeteoMarineExtractor(BaseExtractor):
    """
    Extractor for Open-Meteo Marine/Coastal Weather API.
    """

    def extract(self, **context):
        """
        Extract marine/coastal weather from Open-Meteo API
        Only for coastal cities (Barcelona, Valencia, Málaga)
        Stores data in MinIO bronze-openmeteo bucket
        """
        self.log_start("Starting Open-Meteo marine data extraction (Bronze Layer)")
        
        coastal_cities = get_coastal_cities()
        session = get_retrying_session()
        minio_client = MinIOClient()
        
        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        uploaded_objects = []

        for city in coastal_cities:
            params = {
                "latitude": city["lat"],
                "longitude": city["lon"],
                "daily": ",".join(MARINE_PARAMS),
                "timezone": DEFAULT_TIMEZONE
            }

            if OPENMETEO_API_KEY:
                params["apikey"] = OPENMETEO_API_KEY

            try:
                self.logger.info(f"Fetching marine data for {city['name']}")
                response = session.get(OPENMETEO_MARINE_URL, params=params, verify=False, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Add metadata
                    data["city_code"] = city["code"]
                    data["municipio_nombre"] = city["name"]
                    data["_metadata"] = {
                        "extraction_timestamp": timestamp,
                        "execution_date": execution_date
                    }
                    
                    # Upload to MinIO
                    object_path = f"marine/{execution_date}/marine_{city['name'].lower()}_{timestamp}.json"
                    
                    file_size = minio_client.upload_json(BRONZE_OPENMETEO_BUCKET, object_path, data)
                    
                    uploaded_objects.append({
                        'object_path': object_path,
                        'city': city['name'],
                        'file_size': file_size
                    })
                    
                    self.logger.info(f"✅ Stored marine data for {city['name']}")
                else:
                    self.logger.warning(f"Non-200 status for {city['name']}: {response.status_code}")
                    
                time.sleep(0.05)
                
            except Exception as e:
                self.log_error(f"Error fetching marine data for {city['name']}", e)
                continue

        self.log_end(f"Stored {len(uploaded_objects)} marine files")
        
        if context.get('task_instance'):
            context['task_instance'].xcom_push(key='openmeteo_marine_objects', value=uploaded_objects)
        
        return len(uploaded_objects)


def extract_openmeteo_marine(**context):
    """
    Wrapper function for Airflow PythonOperator
    """
    extractor = OpenMeteoMarineExtractor()
    return extractor.extract(**context)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_openmeteo_marine()

