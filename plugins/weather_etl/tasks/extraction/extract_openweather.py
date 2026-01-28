"""
Extract OpenWeatherMap Data
Fetches current weather observation data for all configured cities
"""
import logging
import requests
from datetime import datetime
import os

from weather_etl.base import BaseExtractor
from weather_etl.utils.minio_client import MinIOClient
from weather_etl.config.lake_config import BRONZE_BUCKET, BRONZE_PATH_TEMPLATE
from weather_etl.config.app_config import API_KEY, CITIES

class OpenWeatherExtractor(BaseExtractor):
    """
    Extractor for OpenWeatherMap Current Weather API.
    """

    def extract(self, **context):
        """
        Extract current weather data from OpenWeatherMap API
        Stores data in MinIO bronze bucket
        """
        self.log_start(f"Starting OpenWeatherMap extraction for {len(CITIES)} cities")
        
        minio_client = MinIOClient()
        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        uploaded_objects = []
        
        for city in CITIES:
            try:
                # API endpoint for current weather
                url = f"https://api.openweathermap.org/data/2.5/weather"
                params = {
                    'lat': city['lat'],
                    'lon': city['lon'],
                    'appid': API_KEY,
                    'units': 'metric'
                }
                
                self.logger.info(f"Fetching weather data for {city['name']}")
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                
                raw_data = response.json()
                
                # Add metadata
                raw_data['_metadata'] = {
                    'city_name': city['name'],
                    'extraction_timestamp': timestamp,
                    'execution_date': execution_date
                }
                
                # Create object path
                object_path = BRONZE_PATH_TEMPLATE.format(
                    date=execution_date,
                    city=city['name'].lower(),
                    timestamp=timestamp
                )
                
                # Upload to MinIO bronze bucket
                file_size = minio_client.upload_json(BRONZE_BUCKET, object_path, raw_data)
                
                uploaded_objects.append({
                    'object_path': object_path,
                    'city': city['name'],
                    'file_size': file_size
                })
                
                self.logger.info(f"✅ Stored raw data for {city['name']}")
                
            except requests.exceptions.RequestException as e:
                self.log_error(f"Error fetching data for {city['name']}", e)
                continue
            except Exception as e:
                self.log_error(f"Error processing {city['name']}", e)
                continue
        
        self.log_end(f"Stored {len(uploaded_objects)} OpenWeather files")
        
        if context.get('task_instance'):
            context['task_instance'].xcom_push(key='bronze_objects', value=uploaded_objects)
            context['task_instance'].xcom_push(key='execution_date', value=execution_date)
        
        return len(uploaded_objects)


def extract_openweather(**context):
    """
    Wrapper function for Airflow PythonOperator
    """
    extractor = OpenWeatherExtractor()
    return extractor.extract(**context)
    
# Backward compatibility
extract_to_bronze = extract_openweather


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    extract_openweather()
