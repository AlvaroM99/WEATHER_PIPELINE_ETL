"""
Transform OpenWeatherMap Data
Reads JSON from bronze and writes Parquet to silver-openweather
"""
import logging
import pandas as pd
import os

from weather_etl.base import BaseTransformer
from weather_etl.utils.minio_client import MinIOClient
from weather_etl.config.lake_config import BRONZE_BUCKET, SILVER_BUCKET, SILVER_PATH_TEMPLATE

class OpenWeatherTransformer(BaseTransformer):
    """
    Transformer for OpenWeatherMap observation data.
    """

    def transform(self, **context):
        """Transform OpenWeather data from JSON to Parquet"""
        self.log_start("Transforming OpenWeather data (Bronze → Silver)")
        
        # Try to get from upstream task
        bronze_objects = None
        execution_date = None
        
        for task_id in ['extract_to_bronze', 'extract_openweather']:
            try:
                bronze_objects = context['task_instance'].xcom_pull(key='bronze_objects', task_ids=task_id)
                execution_date = context['task_instance'].xcom_pull(key='execution_date', task_ids=task_id)
                if bronze_objects:
                    self.logger.info(f"Found XCom data from task: {task_id}")
                    break
            except:
                continue
        
        if not execution_date:
             execution_date = context.get('ds')

        minio_client = MinIOClient()
        
        if not bronze_objects:
            # Fallback: Scan bronze bucket
            self.logger.info(f"No XCom data, scanning bronze bucket for date: {execution_date}")
            prefix = f"current/{execution_date}/"
            try:
                objects = minio_client.client.list_objects(BRONZE_BUCKET, prefix=prefix, recursive=True)
                bronze_objects = [{'object_path': obj.object_name} for obj in objects if obj.object_name.endswith('.json')]
                self.logger.info(f"Found {len(bronze_objects)} bronze files")
            except Exception as e:
                self.logger.warning(f"Could not scan bucket: {e}")
                pass
    
        if not bronze_objects:
            self.logger.warning("No bronze data to transform")
            return 0
        
        transformed_records = []
        
        for bronze_obj in bronze_objects:
            try:
                object_path = bronze_obj.get('object_path')
                if not object_path: 
                    continue
                    
                # Read raw JSON from bronze
                raw_data = minio_client.read_json(BRONZE_BUCKET, object_path)
                
                # Transform and clean data
                transformed = {
                    'city': raw_data.get('_metadata', {}).get('city_name', 'Unknown'),
                    'country': raw_data.get('sys', {}).get('country', 'ES'),
                    'latitude': raw_data.get('coord', {}).get('lat'),
                    'longitude': raw_data.get('coord', {}).get('lon'),
                    'temperature': raw_data.get('main', {}).get('temp'),
                    'feels_like': raw_data.get('main', {}).get('feels_like'),
                    'temp_min': raw_data.get('main', {}).get('temp_min'),
                    'temp_max': raw_data.get('main', {}).get('temp_max'),
                    'pressure': raw_data.get('main', {}).get('pressure'),
                    'humidity': raw_data.get('main', {}).get('humidity'),
                    'weather_main': raw_data.get('weather', [{}])[0].get('main', 'Unknown'),
                    'weather_description': raw_data.get('weather', [{}])[0].get('description', 'No description'),
                    'wind_speed': raw_data.get('wind', {}).get('speed'),
                    'wind_deg': raw_data.get('wind', {}).get('deg'),
                    'clouds': raw_data.get('clouds', {}).get('all'),
                    'visibility': raw_data.get('visibility'),
                    'date': execution_date,
                    'bronze_source': object_path
                }
                
                transformed_records.append(transformed)
                
            except Exception as e:
                self.log_error(f"Error transforming {object_path}", e)
                continue
        
        if not transformed_records:
            self.logger.warning("No records to save to silver layer")
            return 0
        
        # Convert to DataFrame
        df = pd.DataFrame(transformed_records)
        
        # Create silver object path
        silver_path = SILVER_PATH_TEMPLATE.format(date=execution_date)
        
        # Upload to MinIO silver bucket as Parquet
        file_size = minio_client.upload_parquet(SILVER_BUCKET, silver_path, df)
        
        self.log_end(f"Stored {len(df)} records in {silver_path}")
        
        # Push metadata to XCom
        if context.get('task_instance'):
            context['task_instance'].xcom_push(key='silver_path', value=silver_path)
            context['task_instance'].xcom_push(key='record_count', value=len(df))
            context['task_instance'].xcom_push(key='file_size', value=file_size)
            context['task_instance'].xcom_push(key='execution_date', value=execution_date)
        
        return len(df)


def transform_openweather(**context):
    """
    Wrapper function for Airflow PythonOperator
    """
    transformer = OpenWeatherTransformer()
    return transformer.transform(**context)

# Backward compatibility
transform_to_silver = transform_openweather


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    transform_openweather()
