"""
Transform Open-Meteo Daily Forecast Data
Reads JSON from bronze-openmeteo and writes Parquet to silver-openmeteo
"""
import logging
import pandas as pd
from datetime import datetime

from weather_etl.base import BaseTransformer
from weather_etl.utils.minio_client import MinIOClient
from weather_etl.config.lake_config import BRONZE_OPENMETEO_BUCKET, SILVER_OPENMETEO_BUCKET

class OpenMeteoDailyTransformer(BaseTransformer):
    """
    Transformer for Open-Meteo Daily Forecast data.
    """

    def transform(self, **context):
        """
        Transform Open-Meteo daily forecast from JSON to Parquet
        Bronze → Silver transformation
        """
        self.log_start("Transforming Open-Meteo daily forecast (Bronze → Silver)")
        
        # Intento de recuperar XCom de la tarea de extracción
        bronze_objects = None
        execution_date = None
        
        # Task IDs posibles para la extracción diaria
        possible_upstream_tasks = ['extract_openmeteo_daily', 'extract_weather_data']
        
        for task_id in possible_upstream_tasks:
            try:
                bronze_objects = context['task_instance'].xcom_pull(key='openmeteo_daily_objects', task_ids=task_id)
                execution_date = context['task_instance'].xcom_pull(key='execution_date', task_ids=task_id)
                if bronze_objects:
                    self.logger.info(f"✅ Found XCom data from task: {task_id}")
                    break
            except Exception:
                continue
        
        # Si no hay XCom, usar fecha actual o pasada por contexto
        if not execution_date:
            execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        
        minio_client = MinIOClient()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Fallback: Escanear bucket si no hay XCom
        if not bronze_objects:
            self.logger.warning(f"No XCom data, scanning bronze bucket for date: {execution_date}")
            prefix = f"forecast/daily/{execution_date}/"
            try:
                objects = list(minio_client.client.list_objects(BRONZE_OPENMETEO_BUCKET, prefix=prefix, recursive=True))
                bronze_objects = [{'object_path': obj.object_name} for obj in objects if obj.object_name.endswith('.json')]
                self.logger.info(f"Found {len(bronze_objects)} bronze files via scan")
            except Exception as e:
                self.logger.warning(f"Could not scan bucket: {e}")
                pass

        if not bronze_objects:
            self.logger.warning("No bronze data to transform")
            return 0

        all_forecasts = []
        
        # Procesar cada archivo encontrado
        for bronze_obj in bronze_objects:
            try:
                object_path = bronze_obj.get('object_path')
                if not object_path:
                    continue
                    
                self.logger.info(f"Reading bronze file: {object_path}")
                data = minio_client.read_json(BRONZE_OPENMETEO_BUCKET, object_path)
                
                # Transform to flat structure
                if 'daily' in data and 'time' in data['daily']:
                    df = pd.DataFrame(data['daily'])
                    
                    # Retrieve metadata safely
                    city_code = data.get('city_code')
                    city_name = data.get('municipio_nombre')
                    
                    # If not in file, try to infer or skip 
                    if not city_name:
                        try:
                            city_name = object_path.split('_')[2].title() 
                        except:
                            city_name = "Unknown"
                    
                    df['city_code'] = city_code
                    df['city_name'] = city_name
                    df['extraction_date'] = execution_date
                    df['extraction_timestamp'] = data.get('_metadata', {}).get('extraction_timestamp', timestamp)
                    
                    all_forecasts.append(df)
                    
            except Exception as e:
                self.log_error(f"Error transforming file {bronze_obj.get('object_path')}", e)
                continue
        
        if not all_forecasts:
            self.logger.warning("No data to transform")
            return 0
        
        # Combine all cities
        combined_df = pd.concat(all_forecasts, ignore_index=True)
        
        # Write to silver layer as Parquet
        silver_path = f"forecast/daily/{execution_date}/daily_forecast_{timestamp}.parquet"
        file_size = minio_client.upload_parquet(SILVER_OPENMETEO_BUCKET, silver_path, combined_df)
        
        self.log_end(f"Wrote {len(combined_df)} records to silver layer ({file_size} bytes)")
        
        if context.get('task_instance'):
            context['task_instance'].xcom_push(key='records_transformed', value=len(combined_df))
            context['task_instance'].xcom_push(key='silver_path', value=silver_path)
        
        return len(combined_df)


def transform_openmeteo_daily(**context):
    """
    Wrapper function for Airflow PythonOperator
    """
    transformer = OpenMeteoDailyTransformer()
    return transformer.transform(**context)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    transform_openmeteo_daily()

