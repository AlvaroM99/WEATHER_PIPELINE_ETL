"""
Transform Open-Meteo Pollen Data
Reads JSON from bronze-openmeteo and writes Parquet to silver-openmeteo
"""
import logging
import pandas as pd
from datetime import datetime
import os

from weather_etl.base import BaseTransformer
from weather_etl.utils.minio_client import MinIOClient
from weather_etl.config.lake_config import BRONZE_OPENMETEO_BUCKET, SILVER_OPENMETEO_BUCKET

class OpenMeteoPollenTransformer(BaseTransformer):
    """
    Transformer for Open-Meteo Pollen Forecast data.
    """

    def transform(self, **context):
        """Transform Open-Meteo pollen data from JSON to Parquet"""
        self.log_start("Transforming Open-Meteo pollen data (Bronze → Silver)")
        
        # Intento de recuperar XCom
        bronze_objects = None
        execution_date = None
        
        for task_id in ['extract_openmeteo_pollen', 'extract_pollen_data']:
            try:
                bronze_objects = context['task_instance'].xcom_pull(key='openmeteo_pollen_objects', task_ids=task_id)
                execution_date = context['task_instance'].xcom_pull(key='execution_date', task_ids=task_id)
                if bronze_objects:
                    self.logger.info(f"✅ Found XCom data from task: {task_id}")
                    break
            except Exception:
                continue
                
        if not execution_date:
            execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
            
        minio_client = MinIOClient()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Fallback to bucket scan
        if not bronze_objects:
            self.logger.warning(f"No XCom data, scanning bronze bucket for date: {execution_date}")
            prefix = f"pollen/{execution_date}/"
            try:
                objects = list(minio_client.client.list_objects(BRONZE_OPENMETEO_BUCKET, prefix=prefix, recursive=True))
                bronze_objects = [{'object_path': obj.object_name} for obj in objects if obj.object_name.endswith('.json')]
            except Exception as e:
                self.logger.warning(f"Could not scan bucket: {e}")
                pass

        if not bronze_objects:
            self.logger.warning("No pollen data to transform")
            return 0
        
        all_data = []
        
        for bronze_obj in bronze_objects:
            try:
                object_path = bronze_obj.get('object_path')
                if not object_path: continue
                
                data = minio_client.read_json(BRONZE_OPENMETEO_BUCKET, object_path)
                
                if 'hourly' in data and 'time' in data['hourly']:
                    df = pd.DataFrame(data['hourly'])
                    # Metadata extraction
                    df['city_code'] = data.get('city_code')
                    df['city_name'] = data.get('municipio_nombre', 'Unknown')
                    df['extraction_date'] = execution_date
                    
                    all_data.append(df)
                    
            except Exception as e:
                self.log_error(f"Error transforming pollen file {bronze_obj.get('object_path')}", e)
                continue
        
        if not all_data:
            self.logger.warning("No pollen data to transform")
            return 0
        
        combined_df = pd.concat(all_data, ignore_index=True)
        silver_path = f"pollen/{execution_date}/pollen_{timestamp}.parquet"
        file_size = minio_client.upload_parquet(SILVER_OPENMETEO_BUCKET, silver_path, combined_df)
        
        self.log_end(f"Wrote {len(combined_df)} pollen records to silver ({file_size} bytes)")
        
        if context.get('task_instance'):
            context['task_instance'].xcom_push(key='records_transformed', value=len(combined_df))
            context['task_instance'].xcom_push(key='silver_path', value=silver_path)
        
        return len(combined_df)


def transform_openmeteo_pollen(**context):
    """
    Wrapper function for Airflow PythonOperator
    """
    transformer = OpenMeteoPollenTransformer()
    return transformer.transform(**context)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    transform_openmeteo_pollen()

