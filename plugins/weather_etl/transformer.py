"""
Unified Transformer
Consolidates all transformation logic into a single class.
"""
import logging
import pandas as pd
from datetime import datetime
from weather_etl.utils.minio_client import MinIOClient
from weather_etl.config.lake_config import (
    BRONZE_BUCKET, SILVER_BUCKET, 
    BRONZE_OPENMETEO_BUCKET, SILVER_OPENMETEO_BUCKET,
    SILVER_PATH_TEMPLATE
)

class Transformer:
    """
    Unified Transformer Manager.
    Handles transformation for OpenWeatherMap and Open-Meteo services.
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.minio_client = MinIOClient()

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
    # OpenWeatherMap Transformation
    # ========================================================================

    def transform_openweather(self, **context):
        """Transform OpenWeather data from JSON to Parquet"""
        self.log_start("Transforming OpenWeather data (Bronze → Silver)")
        
        execution_date = context.get('ds')
        bronze_objects = self._get_upstream_data(context, ['extract_to_bronze', 'extract_openweather'], 'bronze_objects')
        
        if not execution_date:
            execution_date = datetime.now().strftime('%Y-%m-%d')

        if not bronze_objects:
             # Fallback: Scan bronze bucket
            prefix = f"current/{execution_date}/"
            try:
                objects = self.minio_client.client.list_objects(BRONZE_BUCKET, prefix=prefix, recursive=True)
                bronze_objects = [{'object_path': obj.object_name} for obj in objects if obj.object_name.endswith('.json')]
            except Exception:
                pass
        
        if not bronze_objects:
            self.logger.warning("No bronze data to transform")
            return 0
        
        transformed_records = []
        for bronze_obj in bronze_objects:
            try:
                object_path = bronze_obj.get('object_path')
                if not object_path: continue
                
                raw_data = self.minio_client.read_json(BRONZE_BUCKET, object_path)
                
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
        
        if not transformed_records: return 0
        
        df = pd.DataFrame(transformed_records)
        silver_path = SILVER_PATH_TEMPLATE.format(date=execution_date)
        file_size = self.minio_client.upload_parquet(SILVER_BUCKET, silver_path, df)
        
        self.log_end(f"Stored {len(df)} records in {silver_path}")
        return len(df)

    # ========================================================================
    # Open-Meteo Daily Transformation
    # ========================================================================

    def transform_openmeteo_daily(self, **context):
        """Transform Open-Meteo daily forecast from JSON to Parquet"""
        self.log_start("Transforming Open-Meteo DAILY forecast")
        return self._transform_generic(
            context=context,
            upstream_keys=['openmeteo_daily_objects'],
            upstream_tasks=['extract_openmeteo_daily'],
            bucket_search_prefix='forecast/daily/',
            data_key='daily',
            silver_path_prefix='forecast/daily/',
            file_suffix='daily_forecast'
        )

    # ========================================================================
    # Open-Meteo Hourly Transformation
    # ========================================================================

    def transform_openmeteo_hourly(self, **context):
        """Transform Open-Meteo hourly forecast from JSON to Parquet"""
        self.log_start("Transforming Open-Meteo HOURLY forecast")
        return self._transform_generic(
            context=context,
            upstream_keys=['openmeteo_hourly_objects'],
            upstream_tasks=['extract_openmeteo_hourly'],
            bucket_search_prefix='forecast/hourly/',
            data_key='hourly',
            silver_path_prefix='forecast/hourly/',
            file_suffix='hourly_forecast'
        )

    # ========================================================================
    # Open-Meteo Air Quality Transformation
    # ========================================================================

    def transform_openmeteo_air_quality(self, **context):
        """Transform Open-Meteo air quality from JSON to Parquet"""
        self.log_start("Transforming Open-Meteo AIR QUALITY")
        return self._transform_generic(
            context=context,
            upstream_keys=['openmeteo_air_quality_objects'],
            upstream_tasks=['extract_openmeteo_air_quality'],
            bucket_search_prefix='air_quality/',
            data_key='hourly',
            silver_path_prefix='air_quality/',
            file_suffix='air_quality'
        )

    # ========================================================================
    # Open-Meteo Pollen Transformation
    # ========================================================================

    def transform_openmeteo_pollen(self, **context):
        """Transform Open-Meteo pollen from JSON to Parquet"""
        self.log_start("Transforming Open-Meteo POLLEN")
        return self._transform_generic(
            context=context,
            upstream_keys=['openmeteo_pollen_objects'],
            upstream_tasks=['extract_openmeteo_pollen'],
            bucket_search_prefix='pollen/',
            data_key='hourly',
            silver_path_prefix='pollen/',
            file_suffix='pollen'
        )

    # ========================================================================
    # Open-Meteo Marine Transformation
    # ========================================================================

    def transform_openmeteo_marine(self, **context):
        """Transform Open-Meteo marine from JSON to Parquet"""
        self.log_start("Transforming Open-Meteo MARINE")
        return self._transform_generic(
            context=context,
            upstream_keys=['openmeteo_marine_objects'],
            upstream_tasks=['extract_openmeteo_marine'],
            bucket_search_prefix='marine/',
            data_key='hourly',  # Marine data often comes in hourly blocks even if daily
            silver_path_prefix='marine/',
            file_suffix='marine'
        )

    # ========================================================================
    # Generic Helper
    # ========================================================================

    def _transform_generic(self, context, upstream_keys, upstream_tasks, 
                           bucket_search_prefix, data_key, silver_path_prefix, file_suffix):
        """Generic transformation logic for Open-Meteo files"""
        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Get Upstream Data
        bronze_objects = None
        for task_id in upstream_tasks:
            for key in upstream_keys:
                try:
                    bronze_objects = context['task_instance'].xcom_pull(key=key, task_ids=task_id)
                    if bronze_objects: break
                except: continue
            if bronze_objects: break
            
        # Fallback Scan
        if not bronze_objects:
            self.logger.warning(f"No XCom data, scanning bronze bucket for {execution_date}")
            prefix = f"{bucket_search_prefix}{execution_date}/"
            try:
                objects = list(self.minio_client.client.list_objects(BRONZE_OPENMETEO_BUCKET, prefix=prefix, recursive=True))
                bronze_objects = [{'object_path': obj.object_name} for obj in objects if obj.object_name.endswith('.json')]
            except Exception: pass

        if not bronze_objects:
            self.logger.warning("No data to transform")
            return 0

        all_dfs = []
        for bronze_obj in bronze_objects:
            try:
                object_path = bronze_obj.get('object_path')
                if not object_path: continue
                
                data = self.minio_client.read_json(BRONZE_OPENMETEO_BUCKET, object_path)
                
                # Handle different data structures (daily vs hourly dicts inside response)
                # Some endpoints return 'hourly' dict, some 'daily' dict.
                # We check the generic data_key passed in, but also fallback if needed.
                target_data = data.get(data_key)
                if not target_data and data_key == 'hourly' and 'daily' in data:
                    target_data = data['daily'] # Fallback for marine if it uses daily
                
                if target_data and 'time' in target_data:
                    df = pd.DataFrame(target_data)
                    df['city_code'] = data.get('city_code')
                    df['city_name'] = data.get('municipio_nombre', 'Unknown')
                    df['extraction_date'] = execution_date
                    df['extraction_timestamp'] = data.get('_metadata', {}).get('extraction_timestamp', timestamp)
                    all_dfs.append(df)
            except Exception as e:
                self.log_error(f"Error transforming {object_path}", e)
                continue
        
        if not all_dfs:
            return 0
            
        combined_df = pd.concat(all_dfs, ignore_index=True)
        silver_path = f"{silver_path_prefix}{execution_date}/{file_suffix}_{timestamp}.parquet"
        file_size = self.minio_client.upload_parquet(SILVER_OPENMETEO_BUCKET, silver_path, combined_df)
        
        self.log_end(f"Stored {len(combined_df)} records to {silver_path}")
        return len(combined_df)

    def _get_upstream_data(self, context, task_ids, key):
        if not context.get('task_instance'): return None
        for task_id in task_ids:
            try:
                val = context['task_instance'].xcom_pull(key=key, task_ids=task_id)
                if val: return val
            except: continue
        return None
