"""
Unified Loader
Consolidates all loading logic into a single class.
"""
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

from weather_etl.config.app_config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST
from weather_etl.config.lake_config import (
    SILVER_OPENWEATHER_BUCKET, SILVER_PATH_TEMPLATE
)
from weather_etl.utils.minio_client import MinIOClient

class Loader:
    """
    Unified Loader Manager.
    Handles loading for all fact tables.
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

    def get_db_connection(self):
        return psycopg2.connect(
            host=POSTGRES_HOST, database=POSTGRES_DB,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD
        )

    def get_city_id_mapping(self, conn):
        cur = conn.cursor()
        cur.execute("SELECT city_code, city_id FROM dwh.dim_city")
        # Also map city_name for OpenWeather which uses names
        cur.execute("SELECT city_name, city_id FROM dwh.dim_city")
        name_map = {row[0]: row[1] for row in cur.fetchall()}
        
        cur.execute("SELECT city_code, city_id FROM dwh.dim_city")
        code_map = {row[0]: row[1] for row in cur.fetchall()}
        
        cur.close()
        return name_map, code_map

    def get_date_id(self, date_str):
        try:
            return int(str(date_str)[:10].replace('-', ''))
        except ValueError:
            return None

    # ========================================================================
    # Fact Observation Load
    # ========================================================================

    def load_fact_observation(self, **context):
        """Load weather observations to dwh.fct_weather_observation"""
        self.log_start("Loading fct_weather_observation")
        conn = self.get_db_connection()
        try:
            name_map, _ = self.get_city_id_mapping(conn)
            execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
            date_id = self.get_date_id(execution_date)
            
            object_path = SILVER_PATH_TEMPLATE.format(date=execution_date)
            try:
                df = self.minio_client.read_parquet(SILVER_OPENWEATHER_BUCKET, object_path)
            except Exception:
                self.logger.warning(f"No data found for {execution_date}")
                return 0

            records = []
            for _, row in df.iterrows():
                city_id = name_map.get(row.get('city'))
                if not city_id: continue
                
                obs_timestamp = pd.to_datetime(row.get('dt'), unit='s') if 'dt' in row else datetime.now()
                
                records.append((
                    city_id, date_id, obs_timestamp,
                    row.get('temp'), row.get('feels_like'), row.get('temp_min'), row.get('temp_max'),
                    row.get('pressure'), row.get('humidity'), row.get('visibility'),
                    row.get('wind_speed'), row.get('wind_deg'), row.get('wind_gust'),
                    row.get('clouds'), row.get('weather_id'), row.get('weather_main'), 
                    row.get('weather_description'),
                    row.get('rain_1h'), row.get('rain_3h'), row.get('snow_1h'), row.get('snow_3h')
                ))

            if not records: return 0

            insert_query = """
                INSERT INTO dwh.fct_weather_observation (
                    city_id, date_id, observation_timestamp,
                    temperature, feels_like, temp_min, temp_max,
                    pressure, humidity, visibility,
                    wind_speed, wind_deg, wind_gust,
                    cloudiness, weather_code, weather_main, weather_description,
                    rain_1h, rain_3h, snow_1h, snow_3h
                ) VALUES %s ON CONFLICT DO NOTHING
            """
            
            cur = conn.cursor()
            execute_values(cur, insert_query, records)
            conn.commit()
            self.log_end(f"Inserted {cur.rowcount} records")
            cur.close()
            return cur.rowcount
        except Exception as e:
            conn.rollback()
            self.log_error("Error loading observation", e)
            raise
        finally:
            conn.close()

    # ========================================================================
    # Fact Forecast Daily Load
    # ========================================================================

    def load_fact_forecast_daily(self, **context):
        """Load daily forecast to dwh.fct_weather_forecast"""
        self.log_start("Loading fct_weather_forecast")
        return self._load_generic(
            context, 
            bucket='silver-openmeteo',
            prefix_template="forecast/daily/{execution_date}/",
            insert_query="""
                INSERT INTO dwh.fct_weather_forecast (
                    city_id, forecast_date_id, extraction_date_id,
                    temp_max, temp_min, apparent_temp_max, apparent_temp_min,
                    precipitation_sum, rain_sum, showers_sum, snowfall_sum, precipitation_hours,
                    wind_speed_max, wind_gusts_max, wind_direction_dominant,
                    sunrise, sunset, shortwave_radiation_sum,
                    weather_code, et0_fao_evapotranspiration,
                    created_at
                ) VALUES %s
            """,
            mapper=self._map_daily_forecast
        )

    def _map_daily_forecast(self, row, city_id, extraction_date_id):
        forecast_date = pd.to_datetime(row.get('time')).strftime('%Y-%m-%d')
        return (
            city_id, self.get_date_id(forecast_date), extraction_date_id,
            row.get('temperature_2m_max'), row.get('temperature_2m_min'),
            row.get('apparent_temperature_max'), row.get('apparent_temperature_min'),
            row.get('precipitation_sum'), row.get('rain_sum'), row.get('showers_sum'),
            row.get('snowfall_sum'), row.get('precipitation_hours'),
            row.get('wind_speed_10m_max'), row.get('wind_gusts_10m_max'),
            row.get('wind_direction_10m_dominant'),
            pd.to_datetime(row.get('sunrise')).time() if pd.notna(row.get('sunrise')) else None,
            pd.to_datetime(row.get('sunset')).time() if pd.notna(row.get('sunset')) else None,
            row.get('shortwave_radiation_sum'), row.get('weather_code'),
            row.get('et0_fao_evapotranspiration')
        )

    # ========================================================================
    # Fact Forecast Hourly Load
    # ========================================================================

    def load_fact_forecast_hourly(self, **context):
        """Load hourly forecast to dwh.fct_weather_forecast_hourly"""
        self.log_start("Loading fct_weather_forecast_hourly")
        return self._load_generic(
            context,
            bucket='silver-openmeteo',
            prefix_template="forecast/hourly/{execution_date}/",
            insert_query="""
                INSERT INTO dwh.fct_weather_forecast_hourly (
                    city_id, forecast_datetime, extraction_date_id,
                    temp_2m, temp_80m, apparent_temperature,
                    relative_humidity_2m, dew_point_2m,
                    precipitation_probability, precipitation, rain, snowfall,
                    pressure_msl, surface_pressure,
                    cloud_cover, visibility, uv_index,
                    wind_speed_10m, wind_direction_10m, wind_gusts_10m,
                    weather_code, created_at
                ) VALUES %s
            """,
            mapper=self._map_hourly_forecast
        )

    def _map_hourly_forecast(self, row, city_id, extraction_date_id):
        return (
            city_id, pd.to_datetime(row.get('time')), extraction_date_id,
            row.get('temperature_2m'), row.get('temperature_80m'), row.get('apparent_temperature'),
            row.get('relative_humidity_2m'), row.get('dew_point_2m'),
            row.get('precipitation_probability'), row.get('precipitation'),
            row.get('rain'), row.get('snowfall'),
            row.get('pressure_msl'), row.get('surface_pressure'),
            row.get('cloud_cover'), row.get('visibility'), row.get('uv_index'),
            row.get('wind_speed_10m'), row.get('wind_direction_10m'), row.get('wind_gusts_10m'),
            row.get('weather_code')
        )

    # ========================================================================
    # Fact Air Quality Load
    # ========================================================================

    def load_fact_air_quality(self, **context):
        """Load air quality to dwh.fct_air_quality"""
        self.log_start("Loading fct_air_quality")
        return self._load_generic(
            context,
            bucket='silver-openmeteo',
            prefix_template="air_quality/{execution_date}/",
            insert_query="""
                INSERT INTO dwh.fct_air_quality (
                    city_id, measurement_datetime, extraction_date_id,
                    pm10, pm2_5, carbon_monoxide, nitrogen_dioxide,
                    sulphur_dioxide, ozone, aerosol_optical_depth, dust,
                    created_at
                ) VALUES %s
            """,
            mapper=self._map_air_quality
        )

    def _map_air_quality(self, row, city_id, extraction_date_id):
        def g(k): return row.get(k) if pd.notna(row.get(k)) else None
        return (
            city_id, pd.to_datetime(row.get('time')), extraction_date_id,
            g('pm10'), g('pm2_5'), g('carbon_monoxide'), g('nitrogen_dioxide'),
            g('sulphur_dioxide'), g('ozone'), g('aerosol_optical_depth'), g('dust')
        )

    # ========================================================================
    # Fact Pollen Load
    # ========================================================================

    def load_fact_pollen(self, **context):
        """Load pollen to dwh.fct_pollen"""
        self.log_start("Loading fct_pollen")
        return self._load_generic(
            context,
            bucket='silver-openmeteo',
            prefix_template="pollen/{execution_date}/",
            insert_query="""
                INSERT INTO dwh.fct_pollen (
                    city_id, measurement_datetime, extraction_date_id,
                    alder_pollen, birch_pollen, grass_pollen, 
                    mugwort_pollen, olive_pollen, ragweed_pollen,
                    created_at
                ) VALUES %s
            """,
            mapper=self._map_pollen
        )

    def _map_pollen(self, row, city_id, extraction_date_id):
        def g(k): return row.get(k) if pd.notna(row.get(k)) else None
        return (
            city_id, pd.to_datetime(row.get('time')), extraction_date_id,
            g('alder_pollen'), g('birch_pollen'), g('grass_pollen'),
            g('mugwort_pollen'), g('olive_pollen'), g('ragweed_pollen')
        )

    # ========================================================================
    # Fact Marine Load
    # ========================================================================

    def load_fact_marine(self, **context):
        """Load marine data to dwh.fct_marine"""
        self.log_start("Loading fct_marine")
        return self._load_generic(
            context,
            bucket='silver-openmeteo',
            prefix_template="marine/{execution_date}/",
            insert_query="""
                INSERT INTO dwh.fct_marine (
                    city_id, forecast_date_id, extraction_date_id,
                    wave_height_max, wave_direction_dominant, wave_period_max,
                    wind_wave_height_max, swell_wave_height_max, created_at
                ) VALUES %s
            """,
            mapper=self._map_marine
        )

    def _map_marine(self, row, city_id, extraction_date_id):
        def g(k): return float(row.get(k)) if pd.notna(row.get(k)) else None
        forecast_date_id = self.get_date_id(row.get('time'))
        return (
            city_id, forecast_date_id, extraction_date_id,
            g('wave_height_max'),
            int(row.get('wave_direction_dominant')) if pd.notna(row.get('wave_direction_dominant')) else None,
            g('wave_period_max'), g('wind_wave_height_max'), g('swell_wave_height_max')
        )

    # ========================================================================
    # Generic Helper
    # ========================================================================

    def _load_generic(self, context, bucket, prefix_template, insert_query, mapper):
        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        extraction_date_id = self.get_date_id(execution_date)
        conn = self.get_db_connection()
        
        try:
            _, code_map = self.get_city_id_mapping(conn)
            silver_path = prefix_template.format(execution_date=execution_date)
            
            try:
                objects = self.minio_client.client.list_objects(bucket, prefix=silver_path)
                parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
            except Exception:
                parquet_files = []

            if not parquet_files:
                self.logger.warning(f"No files found for {execution_date}")
                return 0

            latest_file = sorted(parquet_files)[-1]
            df = self.minio_client.read_parquet(bucket, latest_file)
            
            # Deduplicate data to avoid PK violations (if multiple bronze files were aggregated)
            # We assume unique combination of city_code and time is expected per file
            if 'city_code' in df.columns and 'time' in df.columns:
                initial_count = len(df)
                df.drop_duplicates(subset=['city_code', 'time'], inplace=True)
                if len(df) < initial_count:
                    self.logger.warning(f"Attributes dropped {initial_count - len(df)} duplicate rows")

            records = []
            for _, row in df.iterrows():
                city_id = code_map.get(row.get('city_code'))
                if not city_id: continue
                records.append(mapper(row, city_id, extraction_date_id))
            
            if not records: return 0
            
            # Add created_at timestamp
            current_time = datetime.now()
            records_with_time = [(*rec, current_time) for rec in records]
            
            cur = conn.cursor()
            execute_values(cur, insert_query, records_with_time)
            conn.commit()
            
            self.logger.info(f"Inserted {cur.rowcount} records")
            cur.close()
            return cur.rowcount
            
        except Exception as e:
            conn.rollback()
            self.log_error("Error loading data", e)
            raise
        finally:
            conn.close()
