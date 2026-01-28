"""
Load Fact Table: Marine Weather
Reads from MinIO silver-openmeteo (Parquet) and loads into dwh.fct_marine
Uses UPSERT strategy: Updates existing records if they exist.
"""
import logging
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

from weather_etl.base import BaseLoader
from weather_etl.config.app_config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST
from weather_etl.config.lake_config import SILVER_OPENMETEO_BUCKET
from weather_etl.utils.minio_client import MinIOClient

class MarineLoader(BaseLoader):
    """
    Loader for Marine Weather fact table.
    """

    def get_db_connection(self):
        """Create PostgreSQL database connection"""
        return psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )

    def get_city_id_mapping(self, conn):
        """Get mapping of city_code -> city_id from dim_city"""
        cur = conn.cursor()
        cur.execute("SELECT city_code, city_id FROM dwh.dim_city")
        mapping = {row[0]: row[1] for row in cur.fetchall()}
        cur.close()
        return mapping

    def get_date_id(self, date_str):
        """Convert date string YYYY-MM-DD to date_id (YYYYMMDD as integer)"""
        try:
            return int(str(date_str)[:10].replace('-', ''))
        except ValueError:
            return None

    def load(self, **context):
        """
        Load marine weather data from MinIO (Silver) to DWH (Gold)
        """
        self.log_start("Loading fct_marine (Silver → Gold)")
        
        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        minio_client = MinIOClient()
        conn = self.get_db_connection()
        
        try:
            # Get city mapping
            city_mapping = self.get_city_id_mapping(conn)
            extraction_date_id = self.get_date_id(execution_date)
            
            # Determine file path
            silver_path = f"marine/{execution_date}/"
            self.logger.info(f"Reading from MinIO: silver-openmeteo/{silver_path}")
            
            # List objects to find latest parquet
            objects = minio_client.client.list_objects('silver-openmeteo', prefix=silver_path)
            parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
            
            if not parquet_files:
                self.logger.warning(f"No marine parquet files found for {execution_date}")
                return 0
                
            latest_file = sorted(parquet_files)[-1]
            self.logger.info(f"Reading file: {latest_file}")
            
            # Read Data
            df = minio_client.read_parquet('silver-openmeteo', latest_file)
            
            if df.empty:
                self.logger.warning("Empty dataframe")
                return 0
                
            self.logger.info(f"Read {len(df)} records from Parquet")

            # Transform DataFrame to Records
            records = []
            for _, row in df.iterrows():
                city_code = row.get('city_code')
                city_id = city_mapping.get(city_code)
                
                if not city_id:
                    continue
                
                forecast_time = row.get('time')
                forecast_date_id = self.get_date_id(forecast_time)
                
                # Helper for safe get
                def get_val(key):
                    val = row.get(key)
                    return float(val) if pd.notna(val) else None

                records.append((
                    city_id,
                    forecast_date_id,
                    extraction_date_id,
                    get_val('wave_height_max'),
                    int(row.get('wave_direction_dominant')) if pd.notna(row.get('wave_direction_dominant')) else None,
                    get_val('wave_period_max'),
                    get_val('wind_wave_height_max'),
                    get_val('swell_wave_height_max')
                ))
            
            if not records:
                self.logger.warning("No valid records to insert")
                return 0

            # Execute Upsert
            cur = conn.cursor()
            
            insert_query = """
                INSERT INTO dwh.fct_marine (
                    city_id, forecast_date_id, extraction_date_id,
                    wave_height_max, wave_direction_dominant, wave_period_max,
                    wind_wave_height_max, swell_wave_height_max, created_at
                ) VALUES %s
            """
            
            # Add created_at to records locally to match schema
            current_time = datetime.now()
            records_with_time = [(*rec, current_time) for rec in records]
            
            execute_values(cur, insert_query, records_with_time)
            conn.commit()
            
            inserted_count = cur.rowcount
            self.log_end(f"Inserted {inserted_count} records into fct_marine")
            
            cur.close()
            
            if context.get('task_instance'):
                context['task_instance'].xcom_push(key='records_inserted', value=inserted_count)
                
            return inserted_count
            
        except Exception as e:
            conn.rollback()
            self.log_error("Error loading marine data", e)
            raise
        finally:
            conn.close()


def load_fact_marine(**context):
    """
    Wrapper function for Airflow PythonOperator
    """
    loader = MarineLoader()
    return loader.load(**context)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_fact_marine()

