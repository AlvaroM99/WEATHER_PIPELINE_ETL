"""
Load Fact Table: Weather Observations
Reads from MinIO silver-openweather (Parquet) and loads into dwh.fct_weather_observation
"""
import logging
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

from weather_etl.base import BaseLoader
from weather_etl.config.app_config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST
from weather_etl.config.lake_config import SILVER_OPENWEATHER_BUCKET, SILVER_PATH_TEMPLATE
from weather_etl.utils.minio_client import MinIOClient

class ObservationLoader(BaseLoader):
    """
    Loader for OpenWeather Observation/Current Weather fact table.
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
        """Get mapping of city_name -> city_id from dim_city"""
        cur = conn.cursor()
        cur.execute("SELECT city_name, city_id FROM dwh.dim_city")
        mapping = {row[0]: row[1] for row in cur.fetchall()}
        cur.close()
        return mapping

    def get_date_id(self, date_str):
        """Convert date string YYYY-MM-DD to date_id (YYYYMMDD as integer)"""
        return int(date_str.replace('-', ''))

    def load(self, **context):
        """
        Load weather observations from MinIO silver layer to fact table
        """
        self.log_start("Loading fct_weather_observation (Silver → Gold)")
        
        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        minio_client = MinIOClient()
        conn = self.get_db_connection()
        
        try:
            # Get city mapping
            city_mapping = self.get_city_id_mapping(conn)
            date_id = self.get_date_id(execution_date)
            
            # Read Parquet from MinIO silver layer
            object_path = SILVER_PATH_TEMPLATE.format(date=execution_date)
            
            self.logger.info(f"Reading from MinIO: {SILVER_OPENWEATHER_BUCKET}/{object_path}")
            
            try:
                df = minio_client.read_parquet(SILVER_OPENWEATHER_BUCKET, object_path)
                self.logger.info(f"Loaded {len(df)} records from Parquet")
            except Exception as e:
                self.logger.warning(f"No data found for {execution_date}: {e}")
                return 0
            
            # Transform DataFrame to fact table format
            records = []
            for _, row in df.iterrows():
                # Get city_id from mapping
                city_name = row.get('city')
                city_id = city_mapping.get(city_name)
                
                if not city_id:
                    self.logger.warning(f"City '{city_name}' not found in dim_city, skipping")
                    continue
                
                # Parse timestamp
                obs_timestamp = pd.to_datetime(row.get('dt'), unit='s') if 'dt' in row else datetime.now()
                
                record = (
                    city_id,
                    date_id,
                    obs_timestamp,
                    row.get('temp'),
                    row.get('feels_like'),
                    row.get('temp_min'),
                    row.get('temp_max'),
                    row.get('pressure'),
                    row.get('humidity'),
                    row.get('visibility'),
                    row.get('wind_speed'),
                    row.get('wind_deg'),
                    row.get('wind_gust'),
                    row.get('clouds'),
                    row.get('weather_id'),
                    row.get('weather_main'),
                    row.get('weather_description'),
                    row.get('rain_1h'),
                    row.get('rain_3h'),
                    row.get('snow_1h'),
                    row.get('snow_3h'),
                )
                records.append(record)
            
            if not records:
                self.logger.warning("No valid records to insert")
                return 0
            
            # Bulk insert into fact table
            cur = conn.cursor()
            
            insert_query = """
                INSERT INTO dwh.fct_weather_observation (
                    city_id, date_id, observation_timestamp,
                    temperature, feels_like, temp_min, temp_max,
                    pressure, humidity, visibility,
                    wind_speed, wind_deg, wind_gust,
                    cloudiness, weather_code, weather_main, weather_description,
                    rain_1h, rain_3h, snow_1h, snow_3h
                ) VALUES %s
                ON CONFLICT DO NOTHING
            """
            
            execute_values(cur, insert_query, records)
            conn.commit()
            
            inserted_count = cur.rowcount
            self.log_end(f"Inserted {inserted_count} records into fct_weather_observation")
            
            cur.close()
            
            # Push to XCom
            if context.get('task_instance'):
                context['task_instance'].xcom_push(key='records_inserted', value=inserted_count)
            
            return inserted_count
            
        except Exception as e:
            conn.rollback()
            self.log_error("Error loading fact table", e)
            raise
        finally:
            conn.close()


def load_fact_observation(**context):
    """
    Wrapper function for Airflow PythonOperator
    """
    loader = ObservationLoader()
    return loader.load(**context)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_fact_observation()

