"""
Load Fact Table: Pollen
Reads from MinIO silver-openmeteo (Parquet) and loads into dwh.fct_pollen
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

class PollenLoader(BaseLoader):
    """
    Loader for Pollen forecast fact table.
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
        return int(date_str.replace('-', ''))

    def load(self, **context):
        """
        Load pollen data from MinIO silver layer to fact table
        """
        self.log_start("Loading fct_pollen (Silver → Gold)")
        
        execution_date = context.get('ds', datetime.now().strftime('%Y-%m-%d'))
        minio_client = MinIOClient()
        conn = self.get_db_connection()
        
        try:
            # Get city mapping
            city_mapping = self.get_city_id_mapping(conn)
            extraction_date_id = self.get_date_id(execution_date)
            
            # Read Parquet from MinIO silver layer
            silver_path = f"pollen/{execution_date}/"
            
            self.logger.info(f"Reading from MinIO: silver-openmeteo/{silver_path}")
            
            # List all parquet files for this date
            objects = minio_client.client.list_objects('silver-openmeteo', prefix=silver_path)
            parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
            
            if not parquet_files:
                self.logger.warning(f"No parquet files found for {execution_date}")
                return 0
            
            # Read the latest file
            latest_file = sorted(parquet_files)[-1]
            df = minio_client.read_parquet('silver-openmeteo', latest_file)
            self.logger.info(f"Loaded {len(df)} records from Parquet")
            
            # Transform DataFrame to fact table format
            records = []
            for _, row in df.iterrows():
                # Get city_id from mapping
                city_code = row.get('city_code')
                city_id = city_mapping.get(city_code)
                
                if not city_id:
                    continue
                
                # Helper to safely get value or None
                def get_val(key):
                    val = row.get(key)
                    return val if pd.notna(val) else None
                
                records.append((
                    city_id,
                    pd.to_datetime(row.get('time')),  # measurement_datetime
                    extraction_date_id,
                    get_val('alder_pollen'),
                    get_val('birch_pollen'),
                    get_val('grass_pollen'),
                    get_val('mugwort_pollen'),
                    get_val('olive_pollen'),
                    get_val('ragweed_pollen')
                ))
            
            if not records:
                self.logger.warning("No valid records to insert")
                return 0
            
            # Bulk insert into fact table
            cur = conn.cursor()
            
            insert_query = """
                INSERT INTO dwh.fct_pollen (
                    city_id, measurement_datetime, extraction_date_id,
                    alder_pollen, birch_pollen, grass_pollen, 
                    mugwort_pollen, olive_pollen, ragweed_pollen,
                    created_at
                ) VALUES %s
            """
            
            # Add created_at to records
            current_time = datetime.now()
            records_with_time = [(*rec, current_time) for rec in records]
            
            execute_values(cur, insert_query, records_with_time)
            conn.commit()
            
            inserted_count = cur.rowcount
            self.log_end(f"Inserted {inserted_count} records into fct_pollen")
            
            cur.close()
            
            if context.get('task_instance'):
                context['task_instance'].xcom_push(key='records_inserted', value=inserted_count)
            
            return inserted_count
            
        except Exception as e:
            conn.rollback()
            self.log_error("Error loading pollen", e)
            raise
        finally:
            conn.close()


def load_fact_pollen(**context):
    """
    Wrapper function for Airflow PythonOperator
    """
    loader = PollenLoader()
    return loader.load(**context)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_fact_pollen()
