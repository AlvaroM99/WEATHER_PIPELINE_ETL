import logging
import psycopg2
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.minio_client import MinIOClient
from config.lake_config import SILVER_BUCKET, SILVER_PATH_TEMPLATE
from config.app_config import POSTGRES_DB, POSTGRES_HOST, POSTGRES_PASSWORD, POSTGRES_USER

logger = logging.getLogger(__name__)

def load_from_silver_to_dwh(**context):
    """
    GOLD LAYER: Read clean data from silver layer and load to PostgreSQL DWH
    """
    logger.info("🥇 GOLD LAYER: Starting load to PostgreSQL DWH")
    
    # Pull metadata from previous task
    silver_path = context['task_instance'].xcom_pull(key='silver_path', task_ids='transform_to_silver')
    
    # Fallback if no XCom (e.g. running independently)
    if not silver_path:
        execution_date = context['task_instance'].xcom_pull(key='execution_date', task_ids='transform_to_silver') or context['ds']
        silver_path = SILVER_PATH_TEMPLATE.format(date=execution_date)
        logger.info(f"No XCom silver_path, assuming path: {silver_path}")
        
    # We might need to check if file exists
    minio_client = MinIOClient()
    
    try:
        # Check if object exists by trying to stat it (or just read it)
        # minio_client.client.stat_object(SILVER_BUCKET, silver_path)
        pass
    except Exception:
        logger.warning(f"Silver file {silver_path} does not exist or is not accessible")
        return 0
        
    execution_date = context['task_instance'].xcom_pull(key='execution_date', task_ids='transform_to_silver') or context['ds']
    
    # Read Parquet from silver layer
    try:
        df = minio_client.read_parquet(SILVER_BUCKET, silver_path)
    except Exception as e:
        logger.warning(f"Could not read silver file {silver_path}: {e}")
        return 0

    record_count = len(df)
    # Estimate file size or get from metadata if available; not critical for load
    file_size = 0 
    
    logger.info(f"Read {len(df)} records from silver layer: {silver_path}")
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        logger.info("Successfully connected to PostgreSQL")
        
        # Insert weather data
        insert_query = """
            INSERT INTO weather (
                city, country, latitude, longitude, temperature, feels_like,
                temp_min, temp_max, pressure, humidity, weather_main,
                weather_description, wind_speed, wind_deg, clouds, visibility, date
            ) VALUES (
                %(city)s, %(country)s, %(latitude)s, %(longitude)s, %(temperature)s,
                %(feels_like)s, %(temp_min)s, %(temp_max)s, %(pressure)s, %(humidity)s,
                %(weather_main)s, %(weather_description)s, %(wind_speed)s, %(wind_deg)s,
                %(clouds)s, %(visibility)s, %(date)s
            )
            ON CONFLICT (city, date) 
            DO UPDATE SET
                temperature = EXCLUDED.temperature,
                feels_like = EXCLUDED.feels_like,
                temp_min = EXCLUDED.temp_min,
                temp_max = EXCLUDED.temp_max,
                pressure = EXCLUDED.pressure,
                humidity = EXCLUDED.humidity,
                weather_main = EXCLUDED.weather_main,
                weather_description = EXCLUDED.weather_description,
                wind_speed = EXCLUDED.wind_speed,
                wind_deg = EXCLUDED.wind_deg,
                clouds = EXCLUDED.clouds,
                visibility = EXCLUDED.visibility,
                timestamp = CURRENT_TIMESTAMP
        """
        
        records_loaded = 0
        for _, record in df.iterrows():
            try:
                # Convert to dict and remove bronze_source field
                record_dict = record.to_dict()
                record_dict.pop('bronze_source', None)
                
                cursor.execute(insert_query, record_dict)
                records_loaded += 1
            except Exception as e:
                logger.error(f"Error loading data for {record_dict.get('city', 'unknown')}: {str(e)}")
                continue
        
        # Insert lake metadata for tracking
        metadata_query = """
            INSERT INTO lake_metadata (layer, object_path, record_count, file_size_bytes, status)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (object_path) DO UPDATE SET
                record_count = EXCLUDED.record_count,
                file_size_bytes = EXCLUDED.file_size_bytes,
                status = EXCLUDED.status,
                load_timestamp = CURRENT_TIMESTAMP
        """
        
        cursor.execute(metadata_query, ('silver', silver_path, record_count, file_size, 'processed'))
        
        conn.commit()
        logger.info(f"🥇 GOLD LAYER: Successfully loaded {records_loaded} records to DWH")
        
        cursor.close()
        conn.close()
        
        return records_loaded
        
    except Exception as e:
        logger.error(f"❌ Database error: {str(e)}")
        raise