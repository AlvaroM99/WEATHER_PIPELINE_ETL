
import os
import sys
import logging
import pandas as pd
from datetime import datetime

# Add plugins to path
plugin_path = os.path.abspath('plugins')
if plugin_path not in sys.path:
    sys.path.insert(0, plugin_path)

# Add weather_etl to path so 'config' module can be found as 'config' (matching likely minio_client behavior)
weather_etl_path = os.path.join(plugin_path, 'weather_etl')
if weather_etl_path not in sys.path:
    sys.path.insert(0, weather_etl_path)

# Hardcode credentials to bypass encoding/env issues
os.environ['POSTGRES_USER'] = 'weatheruser'
os.environ['POSTGRES_PASSWORD'] = 'Weather2024!Secure'
os.environ['POSTGRES_DB'] = 'weatherdb'
os.environ['POSTGRES_HOST'] = 'localhost'

os.environ['MINIO_ENDPOINT'] = 'localhost:9000'
os.environ['MINIO_ACCESS_KEY'] = 'minioadmin'
os.environ['MINIO_SECRET_KEY'] = 'MinIO2024!Secure'
os.environ['MINIO_SECURE'] = 'False'

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DebugMarine")

try:
    # Overwrite config manually just in case
    import weather_etl.config.app_config
    weather_etl.config.app_config.POSTGRES_HOST = 'localhost'
    weather_etl.config.app_config.POSTGRES_USER = 'weatheruser'
    weather_etl.config.app_config.POSTGRES_PASSWORD = 'Weather2024!Secure'
    weather_etl.config.app_config.POSTGRES_DB = 'weatherdb'

    import config.storage_config
    config.storage_config.MINIO_ENDPOINT = 'localhost:9000'
    config.storage_config.MINIO_ACCESS_KEY = 'minioadmin'
    config.storage_config.MINIO_SECRET_KEY = 'MinIO2024!Secure'
    
    from weather_etl.loader import Loader
    from weather_etl.utils.minio_client import MinIOClient

except ImportError as e:
    print(f"CRITICAL: Import failed: {e}")
    sys.exit(1)

# Mock Airflow context
context = {
    'ds': datetime.now().strftime('%Y-%m-%d'),
    'task_instance': None 
}

def main():
    print("---------------------------------------------------")
    print("STARTING DEBUG LOAD")
    print("---------------------------------------------------")
    
    try:
        print("Initializing Loader...")
        loader = Loader()
        
        # Ensure minio client is pointing to localhost
        # (Though env var override above should handle it if MinIOClient re-reads or if we re-instantiate correct one)
        # Loader.__init__ creates self.minio_client. 
        # But if MinIOClient uses global var from config, and config was imported/set correctly, it should be fine.
        
        print(f"MinIO Endpoint in config: {config.storage_config.MINIO_ENDPOINT}")
        print(f"Postgres Host in config: {weather_etl.config.app_config.POSTGRES_HOST}")

        print("Attempting to load marine data...")
        # Calls load_fact_marine
        rows = loader.load_fact_marine(**context)
        print(f"SUCCESS! Loaded {rows} rows.")
        
    except Exception as e:
        logger.exception("Failed to load marine data")
        print(f"\nFAILURE: {e}")

if __name__ == "__main__":
    main()
