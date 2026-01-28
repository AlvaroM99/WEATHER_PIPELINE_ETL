
import os
import sys
import logging
import pandas as pd
from datetime import datetime

# Add project root to path
# Assuming the script is run from project root or tests/
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Hardcode credentials to bypass encoding/env issues
# Load .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("Loaded .env file")
except ImportError:
    print("python-dotenv not installed, relying on existing env vars")

# Hardcode credentials to bypass encoding/env issues
# UPDATED: Use env vars or defaults, but DO NOT commit secrets
os.environ['POSTGRES_USER'] = os.getenv('POSTGRES_USER', 'weatheruser')
os.environ['POSTGRES_PASSWORD'] = os.getenv('POSTGRES_PASSWORD', 'weather_password_placeholder')
os.environ['POSTGRES_DB'] = os.getenv('POSTGRES_DB', 'weatherdb')
# Force localhost for local debugging, even if .env says 'postgres'
os.environ['POSTGRES_HOST'] = 'localhost'

os.environ['MINIO_ENDPOINT'] = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
if os.environ['MINIO_ENDPOINT'].startswith('minio:'):
     os.environ['MINIO_ENDPOINT'] = 'localhost:9000'
     
os.environ['MINIO_ACCESS_KEY'] = os.getenv('MINIO_ROOT_USER', 'minioadmin')
os.environ['MINIO_SECRET_KEY'] = os.getenv('MINIO_ROOT_PASSWORD', 'minio_password_placeholder')
os.environ['MINIO_SECURE'] = 'False'

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DebugMarine")

try:
    import src.weather_config.app_config
    src.weather_config.app_config.POSTGRES_HOST = os.environ['POSTGRES_HOST']
    src.weather_config.app_config.POSTGRES_USER = os.environ['POSTGRES_USER']
    src.weather_config.app_config.POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
    src.weather_config.app_config.POSTGRES_DB = os.environ['POSTGRES_DB']
    
    print(f"DEBUG: Host={src.weather_config.app_config.POSTGRES_HOST}")
    # print(f"DEBUG: User={src.weather_config.app_config.POSTGRES_USER}")
    # print(f"DEBUG: DB={src.weather_config.app_config.POSTGRES_DB}")

    # lake_config uses os.getenv directly usually

    # lake_config uses os.getenv directly usually
    import src.weather_config.lake_config as lake_config
    
    from src.loader import Loader
    from src.weather_utils.minio_client import MinIOClient

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
        
        # print(f"MinIO Endpoint in config: {config.storage_config.MINIO_ENDPOINT}")
        print(f"Postgres Host in config: {src.weather_config.app_config.POSTGRES_HOST}")

        print("Attempting to load marine data...")
        # Calls load_fact_marine
        rows = loader.load_fact_marine(**context)
        print(f"SUCCESS! Loaded {rows} rows.")
        
    except Exception as e:
        logger.exception("Failed to load marine data")
        try:
            with open("error.log", "w", encoding="utf-8") as f:
                import traceback
                f.write(traceback.format_exc())
            print("Error written to error.log")
        except:
            print("Failed to write error log")

if __name__ == "__main__":
    main()
