
import sys
import os
import psycopg2
import logging

# Add parent directory to path (Since we are in /opt/airflow/dags, parent /opt/airflow should be in path for 'dags' package resolution?)
# Wait, if we are in dags/, then 'from config' implies dags/config.
# If we run from /opt/airflow/dags/reset_database.py
# We need to import 'config.app_config'.
# If 'dags' is NOT a package (no __init__.py), we might need to add /opt/airflow to path and import dags.config?
# Or if we are in dags/, just 'import config.app_config'.

# Let's set path to include the folder containing 'dags' package, which is /opt/airflow
sys.path.append('/opt/airflow')

# Now we can import dags.config if dags is package
# OR if we are in dags/ folder, we can import config directly if config is in dags/config
# But let's assume standard airflow structure.
# Best bet: Append /opt/airflow and import dags.config.app_config

from dags.config.app_config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST
from dags.tasks.loading.load_dimensional_tables import load_all_dimensional_tables

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def reset_database():
    """
    Drops DWH schema and recreating all tables from SQL scripts.
    Then reloads dimensional data.
    """
    logger.info("⚠️ STARTING DATABASE HARD RESET (INSIDE CONTAINER / DAGS FOLDER) ⚠️")
    
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    try:
        # 1. Drop Schema Cascade
        logger.info("Dropping schema 'dwh' and all objects...")
        cur.execute("DROP SCHEMA IF EXISTS dwh CASCADE;")
        
        # 2. Recreate Schema and Dimensions
        logger.info("Executing init-dimensional-tables.sql...")
        # Paths are relative to /opt/airflow inside container
        # init-dimensional-tables.sql is in /opt/airflow/postgres/ (assuming postgres folder is mounted)
        # If postgres folder is NOT mounted, we might fail reading it.
        # But 'postgres' is usually a sibling of 'dags'. 
        # If root is not mounted, 'postgres' folder might strictly be local.
        # Check if 'postgres' folder exists in container. If not, we found another problem.
        # But let's assume complete repo mount.
        
        with open('/opt/postgres/init-dimensional-tables.sql', 'r', encoding='utf-8') as f:
            dim_sql = f.read()
            cur.execute(dim_sql)
            
        # 3. Recreate Fact Tables (New Schema)
        logger.info("Executing init-fact-tables.sql...")
        with open('/opt/postgres/init-fact-tables.sql', 'r', encoding='utf-8') as f:
            fact_sql = f.read()
            cur.execute(fact_sql)
            
        logger.info("✅ Schema re-initialized successfully.")
        
    except Exception as e:
        logger.error(f"❌ Error during reset: {e}")
        # If file not found, we print it
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

    # 4. Load Dimensional Data
    logger.info("Reloading dimensional data...")
    load_all_dimensional_tables()
    logger.info("✅ Database Reset Complete.")

if __name__ == "__main__":
    reset_database()
