"""
Database Schema Initialization DAG
Creates and populates ALL dimensional and fact tables
Executes SQL scripts automatically via Python
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import logging
# Add dags directory to path
# sys.path hack removed


from weather_etl.config.app_config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST
from weather_etl.tasks.loading.load_dimensional_tables import load_all_dimensional_tables

logger = logging.getLogger(__name__)


def execute_sql_file(sql_file_path):
    """Execute SQL file in PostgreSQL"""
    logger.info(f"Executing SQL file: {sql_file_path}")
    
    # Read SQL file
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Connect and execute
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    
    try:
        cur = conn.cursor()
        cur.execute(sql_content)
        conn.commit()
        logger.info(f"✅ Successfully executed {sql_file_path}")
        cur.close()
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error executing {sql_file_path}: {e}")
        raise
    finally:
        conn.close()


def create_dimensional_tables(**context):
    """Create dimensional table schemas"""
    sql_path = '/opt/airflow/postgres/init-dimensional-tables.sql'
    execute_sql_file(sql_path)
    return "Dimensional tables created"


def create_fact_tables(**context):
    """Create fact table schemas"""
    sql_path = '/opt/airflow/postgres/init-fact-tables.sql'
    execute_sql_file(sql_path)
    return "Fact tables created"


# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'schema_initialization',
    default_args=default_args,
    description='Initialize ALL database schemas (dimensional + fact tables)',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 1, 27),
    catchup=False,
    tags=['setup', 'initialization', 'dwh'],
) as dag:
    
    # Step 1: Create dimensional table schemas
    create_dim_tables = PythonOperator(
        task_id='create_dimensional_tables',
        python_callable=create_dimensional_tables,
        provide_context=True,
    )
    
    # Step 2: Populate dimensional tables with data
    populate_dim_tables = PythonOperator(
        task_id='populate_dimensional_tables',
        python_callable=load_all_dimensional_tables,
        provide_context=True,
    )
    
    # Step 3: Create fact table schemas
    create_fct_tables = PythonOperator(
        task_id='create_fact_tables',
        python_callable=create_fact_tables,
        provide_context=True,
    )
    
    # Dependencies
    create_dim_tables >> populate_dim_tables >> create_fct_tables
