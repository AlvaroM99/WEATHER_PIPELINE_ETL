"""
Unified Database Reset Pipeline
Performs a full reset of the Data Warehouse:
1. Drops the entire 'dwh' schema (Cascading to all tables).
2. Recreates Dimensional Tables.
3. Populates Dimensional Tables (Cities, Dates, etc.).
4. Recreates Fact Tables.
"""

import logging
import os
from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.config.database_config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_USER,
)
from src.dimensional_loader import DimensionalLoader

logger = logging.getLogger(__name__)


def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST, database=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD
    )


def drop_dwh_schema(**context):
    """Drops the 'dwh' schema and all its objects"""
    logger.info("Dropping schema 'dwh' CASCADE...")
    conn = get_db_connection()
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute("DROP SCHEMA IF EXISTS dwh CASCADE;")
        logger.info("✅ Schema 'dwh' dropped.")
        cur.close()
    except Exception as e:
        logger.error(f"❌ Error dropping schema: {e}")
        raise
    finally:
        conn.close()


def execute_sql_file(sql_path):
    """Helper to execute a SQL file"""
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    logger.info(f"Executing SQL: {sql_path}")
    conn = get_db_connection()
    conn.autocommit = True
    try:
        cur = conn.cursor()
        with open(sql_path, "r", encoding="utf-8") as f:
            cur.execute(f.read())
        logger.info(f"✅ Executed {os.path.basename(sql_path)}")
        cur.close()
    except Exception as e:
        logger.error(f"❌ Error executing {sql_path}: {e}")
        raise
    finally:
        conn.close()


def create_dimensional_tables(**context):
    execute_sql_file("/opt/airflow/sql/init-dimensional-tables.sql")


def create_fact_tables(**context):
    execute_sql_file("/opt/airflow/sql/init-fact-tables.sql")


def populate_dimensions(**context):
    """Uses DimensionalLoader to populate data"""
    logger.info("Populating dimensional tables...")
    loader = DimensionalLoader()
    loader.load_all_dimensional_tables()
    logger.info("✅ Dimensional tables populated.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    "reset_database_pipeline",
    default_args=default_args,
    description="FULL RESET: Drop Schema -> Create Dims -> Load Dims -> Create Facts",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 1, 27),
    catchup=False,
    tags=["maintenance", "reset", "dangerous"],
) as dag:

    # Task 1: Drop Schema
    task_drop_schema = PythonOperator(
        task_id="drop_dwh_schema",
        python_callable=drop_dwh_schema,
    )

    # Task 2: Create Dimensional Tables
    task_create_dims = PythonOperator(
        task_id="create_dimensional_tables",
        python_callable=create_dimensional_tables,
    )

    # Task 3: Populate Dimensions (City, Date)
    task_populate_dims = PythonOperator(
        task_id="populate_dimensional_tables",
        python_callable=populate_dimensions,
    )

    # Task 4: Create Fact Tables
    task_create_facts = PythonOperator(
        task_id="create_fact_tables",
        python_callable=create_fact_tables,
    )

    # Flow
    task_drop_schema >> task_create_dims >> task_populate_dims >> task_create_facts
