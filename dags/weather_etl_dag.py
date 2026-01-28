"""
Weather ETL Pipeline DAG - Medallion Architecture
Extracts weather data from OpenWeatherMap API, stores in Bronze layer (raw JSON),
transforms to Silver layer (cleaned Parquet), and loads to Gold layer (PostgreSQL DWH)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import psycopg2
import os
import logging
import sys
import pandas as pd

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.minio_client import MinIOClient
from config.lake_config import BRONZE_BUCKET, SILVER_BUCKET, BRONZE_PATH_TEMPLATE, SILVER_PATH_TEMPLATE

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
API_KEY = os.getenv('OPENWEATHER_API_KEY')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_HOST = 'postgres'  # Docker service name

# Cities to fetch weather data for (major Spanish cities)
CITIES = [
    {'name': 'Madrid', 'lat': 40.4168, 'lon': -3.7038},
    {'name': 'Barcelona', 'lat': 41.3851, 'lon': 2.1734},
    {'name': 'Valencia', 'lat': 39.4699, 'lon': -0.3763},
    {'name': 'Sevilla', 'lat': 37.3891, 'lon': -5.9845},
    {'name': 'Bilbao', 'lat': 43.2630, 'lon': -2.9350},
    {'name': 'Málaga', 'lat': 36.7213, 'lon': -4.4214},
    {'name': 'Zaragoza', 'lat': 41.6488, 'lon': -0.8891},
]

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline with Medallion Architecture: Bronze (raw) → Silver (cleaned) → Gold (DWH)',
    schedule_interval='0 6 * * *',  # Run daily at 6:00 AM
    catchup=False,
    tags=['weather', 'etl', 'medallion', 'data-lake'],
)

# Define tasks following Medallion Architecture
extract_task = PythonOperator(
    task_id='extract_to_bronze',
    python_callable=extract_to_bronze,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_to_silver',
    python_callable=transform_to_silver,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_from_silver_to_dwh',
    python_callable=load_from_silver_to_dwh,
    provide_context=True,
    dag=dag,
)

# Set task dependencies: Bronze → Silver → Gold
extract_task >> transform_task >> load_task
