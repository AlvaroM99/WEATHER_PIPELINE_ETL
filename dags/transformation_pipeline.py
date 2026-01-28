"""
Modular DAG: Transformation Only
Transforms data from Bronze (JSON) to Silver (Parquet) using Unified Transformer
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from weather_etl.transformer import Transformer

def run_transformation(method_name, **context):
    transformer = Transformer()
    method = getattr(transformer, method_name)
    return method(**context)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'transformation_pipeline',
    default_args=default_args,
    description='Transform data from Bronze to Silver layer',
    schedule_interval=None,
    start_date=datetime(2026, 1, 27),
    catchup=False,
    tags=['transformation', 'silver', 'modular', 'unified'],
) as dag:
    
    start = EmptyOperator(task_id='start_transformation')
    
    transform_openweather = PythonOperator(
        task_id='transform_openweather',
        python_callable=run_transformation,
        op_kwargs={'method_name': 'transform_openweather'},
        provide_context=True,
    )
    
    transform_openmeteo_daily = PythonOperator(
        task_id='transform_openmeteo_daily',
        python_callable=run_transformation,
        op_kwargs={'method_name': 'transform_openmeteo_daily'},
        provide_context=True,
    )
    
    transform_openmeteo_hourly = PythonOperator(
        task_id='transform_openmeteo_hourly',
        python_callable=run_transformation,
        op_kwargs={'method_name': 'transform_openmeteo_hourly'},
        provide_context=True,
    )
    
    transform_openmeteo_air_quality = PythonOperator(
        task_id='transform_openmeteo_air_quality',
        python_callable=run_transformation,
        op_kwargs={'method_name': 'transform_openmeteo_air_quality'},
        provide_context=True,
    )
    
    transform_openmeteo_pollen = PythonOperator(
        task_id='transform_openmeteo_pollen',
        python_callable=run_transformation,
        op_kwargs={'method_name': 'transform_openmeteo_pollen'},
        provide_context=True,
    )
    
    transform_openmeteo_marine = PythonOperator(
        task_id='transform_openmeteo_marine',
        python_callable=run_transformation,
        op_kwargs={'method_name': 'transform_openmeteo_marine'},
        provide_context=True,
    )
    
    end = EmptyOperator(task_id='transformation_complete')
    
    start >> [transform_openweather, transform_openmeteo_daily, transform_openmeteo_hourly,
              transform_openmeteo_air_quality, transform_openmeteo_pollen, transform_openmeteo_marine] >> end

