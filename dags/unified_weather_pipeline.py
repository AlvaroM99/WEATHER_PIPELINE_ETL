"""
Unified Weather Pipeline
Complete ETL: Bronze → Silver → Gold
Executes all tasks directly in one DAG using Unified Manager Objects
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# ============================================================================
# UNIFIED MANAGER IMPORTS
# ============================================================================

from weather_etl.extractor import Extractor
from weather_etl.transformer import Transformer
from weather_etl.loader import Loader

# Helper functions to run methods from the Unified Managers
def run_extraction(method_name, **context):
    extractor = Extractor()
    method = getattr(extractor, method_name)
    return method(**context)

def run_transformation(method_name, **context):
    transformer = Transformer()
    method = getattr(transformer, method_name)
    return method(**context)

def run_loading(method_name, **context):
    loader = Loader()
    method = getattr(loader, method_name)
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
    'unified_weather_pipeline',
    default_args=default_args,
    description='Complete ETL pipeline using Unified Manager Architecture',
    schedule_interval='0 6 * * *',  # Daily at 6:00 AM
    start_date=datetime(2026, 1, 27),
    catchup=False,
    tags=['unified', 'production', 'etl', 'manager_pattern'],
) as dag:
    
    start = EmptyOperator(task_id='start_pipeline')
    
    # ========================================================================
    # PHASE 1: EXTRACTION (Bronze Layer)
    # ========================================================================
    
    extraction_start = EmptyOperator(task_id='extraction_start')
    
    extract_openweather = PythonOperator(
        task_id='extract_openweather',
        python_callable=run_extraction,
        op_kwargs={'method_name': 'extract_openweather'},
        provide_context=True,
    )
    
    extract_om_daily = PythonOperator(
        task_id='extract_openmeteo_daily',
        python_callable=run_extraction,
        op_kwargs={'method_name': 'extract_openmeteo_daily'},
        provide_context=True,
    )
    
    extract_om_hourly = PythonOperator(
        task_id='extract_openmeteo_hourly',
        python_callable=run_extraction,
        op_kwargs={'method_name': 'extract_openmeteo_hourly'},
        provide_context=True,
    )
    
    extract_om_air_quality = PythonOperator(
        task_id='extract_openmeteo_air_quality',
        python_callable=run_extraction,
        op_kwargs={'method_name': 'extract_openmeteo_air_quality'},
        provide_context=True,
    )
    
    extract_om_pollen = PythonOperator(
        task_id='extract_openmeteo_pollen',
        python_callable=run_extraction,
        op_kwargs={'method_name': 'extract_openmeteo_pollen'},
        provide_context=True,
    )
    
    extract_om_marine = PythonOperator(
        task_id='extract_openmeteo_marine',
        python_callable=run_extraction,
        op_kwargs={'method_name': 'extract_openmeteo_marine'},
        provide_context=True,
    )
    
    extraction_complete = EmptyOperator(task_id='extraction_complete')
    
    # ========================================================================
    # PHASE 2: TRANSFORMATION (Silver Layer)
    # ========================================================================
    
    transformation_start = EmptyOperator(task_id='transformation_start')
    
    transform_openweather_task = PythonOperator(
        task_id='transform_openweather',
        python_callable=run_transformation,
        op_kwargs={'method_name': 'transform_openweather'},
        provide_context=True,
    )
    
    transform_openmeteo_daily_task = PythonOperator(
        task_id='transform_openmeteo_daily',
        python_callable=run_transformation,
        op_kwargs={'method_name': 'transform_openmeteo_daily'},
        provide_context=True,
    )

    transform_openmeteo_hourly_task = PythonOperator(
        task_id='transform_openmeteo_hourly',
        python_callable=run_transformation,
        op_kwargs={'method_name': 'transform_openmeteo_hourly'},
        provide_context=True,
    )

    transform_openmeteo_air_quality_task = PythonOperator(
        task_id='transform_openmeteo_air_quality',
        python_callable=run_transformation,
        op_kwargs={'method_name': 'transform_openmeteo_air_quality'},
        provide_context=True,
    )

    transform_openmeteo_pollen_task = PythonOperator(
        task_id='transform_openmeteo_pollen',
        python_callable=run_transformation,
        op_kwargs={'method_name': 'transform_openmeteo_pollen'},
        provide_context=True,
    )

    transform_openmeteo_marine_task = PythonOperator(
        task_id='transform_openmeteo_marine',
        python_callable=run_transformation,
        op_kwargs={'method_name': 'transform_openmeteo_marine'},
        provide_context=True,
    )
    
    transformation_complete = EmptyOperator(task_id='transformation_complete')
    
    # ========================================================================
    # PHASE 3: LOADING (Gold Layer - DWH)
    # ========================================================================
    
    loading_start = EmptyOperator(task_id='loading_start')
    
    load_observation = PythonOperator(
        task_id='load_fct_observation',
        python_callable=run_loading,
        op_kwargs={'method_name': 'load_fact_observation'},
        provide_context=True,
    )
    
    load_forecast_daily = PythonOperator(
        task_id='load_fct_forecast_daily',
        python_callable=run_loading,
        op_kwargs={'method_name': 'load_fact_forecast_daily'},
        provide_context=True,
    )

    load_forecast_hourly = PythonOperator(
        task_id='load_fct_forecast_hourly',
        python_callable=run_loading,
        op_kwargs={'method_name': 'load_fact_forecast_hourly'},
        provide_context=True,
    )

    load_air_quality = PythonOperator(
        task_id='load_fct_air_quality',
        python_callable=run_loading,
        op_kwargs={'method_name': 'load_fact_air_quality'},
        provide_context=True,
    )

    load_pollen = PythonOperator(
        task_id='load_fct_pollen',
        python_callable=run_loading,
        op_kwargs={'method_name': 'load_fact_pollen'},
        provide_context=True,
    )

    load_marine = PythonOperator(
        task_id='load_fct_marine',
        python_callable=run_loading,
        op_kwargs={'method_name': 'load_fact_marine'},
        provide_context=True,
    )

    loading_complete = EmptyOperator(task_id='loading_complete')
    
    end = EmptyOperator(task_id='pipeline_complete')
    
    # ========================================================================
    # DEPENDENCIES
    # ========================================================================
    
    # Phase 1: Extraction (all in parallel)
    start >> extraction_start
    extraction_start >> [extract_openweather, extract_om_daily, extract_om_hourly,
                         extract_om_air_quality, extract_om_pollen, extract_om_marine]
    [extract_openweather, extract_om_daily, extract_om_hourly,
     extract_om_air_quality, extract_om_pollen, extract_om_marine] >> extraction_complete
    
    # Phase 2: Transformation (after extraction)
    extraction_complete >> transformation_start
    transformation_start >> [transform_openweather_task, transform_openmeteo_daily_task,
                             transform_openmeteo_hourly_task, transform_openmeteo_air_quality_task,
                             transform_openmeteo_pollen_task, transform_openmeteo_marine_task]
    [transform_openweather_task, transform_openmeteo_daily_task, 
     transform_openmeteo_hourly_task, transform_openmeteo_air_quality_task,
     transform_openmeteo_pollen_task, transform_openmeteo_marine_task] >> transformation_complete
    
    # Phase 3: Loading (after transformation)
    transformation_complete >> loading_start
    loading_start >> [load_observation, load_forecast_daily, load_forecast_hourly, 
                      load_air_quality, load_pollen, load_marine]
    [load_observation, load_forecast_daily, load_forecast_hourly, 
     load_air_quality, load_pollen, load_marine] >> loading_complete
    
    # Complete
    loading_complete >> end


