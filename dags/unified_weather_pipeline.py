"""
Unified Weather Pipeline (Independent Execution)
This DAG executes the complete ETL process independently:
1. Extraction Pipeline (Bronze Layer)
2. Transformation Pipeline (Silver Layer)
3. Loading Pipeline (Gold Layer/DWH)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from src.extractor import Extractor
from src.transformer import Transformer
from src.loader import Loader


def run_extraction(method_name, **context):
    """Execute a specific extraction method"""
    extractor = Extractor()
    method = getattr(extractor, method_name)
    return method(**context)


def run_transformation(method_name, **context):
    """Execute a specific transformation method"""
    transformer = Transformer()
    method = getattr(transformer, method_name)
    return method(**context)


def run_loading(method_name, **context):
    """Execute a specific loading method"""
    loader = Loader()
    method = getattr(loader, method_name)
    return method(**context)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "unified_weather_pipeline",
    default_args=default_args,
    description="Independent ETL: Extraction -> Transformation -> Loading",
    schedule_interval="0 6 * * *",  # Daily at 6:00 AM
    start_date=datetime(2026, 1, 27),
    catchup=False,
    tags=["unified", "production", "independent"],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")

    # ==================== EXTRACTION PHASE ====================
    extraction_start = EmptyOperator(task_id="extraction_start")

    extract_openweather = PythonOperator(
        task_id="extract_openweather",
        python_callable=run_extraction,
        op_kwargs={"method_name": "extract_openweather"},
        provide_context=True,
    )

    extract_om_daily = PythonOperator(
        task_id="extract_openmeteo_daily",
        python_callable=run_extraction,
        op_kwargs={"method_name": "extract_openmeteo_daily"},
        provide_context=True,
    )

    extract_om_hourly = PythonOperator(
        task_id="extract_openmeteo_hourly",
        python_callable=run_extraction,
        op_kwargs={"method_name": "extract_openmeteo_hourly"},
        provide_context=True,
    )

    extract_om_air_quality = PythonOperator(
        task_id="extract_openmeteo_air_quality",
        python_callable=run_extraction,
        op_kwargs={"method_name": "extract_openmeteo_air_quality"},
        provide_context=True,
    )

    extract_om_pollen = PythonOperator(
        task_id="extract_openmeteo_pollen",
        python_callable=run_extraction,
        op_kwargs={"method_name": "extract_openmeteo_pollen"},
        provide_context=True,
    )

    extract_om_marine = PythonOperator(
        task_id="extract_openmeteo_marine",
        python_callable=run_extraction,
        op_kwargs={"method_name": "extract_openmeteo_marine"},
        provide_context=True,
    )

    extract_aemet_stations = PythonOperator(
        task_id="extract_aemet_stations",
        python_callable=run_extraction,
        op_kwargs={"method_name": "extract_aemet_stations"},
        provide_context=True,
    )

    extract_aemet_daily = PythonOperator(
        task_id="extract_aemet_daily_climatology",
        python_callable=run_extraction,
        op_kwargs={"method_name": "extract_aemet_daily_climatology"},
        provide_context=True,
    )

    extraction_end = EmptyOperator(task_id="extraction_complete")

    # ==================== TRANSFORMATION PHASE ====================
    transformation_start = EmptyOperator(task_id="transformation_start")

    transform_openweather = PythonOperator(
        task_id="transform_openweather",
        python_callable=run_transformation,
        op_kwargs={"method_name": "transform_openweather"},
        provide_context=True,
    )

    transform_om_daily = PythonOperator(
        task_id="transform_openmeteo_daily",
        python_callable=run_transformation,
        op_kwargs={"method_name": "transform_openmeteo_daily"},
        provide_context=True,
    )

    transform_om_hourly = PythonOperator(
        task_id="transform_openmeteo_hourly",
        python_callable=run_transformation,
        op_kwargs={"method_name": "transform_openmeteo_hourly"},
        provide_context=True,
    )

    transform_om_air_quality = PythonOperator(
        task_id="transform_openmeteo_air_quality",
        python_callable=run_transformation,
        op_kwargs={"method_name": "transform_openmeteo_air_quality"},
        provide_context=True,
    )

    transform_om_pollen = PythonOperator(
        task_id="transform_openmeteo_pollen",
        python_callable=run_transformation,
        op_kwargs={"method_name": "transform_openmeteo_pollen"},
        provide_context=True,
    )

    transform_om_marine = PythonOperator(
        task_id="transform_openmeteo_marine",
        python_callable=run_transformation,
        op_kwargs={"method_name": "transform_openmeteo_marine"},
        provide_context=True,
    )

    transform_aemet_stations = PythonOperator(
        task_id="transform_aemet_stations",
        python_callable=run_transformation,
        op_kwargs={"method_name": "transform_aemet_stations"},
        provide_context=True,
    )

    transform_aemet_daily = PythonOperator(
        task_id="transform_aemet_daily_climatology",
        python_callable=run_transformation,
        op_kwargs={"method_name": "transform_aemet_daily_climatology"},
        provide_context=True,
    )

    transformation_end = EmptyOperator(task_id="transformation_complete")

    # ==================== LOADING PHASE ====================
    loading_start = EmptyOperator(task_id="loading_start")

    load_weather_obs = PythonOperator(
        task_id="load_fct_observation",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_fact_observation"},
        provide_context=True,
    )

    load_daily_weather = PythonOperator(
        task_id="load_fct_forecast_daily",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_fact_forecast_daily"},
        provide_context=True,
    )

    load_hourly_weather = PythonOperator(
        task_id="load_fct_forecast_hourly",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_fact_forecast_hourly"},
        provide_context=True,
    )

    load_air_quality = PythonOperator(
        task_id="load_fct_air_quality",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_fact_air_quality"},
        provide_context=True,
    )

    load_pollen = PythonOperator(
        task_id="load_fct_pollen",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_fact_pollen"},
        provide_context=True,
    )

    load_marine = PythonOperator(
        task_id="load_fct_marine",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_fact_marine"},
        provide_context=True,
    )

    load_aemet_stations = PythonOperator(
        task_id="load_aemet_stations",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_aemet_stations"},
        provide_context=True,
    )

    load_aemet_daily = PythonOperator(
        task_id="load_fct_aemet_daily",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_fact_aemet_daily"},
        provide_context=True,
    )

    loading_end = EmptyOperator(task_id="loading_complete")

    end = EmptyOperator(task_id="pipeline_complete")

    # ==================== DEPENDENCIES ====================
    # Extraction Phase
    (
        start
        >> extraction_start
        >> [
            extract_openweather,
            extract_om_daily,
            extract_om_hourly,
            extract_om_air_quality,
            extract_om_pollen,
            extract_om_marine,
            extract_aemet_stations,
            extract_aemet_daily,
        ]
        >> extraction_end
    )

    # Transformation Phase
    (
        extraction_end
        >> transformation_start
        >> [
            transform_openweather,
            transform_om_daily,
            transform_om_hourly,
            transform_om_air_quality,
            transform_om_pollen,
            transform_om_marine,
            transform_aemet_stations,
            transform_aemet_daily,
        ]
        >> transformation_end
    )

    # Loading Phase
    (
        transformation_end
        >> loading_start
        >> [
            load_weather_obs,
            load_daily_weather,
            load_hourly_weather,
            load_air_quality,
            load_pollen,
            load_marine,
            load_aemet_stations,
            load_aemet_daily,
        ]
        >> loading_end
    )

    # Final
    loading_end >> end
