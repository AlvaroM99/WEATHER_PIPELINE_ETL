"""
Modular DAG: Transformation Only
Transforms data from Bronze (JSON) to Silver (Parquet) using Unified Transformer
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from src.transformer import Transformer


def run_transformation(method_name, **context):
    """Execute a specific transformation method via explicit dispatch."""
    transformer = Transformer()
    methods = {
        "transform_openweather": transformer.transform_openweather,
        "transform_openmeteo_daily": transformer.transform_openmeteo_daily,
        "transform_openmeteo_hourly": transformer.transform_openmeteo_hourly,
        "transform_openmeteo_air_quality": transformer.transform_openmeteo_air_quality,
        "transform_openmeteo_pollen": transformer.transform_openmeteo_pollen,
        "transform_openmeteo_marine": transformer.transform_openmeteo_marine,
        "transform_aemet_stations": transformer.transform_aemet_stations,
        "transform_aemet_daily_climatology": transformer.transform_aemet_daily_climatology,
    }
    method = methods.get(method_name)
    if method is None:
        raise ValueError(
            f"Unknown transformation method: '{method_name}'. "
            f"Valid methods: {list(methods)}"
        )
    return method(**context)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "transformation_pipeline",
    default_args=default_args,
    description="Transform data from Bronze to Silver layer",
    schedule_interval=None,
    start_date=datetime(2026, 1, 27),
    catchup=False,
    tags=["transformation", "silver", "modular", "unified"],
) as dag:

    start = EmptyOperator(task_id="start_transformation")

    transform_openweather = PythonOperator(
        task_id="transform_openweather",
        python_callable=run_transformation,
        op_kwargs={"method_name": "transform_openweather"},
    )

    transform_openmeteo_daily = PythonOperator(
        task_id="transform_openmeteo_daily",
        python_callable=run_transformation,
        op_kwargs={"method_name": "transform_openmeteo_daily"},
    )

    transform_openmeteo_hourly = PythonOperator(
        task_id="transform_openmeteo_hourly",
        python_callable=run_transformation,
        op_kwargs={"method_name": "transform_openmeteo_hourly"},
    )

    transform_openmeteo_air_quality = PythonOperator(
        task_id="transform_openmeteo_air_quality",
        python_callable=run_transformation,
        op_kwargs={"method_name": "transform_openmeteo_air_quality"},
    )

    transform_openmeteo_pollen = PythonOperator(
        task_id="transform_openmeteo_pollen",
        python_callable=run_transformation,
        op_kwargs={"method_name": "transform_openmeteo_pollen"},
    )

    transform_openmeteo_marine = PythonOperator(
        task_id="transform_openmeteo_marine",
        python_callable=run_transformation,
        op_kwargs={"method_name": "transform_openmeteo_marine"},
    )

    # AEMET Transformation Tasks
    transform_aemet_stations = PythonOperator(
        task_id="transform_aemet_stations",
        python_callable=run_transformation,
        op_kwargs={"method_name": "transform_aemet_stations"},
    )

    transform_aemet_daily = PythonOperator(
        task_id="transform_aemet_daily_climatology",
        python_callable=run_transformation,
        op_kwargs={"method_name": "transform_aemet_daily_climatology"},
    )

    end = EmptyOperator(task_id="transformation_complete")

    (
        start
        >> [
            transform_openweather,
            transform_openmeteo_daily,
            transform_openmeteo_hourly,
            transform_openmeteo_air_quality,
            transform_openmeteo_pollen,
            transform_openmeteo_marine,
            transform_aemet_stations,
            transform_aemet_daily,
        ]
        >> end
    )
