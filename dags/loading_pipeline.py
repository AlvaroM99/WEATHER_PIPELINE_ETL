"""
Modular DAG: Loading Only
Loads data from Silver layer to Data Warehouse (Gold) using Unified Loader
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from src.loader import Loader


def run_loading(method_name, **context):
    """Execute a specific loading method via explicit dispatch."""
    loader = Loader()
    methods = {
        "load_fact_observation": loader.load_fact_observation,
        "load_fact_forecast_daily": loader.load_fact_forecast_daily,
        "load_fact_forecast_hourly": loader.load_fact_forecast_hourly,
        "load_fact_air_quality": loader.load_fact_air_quality,
        "load_fact_pollen": loader.load_fact_pollen,
        "load_fact_marine": loader.load_fact_marine,
        "load_aemet_stations": loader.load_aemet_stations,
        "load_fact_aemet_daily": loader.load_fact_aemet_daily,
    }
    method = methods.get(method_name)
    if method is None:
        raise ValueError(
            f"Unknown loading method: '{method_name}'. "
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
    "loading_pipeline",
    default_args=default_args,
    description="Load data from Silver layer to Data Warehouse (Gold)",
    schedule_interval=None,
    start_date=datetime(2026, 1, 27),
    catchup=False,
    tags=["loading", "gold", "modular", "unified"],
) as dag:

    start = EmptyOperator(task_id="start_loading")

    load_observation = PythonOperator(
        task_id="load_fct_observation",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_fact_observation"},
    )

    load_forecast_daily = PythonOperator(
        task_id="load_fct_forecast_daily",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_fact_forecast_daily"},
    )

    load_forecast_hourly = PythonOperator(
        task_id="load_fct_forecast_hourly",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_fact_forecast_hourly"},
    )

    load_air_quality = PythonOperator(
        task_id="load_fct_air_quality",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_fact_air_quality"},
    )

    load_pollen = PythonOperator(
        task_id="load_fct_pollen",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_fact_pollen"},
    )

    load_marine = PythonOperator(
        task_id="load_fct_marine",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_fact_marine"},
    )

    # AEMET Loading Tasks
    load_aemet_stations = PythonOperator(
        task_id="load_aemet_stations",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_aemet_stations"},
    )

    load_aemet_daily = PythonOperator(
        task_id="load_fct_aemet_daily",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_fact_aemet_daily"},
    )

    end = EmptyOperator(task_id="loading_complete")

    # Main loading tasks run in parallel
    (
        start
        >> [
            load_observation,
            load_forecast_daily,
            load_forecast_hourly,
            load_air_quality,
            load_pollen,
            load_marine,
            load_aemet_stations,
        ]
    )

    # AEMET stations must complete before AEMET daily data (FK dependency)
    load_aemet_stations >> load_aemet_daily

    # All tasks converge to end
    [
        load_observation,
        load_forecast_daily,
        load_forecast_hourly,
        load_air_quality,
        load_pollen,
        load_marine,
        load_aemet_daily,
    ] >> end
