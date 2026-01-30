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
    loader = Loader()
    method = getattr(loader, method_name)
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
        provide_context=True,
    )

    load_forecast_daily = PythonOperator(
        task_id="load_fct_forecast_daily",
        python_callable=run_loading,
        op_kwargs={"method_name": "load_fact_forecast_daily"},
        provide_context=True,
    )

    load_forecast_hourly = PythonOperator(
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

    # AEMET Loading Tasks
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
