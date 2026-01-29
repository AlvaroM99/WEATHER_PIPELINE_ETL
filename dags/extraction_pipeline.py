"""
Modular DAG: Extraction Only
Extracts data from all APIs to MinIO bronze layer using Unified Extractor
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from src.extractor import Extractor


def run_extraction(method_name, **context):
    extractor = Extractor()
    method = getattr(extractor, method_name)
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
    "extraction_pipeline",
    default_args=default_args,
    description="Extract data from all APIs to MinIO bronze layer",
    schedule_interval=None,
    start_date=datetime(2026, 1, 27),
    catchup=False,
    tags=["extraction", "bronze", "modular", "unified"],
) as dag:

    start = EmptyOperator(task_id="start_extraction")

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

    end = EmptyOperator(task_id="extraction_complete")

    (
        start
        >> [
            extract_openweather,
            extract_om_daily,
            extract_om_hourly,
            extract_om_air_quality,
            extract_om_pollen,
            extract_om_marine,
        ]
        >> end
    )
