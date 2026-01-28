"""
Unified Weather Pipeline (Orchestrator)
This DAG acts as a master controller that triggers the modular DAGs in sequence:
1. Extraction Pipeline (Bronze Layer)
2. Transformation Pipeline (Silver Layer)
3. Loading Pipeline (Gold Layer/DWH)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'unified_weather_pipeline',
    default_args=default_args,
    description='Orchestrator DAG: Triggers Extraction -> Transformation -> Loading',
    schedule_interval='0 6 * * *',  # Daily at 6:00 AM
    start_date=datetime(2026, 1, 27),
    catchup=False,
    tags=['orchestrator', 'unified', 'production'],
) as dag:
    
    start = EmptyOperator(task_id='start_orchestration')
    
    # Step 1: Trigger Extraction
    trigger_extraction = TriggerDagRunOperator(
        task_id='trigger_extraction_pipeline',
        trigger_dag_id='extraction_pipeline',
        wait_for_completion=True,  # Wait for extraction to finish before moving on
        poke_interval=30,          # Check status every 30 seconds
        reset_dag_run=True,        # Allow re-running the same logical date
        failed_states=['failed'], # Helper to catch failures
    )
    
    # Step 2: Trigger Transformation
    trigger_transformation = TriggerDagRunOperator(
        task_id='trigger_transformation_pipeline',
        trigger_dag_id='transformation_pipeline',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        failed_states=['failed'],
    )
    
    # Step 3: Trigger Loading
    trigger_loading = TriggerDagRunOperator(
        task_id='trigger_loading_pipeline',
        trigger_dag_id='loading_pipeline',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        failed_states=['failed'],
    )
    
    end = EmptyOperator(task_id='orchestration_complete')
    
    # Define Dependencies
    start >> trigger_extraction >> trigger_transformation >> trigger_loading >> end
