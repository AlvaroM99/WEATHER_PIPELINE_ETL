"""
Backfill Pipeline DAG

Parametrized DAG for re-executing failed or missing ETL runs.
Detects gaps in etl_run_log and re-runs extraction, transformation, and loading
for specified date ranges and data sources.

Usage:
    airflow dags trigger backfill_pipeline \
        --conf '{"start_date": "2024-01-01", "end_date": "2024-01-07", "data_source": "openmeteo"}'
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.models.param import Param

from src.config.db_pool import get_db_connection, return_db_connection
from src.extractor import Extractor
from src.loader import Loader
from src.transformer import Transformer
from src.utils.etl_logger import BaseETLLogger


class BackfillOrchestrator(BaseETLLogger):
    """Orchestrates backfill operations with gap detection and validation."""

    def __init__(self) -> None:
        """Initialize backfill orchestrator."""
        super().__init__()

    def detect_gaps(
        self, start_date: str, end_date: str, data_source: str = "all"
    ) -> List[Dict[str, str]]:
        """
        Detect missing or failed ETL runs in the specified date range.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            data_source: Data source filter ('all', 'openweather', 'openmeteo', 'aemet')

        Returns:
            List of dicts with {execution_date, dag_id, task_id, status}
        """
        conn = get_db_connection()
        try:
            cur = conn.cursor()

            # Build task filter based on data source
            task_filter = ""
            if data_source == "openweather":
                task_filter = "AND task_id LIKE '%openweather%'"
            elif data_source == "openmeteo":
                task_filter = "AND task_id LIKE '%openmeteo%'"
            elif data_source == "aemet":
                task_filter = "AND task_id LIKE '%aemet%'"

            # Find all dates in range
            query = f"""
                WITH date_range AS (
                    SELECT generate_series(
                        %s::date,
                        %s::date,
                        '1 day'::interval
                    )::date AS execution_date
                ),
                expected_tasks AS (
                    SELECT 
                        execution_date,
                        'extraction_pipeline' AS dag_id,
                        task_id
                    FROM date_range
                    CROSS JOIN (
                        SELECT unnest(ARRAY[
                            'extract_openweather',
                            'extract_openmeteo_daily',
                            'extract_openmeteo_hourly',
                            'extract_openmeteo_air_quality',
                            'extract_openmeteo_pollen',
                            'extract_openmeteo_marine',
                            'extract_aemet_stations',
                            'extract_aemet_daily_climatology'
                        ]) AS task_id
                    ) tasks
                    WHERE 1=1 {task_filter}
                )
                SELECT 
                    et.execution_date::text,
                    et.dag_id,
                    et.task_id,
                    COALESCE(erl.status, 'missing') AS status
                FROM expected_tasks et
                LEFT JOIN dwh.etl_run_log erl 
                    ON et.execution_date = erl.execution_date
                    AND et.dag_id = erl.dag_id
                    AND et.task_id = erl.task_id
                WHERE erl.status IS NULL OR erl.status = 'failed'
                ORDER BY et.execution_date, et.task_id
            """

            cur.execute(query, (start_date, end_date))
            gaps = [
                {
                    "execution_date": row[0],
                    "dag_id": row[1],
                    "task_id": row[2],
                    "status": row[3],
                }
                for row in cur.fetchall()
            ]

            cur.close()
            self.logger.info(f"Detected {len(gaps)} gaps/failures in date range {start_date} to {end_date}")
            return gaps

        finally:
            return_db_connection(conn)

    def check_existing_data(self, execution_date: str, task_id: str) -> bool:
        """
        Check if data already exists for a specific execution date and task.

        Args:
            execution_date: Execution date (YYYY-MM-DD)
            task_id: Task identifier

        Returns:
            True if data exists and is successful, False otherwise
        """
        conn = get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT status FROM dwh.etl_run_log
                WHERE execution_date = %s
                AND task_id = %s
                AND status = 'success'
                """,
                (execution_date, task_id),
            )
            result = cur.fetchone()
            cur.close()
            return result is not None
        finally:
            return_db_connection(conn)


@dag(
    dag_id="backfill_pipeline",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["backfill", "maintenance"],
    params={
        "start_date": Param(
            "2024-01-01",
            type="string",
            description="Start date for backfill (YYYY-MM-DD)",
        ),
        "end_date": Param(
            "2024-01-31",
            type="string",
            description="End date for backfill (YYYY-MM-DD)",
        ),
        "data_source": Param(
            "all",
            enum=["all", "openweather", "openmeteo", "aemet"],
            description="Data source to backfill",
        ),
        "force_rerun": Param(
            False,
            type="boolean",
            description="Force re-run even if data exists",
        ),
    },
    description="Backfill missing or failed ETL runs for a specified date range",
)
def backfill_pipeline():
    """
    Backfill Pipeline DAG.

    Detects gaps in ETL execution and re-runs extraction, transformation,
    and loading for missing or failed dates.
    """

    @task
    def detect_gaps_task(**context: Any) -> List[Dict[str, str]]:
        """Detect gaps in ETL execution."""
        params = context["params"]
        start_date = params["start_date"]
        end_date = params["end_date"]
        data_source = params["data_source"]

        orchestrator = BackfillOrchestrator()
        gaps = orchestrator.detect_gaps(start_date, end_date, data_source)

        if not gaps:
            orchestrator.logger.info("No gaps detected, backfill not needed")
        else:
            orchestrator.logger.info(f"Found {len(gaps)} tasks to backfill")

        return gaps

    @task
    def backfill_extraction(**context: Any) -> Dict[str, int]:
        """Re-run extraction for missing dates."""
        gaps = context["ti"].xcom_pull(task_ids="detect_gaps_task")
        params = context["params"]
        force_rerun = params["force_rerun"]

        if not gaps:
            return {"skipped": 0}

        orchestrator = BackfillOrchestrator()
        extractor = Extractor()

        results = {"success": 0, "failed": 0, "skipped": 0}

        # Group gaps by execution_date
        gaps_by_date: Dict[str, List[str]] = {}
        for gap in gaps:
            date = gap["execution_date"]
            task = gap["task_id"]
            if date not in gaps_by_date:
                gaps_by_date[date] = []
            gaps_by_date[date].append(task)

        for execution_date, tasks in gaps_by_date.items():
            for task_id in tasks:
                # Skip if data exists and not forcing rerun
                if not force_rerun and orchestrator.check_existing_data(execution_date, task_id):
                    orchestrator.logger.info(f"Skipping {task_id} for {execution_date} (data exists)")
                    results["skipped"] += 1
                    continue

                try:
                    # Map task_id to extractor method
                    method_map = {
                        "extract_openweather": extractor.extract_openweather,
                        "extract_openmeteo_daily": extractor.extract_openmeteo_daily,
                        "extract_openmeteo_hourly": extractor.extract_openmeteo_hourly,
                        "extract_openmeteo_air_quality": extractor.extract_openmeteo_air_quality,
                        "extract_openmeteo_pollen": extractor.extract_openmeteo_pollen,
                        "extract_openmeteo_marine": extractor.extract_openmeteo_marine,
                        "extract_aemet_stations": extractor.extract_aemet_stations,
                        "extract_aemet_daily_climatology": extractor.extract_aemet_daily_climatology,
                    }

                    method = method_map.get(task_id)
                    if method:
                        orchestrator.logger.info(f"Backfilling {task_id} for {execution_date}")
                        method(ds=execution_date)
                        results["success"] += 1
                    else:
                        orchestrator.logger.warning(f"Unknown task_id: {task_id}")
                        results["failed"] += 1

                except Exception as e:
                    orchestrator.logger.error(f"Failed to backfill {task_id} for {execution_date}: {e}")
                    results["failed"] += 1

        orchestrator.logger.info(f"Backfill extraction results: {results}")
        return results

    @task
    def backfill_transformation(**context: Any) -> Dict[str, int]:
        """Re-run transformation for backfilled dates."""
        extraction_results = context["ti"].xcom_pull(task_ids="backfill_extraction")

        if not extraction_results or extraction_results.get("success", 0) == 0:
            return {"skipped": 0}

        gaps = context["ti"].xcom_pull(task_ids="detect_gaps_task")
        orchestrator = BackfillOrchestrator()
        transformer = Transformer()

        results = {"success": 0, "failed": 0}

        # Group gaps by execution_date
        gaps_by_date: Dict[str, List[str]] = {}
        for gap in gaps:
            date = gap["execution_date"]
            task = gap["task_id"]
            if date not in gaps_by_date:
                gaps_by_date[date] = []
            gaps_by_date[date].append(task)

        for execution_date, tasks in gaps_by_date.items():
            for task_id in tasks:
                try:
                    # Map extraction task to transformation task
                    transform_map = {
                        "extract_openweather": transformer.transform_openweather,
                        "extract_openmeteo_daily": transformer.transform_openmeteo_daily,
                        "extract_openmeteo_hourly": transformer.transform_openmeteo_hourly,
                        "extract_openmeteo_air_quality": transformer.transform_openmeteo_air_quality,
                        "extract_openmeteo_pollen": transformer.transform_openmeteo_pollen,
                        "extract_openmeteo_marine": transformer.transform_openmeteo_marine,
                        "extract_aemet_stations": transformer.transform_aemet_stations,
                        "extract_aemet_daily_climatology": transformer.transform_aemet_daily_climatology,
                    }

                    method = transform_map.get(task_id)
                    if method:
                        orchestrator.logger.info(f"Transforming {task_id} for {execution_date}")
                        method(ds=execution_date)
                        results["success"] += 1

                except Exception as e:
                    orchestrator.logger.error(f"Failed to transform {task_id} for {execution_date}: {e}")
                    results["failed"] += 1

        orchestrator.logger.info(f"Backfill transformation results: {results}")
        return results

    @task
    def backfill_loading(**context: Any) -> Dict[str, int]:
        """Re-run loading for backfilled dates."""
        transformation_results = context["ti"].xcom_pull(task_ids="backfill_transformation")

        if not transformation_results or transformation_results.get("success", 0) == 0:
            return {"skipped": 0}

        gaps = context["ti"].xcom_pull(task_ids="detect_gaps_task")
        orchestrator = BackfillOrchestrator()
        loader = Loader()

        results = {"success": 0, "failed": 0}

        # Group gaps by execution_date
        gaps_by_date: Dict[str, List[str]] = {}
        for gap in gaps:
            date = gap["execution_date"]
            task = gap["task_id"]
            if date not in gaps_by_date:
                gaps_by_date[date] = []
            gaps_by_date[date].append(task)

        for execution_date, tasks in gaps_by_date.items():
            for task_id in tasks:
                try:
                    # Map extraction task to loading task
                    load_map = {
                        "extract_openweather": loader.load_fact_weather_observation,
                        "extract_openmeteo_daily": loader.load_fact_forecast_daily,
                        "extract_openmeteo_hourly": loader.load_fact_forecast_hourly,
                        "extract_openmeteo_air_quality": loader.load_fact_air_quality,
                        "extract_openmeteo_pollen": loader.load_fact_pollen,
                        "extract_openmeteo_marine": loader.load_fact_marine,
                        "extract_aemet_daily_climatology": loader.load_fact_aemet_daily,
                    }

                    method = load_map.get(task_id)
                    if method:
                        orchestrator.logger.info(f"Loading {task_id} for {execution_date}")
                        method(ds=execution_date)
                        results["success"] += 1

                except Exception as e:
                    orchestrator.logger.error(f"Failed to load {task_id} for {execution_date}: {e}")
                    results["failed"] += 1

        orchestrator.logger.info(f"Backfill loading results: {results}")
        return results

    # Define task dependencies
    gaps = detect_gaps_task()
    extraction = backfill_extraction()
    transformation = backfill_transformation()
    loading = backfill_loading()

    gaps >> extraction >> transformation >> loading


# Instantiate the DAG
backfill_dag = backfill_pipeline()
