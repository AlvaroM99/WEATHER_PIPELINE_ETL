"""
Base Loader Module

Abstract base class providing database connection management
via context manager and standardized logging for all loaders.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Generator, Optional

import psycopg2

from src.config.db_pool import get_db_connection, return_db_connection
from src.utils.etl_logger import BaseETLLogger


class BaseLoader(BaseETLLogger):
    """
    Abstract base for Loader and DimensionalLoader.

    Provides:
        - connection() context manager with auto commit/rollback
        - Logging via BaseETLLogger

    Attributes:
        logger: Logger instance named after the concrete class
    """

    def __init__(self) -> None:
        """Initialize BaseLoader with logger."""
        super().__init__()

    @contextmanager
    def connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """
        Context manager for database connections.

        Yields a connection from the pool. Commits on clean exit,
        rolls back on exception, and always returns to pool.

        Yields:
            psycopg2 connection object
        """
        conn = get_db_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            return_db_connection(conn)

    # ========================================================================
    # ETL Run Logging for Observability
    # ========================================================================

    def log_etl_start(
        self, dag_id: str, task_id: str, execution_date: str, conn: psycopg2.extensions.connection
    ) -> int:
        """
        Log the start of an ETL run to etl_run_log table.

        Args:
            dag_id: Airflow DAG identifier
            task_id: Airflow task identifier
            execution_date: Logical execution date (YYYY-MM-DD)
            conn: Active database connection

        Returns:
            run_id: Primary key of the created etl_run_log entry
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dwh.etl_run_log (
                    dag_id, task_id, execution_date, start_time, status
                ) VALUES (%s, %s, %s, %s, 'running')
                ON CONFLICT (dag_id, task_id, execution_date)
                DO UPDATE SET
                    start_time = EXCLUDED.start_time,
                    status = 'running',
                    end_time = NULL,
                    error_message = NULL
                RETURNING id
                """,
                (dag_id, task_id, execution_date, datetime.now()),
            )
            result = cur.fetchone()
            run_id = result[0] if result else 0
            self.logger.debug(f"ETL run started: {dag_id}.{task_id} [{execution_date}] - run_id={run_id}")
            return run_id

    def log_etl_end(
        self,
        run_id: int,
        records_processed: int,
        conn: psycopg2.extensions.connection,
        status: str = "success",
        error_message: str | None = None,
    ) -> None:
        """
        Log the completion of an ETL run to etl_run_log table.

        Args:
            run_id: Primary key from log_etl_start
            records_processed: Number of records processed
            conn: Active database connection
            status: 'success' or 'failed'
            error_message: Error message if status is 'failed'
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE dwh.etl_run_log
                SET end_time = %s,
                    status = %s,
                    records_processed = %s,
                    error_message = %s,
                    duration_seconds = EXTRACT(EPOCH FROM (%s - start_time))::INTEGER,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (datetime.now(), status, records_processed, error_message, datetime.now(), run_id),
            )
            self.logger.debug(f"ETL run ended: run_id={run_id}, status={status}, records={records_processed}")

    def log_to_lake_metadata(
        self,
        bucket_name: str,
        object_path: str,
        layer: str,
        data_source: str,
        record_count: int,
        file_size_bytes: int,
        conn: psycopg2.extensions.connection,
        status: str = "success",
    ) -> None:
        """
        Log file metadata to lake_metadata table for data lineage tracking.

        Args:
            bucket_name: MinIO bucket name
            object_path: Full path to object in bucket
            layer: 'bronze' or 'silver'
            data_source: Source identifier (e.g., 'openweather', 'openmeteo', 'aemet')
            record_count: Number of records in the file
            file_size_bytes: File size in bytes
            conn: Active database connection
            status: 'success' or 'failed'
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO lake_metadata (
                    bucket_name, object_path, layer, data_source,
                    record_count, file_size_bytes, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (object_path) DO UPDATE SET
                    record_count = EXCLUDED.record_count,
                    file_size_bytes = EXCLUDED.file_size_bytes,
                    status = EXCLUDED.status,
                    load_timestamp = CURRENT_TIMESTAMP
                """,
                (bucket_name, object_path, layer, data_source, record_count, file_size_bytes, status),
            )
            self.logger.debug(
                f"Lake metadata logged: {layer}/{data_source} - {object_path} ({record_count} records)"
            )

    def log_etl_metrics(
        self,
        etl_run_id: int,
        execution_date: str,
        target_table: str,
        data_source: str,
        rows_input: int,
        rows_loaded: int,
        rows_rejected: int,
        conn: psycopg2.extensions.connection,
        completeness_score: Optional[float] = None,
        validity_score: Optional[float] = None,
        uniqueness_score: Optional[float] = None,
        freshness_score: Optional[float] = None,
        overall_quality_score: Optional[float] = None,
        quality_details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Persist data quality metrics to dwh.etl_metrics for Metabase dashboard.

        Args:
            etl_run_id: FK to etl_run_log.id
            execution_date: Logical execution date (YYYY-MM-DD)
            target_table: Destination table name (e.g. 'fct_weather_observation')
            data_source: Origin identifier ('openweather', 'openmeteo', 'aemet')
            rows_input: Rows read from Silver layer
            rows_loaded: Rows inserted into Gold layer
            rows_rejected: Rows rejected by validation or ON CONFLICT
            conn: Active database connection
            completeness_score: Non-null ratio (0.0-1.0)
            validity_score: Passed expectations ratio (0.0-1.0)
            uniqueness_score: Unique keys ratio (0.0-1.0)
            freshness_score: Data recency score (0.0-1.0)
            overall_quality_score: Weighted average of dimension scores
            quality_details: Full quality report as JSON
        """
        with conn.cursor() as cur:
            details_json = json.dumps(quality_details) if quality_details else None
            cur.execute(
                """
                INSERT INTO dwh.etl_metrics (
                    etl_run_id, execution_date, target_table, data_source,
                    rows_input, rows_loaded, rows_rejected,
                    completeness_score, validity_score, uniqueness_score,
                    freshness_score, overall_quality_score, quality_details
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (etl_run_id, target_table) DO UPDATE SET
                    rows_input = EXCLUDED.rows_input,
                    rows_loaded = EXCLUDED.rows_loaded,
                    rows_rejected = EXCLUDED.rows_rejected,
                    completeness_score = EXCLUDED.completeness_score,
                    validity_score = EXCLUDED.validity_score,
                    uniqueness_score = EXCLUDED.uniqueness_score,
                    freshness_score = EXCLUDED.freshness_score,
                    overall_quality_score = EXCLUDED.overall_quality_score,
                    quality_details = EXCLUDED.quality_details,
                    measured_at = CURRENT_TIMESTAMP
                """,
                (
                    etl_run_id, execution_date, target_table, data_source,
                    rows_input, rows_loaded, rows_rejected,
                    completeness_score, validity_score, uniqueness_score,
                    freshness_score, overall_quality_score, details_json,
                ),
            )
            self.logger.debug(
                f"ETL metrics logged: {target_table} [{execution_date}] "
                f"input={rows_input} loaded={rows_loaded} rejected={rows_rejected} "
                f"quality={overall_quality_score}"
            )

