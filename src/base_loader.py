"""
Base Loader Module

Abstract base class providing database connection management
via context manager and standardized logging for all loaders.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

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
        from datetime import datetime

        cur = conn.cursor()
        try:
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
            conn.commit()
            self.logger.debug(f"ETL run started: {dag_id}.{task_id} [{execution_date}] - run_id={run_id}")
            return run_id
        finally:
            cur.close()

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
        from datetime import datetime

        cur = conn.cursor()
        try:
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
            conn.commit()
            self.logger.debug(f"ETL run ended: run_id={run_id}, status={status}, records={records_processed}")
        finally:
            cur.close()

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
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO lake_metadata (
                    bucket_name, object_path, layer, data_source,
                    record_count, file_size_bytes, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (bucket_name, object_path, layer, data_source, record_count, file_size_bytes, status),
            )
            conn.commit()
            self.logger.debug(
                f"Lake metadata logged: {layer}/{data_source} - {object_path} ({record_count} records)"
            )
        finally:
            cur.close()

