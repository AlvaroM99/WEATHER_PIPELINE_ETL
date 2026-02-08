-- ============================================================================
-- ETL Audit and Lineage Tables
-- ============================================================================
-- This script creates tables for tracking ETL execution and data lineage

-- ETL Run Log: Track all ETL executions for observability and backfill
CREATE TABLE IF NOT EXISTS dwh.etl_run_log (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    task_id VARCHAR(100) NOT NULL,
    execution_date DATE NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    records_processed INTEGER,
    error_message TEXT,
    duration_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(dag_id, task_id, execution_date)
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_etl_run_log_status ON dwh.etl_run_log(status);
CREATE INDEX IF NOT EXISTS idx_etl_run_log_execution_date ON dwh.etl_run_log(execution_date);
CREATE INDEX IF NOT EXISTS idx_etl_run_log_dag_task ON dwh.etl_run_log(dag_id, task_id);
CREATE INDEX IF NOT EXISTS idx_etl_run_log_created_at ON dwh.etl_run_log(created_at);

-- Comments
COMMENT ON TABLE dwh.etl_run_log IS 'Tracks all ETL pipeline executions for observability, monitoring, and backfill gap detection';
COMMENT ON COLUMN dwh.etl_run_log.dag_id IS 'Airflow DAG identifier';
COMMENT ON COLUMN dwh.etl_run_log.task_id IS 'Airflow task identifier';
COMMENT ON COLUMN dwh.etl_run_log.execution_date IS 'Logical execution date for the ETL run';
COMMENT ON COLUMN dwh.etl_run_log.status IS 'Current status: running, success, or failed';
COMMENT ON COLUMN dwh.etl_run_log.records_processed IS 'Number of records processed in this run';
COMMENT ON COLUMN dwh.etl_run_log.error_message IS 'Error message if status is failed';
COMMENT ON COLUMN dwh.etl_run_log.duration_seconds IS 'Total execution time in seconds';
