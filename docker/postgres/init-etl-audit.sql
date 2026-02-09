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

-- Comments for etl_run_log
COMMENT ON TABLE dwh.etl_run_log IS 'Tracks all ETL pipeline executions for observability, monitoring, and backfill gap detection';
COMMENT ON COLUMN dwh.etl_run_log.dag_id IS 'Airflow DAG identifier';
COMMENT ON COLUMN dwh.etl_run_log.task_id IS 'Airflow task identifier';
COMMENT ON COLUMN dwh.etl_run_log.execution_date IS 'Logical execution date for the ETL run';
COMMENT ON COLUMN dwh.etl_run_log.status IS 'Current status: running, success, or failed';
COMMENT ON COLUMN dwh.etl_run_log.records_processed IS 'Number of records processed in this run';
COMMENT ON COLUMN dwh.etl_run_log.error_message IS 'Error message if status is failed';
COMMENT ON COLUMN dwh.etl_run_log.duration_seconds IS 'Total execution time in seconds';

-- ============================================================================
-- ETL Metrics: Data quality metrics per load for Metabase dashboard
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwh.etl_metrics (
    id SERIAL PRIMARY KEY,
    etl_run_id INTEGER REFERENCES dwh.etl_run_log(id),
    execution_date DATE NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    data_source VARCHAR(50) NOT NULL,
    rows_input INTEGER NOT NULL DEFAULT 0,
    rows_loaded INTEGER NOT NULL DEFAULT 0,
    rows_rejected INTEGER NOT NULL DEFAULT 0,
    completeness_score NUMERIC(5,4),
    validity_score NUMERIC(5,4),
    uniqueness_score NUMERIC(5,4),
    freshness_score NUMERIC(5,4),
    overall_quality_score NUMERIC(5,4),
    quality_details JSONB,
    measured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(etl_run_id, target_table)
);

-- Indexes for Metabase dashboard queries
CREATE INDEX IF NOT EXISTS idx_etl_metrics_execution_date ON dwh.etl_metrics(execution_date);
CREATE INDEX IF NOT EXISTS idx_etl_metrics_target_table ON dwh.etl_metrics(target_table);
CREATE INDEX IF NOT EXISTS idx_etl_metrics_data_source ON dwh.etl_metrics(data_source);
CREATE INDEX IF NOT EXISTS idx_etl_metrics_measured_at ON dwh.etl_metrics(measured_at);
CREATE INDEX IF NOT EXISTS idx_etl_metrics_quality ON dwh.etl_metrics(overall_quality_score);

-- Comments for etl_metrics
COMMENT ON TABLE dwh.etl_metrics IS 'Data quality metrics per ETL load, feeds Metabase observability dashboard';
COMMENT ON COLUMN dwh.etl_metrics.etl_run_id IS 'FK to etl_run_log for correlation';
COMMENT ON COLUMN dwh.etl_metrics.target_table IS 'Destination fact/dim table name';
COMMENT ON COLUMN dwh.etl_metrics.data_source IS 'Origin: openweather, openmeteo, aemet';
COMMENT ON COLUMN dwh.etl_metrics.rows_input IS 'Rows read from Silver layer';
COMMENT ON COLUMN dwh.etl_metrics.rows_loaded IS 'Rows successfully inserted into Gold';
COMMENT ON COLUMN dwh.etl_metrics.rows_rejected IS 'Rows rejected by validation or conflicts';
COMMENT ON COLUMN dwh.etl_metrics.completeness_score IS 'Ratio of non-null values (0.0 to 1.0)';
COMMENT ON COLUMN dwh.etl_metrics.validity_score IS 'Ratio of passed expectations (0.0 to 1.0)';
COMMENT ON COLUMN dwh.etl_metrics.uniqueness_score IS 'Ratio of unique key combinations (0.0 to 1.0)';
COMMENT ON COLUMN dwh.etl_metrics.freshness_score IS 'Data recency score (0.0 to 1.0)';
COMMENT ON COLUMN dwh.etl_metrics.overall_quality_score IS 'Weighted average of dimension scores';
COMMENT ON COLUMN dwh.etl_metrics.quality_details IS 'Full quality report as JSON (per-column scores, failed expectations)';
