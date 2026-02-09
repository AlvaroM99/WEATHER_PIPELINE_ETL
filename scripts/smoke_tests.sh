#!/bin/bash
# ============================================================================
# Smoke Tests for Weather ETL Pipeline
# ============================================================================
# Quick tests to verify basic functionality after deployment:
# 1. Airflow API is responding
# 2. DAGs are loaded
# 3. Database tables exist
# 4. MinIO buckets exist
# ============================================================================

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Configuration
AIRFLOW_WEBSERVER_URL="${AIRFLOW_WEBSERVER_URL:-http://localhost:8080}"
AIRFLOW_USER="${AIRFLOW_USER:-admin}"
AIRFLOW_PASSWORD="${AIRFLOW_PASSWORD:-admin}"

# ============================================================================
# Smoke Tests
# ============================================================================

test_airflow_api() {
    log_info "Testing Airflow API..."

    local response=$(curl -s -o /dev/null -w "%{http_code}" \
        -u "${AIRFLOW_USER}:${AIRFLOW_PASSWORD}" \
        "${AIRFLOW_WEBSERVER_URL}/api/v1/health")

    if [ "$response" = "200" ]; then
        log_info "✅ Airflow API is responding"
        return 0
    else
        log_error "❌ Airflow API returned: $response"
        return 1
    fi
}

test_dags_loaded() {
    log_info "Testing DAGs are loaded..."

    local response=$(curl -s -u "${AIRFLOW_USER}:${AIRFLOW_PASSWORD}" \
        "${AIRFLOW_WEBSERVER_URL}/api/v1/dags")

    local dag_count=$(echo "$response" | grep -o '"dag_id"' | wc -l)

    if [ "$dag_count" -gt 0 ]; then
        log_info "✅ Found $dag_count DAGs loaded"
        return 0
    else
        log_error "❌ No DAGs found"
        return 1
    fi
}

test_database_tables() {
    log_info "Testing database tables exist..."

    python3 << 'EOF'
import psycopg2
import os
import sys

try:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", 5432),
        user=os.getenv("POSTGRES_USER", "weather_user"),
        password=os.getenv("POSTGRES_PASSWORD", "weather_pass"),
        database=os.getenv("POSTGRES_DB", "weather_db")
    )

    cursor = conn.cursor()

    # Check critical tables
    critical_tables = ['cities', 'time_dim', 'weather_observations']

    for table in critical_tables:
        cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='{table}')")
        exists = cursor.fetchone()[0]
        if not exists:
            print(f"Table {table} does not exist")
            sys.exit(1)

    print(f"All critical tables exist")
    conn.close()
    sys.exit(0)

except Exception as e:
    print(f"Database test failed: {e}")
    sys.exit(1)
EOF

    if [ $? -eq 0 ]; then
        log_info "✅ Database tables exist"
        return 0
    else
        log_error "❌ Database tables check failed"
        return 1
    fi
}

test_minio_buckets() {
    log_info "Testing MinIO buckets..."

    python3 << 'EOF'
import os
import sys

try:
    from minio import Minio

    client = Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        secure=False
    )

    # Check if buckets exist
    buckets = [bucket.name for bucket in client.list_buckets()]

    required_buckets = ['bronze', 'silver', 'gold']
    missing_buckets = [b for b in required_buckets if b not in buckets]

    if missing_buckets:
        print(f"Missing buckets: {missing_buckets}")
        sys.exit(1)

    print(f"All required buckets exist: {buckets}")
    sys.exit(0)

except Exception as e:
    print(f"MinIO test failed: {e}")
    sys.exit(1)
EOF

    if [ $? -eq 0 ]; then
        log_info "✅ MinIO buckets exist"
        return 0
    else
        log_warn "⚠️  MinIO buckets check failed (non-critical)"
        return 0  # Non-critical
    fi
}

test_python_imports() {
    log_info "Testing critical Python imports..."

    python3 << 'EOF'
import sys
sys.path.insert(0, '/opt/airflow')

try:
    from src.extractor import Extractor
    from src.transformer import Transformer
    from src.loader import Loader
    from src.dimensional_loader import DimensionalLoader
    from src.utils.structured_logger import StructuredETLLogger
    from src.utils.rate_limiter import get_rate_limiter
    from src.config.db_pool import get_db_connection

    print("All critical modules imported successfully")
    sys.exit(0)

except ImportError as e:
    print(f"Import failed: {e}")
    sys.exit(1)
EOF

    if [ $? -eq 0 ]; then
        log_info "✅ Python imports successful"
        return 0
    else
        log_error "❌ Python imports failed"
        return 1
    fi
}

# ============================================================================
# Main Test Runner
# ============================================================================

main() {
    log_info "========================================="
    log_info "Weather ETL Pipeline - Smoke Tests"
    log_info "Environment: ${ENVIRONMENT:-development}"
    log_info "========================================="

    local exit_code=0

    # Run smoke tests
    test_airflow_api || exit_code=1
    test_dags_loaded || exit_code=1
    test_database_tables || exit_code=1
    test_minio_buckets || true  # Non-critical
    test_python_imports || exit_code=1

    log_info "========================================="
    if [ $exit_code -eq 0 ]; then
        log_info "✅ All smoke tests PASSED"
    else
        log_error "❌ Some smoke tests FAILED"
    fi
    log_info "========================================="

    exit $exit_code
}

# Run main function
main
