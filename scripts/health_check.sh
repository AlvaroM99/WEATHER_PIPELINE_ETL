#!/bin/bash
# ============================================================================
# Health Check Script for Weather ETL Pipeline
# ============================================================================
# Checks:
# 1. Airflow webserver is responding
# 2. Airflow scheduler is running
# 3. PostgreSQL (DWH) is accessible
# 4. MinIO is accessible
# 5. Python modules can be imported
# ============================================================================

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
AIRFLOW_WEBSERVER_URL="${AIRFLOW_WEBSERVER_URL:-http://localhost:8080}"
MAX_RETRIES=3
RETRY_DELAY=5

# ============================================================================
# Helper Functions
# ============================================================================

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

retry_command() {
    local command="$1"
    local description="$2"
    local retries=0

    while [ $retries -lt $MAX_RETRIES ]; do
        if eval "$command" > /dev/null 2>&1; then
            log_info "$description: OK"
            return 0
        fi
        retries=$((retries + 1))
        if [ $retries -lt $MAX_RETRIES ]; then
            log_warn "$description: Failed (attempt $retries/$MAX_RETRIES). Retrying in ${RETRY_DELAY}s..."
            sleep $RETRY_DELAY
        fi
    done

    log_error "$description: FAILED after $MAX_RETRIES attempts"
    return 1
}

# ============================================================================
# Health Checks
# ============================================================================

check_airflow_webserver() {
    log_info "Checking Airflow webserver..."
    retry_command \
        "curl -f -s ${AIRFLOW_WEBSERVER_URL}/health" \
        "Airflow webserver"
}

check_airflow_scheduler() {
    log_info "Checking Airflow scheduler..."
    # Check if scheduler process is running
    if pgrep -f "airflow scheduler" > /dev/null; then
        log_info "Airflow scheduler: OK"
        return 0
    else
        log_error "Airflow scheduler: NOT RUNNING"
        return 1
    fi
}

check_postgresql() {
    log_info "Checking PostgreSQL connection..."
    retry_command \
        "python3 -c '
import psycopg2
import os
conn = psycopg2.connect(
    host=os.getenv(\"POSTGRES_HOST\", \"postgres\"),
    port=os.getenv(\"POSTGRES_PORT\", 5432),
    user=os.getenv(\"POSTGRES_USER\", \"weather_user\"),
    password=os.getenv(\"POSTGRES_PASSWORD\", \"weather_pass\"),
    database=os.getenv(\"POSTGRES_DB\", \"weather_db\")
)
conn.close()
'" \
        "PostgreSQL connection"
}

check_minio() {
    log_info "Checking MinIO connection..."
    local minio_endpoint="${MINIO_ENDPOINT:-minio:9000}"

    retry_command \
        "curl -f -s http://${minio_endpoint}/minio/health/live" \
        "MinIO health"
}

check_python_imports() {
    log_info "Checking Python module imports..."

    python3 -c "
import sys
sys.path.insert(0, '/opt/airflow')

try:
    from src.extractor import Extractor
    from src.transformer import Transformer
    from src.loader import Loader
    from src.dimensional_loader import DimensionalLoader
    print('All modules imported successfully')
except ImportError as e:
    print(f'Import error: {e}')
    sys.exit(1)
" || {
        log_error "Python imports: FAILED"
        return 1
    }

    log_info "Python imports: OK"
    return 0
}

check_disk_space() {
    log_info "Checking disk space..."
    local usage=$(df -h / | awk 'NR==2 {print $5}' | sed 's/%//')

    if [ "$usage" -gt 90 ]; then
        log_error "Disk space: CRITICAL ($usage% used)"
        return 1
    elif [ "$usage" -gt 80 ]; then
        log_warn "Disk space: WARNING ($usage% used)"
    else
        log_info "Disk space: OK ($usage% used)"
    fi
    return 0
}

check_memory() {
    log_info "Checking memory usage..."
    local usage=$(free | awk 'NR==2 {printf "%.0f", $3/$2 * 100}')

    if [ "$usage" -gt 90 ]; then
        log_warn "Memory usage: HIGH ($usage%)"
    else
        log_info "Memory usage: OK ($usage%)"
    fi
    return 0
}

# ============================================================================
# Main Health Check
# ============================================================================

main() {
    log_info "========================================"
    log_info "Weather ETL Pipeline - Health Check"
    log_info "Environment: ${ENVIRONMENT:-development}"
    log_info "========================================"

    local exit_code=0

    # Core services (critical)
    check_airflow_webserver || exit_code=1
    check_airflow_scheduler || exit_code=1
    check_postgresql || exit_code=1

    # Optional services (warnings only)
    check_minio || log_warn "MinIO check failed (non-critical)"
    check_python_imports || exit_code=1

    # System resources
    check_disk_space || exit_code=1
    check_memory

    log_info "========================================"
    if [ $exit_code -eq 0 ]; then
        log_info "✅ All health checks PASSED"
    else
        log_error "❌ Some health checks FAILED"
    fi
    log_info "========================================"

    exit $exit_code
}

# Run main function
main
