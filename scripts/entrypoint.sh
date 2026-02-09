#!/bin/bash
# ============================================================================
# Entrypoint Script for Weather ETL Pipeline Container
# ============================================================================
# Handles:
# 1. Airflow database initialization
# 2. Admin user creation
# 3. Service startup (webserver or scheduler)
# 4. Graceful shutdown
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

# ============================================================================
# Configuration
# ============================================================================

AIRFLOW_ROLE="${AIRFLOW_ROLE:-webserver}"  # webserver, scheduler, worker
ENVIRONMENT="${ENVIRONMENT:-development}"

log_info "========================================="
log_info "Weather ETL Pipeline - Starting"
log_info "Role: $AIRFLOW_ROLE"
log_info "Environment: $ENVIRONMENT"
log_info "========================================="

# ============================================================================
# Wait for Dependencies
# ============================================================================

wait_for_postgres() {
    log_info "Waiting for PostgreSQL (DWH)..."
    local retries=30
    local count=0

    while [ $count -lt $retries ]; do
        if pg_isready -h "${POSTGRES_HOST:-postgres}" -p "${POSTGRES_PORT:-5432}" -U "${POSTGRES_USER:-weather_user}" > /dev/null 2>&1; then
            log_info "PostgreSQL is ready!"
            return 0
        fi
        count=$((count + 1))
        log_warn "Waiting for PostgreSQL... ($count/$retries)"
        sleep 2
    done

    log_error "PostgreSQL did not become ready in time"
    return 1
}

wait_for_postgres_airflow() {
    log_info "Waiting for PostgreSQL (Airflow metadata)..."
    local postgres_airflow_host=$(echo $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN | sed 's/.*@\([^/]*\).*/\1/' | cut -d: -f1)
    local retries=30
    local count=0

    while [ $count -lt $retries ]; do
        if pg_isready -h "${postgres_airflow_host:-postgres-airflow}" -p 5432 > /dev/null 2>&1; then
            log_info "PostgreSQL (Airflow) is ready!"
            return 0
        fi
        count=$((count + 1))
        log_warn "Waiting for PostgreSQL (Airflow)... ($count/$retries)"
        sleep 2
    done

    log_error "PostgreSQL (Airflow) did not become ready in time"
    return 1
}

# ============================================================================
# Airflow Initialization
# ============================================================================

initialize_airflow() {
    log_info "Initializing Airflow database..."

    # Initialize database (idempotent)
    airflow db init || airflow db migrate

    log_info "Airflow database initialized"
}

create_admin_user() {
    log_info "Creating Airflow admin user..."

    # Check if user already exists
    if airflow users list | grep -q "${AIRFLOW_USER:-admin}"; then
        log_info "Admin user already exists, skipping creation"
        return 0
    fi

    # Create admin user
    airflow users create \
        --username "${AIRFLOW_USER:-admin}" \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email "${AIRFLOW_EMAIL:-admin@example.com}" \
        --password "${AIRFLOW_PASSWORD:-admin}" || {
        log_warn "Admin user creation failed (may already exist)"
    }

    log_info "Admin user created successfully"
}

# ============================================================================
# Service Startup
# ============================================================================

start_webserver() {
    log_info "Starting Airflow webserver..."
    exec airflow webserver
}

start_scheduler() {
    log_info "Starting Airflow scheduler..."
    exec airflow scheduler
}

start_worker() {
    log_info "Starting Airflow worker..."
    exec airflow celery worker
}

# ============================================================================
# Graceful Shutdown Handler
# ============================================================================

shutdown_handler() {
    log_warn "Received shutdown signal, gracefully stopping..."
    # Kill all child processes
    pkill -P $$
    exit 0
}

trap shutdown_handler SIGTERM SIGINT

# ============================================================================
# Main Entrypoint
# ============================================================================

main() {
    # Wait for dependencies
    wait_for_postgres_airflow || exit 1
    wait_for_postgres || exit 1

    # Initialize Airflow (only for webserver/scheduler)
    if [ "$AIRFLOW_ROLE" = "webserver" ] || [ "$AIRFLOW_ROLE" = "scheduler" ]; then
        initialize_airflow
        create_admin_user
    fi

    # Start appropriate service
    case "$AIRFLOW_ROLE" in
        webserver)
            start_webserver
            ;;
        scheduler)
            start_scheduler
            ;;
        worker)
            start_worker
            ;;
        *)
            log_error "Unknown AIRFLOW_ROLE: $AIRFLOW_ROLE"
            log_error "Valid roles: webserver, scheduler, worker"
            exit 1
            ;;
    esac
}

# Run main function
main
