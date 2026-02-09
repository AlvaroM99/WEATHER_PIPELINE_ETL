#!/bin/bash
# ============================================================================
# Deployment Script for Weather ETL Pipeline
# ============================================================================
# Usage:
#   ./scripts/deploy.sh <environment> <version>
#   ./scripts/deploy.sh development latest
#   ./scripts/deploy.sh production v1.2.3
# ============================================================================

set -e

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

log_step() {
    echo -e "\n${BLUE}===================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}===================================================${NC}\n"
}

# ============================================================================
# Configuration
# ============================================================================

ENVIRONMENT="${1:-development}"
VERSION="${2:-latest}"
PROJECT_NAME="weather-pipeline-etl"
DOCKER_REGISTRY="${DOCKER_REGISTRY:-ghcr.io}"
DOCKER_IMAGE="${DOCKER_REGISTRY}/${GITHUB_REPOSITORY:-alvarom99/weather-pipeline-etl}"

# Validate environment
if [[ "$ENVIRONMENT" != "development" && "$ENVIRONMENT" != "production" ]]; then
    log_error "Invalid environment: $ENVIRONMENT"
    log_error "Valid environments: development, production"
    exit 1
fi

log_step "Deployment Configuration"
log_info "Environment: $ENVIRONMENT"
log_info "Version: $VERSION"
log_info "Docker Image: $DOCKER_IMAGE:$VERSION"
log_info "Project: $PROJECT_NAME"

# ============================================================================
# Pre-deployment Checks
# ============================================================================

pre_deployment_checks() {
    log_step "Pre-deployment Checks"

    # Check if docker is installed
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi
    log_info "✅ Docker is installed"

    # Check if docker-compose is installed
    if ! command -v docker-compose &> /dev/null; then
        log_error "docker-compose is not installed"
        exit 1
    fi
    log_info "✅ docker-compose is installed"

    # Check if .env file exists for environment
    if [ ! -f ".env.${ENVIRONMENT}" ]; then
        log_warn ".env.${ENVIRONMENT} not found, using .env"
        if [ ! -f ".env" ]; then
            log_error "No .env file found"
            exit 1
        fi
    else
        log_info "✅ .env.${ENVIRONMENT} found"
    fi

    # Check disk space
    local available_space=$(df -h . | awk 'NR==2 {print $4}' | sed 's/G//')
    if (( $(echo "$available_space < 5" | bc -l) )); then
        log_error "Insufficient disk space: ${available_space}GB available"
        exit 1
    fi
    log_info "✅ Disk space: ${available_space}GB available"
}

# ============================================================================
# Backup Current State
# ============================================================================

backup_current_state() {
    log_step "Backing up Current State"

    local backup_dir="backups/$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$backup_dir"

    # Backup environment file
    if [ -f ".env.${ENVIRONMENT}" ]; then
        cp ".env.${ENVIRONMENT}" "$backup_dir/.env.${ENVIRONMENT}.backup"
        log_info "✅ Environment file backed up"
    fi

    # Export current database (if in production)
    if [ "$ENVIRONMENT" = "production" ]; then
        log_info "Exporting production database..."
        docker-compose -f docker-compose.prod.yml exec -T postgres \
            pg_dump -U "${POSTGRES_USER}" "${POSTGRES_DB}" | gzip > "$backup_dir/database_backup.sql.gz" || {
            log_warn "Database backup failed (non-critical)"
        }
        log_info "✅ Database backed up to $backup_dir/database_backup.sql.gz"
    fi

    # Save current docker-compose state
    docker-compose -f "docker-compose.${ENVIRONMENT}.yml" ps > "$backup_dir/services_state.txt" || true
    log_info "✅ Services state saved"

    echo "$backup_dir" > .last_backup_dir
    log_info "Backup completed: $backup_dir"
}

# ============================================================================
# Pull New Images
# ============================================================================

pull_images() {
    log_step "Pulling Docker Images"

    # Pull the new image
    log_info "Pulling image: $DOCKER_IMAGE:$VERSION"
    docker pull "$DOCKER_IMAGE:$VERSION" || {
        log_error "Failed to pull image: $DOCKER_IMAGE:$VERSION"
        exit 1
    }

    log_info "✅ Image pulled successfully"

    # Tag as environment-specific
    docker tag "$DOCKER_IMAGE:$VERSION" "$PROJECT_NAME:$ENVIRONMENT"
    log_info "✅ Image tagged as $PROJECT_NAME:$ENVIRONMENT"
}

# ============================================================================
# Deploy Services
# ============================================================================

deploy_services() {
    log_step "Deploying Services"

    local compose_file="docker-compose.${ENVIRONMENT}.yml"

    # Load environment variables
    if [ -f ".env.${ENVIRONMENT}" ]; then
        export $(cat ".env.${ENVIRONMENT}" | grep -v '^#' | xargs)
    fi

    # Pull all required images
    log_info "Pulling dependencies..."
    docker-compose -f "$compose_file" pull

    # Start services with zero-downtime deployment
    if [ "$ENVIRONMENT" = "production" ]; then
        log_info "Performing zero-downtime deployment..."

        # Start new containers without removing old ones
        docker-compose -f "$compose_file" up -d --no-deps --build airflow

        # Wait for health checks
        log_info "Waiting for health checks..."
        sleep 30

        # Run health checks
        if ! bash scripts/health_check.sh; then
            log_error "Health checks failed, rolling back..."
            rollback_deployment
            exit 1
        fi

        log_info "✅ Health checks passed"

        # Remove old containers
        docker-compose -f "$compose_file" up -d --remove-orphans
    else
        # Development: simple restart
        log_info "Restarting services..."
        docker-compose -f "$compose_file" up -d --force-recreate
    fi

    log_info "✅ Services deployed successfully"
}

# ============================================================================
# Post-deployment Tests
# ============================================================================

post_deployment_tests() {
    log_step "Post-deployment Tests"

    # Wait for services to stabilize
    log_info "Waiting for services to stabilize (30s)..."
    sleep 30

    # Run health checks
    log_info "Running health checks..."
    if bash scripts/health_check.sh; then
        log_info "✅ Health checks passed"
    else
        log_error "Health checks failed"
        return 1
    fi

    # Run smoke tests
    if [ -f "scripts/smoke_tests.sh" ]; then
        log_info "Running smoke tests..."
        if bash scripts/smoke_tests.sh; then
            log_info "✅ Smoke tests passed"
        else
            log_warn "Smoke tests failed (non-critical)"
        fi
    fi

    return 0
}

# ============================================================================
# Rollback Deployment
# ============================================================================

rollback_deployment() {
    log_step "Rolling Back Deployment"

    if [ ! -f ".last_backup_dir" ]; then
        log_error "No backup found for rollback"
        exit 1
    fi

    local backup_dir=$(cat .last_backup_dir)

    log_info "Rolling back to state from $backup_dir"

    # Restore environment file
    if [ -f "$backup_dir/.env.${ENVIRONMENT}.backup" ]; then
        cp "$backup_dir/.env.${ENVIRONMENT}.backup" ".env.${ENVIRONMENT}"
        log_info "✅ Environment file restored"
    fi

    # Restore database (production only)
    if [ "$ENVIRONMENT" = "production" ] && [ -f "$backup_dir/database_backup.sql.gz" ]; then
        log_warn "Database rollback requires manual intervention"
        log_warn "Backup location: $backup_dir/database_backup.sql.gz"
    fi

    # Restart with previous configuration
    docker-compose -f "docker-compose.${ENVIRONMENT}.yml" down
    docker-compose -f "docker-compose.${ENVIRONMENT}.yml" up -d

    log_info "✅ Rollback completed"
}

# ============================================================================
# Cleanup
# ============================================================================

cleanup() {
    log_step "Cleanup"

    # Remove dangling images
    log_info "Removing dangling images..."
    docker image prune -f || true

    # Remove old backups (keep last 5)
    log_info "Cleaning old backups..."
    ls -t backups/ | tail -n +6 | xargs -I {} rm -rf "backups/{}" || true

    log_info "✅ Cleanup completed"
}

# ============================================================================
# Main Deployment Flow
# ============================================================================

main() {
    log_step "Weather ETL Pipeline - Deployment"
    log_info "Starting deployment to $ENVIRONMENT"

    # Pre-deployment
    pre_deployment_checks
    backup_current_state

    # Deployment
    pull_images
    deploy_services

    # Post-deployment
    if post_deployment_tests; then
        log_info "✅ Deployment successful!"
        cleanup
    else
        log_error "Post-deployment tests failed"
        if [ "$ENVIRONMENT" = "production" ]; then
            log_warn "Initiating automatic rollback..."
            rollback_deployment
            exit 1
        else
            log_warn "Development environment, skipping rollback"
            exit 1
        fi
    fi

    log_step "Deployment Completed Successfully"
    log_info "Environment: $ENVIRONMENT"
    log_info "Version: $VERSION"
    log_info "Status: ✅ SUCCESS"
}

# Handle script arguments
case "${1:-}" in
    --rollback)
        rollback_deployment
        exit 0
        ;;
    --help|-h)
        echo "Usage: $0 <environment> <version>"
        echo "       $0 --rollback"
        echo ""
        echo "Environments: development, production"
        echo "Version: Docker image tag (e.g., latest, v1.2.3)"
        exit 0
        ;;
    *)
        main
        ;;
esac
