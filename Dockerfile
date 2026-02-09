# ============================================================================
# Multi-stage Dockerfile for Weather ETL Pipeline
# ============================================================================
# Stage 1: Builder - Install dependencies
# Stage 2: Production - Minimal runtime image
# ============================================================================

# ============================================================================
# Stage 1: Builder
# ============================================================================
FROM apache/airflow:2.9.1-python3.11 AS builder

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements files
COPY --chown=airflow:root requirements/ /opt/airflow/requirements/

# Install Python dependencies
RUN pip install --user --no-cache-dir \
    -r /opt/airflow/requirements/base.txt

# ============================================================================
# Stage 2: Production Runtime
# ============================================================================
FROM apache/airflow:2.9.1-python3.11

USER root

# Install runtime system dependencies (PostgreSQL client only)
RUN apt-get update && apt-get install -y \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy installed packages from builder
COPY --from=builder --chown=airflow:root /home/airflow/.local /home/airflow/.local

# Set Python path
ENV PATH=/home/airflow/.local/bin:$PATH
ENV PYTHONPATH=/opt/airflow

# Copy application code
COPY --chown=airflow:root src/ /opt/airflow/src/
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root pyproject.toml /opt/airflow/

# Copy deployment scripts
COPY --chown=airflow:root scripts/health_check.sh /opt/airflow/scripts/
COPY --chown=airflow:root scripts/entrypoint.sh /opt/airflow/scripts/

# Make scripts executable
USER root
RUN chmod +x /opt/airflow/scripts/*.sh
USER airflow

# Set working directory
WORKDIR /opt/airflow

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD /opt/airflow/scripts/health_check.sh

# Labels for metadata
LABEL maintainer="alvaro@example.com"
LABEL project="weather-pipeline-etl"
LABEL version="1.0.0"
LABEL description="Weather ETL Pipeline with Airflow"

# Default command (can be overridden in docker-compose)
CMD ["/opt/airflow/scripts/entrypoint.sh"]
