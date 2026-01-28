#!/bin/bash
set -e

echo "Installing Python dependencies..."
pip install --no-cache-dir requests psycopg2-binary

echo "Waiting for PostgreSQL to be ready..."
sleep 5

# Install additional Python packages
if [ -f /opt/airflow/requirements.txt ]; then
    echo "Installing additional Python packages..."
    pip install --no-cache-dir -r /opt/airflow/requirements.txt
fi

echo "Initializing Airflow database..."
airflow db init

echo "Creating Airflow admin user..."
airflow users create \
    --username ${AIRFLOW_USER} \
    --password ${AIRFLOW_PASSWORD} \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@example.com || echo "User already exists"

echo "Starting Airflow webserver and scheduler..."
airflow webserver & airflow scheduler
