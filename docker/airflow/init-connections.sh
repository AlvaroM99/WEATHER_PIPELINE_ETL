#!/bin/bash
# Initialize Airflow Connections for Weather Pipeline
# This script creates all necessary connections for secure credential management.
#
# Connections are created from environment variables passed to the container.
# If a connection already exists, it will be updated.

set -e

echo "============================================"
echo "Initializing Airflow Connections..."
echo "============================================"

# Function to add or update a connection
add_or_update_connection() {
    local conn_id=$1
    shift

    # Check if connection exists
    if airflow connections get "$conn_id" > /dev/null 2>&1; then
        echo "Updating existing connection: $conn_id"
        airflow connections delete "$conn_id" > /dev/null 2>&1
    else
        echo "Creating new connection: $conn_id"
    fi

    airflow connections add "$conn_id" "$@"
}

# PostgreSQL Connection
# Used for: Weather data storage, dimensional tables, fact tables
if [ -n "$POSTGRES_USER" ] && [ -n "$POSTGRES_PASSWORD" ]; then
    echo ""
    echo ">> Configuring PostgreSQL connection..."
    add_or_update_connection "postgres_weather" \
        --conn-type postgres \
        --conn-host "${POSTGRES_HOST:-postgres}" \
        --conn-port "${POSTGRES_PORT:-5432}" \
        --conn-schema "${POSTGRES_DB:-weatherdb}" \
        --conn-login "$POSTGRES_USER" \
        --conn-password "$POSTGRES_PASSWORD"
    echo "   [OK] postgres_weather configured"
else
    echo "   [SKIP] PostgreSQL credentials not provided"
fi

# MinIO Connection (S3-compatible)
# Used for: Data lake storage (Bronze/Silver layers)
if [ -n "$MINIO_ROOT_USER" ] && [ -n "$MINIO_ROOT_PASSWORD" ]; then
    echo ""
    echo ">> Configuring MinIO connection..."
    add_or_update_connection "minio_datalake" \
        --conn-type aws \
        --conn-host "${MINIO_ENDPOINT:-minio:9000}" \
        --conn-login "$MINIO_ROOT_USER" \
        --conn-password "$MINIO_ROOT_PASSWORD" \
        --conn-extra "{\"secure\": false, \"endpoint_url\": \"http://${MINIO_ENDPOINT:-minio:9000}\"}"
    echo "   [OK] minio_datalake configured"
else
    echo "   [SKIP] MinIO credentials not provided"
fi

# OpenWeatherMap API Connection
# Used for: Current weather data extraction
if [ -n "$OPENWEATHER_API_KEY" ]; then
    echo ""
    echo ">> Configuring OpenWeatherMap API connection..."
    add_or_update_connection "openweather_api" \
        --conn-type http \
        --conn-host "api.openweathermap.org" \
        --conn-password "$OPENWEATHER_API_KEY" \
        --conn-extra "{\"api_key\": \"$OPENWEATHER_API_KEY\"}"
    echo "   [OK] openweather_api configured"
else
    echo "   [SKIP] OpenWeather API key not provided"
fi

# AEMET API Connection
# Used for: Spanish meteorological data
if [ -n "$AEMET_API_KEY" ]; then
    echo ""
    echo ">> Configuring AEMET API connection..."
    add_or_update_connection "aemet_api" \
        --conn-type http \
        --conn-host "opendata.aemet.es" \
        --conn-password "$AEMET_API_KEY" \
        --conn-extra "{\"api_key\": \"$AEMET_API_KEY\"}"
    echo "   [OK] aemet_api configured"
else
    echo "   [SKIP] AEMET API key not provided"
fi

# Open-Meteo API Connection (optional key)
# Used for: Forecast, air quality, pollen, marine data
echo ""
echo ">> Configuring Open-Meteo API connection..."
if [ -n "$OPENMETEO_API_KEY" ]; then
    add_or_update_connection "openmeteo_api" \
        --conn-type http \
        --conn-host "api.open-meteo.com" \
        --conn-password "$OPENMETEO_API_KEY" \
        --conn-extra "{\"api_key\": \"$OPENMETEO_API_KEY\"}"
    echo "   [OK] openmeteo_api configured (with API key)"
else
    add_or_update_connection "openmeteo_api" \
        --conn-type http \
        --conn-host "api.open-meteo.com" \
        --conn-extra "{}"
    echo "   [OK] openmeteo_api configured (no API key - free tier)"
fi

# GitHub API Connection
# Used for: Fetching city data from GitHub
if [ -n "$GITHUB_TOKEN" ]; then
    echo ""
    echo ">> Configuring GitHub API connection..."
    add_or_update_connection "github_api" \
        --conn-type http \
        --conn-host "api.github.com" \
        --conn-password "$GITHUB_TOKEN" \
        --conn-extra "{\"token\": \"$GITHUB_TOKEN\"}"
    echo "   [OK] github_api configured"
else
    echo "   [SKIP] GitHub token not provided"
fi

echo ""
echo "============================================"
echo "Connection initialization complete!"
echo "============================================"
echo ""
echo "Configured connections:"
airflow connections list 2>/dev/null | grep -E "postgres_weather|minio_datalake|openweather_api|aemet_api|openmeteo_api|github_api" || echo "  (use 'airflow connections list' to view)"
echo ""
