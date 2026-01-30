"""
Secrets Manager for Weather Pipeline
Secure credential management using Airflow Connections with environment variable fallback.

This module provides a unified interface for accessing secrets across the pipeline,
prioritizing Airflow Connections when running in an Airflow context and falling back
to environment variables for local development or non-Airflow execution.

Connection IDs:
    - postgres_weather: PostgreSQL database connection
    - minio_datalake: MinIO/S3 data lake connection
    - openweather_api: OpenWeatherMap API credentials
    - aemet_api: AEMET (Spanish Met Agency) API credentials
    - openmeteo_api: Open-Meteo API credentials (optional)
    - github_api: GitHub API token for fetching city data
"""

import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _is_airflow_context() -> bool:
    """Check if running within an Airflow context."""
    try:
        from airflow.configuration import conf

        conf.get("core", "executor")
        return True
    except Exception:
        return False


def _get_airflow_connection(conn_id: str) -> Optional[Any]:
    """
    Retrieve an Airflow Connection by ID.

    Args:
        conn_id: The connection identifier in Airflow

    Returns:
        Connection object or None if not found
    """
    try:
        from airflow.hooks.base import BaseHook

        return BaseHook.get_connection(conn_id)
    except Exception as e:
        logger.debug(f"Could not retrieve Airflow connection '{conn_id}': {e}")
        return None


@dataclass
class PostgresCredentials:
    """PostgreSQL connection credentials."""

    host: str
    port: int
    database: str
    user: str
    password: str

    def get_connection_string(self, driver: str = "psycopg2") -> str:
        """Generate SQLAlchemy connection string."""
        return (
            f"postgresql+{driver}://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass
class MinioCredentials:
    """MinIO/S3 connection credentials."""

    endpoint: str
    access_key: str
    secret_key: str
    secure: bool = False


@dataclass
class ApiCredentials:
    """API credentials for external services."""

    api_key: str
    base_url: Optional[str] = None


class SecretsManager:
    """
    Centralized secrets management for the Weather Pipeline.

    Prioritizes Airflow Connections when available, with automatic
    fallback to environment variables for development/testing.

    Usage:
        secrets = SecretsManager()

        # Get PostgreSQL credentials
        pg_creds = secrets.get_postgres_credentials()

        # Get API key
        api_key = secrets.get_openweather_api_key()
    """

    # Airflow Connection IDs
    CONN_POSTGRES = "postgres_weather"
    CONN_MINIO = "minio_datalake"
    CONN_OPENWEATHER = "openweather_api"
    CONN_AEMET = "aemet_api"
    CONN_OPENMETEO = "openmeteo_api"
    CONN_GITHUB = "github_api"

    def __init__(self, use_airflow: Optional[bool] = None):
        """
        Initialize the secrets manager.

        Args:
            use_airflow: Force Airflow mode (True), env mode (False),
                        or auto-detect (None, default)
        """
        if use_airflow is None:
            self._use_airflow = _is_airflow_context()
        else:
            self._use_airflow = use_airflow

        if self._use_airflow:
            logger.info("SecretsManager: Using Airflow Connections")
        else:
            logger.info("SecretsManager: Using environment variables")

    @property
    def is_airflow_mode(self) -> bool:
        """Check if using Airflow Connections."""
        return self._use_airflow

    def get_postgres_credentials(self) -> PostgresCredentials:
        """
        Get PostgreSQL connection credentials.

        Airflow Connection format:
            conn_id: postgres_weather
            conn_type: postgres
            host: postgres (or hostname)
            schema: weatherdb (database name)
            login: username
            password: password
            port: 5432
        """
        if self._use_airflow:
            conn = _get_airflow_connection(self.CONN_POSTGRES)
            if conn:
                return PostgresCredentials(
                    host=conn.host or "postgres",
                    port=conn.port or 5432,
                    database=conn.schema or "weatherdb",
                    user=conn.login or "",
                    password=conn.password or "",
                )
            logger.warning(
                f"Airflow connection '{self.CONN_POSTGRES}' not found, "
                "falling back to environment variables"
            )

        return PostgresCredentials(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "weatherdb"),
            user=os.getenv("POSTGRES_USER", ""),
            password=os.getenv("POSTGRES_PASSWORD", ""),
        )

    def get_minio_credentials(self) -> MinioCredentials:
        """
        Get MinIO/S3 connection credentials.

        Airflow Connection format:
            conn_id: minio_datalake
            conn_type: aws (or s3)
            host: minio:9000 (endpoint)
            login: access_key
            password: secret_key
            extra: {"secure": false}
        """
        if self._use_airflow:
            conn = _get_airflow_connection(self.CONN_MINIO)
            if conn:
                extra = conn.extra_dejson if conn.extra else {}
                endpoint = conn.host or "minio:9000"
                if conn.port:
                    endpoint = f"{conn.host}:{conn.port}"
                return MinioCredentials(
                    endpoint=endpoint,
                    access_key=conn.login or "",
                    secret_key=conn.password or "",
                    secure=extra.get("secure", False),
                )
            logger.warning(
                f"Airflow connection '{self.CONN_MINIO}' not found, "
                "falling back to environment variables"
            )

        return MinioCredentials(
            endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
            access_key=os.getenv("MINIO_ROOT_USER", ""),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", ""),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        )

    def get_openweather_api_key(self) -> Optional[str]:
        """
        Get OpenWeatherMap API key.

        Airflow Connection format:
            conn_id: openweather_api
            conn_type: http
            password: api_key (stored in password field)
            OR
            extra: {"api_key": "..."}
        """
        if self._use_airflow:
            conn = _get_airflow_connection(self.CONN_OPENWEATHER)
            if conn:
                if conn.password:
                    return conn.password
                extra = conn.extra_dejson if conn.extra else {}
                if extra.get("api_key"):
                    return extra["api_key"]
            logger.warning(
                f"Airflow connection '{self.CONN_OPENWEATHER}' not found, "
                "falling back to environment variables"
            )

        return os.getenv("OPENWEATHER_API_KEY")

    def get_aemet_api_key(self) -> Optional[str]:
        """
        Get AEMET (Spanish Meteorological Agency) API key.

        Airflow Connection format:
            conn_id: aemet_api
            conn_type: http
            host: opendata.aemet.es
            password: api_key (JWT token)
        """
        if self._use_airflow:
            conn = _get_airflow_connection(self.CONN_AEMET)
            if conn:
                if conn.password:
                    return conn.password
                extra = conn.extra_dejson if conn.extra else {}
                if extra.get("api_key"):
                    return extra["api_key"]
            logger.warning(
                f"Airflow connection '{self.CONN_AEMET}' not found, "
                "falling back to environment variables"
            )

        return os.getenv("AEMET_API_KEY")

    def get_openmeteo_api_key(self) -> Optional[str]:
        """
        Get Open-Meteo API key (optional for non-commercial use).

        Airflow Connection format:
            conn_id: openmeteo_api
            conn_type: http
            host: api.open-meteo.com
            password: api_key (optional)
        """
        if self._use_airflow:
            conn = _get_airflow_connection(self.CONN_OPENMETEO)
            if conn:
                if conn.password:
                    return conn.password
                extra = conn.extra_dejson if conn.extra else {}
                if extra.get("api_key"):
                    return extra["api_key"]

        return os.getenv("OPENMETEO_API_KEY")

    def get_github_token(self) -> Optional[str]:
        """
        Get GitHub API token for fetching city data.

        Airflow Connection format:
            conn_id: github_api
            conn_type: http
            host: api.github.com
            password: token
        """
        if self._use_airflow:
            conn = _get_airflow_connection(self.CONN_GITHUB)
            if conn:
                if conn.password:
                    return conn.password
                extra = conn.extra_dejson if conn.extra else {}
                if extra.get("token"):
                    return extra["token"]
            logger.warning(
                f"Airflow connection '{self.CONN_GITHUB}' not found, "
                "falling back to environment variables"
            )

        return os.getenv("GITHUB_TOKEN")

    def get_api_credentials(self, service: str) -> ApiCredentials:
        """
        Get API credentials for a specific service.

        Args:
            service: One of 'openweather', 'aemet', 'openmeteo', 'github'

        Returns:
            ApiCredentials with api_key and optional base_url
        """
        service_map = {
            "openweather": (
                self.get_openweather_api_key,
                "https://api.openweathermap.org/data/2.5",
            ),
            "aemet": (
                self.get_aemet_api_key,
                "https://opendata.aemet.es/opendata/api",
            ),
            "openmeteo": (
                self.get_openmeteo_api_key,
                "https://api.open-meteo.com/v1",
            ),
            "github": (
                self.get_github_token,
                "https://api.github.com",
            ),
        }

        if service not in service_map:
            raise ValueError(
                f"Unknown service: {service}. " f"Valid services: {list(service_map.keys())}"
            )

        getter, base_url = service_map[service]
        return ApiCredentials(api_key=getter() or "", base_url=base_url)


# Global instance for convenience
_secrets_manager: Optional[SecretsManager] = None


def get_secrets_manager(use_airflow: Optional[bool] = None) -> SecretsManager:
    """
    Get or create the global SecretsManager instance.

    Args:
        use_airflow: Force Airflow mode (True), env mode (False),
                    or auto-detect (None, default)

    Returns:
        SecretsManager instance
    """
    global _secrets_manager
    if _secrets_manager is None or use_airflow is not None:
        _secrets_manager = SecretsManager(use_airflow=use_airflow)
    return _secrets_manager


# Convenience functions for direct access
def get_postgres_credentials() -> PostgresCredentials:
    """Get PostgreSQL credentials using global secrets manager."""
    return get_secrets_manager().get_postgres_credentials()


def get_minio_credentials() -> MinioCredentials:
    """Get MinIO credentials using global secrets manager."""
    return get_secrets_manager().get_minio_credentials()


def get_openweather_api_key() -> Optional[str]:
    """Get OpenWeather API key using global secrets manager."""
    return get_secrets_manager().get_openweather_api_key()


def get_aemet_api_key() -> Optional[str]:
    """Get AEMET API key using global secrets manager."""
    return get_secrets_manager().get_aemet_api_key()


def get_openmeteo_api_key() -> Optional[str]:
    """Get Open-Meteo API key using global secrets manager."""
    return get_secrets_manager().get_openmeteo_api_key()


def get_github_token() -> Optional[str]:
    """Get GitHub token using global secrets manager."""
    return get_secrets_manager().get_github_token()
