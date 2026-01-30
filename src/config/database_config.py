"""
Database Configuration
PostgreSQL connection settings for the data warehouse.

Credentials are managed through SecretsManager which prioritizes
Airflow Connections when available, with fallback to environment variables.
"""

import os

try:
    from src.config.secrets_manager import get_postgres_credentials
except ImportError:
    from config.secrets_manager import get_postgres_credentials  # type: ignore[no-redef]

# PostgreSQL Configuration (via SecretsManager)
_pg_creds = None


def _get_pg_creds():
    """Get PostgreSQL credentials with lazy loading."""
    global _pg_creds
    if _pg_creds is None:
        _pg_creds = get_postgres_credentials()
    return _pg_creds


# Legacy: Direct access for backward compatibility
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))


def get_postgres_config() -> dict:
    """
    Get PostgreSQL configuration dictionary.

    Returns dict with: host, port, database, user, password
    Uses SecretsManager for credential retrieval.
    """
    creds = _get_pg_creds()
    return {
        "host": creds.host,
        "port": creds.port,
        "database": creds.database,
        "user": creds.user,
        "password": creds.password,
    }
