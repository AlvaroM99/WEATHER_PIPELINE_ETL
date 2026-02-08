"""
Database Configuration
PostgreSQL connection settings for the data warehouse.

Credentials are managed through SecretsManager which prioritizes
Airflow Connections when available, with fallback to environment variables.
"""

from typing import Any, Dict

try:
    from src.config.secrets_manager import get_postgres_credentials
except ImportError:
    from config.secrets_manager import get_postgres_credentials  # type: ignore[no-redef]


def get_postgres_config() -> Dict[str, Any]:
    """
    Get PostgreSQL configuration dictionary.

    Returns dict with: host, port, database, user, password
    Uses SecretsManager for credential retrieval.
    """
    creds = get_postgres_credentials()
    return {
        "host": creds.host,
        "port": creds.port,
        "database": creds.database,
        "user": creds.user,
        "password": creds.password,
    }
