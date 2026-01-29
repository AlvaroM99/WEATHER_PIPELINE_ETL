"""
Storage Configuration
MinIO connection settings

Credentials are managed through SecretsManager which prioritizes
Airflow Connections when available, with fallback to environment variables.
"""

import os

try:
    from src.weather_config.secrets_manager import get_minio_credentials
except ImportError:
    from weather_config.secrets_manager import get_minio_credentials

# MinIO credentials via SecretsManager
_minio_creds = None


def _get_minio_creds():
    """Get MinIO credentials with lazy loading."""
    global _minio_creds
    if _minio_creds is None:
        _minio_creds = get_minio_credentials()
    return _minio_creds


def get_minio_config() -> dict:
    """
    Get MinIO configuration dictionary.

    Returns dict with: endpoint, access_key, secret_key, secure
    Uses SecretsManager for credential retrieval.
    """
    creds = _get_minio_creds()
    return {
        "endpoint": creds.endpoint,
        "access_key": creds.access_key,
        "secret_key": creds.secret_key,
        "secure": creds.secure,
    }


# Legacy: Direct access for backward compatibility
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
