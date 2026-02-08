"""
Comprehensive tests for SecretsManager module.

Tests cover all credential retrieval methods to achieve 70%+ coverage.
"""

import os
from unittest.mock import MagicMock, Mock, patch

import pytest

from src.config.secrets_manager import (
    ApiCredentials,
    MinioCredentials,
    PostgresCredentials,
    SecretsManager,
    _get_airflow_connection,
    _is_airflow_context,
)


class TestHelperFunctions:
    """Test helper functions."""

    @patch("src.config.secrets_manager.conf")
    def test_is_airflow_context_true(self, mock_conf):
        """Test _is_airflow_context returns True when in Airflow."""
        mock_conf.get.return_value = "SequentialExecutor"
        assert _is_airflow_context() is True

    def test_is_airflow_context_false(self):
        """Test _is_airflow_context returns False when not in Airflow."""
        with patch("src.config.secrets_manager.conf", side_effect=ImportError):
            assert _is_airflow_context() is False

    @patch("src.config.secrets_manager.BaseHook")
    def test_get_airflow_connection_success(self, mock_basehook):
        """Test successful retrieval of Airflow connection."""
        mock_conn = Mock()
        mock_basehook.get_connection.return_value = mock_conn

        result = _get_airflow_connection("test_conn")

        assert result == mock_conn
        mock_basehook.get_connection.assert_called_once_with("test_conn")

    @patch("src.config.secrets_manager.BaseHook")
    def test_get_airflow_connection_failure(self, mock_basehook):
        """Test _get_airflow_connection returns None on error."""
        mock_basehook.get_connection.side_effect = Exception("Connection not found")

        result = _get_airflow_connection("test_conn")

        assert result is None


class TestDataclasses:
    """Test dataclass models."""

    def test_postgres_credentials_connection_string(self):
        """Test PostgresCredentials generates correct connection string."""
        creds = PostgresCredentials(
            host="localhost", port=5432, database="testdb", user="testuser", password="testpass"
        )

        conn_str = creds.get_connection_string()

        assert conn_str == "postgresql+psycopg2://testuser:testpass@localhost:5432/testdb"

    def test_postgres_credentials_custom_driver(self):
        """Test PostgresCredentials with custom driver."""
        creds = PostgresCredentials(
            host="localhost", port=5432, database="testdb", user="testuser", password="testpass"
        )

        conn_str = creds.get_connection_string(driver="asyncpg")

        assert conn_str == "postgresql+asyncpg://testuser:testpass@localhost:5432/testdb"

    def test_minio_credentials_creation(self):
        """Test MinioCredentials dataclass creation."""
        creds = MinioCredentials(
            endpoint="localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False
        )

        assert creds.endpoint == "localhost:9000"
        assert creds.access_key == "minioadmin"
        assert creds.secret_key == "minioadmin"
        assert creds.secure is False

    def test_api_credentials_creation(self):
        """Test ApiCredentials dataclass creation."""
        creds = ApiCredentials(api_key="test_key", base_url="https://api.example.com")

        assert creds.api_key == "test_key"
        assert creds.base_url == "https://api.example.com"


class TestSecretsManagerInit:
    """Test SecretsManager initialization."""

    def test_init_creates_instance(self):
        """Test that SecretsManager initializes correctly."""
        manager = SecretsManager()
        assert manager is not None


class TestGetPostgresCredentials:
    """Test get_postgres_credentials method."""

    @patch("src.config.secrets_manager._is_airflow_context")
    @patch("src.config.secrets_manager._get_airflow_connection")
    def test_get_postgres_from_airflow(self, mock_get_conn, mock_is_airflow):
        """Test retrieving Postgres credentials from Airflow connection."""
        mock_is_airflow.return_value = True

        mock_conn = Mock()
        mock_conn.host = "airflow-host"
        mock_conn.port = 5432
        mock_conn.schema = "airflow_db"
        mock_conn.login = "airflow_user"
        mock_conn.password = "airflow_pass"
        mock_get_conn.return_value = mock_conn

        manager = SecretsManager()
        creds = manager.get_postgres_credentials()

        assert creds.host == "airflow-host"
        assert creds.port == 5432
        assert creds.database == "airflow_db"
        assert creds.user == "airflow_user"
        assert creds.password == "airflow_pass"

    @patch("src.config.secrets_manager._is_airflow_context")
    def test_get_postgres_from_env(self, mock_is_airflow, monkeypatch):
        """Test retrieving Postgres credentials from environment variables."""
        mock_is_airflow.return_value = False

        monkeypatch.setenv("POSTGRES_HOST", "env-host")
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        monkeypatch.setenv("POSTGRES_DB", "env_db")
        monkeypatch.setenv("POSTGRES_USER", "env_user")
        monkeypatch.setenv("POSTGRES_PASSWORD", "env_pass")

        manager = SecretsManager()
        creds = manager.get_postgres_credentials()

        assert creds.host == "env-host"
        assert creds.port == 5433
        assert creds.database == "env_db"
        assert creds.user == "env_user"
        assert creds.password == "env_pass"

    @patch("src.config.secrets_manager._is_airflow_context")
    def test_get_postgres_defaults(self, mock_is_airflow, monkeypatch):
        """Test Postgres credentials with default values."""
        mock_is_airflow.return_value = False

        # Clear environment variables
        for var in ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]:
            monkeypatch.delenv(var, raising=False)

        manager = SecretsManager()
        creds = manager.get_postgres_credentials()

        assert creds.host == "localhost"
        assert creds.port == 5432
        assert creds.database == "weather_db"
        assert creds.user == "postgres"
        assert creds.password == "postgres"


class TestGetMinioCredentials:
    """Test get_minio_credentials method."""

    @patch("src.config.secrets_manager._is_airflow_context")
    @patch("src.config.secrets_manager._get_airflow_connection")
    def test_get_minio_from_airflow(self, mock_get_conn, mock_is_airflow):
        """Test retrieving MinIO credentials from Airflow connection."""
        mock_is_airflow.return_value = True

        mock_conn = Mock()
        mock_conn.host = "minio.example.com:9000"
        mock_conn.login = "airflow_access"
        mock_conn.password = "airflow_secret"
        mock_conn.extra_dejson = {"secure": True}
        mock_get_conn.return_value = mock_conn

        manager = SecretsManager()
        creds = manager.get_minio_credentials()

        assert creds.endpoint == "minio.example.com:9000"
        assert creds.access_key == "airflow_access"
        assert creds.secret_key == "airflow_secret"
        assert creds.secure is True

    @patch("src.config.secrets_manager._is_airflow_context")
    def test_get_minio_from_env(self, mock_is_airflow, monkeypatch):
        """Test retrieving MinIO credentials from environment variables."""
        mock_is_airflow.return_value = False

        monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
        monkeypatch.setenv("MINIO_ACCESS_KEY", "env_access")
        monkeypatch.setenv("MINIO_SECRET_KEY", "env_secret")
        monkeypatch.setenv("MINIO_SECURE", "false")

        manager = SecretsManager()
        creds = manager.get_minio_credentials()

        assert creds.endpoint == "localhost:9000"
        assert creds.access_key == "env_access"
        assert creds.secret_key == "env_secret"
        assert creds.secure is False


class TestGetApiCredentials:
    """Test API credential retrieval methods."""

    @patch("src.config.secrets_manager._is_airflow_context")
    @patch("src.config.secrets_manager._get_airflow_connection")
    def test_get_openweather_from_airflow(self, mock_get_conn, mock_is_airflow):
        """Test retrieving OpenWeather API key from Airflow."""
        mock_is_airflow.return_value = True

        mock_conn = Mock()
        mock_conn.password = "airflow_ow_key"
        mock_conn.host = "https://api.openweathermap.org"
        mock_get_conn.return_value = mock_conn

        manager = SecretsManager()
        api_key = manager.get_openweather_api_key()

        assert api_key == "airflow_ow_key"

    @patch("src.config.secrets_manager._is_airflow_context")
    def test_get_openweather_from_env(self, mock_is_airflow, monkeypatch):
        """Test retrieving OpenWeather API key from environment."""
        mock_is_airflow.return_value = False

        monkeypatch.setenv("OPENWEATHER_API_KEY", "env_ow_key")

        manager = SecretsManager()
        api_key = manager.get_openweather_api_key()

        assert api_key == "env_ow_key"

    @patch("src.config.secrets_manager._is_airflow_context")
    def test_get_aemet_from_env(self, mock_is_airflow, monkeypatch):
        """Test retrieving AEMET API key from environment."""
        mock_is_airflow.return_value = False

        monkeypatch.setenv("AEMET_API_KEY", "env_aemet_key")

        manager = SecretsManager()
        api_key = manager.get_aemet_api_key()

        assert api_key == "env_aemet_key"

    @patch("src.config.secrets_manager._is_airflow_context")
    def test_get_github_token_from_env(self, mock_is_airflow, monkeypatch):
        """Test retrieving GitHub token from environment."""
        mock_is_airflow.return_value = False

        monkeypatch.setenv("GITHUB_TOKEN", "env_github_token")

        manager = SecretsManager()
        token = manager.get_github_token()

        assert token == "env_github_token"

    @patch("src.config.secrets_manager._is_airflow_context")
    def test_get_github_token_none_when_missing(self, mock_is_airflow, monkeypatch):
        """Test GitHub token returns None when not set."""
        mock_is_airflow.return_value = False

        monkeypatch.delenv("GITHUB_TOKEN", raising=False)

        manager = SecretsManager()
        token = manager.get_github_token()

        assert token is None


class TestErrorHandling:
    """Test error handling in SecretsManager."""

    @patch("src.config.secrets_manager._is_airflow_context")
    @patch("src.config.secrets_manager._get_airflow_connection")
    def test_fallback_to_env_when_airflow_connection_missing(self, mock_get_conn, mock_is_airflow, monkeypatch):
        """Test fallback to environment when Airflow connection is missing."""
        mock_is_airflow.return_value = True
        mock_get_conn.return_value = None  # Connection not found

        monkeypatch.setenv("POSTGRES_HOST", "fallback-host")
        monkeypatch.setenv("POSTGRES_PORT", "5432")
        monkeypatch.setenv("POSTGRES_DB", "fallback_db")
        monkeypatch.setenv("POSTGRES_USER", "fallback_user")
        monkeypatch.setenv("POSTGRES_PASSWORD", "fallback_pass")

        manager = SecretsManager()
        creds = manager.get_postgres_credentials()

        assert creds.host == "fallback-host"
        assert creds.database == "fallback_db"
