"""
Unit tests for Debug Utilities module
Tests diagnostic and debugging tools
"""

import logging
import time
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from src.utils.debug_utils import (
    check_minio_connectivity,
    check_postgres_connectivity,
    compare_dataframes,
    generate_diagnostic_report,
    inspect_json_structure,
    setup_debug_logging,
    timing_decorator,
    trace_exception,
    validate_dataframe,
)

# ===== Logging Tests =====


@pytest.mark.unit
def test_setup_debug_logging_default():
    """Test debug logging setup with default level"""
    logger = setup_debug_logging()

    assert logger is not None
    assert logger.level == logging.DEBUG
    assert len(logger.handlers) > 0


@pytest.mark.unit
def test_setup_debug_logging_info_level():
    """Test debug logging setup with INFO level"""
    logger = setup_debug_logging(level="INFO")

    assert logger is not None
    assert logger.level == logging.INFO


@pytest.mark.unit
def test_setup_debug_logging_warning_level():
    """Test debug logging setup with WARNING level"""
    logger = setup_debug_logging(level="WARNING")

    assert logger.level == logging.WARNING


# ===== Timing Decorator Tests =====


@pytest.mark.unit
def test_timing_decorator_success():
    """Test timing decorator logs execution time"""

    @timing_decorator
    def test_func():
        return "success"

    result = test_func()

    assert result == "success"


@pytest.mark.unit
def test_timing_decorator_with_exception():
    """Test timing decorator handles exceptions"""

    @timing_decorator
    def failing_func():
        raise ValueError("Test error")

    with pytest.raises(ValueError, match="Test error"):
        failing_func()


@pytest.mark.unit
def test_timing_decorator_preserves_function_name():
    """Test timing decorator preserves function metadata"""

    @timing_decorator
    def my_named_function():
        pass

    assert my_named_function.__name__ == "my_named_function"


# ===== DataFrame Validation Tests =====


@pytest.mark.unit
def test_validate_dataframe_valid():
    """Test validation of valid DataFrame"""
    df = pd.DataFrame({"city": ["Madrid", "Barcelona"], "temperature": [20.5, 18.3]})

    result = validate_dataframe(df, ["city", "temperature"], "test_df")

    assert result["valid"] is True
    assert result["name"] == "test_df"
    assert result["stats"]["row_count"] == 2
    assert result["stats"]["column_count"] == 2


@pytest.mark.unit
def test_validate_dataframe_missing_columns():
    """Test validation catches missing columns"""
    df = pd.DataFrame({"city": ["Madrid", "Barcelona"]})

    result = validate_dataframe(df, ["city", "temperature"], "test_df")

    assert result["valid"] is False
    assert any("Missing columns" in issue for issue in result["issues"])


@pytest.mark.unit
def test_validate_dataframe_empty():
    """Test validation catches empty DataFrame"""
    df = pd.DataFrame()

    result = validate_dataframe(df, ["city"], "empty_df")

    assert result["valid"] is False
    assert any("empty" in issue.lower() for issue in result["issues"])


@pytest.mark.unit
def test_validate_dataframe_none():
    """Test validation handles None DataFrame"""
    result = validate_dataframe(None, ["city"], "null_df")

    assert result["valid"] is False
    assert any("None" in issue for issue in result["issues"])


@pytest.mark.unit
def test_validate_dataframe_high_null_ratio():
    """Test validation warns about high null ratios"""
    df = pd.DataFrame({"city": ["Madrid", None, None, None], "temp": [20.0, 18.0, 22.0, 15.0]})

    result = validate_dataframe(df, ["city", "temp"], "null_df")

    assert any("null values" in issue for issue in result["issues"])


# ===== JSON Structure Inspection Tests =====


@pytest.mark.unit
def test_inspect_json_structure_simple():
    """Test inspection of simple JSON structure"""
    data = {"name": "Madrid", "temp": 20.5}

    result = inspect_json_structure(data)

    assert result["_type"] == "dict"
    assert "name" in result["_keys"]
    assert "temp" in result["_keys"]


@pytest.mark.unit
def test_inspect_json_structure_nested():
    """Test inspection of nested JSON structure"""
    data = {
        "city": "Madrid",
        "weather": {"main": "Clear", "description": "clear sky"},
        "coord": {"lat": 40.4, "lon": -3.7},
    }

    result = inspect_json_structure(data, max_depth=2)

    assert result["_type"] == "dict"
    assert "weather" in result["_keys"]


@pytest.mark.unit
def test_inspect_json_structure_with_list():
    """Test inspection of JSON with lists"""
    data = {"cities": [{"name": "Madrid"}, {"name": "Barcelona"}]}

    result = inspect_json_structure(data)

    assert result["_type"] == "dict"
    assert "cities" in result["_keys"]


@pytest.mark.unit
def test_inspect_json_structure_max_depth():
    """Test that max depth is respected"""
    data = {"level1": {"level2": {"level3": {"level4": "value"}}}}

    result = inspect_json_structure(data, max_depth=2)

    # Should stop at max depth
    assert "_type" in result


# ===== DataFrame Comparison Tests =====


@pytest.mark.unit
def test_compare_dataframes_identical():
    """Test comparison of identical DataFrames"""
    df1 = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    df2 = pd.DataFrame({"a": [5, 6], "b": [7, 8]})

    result = compare_dataframes(df1, df2, "first", "second")

    assert result["shapes"]["first"] == (2, 2)
    assert result["shapes"]["second"] == (2, 2)
    assert len(result["columns"]["common"]) == 2


@pytest.mark.unit
def test_compare_dataframes_different_columns():
    """Test comparison with different columns"""
    df1 = pd.DataFrame({"a": [1], "b": [2]})
    df2 = pd.DataFrame({"b": [3], "c": [4]})

    result = compare_dataframes(df1, df2)

    assert "a" in result["columns"]["only_in_df1"]
    assert "c" in result["columns"]["only_in_df2"]
    assert "b" in result["columns"]["common"]


@pytest.mark.unit
def test_compare_dataframes_with_none():
    """Test comparison handles None DataFrame"""
    df1 = pd.DataFrame({"a": [1]})

    result = compare_dataframes(df1, None)

    assert result["shapes"]["df2"] is None


@pytest.mark.unit
def test_compare_dataframes_dtype_differences():
    """Test comparison detects dtype differences"""
    df1 = pd.DataFrame({"a": [1, 2, 3]})  # int
    df2 = pd.DataFrame({"a": [1.0, 2.0, 3.0]})  # float

    result = compare_dataframes(df1, df2)

    assert "a" in result["dtype_differences"]


# ===== Exception Tracing Tests =====


@pytest.mark.unit
def test_trace_exception_basic():
    """Test exception tracing captures details"""
    try:
        raise ValueError("Test error message")
    except Exception as e:
        result = trace_exception(e)

    assert result["type"] == "ValueError"
    assert result["message"] == "Test error message"
    assert len(result["traceback"]) > 0


@pytest.mark.unit
def test_trace_exception_traceback_content():
    """Test exception traceback has expected fields"""
    try:
        raise RuntimeError("Runtime error")
    except Exception as e:
        result = trace_exception(e)

    tb_frame = result["traceback"][0]
    assert "file" in tb_frame
    assert "line" in tb_frame
    assert "function" in tb_frame


# ===== MinIO Connectivity Tests =====


@pytest.mark.unit
def test_check_minio_connectivity_success():
    """Test successful MinIO connectivity check"""
    import src.utils.debug_utils as debug_module

    # Save original Minio
    original_minio = debug_module.Minio

    # Create mock
    mock_minio_class = Mock()
    mock_client = Mock()
    mock_bucket = Mock()
    mock_bucket.name = "test-bucket"
    mock_client.list_buckets.return_value = [mock_bucket]
    mock_minio_class.return_value = mock_client

    # Patch
    debug_module.Minio = mock_minio_class

    try:
        result = check_minio_connectivity(
            endpoint="localhost:9000", access_key="test", secret_key="test"
        )

        assert result["connected"] is True
        assert result["buckets_accessible"] is True
        assert "test-bucket" in result["buckets"]
    finally:
        # Restore
        debug_module.Minio = original_minio


@pytest.mark.unit
def test_check_minio_connectivity_failure():
    """Test MinIO connectivity failure handling"""
    import src.utils.debug_utils as debug_module

    original_minio = debug_module.Minio

    mock_minio_class = Mock()
    mock_minio_class.side_effect = Exception("Connection refused")
    debug_module.Minio = mock_minio_class

    try:
        result = check_minio_connectivity(
            endpoint="invalid:9000", access_key="test", secret_key="test"
        )

        assert result["connected"] is False
        assert result["error"] is not None
    finally:
        debug_module.Minio = original_minio


# ===== PostgreSQL Connectivity Tests =====


@pytest.mark.unit
def test_check_postgres_connectivity_success():
    """Test successful PostgreSQL connectivity check"""
    import src.utils.debug_utils as debug_module

    original_psycopg2 = debug_module.psycopg2

    # Create mock psycopg2 module
    mock_psycopg2 = Mock()
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("PostgreSQL 14.0",)
    mock_cursor.fetchall.return_value = [("dim_city",), ("fact_weather",)]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_psycopg2.connect.return_value = mock_conn

    debug_module.psycopg2 = mock_psycopg2

    try:
        result = check_postgres_connectivity(
            host="localhost", port=5432, database="weather", user="test", password="test"
        )

        assert result["connected"] is True
        assert "PostgreSQL" in result["version"]
        assert len(result["tables"]) == 2
    finally:
        debug_module.psycopg2 = original_psycopg2


@pytest.mark.unit
def test_check_postgres_connectivity_failure():
    """Test PostgreSQL connectivity failure handling"""
    import src.utils.debug_utils as debug_module

    original_psycopg2 = debug_module.psycopg2

    # Create mock that raises error
    mock_psycopg2 = Mock()
    mock_psycopg2.connect.side_effect = Exception("Connection refused")
    mock_psycopg2.Error = Exception

    debug_module.psycopg2 = mock_psycopg2

    try:
        result = check_postgres_connectivity(
            host="invalid", port=5432, database="weather", user="test", password="test"
        )

        assert result["connected"] is False
        assert result["error"] is not None
    finally:
        debug_module.psycopg2 = original_psycopg2


# ===== Diagnostic Report Tests =====


@pytest.mark.unit
@patch("src.utils.debug_utils.check_minio_connectivity")
@patch("src.utils.debug_utils.check_postgres_connectivity")
def test_generate_diagnostic_report(mock_postgres, mock_minio):
    """Test diagnostic report generation"""
    mock_minio.return_value = {"connected": True}
    mock_postgres.return_value = {"connected": True}

    result = generate_diagnostic_report()

    assert "timestamp" in result
    assert "environment" in result
    assert "connectivity" in result
    assert result["environment"]["python_version"] is not None


@pytest.mark.unit
@patch("src.utils.debug_utils.check_minio_connectivity")
@patch("src.utils.debug_utils.check_postgres_connectivity")
def test_generate_diagnostic_report_skip_checks(mock_postgres, mock_minio):
    """Test diagnostic report without connectivity checks"""
    result = generate_diagnostic_report(include_minio=False, include_postgres=False)

    assert "minio" not in result["connectivity"]
    assert "postgres" not in result["connectivity"]
    mock_minio.assert_not_called()
    mock_postgres.assert_not_called()


@pytest.mark.unit
def test_generate_diagnostic_report_env_vars(monkeypatch):
    """Test diagnostic report captures environment variables"""
    monkeypatch.setenv("OPENWEATHER_API_KEY", "test_key")

    with (
        patch("src.utils.debug_utils.check_minio_connectivity"),
        patch("src.utils.debug_utils.check_postgres_connectivity"),
    ):
        result = generate_diagnostic_report()

    env_vars = result["environment"]["required_env_vars"]
    assert "OPENWEATHER_API_KEY" in env_vars
    assert env_vars["OPENWEATHER_API_KEY"]["set"] is True
