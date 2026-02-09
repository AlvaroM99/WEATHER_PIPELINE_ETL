"""
Integration tests for the ETL Pipeline
Tests end-to-end data flow from extraction to loading
"""

import logging
import threading
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import pandas as pd
import pytest
import responses

from src.config.db_pool import reset_pool
from src.utils.minio_client import reset_minio_client


@pytest.fixture(autouse=True)
def reset_singletons():
    """Reset singletons before and after each test."""
    reset_pool()
    reset_minio_client()
    yield
    reset_pool()
    reset_minio_client()


@pytest.fixture
def mock_extractor_metadata():
    """Mock Extractor.log_to_lake_metadata (defined in BaseLoader, not BaseETLLogger)."""
    from src.extractor import Extractor

    with patch.object(Extractor, "log_to_lake_metadata", create=True):
        yield


@pytest.fixture
def mock_extractor_db_access():
    """Mock Extractor's DB pool access and lake metadata logging."""
    from src.extractor import Extractor

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_conn.cursor.return_value = mock_cursor

    with patch("src.config.db_pool.get_db_connection", return_value=mock_conn), \
         patch("src.config.db_pool.return_db_connection"), \
         patch.object(Extractor, "log_to_lake_metadata", create=True):
        yield mock_conn


# ===== ETL Pipeline Flow Tests =====


@pytest.mark.integration
@responses.activate
@patch("src.extractor.get_minio_client")
@patch("src.extractor.get_cities")
@patch("src.transformer.get_minio_client")
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
@patch("src.loader.execute_values")
def test_openweather_etl_pipeline(
    mock_execute_values,
    mock_connect,
    mock_loader_get_minio_client,
    mock_transformer_get_minio_client,
    mock_get_cities,
    mock_extractor_get_minio_client,
    sample_openweather_response,
    sample_cities,
    mock_db_connection,
    mock_extractor_metadata,
):
    """Test complete OpenWeather ETL pipeline: Extract -> Transform -> Load"""
    # Setup Extractor
    mock_extractor_minio = Mock()
    mock_extractor_minio.upload_json.return_value = 1024
    mock_extractor_get_minio_client.return_value = mock_extractor_minio

    responses.add(
        responses.GET,
        "https://api.openweathermap.org/data/2.5/weather",
        json=sample_openweather_response,
        status=200,
    )
    mock_get_cities.return_value = sample_cities

    # Execute Extraction
    from src.extractor import Extractor

    extractor = Extractor()

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = None
    mock_ti.xcom_push.return_value = None

    extract_result = extractor.extract_openweather(ds="2026-01-29", task_instance=mock_ti)

    assert extract_result == len(sample_cities)

    # Setup Transformer
    mock_transformer_minio = Mock()

    sample_openweather_response["_metadata"] = {
        "city_name": "Madrid",
        "extraction_timestamp": "20260129_120000",
        "execution_date": "2026-01-29",
    }

    bronze_objects = [{"object_path": "current/2026-01-29/madrid.json"}]
    mock_transformer_minio.read_json.return_value = sample_openweather_response
    mock_transformer_minio.upload_parquet.return_value = 2048
    mock_transformer_get_minio_client.return_value = mock_transformer_minio

    # Execute Transformation
    from src.transformer import Transformer

    transformer = Transformer()

    mock_ti.xcom_pull.return_value = bronze_objects

    transform_result = transformer.transform_openweather(ds="2026-01-29", task_instance=mock_ti)

    assert transform_result > 0

    # Get the transformed DataFrame
    transform_call_args = mock_transformer_minio.upload_parquet.call_args
    transformed_df = transform_call_args[0][2]

    # Setup Loader
    mock_loader_minio = Mock()
    mock_loader_minio.read_parquet.return_value = transformed_df
    mock_loader_get_minio_client.return_value = mock_loader_minio

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = len(transformed_df)
    mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    # Execute Loading
    from src.loader import Loader

    loader = Loader()

    load_result = loader.load_fact_observation(ds="2026-01-29")

    # Verify end-to-end success
    assert load_result >= 0
    assert mock_execute_values.called or mock_cursor.execute.called


@pytest.mark.integration
@responses.activate
@patch("src.extractor.get_minio_client")
@patch("src.extractor.get_capitals_dataframe")
@patch("src.transformer.get_minio_client")
def test_openmeteo_daily_extract_transform(
    mock_transformer_get_minio_client,
    mock_get_capitals,
    mock_extractor_get_minio_client,
    sample_openmeteo_daily_response,
    sample_capitals_df,
    mock_extractor_db_access,
):
    """Test Open-Meteo daily Extract -> Transform pipeline"""
    # Setup Extractor
    mock_extractor_minio = Mock()
    mock_extractor_minio.upload_json.return_value = 1024
    mock_extractor_get_minio_client.return_value = mock_extractor_minio

    responses.add(
        responses.GET,
        "https://api.open-meteo.com/v1/forecast",
        json=sample_openmeteo_daily_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute Extraction
    from src.extractor import Extractor

    extractor = Extractor()

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = None
    mock_ti.xcom_push.return_value = None

    extract_result = extractor.extract_openmeteo_daily(ds="2026-01-29", task_instance=mock_ti)

    assert extract_result == len(sample_capitals_df)

    # Capture what was pushed to XCom
    xcom_calls = mock_ti.xcom_push.call_args_list
    bronze_objects = None
    for call in xcom_calls:
        if call[1].get("key") == "openmeteo_daily_objects":
            bronze_objects = call[1].get("value")

    assert bronze_objects is not None

    # Setup Transformer
    mock_transformer_minio = Mock()
    mock_transformer_minio.read_json.return_value = sample_openmeteo_daily_response
    mock_transformer_minio.upload_parquet.return_value = 2048
    mock_transformer_get_minio_client.return_value = mock_transformer_minio

    # Execute Transformation
    from src.transformer import Transformer

    transformer = Transformer()

    mock_ti.xcom_pull.return_value = bronze_objects

    transform_result = transformer.transform_openmeteo_daily(ds="2026-01-29", task_instance=mock_ti)

    assert transform_result > 0

    # Verify transformed data structure
    call_args = mock_transformer_minio.upload_parquet.call_args
    df = call_args[0][2]

    assert "time" in df.columns
    assert "city_code" in df.columns
    assert "temperature_2m_max" in df.columns


@pytest.mark.integration
@responses.activate
@patch("src.extractor.get_minio_client")
@patch("src.extractor.get_capitals_dataframe")
def test_multiple_extraction_types_parallel(
    mock_get_capitals,
    mock_extractor_get_minio_client,
    sample_openmeteo_daily_response,
    sample_openmeteo_hourly_response,
    sample_capitals_df,
    mock_extractor_db_access,
):
    """Test multiple extraction types can run (simulated parallel)"""
    mock_extractor_minio = Mock()
    mock_extractor_minio.upload_json.return_value = 1024
    mock_extractor_get_minio_client.return_value = mock_extractor_minio

    # Setup responses for multiple APIs
    responses.add(
        responses.GET,
        "https://api.open-meteo.com/v1/forecast",
        json=sample_openmeteo_daily_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df

    from src.extractor import Extractor

    extractor = Extractor()

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = None
    mock_ti.xcom_push.return_value = None

    # Run multiple extractions
    daily_result = extractor.extract_openmeteo_daily(ds="2026-01-29", task_instance=mock_ti)

    # Clear responses and add hourly
    responses.reset()
    responses.add(
        responses.GET,
        "https://api.open-meteo.com/v1/forecast",
        json=sample_openmeteo_hourly_response,
        status=200,
    )

    hourly_result = extractor.extract_openmeteo_hourly(ds="2026-01-29", task_instance=mock_ti)

    assert daily_result > 0
    assert hourly_result > 0


# ===== Data Consistency Tests =====


@pytest.mark.integration
@patch("src.transformer.get_minio_client")
def test_transformation_preserves_data_integrity(
    mock_transformer_get_minio_client, sample_openmeteo_daily_response
):
    """Test that transformation preserves data integrity"""
    mock_minio = Mock()

    sample_openmeteo_daily_response["city_code"] = "28079"
    sample_openmeteo_daily_response["municipio_nombre"] = "Madrid"
    sample_openmeteo_daily_response["_metadata"] = {"extraction_timestamp": "20260129_120000"}

    bronze_objects = [{"object_path": "forecast/daily/2026-01-29/madrid.json"}]
    mock_minio.read_json.return_value = sample_openmeteo_daily_response
    mock_minio.upload_parquet.return_value = 2048
    mock_transformer_get_minio_client.return_value = mock_minio

    from src.transformer import Transformer

    transformer = Transformer()

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = bronze_objects

    result = transformer.transform_openmeteo_daily(ds="2026-01-29", task_instance=mock_ti)

    call_args = mock_minio.upload_parquet.call_args
    df = call_args[0][2]

    # Verify original values preserved
    original_temps = sample_openmeteo_daily_response["daily"]["temperature_2m_max"]
    assert list(df["temperature_2m_max"]) == original_temps

    # Verify city info preserved
    assert df["city_code"].iloc[0] == "28079"
    assert df["city_name"].iloc[0] == "Madrid"


@pytest.mark.integration
@patch("src.loader.execute_values")
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_loader_deduplication(
    mock_connect, mock_get_minio_client, mock_execute_values, mock_db_connection
):
    """Test that loader handles duplicate data correctly"""
    mock_minio = Mock()

    # DataFrame with duplicate rows
    df = pd.DataFrame(
        {
            "time": ["2026-01-29", "2026-01-29"],  # Same time
            "city_code": ["28079", "28079"],  # Same city
            "city_name": ["Madrid", "Madrid"],
            "temperature_2m_max": [20.0, 20.0],
            "temperature_2m_min": [10.0, 10.0],
        }
    )

    mock_minio.read_parquet.return_value = df
    mock_get_minio_client.return_value = mock_minio

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1  # Only one after dedup
    mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    from src.loader import Loader

    loader = Loader()

    result = loader.load_fact_forecast_daily(ds="2026-01-29")

    # Should complete without error
    assert result >= 0


# ===== Error Recovery Tests =====


@pytest.mark.integration
@responses.activate
@patch("src.extractor.get_minio_client")
@patch("src.extractor.get_cities")
def test_extraction_partial_failure_recovery(
    mock_get_cities, mock_get_minio_client, sample_openweather_response,
    mock_extractor_db_access,
):
    """Test extraction continues after partial failures"""
    mock_minio = Mock()
    mock_minio.upload_json.return_value = 1024
    mock_get_minio_client.return_value = mock_minio

    # First city succeeds, second fails, third succeeds
    responses.add(
        responses.GET,
        "https://api.openweathermap.org/data/2.5/weather",
        json=sample_openweather_response,
        status=200,
    )
    responses.add(
        responses.GET,
        "https://api.openweathermap.org/data/2.5/weather",
        json={"error": "API limit"},
        status=429,
    )
    responses.add(
        responses.GET,
        "https://api.openweathermap.org/data/2.5/weather",
        json=sample_openweather_response,
        status=200,
    )

    mock_get_cities.return_value = [
        {"name": "Madrid", "lat": 40.4, "lon": -3.7},
        {"name": "BadCity", "lat": 0, "lon": 0},
        {"name": "Barcelona", "lat": 41.3, "lon": 2.1},
    ]

    from src.extractor import Extractor

    extractor = Extractor()

    result = extractor.extract_openweather(ds="2026-01-29")

    # Should have extracted 2 out of 3 cities
    assert result == 2
    assert mock_minio.upload_json.call_count == 2


@pytest.mark.integration
@patch("src.transformer.get_minio_client")
def test_transformation_handles_corrupted_file(mock_get_minio_client):
    """Test transformation gracefully handles corrupted JSON"""
    mock_minio = Mock()

    # First file is valid, second is corrupted
    valid_data = {
        "daily": {"time": ["2026-01-29"], "temperature_2m_max": [20.0]},
        "city_code": "28079",
        "municipio_nombre": "Madrid",
        "_metadata": {},
    }

    mock_minio.read_json.side_effect = [valid_data, Exception("JSON decode error")]
    mock_minio.upload_parquet.return_value = 2048
    mock_get_minio_client.return_value = mock_minio

    from src.transformer import Transformer

    transformer = Transformer()

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = [
        {"object_path": "forecast/daily/2026-01-29/madrid.json"},
        {"object_path": "forecast/daily/2026-01-29/corrupted.json"},
    ]

    result = transformer.transform_openmeteo_daily(ds="2026-01-29", task_instance=mock_ti)

    # Should process the valid file
    assert result == 1


# ===== Configuration Tests =====


@pytest.mark.integration
def test_bucket_configuration():
    """Test that bucket configuration is correctly set"""
    from src.config.lake_config import (
        BRONZE_BUCKET,
        BRONZE_OPENMETEO_BUCKET,
        SILVER_BUCKET,
        SILVER_OPENMETEO_BUCKET,
    )

    assert BRONZE_BUCKET is not None
    assert SILVER_BUCKET is not None
    assert BRONZE_OPENMETEO_BUCKET is not None
    assert SILVER_OPENMETEO_BUCKET is not None

    # Verify naming convention
    assert "bronze" in BRONZE_BUCKET.lower()
    assert "silver" in SILVER_BUCKET.lower()


@pytest.mark.integration
def test_api_configuration():
    """Test that API configuration is correctly set"""
    from src.config.apis.openmeteo_config import (
        DAILY_FORECAST_PARAMS,
        HOURLY_FORECAST_PARAMS,
        OPENMETEO_AIR_QUALITY_URL,
        OPENMETEO_FORECAST_URL,
        OPENMETEO_MARINE_URL,
    )

    assert OPENMETEO_FORECAST_URL.startswith("https://")
    assert OPENMETEO_AIR_QUALITY_URL.startswith("https://")
    assert OPENMETEO_MARINE_URL.startswith("https://")

    assert len(DAILY_FORECAST_PARAMS) > 0
    assert len(HOURLY_FORECAST_PARAMS) > 0


# ===== Backfill Scenario Tests =====


@pytest.mark.integration
@patch("src.loader.execute_values")
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_backfill_multiple_dates(
    mock_connect, mock_get_minio_client, mock_execute_values, mock_db_connection
):
    """Test loading data for multiple dates in sequence (backfill scenario)"""
    mock_minio_instance = Mock()

    dates = ["2026-01-27", "2026-01-28", "2026-01-29"]
    dfs_by_date = {}
    for d in dates:
        dfs_by_date[d] = pd.DataFrame(
            {
                "city_name": ["Madrid", "Barcelona"],
                "time": [d, d],
                "temperature_2m_max": [18.0, 19.0],
                "temperature_2m_min": [8.0, 9.0],
            }
        )

    mock_obj = Mock()
    mock_obj.object_name = "forecast/daily/test/daily.parquet"
    mock_minio_instance.client.list_objects.return_value = [mock_obj]
    mock_minio_instance.read_parquet.side_effect = [dfs_by_date[d] for d in dates]
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 2
    mock_cursor.fetchone.return_value = (1,)
    mock_cursor.fetchall.side_effect = (
        [[("Madrid", 1), ("Barcelona", 2)], [("28079", 1), ("08019", 2)]] * 3
    )
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    from src.loader import Loader

    loader = Loader()

    results = []
    for d in dates:
        # Reset list_objects for each call
        mock_minio_instance.client.list_objects.return_value = [mock_obj]
        result = loader.load_fact_forecast_daily(ds=d)
        results.append(result)

    # All dates should have loaded successfully
    assert all(r >= 0 for r in results)
    assert mock_execute_values.call_count == 3


# ===== Timestamp Skip Behavior Tests =====


@pytest.mark.integration
@patch("src.loader.execute_values")
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_observation_skips_records_without_timestamp(
    mock_connect, mock_get_minio_client, mock_execute_values, mock_db_connection
):
    """Test that load_fact_observation skips records missing the 'dt' timestamp"""
    mock_minio_instance = Mock()

    # 3 records: 2 with valid dt, 1 with missing dt
    df = pd.DataFrame(
        {
            "city": ["Madrid", "Barcelona", "Sevilla"],
            "temperature": [15.5, 18.0, 22.0],
            "feels_like": [14.0, 17.0, 21.0],
            "temp_min": [12.0, 15.0, 18.0],
            "temp_max": [18.0, 20.0, 25.0],
            "pressure": [1013, 1012, 1015],
            "humidity": [65, 70, 55],
            "visibility": [10000, 9000, 10000],
            "wind_speed": [3.5, 4.0, 2.5],
            "wind_deg": [180, 200, 150],
            "wind_gust": [5.0, 6.0, 4.0],
            "clouds": [10, 20, 5],
            "weather_id": [800, 801, 800],
            "weather_main": ["Clear", "Clouds", "Clear"],
            "weather_description": ["clear sky", "few clouds", "clear sky"],
            "rain_1h": [None, None, None],
            "rain_3h": [None, None, None],
            "snow_1h": [None, None, None],
            "snow_3h": [None, None, None],
            "dt": [1706543400, 1706543500, np.nan],  # Sevilla missing dt
        }
    )

    mock_minio_instance.read_parquet.return_value = df
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 2
    mock_cursor.fetchone.return_value = (1,)
    mock_cursor.fetchall.side_effect = [
        [("Madrid", 1), ("Barcelona", 2), ("Sevilla", 3)],
        [("28079", 1), ("08019", 2), ("41091", 3)],
    ]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    from src.loader import Loader

    loader = Loader()
    result = loader.load_fact_observation(ds="2026-01-29")

    # Should have inserted only 2 records (Sevilla skipped)
    assert result == 2
    assert mock_execute_values.called
    # Verify only 2 records were passed to execute_values
    call_args = mock_execute_values.call_args
    records = call_args[0][2]  # Third positional arg is the records list
    assert len(records) == 2


@pytest.mark.integration
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_observation_returns_zero_when_all_timestamps_missing(
    mock_connect, mock_get_minio_client, mock_db_connection
):
    """Test load_fact_observation returns 0 when all records lack timestamps"""
    mock_minio_instance = Mock()

    df = pd.DataFrame(
        {
            "city": ["Madrid", "Barcelona"],
            "temperature": [15.5, 18.0],
            "humidity": [65, 70],
            "pressure": [1013, 1012],
            # No 'dt' column at all
        }
    )

    mock_minio_instance.read_parquet.return_value = df
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (1,)
    mock_cursor.fetchall.side_effect = [
        [("Madrid", 1), ("Barcelona", 2)],
        [("28079", 1), ("08019", 2)],
    ]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    from src.loader import Loader

    loader = Loader()
    result = loader.load_fact_observation(ds="2026-01-29")

    assert result == 0


# ===== AEMET Historical Partial Failure Tests =====


@pytest.mark.integration
@patch("src.loader.execute_values")
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_aemet_historical_continues_on_file_error(
    mock_connect, mock_get_minio_client, mock_execute_values, mock_db_connection
):
    """Test AEMET historical loading continues when individual files fail"""
    mock_minio_instance = Mock()

    # 3 parquet files; second one raises an error
    mock_obj1 = Mock()
    mock_obj1.object_name = "historical/2025/file1.parquet"
    mock_obj2 = Mock()
    mock_obj2.object_name = "historical/2025/file2.parquet"
    mock_obj3 = Mock()
    mock_obj3.object_name = "historical/2025/file3.parquet"

    mock_minio_instance.client.list_objects.return_value = [mock_obj1, mock_obj2, mock_obj3]

    good_df = pd.DataFrame(
        {
            "station_id": ["3129"],
            "date": ["2025-06-15"],
            "temp_avg": [22.0],
            "temp_min": [15.0],
            "temp_max": [29.0],
        }
    )

    mock_minio_instance.read_parquet.side_effect = [
        good_df,
        Exception("Corrupted parquet file"),  # file2 fails
        good_df,
    ]
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1
    mock_cursor.fetchall.return_value = [("3129", 1)]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    from src.loader import Loader

    loader = Loader()
    result = loader.load_fact_aemet_historical(ds="2026-01-29")

    # Should have processed file1 and file3 (file2 skipped with error)
    assert result == 2
    assert mock_execute_values.call_count == 2


# ===== Cross-Table Data Consistency Tests =====


@pytest.mark.integration
@patch("src.loader.execute_values")
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_fact_records_only_reference_valid_dimensions(
    mock_connect, mock_get_minio_client, mock_execute_values, mock_db_connection
):
    """Test that fact records only reference city IDs that exist in dim_city"""
    mock_minio_instance = Mock()

    # 3 cities in data, but only 2 exist in dimension table
    df = pd.DataFrame(
        {
            "city_name": ["Madrid", "Barcelona", "UnknownCity"],
            "time": ["2026-01-29", "2026-01-29", "2026-01-29"],
            "temperature_2m_max": [18.0, 19.0, 20.0],
            "temperature_2m_min": [8.0, 9.0, 10.0],
        }
    )

    mock_obj = Mock()
    mock_obj.object_name = "forecast/daily/2026-01-29/daily.parquet"
    mock_minio_instance.client.list_objects.return_value = [mock_obj]
    mock_minio_instance.read_parquet.return_value = df
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 2
    mock_cursor.fetchone.return_value = (1,)
    # Only Madrid and Barcelona in dim_city
    mock_cursor.fetchall.side_effect = [
        [("Madrid", 1), ("Barcelona", 2)],
        [("28079", 1), ("08019", 2)],
    ]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    from src.loader import Loader

    loader = Loader()
    result = loader.load_fact_forecast_daily(ds="2026-01-29")

    # Only 2 records should be inserted (UnknownCity skipped)
    assert result == 2
    call_args = mock_execute_values.call_args
    records = call_args[0][2]
    assert len(records) == 2


# ===== Configurable Data Quality Thresholds Tests =====


@pytest.mark.integration
@pytest.mark.data_quality
def test_configurable_completeness_threshold():
    """Test that completeness threshold can be overridden via parameter"""
    from src.data_quality.expectations import validate_weather_observation

    # DataFrame with 50% completeness on temperature
    df = pd.DataFrame(
        {
            "city": ["Madrid", "Barcelona", "Sevilla", "Malaga"],
            "temperature": [15.0, None, 22.0, None],
            "humidity": [65.0, 70.0, 55.0, 60.0],
            "pressure": [1013.0, 1012.0, 1015.0, 1014.0],
        }
    )

    # With default threshold (0.90) this should fail completeness
    result_strict = validate_weather_observation(df, completeness_threshold=0.90)
    assert result_strict.success is False

    # With a relaxed threshold (0.40) this should pass
    result_relaxed = validate_weather_observation(df, completeness_threshold=0.40)
    assert result_relaxed.success is True


@pytest.mark.integration
@pytest.mark.data_quality
def test_configurable_mostly_tolerance():
    """Test that 'mostly' range-check tolerance can be overridden"""
    from src.data_quality.expectations import validate_weather_observation

    # 1 out of 4 values is out of range (25% outlier)
    df = pd.DataFrame(
        {
            "city": ["Madrid", "Barcelona", "Sevilla", "Malaga"],
            "temperature": [15.0, 20.0, 25.0, 999.0],  # 999 is out of range
            "humidity": [65.0, 70.0, 55.0, 60.0],
            "pressure": [1013.0, 1012.0, 1015.0, 1014.0],
        }
    )

    # With strict mostly (0.99) this should fail
    result_strict = validate_weather_observation(df, mostly=0.99)
    assert result_strict.success is False

    # With relaxed mostly (0.50) this should pass
    result_relaxed = validate_weather_observation(df, mostly=0.50)
    assert result_relaxed.success is True


@pytest.mark.integration
@pytest.mark.data_quality
def test_configurable_metrics_weights():
    """Test that DataQualityMetrics accepts custom weights"""
    from src.data_quality.metrics import DataQualityMetrics
    from src.data_quality.validators import DataQualityValidator, ValidationResult

    df = pd.DataFrame(
        {
            "city": ["Madrid", "Barcelona"],
            "temperature": [15.0, 20.0],
            "humidity": [65.0, 70.0],
        }
    )

    validation_result = ValidationResult(
        success=True,
        table_name="test",
        total_expectations=5,
        successful_expectations=5,
        failed_expectations=0,
    )

    # Custom weights emphasizing completeness
    custom_weights = {
        "completeness": 0.80,
        "validity": 0.10,
        "uniqueness": 0.05,
        "freshness": 0.05,
    }

    metrics = DataQualityMetrics(weights=custom_weights)
    assert metrics.weights["completeness"] == 0.80

    report = metrics.generate_report(
        df, "test_table", validation_result, key_columns=["city"]
    )

    assert report.overall_score > 0
    assert "completeness" in report.dimension_scores


# ===== Credential Validation Tests =====


@pytest.mark.integration
def test_secrets_manager_raises_on_missing_credentials(monkeypatch):
    """Test that SecretsManager raises ValueError when credentials are incomplete"""
    from src.config.secrets_manager import SecretsManager

    # Force non-Airflow mode and clear env vars
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.setenv("POSTGRES_DB", "testdb")

    manager = SecretsManager(use_airflow=False)

    with pytest.raises(ValueError, match="incomplete"):
        manager.get_postgres_credentials()


@pytest.mark.integration
def test_secrets_manager_no_warning_when_complete(monkeypatch, caplog):
    """Test that SecretsManager does NOT warn when credentials are complete"""
    from src.config.secrets_manager import SecretsManager

    monkeypatch.setenv("POSTGRES_USER", "validuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "validpass")
    monkeypatch.setenv("POSTGRES_DB", "validdb")

    with caplog.at_level(logging.WARNING):
        manager = SecretsManager(use_airflow=False)
        creds = manager.get_postgres_credentials()

    # Should NOT contain credential-incomplete warnings
    credential_warnings = [
        r.message for r in caplog.records
        if r.levelno >= logging.WARNING and "incomplete" in r.message.lower()
    ]
    assert len(credential_warnings) == 0
    assert creds.user == "validuser"


# ===== _find_parquet_files Helper Tests =====


@pytest.mark.integration
@patch("src.loader.get_minio_client")
def test_find_parquet_files_filters_non_parquet(mock_get_minio_client):
    """Test that _find_parquet_files only returns .parquet files"""
    mock_minio = Mock()

    mock_parquet = Mock()
    mock_parquet.object_name = "data/file1.parquet"
    mock_json = Mock()
    mock_json.object_name = "data/file1.json"
    mock_csv = Mock()
    mock_csv.object_name = "data/file1.csv"
    mock_parquet2 = Mock()
    mock_parquet2.object_name = "data/file2.parquet"

    mock_minio.client.list_objects.return_value = [
        mock_parquet, mock_json, mock_csv, mock_parquet2
    ]
    mock_get_minio_client.return_value = mock_minio

    from src.loader import Loader

    loader = Loader()
    result = loader._find_parquet_files("test-bucket", "data/")

    assert len(result) == 2
    assert all(f.endswith(".parquet") for f in result)
    # Should be sorted
    assert result == sorted(result)


@pytest.mark.integration
@patch("src.loader.get_minio_client")
def test_find_parquet_files_returns_empty_on_error(mock_get_minio_client):
    """Test that _find_parquet_files returns empty list on MinIO error"""
    mock_minio = Mock()
    mock_minio.client.list_objects.side_effect = Exception("Connection refused")
    mock_get_minio_client.return_value = mock_minio

    from src.loader import Loader

    loader = Loader()
    result = loader._find_parquet_files("test-bucket", "data/")

    assert result == []


# ===== Corrupted Parquet Tests =====


@pytest.mark.integration
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_corrupted_parquet_handled_gracefully(
    mock_connect, mock_get_minio_client, mock_db_connection
):
    """Test that corrupted parquet data from MinIO is handled gracefully."""
    mock_minio_instance = Mock()
    mock_minio_instance.read_parquet.side_effect = Exception(
        "ArrowInvalid: Parquet magic bytes not found"
    )
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (1,)
    mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    from src.loader import Loader

    loader = Loader()
    result = loader.load_fact_observation(ds="2026-01-29")
    assert result == 0


# ===== Concurrency Tests =====


@pytest.mark.integration
@patch("src.loader.execute_values")
@patch("src.loader.get_minio_client")
@patch("src.base_loader.return_db_connection")
@patch("src.base_loader.get_db_connection")
def test_concurrent_loader_calls_no_race_condition(
    mock_get_conn, mock_return_conn, mock_get_minio_client, mock_execute_values,
):
    """Two threads loading the same date should both complete without error."""
    mock_minio_instance = Mock()

    df = pd.DataFrame({
        "city_name": ["Madrid"],
        "time": ["2026-01-29"],
        "temperature_2m_max": [18.5],
        "temperature_2m_min": [8.3],
    })

    mock_obj = Mock()
    mock_obj.object_name = "forecast/daily/2026-01-29/daily.parquet"
    mock_minio_instance.client.list_objects.return_value = [mock_obj]
    mock_minio_instance.read_parquet.return_value = df
    mock_get_minio_client.return_value = mock_minio_instance

    # Each thread gets its own mock connection to avoid shared-state issues
    def make_mock_conn():
        conn = MagicMock()
        cursor = MagicMock()
        cursor.__enter__.return_value = cursor
        cursor.rowcount = 1
        cursor.fetchone.return_value = (1,)
        cursor.fetchall.return_value = [("Madrid", 1)]
        conn.cursor.return_value = cursor
        return conn

    mock_get_conn.side_effect = lambda: make_mock_conn()

    from src.loader import Loader

    errors = []
    results = []

    def load_in_thread():
        try:
            loader = Loader()
            r = loader.load_fact_forecast_daily(ds="2026-01-29")
            results.append(r)
        except Exception as e:
            errors.append(e)

    t1 = threading.Thread(target=load_in_thread)
    t2 = threading.Thread(target=load_in_thread)
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert len(errors) == 0, f"Unexpected errors: {errors}"
    assert len(results) == 2
    assert all(r >= 0 for r in results)
    # Both threads returned their connections to the pool
    assert mock_return_conn.call_count >= 2
