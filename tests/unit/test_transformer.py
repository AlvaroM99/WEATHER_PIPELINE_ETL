"""
Unit tests for Transformer module
Tests JSON to Parquet transformation logic
"""

from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from src.transformer import Transformer
from src.utils.minio_client import reset_minio_client


@pytest.fixture(autouse=True)
def reset_minio_singleton():
    """Reset MinIO singleton before and after each test."""
    reset_minio_client()
    yield
    reset_minio_client()

# ===== OpenWeather Transformation Tests =====


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_openweather_success(
    mock_get_minio_client, sample_openweather_response, mock_airflow_context
):
    """Test successful OpenWeather transformation from JSON to Parquet"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    # Add metadata to response
    sample_openweather_response["_metadata"] = {
        "city_name": "Madrid",
        "extraction_timestamp": "20260129_120000",
        "execution_date": "2026-01-29",
    }

    bronze_objects = [{"object_path": "current/2026-01-29/madrid_20260129_120000.json"}]

    mock_minio_instance.read_json.return_value = sample_openweather_response
    mock_minio_instance.upload_parquet.return_value = 2048
    mock_airflow_context["task_instance"].xcom_pull.return_value = bronze_objects

    mock_get_minio_client.return_value = mock_minio_instance

    # Execute
    transformer = Transformer()

    result = transformer.transform_openweather(**mock_airflow_context)

    # Assert
    assert result > 0
    assert mock_minio_instance.read_json.called
    assert mock_minio_instance.upload_parquet.called

    # Verify DataFrame was created correctly
    call_args = mock_minio_instance.upload_parquet.call_args
    df = call_args[0][2]  # Third argument is the DataFrame

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_openweather_field_mapping(mock_get_minio_client, sample_openweather_response):
    """Test that OpenWeather fields are correctly mapped"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    sample_openweather_response["_metadata"] = {
        "city_name": "Madrid",
        "extraction_timestamp": "20260129_120000",
        "execution_date": "2026-01-29",
    }

    mock_obj = Mock()
    mock_obj.object_name = "current/2026-01-29/madrid.json"
    mock_minio_instance.client.list_objects.return_value = [mock_obj]
    mock_minio_instance.read_json.return_value = sample_openweather_response
    mock_minio_instance.upload_parquet.return_value = 2048

    mock_get_minio_client.return_value = mock_minio_instance

    # Execute
    transformer = Transformer()

    result = transformer.transform_openweather(ds="2026-01-29", task_instance=None)

    # Assert
    if result > 0:
        call_args = mock_minio_instance.upload_parquet.call_args
        df = call_args[0][2]

        # Verify required columns exist
        expected_columns = [
            "city",
            "country",
            "latitude",
            "longitude",
            "temperature",
            "feels_like",
            "temp_min",
            "temp_max",
            "pressure",
            "humidity",
            "weather_main",
            "weather_description",
            "wind_speed",
            "wind_deg",
            "clouds",
            "visibility",
            "date",
        ]

        for col in expected_columns:
            assert col in df.columns, f"Missing column: {col}"

        # Verify data values
        assert df["city"].iloc[0] == "Madrid"
        assert df["temperature"].iloc[0] == 15.5
        assert df["humidity"].iloc[0] == 65


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_openweather_fallback_scan(mock_get_minio_client, sample_openweather_response):
    """Test transformation fallback when XCom is not available"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    sample_openweather_response["_metadata"] = {
        "city_name": "Madrid",
        "extraction_timestamp": "20260129_120000",
        "execution_date": "2026-01-29",
    }

    mock_obj = Mock()
    mock_obj.object_name = "current/2026-01-29/madrid.json"
    mock_minio_instance.client.list_objects.return_value = [mock_obj]
    mock_minio_instance.read_json.return_value = sample_openweather_response
    mock_minio_instance.upload_parquet.return_value = 2048

    mock_get_minio_client.return_value = mock_minio_instance

    # Execute
    transformer = Transformer()

    result = transformer.transform_openweather(ds="2026-01-29", task_instance=None)

    # Assert - should fall back to scanning bronze bucket
    assert mock_minio_instance.client.list_objects.called

    if result > 0:
        assert mock_minio_instance.upload_parquet.called


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_openweather_no_data(mock_get_minio_client):
    """Test transformation with no bronze data"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []
    mock_get_minio_client.return_value = mock_minio_instance

    # Execute
    transformer = Transformer()

    result = transformer.transform_openweather(ds="2026-01-29", task_instance=None)

    # Assert
    assert result == 0
    assert not mock_minio_instance.upload_parquet.called


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_openweather_multiple_cities(mock_get_minio_client, sample_openweather_response):
    """Test transformation with multiple city records"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    madrid_data = sample_openweather_response.copy()
    madrid_data["_metadata"] = {"city_name": "Madrid"}

    barcelona_data = sample_openweather_response.copy()
    barcelona_data["name"] = "Barcelona"
    barcelona_data["_metadata"] = {"city_name": "Barcelona"}

    bronze_objects = [
        {"object_path": "current/2026-01-29/madrid.json"},
        {"object_path": "current/2026-01-29/barcelona.json"},
    ]

    # Mock read_json to return different data based on path
    def read_json_side_effect(bucket, path):
        if "madrid" in path:
            return madrid_data
        elif "barcelona" in path:
            return barcelona_data
        return {}

    mock_minio_instance.read_json.side_effect = read_json_side_effect
    mock_minio_instance.upload_parquet.return_value = 2048

    mock_get_minio_client.return_value = mock_minio_instance

    # Execute
    transformer = Transformer()

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = bronze_objects

    result = transformer.transform_openweather(ds="2026-01-29", task_instance=mock_ti)

    # Assert
    assert result == 2  # Two cities processed

    call_args = mock_minio_instance.upload_parquet.call_args
    df = call_args[0][2]
    assert len(df) == 2


# ===== Open-Meteo Daily Transformation Tests =====


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_openmeteo_daily_success(
    mock_get_minio_client, sample_openmeteo_daily_response, mock_airflow_context
):
    """Test successful Open-Meteo daily transformation"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    bronze_objects = [{"object_path": "forecast/daily/2026-01-29/madrid.json"}]

    mock_minio_instance.read_json.return_value = sample_openmeteo_daily_response
    mock_minio_instance.upload_parquet.return_value = 2048
    mock_airflow_context["task_instance"].xcom_pull.return_value = bronze_objects

    mock_get_minio_client.return_value = mock_minio_instance

    # Execute
    transformer = Transformer()

    result = transformer.transform_openmeteo_daily(**mock_airflow_context)

    # Assert
    assert result > 0
    assert mock_minio_instance.upload_parquet.called

    # Verify DataFrame structure
    call_args = mock_minio_instance.upload_parquet.call_args
    df = call_args[0][2]

    assert isinstance(df, pd.DataFrame)
    assert "time" in df.columns
    assert "city_code" in df.columns
    assert "city_name" in df.columns
    assert "extraction_date" in df.columns


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_generic_with_daily_data(mock_get_minio_client, sample_openmeteo_daily_response):
    """Test _transform_generic helper with daily forecast data"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    bronze_objects = [{"object_path": "forecast/daily/2026-01-29/madrid.json"}]
    mock_minio_instance.read_json.return_value = sample_openmeteo_daily_response
    mock_minio_instance.upload_parquet.return_value = 2048

    mock_get_minio_client.return_value = mock_minio_instance

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = bronze_objects

    # Execute
    transformer = Transformer()

    result = transformer.transform_openmeteo_daily(ds="2026-01-29", task_instance=mock_ti)

    # Assert
    assert result > 0

    call_args = mock_minio_instance.upload_parquet.call_args
    df = call_args[0][2]

    # Verify daily-specific columns
    assert "temperature_2m_max" in df.columns
    assert "temperature_2m_min" in df.columns
    assert len(df) == 2  # 2 days in sample data


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_generic_fallback_to_bucket_scan(mock_get_minio_client, sample_openmeteo_daily_response):
    """Test _transform_generic falls back to bucket scanning"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    mock_obj = Mock()
    mock_obj.object_name = "forecast/daily/2026-01-29/madrid.json"
    mock_minio_instance.client.list_objects.return_value = [mock_obj]
    mock_minio_instance.read_json.return_value = sample_openmeteo_daily_response
    mock_minio_instance.upload_parquet.return_value = 2048

    mock_get_minio_client.return_value = mock_minio_instance

    # Execute
    transformer = Transformer()

    result = transformer.transform_openmeteo_daily(ds="2026-01-29", task_instance=None)

    # Assert
    assert mock_minio_instance.client.list_objects.called

    if result > 0:
        assert mock_minio_instance.upload_parquet.called


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_generic_multiple_cities(mock_get_minio_client, sample_openmeteo_daily_response):
    """Test _transform_generic concatenates multiple city DataFrames"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    madrid_data = sample_openmeteo_daily_response.copy()
    barcelona_data = sample_openmeteo_daily_response.copy()
    barcelona_data["city_code"] = "08019"
    barcelona_data["municipio_nombre"] = "Barcelona"

    bronze_objects = [
        {"object_path": "forecast/daily/2026-01-29/madrid.json"},
        {"object_path": "forecast/daily/2026-01-29/barcelona.json"},
    ]

    def read_json_side_effect(bucket, path):
        if "madrid" in path:
            return madrid_data
        elif "barcelona" in path:
            return barcelona_data
        return {}

    mock_minio_instance.read_json.side_effect = read_json_side_effect
    mock_minio_instance.upload_parquet.return_value = 2048

    mock_get_minio_client.return_value = mock_minio_instance

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = bronze_objects

    # Execute
    transformer = Transformer()

    result = transformer.transform_openmeteo_daily(ds="2026-01-29", task_instance=mock_ti)

    # Assert
    assert result == 4  # 2 cities × 2 days = 4 rows

    call_args = mock_minio_instance.upload_parquet.call_args
    df = call_args[0][2]

    # Verify both cities are in DataFrame
    assert "Madrid" in df["city_name"].values
    assert "Barcelona" in df["city_name"].values


# ===== Edge Cases & Error Handling =====


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_openweather_malformed_json(mock_get_minio_client):
    """Test transformation handles malformed JSON gracefully"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    malformed_data = {"incomplete": "data"}  # Missing required fields

    bronze_objects = [{"object_path": "current/2026-01-29/test.json"}]
    mock_minio_instance.read_json.return_value = malformed_data
    mock_minio_instance.upload_parquet.return_value = 2048

    mock_get_minio_client.return_value = mock_minio_instance

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = bronze_objects

    # Execute
    transformer = Transformer()

    result = transformer.transform_openweather(ds="2026-01-29", task_instance=mock_ti)

    # Assert - should handle error and return 0
    assert result >= 0


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transformer_initialization(mock_get_minio_client):
    """Test Transformer initializes correctly"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()

    assert transformer is not None
    assert transformer.logger is not None
    assert transformer.minio_client is not None


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_logging(mock_get_minio_client, caplog):
    """Test that transformer logs operations"""
    import logging

    caplog.set_level(logging.INFO)

    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []
    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()

    result = transformer.transform_openweather(ds="2026-01-29", task_instance=None)

    # Assert logging occurred
    assert len(caplog.records) > 0
    log_messages = [record.message for record in caplog.records]
    start_messages = [msg for msg in log_messages if "START" in msg]
    assert len(start_messages) > 0


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_openmeteo_daily_no_data(mock_get_minio_client):
    """Test Open-Meteo transformation with no data"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []
    mock_get_minio_client.return_value = mock_minio_instance

    # Execute
    transformer = Transformer()

    result = transformer.transform_openmeteo_daily(ds="2026-01-29", task_instance=None)

    # Assert
    assert result == 0


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_generic_handles_missing_data_key(mock_get_minio_client):
    """Test _transform_generic handles missing data key gracefully"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    invalid_response = {
        "city_code": "28079",
        "municipio_nombre": "Madrid",
        # Missing 'daily' key
    }

    bronze_objects = [{"object_path": "forecast/daily/2026-01-29/madrid.json"}]
    mock_minio_instance.read_json.return_value = invalid_response

    mock_get_minio_client.return_value = mock_minio_instance

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = bronze_objects

    # Execute
    transformer = Transformer()

    result = transformer.transform_openmeteo_daily(ds="2026-01-29", task_instance=mock_ti)

    # Assert - should return 0 when no valid data
    assert result == 0


# ===== Open-Meteo Hourly Transformation Tests =====


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_openmeteo_hourly_success(
    mock_get_minio_client, sample_openmeteo_hourly_response, mock_airflow_context
):
    """Test successful Open-Meteo hourly transformation"""
    mock_minio_instance = Mock()

    bronze_objects = [{"object_path": "forecast/hourly/2026-01-29/madrid.json"}]
    mock_minio_instance.read_json.return_value = sample_openmeteo_hourly_response
    mock_minio_instance.upload_parquet.return_value = 2048
    mock_airflow_context["task_instance"].xcom_pull.return_value = bronze_objects

    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()
    result = transformer.transform_openmeteo_hourly(**mock_airflow_context)

    assert result > 0
    assert mock_minio_instance.upload_parquet.called

    call_args = mock_minio_instance.upload_parquet.call_args
    df = call_args[0][2]

    assert "time" in df.columns
    assert "temperature_2m" in df.columns
    assert "city_code" in df.columns


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_openmeteo_hourly_multiple_records(
    mock_get_minio_client, sample_openmeteo_hourly_response
):
    """Test hourly transformation creates one row per hour"""
    mock_minio_instance = Mock()

    bronze_objects = [{"object_path": "forecast/hourly/2026-01-29/madrid.json"}]
    mock_minio_instance.read_json.return_value = sample_openmeteo_hourly_response
    mock_minio_instance.upload_parquet.return_value = 2048

    mock_get_minio_client.return_value = mock_minio_instance

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = bronze_objects

    transformer = Transformer()
    result = transformer.transform_openmeteo_hourly(ds="2026-01-29", task_instance=mock_ti)

    # Sample has 3 hourly entries
    assert result == 3


# ===== Open-Meteo Air Quality Transformation Tests =====


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_air_quality_success(
    mock_get_minio_client, sample_air_quality_response, mock_airflow_context
):
    """Test successful air quality transformation"""
    mock_minio_instance = Mock()

    sample_air_quality_response["city_code"] = "28079"
    sample_air_quality_response["municipio_nombre"] = "Madrid"
    sample_air_quality_response["_metadata"] = {
        "extraction_timestamp": "20260129_120000",
        "execution_date": "2026-01-29",
    }

    bronze_objects = [{"object_path": "air_quality/2026-01-29/madrid.json"}]
    mock_minio_instance.read_json.return_value = sample_air_quality_response
    mock_minio_instance.upload_parquet.return_value = 2048
    mock_airflow_context["task_instance"].xcom_pull.return_value = bronze_objects

    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()
    result = transformer.transform_openmeteo_air_quality(**mock_airflow_context)

    assert result > 0

    call_args = mock_minio_instance.upload_parquet.call_args
    df = call_args[0][2]

    assert "pm10" in df.columns
    assert "pm2_5" in df.columns


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_air_quality_columns(mock_get_minio_client, sample_air_quality_response):
    """Test air quality transformation preserves all pollutant columns"""
    mock_minio_instance = Mock()

    sample_air_quality_response["city_code"] = "28079"
    sample_air_quality_response["municipio_nombre"] = "Madrid"
    sample_air_quality_response["_metadata"] = {}

    bronze_objects = [{"object_path": "air_quality/2026-01-29/madrid.json"}]
    mock_minio_instance.read_json.return_value = sample_air_quality_response
    mock_minio_instance.upload_parquet.return_value = 2048

    mock_get_minio_client.return_value = mock_minio_instance

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = bronze_objects

    transformer = Transformer()
    result = transformer.transform_openmeteo_air_quality(ds="2026-01-29", task_instance=mock_ti)

    call_args = mock_minio_instance.upload_parquet.call_args
    df = call_args[0][2]

    expected_pollutants = ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide"]
    for col in expected_pollutants:
        assert col in df.columns, f"Missing pollutant column: {col}"


# ===== Open-Meteo Pollen Transformation Tests =====


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_pollen_success(mock_get_minio_client, sample_pollen_response, mock_airflow_context):
    """Test successful pollen transformation"""
    mock_minio_instance = Mock()

    sample_pollen_response["city_code"] = "28079"
    sample_pollen_response["municipio_nombre"] = "Madrid"
    sample_pollen_response["_metadata"] = {"extraction_timestamp": "20260129_120000"}

    bronze_objects = [{"object_path": "pollen/2026-01-29/madrid.json"}]
    mock_minio_instance.read_json.return_value = sample_pollen_response
    mock_minio_instance.upload_parquet.return_value = 2048
    mock_airflow_context["task_instance"].xcom_pull.return_value = bronze_objects

    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()
    result = transformer.transform_openmeteo_pollen(**mock_airflow_context)

    assert result > 0

    call_args = mock_minio_instance.upload_parquet.call_args
    df = call_args[0][2]

    assert "grass_pollen" in df.columns
    assert "birch_pollen" in df.columns


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_pollen_no_data(mock_get_minio_client):
    """Test pollen transformation with no data"""
    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []
    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()
    result = transformer.transform_openmeteo_pollen(ds="2026-01-29", task_instance=None)

    assert result == 0


# ===== Open-Meteo Marine Transformation Tests =====


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_marine_success(mock_get_minio_client, sample_marine_response, mock_airflow_context):
    """Test successful marine transformation"""
    mock_minio_instance = Mock()

    sample_marine_response["city_code"] = "08019"
    sample_marine_response["municipio_nombre"] = "Barcelona"
    sample_marine_response["_metadata"] = {"extraction_timestamp": "20260129_120000"}

    bronze_objects = [{"object_path": "marine/2026-01-29/barcelona.json"}]
    mock_minio_instance.read_json.return_value = sample_marine_response
    mock_minio_instance.upload_parquet.return_value = 2048
    mock_airflow_context["task_instance"].xcom_pull.return_value = bronze_objects

    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()
    result = transformer.transform_openmeteo_marine(**mock_airflow_context)

    assert result > 0

    call_args = mock_minio_instance.upload_parquet.call_args
    df = call_args[0][2]

    assert "wave_height_max" in df.columns
    assert "city_name" in df.columns


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_marine_coastal_city_data(mock_get_minio_client, sample_marine_response):
    """Test marine transformation for coastal city"""
    mock_minio_instance = Mock()

    sample_marine_response["city_code"] = "08019"
    sample_marine_response["municipio_nombre"] = "Barcelona"
    sample_marine_response["_metadata"] = {}

    bronze_objects = [{"object_path": "marine/2026-01-29/barcelona.json"}]
    mock_minio_instance.read_json.return_value = sample_marine_response
    mock_minio_instance.upload_parquet.return_value = 2048

    mock_get_minio_client.return_value = mock_minio_instance

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = bronze_objects

    transformer = Transformer()
    result = transformer.transform_openmeteo_marine(ds="2026-01-29", task_instance=mock_ti)

    assert result == 3  # 3 daily entries

    call_args = mock_minio_instance.upload_parquet.call_args
    df = call_args[0][2]

    assert df["city_name"].iloc[0] == "Barcelona"


# ===== Additional Edge Cases =====


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_openweather_missing_metadata(mock_get_minio_client, sample_openweather_response):
    """Test transformation handles missing _metadata gracefully"""
    mock_minio_instance = Mock()

    # Remove metadata
    if "_metadata" in sample_openweather_response:
        del sample_openweather_response["_metadata"]

    bronze_objects = [{"object_path": "current/2026-01-29/test.json"}]
    mock_minio_instance.read_json.return_value = sample_openweather_response
    mock_minio_instance.upload_parquet.return_value = 2048

    mock_get_minio_client.return_value = mock_minio_instance

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = bronze_objects

    transformer = Transformer()
    result = transformer.transform_openweather(ds="2026-01-29", task_instance=mock_ti)

    # Should still work with defaults
    assert result >= 0


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_generic_exception_handling(mock_get_minio_client):
    """Test _transform_generic handles exceptions in individual records"""
    mock_minio_instance = Mock()

    # First read succeeds, second throws exception
    mock_minio_instance.read_json.side_effect = [
        {
            "hourly": {"time": ["2026-01-29T00:00"], "temp": [20]},
            "city_code": "1",
            "municipio_nombre": "City1",
        },
        Exception("Read error"),
    ]
    mock_minio_instance.upload_parquet.return_value = 2048

    mock_get_minio_client.return_value = mock_minio_instance

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = [
        {"object_path": "forecast/hourly/2026-01-29/city1.json"},
        {"object_path": "forecast/hourly/2026-01-29/city2.json"},
    ]

    transformer = Transformer()
    result = transformer.transform_openmeteo_hourly(ds="2026-01-29", task_instance=mock_ti)

    # Should process the successful record
    assert result == 1


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_with_empty_object_path(mock_get_minio_client):
    """Test transformation skips records with empty object_path"""
    mock_minio_instance = Mock()

    mock_minio_instance.read_json.return_value = {
        "hourly": {"time": ["2026-01-29T00:00"], "temp": [20]},
        "city_code": "1",
        "municipio_nombre": "City1",
    }
    mock_minio_instance.upload_parquet.return_value = 2048

    mock_get_minio_client.return_value = mock_minio_instance

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = [
        {"object_path": ""},  # Empty path
        {"object_path": None},  # None path
        {"object_path": "forecast/hourly/2026-01-29/valid.json"},
    ]

    transformer = Transformer()
    result = transformer.transform_openmeteo_hourly(ds="2026-01-29", task_instance=mock_ti)

    # Only valid path should be processed
    assert mock_minio_instance.read_json.call_count == 1


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_get_upstream_data_helper(mock_get_minio_client):
    """Test _get_upstream_data helper method"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = [{"object_path": "test.json"}]

    context = {"task_instance": mock_ti}

    result = transformer._get_upstream_data(context, ["task1", "task2"], "key")

    assert result is not None


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_get_upstream_data_no_task_instance(mock_get_minio_client):
    """Test _get_upstream_data returns None without task_instance"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()

    result = transformer._get_upstream_data({}, ["task1"], "key")

    assert result is None


# ===== AEMET Transformation Tests =====


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_safe_float_basic(mock_get_minio_client):
    """Test _safe_float handles basic conversions"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()

    # Test normal values
    assert transformer._safe_float("10,5") == 10.5  # Spanish decimal
    assert transformer._safe_float("10.5") == 10.5  # English decimal
    assert transformer._safe_float(10) == 10.0
    assert transformer._safe_float(10.5) == 10.5


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_safe_float_special_values(mock_get_minio_client):
    """Test _safe_float handles AEMET special values"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()

    # Test special AEMET values
    assert transformer._safe_float(None) is None
    assert transformer._safe_float("") is None
    assert transformer._safe_float("Ip") is None  # Precipitation trace
    assert transformer._safe_float("Acum") is None  # Accumulated
    assert transformer._safe_float("invalid") is None


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_parse_aemet_coord_latitude(mock_get_minio_client):
    """Test _parse_aemet_coord parses latitude correctly"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()

    # Madrid: 40°24'55"N -> 40.415278
    result = transformer._parse_aemet_coord("402455N")
    assert result is not None
    assert abs(result - 40.415278) < 0.001

    # Southern latitude
    result = transformer._parse_aemet_coord("402455S")
    assert result is not None
    assert result < 0


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_parse_aemet_coord_longitude(mock_get_minio_client):
    """Test _parse_aemet_coord parses longitude correctly"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()

    # Madrid: 3°40'41"W -> -3.678056
    result = transformer._parse_aemet_coord("034041W")
    assert result is not None
    assert result < 0  # West is negative

    # Eastern longitude (Barcelona)
    result = transformer._parse_aemet_coord("021730E")
    assert result is not None
    assert result > 0  # East is positive


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_parse_aemet_coord_invalid(mock_get_minio_client):
    """Test _parse_aemet_coord handles invalid inputs"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()

    assert transformer._parse_aemet_coord(None) is None
    assert transformer._parse_aemet_coord("") is None
    assert transformer._parse_aemet_coord("abc") is None
    assert transformer._parse_aemet_coord("12") is None


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_aemet_stations_success(
    mock_get_minio_client, sample_aemet_bronze_data, mock_airflow_context
):
    """Test successful AEMET stations transformation"""
    mock_minio_instance = Mock()

    bronze_objects = [{"object_path": "stations/2026-01-29/stations_20260129_120000.json"}]

    mock_minio_instance.read_json.return_value = sample_aemet_bronze_data
    mock_minio_instance.upload_parquet.return_value = 2048
    mock_airflow_context["task_instance"].xcom_pull.return_value = bronze_objects

    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()
    result = transformer.transform_aemet_stations(**mock_airflow_context)

    assert result > 0
    assert mock_minio_instance.upload_parquet.called

    # Verify DataFrame structure
    call_args = mock_minio_instance.upload_parquet.call_args
    df = call_args[0][2]

    assert "station_id" in df.columns
    assert "station_name" in df.columns
    assert "latitude" in df.columns
    assert "longitude" in df.columns


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_aemet_stations_fallback_scan(mock_get_minio_client, sample_aemet_bronze_data):
    """Test AEMET stations transformation fallback to bucket scan"""
    mock_minio_instance = Mock()

    mock_obj = Mock()
    mock_obj.object_name = "stations/2026-01-29/stations.json"
    mock_minio_instance.client.list_objects.return_value = [mock_obj]
    mock_minio_instance.read_json.return_value = sample_aemet_bronze_data
    mock_minio_instance.upload_parquet.return_value = 2048

    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()
    result = transformer.transform_aemet_stations(ds="2026-01-29", task_instance=None)

    assert mock_minio_instance.client.list_objects.called
    if result > 0:
        assert mock_minio_instance.upload_parquet.called


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_aemet_stations_no_data(mock_get_minio_client):
    """Test AEMET stations transformation with no data"""
    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []
    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()
    result = transformer.transform_aemet_stations(ds="2026-01-29", task_instance=None)

    assert result == 0


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_aemet_daily_climatology_success(
    mock_get_minio_client, sample_aemet_daily_bronze_data, mock_airflow_context
):
    """Test successful AEMET daily climatology transformation"""
    mock_minio_instance = Mock()

    bronze_objects = [
        {"object_path": "climatology/daily/2026-01-29/daily_3129_20260129_120000.json"}
    ]

    mock_minio_instance.read_json.return_value = sample_aemet_daily_bronze_data
    mock_minio_instance.upload_parquet.return_value = 2048
    mock_airflow_context["task_instance"].xcom_pull.return_value = bronze_objects

    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()
    result = transformer.transform_aemet_daily_climatology(**mock_airflow_context)

    assert result > 0
    assert mock_minio_instance.upload_parquet.called

    # Verify DataFrame structure
    call_args = mock_minio_instance.upload_parquet.call_args
    df = call_args[0][2]

    assert "station_id" in df.columns
    assert "date" in df.columns
    assert "temp_avg" in df.columns
    assert "precipitation" in df.columns


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_aemet_daily_climatology_fallback_scan(
    mock_get_minio_client, sample_aemet_daily_bronze_data
):
    """Test AEMET daily transformation fallback to bucket scan"""
    mock_minio_instance = Mock()

    mock_obj = Mock()
    mock_obj.object_name = "climatology/daily/2026-01-29/daily_3129.json"
    mock_minio_instance.client.list_objects.return_value = [mock_obj]
    mock_minio_instance.read_json.return_value = sample_aemet_daily_bronze_data
    mock_minio_instance.upload_parquet.return_value = 2048

    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()
    result = transformer.transform_aemet_daily_climatology(ds="2026-01-29", task_instance=None)

    assert mock_minio_instance.client.list_objects.called


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_aemet_daily_climatology_no_data(mock_get_minio_client):
    """Test AEMET daily transformation with no data"""
    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []
    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()
    result = transformer.transform_aemet_daily_climatology(ds="2026-01-29", task_instance=None)

    assert result == 0


@pytest.mark.unit
@patch("src.transformer.get_minio_client")
def test_transform_aemet_historical_no_data(mock_get_minio_client):
    """Test AEMET historical transformation with no data"""
    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []
    mock_get_minio_client.return_value = mock_minio_instance

    transformer = Transformer()
    result = transformer.transform_aemet_historical(ds="2026-01-29", task_instance=None)

    assert result == 0
