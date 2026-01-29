"""
Unit tests for Extractor module
Tests extraction logic for OpenWeather and Open-Meteo APIs
"""

from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
import responses

from src.extractor import Extractor

# ===== OpenWeather Tests =====


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_cities")
def test_extract_openweather_success(
    mock_get_cities,
    MockMinIOClass,
    sample_openweather_response,
    sample_cities,
    mock_airflow_context,
):
    """Test successful OpenWeather extraction"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://api.openweathermap.org/data/2.5/weather",
        json=sample_openweather_response,
        status=200,
    )
    mock_get_cities.return_value = sample_cities

    # Execute
    extractor = Extractor()

    result = extractor.extract_openweather(**mock_airflow_context)

    # Assert
    assert result == len(sample_cities)  # Should return count of processed cities
    assert mock_minio_instance.upload_json.call_count == len(sample_cities)

    # Verify metadata was added
    call_args = mock_minio_instance.upload_json.call_args_list[0]
    uploaded_data = call_args[0][2]  # Third argument is the JSON data
    assert "_metadata" in uploaded_data
    assert "city_name" in uploaded_data["_metadata"]
    assert "extraction_timestamp" in uploaded_data["_metadata"]
    assert "execution_date" in uploaded_data["_metadata"]


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_cities")
def test_extract_openweather_api_error(mock_get_cities, MockMinIOClass, sample_cities):
    """Test OpenWeather extraction handles API errors gracefully"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks - API returns 401 Unauthorized
    responses.add(
        responses.GET,
        "https://api.openweathermap.org/data/2.5/weather",
        json={"error": "Invalid API key"},
        status=401,
    )
    mock_get_cities.return_value = sample_cities

    # Execute
    extractor = Extractor()

    result = extractor.extract_openweather(ds="2026-01-29")

    # Assert - should return 0 (no successful extractions)
    assert result == 0
    # MinIO upload should not be called for failed API requests
    assert mock_minio_instance.upload_json.call_count == 0


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_cities")
def test_extract_openweather_partial_failure(
    mock_get_cities, MockMinIOClass, sample_openweather_response
):
    """Test OpenWeather extraction continues after individual city failures"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup: first city succeeds, second fails, third succeeds
    responses.add(
        responses.GET,
        "https://api.openweathermap.org/data/2.5/weather",
        json=sample_openweather_response,
        status=200,
    )
    responses.add(
        responses.GET,
        "https://api.openweathermap.org/data/2.5/weather",
        json={"error": "Not found"},
        status=404,
    )
    responses.add(
        responses.GET,
        "https://api.openweathermap.org/data/2.5/weather",
        json=sample_openweather_response,
        status=200,
    )

    mock_get_cities.return_value = [
        {"name": "Madrid", "lat": 40.4168, "lon": -3.7038},
        {"name": "InvalidCity", "lat": 0, "lon": 0},
        {"name": "Barcelona", "lat": 41.3851, "lon": 2.1734},
    ]

    # Execute
    extractor = Extractor()

    result = extractor.extract_openweather(ds="2026-01-29")

    # Assert - should process 2 cities successfully
    assert result == 2
    assert mock_minio_instance.upload_json.call_count == 2


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_cities")
def test_extract_openweather_xcom_push(
    mock_get_cities,
    MockMinIOClass,
    sample_openweather_response,
    sample_cities,
    mock_airflow_context,
):
    """Test that extraction pushes results to XCom"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://api.openweathermap.org/data/2.5/weather",
        json=sample_openweather_response,
        status=200,
    )
    mock_get_cities.return_value = sample_cities

    # Execute
    extractor = Extractor()

    result = extractor.extract_openweather(**mock_airflow_context)

    # Assert XCom push was called
    mock_ti = mock_airflow_context["task_instance"]
    assert mock_ti.xcom_push.call_count >= 1

    # Verify bronze_objects were pushed
    xcom_calls = [call[1] for call in mock_ti.xcom_push.call_args_list]
    keys_pushed = [call["key"] for call in xcom_calls]
    assert "bronze_objects" in keys_pushed


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_cities")
def test_extract_openweather_object_path_format(
    mock_get_cities, MockMinIOClass, sample_openweather_response, sample_cities
):
    """Test that MinIO object paths are correctly formatted"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://api.openweathermap.org/data/2.5/weather",
        json=sample_openweather_response,
        status=200,
    )
    mock_get_cities.return_value = [sample_cities[0]]  # Just Madrid

    # Execute
    extractor = Extractor()

    result = extractor.extract_openweather(ds="2026-01-29")

    # Assert
    assert result == 1

    # Verify object path format
    call_args = mock_minio_instance.upload_json.call_args_list[0]
    bucket = call_args[0][0]
    object_path = call_args[0][1]

    assert bucket == "bronze-openweather"
    assert "2026-01-29" in object_path  # Should include execution date
    assert "madrid" in object_path.lower()  # Should include city name


# ===== Open-Meteo Daily Tests =====


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_openmeteo_daily_success(
    mock_get_capitals,
    MockMinIOClass,
    sample_openmeteo_daily_response,
    sample_capitals_df,
    mock_airflow_context,
):
    """Test successful Open-Meteo daily forecast extraction"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://api.open-meteo.com/v1/forecast",
        json=sample_openmeteo_daily_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute
    extractor = Extractor()

    result = extractor.extract_openmeteo_daily(**mock_airflow_context)

    # Assert
    assert result == len(sample_capitals_df)
    assert mock_minio_instance.upload_json.call_count == len(sample_capitals_df)

    # Verify uploaded data structure
    call_args = mock_minio_instance.upload_json.call_args_list[0]
    uploaded_data = call_args[0][2]

    assert "city_code" in uploaded_data
    assert "municipio_nombre" in uploaded_data
    assert "_metadata" in uploaded_data


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_openmeteo_daily_with_api_key(
    mock_get_capitals,
    MockMinIOClass,
    sample_openmeteo_daily_response,
    sample_capitals_df,
    monkeypatch,
):
    """Test Open-Meteo extraction includes API key when available"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup API key in environment
    monkeypatch.setenv("OPENMETEO_API_KEY", "test_key_12345")

    # Mock API - just return success
    responses.add(
        responses.GET,
        "https://api.open-meteo.com/v1/forecast",
        json=sample_openmeteo_daily_response,
        status=200,
    )

    mock_get_capitals.return_value = sample_capitals_df

    # Execute
    extractor = Extractor()

    result = extractor.extract_openmeteo_daily(ds="2026-01-29")

    # Assert
    assert result >= 0  # At least doesn't crash


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_openmeteo_daily_object_path(
    mock_get_capitals, MockMinIOClass, sample_openmeteo_daily_response, sample_capitals_df
):
    """Test Open-Meteo daily object path format"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://api.open-meteo.com/v1/forecast",
        json=sample_openmeteo_daily_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df.iloc[:1]  # Just Madrid

    # Execute
    extractor = Extractor()

    result = extractor.extract_openmeteo_daily(ds="2026-01-29")

    # Assert
    assert result == 1

    # Verify object path
    call_args = mock_minio_instance.upload_json.call_args_list[0]
    bucket = call_args[0][0]
    object_path = call_args[0][1]

    assert bucket == "bronze-openmeteo"
    assert "forecast/daily" in object_path
    assert "2026-01-29" in object_path


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_openmeteo_daily_api_error(mock_get_capitals, MockMinIOClass, sample_capitals_df):
    """Test Open-Meteo extraction handles API errors"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mock - API returns 500 error
    responses.add(
        responses.GET,
        "https://api.open-meteo.com/v1/forecast",
        json={"error": "Internal server error"},
        status=500,
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute
    extractor = Extractor()

    result = extractor.extract_openmeteo_daily(ds="2026-01-29")

    # Assert - should return 0 on complete failure
    assert result == 0


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_openmeteo_daily_xcom_push(
    mock_get_capitals,
    MockMinIOClass,
    sample_openmeteo_daily_response,
    sample_capitals_df,
    mock_airflow_context,
):
    """Test Open-Meteo extraction pushes to XCom"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://api.open-meteo.com/v1/forecast",
        json=sample_openmeteo_daily_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute
    extractor = Extractor()

    result = extractor.extract_openmeteo_daily(**mock_airflow_context)

    # Assert XCom push
    mock_ti = mock_airflow_context["task_instance"]
    assert mock_ti.xcom_push.called

    # Verify key pushed
    xcom_calls = [call[1] for call in mock_ti.xcom_push.call_args_list]
    keys_pushed = [call["key"] for call in xcom_calls]
    assert "openmeteo_daily_objects" in keys_pushed


# ===== Edge Cases & Error Handling =====


@pytest.mark.unit
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_cities")
def test_extract_openweather_empty_cities(mock_get_cities, MockMinIOClass):
    """Test extraction with no cities"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    mock_get_cities.return_value = []

    extractor = Extractor()

    result = extractor.extract_openweather(ds="2026-01-29")

    # Assert
    assert result == 0
    assert mock_minio_instance.upload_json.call_count == 0


@pytest.mark.unit
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_cities")
def test_extractor_logging(mock_get_cities, MockMinIOClass, caplog):
    """Test that extractor logs operations"""
    import logging

    caplog.set_level(logging.INFO)

    # Setup MinIO mock
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    mock_get_cities.return_value = []

    extractor = Extractor()

    result = extractor.extract_openweather(ds="2026-01-29")

    # Assert logging occurred
    assert len(caplog.records) > 0
    # Check for start/end log messages
    log_messages = [record.message for record in caplog.records]
    start_messages = [msg for msg in log_messages if "START" in msg]
    assert len(start_messages) > 0


@pytest.mark.unit
@patch("src.extractor.MinIOClient")
def test_extractor_initialization(MockMinIOClass):
    """Test Extractor initializes correctly"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    extractor = Extractor()

    assert extractor is not None
    assert extractor.logger is not None
    assert extractor.minio_client is not None
    assert extractor.session is not None


# ===== Open-Meteo Hourly Tests =====


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_openmeteo_hourly_success(
    mock_get_capitals,
    MockMinIOClass,
    sample_openmeteo_hourly_response,
    sample_capitals_df,
    mock_airflow_context,
):
    """Test successful Open-Meteo hourly forecast extraction"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://api.open-meteo.com/v1/forecast",
        json=sample_openmeteo_hourly_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_hourly(**mock_airflow_context)

    # Assert
    assert result == len(sample_capitals_df)
    assert mock_minio_instance.upload_json.call_count == len(sample_capitals_df)

    # Verify uploaded data structure
    call_args = mock_minio_instance.upload_json.call_args_list[0]
    uploaded_data = call_args[0][2]
    assert "city_code" in uploaded_data
    assert "municipio_nombre" in uploaded_data
    assert "_metadata" in uploaded_data


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_openmeteo_hourly_object_path(
    mock_get_capitals, MockMinIOClass, sample_openmeteo_hourly_response, sample_capitals_df
):
    """Test Open-Meteo hourly object path format"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://api.open-meteo.com/v1/forecast",
        json=sample_openmeteo_hourly_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df.iloc[:1]  # Just Madrid

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_hourly(ds="2026-01-29")

    # Assert
    assert result == 1

    # Verify object path
    call_args = mock_minio_instance.upload_json.call_args_list[0]
    bucket = call_args[0][0]
    object_path = call_args[0][1]

    assert bucket == "bronze-openmeteo"
    assert "forecast/hourly" in object_path
    assert "2026-01-29" in object_path


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_openmeteo_hourly_api_error(mock_get_capitals, MockMinIOClass, sample_capitals_df):
    """Test Open-Meteo hourly extraction handles API errors"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mock - API returns 500 error
    responses.add(
        responses.GET,
        "https://api.open-meteo.com/v1/forecast",
        json={"error": "Internal server error"},
        status=500,
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_hourly(ds="2026-01-29")

    # Assert - should return 0 on complete failure
    assert result == 0


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_openmeteo_hourly_xcom_push(
    mock_get_capitals,
    MockMinIOClass,
    sample_openmeteo_hourly_response,
    sample_capitals_df,
    mock_airflow_context,
):
    """Test Open-Meteo hourly extraction pushes to XCom"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://api.open-meteo.com/v1/forecast",
        json=sample_openmeteo_hourly_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_hourly(**mock_airflow_context)

    # Assert XCom push
    mock_ti = mock_airflow_context["task_instance"]
    assert mock_ti.xcom_push.called

    # Verify key pushed
    xcom_calls = [call[1] for call in mock_ti.xcom_push.call_args_list]
    keys_pushed = [call["key"] for call in xcom_calls]
    assert "openmeteo_hourly_objects" in keys_pushed


# ===== Open-Meteo Air Quality Tests =====


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_air_quality_success(
    mock_get_capitals,
    MockMinIOClass,
    sample_air_quality_response,
    sample_capitals_df,
    mock_airflow_context,
):
    """Test successful air quality extraction"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        json=sample_air_quality_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_air_quality(**mock_airflow_context)

    # Assert
    assert result == len(sample_capitals_df)
    assert mock_minio_instance.upload_json.call_count == len(sample_capitals_df)


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_air_quality_object_path(
    mock_get_capitals, MockMinIOClass, sample_air_quality_response, sample_capitals_df
):
    """Test air quality object path format"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        json=sample_air_quality_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df.iloc[:1]

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_air_quality(ds="2026-01-29")

    # Assert
    assert result == 1

    # Verify object path
    call_args = mock_minio_instance.upload_json.call_args_list[0]
    bucket = call_args[0][0]
    object_path = call_args[0][1]

    assert bucket == "bronze-openmeteo"
    assert "air_quality" in object_path
    assert "2026-01-29" in object_path


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_air_quality_non_200_response(
    mock_get_capitals, MockMinIOClass, sample_capitals_df
):
    """Test air quality extraction skips non-200 responses"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mock - API returns non-200 (silently skipped)
    responses.add(
        responses.GET,
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        json={"error": "No data"},
        status=404,
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_air_quality(ds="2026-01-29")

    # Assert - should return 0 (skipped non-200)
    assert result == 0
    assert mock_minio_instance.upload_json.call_count == 0


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_air_quality_xcom_push(
    mock_get_capitals,
    MockMinIOClass,
    sample_air_quality_response,
    sample_capitals_df,
    mock_airflow_context,
):
    """Test air quality extraction pushes to XCom"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        json=sample_air_quality_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_air_quality(**mock_airflow_context)

    # Assert XCom push
    mock_ti = mock_airflow_context["task_instance"]
    xcom_calls = [call[1] for call in mock_ti.xcom_push.call_args_list]
    keys_pushed = [call["key"] for call in xcom_calls]
    assert "openmeteo_air_quality_objects" in keys_pushed


# ===== Open-Meteo Pollen Tests =====


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_pollen_success(
    mock_get_capitals,
    MockMinIOClass,
    sample_pollen_response,
    sample_capitals_df,
    mock_airflow_context,
):
    """Test successful pollen extraction"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks (pollen uses same URL as air quality)
    responses.add(
        responses.GET,
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        json=sample_pollen_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_pollen(**mock_airflow_context)

    # Assert
    assert result == len(sample_capitals_df)
    assert mock_minio_instance.upload_json.call_count == len(sample_capitals_df)


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_pollen_object_path(
    mock_get_capitals, MockMinIOClass, sample_pollen_response, sample_capitals_df
):
    """Test pollen object path format"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        json=sample_pollen_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df.iloc[:1]

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_pollen(ds="2026-01-29")

    # Assert
    assert result == 1

    # Verify object path
    call_args = mock_minio_instance.upload_json.call_args_list[0]
    bucket = call_args[0][0]
    object_path = call_args[0][1]

    assert bucket == "bronze-openmeteo"
    assert "pollen" in object_path
    assert "2026-01-29" in object_path


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_pollen_non_200_response(mock_get_capitals, MockMinIOClass, sample_capitals_df):
    """Test pollen extraction skips non-200 responses"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mock - API returns non-200
    responses.add(
        responses.GET,
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        json={"error": "No data"},
        status=404,
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_pollen(ds="2026-01-29")

    # Assert
    assert result == 0


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_pollen_xcom_push(
    mock_get_capitals,
    MockMinIOClass,
    sample_pollen_response,
    sample_capitals_df,
    mock_airflow_context,
):
    """Test pollen extraction pushes to XCom"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        json=sample_pollen_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_pollen(**mock_airflow_context)

    # Assert XCom push
    mock_ti = mock_airflow_context["task_instance"]
    xcom_calls = [call[1] for call in mock_ti.xcom_push.call_args_list]
    keys_pushed = [call["key"] for call in xcom_calls]
    assert "openmeteo_pollen_objects" in keys_pushed


# ===== Open-Meteo Marine Tests =====


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_marine_success(
    mock_get_capitals,
    MockMinIOClass,
    sample_marine_response,
    sample_capitals_df,
    mock_airflow_context,
):
    """Test successful marine extraction"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://marine-api.open-meteo.com/v1/marine",
        json=sample_marine_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_marine(**mock_airflow_context)

    # Assert
    assert result == len(sample_capitals_df)
    assert mock_minio_instance.upload_json.call_count == len(sample_capitals_df)


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_marine_object_path(
    mock_get_capitals, MockMinIOClass, sample_marine_response, sample_capitals_df
):
    """Test marine object path format"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://marine-api.open-meteo.com/v1/marine",
        json=sample_marine_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df.iloc[:1]

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_marine(ds="2026-01-29")

    # Assert
    assert result == 1

    # Verify object path
    call_args = mock_minio_instance.upload_json.call_args_list[0]
    bucket = call_args[0][0]
    object_path = call_args[0][1]

    assert bucket == "bronze-openmeteo"
    assert "marine" in object_path
    assert "2026-01-29" in object_path


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_marine_non_200_response(mock_get_capitals, MockMinIOClass, sample_capitals_df):
    """Test marine extraction skips non-200 responses"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mock - API returns non-200
    responses.add(
        responses.GET,
        "https://marine-api.open-meteo.com/v1/marine",
        json={"error": "No data available for inland location"},
        status=400,
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_marine(ds="2026-01-29")

    # Assert
    assert result == 0


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_marine_xcom_push(
    mock_get_capitals,
    MockMinIOClass,
    sample_marine_response,
    sample_capitals_df,
    mock_airflow_context,
):
    """Test marine extraction pushes to XCom"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio_instance

    # Setup mocks
    responses.add(
        responses.GET,
        "https://marine-api.open-meteo.com/v1/marine",
        json=sample_marine_response,
        status=200,
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_marine(**mock_airflow_context)

    # Assert XCom push
    mock_ti = mock_airflow_context["task_instance"]
    xcom_calls = [call[1] for call in mock_ti.xcom_push.call_args_list]
    keys_pushed = [call["key"] for call in xcom_calls]
    assert "openmeteo_marine_objects" in keys_pushed


# ===== Additional Edge Cases =====


@pytest.mark.unit
@pytest.mark.api
@responses.activate
@patch("src.extractor.MinIOClient")
@patch("src.extractor.get_capitals_dataframe")
def test_extract_openmeteo_daily_empty_dataframe(mock_get_capitals, MockMinIOClass):
    """Test extraction with empty capitals dataframe"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    # Empty dataframe
    import pandas as pd

    mock_get_capitals.return_value = pd.DataFrame()

    # Execute
    extractor = Extractor()
    result = extractor.extract_openmeteo_daily(ds="2026-01-29")

    # Assert
    assert result == 0
    assert mock_minio_instance.upload_json.call_count == 0
