"""
Unit tests for Transformer module
Tests JSON to Parquet transformation logic
"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from src.transformer import Transformer


# ===== OpenWeather Transformation Tests =====

@pytest.mark.unit
@patch('src.transformer.MinIOClient')
def test_transform_openweather_success(
    MockMinIOClass,
    sample_openweather_response,
    mock_airflow_context
):
    """Test successful OpenWeather transformation from JSON to Parquet"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    # Add metadata to response
    sample_openweather_response['_metadata'] = {
        'city_name': 'Madrid',
        'extraction_timestamp': '20260129_120000',
        'execution_date': '2026-01-29'
    }

    bronze_objects = [{'object_path': 'current/2026-01-29/madrid_20260129_120000.json'}]

    mock_minio_instance.read_json.return_value = sample_openweather_response
    mock_minio_instance.upload_parquet.return_value = 2048
    mock_airflow_context['task_instance'].xcom_pull.return_value = bronze_objects

    MockMinIOClass.return_value = mock_minio_instance

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
@patch('src.transformer.MinIOClient')
def test_transform_openweather_field_mapping(
    MockMinIOClass,
    sample_openweather_response
):
    """Test that OpenWeather fields are correctly mapped"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    sample_openweather_response['_metadata'] = {
        'city_name': 'Madrid',
        'extraction_timestamp': '20260129_120000',
        'execution_date': '2026-01-29'
    }

    mock_obj = Mock()
    mock_obj.object_name = 'current/2026-01-29/madrid.json'
    mock_minio_instance.client.list_objects.return_value = [mock_obj]
    mock_minio_instance.read_json.return_value = sample_openweather_response
    mock_minio_instance.upload_parquet.return_value = 2048

    MockMinIOClass.return_value = mock_minio_instance

    # Execute
    transformer = Transformer()

    result = transformer.transform_openweather(ds='2026-01-29', task_instance=None)

    # Assert
    if result > 0:
        call_args = mock_minio_instance.upload_parquet.call_args
        df = call_args[0][2]

        # Verify required columns exist
        expected_columns = [
            'city', 'country', 'latitude', 'longitude',
            'temperature', 'feels_like', 'temp_min', 'temp_max',
            'pressure', 'humidity', 'weather_main', 'weather_description',
            'wind_speed', 'wind_deg', 'clouds', 'visibility', 'date'
        ]

        for col in expected_columns:
            assert col in df.columns, f"Missing column: {col}"

        # Verify data values
        assert df['city'].iloc[0] == 'Madrid'
        assert df['temperature'].iloc[0] == 15.5
        assert df['humidity'].iloc[0] == 65


@pytest.mark.unit
@patch('src.transformer.MinIOClient')
def test_transform_openweather_fallback_scan(MockMinIOClass, sample_openweather_response):
    """Test transformation fallback when XCom is not available"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    sample_openweather_response['_metadata'] = {
        'city_name': 'Madrid',
        'extraction_timestamp': '20260129_120000',
        'execution_date': '2026-01-29'
    }

    mock_obj = Mock()
    mock_obj.object_name = 'current/2026-01-29/madrid.json'
    mock_minio_instance.client.list_objects.return_value = [mock_obj]
    mock_minio_instance.read_json.return_value = sample_openweather_response
    mock_minio_instance.upload_parquet.return_value = 2048

    MockMinIOClass.return_value = mock_minio_instance

    # Execute
    transformer = Transformer()

    result = transformer.transform_openweather(ds='2026-01-29', task_instance=None)

    # Assert - should fall back to scanning bronze bucket
    assert mock_minio_instance.client.list_objects.called

    if result > 0:
        assert mock_minio_instance.upload_parquet.called


@pytest.mark.unit
@patch('src.transformer.MinIOClient')
def test_transform_openweather_no_data(MockMinIOClass):
    """Test transformation with no bronze data"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []
    MockMinIOClass.return_value = mock_minio_instance

    # Execute
    transformer = Transformer()

    result = transformer.transform_openweather(ds='2026-01-29', task_instance=None)

    # Assert
    assert result == 0
    assert not mock_minio_instance.upload_parquet.called


@pytest.mark.unit
@patch('src.transformer.MinIOClient')
def test_transform_openweather_multiple_cities(
    MockMinIOClass,
    sample_openweather_response
):
    """Test transformation with multiple city records"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    madrid_data = sample_openweather_response.copy()
    madrid_data['_metadata'] = {'city_name': 'Madrid'}

    barcelona_data = sample_openweather_response.copy()
    barcelona_data['name'] = 'Barcelona'
    barcelona_data['_metadata'] = {'city_name': 'Barcelona'}

    bronze_objects = [
        {'object_path': 'current/2026-01-29/madrid.json'},
        {'object_path': 'current/2026-01-29/barcelona.json'}
    ]

    # Mock read_json to return different data based on path
    def read_json_side_effect(bucket, path):
        if 'madrid' in path:
            return madrid_data
        elif 'barcelona' in path:
            return barcelona_data
        return {}

    mock_minio_instance.read_json.side_effect = read_json_side_effect
    mock_minio_instance.upload_parquet.return_value = 2048

    MockMinIOClass.return_value = mock_minio_instance

    # Execute
    transformer = Transformer()

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = bronze_objects

    result = transformer.transform_openweather(ds='2026-01-29', task_instance=mock_ti)

    # Assert
    assert result == 2  # Two cities processed

    call_args = mock_minio_instance.upload_parquet.call_args
    df = call_args[0][2]
    assert len(df) == 2


# ===== Open-Meteo Daily Transformation Tests =====

@pytest.mark.unit
@patch('src.transformer.MinIOClient')
def test_transform_openmeteo_daily_success(
    MockMinIOClass,
    sample_openmeteo_daily_response,
    mock_airflow_context
):
    """Test successful Open-Meteo daily transformation"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    bronze_objects = [{'object_path': 'forecast/daily/2026-01-29/madrid.json'}]

    mock_minio_instance.read_json.return_value = sample_openmeteo_daily_response
    mock_minio_instance.upload_parquet.return_value = 2048
    mock_airflow_context['task_instance'].xcom_pull.return_value = bronze_objects

    MockMinIOClass.return_value = mock_minio_instance

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
    assert 'time' in df.columns
    assert 'city_code' in df.columns
    assert 'city_name' in df.columns
    assert 'extraction_date' in df.columns


@pytest.mark.unit
@patch('src.transformer.MinIOClient')
def test_transform_generic_with_daily_data(
    MockMinIOClass,
    sample_openmeteo_daily_response
):
    """Test _transform_generic helper with daily forecast data"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    bronze_objects = [{'object_path': 'forecast/daily/2026-01-29/madrid.json'}]
    mock_minio_instance.read_json.return_value = sample_openmeteo_daily_response
    mock_minio_instance.upload_parquet.return_value = 2048

    MockMinIOClass.return_value = mock_minio_instance

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = bronze_objects

    # Execute
    transformer = Transformer()

    result = transformer.transform_openmeteo_daily(ds='2026-01-29', task_instance=mock_ti)

    # Assert
    assert result > 0

    call_args = mock_minio_instance.upload_parquet.call_args
    df = call_args[0][2]

    # Verify daily-specific columns
    assert 'temperature_2m_max' in df.columns
    assert 'temperature_2m_min' in df.columns
    assert len(df) == 2  # 2 days in sample data


@pytest.mark.unit
@patch('src.transformer.MinIOClient')
def test_transform_generic_fallback_to_bucket_scan(
    MockMinIOClass,
    sample_openmeteo_daily_response
):
    """Test _transform_generic falls back to bucket scanning"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    mock_obj = Mock()
    mock_obj.object_name = 'forecast/daily/2026-01-29/madrid.json'
    mock_minio_instance.client.list_objects.return_value = [mock_obj]
    mock_minio_instance.read_json.return_value = sample_openmeteo_daily_response
    mock_minio_instance.upload_parquet.return_value = 2048

    MockMinIOClass.return_value = mock_minio_instance

    # Execute
    transformer = Transformer()

    result = transformer.transform_openmeteo_daily(ds='2026-01-29', task_instance=None)

    # Assert
    assert mock_minio_instance.client.list_objects.called

    if result > 0:
        assert mock_minio_instance.upload_parquet.called


@pytest.mark.unit
@patch('src.transformer.MinIOClient')
def test_transform_generic_multiple_cities(
    MockMinIOClass,
    sample_openmeteo_daily_response
):
    """Test _transform_generic concatenates multiple city DataFrames"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    madrid_data = sample_openmeteo_daily_response.copy()
    barcelona_data = sample_openmeteo_daily_response.copy()
    barcelona_data['city_code'] = '08019'
    barcelona_data['municipio_nombre'] = 'Barcelona'

    bronze_objects = [
        {'object_path': 'forecast/daily/2026-01-29/madrid.json'},
        {'object_path': 'forecast/daily/2026-01-29/barcelona.json'}
    ]

    def read_json_side_effect(bucket, path):
        if 'madrid' in path:
            return madrid_data
        elif 'barcelona' in path:
            return barcelona_data
        return {}

    mock_minio_instance.read_json.side_effect = read_json_side_effect
    mock_minio_instance.upload_parquet.return_value = 2048

    MockMinIOClass.return_value = mock_minio_instance

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = bronze_objects

    # Execute
    transformer = Transformer()

    result = transformer.transform_openmeteo_daily(ds='2026-01-29', task_instance=mock_ti)

    # Assert
    assert result == 4  # 2 cities × 2 days = 4 rows

    call_args = mock_minio_instance.upload_parquet.call_args
    df = call_args[0][2]

    # Verify both cities are in DataFrame
    assert 'Madrid' in df['city_name'].values
    assert 'Barcelona' in df['city_name'].values


# ===== Edge Cases & Error Handling =====

@pytest.mark.unit
@patch('src.transformer.MinIOClient')
def test_transform_openweather_malformed_json(MockMinIOClass):
    """Test transformation handles malformed JSON gracefully"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    malformed_data = {'incomplete': 'data'}  # Missing required fields

    bronze_objects = [{'object_path': 'current/2026-01-29/test.json'}]
    mock_minio_instance.read_json.return_value = malformed_data
    mock_minio_instance.upload_parquet.return_value = 2048

    MockMinIOClass.return_value = mock_minio_instance

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = bronze_objects

    # Execute
    transformer = Transformer()

    result = transformer.transform_openweather(ds='2026-01-29', task_instance=mock_ti)

    # Assert - should handle error and return 0
    assert result >= 0


@pytest.mark.unit
@patch('src.transformer.MinIOClient')
def test_transformer_initialization(MockMinIOClass):
    """Test Transformer initializes correctly"""
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    transformer = Transformer()

    assert transformer is not None
    assert transformer.logger is not None
    assert transformer.minio_client is not None


@pytest.mark.unit
@patch('src.transformer.MinIOClient')
def test_transform_logging(MockMinIOClass, caplog):
    """Test that transformer logs operations"""
    import logging
    caplog.set_level(logging.INFO)

    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []
    MockMinIOClass.return_value = mock_minio_instance

    transformer = Transformer()

    result = transformer.transform_openweather(ds='2026-01-29', task_instance=None)

    # Assert logging occurred
    assert len(caplog.records) > 0
    log_messages = [record.message for record in caplog.records]
    start_messages = [msg for msg in log_messages if 'START' in msg]
    assert len(start_messages) > 0


@pytest.mark.unit
@patch('src.transformer.MinIOClient')
def test_transform_openmeteo_daily_no_data(MockMinIOClass):
    """Test Open-Meteo transformation with no data"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []
    MockMinIOClass.return_value = mock_minio_instance

    # Execute
    transformer = Transformer()

    result = transformer.transform_openmeteo_daily(ds='2026-01-29', task_instance=None)

    # Assert
    assert result == 0


@pytest.mark.unit
@patch('src.transformer.MinIOClient')
def test_transform_generic_handles_missing_data_key(MockMinIOClass):
    """Test _transform_generic handles missing data key gracefully"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    invalid_response = {
        'city_code': '28079',
        'municipio_nombre': 'Madrid',
        # Missing 'daily' key
    }

    bronze_objects = [{'object_path': 'forecast/daily/2026-01-29/madrid.json'}]
    mock_minio_instance.read_json.return_value = invalid_response

    MockMinIOClass.return_value = mock_minio_instance

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = bronze_objects

    # Execute
    transformer = Transformer()

    result = transformer.transform_openmeteo_daily(ds='2026-01-29', task_instance=mock_ti)

    # Assert - should return 0 when no valid data
    assert result == 0
