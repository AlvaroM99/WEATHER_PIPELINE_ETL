"""
Integration tests for the ETL Pipeline
Tests end-to-end data flow from extraction to loading
"""
import pytest
import pandas as pd
import responses
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime


# ===== ETL Pipeline Flow Tests =====

@pytest.mark.integration
@responses.activate
@patch('src.extractor.MinIOClient')
@patch('src.extractor.get_cities')
@patch('src.transformer.MinIOClient')
@patch('src.loader.MinIOClient')
@patch('src.loader.psycopg2.connect')
@patch('src.loader.execute_values')
def test_openweather_etl_pipeline(
    mock_execute_values,
    mock_connect,
    MockLoaderMinIO,
    MockTransformerMinIO,
    mock_get_cities,
    MockExtractorMinIO,
    sample_openweather_response,
    sample_cities,
    mock_db_connection
):
    """Test complete OpenWeather ETL pipeline: Extract -> Transform -> Load"""
    # Setup Extractor
    mock_extractor_minio = Mock()
    mock_extractor_minio.upload_json.return_value = 1024
    MockExtractorMinIO.return_value = mock_extractor_minio

    responses.add(
        responses.GET,
        "https://api.openweathermap.org/data/2.5/weather",
        json=sample_openweather_response,
        status=200
    )
    mock_get_cities.return_value = sample_cities

    # Execute Extraction
    from src.extractor import Extractor
    extractor = Extractor()

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = None
    mock_ti.xcom_push.return_value = None

    extract_result = extractor.extract_openweather(
        ds='2026-01-29',
        task_instance=mock_ti
    )

    assert extract_result == len(sample_cities)

    # Setup Transformer
    mock_transformer_minio = Mock()

    sample_openweather_response['_metadata'] = {
        'city_name': 'Madrid',
        'extraction_timestamp': '20260129_120000',
        'execution_date': '2026-01-29'
    }

    bronze_objects = [{'object_path': 'current/2026-01-29/madrid.json'}]
    mock_transformer_minio.read_json.return_value = sample_openweather_response
    mock_transformer_minio.upload_parquet.return_value = 2048
    MockTransformerMinIO.return_value = mock_transformer_minio

    # Execute Transformation
    from src.transformer import Transformer
    transformer = Transformer()

    mock_ti.xcom_pull.return_value = bronze_objects

    transform_result = transformer.transform_openweather(
        ds='2026-01-29',
        task_instance=mock_ti
    )

    assert transform_result > 0

    # Get the transformed DataFrame
    transform_call_args = mock_transformer_minio.upload_parquet.call_args
    transformed_df = transform_call_args[0][2]

    # Setup Loader
    mock_loader_minio = Mock()
    mock_loader_minio.read_parquet.return_value = transformed_df
    MockLoaderMinIO.return_value = mock_loader_minio

    mock_cursor = Mock()
    mock_cursor.rowcount = len(transformed_df)
    mock_cursor.fetchall.side_effect = [
        [('Madrid', 1)],
        [('28079', 1)]
    ]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    # Execute Loading
    from src.loader import Loader
    loader = Loader()

    load_result = loader.load_fact_observation(ds='2026-01-29')

    # Verify end-to-end success
    assert load_result >= 0
    assert mock_execute_values.called or mock_cursor.execute.called


@pytest.mark.integration
@responses.activate
@patch('src.extractor.MinIOClient')
@patch('src.extractor.get_capitals_dataframe')
@patch('src.transformer.MinIOClient')
def test_openmeteo_daily_extract_transform(
    MockTransformerMinIO,
    mock_get_capitals,
    MockExtractorMinIO,
    sample_openmeteo_daily_response,
    sample_capitals_df
):
    """Test Open-Meteo daily Extract -> Transform pipeline"""
    # Setup Extractor
    mock_extractor_minio = Mock()
    mock_extractor_minio.upload_json.return_value = 1024
    MockExtractorMinIO.return_value = mock_extractor_minio

    responses.add(
        responses.GET,
        "https://api.open-meteo.com/v1/forecast",
        json=sample_openmeteo_daily_response,
        status=200
    )
    mock_get_capitals.return_value = sample_capitals_df

    # Execute Extraction
    from src.extractor import Extractor
    extractor = Extractor()

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = None
    mock_ti.xcom_push.return_value = None

    extract_result = extractor.extract_openmeteo_daily(
        ds='2026-01-29',
        task_instance=mock_ti
    )

    assert extract_result == len(sample_capitals_df)

    # Capture what was pushed to XCom
    xcom_calls = mock_ti.xcom_push.call_args_list
    bronze_objects = None
    for call in xcom_calls:
        if call[1].get('key') == 'openmeteo_daily_objects':
            bronze_objects = call[1].get('value')

    assert bronze_objects is not None

    # Setup Transformer
    mock_transformer_minio = Mock()
    mock_transformer_minio.read_json.return_value = sample_openmeteo_daily_response
    mock_transformer_minio.upload_parquet.return_value = 2048
    MockTransformerMinIO.return_value = mock_transformer_minio

    # Execute Transformation
    from src.transformer import Transformer
    transformer = Transformer()

    mock_ti.xcom_pull.return_value = bronze_objects

    transform_result = transformer.transform_openmeteo_daily(
        ds='2026-01-29',
        task_instance=mock_ti
    )

    assert transform_result > 0

    # Verify transformed data structure
    call_args = mock_transformer_minio.upload_parquet.call_args
    df = call_args[0][2]

    assert 'time' in df.columns
    assert 'city_code' in df.columns
    assert 'temperature_2m_max' in df.columns


@pytest.mark.integration
@responses.activate
@patch('src.extractor.MinIOClient')
@patch('src.extractor.get_capitals_dataframe')
def test_multiple_extraction_types_parallel(
    mock_get_capitals,
    MockExtractorMinIO,
    sample_openmeteo_daily_response,
    sample_openmeteo_hourly_response,
    sample_capitals_df
):
    """Test multiple extraction types can run (simulated parallel)"""
    mock_extractor_minio = Mock()
    mock_extractor_minio.upload_json.return_value = 1024
    MockExtractorMinIO.return_value = mock_extractor_minio

    # Setup responses for multiple APIs
    responses.add(
        responses.GET,
        "https://api.open-meteo.com/v1/forecast",
        json=sample_openmeteo_daily_response,
        status=200
    )
    mock_get_capitals.return_value = sample_capitals_df

    from src.extractor import Extractor
    extractor = Extractor()

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = None
    mock_ti.xcom_push.return_value = None

    # Run multiple extractions
    daily_result = extractor.extract_openmeteo_daily(
        ds='2026-01-29',
        task_instance=mock_ti
    )

    # Clear responses and add hourly
    responses.reset()
    responses.add(
        responses.GET,
        "https://api.open-meteo.com/v1/forecast",
        json=sample_openmeteo_hourly_response,
        status=200
    )

    hourly_result = extractor.extract_openmeteo_hourly(
        ds='2026-01-29',
        task_instance=mock_ti
    )

    assert daily_result > 0
    assert hourly_result > 0


# ===== Data Consistency Tests =====

@pytest.mark.integration
@patch('src.transformer.MinIOClient')
def test_transformation_preserves_data_integrity(
    MockTransformerMinIO,
    sample_openmeteo_daily_response
):
    """Test that transformation preserves data integrity"""
    mock_minio = Mock()

    sample_openmeteo_daily_response['city_code'] = '28079'
    sample_openmeteo_daily_response['municipio_nombre'] = 'Madrid'
    sample_openmeteo_daily_response['_metadata'] = {
        'extraction_timestamp': '20260129_120000'
    }

    bronze_objects = [{'object_path': 'forecast/daily/2026-01-29/madrid.json'}]
    mock_minio.read_json.return_value = sample_openmeteo_daily_response
    mock_minio.upload_parquet.return_value = 2048
    MockTransformerMinIO.return_value = mock_minio

    from src.transformer import Transformer
    transformer = Transformer()

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = bronze_objects

    result = transformer.transform_openmeteo_daily(
        ds='2026-01-29',
        task_instance=mock_ti
    )

    call_args = mock_minio.upload_parquet.call_args
    df = call_args[0][2]

    # Verify original values preserved
    original_temps = sample_openmeteo_daily_response['daily']['temperature_2m_max']
    assert list(df['temperature_2m_max']) == original_temps

    # Verify city info preserved
    assert df['city_code'].iloc[0] == '28079'
    assert df['city_name'].iloc[0] == 'Madrid'


@pytest.mark.integration
@patch('src.loader.execute_values')
@patch('src.loader.MinIOClient')
@patch('src.loader.psycopg2.connect')
def test_loader_deduplication(
    mock_connect,
    MockMinIOClass,
    mock_execute_values,
    mock_db_connection
):
    """Test that loader handles duplicate data correctly"""
    mock_minio = Mock()

    # DataFrame with duplicate rows
    df = pd.DataFrame({
        'time': ['2026-01-29', '2026-01-29'],  # Same time
        'city_code': ['28079', '28079'],  # Same city
        'city_name': ['Madrid', 'Madrid'],
        'temperature_2m_max': [20.0, 20.0],
        'temperature_2m_min': [10.0, 10.0]
    })

    mock_minio.read_parquet.return_value = df
    MockMinIOClass.return_value = mock_minio

    mock_cursor = Mock()
    mock_cursor.rowcount = 1  # Only one after dedup
    mock_cursor.fetchall.side_effect = [
        [('Madrid', 1)],
        [('28079', 1)]
    ]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    from src.loader import Loader
    loader = Loader()

    result = loader.load_fact_forecast_daily(ds='2026-01-29')

    # Should complete without error
    assert result >= 0


# ===== Error Recovery Tests =====

@pytest.mark.integration
@responses.activate
@patch('src.extractor.MinIOClient')
@patch('src.extractor.get_cities')
def test_extraction_partial_failure_recovery(
    mock_get_cities,
    MockMinIOClass,
    sample_openweather_response
):
    """Test extraction continues after partial failures"""
    mock_minio = Mock()
    mock_minio.upload_json.return_value = 1024
    MockMinIOClass.return_value = mock_minio

    # First city succeeds, second fails, third succeeds
    responses.add(
        responses.GET,
        "https://api.openweathermap.org/data/2.5/weather",
        json=sample_openweather_response,
        status=200
    )
    responses.add(
        responses.GET,
        "https://api.openweathermap.org/data/2.5/weather",
        json={"error": "API limit"},
        status=429
    )
    responses.add(
        responses.GET,
        "https://api.openweathermap.org/data/2.5/weather",
        json=sample_openweather_response,
        status=200
    )

    mock_get_cities.return_value = [
        {'name': 'Madrid', 'lat': 40.4, 'lon': -3.7},
        {'name': 'BadCity', 'lat': 0, 'lon': 0},
        {'name': 'Barcelona', 'lat': 41.3, 'lon': 2.1}
    ]

    from src.extractor import Extractor
    extractor = Extractor()

    result = extractor.extract_openweather(ds='2026-01-29')

    # Should have extracted 2 out of 3 cities
    assert result == 2
    assert mock_minio.upload_json.call_count == 2


@pytest.mark.integration
@patch('src.transformer.MinIOClient')
def test_transformation_handles_corrupted_file(MockMinIOClass):
    """Test transformation gracefully handles corrupted JSON"""
    mock_minio = Mock()

    # First file is valid, second is corrupted
    valid_data = {
        'daily': {
            'time': ['2026-01-29'],
            'temperature_2m_max': [20.0]
        },
        'city_code': '28079',
        'municipio_nombre': 'Madrid',
        '_metadata': {}
    }

    mock_minio.read_json.side_effect = [
        valid_data,
        Exception("JSON decode error")
    ]
    mock_minio.upload_parquet.return_value = 2048
    MockMinIOClass.return_value = mock_minio

    from src.transformer import Transformer
    transformer = Transformer()

    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = [
        {'object_path': 'forecast/daily/2026-01-29/madrid.json'},
        {'object_path': 'forecast/daily/2026-01-29/corrupted.json'}
    ]

    result = transformer.transform_openmeteo_daily(
        ds='2026-01-29',
        task_instance=mock_ti
    )

    # Should process the valid file
    assert result == 1


# ===== Configuration Tests =====

@pytest.mark.integration
def test_bucket_configuration():
    """Test that bucket configuration is correctly set"""
    from src.weather_config.lake_config import (
        BRONZE_BUCKET, SILVER_BUCKET,
        BRONZE_OPENMETEO_BUCKET, SILVER_OPENMETEO_BUCKET
    )

    assert BRONZE_BUCKET is not None
    assert SILVER_BUCKET is not None
    assert BRONZE_OPENMETEO_BUCKET is not None
    assert SILVER_OPENMETEO_BUCKET is not None

    # Verify naming convention
    assert 'bronze' in BRONZE_BUCKET.lower()
    assert 'silver' in SILVER_BUCKET.lower()


@pytest.mark.integration
def test_api_configuration():
    """Test that API configuration is correctly set"""
    from src.weather_config.openmeteo_config import (
        OPENMETEO_FORECAST_URL,
        OPENMETEO_AIR_QUALITY_URL,
        OPENMETEO_MARINE_URL,
        DAILY_FORECAST_PARAMS,
        HOURLY_FORECAST_PARAMS
    )

    assert OPENMETEO_FORECAST_URL.startswith('https://')
    assert OPENMETEO_AIR_QUALITY_URL.startswith('https://')
    assert OPENMETEO_MARINE_URL.startswith('https://')

    assert len(DAILY_FORECAST_PARAMS) > 0
    assert len(HOURLY_FORECAST_PARAMS) > 0
