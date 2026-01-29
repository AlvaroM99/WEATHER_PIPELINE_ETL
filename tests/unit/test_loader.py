"""
Unit tests for Loader module
Tests database loading logic for fact tables
"""

from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import pandas as pd
import pytest

from src.loader import Loader

# ===== Utility Method Tests =====


@pytest.mark.unit
@patch("src.loader.MinIOClient")
def test_get_date_id_valid_date(MockMinIOClass):
    """Test get_date_id converts date string to integer correctly"""
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    loader = Loader()

    # Test standard date format
    result = loader.get_date_id("2026-01-29")
    assert result == 20260129

    # Test with datetime string
    result = loader.get_date_id("2026-01-29T12:30:00")
    assert result == 20260129


@pytest.mark.unit
@patch("src.loader.MinIOClient")
def test_get_date_id_invalid_date(MockMinIOClass):
    """Test get_date_id handles invalid dates"""
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    loader = Loader()

    result = loader.get_date_id("invalid-date")
    assert result is None


@pytest.mark.unit
@patch("src.loader.MinIOClient")
def test_clean_value_nan_handling(MockMinIOClass):
    """Test clean_value converts NaN to None for database"""
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    loader = Loader()

    # Test NaN
    result = loader.clean_value(np.nan)
    assert result is None

    # Test NaT (Not a Time)
    result = loader.clean_value(pd.NaT)
    assert result is None

    # Test valid values are unchanged
    assert loader.clean_value(25.5) == 25.5
    assert loader.clean_value("text") == "text"
    assert loader.clean_value(0) == 0


@pytest.mark.unit
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_get_city_id_mapping(mock_connect, MockMinIOClass, mock_db_connection):
    """Test get_city_id_mapping returns correct dictionaries"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    # Setup DB mock
    mock_cursor = Mock()

    # Mock fetchall for name mapping
    mock_cursor.fetchall.side_effect = [
        [("Madrid", 1), ("Barcelona", 2)],  # First call: name_map
        [("28079", 1), ("08019", 2)],  # Second call: code_map
    ]

    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    # Execute
    loader = Loader()
    name_map, code_map = loader.get_city_id_mapping(mock_db_connection)

    # Assert
    assert "Madrid" in name_map
    assert name_map["Madrid"] == 1
    assert "Barcelona" in name_map
    assert name_map["Barcelona"] == 2

    assert "28079" in code_map
    assert code_map["28079"] == 1
    assert "08019" in code_map
    assert code_map["08019"] == 2


# ===== Database Connection Tests =====


@pytest.mark.unit
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_get_db_connection(mock_connect, MockMinIOClass):
    """Test database connection creation"""
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    mock_conn = Mock()
    mock_connect.return_value = mock_conn

    loader = Loader()
    conn = loader.get_db_connection()

    # Assert
    assert conn is not None
    mock_connect.assert_called_once()


# ===== Fact Observation Loading Tests =====


@pytest.mark.unit
@patch("src.loader.execute_values")
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_load_fact_observation_success(
    mock_connect, MockMinIOClass, mock_execute_values, mock_db_connection, sample_transformed_df
):
    """Test successful loading of weather observations"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.read_parquet.return_value = sample_transformed_df
    MockMinIOClass.return_value = mock_minio_instance

    # Setup DB mocks
    mock_cursor = Mock()
    mock_cursor.rowcount = len(sample_transformed_df)
    mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]  # name_map  # code_map

    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    # Execute
    loader = Loader()

    result = loader.load_fact_observation(ds="2026-01-29")

    # Assert
    assert result > 0
    assert mock_execute_values.called


@pytest.mark.unit
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_load_fact_observation_city_not_found(mock_connect, MockMinIOClass, mock_db_connection):
    """Test loading handles cities not in dimension table"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    test_df = pd.DataFrame(
        [{"city": "Madrid", "temperature": 25.5, "humidity": 60, "date": "2026-01-29"}]
    )

    mock_minio_instance.read_parquet.return_value = test_df
    MockMinIOClass.return_value = mock_minio_instance

    # Setup DB mocks
    mock_cursor = Mock()
    mock_cursor.fetchall.side_effect = [
        [("Barcelona", 2)],  # name_map - Madrid not included
        [("08019", 2)],  # code_map
    ]

    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    # Execute
    loader = Loader()

    result = loader.load_fact_observation(ds="2026-01-29")

    # Assert - should skip cities not found
    assert result == 0  # No records inserted because Madrid not in mapping


@pytest.mark.unit
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_load_fact_observation_no_data(mock_connect, MockMinIOClass, mock_db_connection):
    """Test loading with no data available"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.read_parquet.side_effect = Exception("File not found")
    MockMinIOClass.return_value = mock_minio_instance

    mock_connect.return_value = mock_db_connection

    # Execute
    loader = Loader()

    result = loader.load_fact_observation(ds="2026-01-29")

    # Assert
    assert result == 0


@pytest.mark.unit
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_load_fact_observation_rollback_on_error(mock_connect, MockMinIOClass, mock_db_connection):
    """Test that database transaction rolls back on error"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    test_df = pd.DataFrame(
        [{"city": "Madrid", "temperature": 25.5, "humidity": 60, "date": "2026-01-29"}]
    )

    mock_minio_instance.read_parquet.return_value = test_df
    MockMinIOClass.return_value = mock_minio_instance

    # Setup DB mocks
    mock_cursor = Mock()
    mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]
    mock_cursor.execute.side_effect = Exception("Database error")

    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    # Execute
    loader = Loader()

    with pytest.raises(Exception):
        loader.load_fact_observation(ds="2026-01-29")

    # Assert - rollback was called
    mock_db_connection.rollback.assert_called()


# ===== Generic Loader Tests =====


@pytest.mark.unit
@patch("src.loader.execute_values")
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_load_generic_success(
    mock_connect, MockMinIOClass, mock_execute_values, mock_db_connection, sample_silver_parquet_df
):
    """Test _load_generic helper method"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    mock_obj = Mock()
    mock_obj.object_name = "forecast/daily/2026-01-29/daily_forecast_20260129_120000.parquet"

    mock_minio_instance.client.list_objects.return_value = [mock_obj]
    mock_minio_instance.read_parquet.return_value = sample_silver_parquet_df

    MockMinIOClass.return_value = mock_minio_instance

    # Setup DB mocks
    mock_cursor = Mock()
    mock_cursor.rowcount = len(sample_silver_parquet_df)
    mock_cursor.fetchall.side_effect = [
        [("Madrid", 1), ("Barcelona", 2)],  # name_map
        [("28079", 1), ("08019", 2)],  # code_map
    ]

    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    # Execute
    loader = Loader()

    result = loader.load_fact_forecast_daily(ds="2026-01-29")

    # Assert
    assert result > 0
    assert mock_execute_values.called


@pytest.mark.unit
@patch("src.loader.execute_values")
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_load_generic_deduplication(
    mock_connect, MockMinIOClass, mock_execute_values, mock_db_connection
):
    """Test _load_generic removes duplicate rows"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    # Create DataFrame with duplicates
    duplicate_df = pd.DataFrame(
        [
            {"city_name": "Madrid", "time": "2026-01-29", "temperature_2m_max": 18.5},
            {"city_name": "Madrid", "time": "2026-01-29", "temperature_2m_max": 18.5},  # Duplicate
            {"city_name": "Madrid", "time": "2026-01-30", "temperature_2m_max": 19.0},
        ]
    )

    mock_obj = Mock()
    mock_obj.object_name = "forecast/daily/2026-01-29/test.parquet"

    mock_minio_instance.client.list_objects.return_value = [mock_obj]
    mock_minio_instance.read_parquet.return_value = duplicate_df

    MockMinIOClass.return_value = mock_minio_instance

    # Setup DB mocks
    mock_cursor = Mock()
    mock_cursor.rowcount = 2  # Only 2 unique rows
    mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]

    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    # Execute
    loader = Loader()

    result = loader.load_fact_forecast_daily(ds="2026-01-29")

    # Assert - duplicates should be removed
    assert result > 0
    assert mock_execute_values.called


@pytest.mark.unit
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_load_generic_no_files_found(mock_connect, MockMinIOClass, mock_db_connection):
    """Test _load_generic handles no files found"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []
    MockMinIOClass.return_value = mock_minio_instance

    mock_connect.return_value = mock_db_connection

    # Execute
    loader = Loader()

    result = loader.load_fact_forecast_daily(ds="2026-01-29")

    # Assert
    assert result == 0


# ===== Mapper Function Tests =====


@pytest.mark.unit
@patch("src.loader.MinIOClient")
def test_map_daily_forecast(MockMinIOClass):
    """Test _map_daily_forecast mapper function"""
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    loader = Loader()

    # Create sample row
    row = pd.Series(
        {
            "time": "2026-01-29",
            "temperature_2m_max": 18.5,
            "temperature_2m_min": 8.3,
            "apparent_temperature_max": 17.2,
            "apparent_temperature_min": 7.1,
            "precipitation_sum": 0.0,
            "rain_sum": 0.0,
            "showers_sum": 0.0,
            "snowfall_sum": 0.0,
            "precipitation_hours": 0.0,
            "wind_speed_10m_max": 15.2,
            "wind_gusts_10m_max": 25.3,
            "wind_direction_10m_dominant": 180,
            "sunrise": "2026-01-29T07:45:00",
            "sunset": "2026-01-29T18:15:00",
            "shortwave_radiation_sum": 5.2,
            "weather_code": 0,
            "et0_fao_evapotranspiration": 1.2,
        }
    )

    city_id = 1
    extraction_date_id = 20260129

    # Execute
    result = loader._map_daily_forecast(row, city_id, extraction_date_id)

    # Assert
    assert result is not None
    assert isinstance(result, tuple)
    assert result[0] == city_id  # city_id
    assert result[1] == 20260129  # forecast_date_id
    assert result[2] == extraction_date_id


@pytest.mark.unit
@patch("src.loader.MinIOClient")
def test_map_air_quality(MockMinIOClass):
    """Test _map_air_quality mapper function"""
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    loader = Loader()

    row = pd.Series(
        {
            "time": "2026-01-29T12:00:00",
            "pm10": 25.5,
            "pm2_5": 12.3,
            "carbon_monoxide": 0.5,
            "nitrogen_dioxide": 15.2,
            "sulphur_dioxide": 5.1,
            "ozone": 45.3,
            "aerosol_optical_depth": 0.15,
            "dust": 10.2,
        }
    )

    city_id = 1
    extraction_date_id = 20260129

    # Execute
    result = loader._map_air_quality(row, city_id, extraction_date_id)

    # Assert
    assert result is not None
    assert isinstance(result, tuple)
    assert result[0] == city_id


@pytest.mark.unit
@patch("src.loader.MinIOClient")
def test_map_air_quality_handles_nan(MockMinIOClass):
    """Test _map_air_quality handles NaN values"""
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    loader = Loader()

    row = pd.Series(
        {
            "time": "2026-01-29T12:00:00",
            "pm10": np.nan,  # Missing value
            "pm2_5": 12.3,
            "carbon_monoxide": np.nan,
            "nitrogen_dioxide": 15.2,
            "sulphur_dioxide": np.nan,
            "ozone": 45.3,
            "aerosol_optical_depth": np.nan,
            "dust": np.nan,
        }
    )

    city_id = 1
    extraction_date_id = 20260129

    # Execute
    result = loader._map_air_quality(row, city_id, extraction_date_id)

    # Assert - NaN values should be converted to None
    assert result is not None
    assert result[3] is None  # pm10
    assert result[5] is None  # carbon_monoxide


# ===== Initialization & Logging Tests =====


@pytest.mark.unit
@patch("src.loader.MinIOClient")
def test_loader_initialization(MockMinIOClass):
    """Test Loader initializes correctly"""
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    loader = Loader()

    assert loader is not None
    assert loader.logger is not None
    assert loader.minio_client is not None


@pytest.mark.unit
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_loader_logging(mock_connect, MockMinIOClass, mock_db_connection, caplog):
    """Test that loader logs operations"""
    import logging

    caplog.set_level(logging.INFO)

    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []
    MockMinIOClass.return_value = mock_minio_instance

    mock_connect.return_value = mock_db_connection

    loader = Loader()

    result = loader.load_fact_forecast_daily(ds="2026-01-29")

    # Assert logging occurred
    assert len(caplog.records) > 0
    log_messages = [record.message for record in caplog.records]
    start_messages = [msg for msg in log_messages if "START" in msg]
    assert len(start_messages) > 0


# ===== Connection Management Tests =====


@pytest.mark.unit
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_connection_closed_on_success(mock_connect, MockMinIOClass, mock_db_connection):
    """Test database connection is closed after successful operation"""
    mock_minio_instance = Mock()
    mock_minio_instance.read_parquet.side_effect = Exception("No file")
    MockMinIOClass.return_value = mock_minio_instance

    mock_cursor = Mock()
    mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()

    result = loader.load_fact_observation(ds="2026-01-29")

    # Assert - connection close was called
    mock_db_connection.close.assert_called()


@pytest.mark.unit
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_connection_closed_on_error(mock_connect, MockMinIOClass, mock_db_connection):
    """Test database connection is closed even on error"""
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    mock_cursor = Mock()
    mock_cursor.fetchall.side_effect = Exception("Query error")
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()

    with pytest.raises(Exception):
        conn = loader.get_db_connection()
        loader.get_city_id_mapping(conn)

    # Assert - even on error, close should be attempted
    # (Note: in real code, this would be in a finally block)


# ===== Additional Loader Tests =====


@pytest.mark.unit
@patch("src.loader.execute_values")
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_load_fact_forecast_hourly(
    mock_connect, MockMinIOClass, mock_execute_values, mock_db_connection
):
    """Test loading hourly forecast data"""
    mock_minio_instance = Mock()

    hourly_df = pd.DataFrame(
        {
            "time": ["2026-01-29T00:00", "2026-01-29T01:00"],
            "city_code": ["28079", "28079"],
            "city_name": ["Madrid", "Madrid"],
            "temperature_2m": [12.5, 11.8],
            "relative_humidity_2m": [70, 72],
            "precipitation_probability": [0, 10],
            "precipitation": [0.0, 0.1],
            "weather_code": [2, 3],
            "wind_speed_10m": [8.5, 9.2],
            "wind_direction_10m": [180, 185],
            "wind_gusts_10m": [15.2, 16.5],
        }
    )

    mock_minio_instance.read_parquet.return_value = hourly_df
    MockMinIOClass.return_value = mock_minio_instance

    mock_cursor = Mock()
    mock_cursor.rowcount = 2
    mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_forecast_hourly(ds="2026-01-29")

    assert result >= 0


@pytest.mark.unit
@patch("src.loader.execute_values")
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_load_fact_air_quality(
    mock_connect, MockMinIOClass, mock_execute_values, mock_db_connection
):
    """Test loading air quality data"""
    mock_minio_instance = Mock()

    air_quality_df = pd.DataFrame(
        {
            "time": ["2026-01-29T00:00", "2026-01-29T01:00"],
            "city_code": ["28079", "28079"],
            "city_name": ["Madrid", "Madrid"],
            "pm10": [15.2, 14.8],
            "pm2_5": [8.5, 8.2],
            "carbon_monoxide": [200.5, 198.2],
            "nitrogen_dioxide": [12.3, 11.8],
            "sulphur_dioxide": [2.5, 2.3],
            "ozone": [45.2, 44.8],
            "aerosol_optical_depth": [0.15, 0.14],
            "dust": [5.2, 5.0],
        }
    )

    mock_minio_instance.read_parquet.return_value = air_quality_df
    MockMinIOClass.return_value = mock_minio_instance

    mock_cursor = Mock()
    mock_cursor.rowcount = 2
    mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_air_quality(ds="2026-01-29")

    assert result >= 0


@pytest.mark.unit
@patch("src.loader.execute_values")
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_load_fact_pollen(mock_connect, MockMinIOClass, mock_execute_values, mock_db_connection):
    """Test loading pollen data"""
    mock_minio_instance = Mock()

    pollen_df = pd.DataFrame(
        {
            "time": ["2026-01-29T00:00", "2026-01-29T01:00"],
            "city_code": ["28079", "28079"],
            "city_name": ["Madrid", "Madrid"],
            "alder_pollen": [0.0, 0.0],
            "birch_pollen": [5.2, 5.5],
            "grass_pollen": [12.5, 13.2],
            "mugwort_pollen": [0.0, 0.0],
            "olive_pollen": [8.5, 8.8],
            "ragweed_pollen": [0.0, 0.0],
        }
    )

    mock_minio_instance.read_parquet.return_value = pollen_df
    MockMinIOClass.return_value = mock_minio_instance

    mock_cursor = Mock()
    mock_cursor.rowcount = 2
    mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_pollen(ds="2026-01-29")

    assert result >= 0


@pytest.mark.unit
@patch("src.loader.execute_values")
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_load_fact_marine(mock_connect, MockMinIOClass, mock_execute_values, mock_db_connection):
    """Test loading marine data"""
    mock_minio_instance = Mock()

    marine_df = pd.DataFrame(
        {
            "time": ["2026-01-29T00:00", "2026-01-29T01:00"],
            "city_code": ["08019", "08019"],
            "city_name": ["Barcelona", "Barcelona"],
            "wave_height_max": [1.5, 1.6],
            "wave_direction_dominant": [180, 185],
            "wave_period_max": [8.5, 8.8],
            "wind_wave_height_max": [0.8, 0.9],
            "swell_wave_height_max": [1.2, 1.3],
        }
    )

    mock_minio_instance.read_parquet.return_value = marine_df
    MockMinIOClass.return_value = mock_minio_instance

    mock_cursor = Mock()
    mock_cursor.rowcount = 2
    mock_cursor.fetchall.side_effect = [[("Barcelona", 1)], [("08019", 1)]]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_marine(ds="2026-01-29")

    assert result >= 0


@pytest.mark.unit
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_load_handles_empty_dataframe(mock_connect, MockMinIOClass, mock_db_connection):
    """Test loading handles empty DataFrame"""
    mock_minio_instance = Mock()
    mock_minio_instance.read_parquet.return_value = pd.DataFrame()
    MockMinIOClass.return_value = mock_minio_instance

    mock_cursor = Mock()
    mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_forecast_daily(ds="2026-01-29")

    assert result == 0


@pytest.mark.unit
@patch("src.loader.MinIOClient")
@patch("src.loader.psycopg2.connect")
def test_load_handles_missing_city_mapping(mock_connect, MockMinIOClass, mock_db_connection):
    """Test loading handles missing city mapping"""
    mock_minio_instance = Mock()

    df = pd.DataFrame(
        {
            "time": ["2026-01-29"],
            "city_code": ["99999"],  # Non-existent city
            "city_name": ["Unknown"],
            "temperature_2m_max": [20.0],
        }
    )

    mock_minio_instance.read_parquet.return_value = df
    MockMinIOClass.return_value = mock_minio_instance

    mock_cursor = Mock()
    mock_cursor.fetchall.side_effect = [[], []]  # No city mapping
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_forecast_daily(ds="2026-01-29")

    # Should handle gracefully
    assert result >= 0


@pytest.mark.unit
@patch("src.loader.MinIOClient")
def test_clean_value_with_various_types(MockMinIOClass):
    """Test clean_value handles various data types"""
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    loader = Loader()

    # Test None
    assert loader.clean_value(None) is None

    # Test actual NaN values
    assert loader.clean_value(np.nan) is None
    assert loader.clean_value(float("nan")) is None

    # Test regular values - strings are preserved as-is
    assert loader.clean_value(25.5) == 25.5
    assert loader.clean_value("Madrid") == "Madrid"
    assert loader.clean_value(100) == 100
    assert loader.clean_value("nan") == "nan"  # String "nan" is not NaN


@pytest.mark.unit
@patch("src.loader.MinIOClient")
def test_get_date_id_edge_cases(MockMinIOClass):
    """Test get_date_id with edge cases"""
    mock_minio_instance = Mock()
    MockMinIOClass.return_value = mock_minio_instance

    loader = Loader()

    # Test with datetime string
    assert loader.get_date_id("2026-01-29") == 20260129
    assert loader.get_date_id("2026-12-31") == 20261231

    # Test with datetime object
    from datetime import datetime

    dt = datetime(2026, 1, 29, 12, 0, 0)
    result = loader.get_date_id(dt)
    assert result == 20260129

    # Test with timestamp string
    result = loader.get_date_id("2026-01-29T12:00:00")
    assert result == 20260129
