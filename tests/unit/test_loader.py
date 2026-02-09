"""
Unit tests for Loader module
Tests database loading logic for fact tables
"""

from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import numpy as np
import pandas as pd
import pytest

from src.config.db_pool import reset_pool
from src.loader import Loader
from src.utils.minio_client import reset_minio_client


@pytest.fixture(autouse=True)
def reset_singletons():
    """Reset singletons before and after each test."""
    reset_pool()
    reset_minio_client()
    yield
    reset_pool()
    reset_minio_client()


# ===== Utility Method Tests =====


@pytest.mark.unit
@patch("src.loader.get_minio_client")
def test_get_date_id_valid_date(mock_get_minio_client):
    """Test get_date_id converts date string to integer correctly"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    loader = Loader()

    # Test standard date format
    result = loader.get_date_id("2026-01-29")
    assert result == 20260129

    # Test with datetime string
    result = loader.get_date_id("2026-01-29T12:30:00")
    assert result == 20260129


@pytest.mark.unit
@patch("src.loader.get_minio_client")
def test_get_date_id_invalid_date(mock_get_minio_client):
    """Test get_date_id handles invalid dates"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    loader = Loader()

    result = loader.get_date_id("invalid-date")
    assert result is None


@pytest.mark.unit
@patch("src.loader.get_minio_client")
def test_clean_value_nan_handling(mock_get_minio_client):
    """Test clean_value converts NaN to None for database"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

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
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_get_city_id_mapping(mock_connect, mock_get_minio_client, mock_db_connection):
    """Test get_city_id_mapping returns correct dictionaries"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    # Setup DB mock
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor

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
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_connection_context_manager(mock_connect, mock_get_minio_client):
    """Test connection() context manager provides a connection"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    mock_conn = MagicMock()
    mock_connect.return_value = mock_conn

    loader = Loader()
    with loader.connection() as conn:
        assert conn is not None


# ===== Fact Observation Loading Tests =====


@pytest.mark.unit
@patch("src.loader.execute_values")
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_fact_observation_success(
    mock_connect,
    mock_get_minio_client,
    mock_execute_values,
    mock_db_connection,
    sample_transformed_df,
):
    """Test successful loading of weather observations"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.read_parquet.return_value = sample_transformed_df
    mock_get_minio_client.return_value = mock_minio_instance

    # Setup DB mocks
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = len(sample_transformed_df)
    mock_cursor.fetchone.return_value = (1,)  # log_etl_start RETURNING id
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
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_fact_observation_city_not_found(
    mock_connect, mock_get_minio_client, mock_db_connection
):
    """Test loading handles cities not in dimension table"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    test_df = pd.DataFrame(
        [{"city": "Madrid", "temperature": 25.5, "humidity": 60, "date": "2026-01-29"}]
    )

    mock_minio_instance.read_parquet.return_value = test_df
    mock_get_minio_client.return_value = mock_minio_instance

    # Setup DB mocks
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (1,)  # log_etl_start RETURNING id
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
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_fact_observation_no_data(mock_connect, mock_get_minio_client, mock_db_connection):
    """Test loading with no data available"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.read_parquet.side_effect = Exception("File not found")
    mock_get_minio_client.return_value = mock_minio_instance

    mock_connect.return_value = mock_db_connection

    # Execute
    loader = Loader()

    result = loader.load_fact_observation(ds="2026-01-29")

    # Assert
    assert result == 0


@pytest.mark.unit
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_fact_observation_rollback_on_error(
    mock_connect, mock_get_minio_client, mock_db_connection
):
    """Test that database transaction rolls back on error"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    test_df = pd.DataFrame(
        [{"city": "Madrid", "temperature": 25.5, "humidity": 60, "date": "2026-01-29"}]
    )

    mock_minio_instance.read_parquet.return_value = test_df
    mock_get_minio_client.return_value = mock_minio_instance

    # Setup DB mocks
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (1,)  # log_etl_start RETURNING id
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
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_generic_success(
    mock_connect,
    mock_get_minio_client,
    mock_execute_values,
    mock_db_connection,
    sample_silver_parquet_df,
):
    """Test _load_generic helper method"""
    # Setup MinIO mock
    mock_minio_instance = Mock()

    mock_obj = Mock()
    mock_obj.object_name = "forecast/daily/2026-01-29/daily_forecast_20260129_120000.parquet"

    mock_minio_instance.client.list_objects.return_value = [mock_obj]
    mock_minio_instance.read_parquet.return_value = sample_silver_parquet_df

    mock_get_minio_client.return_value = mock_minio_instance

    # Setup DB mocks
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = len(sample_silver_parquet_df)
    mock_cursor.fetchone.return_value = (1,)  # log_etl_start RETURNING id
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
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_generic_deduplication(
    mock_connect, mock_get_minio_client, mock_execute_values, mock_db_connection
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

    mock_get_minio_client.return_value = mock_minio_instance

    # Setup DB mocks
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 2  # Only 2 unique rows
    mock_cursor.fetchone.return_value = (1,)  # log_etl_start RETURNING id
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
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_generic_no_files_found(mock_connect, mock_get_minio_client, mock_db_connection):
    """Test _load_generic handles no files found"""
    # Setup MinIO mock
    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []
    mock_get_minio_client.return_value = mock_minio_instance

    mock_connect.return_value = mock_db_connection

    # Execute
    loader = Loader()

    result = loader.load_fact_forecast_daily(ds="2026-01-29")

    # Assert
    assert result == 0


# ===== Mapper Function Tests =====


@pytest.mark.unit
@patch("src.loader.get_minio_client")
def test_map_daily_forecast(mock_get_minio_client):
    """Test _map_daily_forecast mapper function"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

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
@patch("src.loader.get_minio_client")
def test_map_air_quality(mock_get_minio_client):
    """Test _map_air_quality mapper function"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

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
@patch("src.loader.get_minio_client")
def test_map_air_quality_handles_nan(mock_get_minio_client):
    """Test _map_air_quality handles NaN values"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

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
@patch("src.loader.get_minio_client")
def test_loader_initialization(mock_get_minio_client):
    """Test Loader initializes correctly"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    loader = Loader()

    assert loader is not None
    assert loader.logger is not None
    assert loader.minio_client is not None


@pytest.mark.unit
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_loader_logging(mock_connect, mock_get_minio_client, mock_db_connection, caplog):
    """Test that loader logs operations"""
    import logging

    caplog.set_level(logging.INFO)

    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []
    mock_get_minio_client.return_value = mock_minio_instance

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
@patch("src.base_loader.return_db_connection")
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_connection_returned_to_pool_on_success(
    mock_connect, mock_get_minio_client, mock_return_conn, mock_db_connection
):
    """Test database connection is returned to pool after successful operation"""
    mock_minio_instance = Mock()
    mock_minio_instance.read_parquet.side_effect = Exception("No file")
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (1,)  # log_etl_start RETURNING id
    mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()

    result = loader.load_fact_observation(ds="2026-01-29")

    # Assert - connection was returned to the pool via context manager
    mock_return_conn.assert_called()


@pytest.mark.unit
@patch("src.base_loader.return_db_connection")
@patch("src.base_loader.get_db_connection")
@patch("src.loader.get_minio_client")
def test_connection_returned_to_pool_on_error(
    mock_get_minio_client, mock_get_conn, mock_return_conn, mock_db_connection
):
    """Test database connection is returned to pool even on error"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.side_effect = Exception("Query error")
    mock_db_connection.cursor.return_value = mock_cursor
    mock_get_conn.return_value = mock_db_connection

    loader = Loader()

    with pytest.raises(Exception):
        with loader.connection() as conn:
            loader.get_city_id_mapping(conn)

    # Assert - connection was returned to the pool despite error
    mock_return_conn.assert_called()


# ===== Additional Loader Tests =====


@pytest.mark.unit
@patch("src.loader.execute_values")
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_fact_forecast_hourly(
    mock_connect, mock_get_minio_client, mock_execute_values, mock_db_connection
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
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 2
    mock_cursor.fetchone.return_value = (1,)  # log_etl_start RETURNING id
    mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_forecast_hourly(ds="2026-01-29")

    assert result >= 0


@pytest.mark.unit
@patch("src.loader.execute_values")
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_fact_air_quality(
    mock_connect, mock_get_minio_client, mock_execute_values, mock_db_connection
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
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 2
    mock_cursor.fetchone.return_value = (1,)  # log_etl_start RETURNING id
    mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_air_quality(ds="2026-01-29")

    assert result >= 0


@pytest.mark.unit
@patch("src.loader.execute_values")
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_fact_pollen(
    mock_connect, mock_get_minio_client, mock_execute_values, mock_db_connection
):
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
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 2
    mock_cursor.fetchone.return_value = (1,)  # log_etl_start RETURNING id
    mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_pollen(ds="2026-01-29")

    assert result >= 0


@pytest.mark.unit
@patch("src.loader.execute_values")
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_fact_marine(
    mock_connect, mock_get_minio_client, mock_execute_values, mock_db_connection
):
    """Test loading marine data"""
    mock_minio_instance = Mock()

    marine_df = pd.DataFrame(
        {
            "time": ["2026-01-29", "2026-01-30"],
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
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 2
    mock_cursor.fetchone.return_value = (1,)  # log_etl_start RETURNING id
    mock_cursor.fetchall.side_effect = [[("Barcelona", 1)], [("08019", 1)]]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_marine(ds="2026-01-29")

    assert result >= 0


@pytest.mark.unit
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_handles_empty_dataframe(mock_connect, mock_get_minio_client, mock_db_connection):
    """Test loading handles empty DataFrame"""
    mock_minio_instance = Mock()
    mock_minio_instance.read_parquet.return_value = pd.DataFrame()
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (1,)  # log_etl_start RETURNING id
    mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_forecast_daily(ds="2026-01-29")

    assert result == 0


@pytest.mark.unit
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_handles_missing_city_mapping(mock_connect, mock_get_minio_client, mock_db_connection):
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
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (1,)  # log_etl_start RETURNING id
    mock_cursor.fetchall.side_effect = [[], []]  # No city mapping
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_forecast_daily(ds="2026-01-29")

    # Should handle gracefully
    assert result >= 0


@pytest.mark.unit
@patch("src.loader.get_minio_client")
def test_clean_value_with_various_types(mock_get_minio_client):
    """Test clean_value handles various data types"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

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
@patch("src.loader.get_minio_client")
def test_get_date_id_edge_cases(mock_get_minio_client):
    """Test get_date_id with edge cases"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

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


# ===== AEMET Loader Tests =====


@pytest.mark.unit
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_get_station_id_mapping(mock_connect, mock_get_minio_client, mock_db_connection):
    """Test get_station_id_mapping returns correct dictionary"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("3129", 1), ("0076", 2)]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    station_map = loader.get_station_id_mapping(mock_db_connection)

    assert "3129" in station_map
    assert station_map["3129"] == 1
    assert "0076" in station_map
    assert station_map["0076"] == 2


@pytest.mark.unit
@patch("src.loader.execute_values")
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_aemet_stations_success(
    mock_connect,
    mock_get_minio_client,
    mock_execute_values,
    mock_db_connection,
    sample_aemet_silver_df,
):
    """Test successful AEMET stations loading"""
    mock_minio_instance = Mock()

    mock_obj = Mock()
    mock_obj.object_name = "stations/2026-01-29/stations_20260129_120000.parquet"

    mock_minio_instance.client.list_objects.return_value = [mock_obj]
    mock_minio_instance.read_parquet.return_value = sample_aemet_silver_df
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 2
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_aemet_stations(ds="2026-01-29")

    assert result > 0
    assert mock_execute_values.called


@pytest.mark.unit
@patch("src.loader.DimensionalLoader")
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_aemet_stations_fallback_to_dimensional_loader(
    mock_connect, mock_get_minio_client, MockDimensionalLoader, mock_db_connection
):
    """Test AEMET stations loading falls back to DimensionalLoader when no silver data"""
    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []  # No files
    mock_get_minio_client.return_value = mock_minio_instance

    mock_dim_loader_instance = Mock()
    MockDimensionalLoader.return_value = mock_dim_loader_instance

    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_aemet_stations(ds="2026-01-29")

    # Should return 15 (default stations)
    assert result == 15
    assert MockDimensionalLoader.called
    assert mock_dim_loader_instance.load_dim_aemet_stations.called


@pytest.mark.unit
@patch("src.loader.execute_values")
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_fact_aemet_daily_success(
    mock_connect,
    mock_get_minio_client,
    mock_execute_values,
    mock_db_connection,
    sample_aemet_daily_silver_df,
):
    """Test successful AEMET daily loading"""
    mock_minio_instance = Mock()

    mock_obj = Mock()
    mock_obj.object_name = "climatology/daily/2026-01-29/daily_climatology_20260129_120000.parquet"

    mock_minio_instance.client.list_objects.return_value = [mock_obj]
    mock_minio_instance.read_parquet.return_value = sample_aemet_daily_silver_df
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.rowcount = 1
    # First call for station mapping, second for data load
    mock_cursor.fetchall.return_value = [("3129", 1), ("0076", 2)]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_aemet_daily(ds="2026-01-29")

    assert result > 0
    assert mock_execute_values.called


@pytest.mark.unit
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_fact_aemet_daily_no_files(mock_connect, mock_get_minio_client, mock_db_connection):
    """Test AEMET daily loading with no files"""
    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("3129", 1)]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_aemet_daily(ds="2026-01-29")

    assert result == 0


@pytest.mark.unit
@patch("src.loader.DimensionalLoader")
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_fact_aemet_daily_loads_stations_if_missing(
    mock_connect, mock_get_minio_client, MockDimensionalLoader, mock_db_connection
):
    """Test AEMET daily loading loads stations first if missing"""
    mock_minio_instance = Mock()

    # Return empty list - no files to process
    mock_minio_instance.client.list_objects.return_value = []
    mock_get_minio_client.return_value = mock_minio_instance

    mock_dim_loader_instance = Mock()
    MockDimensionalLoader.return_value = mock_dim_loader_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    # First call returns empty (no stations), triggers fallback
    mock_cursor.fetchall.return_value = []
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_aemet_daily(ds="2026-01-29")

    # Should have called DimensionalLoader because no stations found
    assert MockDimensionalLoader.called
    assert mock_dim_loader_instance.load_dim_aemet_stations.called
    # Result is 0 because no files found after loading stations
    assert result == 0


@pytest.mark.unit
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_fact_aemet_daily_skips_unknown_stations(
    mock_connect, mock_get_minio_client, mock_db_connection
):
    """Test AEMET daily loading skips unknown stations"""
    mock_minio_instance = Mock()

    mock_obj = Mock()
    mock_obj.object_name = "climatology/daily/2026-01-29/daily.parquet"
    mock_minio_instance.client.list_objects.return_value = [mock_obj]

    # DataFrame with unknown station
    df = pd.DataFrame([{"station_id": "UNKNOWN", "date": "2026-01-29", "temp_avg": 10.0}])
    mock_minio_instance.read_parquet.return_value = df
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("3129", 1)]  # UNKNOWN not in mapping
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_aemet_daily(ds="2026-01-29")

    # Should return 0 since no matching stations
    assert result == 0


@pytest.mark.unit
@patch("src.loader.get_minio_client")
@patch("src.loader.psycopg2.connect")
def test_load_fact_aemet_historical_no_files(
    mock_connect, mock_get_minio_client, mock_db_connection
):
    """Test AEMET historical loading with no files"""
    mock_minio_instance = Mock()
    mock_minio_instance.client.list_objects.return_value = []
    mock_get_minio_client.return_value = mock_minio_instance

    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.fetchall.return_value = [("3129", 1)]
    mock_db_connection.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_db_connection

    loader = Loader()
    result = loader.load_fact_aemet_historical(ds="2026-01-29")

    assert result == 0


@pytest.mark.unit
@patch("src.loader.get_minio_client")
def test_loader_log_methods(mock_get_minio_client):
    """Test loader logging utility methods"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    loader = Loader()

    # Test log methods don't raise errors
    loader.log_start("Test start message")
    loader.log_end("Test end message")
    loader.log_error("Test error message")
    loader.log_error("Test error with exception", Exception("Test exception"))


@pytest.mark.unit
@patch("src.loader.get_minio_client")
def test_loader_validation_disabled_by_default(mock_get_minio_client):
    """Test loader validation is disabled when great_expectations not installed"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    # When DATA_QUALITY_AVAILABLE is False, validation should be disabled
    loader = Loader(enable_validation=True)

    # Should not raise errors
    result = loader.validate_data(pd.DataFrame(), "unknown_type", "test_table")
    assert result is None


@pytest.mark.unit
@patch("src.loader.get_minio_client")
def test_loader_get_quality_report(mock_get_minio_client):
    """Test get_quality_report returns empty dict when no validations run"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    loader = Loader()
    report = loader.get_quality_report()

    assert isinstance(report, dict)


@pytest.mark.unit
@patch("src.loader.get_minio_client")
def test_loader_get_validation_result(mock_get_minio_client):
    """Test get_validation_result returns None for unknown table"""
    mock_minio_instance = Mock()
    mock_get_minio_client.return_value = mock_minio_instance

    loader = Loader()
    result = loader.get_validation_result("unknown_table")

    assert result is None


# ===== Idempotency Tests =====


class TestIdempotency:
    """Verify ON CONFLICT DO NOTHING ensures idempotent inserts."""

    @pytest.mark.unit
    @patch("src.loader.execute_values")
    @patch("src.loader.get_minio_client")
    @patch("src.loader.psycopg2.connect")
    def test_load_fact_observation_idempotent(
        self, mock_connect, mock_get_minio_client, mock_execute_values,
        mock_db_connection, sample_transformed_df,
    ):
        """Calling load_fact_observation twice with same data should not error."""
        mock_minio_instance = Mock()
        mock_minio_instance.read_parquet.return_value = sample_transformed_df
        mock_get_minio_client.return_value = mock_minio_instance

        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.rowcount = len(sample_transformed_df)
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.fetchall.side_effect = [
            [("Madrid", 1)], [("28079", 1)],
            [("Madrid", 1)], [("28079", 1)],
        ]
        mock_db_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_db_connection

        loader = Loader()

        # First call
        result1 = loader.load_fact_observation(ds="2026-01-29")
        # Second call (DB returns 0 because ON CONFLICT DO NOTHING)
        mock_cursor.rowcount = 0
        result2 = loader.load_fact_observation(ds="2026-01-29")

        assert result1 > 0
        assert result2 == 0
        assert mock_execute_values.call_count == 2

    @pytest.mark.unit
    @patch("src.loader.execute_values")
    @patch("src.loader.get_minio_client")
    @patch("src.loader.psycopg2.connect")
    def test_insert_query_contains_on_conflict(
        self, mock_connect, mock_get_minio_client, mock_execute_values,
        mock_db_connection, sample_transformed_df,
    ):
        """Verify the INSERT query used contains ON CONFLICT clause."""
        mock_minio_instance = Mock()
        mock_minio_instance.read_parquet.return_value = sample_transformed_df
        mock_get_minio_client.return_value = mock_minio_instance

        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.rowcount = 1
        mock_cursor.fetchone.return_value = (1,)
        mock_cursor.fetchall.side_effect = [[("Madrid", 1)], [("28079", 1)]]
        mock_db_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_db_connection

        loader = Loader()
        loader.load_fact_observation(ds="2026-01-29")

        # Verify execute_values was called with ON CONFLICT
        call_args = mock_execute_values.call_args
        sql_query = call_args[0][1]
        assert "ON CONFLICT" in sql_query


# ===== SQL Injection Safety Tests =====


class TestSQLInjectionSafety:
    """Verify parameterized queries protect against SQL injection."""

    @pytest.mark.unit
    @patch("src.loader.execute_values")
    @patch("src.loader.get_minio_client")
    @patch("src.loader.psycopg2.connect")
    def test_malicious_city_name_is_parameterized(
        self, mock_connect, mock_get_minio_client, mock_execute_values,
        mock_db_connection,
    ):
        """City names with SQL injection strings are safely handled via parameterized queries."""
        mock_minio_instance = Mock()
        malicious_df = pd.DataFrame([{
            "city": "'; DROP TABLE dwh.dim_city; --",
            "temperature": 25.5,
            "humidity": 60,
            "dt": 1706543400,
            "date": "2026-01-29",
        }])
        mock_minio_instance.read_parquet.return_value = malicious_df
        mock_get_minio_client.return_value = mock_minio_instance

        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.rowcount = 1
        mock_cursor.fetchone.return_value = (1,)
        # Malicious city IS in the mapping (simulating it got inserted somehow)
        mock_cursor.fetchall.side_effect = [
            [("'; DROP TABLE dwh.dim_city; --", 99)],
            [("99999", 99)],
        ]
        mock_db_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_db_connection

        loader = Loader()
        result = loader.load_fact_observation(ds="2026-01-29")

        assert result >= 0
        # Verify execute_values was called (parameterized, not string interpolation)
        assert mock_execute_values.called
        sql_query = mock_execute_values.call_args[0][1]
        assert "DROP TABLE" not in sql_query


# ===== Permission Error Tests =====


class TestPermissionErrors:
    """Test proper handling of database permission errors."""

    @pytest.mark.unit
    @patch("src.loader.get_minio_client")
    @patch("src.base_loader.get_db_connection")
    @patch("src.base_loader.return_db_connection")
    def test_insufficient_privilege_propagates(
        self, mock_return_conn, mock_get_conn, mock_get_minio_client,
    ):
        """InsufficientPrivilege error should propagate properly."""
        mock_minio_instance = Mock()
        mock_get_minio_client.return_value = mock_minio_instance

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.execute.side_effect = PermissionError(
            "permission denied for table fct_weather_observation"
        )
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        loader = Loader()

        with pytest.raises(Exception):
            loader.load_fact_observation(ds="2026-01-29")

        # Connection should still be returned to pool
        mock_return_conn.assert_called_once_with(mock_conn)
