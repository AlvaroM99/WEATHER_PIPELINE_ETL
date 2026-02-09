"""
Comprehensive tests for DimensionalLoader module.

Tests cover all dimensional loading methods to achieve 70%+ coverage.
"""

from datetime import date, timedelta
from io import StringIO
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
import requests

from src.dimensional_loader import DimensionalLoader


class TestDimensionalLoaderInit:
    """Test DimensionalLoader initialization."""

    def test_init_creates_instance(self):
        """Test that DimensionalLoader initializes correctly."""
        loader = DimensionalLoader()
        assert loader is not None
        assert hasattr(loader, "logger")


class TestLoadDimDate:
    """Test load_dim_date method."""

    @patch("src.dimensional_loader.DimensionalLoader.connection")
    def test_load_dim_date_success(self, mock_connection):
        """Test successful loading of dim_date."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_connection.return_value = mock_conn

        loader = DimensionalLoader()
        loader.load_dim_date()

        # Verify cursor was created and closed
        mock_conn.cursor.assert_called_once()
        mock_cursor.__enter__.assert_called()

        # Verify execute was called (should be 4018 times for 2020-2030)
        assert mock_cursor.execute.call_count > 4000

    @patch("src.dimensional_loader.DimensionalLoader.connection")
    def test_load_dim_date_generates_correct_date_range(self, mock_connection):
        """Test that dim_date generates dates from 2020-01-01 to 2030-12-31."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_connection.return_value = mock_conn

        loader = DimensionalLoader()
        loader.load_dim_date()

        # Check first call (2020-01-01)
        first_call = mock_cursor.execute.call_args_list[0]
        first_date_params = first_call[0][1]
        assert first_date_params[0] == 20200101  # id_calendar_day
        assert first_date_params[1] == date(2020, 1, 1)  # dt_date

        # Check last call (2030-12-31)
        last_call = mock_cursor.execute.call_args_list[-1]
        last_date_params = last_call[0][1]
        assert last_date_params[0] == 20301231  # id_calendar_day
        assert last_date_params[1] == date(2030, 12, 31)  # dt_date

    @patch("src.dimensional_loader.DimensionalLoader.connection")
    def test_load_dim_date_handles_error(self, mock_connection):
        """Test error handling in load_dim_date."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Database error")
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_connection.return_value = mock_conn

        loader = DimensionalLoader()

        with pytest.raises(Exception, match="Database error"):
            loader.load_dim_date()

        # Verify cursor was closed even on error
        mock_cursor.__enter__.assert_called()


class TestLoadDimCity:
    """Test load_dim_city method."""

    @patch("src.dimensional_loader.get_retrying_session")
    @patch("src.dimensional_loader.DimensionalLoader.connection")
    def test_load_dim_city_success(self, mock_connection, mock_get_session):
        """Test successful loading of dim_city from GitHub CSV."""
        # Mock CSV response via retry session
        csv_content = """city_code,city_name,latitud,longitud,country_code,is_coastal
28079,Madrid,40.4168,-3.7038,ES,0
08019,Barcelona,41.3851,2.1734,ES,1"""

        mock_session = Mock()
        mock_response = Mock()
        mock_response.text = csv_content
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response
        mock_get_session.return_value = mock_session

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_connection.return_value = mock_conn

        loader = DimensionalLoader()
        loader.load_dim_city()

        # Verify CSV was downloaded via session
        mock_session.get.assert_called_once()

        # Verify 2 cities were inserted
        assert mock_cursor.execute.call_count == 2

        # Verify cursor was closed
        mock_cursor.__enter__.assert_called()

    @patch("src.dimensional_loader.get_retrying_session")
    @patch("src.dimensional_loader.DimensionalLoader.connection")
    def test_load_dim_city_missing_columns(self, mock_connection, mock_get_session):
        """Test error handling when CSV has missing columns."""
        # Mock CSV with missing columns
        csv_content = """city_code,city_name
28079,Madrid"""

        mock_session = Mock()
        mock_response = Mock()
        mock_response.text = csv_content
        mock_response.raise_for_status = Mock()
        mock_session.get.return_value = mock_response
        mock_get_session.return_value = mock_session

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_connection.return_value = mock_conn

        loader = DimensionalLoader()

        with pytest.raises(ValueError, match="CSV missing required columns"):
            loader.load_dim_city()

        # Verify cursor was closed
        mock_cursor.__enter__.assert_called()

    @patch("src.dimensional_loader.get_retrying_session")
    @patch("src.dimensional_loader.DimensionalLoader.connection")
    def test_load_dim_city_request_error(self, mock_connection, mock_get_session):
        """Test error handling when GitHub request fails."""
        # Mock request exception via session
        mock_session = Mock()
        mock_session.get.side_effect = requests.RequestException("Network error")
        mock_get_session.return_value = mock_session

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_connection.return_value = mock_conn

        loader = DimensionalLoader()

        with pytest.raises(requests.RequestException, match="Network error"):
            loader.load_dim_city()

        # Verify cursor was closed
        mock_cursor.__enter__.assert_called()


class TestLoadDimWeek:
    """Test load_dim_week method."""

    @patch("src.dimensional_loader.DimensionalLoader.connection")
    def test_load_dim_week_success(self, mock_connection):
        """Test successful loading of dim_week."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_connection.return_value = mock_conn

        loader = DimensionalLoader()
        loader.load_dim_week()

        # Verify SQL was executed
        mock_cursor.execute.assert_called_once()
        assert "INSERT INTO dwh.dim_week" in mock_cursor.execute.call_args[0][0]

        # Verify cursor was closed
        mock_cursor.__enter__.assert_called()

    @patch("src.dimensional_loader.DimensionalLoader.connection")
    def test_load_dim_week_handles_error(self, mock_connection):
        """Test error handling in load_dim_week."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Database error")
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_connection.return_value = mock_conn

        loader = DimensionalLoader()

        with pytest.raises(Exception, match="Database error"):
            loader.load_dim_week()

        # Verify cursor was closed
        mock_cursor.__enter__.assert_called()


class TestLoadDimMonth:
    """Test load_dim_month method."""

    @patch("src.dimensional_loader.DimensionalLoader.connection")
    def test_load_dim_month_success(self, mock_connection):
        """Test successful loading of dim_month."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_connection.return_value = mock_conn

        loader = DimensionalLoader()
        loader.load_dim_month()

        # Verify SQL was executed
        mock_cursor.execute.assert_called_once()
        assert "INSERT INTO dwh.dim_month" in mock_cursor.execute.call_args[0][0]

        # Verify cursor was closed
        mock_cursor.__enter__.assert_called()


class TestLoadDimAemetStations:
    """Test load_dim_aemet_stations method."""

    @patch("src.dimensional_loader.DimensionalLoader.connection")
    @patch("src.dimensional_loader.DEFAULT_STATIONS")
    def test_load_dim_aemet_stations_success(self, mock_default_stations, mock_connection):
        """Test successful loading of AEMET stations."""
        # Mock default stations
        mock_default_stations.__iter__ = Mock(
            return_value=iter(
                [
                    ("3129", "MADRID, RETIRO", "MADRID", 667.0, 40.415278, -3.678056),
                    ("0076", "BARCELONA, FABRA", "BARCELONA", 412.0, 41.419444, 2.291667),
                ]
            )
        )
        mock_default_stations.__len__ = Mock(return_value=2)

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_connection.return_value = mock_conn

        loader = DimensionalLoader()
        loader.load_dim_aemet_stations()

        # Verify 2 stations were inserted
        assert mock_cursor.execute.call_count == 2

        # Verify cursor was closed
        mock_cursor.__enter__.assert_called()


class TestLoadAllDimensionalTables:
    """Test load_all_dimensional_tables wrapper method."""

    @patch.object(DimensionalLoader, "load_dim_aemet_stations")
    @patch.object(DimensionalLoader, "load_dim_severity")
    @patch.object(DimensionalLoader, "load_dim_layers")
    @patch.object(DimensionalLoader, "load_dim_seasons")
    @patch.object(DimensionalLoader, "load_dim_city")
    @patch.object(DimensionalLoader, "load_dim_month")
    @patch.object(DimensionalLoader, "load_dim_week")
    @patch.object(DimensionalLoader, "load_dim_date")
    def test_load_all_dimensional_tables_calls_all_methods(
        self,
        mock_date,
        mock_week,
        mock_month,
        mock_city,
        mock_seasons,
        mock_layers,
        mock_severity,
        mock_aemet,
    ):
        """Test that load_all_dimensional_tables calls all loading methods."""
        loader = DimensionalLoader()
        loader.load_all_dimensional_tables()

        # Verify all methods were called
        mock_date.assert_called_once()
        mock_week.assert_called_once()
        mock_month.assert_called_once()
        mock_city.assert_called_once()
        mock_seasons.assert_called_once()
        mock_layers.assert_called_once()
        mock_severity.assert_called_once()
        mock_aemet.assert_called_once()

    @patch.object(DimensionalLoader, "load_dim_aemet_stations")
    @patch.object(DimensionalLoader, "load_dim_severity")
    @patch.object(DimensionalLoader, "load_dim_layers")
    @patch.object(DimensionalLoader, "load_dim_seasons")
    @patch.object(DimensionalLoader, "load_dim_city")
    @patch.object(DimensionalLoader, "load_dim_month")
    @patch.object(DimensionalLoader, "load_dim_week")
    @patch.object(DimensionalLoader, "load_dim_date")
    def test_load_all_dimensional_tables_accepts_context(
        self, mock_date, mock_week, mock_month, mock_city,
        mock_seasons, mock_layers, mock_severity, mock_aemet
    ):
        """Test that load_all_dimensional_tables accepts Airflow context."""
        loader = DimensionalLoader()
        context = {"ds": "2026-01-29", "task_instance": Mock()}

        # Should not raise error
        loader.load_all_dimensional_tables(**context)

        mock_date.assert_called_once()
        mock_week.assert_called_once()
        mock_month.assert_called_once()
        mock_city.assert_called_once()
