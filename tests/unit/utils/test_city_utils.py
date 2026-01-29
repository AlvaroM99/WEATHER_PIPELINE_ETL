"""
Unit tests for city_utils module
Tests GitHub CSV download, caching, and data format conversions
"""

from unittest.mock import Mock, patch

import pandas as pd
import pytest
import responses

from src.weather_utils.city_utils import (
    get_capitals_dataframe,
    get_cities,
    get_coastal_cities,
    load_cities_from_github,
)

GITHUB_CSV_URL = (
    "https://raw.githubusercontent.com/AlvaroM99/spanish_capital_cities/main/raw_cities.csv"
)


@pytest.mark.unit
@pytest.mark.api
@responses.activate
def test_load_cities_from_github_success(sample_github_csv):
    """Test successful download of cities CSV from GitHub"""
    # Setup mock response
    responses.add(responses.GET, GITHUB_CSV_URL, body=sample_github_csv, status=200)

    # Clear cache first
    import src.weather_utils.city_utils as city_utils

    city_utils._cities_cache = None

    # Execute
    df = load_cities_from_github()

    # Assert
    assert df is not None
    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
    assert "city_code" in df.columns
    assert "city_name" in df.columns
    assert "latitud" in df.columns
    assert "longitud" in df.columns
    assert "is_coastal" in df.columns


@pytest.mark.unit
@pytest.mark.api
@responses.activate
def test_load_cities_from_github_network_error():
    """Test handling of network errors when downloading CSV"""
    # Setup mock to simulate network error
    responses.add(responses.GET, GITHUB_CSV_URL, body="Connection error", status=500)

    # Clear cache
    import src.weather_utils.city_utils as city_utils

    city_utils._cities_cache = None

    # Execute and expect exception
    with pytest.raises(Exception):
        load_cities_from_github()


@pytest.mark.unit
def test_caching_mechanism():
    """Test that cities are cached and not downloaded twice"""
    import src.weather_utils.city_utils as city_utils

    # Setup cache with sample data
    sample_df = pd.DataFrame(
        [
            {
                "city_code": "28079",
                "city_name": "Madrid",
                "latitud": 40.4168,
                "longitud": -3.7038,
                "country_code": "ES",
                "is_coastal": 0,
            }
        ]
    )
    city_utils._cities_cache = sample_df

    # Call function - should return cached data without HTTP call
    result = load_cities_from_github()

    # Assert - should return a copy of cached data
    assert result is not None
    assert len(result) == 1
    assert result["city_name"].iloc[0] == "Madrid"
    # Verify it's a copy, not the same object
    assert result is not city_utils._cities_cache


@pytest.mark.unit
def test_get_cities_format(sample_github_csv):
    """Test get_cities() returns correct format for OpenWeather API"""
    import src.weather_utils.city_utils as city_utils

    # Setup cache with sample data
    df = pd.read_csv(pd.io.common.StringIO(sample_github_csv))
    city_utils._cities_cache = df

    # Execute
    cities = get_cities()

    # Assert
    assert isinstance(cities, list)
    assert len(cities) > 0

    # Verify first city has correct structure
    first_city = cities[0]
    assert "name" in first_city
    assert "lat" in first_city
    assert "lon" in first_city

    # Verify data types
    assert isinstance(first_city["name"], str)
    assert isinstance(first_city["lat"], (int, float))
    assert isinstance(first_city["lon"], (int, float))

    # Verify specific values
    madrid = next((c for c in cities if c["name"] == "Madrid"), None)
    assert madrid is not None
    assert madrid["lat"] == pytest.approx(40.4168, abs=0.001)
    assert madrid["lon"] == pytest.approx(-3.7038, abs=0.001)


@pytest.mark.unit
def test_get_capitals_dataframe_format(sample_github_csv):
    """Test get_capitals_dataframe() returns correct format for Open-Meteo API"""
    import src.weather_utils.city_utils as city_utils

    # Setup cache
    df = pd.read_csv(pd.io.common.StringIO(sample_github_csv))
    city_utils._cities_cache = df

    # Execute
    capitals_df = get_capitals_dataframe()

    # Assert
    assert isinstance(capitals_df, pd.DataFrame)
    assert len(capitals_df) > 0

    # Verify required columns
    required_columns = ["city_code", "municipio_nombre", "latitude", "longitude"]
    for col in required_columns:
        assert col in capitals_df.columns, f"Missing column: {col}"

    # Verify column mapping (latitud → latitude, city_name → municipio_nombre)
    assert "latitud" not in capitals_df.columns
    assert "longitud" not in capitals_df.columns
    assert "city_name" not in capitals_df.columns

    # Verify data types
    # Note: city_code might be int or string depending on CSV parsing
    assert pd.api.types.is_integer_dtype(capitals_df["city_code"]) or pd.api.types.is_string_dtype(
        capitals_df["city_code"]
    )
    assert pd.api.types.is_string_dtype(capitals_df["municipio_nombre"])
    assert pd.api.types.is_numeric_dtype(capitals_df["latitude"])
    assert pd.api.types.is_numeric_dtype(capitals_df["longitude"])


@pytest.mark.unit
def test_get_coastal_cities(sample_github_csv):
    """Test get_coastal_cities() filters only coastal cities"""
    import src.weather_utils.city_utils as city_utils

    # Setup cache
    df = pd.read_csv(pd.io.common.StringIO(sample_github_csv))
    city_utils._cities_cache = df

    # Execute
    coastal_cities = get_coastal_cities()

    # Assert
    assert isinstance(coastal_cities, list)

    # Verify all returned cities are coastal
    for city in coastal_cities:
        assert "name" in city
        assert "lat" in city
        assert "lon" in city
        assert "code" in city

    # Count coastal cities in sample data (Barcelona, Málaga, Valencia)
    expected_coastal_count = sum(df["is_coastal"] == 1)
    assert len(coastal_cities) == expected_coastal_count

    # Verify Barcelona is included (is_coastal = 1)
    barcelona = next((c for c in coastal_cities if c["name"] == "Barcelona"), None)
    assert barcelona is not None

    # Verify Madrid is NOT included (is_coastal = 0)
    madrid = next((c for c in coastal_cities if c["name"] == "Madrid"), None)
    assert madrid is None


@pytest.mark.unit
def test_empty_dataframe_handling():
    """Test handling of empty DataFrame"""
    import src.weather_utils.city_utils as city_utils

    # Setup cache with empty DataFrame
    city_utils._cities_cache = pd.DataFrame()

    # Execute
    cities = get_cities()

    # Assert
    assert isinstance(cities, list)
    assert len(cities) == 0


@pytest.mark.unit
@responses.activate
def test_required_columns_validation(sample_github_csv):
    """Test that required columns are validated"""
    # Setup mock response with missing columns
    invalid_csv = """city_code,city_name
28079,Madrid
08019,Barcelona
"""

    responses.add(responses.GET, GITHUB_CSV_URL, body=invalid_csv, status=200)

    # Clear cache
    import src.weather_utils.city_utils as city_utils

    city_utils._cities_cache = None

    # Execute - should raise ValueError for missing columns
    with pytest.raises(ValueError, match="CSV missing required columns"):
        load_cities_from_github()


@pytest.mark.unit
def test_coordinates_are_numeric(sample_github_csv):
    """Test that latitude and longitude are properly converted to numeric"""
    import src.weather_utils.city_utils as city_utils

    # Setup cache
    df = pd.read_csv(pd.io.common.StringIO(sample_github_csv))
    city_utils._cities_cache = df

    # Execute
    cities = get_cities()

    # Assert all coordinates are numeric
    for city in cities:
        assert isinstance(city["lat"], (int, float))
        assert isinstance(city["lon"], (int, float))
        assert not pd.isna(city["lat"])
        assert not pd.isna(city["lon"])
