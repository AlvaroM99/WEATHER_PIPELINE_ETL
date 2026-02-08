"""
City Utilities

Type-annotated module with helper functions for working with city data.
"""

from __future__ import annotations

import threading
from io import StringIO
from typing import List, Optional

import pandas as pd

from src.type_aliases import CityDict
from src.utils.http_utils import get_retrying_session

# GitHub URL for raw_cities.csv
CITIES_CSV_URL: str = (
    "https://raw.githubusercontent.com/AlvaroM99/spanish_capital_cities/main/raw_cities.csv"
)

# Thread-safe cache implementation
_cache_lock: threading.Lock = threading.Lock()
_cities_cache: Optional[pd.DataFrame] = None


def clear_cities_cache() -> None:
    """
    Clear the cities cache. Thread-safe.

    Primarily used in tests to reset cache state between test runs.
    """
    global _cities_cache
    with _cache_lock:
        _cities_cache = None


def set_cities_cache(df: Optional[pd.DataFrame]) -> None:
    """
    Set the cities cache directly. Thread-safe.

    Primarily used in tests to inject mock data without HTTP calls.

    Args:
        df: DataFrame to cache, or None to clear cache
    """
    global _cities_cache
    with _cache_lock:
        _cities_cache = df


def load_cities_from_github() -> pd.DataFrame:
    """
    Download and parse cities CSV from GitHub repository.

    Thread-safe with caching. Uses retry logic for resilient HTTP requests.

    Returns:
        DataFrame with columns: city_code, city_name, latitud, longitud, country_code, is_coastal

    Raises:
        requests.RequestException: If download fails after retries
        pd.errors.ParserError: If CSV parsing fails
    """
    global _cities_cache

    # Fast path: check cache without lock
    if _cities_cache is not None:
        return _cities_cache.copy()

    with _cache_lock:
        if _cities_cache is not None:
            return _cities_cache.copy()

        try:
            print("📥 Downloading cities data from GitHub...")
            session = get_retrying_session()
            response = session.get(CITIES_CSV_URL, timeout=10)
            response.raise_for_status()

            # Parse CSV
            csv_data: StringIO = StringIO(response.text)
            df: pd.DataFrame = pd.read_csv(csv_data, sep=",")

            # Validate required columns
            required_cols: List[str] = [
                "city_code",
                "city_name",
                "latitud",
                "longitud",
                "country_code",
                "is_coastal",
            ]
            if not all(col in df.columns for col in required_cols):
                raise ValueError(
                    f"CSV missing required columns. Expected: {required_cols}, Got: {df.columns.tolist()}"
                )

            # Cache the data
            _cities_cache = df

            print(f"✅ Successfully loaded {len(df)} cities from GitHub")
            return df.copy()

        except Exception as e:
            print(f"❌ Error loading cities CSV: {e}")
            raise


def get_capitals_dataframe() -> pd.DataFrame:
    """
    Load cities from GitHub CSV and convert to DataFrame format expected by extraction scripts.

    Returns:
        DataFrame with city_code, municipio_nombre, latitude, longitude
    """
    df: pd.DataFrame = load_cities_from_github()

    # Vectorized column rename and selection
    return df.rename(
        columns={
            "city_name": "municipio_nombre",
            "latitud": "latitude",
            "longitud": "longitude",
        }
    )[["city_code", "municipio_nombre", "latitude", "longitude"]].copy()


def get_cities() -> List[CityDict]:
    """
    Get all cities from GitHub CSV in format compatible with OpenWeather extractor.

    Returns:
        List of city dictionaries with 'name', 'lat', 'lon' keys
    """
    df: pd.DataFrame = load_cities_from_github()

    # Handle empty DataFrame
    if df.empty:
        return []

    # Vectorized conversion to list of dicts
    result: list[dict[str, Any]] = df.rename(columns={"city_name": "name", "latitud": "lat", "longitud": "lon"})[
        ["name", "lat", "lon"]
    ].to_dict("records")  # type: ignore[assignment]
    return result


def get_coastal_cities() -> List[CityDict]:
    """
    Get list of coastal cities for marine data extraction using is_coastal field from CSV.

    Returns:
        List of coastal city dictionaries with coordinates
    """
    df_raw: pd.DataFrame = load_cities_from_github()

    # Filter for coastal cities and vectorized conversion
    coastal_df: pd.DataFrame = df_raw[df_raw["is_coastal"] == 1]

    result: list[dict[str, Any]] = coastal_df.rename(
        columns={
            "city_name": "name",
            "latitud": "lat",
            "longitud": "lon",
            "city_code": "code",
        }
    )[["name", "lat", "lon", "code"]].to_dict("records")  # type: ignore[assignment]
    return result
