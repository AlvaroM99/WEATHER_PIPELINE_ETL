"""
City Utilities

Type-annotated module with helper functions for working with city data.
"""

from __future__ import annotations

import os
import sys
from io import StringIO
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.type_aliases import CityDict

# GitHub URL for raw_cities.csv
CITIES_CSV_URL: str = (
    "https://raw.githubusercontent.com/AlvaroM99/spanish_capital_cities/main/raw_cities.csv"
)

# Cache for cities data (loaded once at startup)
_cities_cache: Optional[pd.DataFrame] = None


def load_cities_from_github() -> pd.DataFrame:
    """
    Download and parse cities CSV from GitHub repository.

    Returns:
        DataFrame with columns: city_code, city_name, latitud, longitud, country_code, is_coastal

    Raises:
        requests.RequestException: If download fails
        pd.errors.ParserError: If CSV parsing fails
    """
    global _cities_cache

    # Return cached data if available
    if _cities_cache is not None:
        return _cities_cache.copy()

    try:
        print("📥 Downloading cities data from GitHub...")
        response: requests.Response = requests.get(CITIES_CSV_URL, timeout=10)
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

    except requests.RequestException as e:
        print(f"❌ Error downloading cities CSV from GitHub: {e}")
        raise
    except Exception as e:
        print(f"❌ Error parsing cities CSV: {e}")
        raise


def get_capitals_dataframe() -> pd.DataFrame:
    """
    Load cities from GitHub CSV and convert to DataFrame format expected by extraction scripts.

    Returns:
        DataFrame with city_code, municipio_nombre, latitude, longitude
    """
    # Load cities from GitHub
    df: pd.DataFrame = load_cities_from_github()

    # Convert to expected format for extractors
    return pd.DataFrame(
        [
            {
                "city_code": row["city_code"],
                "municipio_nombre": row["city_name"],
                "latitude": row["latitud"],
                "longitude": row["longitud"],
            }
            for _, row in df.iterrows()
        ]
    )


def get_cities() -> List[CityDict]:
    """
    Get all cities from GitHub CSV in format compatible with OpenWeather extractor.

    Returns:
        List of city dictionaries with 'name', 'lat', 'lon' keys
    """
    # Load cities from GitHub
    df: pd.DataFrame = load_cities_from_github()

    # Convert to expected format for OpenWeather
    cities: List[CityDict] = []
    for _, row in df.iterrows():
        cities.append({"name": row["city_name"], "lat": row["latitud"], "lon": row["longitud"]})

    return cities


def get_coastal_cities() -> List[CityDict]:
    """
    Get list of coastal cities for marine data extraction using is_coastal field from CSV.

    Returns:
        List of coastal city dictionaries with coordinates
    """
    # Load cities from GitHub
    df_raw: pd.DataFrame = load_cities_from_github()

    # Filter for coastal cities (is_coastal == 1)
    coastal_df: pd.DataFrame = df_raw[df_raw["is_coastal"] == 1]

    # Convert to expected format
    coastal_cities: List[CityDict] = []
    for _, row in coastal_df.iterrows():
        coastal_cities.append(
            {
                "name": row["city_name"],
                "lat": row["latitud"],
                "lon": row["longitud"],
                "code": row["city_code"],
            }
        )

    return coastal_cities
