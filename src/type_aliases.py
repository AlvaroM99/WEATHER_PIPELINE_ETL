"""
Centralized Type Aliases

Shared type definitions used across the weather pipeline ETL modules.
This reduces duplication and ensures consistent typing throughout the codebase.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Tuple

import pandas as pd

# ============================================================================
# Airflow Types
# ============================================================================
AirflowContext = Dict[str, Any]
"""Airflow task context dictionary passed to Python operators."""

# ============================================================================
# City/Location Types
# ============================================================================
CityDict = Dict[str, Any]
"""Dictionary representing a city with name, lat, lon coordinates."""

CityIdMapping = Dict[str, int]
"""Mapping of city names to their database IDs."""

# ============================================================================
# Data Lake Types (Bronze/Silver layers)
# ============================================================================
BronzeObject = Dict[str, Any]
"""Raw JSON object from the Bronze layer (API responses)."""

UploadedObject = Dict[str, Any]
"""Metadata about an object uploaded to MinIO."""

TransformedRecord = Dict[str, Any]
"""A single transformed record ready for Silver layer."""

# ============================================================================
# Loading Types (Silver → Gold)
# ============================================================================
RecordTuple = Tuple[Any, ...]
"""A tuple representing a database record for bulk insertion."""

MapperFunction = Callable[[pd.Series, int, int], RecordTuple]
"""Function that maps a DataFrame row to a database record tuple.

Args:
    row: A pandas Series representing one row of data
    city_id: The foreign key ID for the city dimension
    time_id: The foreign key ID for the time dimension

Returns:
    A tuple of values ready for database insertion
"""

# ============================================================================
# API Response Types
# ============================================================================
APIResponse = Dict[str, Any]
"""Generic API response dictionary."""

WeatherData = Dict[str, Any]
"""Weather data structure from any weather API."""

# ============================================================================
# Database Types
# ============================================================================
DBRow = Dict[str, Any]
"""A single row from a database query result."""

DBRows = List[DBRow]
"""Multiple rows from a database query result."""
