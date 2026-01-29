"""
Data Quality Framework for Weather Pipeline ETL.

This module provides data quality validation using pandas-based checks,
including schema validation, range checks, completeness metrics, and
anomaly detection.

Usage:
    from src.data_quality import validate_weather_observation, DataQualityException

    # Validate a DataFrame
    result = validate_weather_observation(df, strict_mode=False)

    if not result.success:
        print(f"Validation failed: {result.failed_details}")
"""

from src.data_quality.exceptions import (
    AnomalyDetectionError,
    CompletenessError,
    DataQualityException,
    ValidationError,
)
from src.data_quality.expectations import (
    validate_air_quality,
    validate_daily_forecast,
    validate_dataframe,
    validate_hourly_forecast,
    validate_marine,
    validate_pollen,
    validate_weather_observation,
)
from src.data_quality.metrics import (
    DataQualityMetrics,
    QualityMetric,
    QualityReport,
)
from src.data_quality.validators import (
    DataQualityValidator,
    ValidationResult,
)

__all__ = [
    # Exceptions
    "DataQualityException",
    "ValidationError",
    "AnomalyDetectionError",
    "CompletenessError",
    # Validators
    "DataQualityValidator",
    "ValidationResult",
    # Expectation functions
    "validate_weather_observation",
    "validate_daily_forecast",
    "validate_hourly_forecast",
    "validate_air_quality",
    "validate_pollen",
    "validate_marine",
    "validate_dataframe",
    # Metrics
    "DataQualityMetrics",
    "QualityMetric",
    "QualityReport",
]
