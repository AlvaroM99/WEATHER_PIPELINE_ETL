"""
Expectation suites for weather data validation.

Each function validates a specific data type against predefined rules
for schema, ranges, and completeness.
"""

import logging
from typing import Dict

import pandas as pd

from src.data_quality.exceptions import DataQualityException
from src.data_quality.validators import DataQualityValidator, ValidationResult

logger = logging.getLogger(__name__)


# =============================================================================
# Expectation Suite Configurations
# =============================================================================

# Weather observation valid ranges (based on meteorological standards)
WEATHER_OBSERVATION_CONFIG = {
    "required_columns": ["city", "temperature", "humidity", "pressure"],
    "numeric_ranges": {
        "temperature": (-60.0, 60.0),  # Celsius - extreme Earth temps
        "feels_like": (-80.0, 70.0),  # Feels like can be more extreme
        "temp_min": (-60.0, 60.0),
        "temp_max": (-60.0, 60.0),
        "humidity": (0.0, 100.0),  # Percentage
        "pressure": (870.0, 1084.0),  # hPa - recorded extremes
        "visibility": (0.0, 100000.0),  # meters
        "wind_speed": (0.0, 120.0),  # m/s - hurricane force max
        "wind_deg": (0.0, 360.0),  # degrees
        "wind_gust": (0.0, 150.0),  # m/s
        "clouds": (0.0, 100.0),  # percentage
    },
    "completeness_threshold": 0.90,
    "critical_columns": ["city", "temperature", "humidity"],
}

DAILY_FORECAST_CONFIG = {
    "required_columns": ["city_name", "time", "temperature_2m_max", "temperature_2m_min"],
    "numeric_ranges": {
        "temperature_2m_max": (-60.0, 60.0),
        "temperature_2m_min": (-60.0, 60.0),
        "apparent_temperature_max": (-80.0, 70.0),
        "apparent_temperature_min": (-80.0, 70.0),
        "precipitation_sum": (0.0, 500.0),  # mm/day extreme
        "rain_sum": (0.0, 500.0),
        "showers_sum": (0.0, 200.0),
        "snowfall_sum": (0.0, 200.0),  # cm
        "precipitation_hours": (0.0, 24.0),
        "wind_speed_10m_max": (0.0, 120.0),  # m/s
        "wind_gusts_10m_max": (0.0, 150.0),
        "wind_direction_10m_dominant": (0.0, 360.0),
        "shortwave_radiation_sum": (0.0, 50.0),  # MJ/m2
        "weather_code": (0.0, 99.0),  # WMO codes
        "et0_fao_evapotranspiration": (0.0, 20.0),  # mm/day
    },
    "completeness_threshold": 0.85,
    "critical_columns": ["city_name", "time", "temperature_2m_max", "temperature_2m_min"],
}

HOURLY_FORECAST_CONFIG = {
    "required_columns": ["city_name", "time", "temperature_2m"],
    "numeric_ranges": {
        "temperature_2m": (-60.0, 60.0),
        "temperature_80m": (-70.0, 50.0),
        "apparent_temperature": (-80.0, 70.0),
        "relative_humidity_2m": (0.0, 100.0),
        "dew_point_2m": (-80.0, 40.0),
        "precipitation_probability": (0.0, 100.0),
        "precipitation": (0.0, 100.0),  # mm/hour extreme
        "rain": (0.0, 100.0),
        "snowfall": (0.0, 50.0),  # cm/hour
        "pressure_msl": (870.0, 1084.0),
        "surface_pressure": (500.0, 1084.0),
        "cloud_cover": (0.0, 100.0),
        "visibility": (0.0, 100000.0),
        "uv_index": (0.0, 15.0),
        "wind_speed_10m": (0.0, 120.0),
        "wind_direction_10m": (0.0, 360.0),
        "wind_gusts_10m": (0.0, 150.0),
        "weather_code": (0.0, 99.0),
    },
    "completeness_threshold": 0.80,
    "critical_columns": ["city_name", "time", "temperature_2m"],
}

AIR_QUALITY_CONFIG = {
    "required_columns": ["city_name", "time"],
    "numeric_ranges": {
        "pm10": (0.0, 1000.0),  # ug/m3
        "pm2_5": (0.0, 500.0),  # ug/m3
        "carbon_monoxide": (0.0, 50000.0),  # ug/m3
        "nitrogen_dioxide": (0.0, 1000.0),  # ug/m3
        "sulphur_dioxide": (0.0, 1000.0),  # ug/m3
        "ozone": (0.0, 500.0),  # ug/m3
        "aerosol_optical_depth": (0.0, 5.0),  # dimensionless
        "dust": (0.0, 2000.0),  # ug/m3
    },
    "completeness_threshold": 0.70,  # Air quality often has missing sensors
    "critical_columns": ["city_name", "time"],
}

POLLEN_CONFIG = {
    "required_columns": ["city_name", "time"],
    "numeric_ranges": {
        "alder_pollen": (0.0, 500.0),  # grains/m3
        "birch_pollen": (0.0, 500.0),
        "grass_pollen": (0.0, 500.0),
        "mugwort_pollen": (0.0, 500.0),
        "olive_pollen": (0.0, 500.0),
        "ragweed_pollen": (0.0, 500.0),
    },
    "completeness_threshold": 0.60,  # Pollen data varies by season
    "critical_columns": ["city_name", "time"],
}

MARINE_CONFIG = {
    "required_columns": ["city_name", "time"],
    "numeric_ranges": {
        "wave_height_max": (0.0, 30.0),  # meters - extreme waves
        "wave_direction_dominant": (0.0, 360.0),  # degrees
        "wave_period_max": (0.0, 30.0),  # seconds
        "wind_wave_height_max": (0.0, 20.0),
        "swell_wave_height_max": (0.0, 25.0),
    },
    "completeness_threshold": 0.80,
    "critical_columns": ["city_name", "time"],
}


# =============================================================================
# Validation Functions
# =============================================================================


def _run_validation(
    df: pd.DataFrame, suite_name: str, config: Dict[str, Any], strict_mode: bool = False
) -> ValidationResult:
    """
    Run validation against a DataFrame using the specified configuration.

    Args:
        df: DataFrame to validate
        suite_name: Name of the expectation suite
        config: Configuration dictionary
        strict_mode: If True, raise exception on failure

    Returns:
        ValidationResult with detailed results
    """
    validator = DataQualityValidator(strict_mode=strict_mode)

    result = validator.run_validation(
        df=df,
        table_name=suite_name,
        required_columns=config["required_columns"],
        numeric_ranges=config.get("numeric_ranges"),
        completeness_columns=config.get("critical_columns"),
        completeness_threshold=config.get("completeness_threshold", 0.95),
        mostly=0.99,  # Allow 1% outliers for range checks
    )

    # Log summary
    if result.success:
        logger.info(
            f"[{suite_name}] Validation PASSED: {result.successful_expectations}/{result.total_expectations} checks passed"
        )
    else:
        logger.warning(
            f"[{suite_name}] Validation FAILED: {result.failed_expectations}/{result.total_expectations} checks failed"
        )
        for detail in result.failed_details[:5]:
            logger.warning(f"  - {detail.get('expectation_type')}: {detail.get('column', 'N/A')}")

    return result


# =============================================================================
# Public Validation Functions
# =============================================================================


def validate_weather_observation(df: pd.DataFrame, strict_mode: bool = False) -> ValidationResult:
    """
    Validate weather observation data (OpenWeather current conditions).

    Checks:
    - Required columns exist: city, temperature, humidity, pressure
    - Temperature between -60 and 60 C
    - Humidity between 0 and 100%
    - Pressure between 870 and 1084 hPa
    - Wind speed between 0 and 120 m/s
    - Critical columns have >90% completeness

    Args:
        df: DataFrame with weather observation data
        strict_mode: If True, raise exception on validation failure

    Returns:
        ValidationResult with detailed validation results
    """
    return _run_validation(
        df,
        suite_name="weather_observation_suite",
        config=WEATHER_OBSERVATION_CONFIG,
        strict_mode=strict_mode,
    )


def validate_daily_forecast(df: pd.DataFrame, strict_mode: bool = False) -> ValidationResult:
    """
    Validate daily weather forecast data (Open-Meteo daily).

    Checks:
    - Required columns exist: city_name, time, temperature_2m_max, temperature_2m_min
    - Temperature ranges valid
    - Precipitation values non-negative
    - Wind speeds reasonable
    - Weather codes within WMO range (0-99)

    Args:
        df: DataFrame with daily forecast data
        strict_mode: If True, raise exception on validation failure

    Returns:
        ValidationResult with detailed validation results
    """
    return _run_validation(
        df, suite_name="daily_forecast_suite", config=DAILY_FORECAST_CONFIG, strict_mode=strict_mode
    )


def validate_hourly_forecast(df: pd.DataFrame, strict_mode: bool = False) -> ValidationResult:
    """
    Validate hourly weather forecast data (Open-Meteo hourly).

    Checks:
    - Required columns exist: city_name, time, temperature_2m
    - Temperature values within physical limits
    - Relative humidity 0-100%
    - Precipitation probability 0-100%
    - UV index 0-15

    Args:
        df: DataFrame with hourly forecast data
        strict_mode: If True, raise exception on validation failure

    Returns:
        ValidationResult with detailed validation results
    """
    return _run_validation(
        df,
        suite_name="hourly_forecast_suite",
        config=HOURLY_FORECAST_CONFIG,
        strict_mode=strict_mode,
    )


def validate_air_quality(df: pd.DataFrame, strict_mode: bool = False) -> ValidationResult:
    """
    Validate air quality data.

    Checks:
    - Required columns exist: city_name, time
    - PM10 and PM2.5 within sensor limits
    - Gas concentrations within expected ranges
    - Aerosol optical depth reasonable

    Args:
        df: DataFrame with air quality data
        strict_mode: If True, raise exception on validation failure

    Returns:
        ValidationResult with detailed validation results
    """
    return _run_validation(
        df, suite_name="air_quality_suite", config=AIR_QUALITY_CONFIG, strict_mode=strict_mode
    )


def validate_pollen(df: pd.DataFrame, strict_mode: bool = False) -> ValidationResult:
    """
    Validate pollen data.

    Checks:
    - Required columns exist: city_name, time
    - Pollen counts non-negative
    - Pollen counts within reasonable maximums

    Args:
        df: DataFrame with pollen data
        strict_mode: If True, raise exception on validation failure

    Returns:
        ValidationResult with detailed validation results
    """
    return _run_validation(
        df, suite_name="pollen_suite", config=POLLEN_CONFIG, strict_mode=strict_mode
    )


def validate_marine(df: pd.DataFrame, strict_mode: bool = False) -> ValidationResult:
    """
    Validate marine weather data.

    Checks:
    - Required columns exist: city_name, time
    - Wave heights within physical limits
    - Wave direction 0-360 degrees
    - Wave period reasonable

    Args:
        df: DataFrame with marine weather data
        strict_mode: If True, raise exception on validation failure

    Returns:
        ValidationResult with detailed validation results
    """
    return _run_validation(
        df, suite_name="marine_suite", config=MARINE_CONFIG, strict_mode=strict_mode
    )


# =============================================================================
# Convenience Function
# =============================================================================


def validate_dataframe(
    df: pd.DataFrame, data_type: str, strict_mode: bool = False
) -> ValidationResult:
    """
    Validate a DataFrame based on its data type.

    Args:
        df: DataFrame to validate
        data_type: One of 'observation', 'daily_forecast', 'hourly_forecast',
                   'air_quality', 'pollen', 'marine'
        strict_mode: If True, raise exception on validation failure

    Returns:
        ValidationResult with detailed validation results

    Raises:
        ValueError: If data_type is not recognized
    """
    validators = {
        "observation": validate_weather_observation,
        "daily_forecast": validate_daily_forecast,
        "hourly_forecast": validate_hourly_forecast,
        "air_quality": validate_air_quality,
        "pollen": validate_pollen,
        "marine": validate_marine,
    }

    if data_type not in validators:
        raise ValueError(f"Unknown data type: {data_type}. Valid types: {list(validators.keys())}")

    return validators[data_type](df, strict_mode=strict_mode)
