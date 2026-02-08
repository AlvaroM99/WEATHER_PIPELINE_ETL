"""
Unified Loader
Consolidates all loading logic into a single class.
Includes data quality validation using Great Expectations (optional).

Type-annotated module for loading data into the data warehouse (Silver → Gold).
"""

from __future__ import annotations

import logging
from datetime import datetime
from datetime import time as dt_time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from src.base_loader import BaseLoader
from src.config.lake_config import (
    SILVER_AEMET_BUCKET,
    SILVER_OPENWEATHER_BUCKET,
    SILVER_PATH_TEMPLATE,
)
from src.dimensional_loader import DimensionalLoader
from src.type_aliases import AirflowContext, CityIdMapping, MapperFunction, RecordTuple
from src.utils.minio_client import MinIOClient, get_minio_client

# Data quality imports (optional - graceful degradation if not installed)
DATA_QUALITY_AVAILABLE = False
try:
    from src.data_quality.exceptions import DataQualityException
    from src.data_quality.expectations import (
        validate_air_quality,
        validate_daily_forecast,
        validate_hourly_forecast,
        validate_marine,
        validate_pollen,
        validate_weather_observation,
    )
    from src.data_quality.metrics import DataQualityMetrics
    from src.data_quality.validators import ValidationResult

    DATA_QUALITY_AVAILABLE = True
except ImportError as e:
    # Great Expectations not installed - validation will be disabled
    DataQualityException = Exception  # type: ignore[misc,assignment]
    DataQualityMetrics = None  # type: ignore[misc,assignment]
    ValidationResult = None  # type: ignore[misc,assignment]
    logging.getLogger(__name__).warning(
        f"Data quality module not available: {e}. "
        "Install great_expectations to enable validation."
    )


class Loader(BaseLoader):
    """
    Unified Loader Manager.

    Handles loading for all fact tables with integrated data quality validation.

    Attributes:
        logger: Logger instance for this class (via BaseLoader)
        minio_client: MinIO client for data lake operations
        enable_validation: Whether data validation is enabled
        strict_validation: Whether to raise exceptions on validation failure
        collect_metrics: Whether to collect quality metrics
        quality_metrics: Quality metrics collector instance
    """

    def __init__(
        self,
        enable_validation: bool = True,
        strict_validation: bool = False,
        collect_metrics: bool = True,
    ) -> None:
        """
        Initialize the Loader.

        Args:
            enable_validation: If True, validates data before loading
                              (requires great_expectations to be installed)
            strict_validation: If True, raises exception on validation failure
                              If False, logs warnings but continues loading
            collect_metrics: If True, collects quality metrics for reporting
        """
        super().__init__()
        self.minio_client: MinIOClient = get_minio_client()

        # Data quality configuration (only if module is available)
        self.enable_validation: bool = enable_validation and DATA_QUALITY_AVAILABLE
        self.strict_validation: bool = strict_validation
        self.collect_metrics: bool = collect_metrics and DATA_QUALITY_AVAILABLE

        if enable_validation and not DATA_QUALITY_AVAILABLE:
            self.logger.warning(
                "Data quality validation requested but great_expectations not installed. "
                "Validation will be skipped."
            )

        self.quality_metrics: Optional[Any] = (
            DataQualityMetrics() if self.collect_metrics and DATA_QUALITY_AVAILABLE else None
        )
        self._last_validation_results: Dict[str, Any] = {}

    # ========================================================================
    # Data Quality Validation Methods
    # ========================================================================

    def validate_data(
        self, df: pd.DataFrame, data_type: str, table_name: str
    ) -> Optional[ValidationResult]:
        """
        Validate a DataFrame before loading.

        Args:
            df: DataFrame to validate
            data_type: Type of data ('observation', 'daily_forecast', etc.)
            table_name: Name of target table (for logging/metrics)

        Returns:
            ValidationResult if validation was run, None if disabled

        Raises:
            DataQualityException: If strict_validation is True and validation fails
        """
        if not self.enable_validation or not DATA_QUALITY_AVAILABLE:
            return None

        validators = (
            {
                "observation": validate_weather_observation,
                "daily_forecast": validate_daily_forecast,
                "hourly_forecast": validate_hourly_forecast,
                "air_quality": validate_air_quality,
                "pollen": validate_pollen,
                "marine": validate_marine,
            }
            if DATA_QUALITY_AVAILABLE
            else {}
        )

        validator_func = validators.get(data_type)
        if not validator_func:
            self.logger.warning(f"No validator found for data type: {data_type}")
            return None

        try:
            result = validator_func(df, strict_mode=self.strict_validation)
            self._last_validation_results[table_name] = result

            # Log validation summary
            if result.success:
                self.logger.info(
                    f"✅ Data quality check PASSED for {table_name}: "
                    f"{result.successful_expectations}/{result.total_expectations} expectations met"
                )
            else:
                self.logger.warning(
                    f"⚠️ Data quality check FAILED for {table_name}: "
                    f"{result.failed_expectations}/{result.total_expectations} expectations failed"
                )
                for detail in result.failed_details[:3]:
                    self.logger.warning(f"   - {detail.get('expectation_type')}")

            # Collect metrics if enabled
            if self.collect_metrics and self.quality_metrics:
                key_columns = (
                    ["city_name", "time"] if "city_name" in df.columns else ["city", "date"]
                )
                timestamp_col = "time" if "time" in df.columns else None
                report = self.quality_metrics.generate_report(
                    df, table_name, result, key_columns=key_columns, timestamp_column=timestamp_col
                )
                self.logger.info(f"📊 Quality score for {table_name}: {report.overall_score:.2%}")

            return result

        except DataQualityException:
            raise
        except Exception as e:
            self.logger.error(f"Error during validation: {e}")
            if self.strict_validation:
                raise DataQualityException(f"Validation error for {table_name}: {e}")
            return None

    def get_validation_result(self, table_name: str) -> Optional[Any]:
        """Get the last validation result for a table."""
        return self._last_validation_results.get(table_name)

    def get_quality_report(self) -> Dict[str, Any]:
        """Get a summary of all validation results."""
        return {table: result.to_dict() for table, result in self._last_validation_results.items()}

    def get_city_id_mapping(
        self, conn: psycopg2.extensions.connection
    ) -> Tuple[CityIdMapping, CityIdMapping]:
        """
        Get city ID mappings from the database.

        Args:
            conn: Database connection

        Returns:
            Tuple of (name_map, code_map) dictionaries
        """
        cur: psycopg2.extensions.cursor = conn.cursor()
        cur.execute("SELECT city_name, city_id FROM dwh.dim_city")
        name_map: CityIdMapping = {row[0]: row[1] for row in cur.fetchall()}

        cur.execute("SELECT city_code, city_id FROM dwh.dim_city")
        code_map: CityIdMapping = {row[0]: row[1] for row in cur.fetchall()}

        cur.close()
        return name_map, code_map

    def get_date_id(self, date_str: Any) -> Optional[int]:
        """
        Convert a date string to a date ID (YYYYMMDD format).

        Args:
            date_str: Date string or datetime object

        Returns:
            Integer date ID or None if conversion fails
        """
        try:
            return int(str(date_str)[:10].replace("-", ""))
        except ValueError:
            return None

    def clean_value(self, value: Any) -> Any:
        """
        Convert NaN/NaT to None for database insertion.

        Args:
            value: Value to clean

        Returns:
            None if value is NaN/NaT, otherwise the original value
        """
        if pd.isna(value):
            return None
        return value

    # ========================================================================
    # Fact Observation Load
    # ========================================================================

    def load_fact_observation(self, **context: Any) -> int:
        """
        Load weather observations to dwh.fct_weather_observation.

        Args:
            context: Airflow context containing execution date

        Returns:
            Number of records inserted
        """
        self.log_start("Loading fct_weather_observation")
        
        # Extract DAG context for ETL logging
        dag_id = context.get("dag", {}).get("dag_id", "loading_pipeline")
        task_id = context.get("task_instance", {}).get("task_id", "load_fact_observation")
        execution_date = context.get("ds", datetime.now().strftime("%Y-%m-%d"))

        with self.connection() as conn:
            # Start ETL run logging
            run_id = self.log_etl_start(dag_id, task_id, execution_date, conn)
            
            try:
                name_map, _ = self.get_city_id_mapping(conn)
                date_id = self.get_date_id(execution_date)

                object_path = SILVER_PATH_TEMPLATE.format(date=execution_date)
                try:
                    df = self.minio_client.read_parquet(SILVER_OPENWEATHER_BUCKET, object_path)
                except Exception:
                    self.logger.warning(f"No data found for {execution_date}")
                    self.log_etl_end(run_id, 0, conn, status="success")
                    return 0

                # Validate data quality before loading
                self.validate_data(df, "observation", "fct_weather_observation")

                records = []
                for _, row in df.iterrows():
                    city_id = name_map.get(row.get("city"))
                    if not city_id:
                        continue

                    # Handle timestamp - use date if dt not available
                    obs_timestamp = (
                        pd.to_datetime(row.get("dt"), unit="s")
                        if "dt" in row and pd.notna(row.get("dt"))
                        else datetime.now()
                    )

                    # Clean all values to convert NaN to None
                    records.append(
                        (
                            city_id,
                            date_id,
                            obs_timestamp,
                            self.clean_value(row.get("temperature")),
                            self.clean_value(row.get("feels_like")),
                            self.clean_value(row.get("temp_min")),
                            self.clean_value(row.get("temp_max")),
                            self.clean_value(row.get("pressure")),
                            self.clean_value(row.get("humidity")),
                            self.clean_value(row.get("visibility")),
                            self.clean_value(row.get("wind_speed")),
                            self.clean_value(row.get("wind_deg")),
                            self.clean_value(row.get("wind_gust")),
                            self.clean_value(row.get("clouds")),
                            self.clean_value(row.get("weather_id")),
                            row.get("weather_main"),
                            row.get("weather_description"),
                            self.clean_value(row.get("rain_1h")),
                            self.clean_value(row.get("rain_3h")),
                            self.clean_value(row.get("snow_1h")),
                            self.clean_value(row.get("snow_3h")),
                        )
                    )

                if not records:
                    self.log_etl_end(run_id, 0, conn, status="success")
                    return 0

                insert_query = """
                    INSERT INTO dwh.fct_weather_observation (
                        city_id, date_id, observation_timestamp,
                        temperature, feels_like, temp_min, temp_max,
                        pressure, humidity, visibility,
                        wind_speed, wind_deg, wind_gust,
                        cloudiness, weather_code, weather_main, weather_description,
                        rain_1h, rain_3h, snow_1h, snow_3h
                    ) VALUES %s ON CONFLICT DO NOTHING
                """

                cur = conn.cursor()
                execute_values(cur, insert_query, records)
                rowcount: int = cur.rowcount if cur.rowcount is not None else 0
                self.log_end(f"Inserted {rowcount} records")
                cur.close()
                
                # End ETL run logging with success
                self.log_etl_end(run_id, rowcount, conn, status="success")
                return rowcount
            except Exception as e:
                # End ETL run logging with failure
                self.log_etl_end(run_id, 0, conn, status="failed", error_message=str(e))
                self.log_error("Error loading observation", e)
                raise

    # ========================================================================
    # Fact Forecast Daily Load
    # ========================================================================

    def load_fact_forecast_daily(self, **context: Any) -> int:
        """
        Load daily forecast to dwh.fct_weather_forecast.

        Args:
            context: Airflow context containing execution date

        Returns:
            Number of records inserted
        """
        self.log_start("Loading fct_weather_forecast")
        return self._load_generic(
            context,
            bucket="silver-openmeteo",
            prefix_template="forecast/daily/{execution_date}/",
            insert_query="""
                INSERT INTO dwh.fct_weather_forecast (
                    city_id, forecast_date_id, extraction_date_id,
                    temp_max, temp_min, apparent_temp_max, apparent_temp_min,
                    precipitation_sum, rain_sum, showers_sum, snowfall_sum, precipitation_hours,
                    wind_speed_max, wind_gusts_max, wind_direction_dominant,
                    sunrise, sunset, shortwave_radiation_sum,
                    weather_code, et0_fao_evapotranspiration,
                    created_at
                ) VALUES %s
                ON CONFLICT (city_id, forecast_date_id, extraction_date_id) DO NOTHING
            """,
            mapper=self._map_daily_forecast,
            data_type="daily_forecast",
            table_name="fct_weather_forecast",
        )

    def _map_daily_forecast(
        self, row: pd.Series, city_id: int, extraction_date_id: int
    ) -> RecordTuple:
        """Map a daily forecast row to a database record tuple."""
        forecast_date: str = pd.to_datetime(row.get("time")).strftime("%Y-%m-%d")
        return (
            city_id,
            self.get_date_id(forecast_date),
            extraction_date_id,
            row.get("temperature_2m_max"),
            row.get("temperature_2m_min"),
            row.get("apparent_temperature_max"),
            row.get("apparent_temperature_min"),
            row.get("precipitation_sum"),
            row.get("rain_sum"),
            row.get("showers_sum"),
            row.get("snowfall_sum"),
            row.get("precipitation_hours"),
            row.get("wind_speed_10m_max"),
            row.get("wind_gusts_10m_max"),
            row.get("wind_direction_10m_dominant"),
            pd.to_datetime(row.get("sunrise")).time() if pd.notna(row.get("sunrise")) else None,
            pd.to_datetime(row.get("sunset")).time() if pd.notna(row.get("sunset")) else None,
            row.get("shortwave_radiation_sum"),
            row.get("weather_code"),
            row.get("et0_fao_evapotranspiration"),
        )

    # ========================================================================
    # Fact Forecast Hourly Load
    # ========================================================================

    def load_fact_forecast_hourly(self, **context: Any) -> int:
        """
        Load hourly forecast to dwh.fct_weather_forecast_hourly.

        Args:
            context: Airflow context containing execution date

        Returns:
            Number of records inserted
        """
        self.log_start("Loading fct_weather_forecast_hourly")
        return self._load_generic(
            context,
            bucket="silver-openmeteo",
            prefix_template="forecast/hourly/{execution_date}/",
            insert_query="""
                INSERT INTO dwh.fct_weather_forecast_hourly (
                    city_id, forecast_datetime, extraction_date_id,
                    temp_2m, temp_80m, apparent_temperature,
                    relative_humidity_2m, dew_point_2m,
                    precipitation_probability, precipitation, rain, snowfall,
                    pressure_msl, surface_pressure,
                    cloud_cover, visibility, uv_index,
                    wind_speed_10m, wind_direction_10m, wind_gusts_10m,
                    weather_code, created_at
                ) VALUES %s
                ON CONFLICT (city_id, forecast_datetime, extraction_date_id) DO NOTHING
            """,
            mapper=self._map_hourly_forecast,
            data_type="hourly_forecast",
            table_name="fct_weather_forecast_hourly",
        )

    def _map_hourly_forecast(
        self, row: pd.Series, city_id: int, extraction_date_id: int
    ) -> RecordTuple:
        """Map an hourly forecast row to a database record tuple."""
        return (
            city_id,
            pd.to_datetime(row.get("time")),
            extraction_date_id,
            row.get("temperature_2m"),
            row.get("temperature_80m"),
            row.get("apparent_temperature"),
            row.get("relative_humidity_2m"),
            row.get("dew_point_2m"),
            row.get("precipitation_probability"),
            row.get("precipitation"),
            row.get("rain"),
            row.get("snowfall"),
            row.get("pressure_msl"),
            row.get("surface_pressure"),
            row.get("cloud_cover"),
            row.get("visibility"),
            row.get("uv_index"),
            row.get("wind_speed_10m"),
            row.get("wind_direction_10m"),
            row.get("wind_gusts_10m"),
            row.get("weather_code"),
        )

    # ========================================================================
    # Fact Air Quality Load
    # ========================================================================

    def load_fact_air_quality(self, **context: Any) -> int:
        """
        Load air quality to dwh.fct_air_quality.

        Args:
            context: Airflow context containing execution date

        Returns:
            Number of records inserted
        """
        self.log_start("Loading fct_air_quality")
        return self._load_generic(
            context,
            bucket="silver-openmeteo",
            prefix_template="air_quality/{execution_date}/",
            insert_query="""
                INSERT INTO dwh.fct_air_quality (
                    city_id, measurement_datetime, extraction_date_id,
                    pm10, pm2_5, carbon_monoxide, nitrogen_dioxide,
                    sulphur_dioxide, ozone, aerosol_optical_depth, dust,
                    created_at
                ) VALUES %s
                ON CONFLICT (city_id, measurement_datetime, extraction_date_id) DO NOTHING
            """,
            mapper=self._map_air_quality,
            data_type="air_quality",
            table_name="fct_air_quality",
        )

    def _map_air_quality(
        self, row: pd.Series, city_id: int, extraction_date_id: int
    ) -> RecordTuple:
        """Map an air quality row to a database record tuple."""

        def g(k: str) -> Optional[Any]:
            return row.get(k) if pd.notna(row.get(k)) else None

        return (
            city_id,
            pd.to_datetime(row.get("time")),
            extraction_date_id,
            g("pm10"),
            g("pm2_5"),
            g("carbon_monoxide"),
            g("nitrogen_dioxide"),
            g("sulphur_dioxide"),
            g("ozone"),
            g("aerosol_optical_depth"),
            g("dust"),
        )

    # ========================================================================
    # Fact Pollen Load
    # ========================================================================

    def load_fact_pollen(self, **context: Any) -> int:
        """
        Load pollen to dwh.fct_pollen.

        Args:
            context: Airflow context containing execution date

        Returns:
            Number of records inserted
        """
        self.log_start("Loading fct_pollen")
        return self._load_generic(
            context,
            bucket="silver-openmeteo",
            prefix_template="pollen/{execution_date}/",
            insert_query="""
                INSERT INTO dwh.fct_pollen (
                    city_id, measurement_datetime, extraction_date_id,
                    alder_pollen, birch_pollen, grass_pollen,
                    mugwort_pollen, olive_pollen, ragweed_pollen,
                    created_at
                ) VALUES %s
                ON CONFLICT (city_id, measurement_datetime, extraction_date_id) DO NOTHING
            """,
            mapper=self._map_pollen,
            data_type="pollen",
            table_name="fct_pollen",
        )

    def _map_pollen(self, row: pd.Series, city_id: int, extraction_date_id: int) -> RecordTuple:
        """Map a pollen row to a database record tuple."""

        def g(k: str) -> Optional[Any]:
            return row.get(k) if pd.notna(row.get(k)) else None

        return (
            city_id,
            pd.to_datetime(row.get("time")),
            extraction_date_id,
            g("alder_pollen"),
            g("birch_pollen"),
            g("grass_pollen"),
            g("mugwort_pollen"),
            g("olive_pollen"),
            g("ragweed_pollen"),
        )

    # ========================================================================
    # Fact Marine Load
    # ========================================================================

    def load_fact_marine(self, **context: Any) -> int:
        """
        Load marine data to dwh.fct_marine.

        Filters out rows where all marine metrics are NULL (inland cities).

        Args:
            context: Airflow context containing execution date

        Returns:
            Number of records inserted
        """
        self.log_start("Loading fct_marine")

        execution_date: str = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
        extraction_date_id: int = self.get_date_id(execution_date) or 0

        with self.connection() as conn:
            try:
                name_map, _ = self.get_city_id_mapping(conn)
                silver_path = f"marine/{execution_date}/"

                try:
                    objects = self.minio_client.client.list_objects(
                        "silver-openmeteo", prefix=silver_path
                    )
                    parquet_files = [
                        obj.object_name for obj in objects if obj.object_name.endswith(".parquet")
                    ]
                except Exception:
                    parquet_files = []

                if not parquet_files:
                    self.logger.warning(f"No marine files found for {execution_date}")
                    return 0

                latest_file = sorted(parquet_files)[-1]
                df = self.minio_client.read_parquet("silver-openmeteo", latest_file)

                # Validate data quality before loading
                self.validate_data(df, "marine", "fct_marine")

                # Filter out rows where ALL marine metrics are NULL (inland cities)
                marine_metrics = [
                    "wave_height_max",
                    "wave_direction_dominant",
                    "wave_period_max",
                    "wind_wave_height_max",
                    "swell_wave_height_max",
                ]
                initial_count = len(df)
                df = df.dropna(subset=marine_metrics, how="all")
                filtered_count = initial_count - len(df)
                if filtered_count > 0:
                    self.logger.info(
                        f"Filtered {filtered_count} rows with all NULL metrics (inland cities)"
                    )

                # Deduplicate
                if "city_name" in df.columns and "time" in df.columns:
                    before_dedup = len(df)
                    df.drop_duplicates(subset=["city_name", "time"], inplace=True)
                    if len(df) < before_dedup:
                        self.logger.warning(f"Dropped {before_dedup - len(df)} duplicate rows")

                records = []
                for _, row in df.iterrows():
                    city_id = name_map.get(row.get("city_name"))
                    if not city_id:
                        continue
                    records.append(self._map_marine(row, city_id, extraction_date_id))

                if not records:
                    self.logger.warning("No valid marine records to insert")
                    return 0

                # Add created_at timestamp
                current_time = datetime.now()
                records_with_time = [(*rec, current_time) for rec in records]

                insert_query = """
                    INSERT INTO dwh.fct_marine (
                        city_id, forecast_date_id, extraction_date_id,
                        wave_height_max, wave_direction_dominant, wave_period_max,
                        wind_wave_height_max, swell_wave_height_max, created_at
                    ) VALUES %s
                    ON CONFLICT (city_id, forecast_date_id, extraction_date_id) DO NOTHING
                """

                cur = conn.cursor()
                execute_values(cur, insert_query, records_with_time)

                rowcount: int = cur.rowcount if cur.rowcount is not None else 0
                self.logger.info(f"Inserted {rowcount} marine records")
                cur.close()
                return rowcount

            except Exception as e:
                self.log_error("Error loading marine data", e)
                raise

    def _map_marine(self, row: pd.Series, city_id: int, extraction_date_id: int) -> RecordTuple:
        """Map a marine row to a database record tuple."""

        def g(k: str) -> Optional[float]:
            return float(row.get(k)) if pd.notna(row.get(k)) else None

        forecast_date_id: Optional[int] = self.get_date_id(row.get("time"))
        return (
            city_id,
            forecast_date_id,
            extraction_date_id,
            g("wave_height_max"),
            (
                int(row.get("wave_direction_dominant"))
                if pd.notna(row.get("wave_direction_dominant"))
                else None
            ),
            g("wave_period_max"),
            g("wind_wave_height_max"),
            g("swell_wave_height_max"),
        )

    # ========================================================================
    # Generic Helper
    # ========================================================================

    def _load_generic(
        self,
        context: AirflowContext,
        bucket: str,
        prefix_template: str,
        insert_query: str,
        mapper: MapperFunction,
        data_type: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> int:
        """
        Generic loader with integrated data quality validation and ETL logging.

        Args:
            context: Airflow context with execution date
            bucket: MinIO bucket name
            prefix_template: Path template with {execution_date} placeholder
            insert_query: SQL INSERT statement
            mapper: Function to map DataFrame row to tuple
            data_type: Data type for validation ('daily_forecast', 'hourly_forecast', etc.)
            table_name: Target table name for logging

        Returns:
            Number of records inserted
        """
        execution_date: str = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
        extraction_date_id: int = self.get_date_id(execution_date) or 0
        
        # Extract DAG context for ETL logging
        dag_id = context.get("dag", {}).get("dag_id", "loading_pipeline")
        task_id = context.get("task_instance", {}).get("task_id", f"load_{table_name or 'unknown'}")

        with self.connection() as conn:
            # Start ETL run logging
            run_id = self.log_etl_start(dag_id, task_id, execution_date, conn)
            
            try:
                name_map, _ = self.get_city_id_mapping(conn)
                silver_path = prefix_template.format(execution_date=execution_date)

                try:
                    objects = self.minio_client.client.list_objects(bucket, prefix=silver_path)
                    parquet_files = [
                        obj.object_name for obj in objects if obj.object_name.endswith(".parquet")
                    ]
                except Exception as e:
                    self.logger.warning(
                        f"Failed to list objects in bucket={bucket}, prefix={silver_path}: {e}"
                    )
                    parquet_files = []

                if not parquet_files:
                    self.logger.warning(f"No files found for {execution_date}")
                    self.log_etl_end(run_id, 0, conn, status="success")
                    return 0

                latest_file = sorted(parquet_files)[-1]
                df = self.minio_client.read_parquet(bucket, latest_file)

                # Validate data quality before loading
                if data_type and table_name:
                    self.validate_data(df, data_type, table_name)

                # Deduplicate data to avoid PK violations
                if "city_name" in df.columns and "time" in df.columns:
                    initial_count = len(df)
                    df.drop_duplicates(subset=["city_name", "time"], inplace=True)
                    if len(df) < initial_count:
                        self.logger.warning(f"Dropped {initial_count - len(df)} duplicate rows")

                records = []
                for _, row in df.iterrows():
                    city_id = name_map.get(row.get("city_name"))
                    if not city_id:
                        # Try fallback to city column if city_name missing (common in some dfs)
                        city_id = name_map.get(row.get("city"))

                    if not city_id:
                        self.logger.warning(
                            f"City not found for row: {row.get('city_name') or row.get('city')}"
                        )
                        continue

                    records.append(mapper(row, city_id, extraction_date_id))

                if not records:
                    self.log_etl_end(run_id, 0, conn, status="success")
                    return 0

                # Add created_at timestamp
                current_time = datetime.now()
                records_with_time = [(*rec, current_time) for rec in records]

                cur = conn.cursor()
                execute_values(cur, insert_query, records_with_time)

                rowcount: int = cur.rowcount if cur.rowcount is not None else 0
                cur.close()
                
                # End ETL run logging with success
                self.log_etl_end(run_id, rowcount, conn, status="success")
                return rowcount

            except Exception as e:
                # End ETL run logging with failure
                self.log_etl_end(run_id, 0, conn, status="failed", error_message=str(e))
                self.log_error(f"Error in _load_generic for {table_name}", e)
                raise

    # ========================================================================
    # AEMET Load Methods
    # ========================================================================

    def get_station_id_mapping(self, conn: psycopg2.extensions.connection) -> Dict[str, int]:
        """
        Get AEMET station ID mappings from the database.

        Args:
            conn: Database connection

        Returns:
            Dictionary mapping station_id to database id
        """
        cur: psycopg2.extensions.cursor = conn.cursor()
        cur.execute("SELECT station_id, id FROM dwh.dim_aemet_stations")
        station_map: Dict[str, int] = {row[0]: row[1] for row in cur.fetchall()}
        cur.close()
        return station_map

    def load_aemet_stations(self, **context: Any) -> int:
        """
        Load AEMET stations to dwh.dim_aemet_stations.

        First tries to load from silver bucket. If no data found, falls back
        to loading default stations using DimensionalLoader.

        Args:
            context: Airflow context containing execution date

        Returns:
            Number of records inserted/updated
        """
        self.log_start("Loading dim_aemet_stations")

        with self.connection() as conn:
            try:
                execution_date = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
                silver_path = f"stations/{execution_date}/"

                try:
                    objects = self.minio_client.client.list_objects(
                        SILVER_AEMET_BUCKET, prefix=silver_path
                    )
                    parquet_files = [
                        obj.object_name for obj in objects if obj.object_name.endswith(".parquet")
                    ]
                except Exception:
                    parquet_files = []

                if not parquet_files:
                    self.logger.warning(
                        f"No AEMET stations files found for {execution_date}, "
                        "loading default stations via DimensionalLoader"
                    )
                    # Fallback: load default stations using DimensionalLoader
                    dim_loader = DimensionalLoader()
                    dim_loader.load_dim_aemet_stations()
                    self.log_end("Loaded default AEMET stations via fallback")
                    return 15  # Default number of stations

                latest_file = sorted(parquet_files)[-1]
                df = self.minio_client.read_parquet(SILVER_AEMET_BUCKET, latest_file)

                records = []
                for _, row in df.iterrows():
                    records.append(
                        (
                            row.get("station_id"),
                            row.get("station_name"),
                            row.get("province"),
                            self.clean_value(row.get("altitude")),
                            self.clean_value(row.get("latitude")),
                            self.clean_value(row.get("longitude")),
                            row.get("synop_code"),
                        )
                    )

                if not records:
                    # No records in silver, load defaults
                    dim_loader = DimensionalLoader()
                    dim_loader.load_dim_aemet_stations()
                    self.log_end("Loaded default AEMET stations (empty silver file)")
                    return 15

                insert_query = """
                    INSERT INTO dwh.dim_aemet_stations (
                        station_id, station_name, province, altitude,
                        latitude, longitude, synop_code
                    ) VALUES %s
                    ON CONFLICT (station_id) DO UPDATE SET
                        station_name = EXCLUDED.station_name,
                        province = EXCLUDED.province,
                        altitude = EXCLUDED.altitude,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        synop_code = EXCLUDED.synop_code,
                        updated_at = CURRENT_TIMESTAMP
                """

                cur = conn.cursor()
                execute_values(cur, insert_query, records)

                rowcount: int = cur.rowcount if cur.rowcount is not None else 0
                self.log_end(f"Upserted {rowcount} AEMET stations from silver")
                cur.close()
                return rowcount

            except Exception as e:
                self.log_error("Error loading AEMET stations", e)
                raise

    # Shared INSERT query for AEMET fact table
    _AEMET_INSERT_QUERY = """
        INSERT INTO dwh.fct_aemet_daily_weather (
            station_id, date_id, extraction_date_id,
            temp_avg, temp_min, temp_max,
            precipitation, wind_speed_avg, wind_gust_max, wind_direction,
            sunshine_hours, pressure_max, pressure_min,
            humidity_avg, humidity_min, humidity_max,
            created_at
        ) VALUES %s
        ON CONFLICT (station_id, date_id, extraction_date_id) DO NOTHING
    """

    def _ensure_station_map(self, conn: Any) -> Dict[str, int]:
        """
        Load station mapping, bootstrapping default stations if DB is empty.

        If no stations exist in the database, loads defaults via DimensionalLoader.
        The DimensionalLoader commits on its own connection, and PostgreSQL
        READ COMMITTED isolation ensures this connection sees those changes.

        Args:
            conn: Active database connection

        Returns:
            Dictionary mapping station_id to database id
        """
        station_map = self.get_station_id_mapping(conn)

        if not station_map:
            self.logger.warning(
                "No AEMET stations found in DB. Loading default stations first..."
            )
            dim_loader = DimensionalLoader()
            dim_loader.load_dim_aemet_stations()
            station_map = self.get_station_id_mapping(conn)
            self.logger.info(f"After loading defaults: {len(station_map)} stations")

        return station_map

    def _map_aemet_record(
        self,
        row: pd.Series,
        station_db_id: int,
        extraction_date_id: int,
    ) -> RecordTuple:
        """
        Map a single AEMET DataFrame row to a record tuple.

        Args:
            row: DataFrame row with AEMET weather fields
            station_db_id: Resolved database ID for the station
            extraction_date_id: Date ID for the extraction run

        Returns:
            Tuple of 16 values matching the AEMET fact table schema
        """
        date_id = self.get_date_id(row.get("date"))
        return (
            station_db_id,
            date_id,
            extraction_date_id,
            self.clean_value(row.get("temp_avg")),
            self.clean_value(row.get("temp_min")),
            self.clean_value(row.get("temp_max")),
            self.clean_value(row.get("precipitation")),
            self.clean_value(row.get("wind_speed_avg")),
            self.clean_value(row.get("wind_gust_max")),
            self.clean_value(row.get("wind_direction")),
            self.clean_value(row.get("sunshine_hours")),
            self.clean_value(row.get("pressure_max")),
            self.clean_value(row.get("pressure_min")),
            self.clean_value(row.get("humidity_avg")),
            self.clean_value(row.get("humidity_min")),
            self.clean_value(row.get("humidity_max")),
        )

    def _build_aemet_records(
        self,
        df: pd.DataFrame,
        station_map: Dict[str, int],
        extraction_date_id: int,
    ) -> List[RecordTuple]:
        """
        Build AEMET record tuples from a DataFrame, with deduplication.

        Deduplicates on (station_id, date), maps each row via _map_aemet_record,
        and skips rows whose station_id is not in the station map.

        Args:
            df: DataFrame with AEMET weather data
            station_map: Mapping of station indicativo → database ID
            extraction_date_id: Date ID for the extraction run

        Returns:
            List of record tuples ready for insertion
        """
        if "station_id" in df.columns and "date" in df.columns:
            initial_count = len(df)
            df = df.drop_duplicates(subset=["station_id", "date"])
            if len(df) < initial_count:
                self.logger.warning(f"Dropped {initial_count - len(df)} duplicate rows")

        records: List[RecordTuple] = []
        skipped_stations: set = set()

        for _, row in df.iterrows():
            station_db_id = station_map.get(row.get("station_id"))
            if not station_db_id:
                skipped_stations.add(row.get("station_id"))
                continue
            records.append(self._map_aemet_record(row, station_db_id, extraction_date_id))

        if skipped_stations:
            self.logger.warning(
                f"Skipped {len(skipped_stations)} unknown stations: {list(skipped_stations)[:5]}"
            )

        return records

    def _insert_aemet_records(self, conn: Any, records: List[RecordTuple]) -> int:
        """
        Insert AEMET records into the fact table.

        Appends a created_at timestamp to each record and performs a bulk insert
        with ON CONFLICT DO NOTHING.

        Args:
            conn: Active database connection
            records: List of record tuples (without created_at)

        Returns:
            Number of rows actually inserted
        """
        current_time = datetime.now()
        records_with_time = [(*rec, current_time) for rec in records]

        cur = conn.cursor()
        execute_values(cur, self._AEMET_INSERT_QUERY, records_with_time)
        rowcount: int = cur.rowcount if cur.rowcount is not None else 0
        cur.close()
        return rowcount

    def load_fact_aemet_daily(self, **context: Any) -> int:
        """
        Load AEMET daily climatology to dwh.fct_aemet_daily_weather.

        Args:
            context: Airflow context containing execution date

        Returns:
            Number of records inserted
        """
        self.log_start("Loading fct_aemet_daily_weather")

        with self.connection() as conn:
            try:
                station_map = self._ensure_station_map(conn)

                if station_map:
                    self.logger.info(f"Station map loaded: {len(station_map)} stations")
                    self.logger.info(f"Sample station IDs in DB: {list(station_map.keys())[:5]}")

                execution_date = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
                extraction_date_id = self.get_date_id(execution_date) or 0

                # Try climatology/daily path
                silver_path = f"climatology/daily/{execution_date}/"
                try:
                    objects = self.minio_client.client.list_objects(
                        SILVER_AEMET_BUCKET, prefix=silver_path
                    )
                    parquet_files = [
                        obj.object_name for obj in objects if obj.object_name.endswith(".parquet")
                    ]
                except Exception:
                    parquet_files = []

                if not parquet_files:
                    self.logger.warning(f"No AEMET daily files found for {execution_date}")
                    return 0

                latest_file = sorted(parquet_files)[-1]
                self.logger.info(f"Reading parquet file: {latest_file}")
                df = self.minio_client.read_parquet(SILVER_AEMET_BUCKET, latest_file)
                self.logger.info(f"DataFrame loaded: {len(df)} rows, columns: {df.columns.tolist()}")

                if "station_id" in df.columns:
                    unique_stations = df["station_id"].unique().tolist()
                    self.logger.info(f"Station IDs in parquet: {unique_stations[:5]}")

                records = self._build_aemet_records(df, station_map, extraction_date_id)

                if not records:
                    self.logger.warning("No valid records to insert (all stations unknown)")
                    return 0

                self.logger.info(f"Preparing to insert {len(records)} records")
                rowcount = self._insert_aemet_records(conn, records)
                self.log_end(f"Inserted {rowcount} AEMET daily records")
                return rowcount

            except Exception as e:
                self.log_error("Error loading AEMET daily", e)
                raise

    def load_fact_aemet_historical(self, **context: Any) -> int:
        """
        Load AEMET historical data to dwh.fct_aemet_daily_weather.

        This method loads all historical data from the silver layer.

        Args:
            context: Airflow context containing execution date

        Returns:
            Number of records inserted
        """
        self.log_start("Loading AEMET Historical data")

        with self.connection() as conn:
            try:
                station_map = self._ensure_station_map(conn)

                execution_date = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
                extraction_date_id = self.get_date_id(execution_date) or 0

                # Scan all historical files
                silver_path = "historical/"
                try:
                    objects = list(
                        self.minio_client.client.list_objects(
                            SILVER_AEMET_BUCKET, prefix=silver_path, recursive=True
                        )
                    )
                    parquet_files = [
                        obj.object_name for obj in objects if obj.object_name.endswith(".parquet")
                    ]
                except Exception:
                    parquet_files = []

                if not parquet_files:
                    self.logger.warning("No AEMET historical files found")
                    return 0

                total_inserted = 0

                for parquet_file in parquet_files:
                    try:
                        df = self.minio_client.read_parquet(SILVER_AEMET_BUCKET, parquet_file)
                        records = self._build_aemet_records(df, station_map, extraction_date_id)

                        if records:
                            total_inserted += self._insert_aemet_records(conn, records)

                    except Exception as e:
                        self.log_error(f"Error loading {parquet_file}", e)
                        continue

                self.log_end(f"Inserted {total_inserted} AEMET historical records")
                return total_inserted

            except Exception as e:
                self.log_error("Error loading AEMET historical", e)
                raise
