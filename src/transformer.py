"""
Unified Transformer
Consolidates all transformation logic into a single class.

Type-annotated module for weather data transformation (Bronze → Silver).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from src.config.lake_config import (
    BRONZE_AEMET_BUCKET,
    BRONZE_BUCKET,
    BRONZE_OPENMETEO_BUCKET,
    SILVER_AEMET_BUCKET,
    SILVER_BUCKET,
    SILVER_OPENMETEO_BUCKET,
    SILVER_PATH_TEMPLATE,
)
from src.type_aliases import AirflowContext, BronzeObject, TransformedRecord
from src.utils.etl_logger import BaseETLLogger
from src.utils.minio_client import MinIOClient


class Transformer(BaseETLLogger):
    """
    Unified Transformer Manager.

    Handles transformation for OpenWeatherMap and Open-Meteo services.

    Attributes:
        logger: Logger instance for this class (via BaseETLLogger)
        minio_client: MinIO client for data lake operations
    """

    def __init__(self) -> None:
        """Initialize the Transformer with logger and MinIO client."""
        super().__init__()
        self.minio_client: MinIOClient = MinIOClient()

    # ========================================================================
    # OpenWeatherMap Transformation
    # ========================================================================

    def transform_openweather(self, **context: Any) -> int:
        """
        Transform OpenWeather data from JSON to Parquet.

        Args:
            context: Airflow context containing execution date and task instance

        Returns:
            Number of transformed records
        """
        self.log_start("Transforming OpenWeather data (Bronze → Silver)")

        execution_date: Optional[str] = context.get("ds")
        bronze_objects: Optional[List[BronzeObject]] = self._get_upstream_data(
            context, ["extract_to_bronze", "extract_openweather"], "bronze_objects"
        )

        if not execution_date:
            execution_date = datetime.now().strftime("%Y-%m-%d")

        if not bronze_objects:
            # Fallback: Scan bronze bucket
            prefix: str = f"current/{execution_date}/"
            try:
                objects = self.minio_client.client.list_objects(
                    BRONZE_BUCKET, prefix=prefix, recursive=True
                )
                bronze_objects = [
                    {"object_path": obj.object_name}
                    for obj in objects
                    if obj.object_name.endswith(".json")
                ]
            except Exception:
                pass

        if not bronze_objects:
            self.logger.warning("No bronze data to transform")
            return 0

        transformed_records: List[TransformedRecord] = []
        for bronze_obj in bronze_objects:
            try:
                object_path: Optional[str] = bronze_obj.get("object_path")
                if not object_path:
                    continue

                raw_data: Dict[str, Any] = self.minio_client.read_json(BRONZE_BUCKET, object_path)

                transformed: TransformedRecord = {
                    "city": raw_data.get("_metadata", {}).get("city_name", "Unknown"),
                    "country": raw_data.get("sys", {}).get("country", "ES"),
                    "latitude": raw_data.get("coord", {}).get("lat"),
                    "longitude": raw_data.get("coord", {}).get("lon"),
                    "temperature": raw_data.get("main", {}).get("temp"),
                    "feels_like": raw_data.get("main", {}).get("feels_like"),
                    "temp_min": raw_data.get("main", {}).get("temp_min"),
                    "temp_max": raw_data.get("main", {}).get("temp_max"),
                    "pressure": raw_data.get("main", {}).get("pressure"),
                    "humidity": raw_data.get("main", {}).get("humidity"),
                    "weather_main": raw_data.get("weather", [{}])[0].get("main", "Unknown"),
                    "weather_description": raw_data.get("weather", [{}])[0].get(
                        "description", "No description"
                    ),
                    "wind_speed": raw_data.get("wind", {}).get("speed"),
                    "wind_deg": raw_data.get("wind", {}).get("deg"),
                    "clouds": raw_data.get("clouds", {}).get("all"),
                    "visibility": raw_data.get("visibility"),
                    "date": execution_date,
                    "bronze_source": object_path,
                }
                transformed_records.append(transformed)
            except Exception as e:
                self.log_error(f"Error transforming {object_path}", e)
                continue

        if not transformed_records:
            return 0

        df: pd.DataFrame = pd.DataFrame(transformed_records)
        silver_path: str = SILVER_PATH_TEMPLATE.format(date=execution_date)
        file_size: int = self.minio_client.upload_parquet(SILVER_BUCKET, silver_path, df)

        self.log_end(f"Stored {len(df)} records in {silver_path}")
        return len(df)

    # ========================================================================
    # Open-Meteo Daily Transformation
    # ========================================================================

    def transform_openmeteo_daily(self, **context: Any) -> int:
        """
        Transform Open-Meteo daily forecast from JSON to Parquet.

        Args:
            context: Airflow context containing execution date and task instance

        Returns:
            Number of transformed records
        """
        self.log_start("Transforming Open-Meteo DAILY forecast")
        return self._transform_generic(
            context=context,
            upstream_keys=["openmeteo_daily_objects"],
            upstream_tasks=["extract_openmeteo_daily"],
            bucket_search_prefix="forecast/daily/",
            data_key="daily",
            silver_path_prefix="forecast/daily/",
            file_suffix="daily_forecast",
        )

    # ========================================================================
    # Open-Meteo Hourly Transformation
    # ========================================================================

    def transform_openmeteo_hourly(self, **context: Any) -> int:
        """
        Transform Open-Meteo hourly forecast from JSON to Parquet.

        Args:
            context: Airflow context containing execution date and task instance

        Returns:
            Number of transformed records
        """
        self.log_start("Transforming Open-Meteo HOURLY forecast")
        return self._transform_generic(
            context=context,
            upstream_keys=["openmeteo_hourly_objects"],
            upstream_tasks=["extract_openmeteo_hourly"],
            bucket_search_prefix="forecast/hourly/",
            data_key="hourly",
            silver_path_prefix="forecast/hourly/",
            file_suffix="hourly_forecast",
        )

    # ========================================================================
    # Open-Meteo Air Quality Transformation
    # ========================================================================

    def transform_openmeteo_air_quality(self, **context: Any) -> int:
        """
        Transform Open-Meteo air quality from JSON to Parquet.

        Args:
            context: Airflow context containing execution date and task instance

        Returns:
            Number of transformed records
        """
        self.log_start("Transforming Open-Meteo AIR QUALITY")
        return self._transform_generic(
            context=context,
            upstream_keys=["openmeteo_air_quality_objects"],
            upstream_tasks=["extract_openmeteo_air_quality"],
            bucket_search_prefix="air_quality/",
            data_key="hourly",
            silver_path_prefix="air_quality/",
            file_suffix="air_quality",
        )

    # ========================================================================
    # Open-Meteo Pollen Transformation
    # ========================================================================

    def transform_openmeteo_pollen(self, **context: Any) -> int:
        """
        Transform Open-Meteo pollen from JSON to Parquet.

        Args:
            context: Airflow context containing execution date and task instance

        Returns:
            Number of transformed records
        """
        self.log_start("Transforming Open-Meteo POLLEN")
        return self._transform_generic(
            context=context,
            upstream_keys=["openmeteo_pollen_objects"],
            upstream_tasks=["extract_openmeteo_pollen"],
            bucket_search_prefix="pollen/",
            data_key="hourly",
            silver_path_prefix="pollen/",
            file_suffix="pollen",
        )

    # ========================================================================
    # Open-Meteo Marine Transformation
    # ========================================================================

    def transform_openmeteo_marine(self, **context: Any) -> int:
        """
        Transform Open-Meteo marine from JSON to Parquet.

        Args:
            context: Airflow context containing execution date and task instance

        Returns:
            Number of transformed records
        """
        self.log_start("Transforming Open-Meteo MARINE")
        return self._transform_generic(
            context=context,
            upstream_keys=["openmeteo_marine_objects"],
            upstream_tasks=["extract_openmeteo_marine"],
            bucket_search_prefix="marine/",
            data_key="daily",  # Marine uses daily aggregations (wave_height_max, etc.)
            silver_path_prefix="marine/",
            file_suffix="marine",
        )

    # ========================================================================
    # Generic Helper
    # ========================================================================

    def _transform_generic(
        self,
        context: AirflowContext,
        upstream_keys: List[str],
        upstream_tasks: List[str],
        bucket_search_prefix: str,
        data_key: str,
        silver_path_prefix: str,
        file_suffix: str,
    ) -> int:
        """
        Generic transformation logic for Open-Meteo files.

        Args:
            context: Airflow context
            upstream_keys: XCom keys to search for
            upstream_tasks: Task IDs to pull XCom from
            bucket_search_prefix: Prefix for fallback bucket scan
            data_key: Key in JSON data ('hourly' or 'daily')
            silver_path_prefix: Prefix for silver layer path
            file_suffix: Suffix for output file

        Returns:
            Number of transformed records
        """
        execution_date: str = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Get Upstream Data
        bronze_objects: Optional[List[BronzeObject]] = None
        for task_id in upstream_tasks:
            for key in upstream_keys:
                try:
                    bronze_objects = context["task_instance"].xcom_pull(key=key, task_ids=task_id)
                    if bronze_objects:
                        break
                except Exception:
                    continue
            if bronze_objects:
                break

        # Fallback Scan
        if not bronze_objects:
            self.logger.warning(f"No XCom data, scanning bronze bucket for {execution_date}")
            prefix: str = f"{bucket_search_prefix}{execution_date}/"
            try:
                objects = list(
                    self.minio_client.client.list_objects(
                        BRONZE_OPENMETEO_BUCKET, prefix=prefix, recursive=True
                    )
                )
                bronze_objects = [
                    {"object_path": obj.object_name}
                    for obj in objects
                    if obj.object_name.endswith(".json")
                ]
            except Exception:
                pass

        if not bronze_objects:
            self.logger.warning("No data to transform")
            return 0

        all_dfs: List[pd.DataFrame] = []
        for bronze_obj in bronze_objects:
            try:
                object_path: Optional[str] = bronze_obj.get("object_path")
                if not object_path:
                    continue

                data: Dict[str, Any] = self.minio_client.read_json(
                    BRONZE_OPENMETEO_BUCKET, object_path
                )

                # Handle different data structures (daily vs hourly dicts inside response)
                # Some endpoints return 'hourly' dict, some 'daily' dict.
                # We check the generic data_key passed in, but also fallback if needed.
                target_data: Optional[Dict[str, Any]] = data.get(data_key)
                if not target_data and data_key == "hourly" and "daily" in data:
                    target_data = data["daily"]  # Fallback for marine if it uses daily

                if target_data and "time" in target_data:
                    df: pd.DataFrame = pd.DataFrame(target_data)
                    df["city_code"] = data.get("city_code")
                    df["city_name"] = data.get("municipio_nombre", "Unknown")
                    df["extraction_date"] = execution_date
                    df["extraction_timestamp"] = data.get("_metadata", {}).get(
                        "extraction_timestamp", timestamp
                    )
                    all_dfs.append(df)
            except Exception as e:
                self.log_error(f"Error transforming {object_path}", e)
                continue

        if not all_dfs:
            return 0

        combined_df: pd.DataFrame = pd.concat(all_dfs, ignore_index=True)
        silver_path: str = f"{silver_path_prefix}{execution_date}/{file_suffix}_{timestamp}.parquet"
        file_size: int = self.minio_client.upload_parquet(
            SILVER_OPENMETEO_BUCKET, silver_path, combined_df
        )

        self.log_end(f"Stored {len(combined_df)} records to {silver_path}")
        return len(combined_df)

    def _get_upstream_data(
        self, context: AirflowContext, task_ids: List[str], key: str
    ) -> Optional[List[BronzeObject]]:
        """
        Get data from upstream tasks via XCom.

        Args:
            context: Airflow context
            task_ids: List of task IDs to try
            key: XCom key to retrieve

        Returns:
            Data from XCom or None if not found
        """
        if not context.get("task_instance"):
            return None
        for task_id in task_ids:
            try:
                val = context["task_instance"].xcom_pull(key=key, task_ids=task_id)
                if val:
                    result: List[BronzeObject] = val
                    return result
            except Exception:
                continue
        return None

    # ========================================================================
    # AEMET Transformations
    # ========================================================================

    def transform_aemet_stations(self, **context: Any) -> int:
        """
        Transform AEMET stations inventory from JSON to Parquet.

        Args:
            context: Airflow context containing execution date and task instance

        Returns:
            Number of transformed records
        """
        self.log_start("Transforming AEMET Stations inventory")

        execution_date: str = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Try to get from XCom first
        bronze_objects: Optional[List[BronzeObject]] = self._get_upstream_data(
            context, ["extract_aemet_stations"], "aemet_stations_objects"
        )

        # Fallback: scan bronze bucket
        if not bronze_objects:
            prefix: str = f"stations/{execution_date}/"
            try:
                objects = list(
                    self.minio_client.client.list_objects(
                        BRONZE_AEMET_BUCKET, prefix=prefix, recursive=True
                    )
                )
                bronze_objects = [
                    {"object_path": obj.object_name}
                    for obj in objects
                    if obj.object_name.endswith(".json")
                ]
            except Exception:
                pass

        if not bronze_objects:
            self.logger.warning("No AEMET stations data to transform")
            return 0

        all_stations: List[TransformedRecord] = []

        for bronze_obj in bronze_objects:
            try:
                object_path: Optional[str] = bronze_obj.get("object_path")
                if not object_path:
                    continue

                data: Dict[str, Any] = self.minio_client.read_json(BRONZE_AEMET_BUCKET, object_path)

                stations = data.get("stations", [])
                for station in stations:
                    transformed: TransformedRecord = {
                        "station_id": station.get("indicativo"),
                        "station_name": station.get("nombre"),
                        "province": station.get("provincia"),
                        "altitude": self._safe_float(station.get("altitud")),
                        "latitude": self._parse_aemet_coord(station.get("latitud")),
                        "longitude": self._parse_aemet_coord(station.get("longitud")),
                        "synop_code": station.get("indsinop"),
                        "extraction_date": execution_date,
                    }
                    all_stations.append(transformed)

            except Exception as e:
                self.log_error(f"Error transforming {object_path}", e)
                continue

        if not all_stations:
            return 0

        df: pd.DataFrame = pd.DataFrame(all_stations)
        silver_path: str = f"stations/{execution_date}/stations_{timestamp}.parquet"
        self.minio_client.upload_parquet(SILVER_AEMET_BUCKET, silver_path, df)

        self.log_end(f"Stored {len(df)} AEMET stations to {silver_path}")
        return len(df)

    def transform_aemet_daily_climatology(self, **context: Any) -> int:
        """
        Transform AEMET daily climatology from JSON to Parquet.

        Args:
            context: Airflow context containing execution date and task instance

        Returns:
            Number of transformed records
        """
        self.log_start("Transforming AEMET Daily Climatology")

        execution_date: str = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Try XCom first
        bronze_objects: Optional[List[BronzeObject]] = self._get_upstream_data(
            context, ["extract_aemet_daily_climatology"], "aemet_daily_objects"
        )

        # Fallback: scan bronze bucket
        if not bronze_objects:
            prefix: str = f"climatology/daily/{execution_date}/"
            try:
                objects = list(
                    self.minio_client.client.list_objects(
                        BRONZE_AEMET_BUCKET, prefix=prefix, recursive=True
                    )
                )
                bronze_objects = [
                    {"object_path": obj.object_name}
                    for obj in objects
                    if obj.object_name.endswith(".json")
                ]
            except Exception:
                pass

        if not bronze_objects:
            self.logger.warning("No AEMET daily climatology data to transform")
            return 0

        all_records: List[TransformedRecord] = []

        for bronze_obj in bronze_objects:
            try:
                object_path: Optional[str] = bronze_obj.get("object_path")
                if not object_path:
                    continue

                data: Dict[str, Any] = self.minio_client.read_json(BRONZE_AEMET_BUCKET, object_path)

                station_id = data.get("station_id")
                records = data.get("records", [])

                for record in records:
                    transformed: TransformedRecord = {
                        "station_id": station_id,
                        "station_name": record.get("nombre"),
                        "province": record.get("provincia"),
                        "date": record.get("fecha"),
                        "temp_avg": self._safe_float(record.get("tmed")),
                        "temp_min": self._safe_float(record.get("tmin")),
                        "temp_max": self._safe_float(record.get("tmax")),
                        "precipitation": self._safe_float(record.get("prec")),
                        "wind_speed_avg": self._safe_float(record.get("velmedia")),
                        "wind_gust_max": self._safe_float(record.get("racha")),
                        "wind_direction": self._safe_float(record.get("dir")),
                        "sunshine_hours": self._safe_float(record.get("sol")),
                        "pressure_max": self._safe_float(record.get("presMax")),
                        "pressure_min": self._safe_float(record.get("presMin")),
                        "humidity_avg": self._safe_float(record.get("hrMedia")),
                        "humidity_min": self._safe_float(record.get("hrMin")),
                        "humidity_max": self._safe_float(record.get("hrMax")),
                        "extraction_date": execution_date,
                    }
                    all_records.append(transformed)

            except Exception as e:
                self.log_error(f"Error transforming {object_path}", e)
                continue

        if not all_records:
            return 0

        df: pd.DataFrame = pd.DataFrame(all_records)
        silver_path: str = (
            f"climatology/daily/{execution_date}/daily_climatology_{timestamp}.parquet"
        )
        self.minio_client.upload_parquet(SILVER_AEMET_BUCKET, silver_path, df)

        self.log_end(f"Stored {len(df)} AEMET daily records to {silver_path}")
        return len(df)

    def transform_aemet_historical(self, **context: Any) -> int:
        """
        Transform AEMET historical data from JSON to Parquet.

        Args:
            context: Airflow context containing execution date and task instance

        Returns:
            Number of transformed records
        """
        self.log_start("Transforming AEMET Historical data")

        execution_date: str = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Try XCom first
        bronze_objects: Optional[List[BronzeObject]] = self._get_upstream_data(
            context, ["extract_aemet_historical"], "aemet_historical_objects"
        )

        # Fallback: scan bronze bucket for all historical data
        if not bronze_objects:
            prefix: str = "historical/"
            try:
                objects = list(
                    self.minio_client.client.list_objects(
                        BRONZE_AEMET_BUCKET, prefix=prefix, recursive=True
                    )
                )
                bronze_objects = [
                    {"object_path": obj.object_name}
                    for obj in objects
                    if obj.object_name.endswith(".json")
                ]
            except Exception:
                pass

        if not bronze_objects:
            self.logger.warning("No AEMET historical data to transform")
            return 0

        all_records: List[TransformedRecord] = []

        for bronze_obj in bronze_objects:
            try:
                object_path: Optional[str] = bronze_obj.get("object_path")
                if not object_path:
                    continue

                data: Dict[str, Any] = self.minio_client.read_json(BRONZE_AEMET_BUCKET, object_path)

                station_id = data.get("station_id")
                year = data.get("year")
                records = data.get("records", [])

                for record in records:
                    transformed: TransformedRecord = {
                        "station_id": station_id,
                        "station_name": record.get("nombre"),
                        "province": record.get("provincia"),
                        "date": record.get("fecha"),
                        "year": year,
                        "temp_avg": self._safe_float(record.get("tmed")),
                        "temp_min": self._safe_float(record.get("tmin")),
                        "temp_max": self._safe_float(record.get("tmax")),
                        "precipitation": self._safe_float(record.get("prec")),
                        "wind_speed_avg": self._safe_float(record.get("velmedia")),
                        "wind_gust_max": self._safe_float(record.get("racha")),
                        "wind_direction": self._safe_float(record.get("dir")),
                        "sunshine_hours": self._safe_float(record.get("sol")),
                        "pressure_max": self._safe_float(record.get("presMax")),
                        "pressure_min": self._safe_float(record.get("presMin")),
                        "humidity_avg": self._safe_float(record.get("hrMedia")),
                        "humidity_min": self._safe_float(record.get("hrMin")),
                        "humidity_max": self._safe_float(record.get("hrMax")),
                        "extraction_date": execution_date,
                    }
                    all_records.append(transformed)

            except Exception as e:
                self.log_error(f"Error transforming {object_path}", e)
                continue

        if not all_records:
            return 0

        df: pd.DataFrame = pd.DataFrame(all_records)
        silver_path: str = f"historical/{execution_date}/historical_{timestamp}.parquet"
        self.minio_client.upload_parquet(SILVER_AEMET_BUCKET, silver_path, df)

        self.log_end(f"Stored {len(df)} AEMET historical records to {silver_path}")
        return len(df)

    # ========================================================================
    # AEMET Helper Methods
    # ========================================================================

    def _safe_float(self, value: Any) -> Optional[float]:
        """
        Safely convert a value to float, handling AEMET's special formats.

        AEMET uses comma as decimal separator and may have special values.

        Args:
            value: Value to convert

        Returns:
            Float value or None if conversion fails
        """
        if value is None or value == "" or value == "Ip" or value == "Acum":
            return None
        try:
            # AEMET uses comma as decimal separator
            if isinstance(value, str):
                value = value.replace(",", ".")
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_aemet_coord(self, coord_str: Optional[str]) -> Optional[float]:
        """
        Parse AEMET coordinate format to decimal degrees.

        AEMET coordinates are in format like "403520N" or "034115W"
        (degrees, minutes, seconds, direction).

        Args:
            coord_str: Coordinate string in AEMET format

        Returns:
            Decimal degrees or None if parsing fails
        """
        if not coord_str or len(coord_str) < 5:
            return None

        try:
            direction = coord_str[-1].upper()
            coord_str = coord_str[:-1]

            if len(coord_str) == 6:
                # Format: DDMMSS
                degrees = int(coord_str[:2])
                minutes = int(coord_str[2:4])
                seconds = int(coord_str[4:6])
            elif len(coord_str) == 5:
                # Format: DMMSS (longitude without leading zero)
                degrees = int(coord_str[:1])
                minutes = int(coord_str[1:3])
                seconds = int(coord_str[3:5])
            else:
                return None

            decimal = degrees + minutes / 60 + seconds / 3600

            if direction in ("S", "W"):
                decimal = -decimal

            return round(decimal, 6)

        except (ValueError, IndexError):
            return None
