"""
Unified Extractor
Consolidates all extraction logic into a single class.

Type-annotated module for weather data extraction from multiple APIs.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import urllib3

# Import configuration and utils
from src.weather_config.app_config import API_KEY
from src.weather_config.lake_config import (
    BRONZE_BUCKET,
    BRONZE_OPENMETEO_BUCKET,
    BRONZE_PATH_TEMPLATE,
)
from src.weather_config.openmeteo_config import (
    AIR_QUALITY_PARAMS,
    DAILY_FORECAST_PARAMS,
    DEFAULT_TIMEZONE,
    HOURLY_FORECAST_PARAMS,
    MARINE_PARAMS,
    OPENMETEO_AIR_QUALITY_URL,
    OPENMETEO_API_KEY,
    OPENMETEO_FORECAST_URL,
    OPENMETEO_MARINE_URL,
    OPENMETEO_POLLEN_URL,
    POLLEN_PARAMS,
)
from src.weather_utils.city_utils import get_capitals_dataframe, get_cities
from src.weather_utils.http_utils import get_retrying_session
from src.weather_utils.minio_client import MinIOClient

# Type aliases for common patterns
AirflowContext = Dict[str, Any]
CityDict = Dict[str, Any]
UploadedObject = Dict[str, Any]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Extractor:
    """
    Unified Extractor Manager.

    Handles extraction for OpenWeatherMap and Open-Meteo services.

    Attributes:
        logger: Logger instance for this class
        minio_client: MinIO client for data lake operations
        session: HTTP session with retry logic
    """

    def __init__(self) -> None:
        """Initialize the Extractor with logger, MinIO client, and HTTP session."""
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.minio_client: MinIOClient = MinIOClient()
        self.session: requests.Session = get_retrying_session()

    def log_start(self, msg: str) -> None:
        """Log the start of an operation."""
        self.logger.info(f"🚀 START: {msg}")

    def log_end(self, msg: str) -> None:
        """Log the end of an operation."""
        self.logger.info(f"🏁 END: {msg}")

    def log_error(self, msg: str, error: Optional[Exception] = None) -> None:
        """Log an error message with optional exception details."""
        if error:
            self.logger.error(f"❌ ERROR: {msg} - {str(error)}")
        else:
            self.logger.error(f"❌ ERROR: {msg}")

    # ========================================================================
    # OpenWeatherMap Extraction
    # ========================================================================

    def extract_openweather(self, **context: Any) -> int:
        """
        Extract current weather from OpenWeatherMap.

        Args:
            context: Airflow context containing execution date and task instance

        Returns:
            Number of successfully uploaded files
        """
        # Load cities from GitHub CSV
        cities: List[CityDict] = get_cities()
        self.log_start(f"OpenWeatherMap extraction for {len(cities)} cities")

        execution_date: str = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")
        uploaded_objects: List[UploadedObject] = []

        for city in cities:
            try:
                url: str = "https://api.openweathermap.org/data/2.5/weather"
                params: Dict[str, Any] = {
                    "lat": city["lat"],
                    "lon": city["lon"],
                    "appid": API_KEY,
                    "units": "metric",
                }

                self.logger.info(f"Fetching weather data for {city['name']}")
                response: requests.Response = self.session.get(url, params=params, timeout=10)
                response.raise_for_status()
                raw_data: Dict[str, Any] = response.json()

                raw_data["_metadata"] = {
                    "city_name": city["name"],
                    "extraction_timestamp": timestamp,
                    "execution_date": execution_date,
                }

                object_path: str = BRONZE_PATH_TEMPLATE.format(
                    date=execution_date, city=city["name"].lower(), timestamp=timestamp
                )

                file_size: int = self.minio_client.upload_json(BRONZE_BUCKET, object_path, raw_data)

                uploaded_objects.append(
                    {"object_path": object_path, "city": city["name"], "file_size": file_size}
                )
                self.logger.info(f"✅ Stored raw data for {city['name']}")

            except Exception as e:
                self.log_error(f"Error processing {city['name']}", e)
                continue

        self.log_end(f"Stored {len(uploaded_objects)} OpenWeather files")

        if context.get("task_instance"):
            context["task_instance"].xcom_push(key="bronze_objects", value=uploaded_objects)
            context["task_instance"].xcom_push(key="execution_date", value=execution_date)

        return len(uploaded_objects)

    # ========================================================================
    # Open-Meteo Daily Extraction
    # ========================================================================

    def extract_openmeteo_daily(self, **context: Any) -> int:
        """
        Extract 7-day daily forecast from Open-Meteo.

        Args:
            context: Airflow context containing execution date and task instance

        Returns:
            Number of successfully uploaded files
        """
        self.log_start("Open-Meteo DAILY forecast extraction")

        capitals_df: pd.DataFrame = get_capitals_dataframe()
        execution_date: str = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")
        uploaded_objects: List[UploadedObject] = []

        for _, city in capitals_df.iterrows():
            params: Dict[str, Any] = {
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "daily": ",".join(DAILY_FORECAST_PARAMS),
                "timezone": DEFAULT_TIMEZONE,
            }
            if OPENMETEO_API_KEY:
                params["apikey"] = OPENMETEO_API_KEY

            try:
                self.logger.info(f"Fetching DAILY forecast for {city['municipio_nombre']}")
                response: requests.Response = self.session.get(
                    OPENMETEO_FORECAST_URL, params=params, verify=False, timeout=60
                )
                response.raise_for_status()
                data: Dict[str, Any] = response.json()

                data["city_code"] = city["city_code"]
                data["municipio_nombre"] = city["municipio_nombre"]
                data["_metadata"] = {
                    "extraction_timestamp": timestamp,
                    "execution_date": execution_date,
                }

                object_path: str = (
                    f"forecast/daily/{execution_date}/weather_daily_{city['municipio_nombre'].lower()}_{timestamp}.json"
                )
                file_size: int = self.minio_client.upload_json(
                    BRONZE_OPENMETEO_BUCKET, object_path, data
                )

                uploaded_objects.append(
                    {"object_path": object_path, "city": city["municipio_nombre"]}
                )
                self.logger.info(f"✅ Stored DAILY forecast for {city['municipio_nombre']}")
                time.sleep(0.1)

            except Exception as e:
                self.log_error(f"Error fetching DAILY forecast for {city['municipio_nombre']}", e)
                continue

        self.log_end(f"Stored {len(uploaded_objects)} DAILY forecast files")
        if context.get("task_instance"):
            context["task_instance"].xcom_push(
                key="openmeteo_daily_objects", value=uploaded_objects
            )
            context["task_instance"].xcom_push(key="execution_date", value=execution_date)
        return len(uploaded_objects)

    # ========================================================================
    # Open-Meteo Hourly Extraction
    # ========================================================================

    def extract_openmeteo_hourly(self, **context: Any) -> int:
        """
        Extract 7-day hourly forecast from Open-Meteo.

        Args:
            context: Airflow context containing execution date and task instance

        Returns:
            Number of successfully uploaded files
        """
        self.log_start("Open-Meteo HOURLY forecast extraction")

        capitals_df: pd.DataFrame = get_capitals_dataframe()
        execution_date: str = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")
        uploaded_objects: List[UploadedObject] = []

        for _, city in capitals_df.iterrows():
            params: Dict[str, Any] = {
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "hourly": ",".join(HOURLY_FORECAST_PARAMS),
                "timezone": DEFAULT_TIMEZONE,
            }
            if OPENMETEO_API_KEY:
                params["apikey"] = OPENMETEO_API_KEY

            try:
                self.logger.info(f"Fetching HOURLY forecast for {city['municipio_nombre']}")
                response: requests.Response = self.session.get(
                    OPENMETEO_FORECAST_URL, params=params, verify=False, timeout=60
                )
                response.raise_for_status()
                data: Dict[str, Any] = response.json()

                data["city_code"] = city["city_code"]
                data["municipio_nombre"] = city["municipio_nombre"]
                data["_metadata"] = {
                    "extraction_timestamp": timestamp,
                    "execution_date": execution_date,
                }

                object_path: str = (
                    f"forecast/hourly/{execution_date}/weather_hourly_{city['municipio_nombre'].lower()}_{timestamp}.json"
                )
                file_size: int = self.minio_client.upload_json(
                    BRONZE_OPENMETEO_BUCKET, object_path, data
                )

                uploaded_objects.append(
                    {"object_path": object_path, "city": city["municipio_nombre"]}
                )
                self.logger.info(f"✅ Stored HOURLY forecast for {city['municipio_nombre']}")
                time.sleep(0.1)

            except Exception as e:
                self.log_error(f"Error fetching HOURLY forecast for {city['municipio_nombre']}", e)
                continue

        self.log_end(f"Stored {len(uploaded_objects)} HOURLY forecast files")
        if context.get("task_instance"):
            context["task_instance"].xcom_push(
                key="openmeteo_hourly_objects", value=uploaded_objects
            )
        return len(uploaded_objects)

    # ========================================================================
    # Open-Meteo Air Quality Extraction
    # ========================================================================

    def extract_openmeteo_air_quality(self, **context: Any) -> int:
        """
        Extract Air Quality data from Open-Meteo.

        Args:
            context: Airflow context containing execution date and task instance

        Returns:
            Number of successfully uploaded files
        """
        self.log_start("Open-Meteo AIR QUALITY extraction")

        capitals_df: pd.DataFrame = get_capitals_dataframe()
        execution_date: str = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")
        uploaded_objects: List[UploadedObject] = []

        for _, city in capitals_df.iterrows():
            params: Dict[str, Any] = {
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "hourly": ",".join(AIR_QUALITY_PARAMS),
                "timezone": DEFAULT_TIMEZONE,
            }
            if OPENMETEO_API_KEY:
                params["apikey"] = OPENMETEO_API_KEY

            try:
                self.logger.info(f"Fetching AIR QUALITY for {city['municipio_nombre']}")
                response: requests.Response = self.session.get(
                    OPENMETEO_AIR_QUALITY_URL, params=params, verify=False, timeout=60
                )
                if response.status_code != 200:
                    continue

                data: Dict[str, Any] = response.json()
                data["city_code"] = city["city_code"]
                data["municipio_nombre"] = city["municipio_nombre"]
                data["_metadata"] = {
                    "extraction_timestamp": timestamp,
                    "execution_date": execution_date,
                }

                object_path: str = (
                    f"air_quality/{execution_date}/air_quality_{city['municipio_nombre'].lower()}_{timestamp}.json"
                )
                self.minio_client.upload_json(BRONZE_OPENMETEO_BUCKET, object_path, data)
                uploaded_objects.append(
                    {"object_path": object_path, "city": city["municipio_nombre"]}
                )
                time.sleep(0.1)

            except Exception as e:
                self.log_error(f"Error fetching AIR QUALITY for {city['municipio_nombre']}", e)
                continue

        self.log_end(f"Stored {len(uploaded_objects)} AIR QUALITY files")
        if context.get("task_instance"):
            context["task_instance"].xcom_push(
                key="openmeteo_air_quality_objects", value=uploaded_objects
            )
        return len(uploaded_objects)

    # ========================================================================
    # Open-Meteo Pollen Extraction
    # ========================================================================

    def extract_openmeteo_pollen(self, **context: Any) -> int:
        """
        Extract Pollen data from Open-Meteo.

        Args:
            context: Airflow context containing execution date and task instance

        Returns:
            Number of successfully uploaded files
        """
        self.log_start("Open-Meteo POLLEN extraction")

        capitals_df: pd.DataFrame = get_capitals_dataframe()
        execution_date: str = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")
        uploaded_objects: List[UploadedObject] = []

        for _, city in capitals_df.iterrows():
            params: Dict[str, Any] = {
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "hourly": ",".join(POLLEN_PARAMS),
                "timezone": DEFAULT_TIMEZONE,
            }
            if OPENMETEO_API_KEY:
                params["apikey"] = OPENMETEO_API_KEY

            try:
                self.logger.info(f"Fetching POLLEN for {city['municipio_nombre']}")
                response: requests.Response = self.session.get(
                    OPENMETEO_POLLEN_URL, params=params, verify=False, timeout=60
                )
                if response.status_code != 200:
                    continue

                data: Dict[str, Any] = response.json()
                data["city_code"] = city["city_code"]
                data["municipio_nombre"] = city["municipio_nombre"]
                data["_metadata"] = {
                    "extraction_timestamp": timestamp,
                    "execution_date": execution_date,
                }

                object_path: str = (
                    f"pollen/{execution_date}/pollen_{city['municipio_nombre'].lower()}_{timestamp}.json"
                )
                self.minio_client.upload_json(BRONZE_OPENMETEO_BUCKET, object_path, data)
                uploaded_objects.append(
                    {"object_path": object_path, "city": city["municipio_nombre"]}
                )
                time.sleep(0.1)

            except Exception as e:
                self.log_error(f"Error fetching POLLEN for {city['municipio_nombre']}", e)
                continue

        self.log_end(f"Stored {len(uploaded_objects)} POLLEN files")
        if context.get("task_instance"):
            context["task_instance"].xcom_push(
                key="openmeteo_pollen_objects", value=uploaded_objects
            )
        return len(uploaded_objects)

    # ========================================================================
    # Open-Meteo Marine Extraction
    # ========================================================================

    def extract_openmeteo_marine(self, **context: Any) -> int:
        """
        Extract Marine data from Open-Meteo.

        Args:
            context: Airflow context containing execution date and task instance

        Returns:
            Number of successfully uploaded files
        """
        self.log_start("Open-Meteo MARINE extraction")

        capitals_df: pd.DataFrame = get_capitals_dataframe()
        execution_date: str = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")
        uploaded_objects: List[UploadedObject] = []

        for _, city in capitals_df.iterrows():
            # Only coastal cities logic could be added here
            params: Dict[str, Any] = {
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "hourly": ",".join(MARINE_PARAMS),
                "timezone": DEFAULT_TIMEZONE,
            }
            if OPENMETEO_API_KEY:
                params["apikey"] = OPENMETEO_API_KEY

            try:
                self.logger.info(f"Fetching MARINE for {city['municipio_nombre']}")
                response: requests.Response = self.session.get(
                    OPENMETEO_MARINE_URL, params=params, verify=False, timeout=60
                )
                if response.status_code != 200:
                    continue

                data: Dict[str, Any] = response.json()
                data["city_code"] = city["city_code"]
                data["municipio_nombre"] = city["municipio_nombre"]
                data["_metadata"] = {
                    "extraction_timestamp": timestamp,
                    "execution_date": execution_date,
                }

                object_path: str = (
                    f"marine/{execution_date}/marine_{city['municipio_nombre'].lower()}_{timestamp}.json"
                )
                self.minio_client.upload_json(BRONZE_OPENMETEO_BUCKET, object_path, data)
                uploaded_objects.append(
                    {"object_path": object_path, "city": city["municipio_nombre"]}
                )
                time.sleep(0.1)

            except Exception as e:
                self.log_error(f"Error fetching MARINE for {city['municipio_nombre']}", e)
                continue

        self.log_end(f"Stored {len(uploaded_objects)} MARINE files")
        if context.get("task_instance"):
            context["task_instance"].xcom_push(
                key="openmeteo_marine_objects", value=uploaded_objects
            )
        return len(uploaded_objects)
