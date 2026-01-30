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

from src.config.apis.aemet_config import (
    AEMET_BASE_URL,
    DEFAULT_STATION_IDS,
)
from src.config.apis.aemet_config import ENDPOINTS as AEMET_ENDPOINTS
from src.config.apis.aemet_config import REQUEST_TIMEOUT as AEMET_TIMEOUT
from src.config.apis.aemet_config import get_api_key as get_aemet_api_key
from src.config.apis.openmeteo_config import (
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
from src.config.apis.openweather_config import API_KEY

# Import configuration and utils
from src.config.lake_config import (
    BRONZE_AEMET_BUCKET,
    BRONZE_BUCKET,
    BRONZE_OPENMETEO_BUCKET,
    BRONZE_PATH_TEMPLATE,
)
from src.type_aliases import AirflowContext, CityDict, UploadedObject
from src.utils.city_utils import get_capitals_dataframe, get_cities
from src.utils.etl_logger import BaseETLLogger
from src.utils.http_utils import get_retrying_session
from src.utils.minio_client import MinIOClient

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class Extractor(BaseETLLogger):
    """
    Unified Extractor Manager.

    Handles extraction for OpenWeatherMap and Open-Meteo services.

    Attributes:
        logger: Logger instance for this class (via BaseETLLogger)
        minio_client: MinIO client for data lake operations
        session: HTTP session with retry logic
    """

    def __init__(self) -> None:
        """Initialize the Extractor with logger, MinIO client, and HTTP session."""
        super().__init__()
        self.minio_client: MinIOClient = MinIOClient()
        self.session: requests.Session = get_retrying_session()

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
                "daily": ",".join(MARINE_PARAMS),
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

    # ========================================================================
    # AEMET Extraction Methods
    # ========================================================================

    def _aemet_request(self, endpoint: str) -> Optional[List[Dict[str, Any]]]:
        """
        Make a request to AEMET API (two-step process).

        AEMET API returns a URL in the first response, then the actual data
        must be fetched from that URL.

        Args:
            endpoint: API endpoint to call

        Returns:
            List of data dictionaries or None if request fails
        """
        api_key = get_aemet_api_key()
        if not api_key:
            self.log_error("AEMET API key not configured")
            return None

        url = f"{AEMET_BASE_URL}{endpoint}"
        headers = {"api_key": api_key}

        try:
            # Step 1: Get the data URL
            response = self.session.get(url, headers=headers, timeout=AEMET_TIMEOUT)
            response.raise_for_status()
            result = response.json()

            if result.get("estado") != 200:
                self.log_error(f"AEMET API error: {result.get('descripcion')}")
                return None

            data_url = result.get("datos")
            if not data_url:
                self.log_error("No data URL in AEMET response")
                return None

            # Step 2: Fetch actual data from the URL
            data_response = self.session.get(data_url, timeout=AEMET_TIMEOUT)
            data_response.raise_for_status()
            result_data: List[Dict[str, Any]] = data_response.json()
            return result_data

        except requests.exceptions.RequestException as e:
            self.log_error(f"AEMET request failed: {endpoint}", e)
            return None
        except ValueError as e:
            self.log_error(f"AEMET JSON decode error: {endpoint}", e)
            return None

    def extract_aemet_stations(self, **context: Any) -> int:
        """
        Extract AEMET station inventory.

        Fetches all meteorological stations from AEMET and stores them
        in the bronze layer.

        Args:
            context: Airflow context containing execution date and task instance

        Returns:
            Number of stations extracted (1 file with all stations)
        """
        self.log_start("AEMET Stations inventory extraction")

        execution_date: str = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")

        endpoint = AEMET_ENDPOINTS["stations"]
        stations = self._aemet_request(endpoint)

        if not stations:
            self.logger.warning("No stations data received from AEMET")
            return 0

        # Add metadata
        data: Dict[str, Any] = {
            "stations": stations,
            "_metadata": {
                "extraction_timestamp": timestamp,
                "execution_date": execution_date,
                "station_count": len(stations),
            },
        }

        object_path = f"stations/{execution_date}/stations_{timestamp}.json"
        file_size = self.minio_client.upload_json(BRONZE_AEMET_BUCKET, object_path, data)

        self.log_end(f"Stored {len(stations)} AEMET stations")

        if context.get("task_instance"):
            context["task_instance"].xcom_push(
                key="aemet_stations_objects",
                value=[{"object_path": object_path, "station_count": len(stations)}],
            )

        return 1

    def extract_aemet_daily_climatology(
        self,
        station_ids: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        **context: Any,
    ) -> int:
        """
        Extract daily climatological data from AEMET for specified stations.

        AEMET limits queries to ~31 days per request, so for large date ranges
        this method splits into multiple requests.

        Args:
            station_ids: List of station IDs to extract. If None, uses default capitals
            start_date: Start date in YYYY-MM-DD format. Defaults to execution_date - 30 days
            end_date: End date in YYYY-MM-DD format. Defaults to execution_date
            context: Airflow context

        Returns:
            Number of files uploaded
        """
        self.log_start("AEMET Daily Climatology extraction")

        execution_date: str = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Default date range: last 30 days
        if not end_date:
            end_date = execution_date
        if not start_date:
            from datetime import timedelta

            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            start_dt = end_dt - timedelta(days=30)
            start_date = start_dt.strftime("%Y-%m-%d")

        # Default stations: Spanish capital cities stations
        if not station_ids:
            station_ids = DEFAULT_STATION_IDS.copy()

        # Format dates for AEMET API (ISO format with UTC)
        start_iso = f"{start_date}T00:00:00UTC"
        end_iso = f"{end_date}T23:59:59UTC"

        uploaded_objects: List[UploadedObject] = []

        for station_id in station_ids:
            try:
                endpoint = AEMET_ENDPOINTS["daily_climatology"].format(
                    start=start_iso, end=end_iso, station=station_id
                )

                self.logger.info(f"Fetching climatology for station {station_id}")
                data = self._aemet_request(endpoint)

                if not data:
                    self.logger.warning(f"No data for station {station_id}")
                    continue

                # Store raw data with metadata
                raw_data: Dict[str, Any] = {
                    "station_id": station_id,
                    "start_date": start_date,
                    "end_date": end_date,
                    "records": data,
                    "_metadata": {
                        "extraction_timestamp": timestamp,
                        "execution_date": execution_date,
                        "record_count": len(data),
                    },
                }

                object_path = (
                    f"climatology/daily/{execution_date}/" f"daily_{station_id}_{timestamp}.json"
                )
                file_size = self.minio_client.upload_json(
                    BRONZE_AEMET_BUCKET, object_path, raw_data
                )

                uploaded_objects.append(
                    {
                        "object_path": object_path,
                        "station_id": station_id,
                        "record_count": len(data),
                        "file_size": file_size,
                    }
                )
                self.logger.info(f"Stored {len(data)} records for station {station_id}")

                # Rate limiting - AEMET has strict limits
                time.sleep(0.5)

            except Exception as e:
                self.log_error(f"Error extracting station {station_id}", e)
                continue

        self.log_end(f"Stored {len(uploaded_objects)} AEMET climatology files")

        if context.get("task_instance"):
            context["task_instance"].xcom_push(key="aemet_daily_objects", value=uploaded_objects)

        return len(uploaded_objects)

    def extract_aemet_historical(
        self,
        station_ids: Optional[List[str]] = None,
        start_year: int = 2020,
        end_year: int = 2025,
        **context: Any,
    ) -> int:
        """
        Extract historical climatological data from AEMET (2020-2025).

        This method extracts data year by year to handle AEMET's query limits.
        For each year, it splits into quarterly chunks.

        Args:
            station_ids: List of station IDs. If None, uses default capitals
            start_year: Start year (default 2020)
            end_year: End year (default 2025)
            context: Airflow context

        Returns:
            Number of files uploaded
        """
        self.log_start(f"AEMET Historical extraction ({start_year}-{end_year})")

        execution_date: str = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
        timestamp: str = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Default stations for Spanish capitals
        if not station_ids:
            station_ids = DEFAULT_STATION_IDS.copy()

        uploaded_objects: List[UploadedObject] = []

        for station_id in station_ids:
            self.logger.info(f"Extracting historical data for station {station_id}")

            for year in range(start_year, end_year + 1):
                # Split year into quarters to stay within API limits
                quarters = [
                    (f"{year}-01-01", f"{year}-03-31"),
                    (f"{year}-04-01", f"{year}-06-30"),
                    (f"{year}-07-01", f"{year}-09-30"),
                    (f"{year}-10-01", f"{year}-12-31"),
                ]

                all_records: List[Dict[str, Any]] = []

                for start_date, end_date in quarters:
                    # Don't query future dates
                    if start_date > execution_date:
                        continue

                    # Adjust end_date if it's in the future
                    if end_date > execution_date:
                        end_date = execution_date

                    start_iso = f"{start_date}T00:00:00UTC"
                    end_iso = f"{end_date}T23:59:59UTC"

                    try:
                        endpoint = AEMET_ENDPOINTS["daily_climatology"].format(
                            start=start_iso, end=end_iso, station=station_id
                        )

                        data = self._aemet_request(endpoint)
                        if data:
                            all_records.extend(data)
                            self.logger.info(
                                f"  Station {station_id}, {start_date} to {end_date}: "
                                f"{len(data)} records"
                            )

                        # Rate limiting
                        time.sleep(1)

                    except Exception as e:
                        self.log_error(
                            f"Error fetching {station_id} for {start_date}-{end_date}", e
                        )
                        continue

                # Store yearly data
                if all_records:
                    raw_data: Dict[str, Any] = {
                        "station_id": station_id,
                        "year": year,
                        "records": all_records,
                        "_metadata": {
                            "extraction_timestamp": timestamp,
                            "execution_date": execution_date,
                            "record_count": len(all_records),
                        },
                    }

                    object_path = (
                        f"historical/{year}/{station_id}/" f"historical_{year}_{timestamp}.json"
                    )
                    file_size = self.minio_client.upload_json(
                        BRONZE_AEMET_BUCKET, object_path, raw_data
                    )

                    uploaded_objects.append(
                        {
                            "object_path": object_path,
                            "station_id": station_id,
                            "year": year,
                            "record_count": len(all_records),
                            "file_size": file_size,
                        }
                    )
                    self.logger.info(
                        f"Stored {len(all_records)} records for station {station_id}, year {year}"
                    )

        self.log_end(f"Stored {len(uploaded_objects)} AEMET historical files")

        if context.get("task_instance"):
            context["task_instance"].xcom_push(
                key="aemet_historical_objects", value=uploaded_objects
            )

        return len(uploaded_objects)
