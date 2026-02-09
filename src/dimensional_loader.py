"""
Dimensional Loader Module

Type-annotated module for loading dimensional tables into the data warehouse.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from io import StringIO
from typing import Any, Dict, Optional

import pandas as pd
import psycopg2
import requests

from src.base_loader import BaseLoader
from src.config.apis.aemet_config import DEFAULT_STATIONS
from src.type_aliases import AirflowContext
from src.utils.city_utils import CITIES_CSV_URL
from src.utils.http_utils import get_retrying_session


class DimensionalLoader(BaseLoader):
    """
    Dimensional Loader Manager.

    Handles loading for dimensional tables (Time, City, etc.).

    Attributes:
        logger: Logger instance for this class (via BaseLoader)
    """

    def __init__(self) -> None:
        """Initialize the DimensionalLoader with logger and HTTP session."""
        super().__init__()
        self.session: requests.Session = get_retrying_session()

    def load_dim_date(self) -> None:
        """
        Generate and load date dimension (2020-2030).

        Creates date records with calendar attributes for the full decade.
        """
        self.log_start("Loading dim_date...")

        start_date: date = date(2020, 1, 1)
        end_date: date = date(2030, 12, 31)
        delta: timedelta = end_date - start_date

        with self.connection() as conn:
            with conn.cursor() as cur:
                try:
                    for i in range(delta.days + 1):
                        day = start_date + timedelta(days=i)
                        id_date = int(day.strftime("%Y%m%d"))
                        year = day.year
                        month = day.month
                        week = day.isocalendar()[1]
                        quarter = (month - 1) // 3 + 1

                        cur.execute(
                            """
                            INSERT INTO dwh.dim_date (
                                id_calendar_day, dt_date, id_year, id_calendar_month, id_month,
                                id_calendar_week, id_week, id_weekday, id_quarter,
                                id_calendar_quarter, id_calendar_semester, nm_day, ds_calendar_day
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id_calendar_day) DO NOTHING;
                        """,
                            (
                                id_date,
                                day,
                                year,
                                int(f"{year}{month:02d}"),
                                month,
                                int(f"{year}{week:02d}"),
                                week,
                                day.isoweekday(),
                                quarter,
                                int(f"{year}{quarter}"),
                                (month - 1) // 6 + 1,
                                day.strftime("%A"),
                                day.strftime("%Y-%m-%d"),
                            ),
                        )

                    self.log_end("dim_date loaded successfully.")
                except Exception as e:
                    self.log_error("Error loading dim_date", e)
                    raise

    def load_dim_city(self) -> None:
        """
        Load Spanish capitals from GitHub CSV into dim_city.

        Downloads city data from the configured GitHub repository and upserts
        into the database.
        """
        self.log_start("Loading dim_city from GitHub CSV...")

        with self.connection() as conn:
            with conn.cursor() as cur:
                try:
                    # Download CSV from GitHub
                    self.logger.info("Downloading cities CSV from GitHub...")
                    response = self.session.get(CITIES_CSV_URL, timeout=10)
                    response.raise_for_status()

                    # Parse CSV
                    csv_data = StringIO(response.text)
                    df = pd.read_csv(csv_data, sep=",")  # CSV format with commas

                    # Validate required columns
                    required_cols = [
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

                    self.logger.info(f"Successfully downloaded {len(df)} cities from GitHub")

                    # Insert each city into database
                    for _, row in df.iterrows():
                        cur.execute(
                            """
                            INSERT INTO dwh.dim_city (city_code, city_name, latitude, longitude, country_code, is_coastal)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (city_code) DO UPDATE
                            SET city_name = EXCLUDED.city_name,
                                latitude = EXCLUDED.latitude,
                                longitude = EXCLUDED.longitude,
                                country_code = EXCLUDED.country_code,
                                is_coastal = EXCLUDED.is_coastal;
                        """,
                            (
                                row["city_code"],
                                row["city_name"],
                                row["latitud"],
                                row["longitud"],
                                row["country_code"],
                                row["is_coastal"],
                            ),
                        )

                    self.log_end(f"dim_city loaded successfully with {len(df)} cities.")
                except requests.RequestException as e:
                    self.log_error("Error downloading cities CSV from GitHub", e)
                    raise
                except Exception as e:
                    self.log_error("Error loading dim_city", e)
                    raise

    def load_dim_week(self) -> None:
        """Populate dim_week based on dim_date."""
        self.log_start("Loading dim_week...")

        with self.connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("""
                        INSERT INTO dwh.dim_week (
                            id_calendar_week, id_year, id_week,
                            ds_calendar_week, ds_week_from_to,
                            dt_monday_of_week, dt_sunday_of_week
                        )
                        SELECT DISTINCT
                            id_calendar_week,
                            id_year,
                            id_week,
                            CONCAT('W', LPAD(id_week::text, 2, '0'), '-', id_year) as ds_calendar_week,
                            CONCAT(MIN(dt_date)::text, ' / ', MAX(dt_date)::text) as ds_week_from_to,
                            MIN(dt_date) as dt_monday_of_week,
                            MAX(dt_date) as dt_sunday_of_week
                        FROM dwh.dim_date
                        GROUP BY id_calendar_week, id_year, id_week
                        ON CONFLICT (id_calendar_week) DO NOTHING;
                    """)
                    self.log_end("dim_week loaded successfully.")
                except Exception as e:
                    self.log_error("Error loading dim_week", e)
                    raise

    def load_dim_month(self) -> None:
        """Populate dim_month based on dim_date."""
        self.log_start("Loading dim_month...")

        with self.connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("""
                        INSERT INTO dwh.dim_month (
                            id_calendar_month, id_month, id_year,
                            id_quarter, id_calendar_quarter, id_calendar_semester,
                            ds_calendar_month, dt_month_first_day
                        )
                        SELECT DISTINCT
                            id_calendar_month,
                            id_month,
                            id_year,
                            id_quarter,
                            id_calendar_quarter,
                            id_calendar_semester,
                            TO_CHAR(MIN(dt_date), 'Month YYYY') as ds_calendar_month,
                            MIN(dt_date) as dt_month_first_day
                        FROM dwh.dim_date
                        GROUP BY
                            id_calendar_month, id_month, id_year,
                            id_quarter, id_calendar_quarter, id_calendar_semester
                        ON CONFLICT (id_calendar_month) DO NOTHING;
                    """)
                    self.log_end("dim_month loaded successfully.")
                except Exception as e:
                    self.log_error("Error loading dim_month", e)
                    raise

    def load_dim_seasons(self) -> None:
        """Load seasons dimension (static data)."""
        self.logger.info("Loading dim_seasons...")

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dwh.dim_seasons (
                        season_id, name, hemisphere, season_type,
                        start_month, end_month, precipitation_type,
                        is_growing_season, color_code, description
                    ) VALUES
                    -- Hemisferio Norte (Meteorological)
                    (1, 'Invierno',  'North', 'Meteorological', 12, 2,  'Normal', false, '#A5F2F3', 'Diciembre a Febrero'),
                    (2, 'Primavera', 'North', 'Meteorological', 3,  5,  'Normal', true,  '#98FB98', 'Marzo a Mayo'),
                    (3, 'Verano',    'North', 'Meteorological', 6,  8,  'Dry',    true,  '#FFFFE0', 'Junio a Agosto'),
                    (4, 'Otoño',     'North', 'Meteorological', 9,  11, 'Wet',    false, '#FFD700',  'Septiembre a Noviembre'),
                    -- Hemisferio Sur (Meteorological)
                    (5, 'Verano',    'South', 'Meteorological', 12, 2,  'Dry',    true,  '#FFFFE0', 'Diciembre a Febrero'),
                    (6, 'Otoño',     'South', 'Meteorological', 3,  5,  'Wet',    false, '#FFD700', 'Marzo a Mayo'),
                    (7, 'Invierno',  'South', 'Meteorological', 6,  8,  'Normal', false, '#A5F2F3', 'Junio a Agosto'),
                    (8, 'Primavera', 'South', 'Meteorological', 9,  11, 'Normal', true,  '#98FB98', 'Septiembre a Noviembre')
                    ON CONFLICT (season_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        hemisphere = EXCLUDED.hemisphere,
                        season_type = EXCLUDED.season_type,
                        start_month = EXCLUDED.start_month,
                        end_month = EXCLUDED.end_month,
                        precipitation_type = EXCLUDED.precipitation_type,
                        is_growing_season = EXCLUDED.is_growing_season,
                        color_code = EXCLUDED.color_code,
                        description = EXCLUDED.description
                """)
            self.logger.info("Loaded 8 seasons into dim_seasons")

    def load_dim_layers(self) -> None:
        """Load atmospheric/soil layers dimension (static data)."""
        self.logger.info("Loading dim_layers...")

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dwh.dim_layers (
                        layer_id, name, medium, vertical_level_m,
                        pressure_hpa, is_standard_wmo, layer_category, description
                    ) VALUES
                    -- ATMOSPHERE / SURFACE
                    (1, 'Surface',  'Surface', 0.0,   1013.2, true,  'Surface',          'Surface level'),
                    (2, '2m Air',   'Air',     2.0,   1013.0, true,  'Surface Boundary', 'Standard air temperature height'),
                    (3, '10m Wind', 'Air',     10.0,  1012.0, true,  'Surface Boundary', 'Standard wind measurement height'),
                    (4, '80m Air',  'Air',     80.0,  1004.0, false, 'Low Atmosphere',   'Wind turbine hub height / low shearing'),

                    -- PRESSURE LEVELS (UPPER AIR)
                    (10, '850 hPa', 'Air',     1500.0, 850.0, true,  'Troposphere',      'Top of planetary boundary layer approx'),
                    (11, '500 hPa', 'Air',     5500.0, 500.0, true,  'Troposphere',      'Middle troposphere / steering level'),

                    -- SOIL (Negative values for depth)
                    (20, 'Soil 6cm',  'Soil', -0.06, null, true,  'Topsoil',   'Shallow root zone'),
                    (21, 'Soil 18cm', 'Soil', -0.18, null, true,  'Root Zone', 'Middle root zone'),
                    (22, 'Soil 54cm', 'Soil', -0.54, null, false, 'Subsoil',   'Deep soil moisture')
                    ON CONFLICT (layer_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        medium = EXCLUDED.medium,
                        vertical_level_m = EXCLUDED.vertical_level_m,
                        pressure_hpa = EXCLUDED.pressure_hpa,
                        is_standard_wmo = EXCLUDED.is_standard_wmo,
                        layer_category = EXCLUDED.layer_category,
                        description = EXCLUDED.description
                """)
            self.logger.info("Loaded 9 layers into dim_layers")

    def load_dim_severity(self) -> None:
        """Load weather severity dimension (static data)."""
        self.logger.info("Loading dim_severity...")

        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dwh.dim_severity (severity_id, severity_level, color_code, description) VALUES
                    (1, 'Verde (Sin Riesgo)', '#00FF00', 'No se espera que el tiempo cause impactos significativos.'),
                    (2, 'Amarillo (Riesgo Bajo)', '#FFFF00', 'Riesgo bajo para la población general, pero ciertas actividades pueden verse afectadas.'),
                    (3, 'Naranja (Riesgo Importante)', '#FFA500', 'Riesgo meteorológico importante con fenómenos inusuales y peligrosos.'),
                    (4, 'Rojo (Riesgo Extremo)', '#FF0000', 'Fenómenos excepcionalmente intensos con alto riesgo para la población.')
                    ON CONFLICT (severity_id) DO UPDATE SET
                        severity_level = EXCLUDED.severity_level,
                        color_code = EXCLUDED.color_code,
                        description = EXCLUDED.description
                """)
            self.logger.info("Loaded 4 severity levels into dim_severity")

    def load_dim_aemet_stations(self) -> None:
        """
        Load AEMET stations dimension with default Spanish capital stations.

        This creates initial station records for the most common Spanish cities.
        The full station list will be populated from the AEMET API extraction.
        Uses DEFAULT_STATIONS from aemet_config.py for centralized configuration.
        """
        self.logger.info("Loading dim_aemet_stations with default stations...")

        with self.connection() as conn:
            with conn.cursor() as cur:
                try:
                    for station in DEFAULT_STATIONS:
                        cur.execute(
                            """
                            INSERT INTO dwh.dim_aemet_stations (
                                station_id, station_name, province, altitude, latitude, longitude
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (station_id) DO UPDATE SET
                                station_name = EXCLUDED.station_name,
                                province = EXCLUDED.province,
                                altitude = EXCLUDED.altitude,
                                latitude = EXCLUDED.latitude,
                                longitude = EXCLUDED.longitude,
                                updated_at = CURRENT_TIMESTAMP
                            """,
                            station,
                        )

                    self.logger.info(
                        f"Loaded {len(DEFAULT_STATIONS)} default AEMET stations into dim_aemet_stations"
                    )

                except Exception as e:
                    self.log_error("Error loading dim_aemet_stations", e)
                    raise

    def load_all_dimensional_tables(self, **context: Any) -> None:
        """
        Wrapper to load all dimensional tables.

        Args:
            context: Airflow context (not used, but required for task compatibility)
        """
        self.log_start("Starting usage of load_all_dimensional_tables...")

        # Order matters: dim_date first (dependency for week and month)
        self.load_dim_date()

        # Dependent on dim_date
        self.load_dim_week()
        self.load_dim_month()

        # Independent
        self.load_dim_city()
        self.load_dim_seasons()
        self.load_dim_layers()
        self.load_dim_severity()
        self.load_dim_aemet_stations()

        self.log_end("All dimensional tables loaded.")
