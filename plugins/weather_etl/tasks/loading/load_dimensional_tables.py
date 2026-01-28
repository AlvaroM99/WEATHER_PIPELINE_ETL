"""
Load Dimensional Tables
Populates all dimensional tables in PostgreSQL data warehouse
"""
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import os

from weather_etl.base import BaseLoader
from weather_etl.config.app_config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST
from weather_etl.utils.city_utils import get_capitals_dataframe

class DimensionalLoader(BaseLoader):
    """
    Loader for Dimensional Tables (City, Date, Week, Month, Seasons, Layers, Severity).
    """

    def get_db_connection(self):
        """Create PostgreSQL database connection"""
        return psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )

    def load(self, **context):
        """
        Load all dimensional tables in dependency order
        """
        self.log_start("Loading Dimensional Tables")
        
        try:
            self.load_dim_city()
            self.load_dim_date()
            self.load_dim_week()
            self.load_dim_month()
            self.load_dim_seasons()
            self.load_dim_layers()
            self.load_dim_severity()
            
            self.log_end("All dimensional tables loaded successfully")
            return "SUCCESS"
            
        except Exception as e:
            self.log_error("Error loading dimensional tables", e)
            raise

    def load_dim_city(self):
        """Load city dimension from app_config"""
        self.logger.info("Loading dim_city...")
        
        cities_df = get_capitals_dataframe()
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        try:
            # Insert cities
            for _, city in cities_df.iterrows():
                cur.execute("""
                    INSERT INTO dwh.dim_city (city_code, city_name, latitude, longitude, country_code)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (city_code) DO UPDATE SET
                        city_name = EXCLUDED.city_name,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude
                """, (
                    city['city_code'],
                    city['municipio_nombre'],
                    city['latitude'],
                    city['longitude'],
                    'ES'
                ))
            
            conn.commit()
            self.logger.info(f"✅ Loaded {len(cities_df)} cities into dim_city")
            
        finally:
            cur.close()
            conn.close()


    def load_dim_date(self):
        """Load date dimension (2020-2030)"""
        self.logger.info("Loading dim_date...")
        
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO dwh.dim_date (
                    id_calendar_day, dt_date, id_year, id_calendar_month, id_month,
                    id_calendar_week, id_week, id_weekday, id_quarter, 
                    id_calendar_quarter, id_calendar_semester, nm_day, ds_calendar_day
                )
                WITH dates AS (
                    SELECT CAST(date_series AS DATE) as d
                    FROM generate_series(
                        '2020-01-01'::DATE,
                        '2030-12-31'::DATE,
                        '1 day'::INTERVAL
                    ) AS date_series
                )
                SELECT 
                    CAST(TO_CHAR(d, 'YYYYMMDD') AS INT) as id_calendar_day,
                    d as dt_date,
                    CAST(EXTRACT(YEAR FROM d) AS INT) as id_year,
                    CAST(TO_CHAR(d, 'YYYYMM') AS INT) as id_calendar_month,
                    CAST(EXTRACT(MONTH FROM d) AS INT) as id_month,
                    CAST(TO_CHAR(d, 'IYYYIW') AS INT) as id_calendar_week,
                    CAST(EXTRACT(WEEK FROM d) AS INT) as id_week,
                    CAST(EXTRACT(ISODOW FROM d) AS INT) as id_weekday,
                    CAST(EXTRACT(QUARTER FROM d) AS INT) as id_quarter,
                    CAST(CONCAT(EXTRACT(YEAR FROM d), '0', EXTRACT(QUARTER FROM d)) AS INT) as id_calendar_quarter,
                    CASE WHEN EXTRACT(MONTH FROM d) <= 6 THEN 
                        CAST(CONCAT(EXTRACT(YEAR FROM d), '01') AS INT)
                    ELSE 
                        CAST(CONCAT(EXTRACT(YEAR FROM d), '02') AS INT)
                    END as id_calendar_semester,
                    TO_CHAR(d, 'Day') as nm_day,
                    TO_CHAR(d, 'Month DD, YYYY') as ds_calendar_day
                FROM dates
                ON CONFLICT (id_calendar_day) DO NOTHING
            """)
            
            conn.commit()
            row_count = cur.rowcount
            self.logger.info(f"✅ Loaded {row_count} dates into dim_date")
            
        finally:
            cur.close()
            conn.close()


    def load_dim_week(self):
        """Load week dimension (derived from dim_date)"""
        self.logger.info("Loading dim_week...")
        
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO dwh.dim_week (
                    id_calendar_week, id_year, id_week,
                    ds_calendar_week, ds_week_from_to,
                    dt_monday_of_week, dt_sunday_of_week
                )
                SELECT 
                    id_calendar_week,
                    MIN(id_year) as id_year,
                    MAX(id_week) as id_week,
                    CONCAT('Week ', MAX(id_week), ' ', MIN(id_year)) as ds_calendar_week,
                    CONCAT(MIN(dt_date), ' to ', MAX(dt_date)) as ds_week_from_to,
                    MIN(dt_date) as dt_monday_of_week,
                    MAX(dt_date) as dt_sunday_of_week
                FROM dwh.dim_date
                GROUP BY id_calendar_week
                ON CONFLICT (id_calendar_week) DO NOTHING
            """)
            
            conn.commit()
            row_count = cur.rowcount
            self.logger.info(f"✅ Loaded {row_count} weeks into dim_week")
            
        finally:
            cur.close()
            conn.close()


    def load_dim_month(self):
        """Load month dimension (derived from dim_date)"""
        self.logger.info("Loading dim_month...")
        
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO dwh.dim_month (
                    id_calendar_month, id_month, id_year, id_quarter, 
                    id_calendar_quarter, id_calendar_semester, 
                    ds_calendar_month, dt_month_first_day
                )
                SELECT DISTINCT
                    id_calendar_month,
                    id_month,
                    id_year,
                    id_quarter,
                    id_calendar_quarter,
                    id_calendar_semester,
                    TO_CHAR(dt_date, 'Month YYYY') as ds_calendar_month,
                    DATE_TRUNC('month', dt_date)::DATE as dt_month_first_day
                FROM dwh.dim_date
                ON CONFLICT (id_calendar_month) DO NOTHING
            """)
            
            conn.commit()
            row_count = cur.rowcount
            self.logger.info(f"✅ Loaded {row_count} months into dim_month")
            
        finally:
            cur.close()
            conn.close()


    def load_dim_seasons(self):
        """Load seasons dimension (static data)"""
        self.logger.info("Loading dim_seasons...")
        
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        try:
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
            
            conn.commit()
            self.logger.info(f"✅ Loaded 8 seasons into dim_seasons")
            
        finally:
            cur.close()
            conn.close()


    def load_dim_layers(self):
        """Load atmospheric/soil layers dimension (static data)"""
        self.logger.info("Loading dim_layers...")
        
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        try:
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
            
            conn.commit()
            self.logger.info(f"✅ Loaded 9 layers into dim_layers")
            
        finally:
            cur.close()
            conn.close()


    def load_dim_severity(self):
        """Load weather severity dimension (static data)"""
        self.logger.info("Loading dim_severity...")
        
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        try:
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
            
            conn.commit()
            self.logger.info(f"✅ Loaded 4 severity levels into dim_severity")
            
        finally:
            cur.close()
            conn.close()


def load_all_dimensional_tables(**context):
    """
    Wrapper function for Airflow PythonOperator
    """
    loader = DimensionalLoader()
    return loader.load(**context)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_all_dimensional_tables()

