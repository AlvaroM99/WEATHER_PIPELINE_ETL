

import logging
import psycopg2
from datetime import date, timedelta
from src.weather_config.app_config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_HOST

class DimensionalLoader:
    """
    Dimensional Loader Manager.
    Handles loading for dimensional tables (Time, City, etc.).
    """

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def log_start(self, msg: str):
        self.logger.info(f"🚀 START: {msg}")

    def log_end(self, msg: str):
        self.logger.info(f"🏁 END: {msg}")

    def log_error(self, msg: str, error: Exception = None):
        if error:
            self.logger.error(f"❌ ERROR: {msg} - {str(error)}")
        else:
            self.logger.error(f"❌ ERROR: {msg}")

    def get_db_connection(self):
        return psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )

    def load_dim_date(self):
        """Generates and loads date dimension (2020-2030)"""
        self.log_start("Loading dim_date...")
        conn = self.get_db_connection()
        cur = conn.cursor()

        start_date = date(2020, 1, 1)
        end_date = date(2030, 12, 31)
        delta = end_date - start_date

        try:
            for i in range(delta.days + 1):
                day = start_date + timedelta(days=i)
                id_date = int(day.strftime('%Y%m%d'))
                year = day.year
                month = day.month
                week = day.isocalendar()[1]
                quarter = (month - 1) // 3 + 1
                
                cur.execute("""
                    INSERT INTO dwh.dim_date (
                        id_calendar_day, dt_date, id_year, id_calendar_month, id_month, 
                        id_calendar_week, id_week, id_weekday, id_quarter, 
                        id_calendar_quarter, id_calendar_semester, nm_day, ds_calendar_day
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id_calendar_day) DO NOTHING;
                """, (
                    id_date, day, year, int(f"{year}{month:02d}"), month,
                    int(f"{year}{week:02d}"), week, day.isoweekday(), quarter,
                    int(f"{year}{quarter}"), (month-1)//6 + 1, day.strftime('%A'), day.strftime('%Y-%m-%d')
                ))
            
            conn.commit()
            self.log_end("dim_date loaded successfully.")
        except Exception as e:
            conn.rollback()
            self.log_error(f"Error loading dim_date", e)
            raise
        finally:
            cur.close()
            conn.close()

    def load_dim_city(self):
        """Loads Spanish capitals into dim_city"""
        self.log_start("Loading dim_city...")
        conn = self.get_db_connection()
        cur = conn.cursor()

        cities = [
            ('MAD', 'Madrid', 40.4168, -3.7038),
            ('BCN', 'Barcelona', 41.3851, 2.1734),
            ('VLC', 'Valencia', 39.4699, -0.3763),
            ('SEV', 'Sevilla', 37.3891, -5.9845),
            ('BIO', 'Bilbao', 43.2630, -2.9350)
        ]

        try:
            for code, name, lat, lon in cities:
                cur.execute("""
                    INSERT INTO dwh.dim_city (city_code, city_name, latitude, longitude, country_code)
                    VALUES (%s, %s, %s, %s, 'ES')
                    ON CONFLICT (city_code) DO UPDATE 
                    SET city_name = EXCLUDED.city_name,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude;
                """, (code, name, lat, lon))
            
            conn.commit()
            self.log_end("dim_city loaded successfully.")
        except Exception as e:
            conn.rollback()
            self.log_error(f"Error loading dim_city", e)
            raise
        finally:
            cur.close()
            conn.close()

    def load_dim_week(self):
        """Populates dim_week based on dim_date"""
        self.log_start("Loading dim_week...")
        conn = self.get_db_connection()
        cur = conn.cursor()
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
            conn.commit()
            self.log_end("dim_week loaded successfully.")
        except Exception as e:
            conn.rollback()
            self.log_error("Error loading dim_week", e)
            raise
        finally:
            cur.close()
            conn.close()

    def load_dim_month(self):
        """Populates dim_month based on dim_date"""
        self.log_start("Loading dim_month...")
        conn = self.get_db_connection()
        cur = conn.cursor()
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
            conn.commit()
            self.log_end("dim_month loaded successfully.")
        except Exception as e:
            conn.rollback()
            self.log_error("Error loading dim_month", e)
            raise
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

    def load_all_dimensional_tables(self, **context):
        """Wrapper to load all dimensional tables"""
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
        
        self.log_end("All dimensional tables loaded.")

