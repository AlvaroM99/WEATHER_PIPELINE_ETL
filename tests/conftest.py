"""
Shared pytest fixtures for Weather Pipeline ETL tests
"""

from datetime import datetime
from io import BytesIO
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest

# ===== Sample Data Fixtures =====


@pytest.fixture
def sample_openweather_response():
    """Sample OpenWeather API response based on real structure"""
    return {
        "coord": {"lon": -3.7038, "lat": 40.4168},
        "weather": [{"id": 800, "main": "Clear", "description": "clear sky"}],
        "main": {
            "temp": 15.5,
            "feels_like": 14.2,
            "temp_min": 12.0,
            "temp_max": 18.0,
            "pressure": 1013,
            "humidity": 65,
        },
        "wind": {"speed": 3.5, "deg": 180, "gust": 5.0},
        "clouds": {"all": 10},
        "visibility": 10000,
        "dt": 1706543400,
        "sys": {"country": "ES"},
        "name": "Madrid",
    }


@pytest.fixture
def sample_openmeteo_daily_response():
    """Sample Open-Meteo daily forecast response"""
    return {
        "latitude": 40.4168,
        "longitude": -3.7038,
        "elevation": 667.0,
        "timezone": "Europe/Berlin",
        "daily": {
            "time": ["2026-01-29", "2026-01-30"],
            "temperature_2m_max": [18.5, 19.2],
            "temperature_2m_min": [8.3, 9.1],
            "apparent_temperature_max": [17.2, 18.5],
            "apparent_temperature_min": [7.1, 8.3],
            "precipitation_sum": [0.0, 2.5],
            "rain_sum": [0.0, 2.5],
            "showers_sum": [0.0, 0.0],
            "snowfall_sum": [0.0, 0.0],
            "precipitation_hours": [0.0, 3.0],
            "wind_speed_10m_max": [15.2, 18.5],
            "wind_gusts_10m_max": [25.3, 28.7],
            "wind_direction_10m_dominant": [180, 195],
            "sunrise": ["2026-01-29T07:45:00", "2026-01-30T07:44:00"],
            "sunset": ["2026-01-29T18:15:00", "2026-01-30T18:16:00"],
            "shortwave_radiation_sum": [5.2, 6.1],
            "weather_code": [0, 61],
            "et0_fao_evapotranspiration": [1.2, 1.5],
        },
        "city_code": "28079",
        "municipio_nombre": "Madrid",
        "_metadata": {"extraction_timestamp": "20260129_120000", "execution_date": "2026-01-29"},
    }


@pytest.fixture
def sample_openmeteo_hourly_response():
    """Sample Open-Meteo hourly forecast response"""
    return {
        "latitude": 40.4168,
        "longitude": -3.7038,
        "timezone": "Europe/Berlin",
        "hourly": {
            "time": ["2026-01-29T00:00", "2026-01-29T01:00", "2026-01-29T02:00"],
            "temperature_2m": [12.5, 11.8, 11.2],
            "temperature_80m": [13.2, 12.5, 11.9],
            "apparent_temperature": [11.5, 10.9, 10.3],
            "relative_humidity_2m": [70, 72, 75],
            "dew_point_2m": [7.5, 7.2, 7.0],
            "precipitation_probability": [0, 0, 10],
            "precipitation": [0.0, 0.0, 0.1],
            "rain": [0.0, 0.0, 0.1],
            "snowfall": [0.0, 0.0, 0.0],
            "pressure_msl": [1013.5, 1013.2, 1013.0],
            "surface_pressure": [945.2, 945.0, 944.8],
            "cloud_cover": [20, 25, 30],
            "visibility": [10000, 9500, 9000],
            "uv_index": [0.0, 0.0, 0.0],
            "wind_speed_10m": [8.5, 9.2, 10.1],
            "wind_direction_10m": [180, 185, 190],
            "wind_gusts_10m": [15.2, 16.5, 17.8],
            "weather_code": [2, 2, 3],
        },
        "city_code": "28079",
        "municipio_nombre": "Madrid",
        "_metadata": {"extraction_timestamp": "20260129_120000", "execution_date": "2026-01-29"},
    }


@pytest.fixture
def sample_air_quality_response():
    """Sample Open-Meteo air quality API response"""
    return {
        "latitude": 40.4168,
        "longitude": -3.7038,
        "timezone": "Europe/Berlin",
        "hourly": {
            "time": ["2026-01-29T00:00", "2026-01-29T01:00", "2026-01-29T02:00"],
            "pm10": [15.2, 14.8, 16.1],
            "pm2_5": [8.5, 8.2, 9.0],
            "carbon_monoxide": [200.5, 198.2, 205.1],
            "nitrogen_dioxide": [12.3, 11.8, 13.1],
            "sulphur_dioxide": [2.5, 2.3, 2.8],
            "ozone": [45.2, 44.8, 46.1],
            "aerosol_optical_depth": [0.15, 0.14, 0.16],
            "dust": [5.2, 5.0, 5.5],
        },
    }


@pytest.fixture
def sample_pollen_response():
    """Sample Open-Meteo pollen API response"""
    return {
        "latitude": 40.4168,
        "longitude": -3.7038,
        "timezone": "Europe/Berlin",
        "hourly": {
            "time": ["2026-01-29T00:00", "2026-01-29T01:00", "2026-01-29T02:00"],
            "alder_pollen": [0.0, 0.0, 0.0],
            "birch_pollen": [5.2, 5.5, 5.8],
            "grass_pollen": [12.5, 13.2, 14.1],
            "mugwort_pollen": [0.0, 0.0, 0.0],
            "olive_pollen": [8.5, 8.8, 9.2],
            "ragweed_pollen": [0.0, 0.0, 0.0],
        },
    }


@pytest.fixture
def sample_marine_response():
    """Sample Open-Meteo marine API response (daily aggregations)"""
    return {
        "latitude": 41.3851,
        "longitude": 2.1734,
        "timezone": "Europe/Berlin",
        "daily": {
            "time": ["2026-01-29", "2026-01-30", "2026-01-31"],
            "wave_height_max": [1.5, 1.6, 1.7],
            "wave_direction_dominant": [180, 185, 190],
            "wave_period_max": [8.5, 8.8, 9.0],
            "wind_wave_height_max": [0.8, 0.9, 1.0],
            "swell_wave_height_max": [1.2, 1.3, 1.4],
        },
    }


@pytest.fixture
def sample_cities():
    """Simulation of get_cities() from city_utils"""
    return [
        {"name": "Madrid", "lat": 40.4168, "lon": -3.7038},
        {"name": "Barcelona", "lat": 41.3851, "lon": 2.1734},
    ]


@pytest.fixture
def sample_capitals_df():
    """Simulation of get_capitals_dataframe()"""
    return pd.DataFrame(
        [
            {
                "city_code": "28079",
                "municipio_nombre": "Madrid",
                "latitude": 40.4168,
                "longitude": -3.7038,
            },
            {
                "city_code": "08019",
                "municipio_nombre": "Barcelona",
                "latitude": 41.3851,
                "longitude": 2.1734,
            },
        ]
    )


@pytest.fixture
def sample_github_csv():
    """Sample GitHub CSV content for cities"""
    return """city_code,city_name,latitud,longitud,country_code,is_coastal
28079,Madrid,40.4168,-3.7038,ES,0
08019,Barcelona,41.3851,2.1734,ES,1
41091,Sevilla,37.3886,-5.9823,ES,0
29067,Málaga,36.7213,-4.4214,ES,1
"""


@pytest.fixture
def sample_transformed_df():
    """Sample transformed weather DataFrame"""
    return pd.DataFrame(
        [
            {
                "city": "Madrid",
                "country": "ES",
                "latitude": 40.4168,
                "longitude": -3.7038,
                "temperature": 15.5,
                "feels_like": 14.2,
                "temp_min": 12.0,
                "temp_max": 18.0,
                "pressure": 1013,
                "humidity": 65,
                "weather_main": "Clear",
                "weather_description": "clear sky",
                "wind_speed": 3.5,
                "wind_deg": 180,
                "clouds": 10,
                "visibility": 10000,
                "date": "2026-01-29",
                "bronze_source": "current/2026-01-29/madrid_20260129_120000.json",
            }
        ]
    )


@pytest.fixture
def sample_silver_parquet_df():
    """Sample Silver bucket DataFrame for loading tests"""
    return pd.DataFrame(
        [
            {
                "city_name": "Madrid",
                "time": "2026-01-29",
                "temperature_2m_max": 18.5,
                "temperature_2m_min": 8.3,
                "precipitation_sum": 0.0,
                "weather_code": 0,
            },
            {
                "city_name": "Barcelona",
                "time": "2026-01-29",
                "temperature_2m_max": 19.2,
                "temperature_2m_min": 9.1,
                "precipitation_sum": 2.5,
                "weather_code": 61,
            },
        ]
    )


# ===== Mock Object Fixtures =====


@pytest.fixture
def mock_minio_client():
    """Mock MinIO client to avoid real storage operations"""
    mock = Mock()
    mock.upload_json.return_value = 1024
    mock.upload_parquet.return_value = 2048
    mock.read_json.return_value = {}
    mock.read_parquet.return_value = pd.DataFrame()

    # Mock list_objects to return iterable (not subscriptable)
    mock_obj = Mock()
    mock_obj.object_name = "test.json"
    mock_obj.size = 1024
    # Return iterator instead of list to avoid subscriptable errors
    mock.client.list_objects.return_value = iter([mock_obj])
    mock.list_objects.return_value = iter([mock_obj])

    return mock


@pytest.fixture
def mock_db_connection():
    """Mock PostgreSQL database connection with UTF-8 encoding."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1
    mock_cursor.fetchall.return_value = [(("Madrid", 1), ("Barcelona", 2))]
    mock_cursor.fetchone.return_value = (1,)
    
    # Add UTF-8 encoding to prevent Unicode decode errors
    mock_conn.encoding = 'UTF8'
    
    # Make cursor() return the mock cursor with context manager support
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    
    # Support connection context manager
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.__exit__.return_value = None
    
    return mock_conn


@pytest.fixture
def mock_session():
    """Mock requests Session for HTTP calls"""
    mock = Mock()
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {}
    mock.get.return_value = mock_response

    return mock


@pytest.fixture
def mock_airflow_context():
    """Mock Airflow task context (ds, task_instance, xcom)"""
    mock_ti = Mock()
    mock_ti.xcom_pull.return_value = None
    mock_ti.xcom_push.return_value = None

    return {"ds": "2026-01-29", "execution_date": datetime(2026, 1, 29), "task_instance": mock_ti}


# ===== Environment Setup =====


@pytest.fixture(autouse=True)
def setup_env(monkeypatch):
    """Setup environment variables automatically for all tests"""
    monkeypatch.setenv("OPENWEATHER_API_KEY", "test_api_key_12345")
    monkeypatch.setenv("OPENMETEO_API_KEY", "test_openmeteo_key")
    monkeypatch.setenv("POSTGRES_USER", "testuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
    monkeypatch.setenv("POSTGRES_DB", "testdb")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("MINIO_ROOT_USER", "minioadmin")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "minioadmin")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_SECRET_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_SECURE", "False")
    monkeypatch.setenv("AEMET_API_KEY", "test_aemet_api_key")


# ===== AEMET Fixtures =====


@pytest.fixture
def sample_aemet_stations_response():
    """Sample AEMET stations API response"""
    return [
        {
            "indicativo": "3129",
            "nombre": "MADRID, RETIRO",
            "provincia": "MADRID",
            "altitud": "667",
            "latitud": "402455N",
            "longitud": "034041W",
            "indsinop": "08221",
        },
        {
            "indicativo": "0076",
            "nombre": "BARCELONA, FABRA",
            "provincia": "BARCELONA",
            "altitud": "412",
            "latitud": "412512N",
            "longitud": "021730E",
            "indsinop": "08181",
        },
    ]


@pytest.fixture
def sample_aemet_daily_response():
    """Sample AEMET daily climatology API response"""
    return [
        {
            "fecha": "2026-01-29",
            "indicativo": "3129",
            "nombre": "MADRID, RETIRO",
            "provincia": "MADRID",
            "tmed": "8,5",
            "tmin": "2,3",
            "tmax": "14,7",
            "prec": "0,0",
            "velmedia": "2,8",
            "racha": "8,3",
            "dir": "270",
            "sol": "7,5",
            "presMax": "1025,3",
            "presMin": "1020,1",
            "hrMedia": "55",
            "hrMin": "30",
            "hrMax": "80",
        },
        {
            "fecha": "2026-01-28",
            "indicativo": "3129",
            "nombre": "MADRID, RETIRO",
            "provincia": "MADRID",
            "tmed": "7,2",
            "tmin": "1,5",
            "tmax": "13,0",
            "prec": "2,5",
            "velmedia": "3,5",
            "racha": "12,5",
            "dir": "315",
            "sol": "5,2",
            "presMax": "1022,0",
            "presMin": "1018,5",
            "hrMedia": "65",
            "hrMin": "40",
            "hrMax": "90",
        },
    ]


@pytest.fixture
def sample_aemet_bronze_data():
    """Sample AEMET bronze layer data structure"""
    return {
        "stations": [
            {
                "indicativo": "3129",
                "nombre": "MADRID, RETIRO",
                "provincia": "MADRID",
                "altitud": "667",
                "latitud": "402455N",
                "longitud": "034041W",
                "indsinop": "08221",
            }
        ],
        "_metadata": {
            "extraction_timestamp": "20260129_120000",
            "execution_date": "2026-01-29",
            "station_count": 1,
        },
    }


@pytest.fixture
def sample_aemet_daily_bronze_data():
    """Sample AEMET daily climatology bronze data"""
    return {
        "station_id": "3129",
        "start_date": "2026-01-01",
        "end_date": "2026-01-29",
        "records": [
            {
                "fecha": "2026-01-29",
                "indicativo": "3129",
                "nombre": "MADRID, RETIRO",
                "provincia": "MADRID",
                "tmed": "8,5",
                "tmin": "2,3",
                "tmax": "14,7",
                "prec": "0,0",
            }
        ],
        "_metadata": {
            "extraction_timestamp": "20260129_120000",
            "execution_date": "2026-01-29",
            "record_count": 1,
        },
    }


@pytest.fixture
def sample_aemet_silver_df():
    """Sample AEMET silver layer DataFrame for stations"""
    return pd.DataFrame(
        [
            {
                "station_id": "3129",
                "station_name": "MADRID, RETIRO",
                "province": "MADRID",
                "altitude": 667.0,
                "latitude": 40.415278,
                "longitude": -3.678056,
                "synop_code": "08221",
            },
            {
                "station_id": "0076",
                "station_name": "BARCELONA, FABRA",
                "province": "BARCELONA",
                "altitude": 412.0,
                "latitude": 41.419444,
                "longitude": 2.291667,
                "synop_code": "08181",
            },
        ]
    )


@pytest.fixture
def sample_aemet_daily_silver_df():
    """Sample AEMET daily climatology silver DataFrame"""
    return pd.DataFrame(
        [
            {
                "station_id": "3129",
                "station_name": "MADRID, RETIRO",
                "province": "MADRID",
                "date": "2026-01-29",
                "temp_avg": 8.5,
                "temp_min": 2.3,
                "temp_max": 14.7,
                "precipitation": 0.0,
                "wind_speed_avg": 2.8,
                "wind_gust_max": 8.3,
                "wind_direction": 270.0,
                "sunshine_hours": 7.5,
                "pressure_max": 1025.3,
                "pressure_min": 1020.1,
                "humidity_avg": 55.0,
                "humidity_min": 30.0,
                "humidity_max": 80.0,
            }
        ]
    )
