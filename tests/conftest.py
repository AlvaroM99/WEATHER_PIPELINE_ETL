"""
Shared pytest fixtures for Weather Pipeline ETL tests
"""
import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock
from datetime import datetime
from io import BytesIO


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
            "humidity": 65
        },
        "wind": {"speed": 3.5, "deg": 180, "gust": 5.0},
        "clouds": {"all": 10},
        "visibility": 10000,
        "dt": 1706543400,
        "sys": {"country": "ES"},
        "name": "Madrid"
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
            "et0_fao_evapotranspiration": [1.2, 1.5]
        },
        "city_code": "28079",
        "municipio_nombre": "Madrid",
        "_metadata": {
            "extraction_timestamp": "20260129_120000",
            "execution_date": "2026-01-29"
        }
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
            "weather_code": [2, 2, 3]
        },
        "city_code": "28079",
        "municipio_nombre": "Madrid",
        "_metadata": {
            "extraction_timestamp": "20260129_120000",
            "execution_date": "2026-01-29"
        }
    }


@pytest.fixture
def sample_cities():
    """Simulation of get_cities() from city_utils"""
    return [
        {'name': 'Madrid', 'lat': 40.4168, 'lon': -3.7038},
        {'name': 'Barcelona', 'lat': 41.3851, 'lon': 2.1734}
    ]


@pytest.fixture
def sample_capitals_df():
    """Simulation of get_capitals_dataframe()"""
    return pd.DataFrame([
        {
            'city_code': '28079',
            'municipio_nombre': 'Madrid',
            'latitude': 40.4168,
            'longitude': -3.7038
        },
        {
            'city_code': '08019',
            'municipio_nombre': 'Barcelona',
            'latitude': 41.3851,
            'longitude': 2.1734
        }
    ])


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
    return pd.DataFrame([
        {
            'city': 'Madrid',
            'country': 'ES',
            'latitude': 40.4168,
            'longitude': -3.7038,
            'temperature': 15.5,
            'feels_like': 14.2,
            'temp_min': 12.0,
            'temp_max': 18.0,
            'pressure': 1013,
            'humidity': 65,
            'weather_main': 'Clear',
            'weather_description': 'clear sky',
            'wind_speed': 3.5,
            'wind_deg': 180,
            'clouds': 10,
            'visibility': 10000,
            'date': '2026-01-29',
            'bronze_source': 'current/2026-01-29/madrid_20260129_120000.json'
        }
    ])


@pytest.fixture
def sample_silver_parquet_df():
    """Sample Silver bucket DataFrame for loading tests"""
    return pd.DataFrame([
        {
            'city_name': 'Madrid',
            'time': '2026-01-29',
            'temperature_2m_max': 18.5,
            'temperature_2m_min': 8.3,
            'precipitation_sum': 0.0,
            'weather_code': 0
        },
        {
            'city_name': 'Barcelona',
            'time': '2026-01-29',
            'temperature_2m_max': 19.2,
            'temperature_2m_min': 9.1,
            'precipitation_sum': 2.5,
            'weather_code': 61
        }
    ])


# ===== Mock Object Fixtures =====

@pytest.fixture
def mock_minio_client():
    """Mock MinIO client to avoid real storage operations"""
    mock = Mock()
    mock.upload_json.return_value = 1024
    mock.upload_parquet.return_value = 2048
    mock.read_json.return_value = {}
    mock.read_parquet.return_value = pd.DataFrame()

    # Mock list_objects to return empty list by default
    mock_obj = Mock()
    mock_obj.object_name = "test.json"
    mock.client.list_objects.return_value = []

    return mock


@pytest.fixture
def mock_db_connection():
    """Mock PostgreSQL database connection"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1
    mock_cursor.fetchall.return_value = [('Madrid', 1), ('Barcelona', 2)]

    # Make cursor() return the mock cursor
    mock_conn.cursor.return_value = mock_cursor

    # Support context manager
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

    return {
        'ds': '2026-01-29',
        'execution_date': datetime(2026, 1, 29),
        'task_instance': mock_ti
    }


# ===== Environment Setup =====

@pytest.fixture(autouse=True)
def setup_env(monkeypatch):
    """Setup environment variables automatically for all tests"""
    monkeypatch.setenv('OPENWEATHER_API_KEY', 'test_api_key_12345')
    monkeypatch.setenv('OPENMETEO_API_KEY', 'test_openmeteo_key')
    monkeypatch.setenv('POSTGRES_USER', 'testuser')
    monkeypatch.setenv('POSTGRES_PASSWORD', 'testpass')
    monkeypatch.setenv('POSTGRES_DB', 'testdb')
    monkeypatch.setenv('POSTGRES_HOST', 'localhost')
    monkeypatch.setenv('MINIO_ENDPOINT', 'localhost:9000')
    monkeypatch.setenv('MINIO_ROOT_USER', 'minioadmin')
    monkeypatch.setenv('MINIO_ROOT_PASSWORD', 'minioadmin')
    monkeypatch.setenv('MINIO_ACCESS_KEY', 'minioadmin')
    monkeypatch.setenv('MINIO_SECRET_KEY', 'minioadmin')
    monkeypatch.setenv('MINIO_SECURE', 'False')
