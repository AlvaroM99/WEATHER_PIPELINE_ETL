"""
Application Configuration
Shared settings/constants for the weather pipeline
"""

import os

# OpenWeatherMap API Key
API_KEY = os.getenv("OPENWEATHER_API_KEY")

# PostgreSQL Configuration
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = "postgres"  # Docker service name

# Cities to fetch weather data for (major Spanish cities)
CITIES = [
    {"name": "Madrid", "lat": 40.4168, "lon": -3.7038},
    {"name": "Barcelona", "lat": 41.3851, "lon": 2.1734},
    {"name": "Valencia", "lat": 39.4699, "lon": -0.3763},
    {"name": "Sevilla", "lat": 37.3891, "lon": -5.9845},
    {"name": "Bilbao", "lat": 43.2630, "lon": -2.9350},
    {"name": "Málaga", "lat": 36.7213, "lon": -4.4214},
    {"name": "Zaragoza", "lat": 41.6488, "lon": -0.8891},
]
