"""
City Utilities
Helper functions for working with city data
"""
import pandas as pd
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from weather_etl.config.app_config import CITIES


def get_capitals_dataframe():
    """
    Convert CITIES config to DataFrame format expected by extraction scripts
    
    Returns:
        pd.DataFrame: DataFrame with city_code, municipio_nombre, latitude, longitude
    """
    return pd.DataFrame([
        {
            'city_code': str(i+1).zfill(5),
            'municipio_nombre': city['name'],
            'latitude': city['lat'],
            'longitude': city['lon']
        }
        for i, city in enumerate(CITIES)
    ])


def get_coastal_cities():
    """
    Get list of coastal cities for marine data extraction
    
    Returns:
        list: List of coastal city dictionaries with coordinates
    """
    # Get all cities with their generated codes
    df = get_capitals_dataframe()
    
    # Filter for known coastal cities
    target_cities = ["Barcelona", "Valencia", "Málaga"]
    
    coastal_cities = []
    for _, row in df.iterrows():
        if row['municipio_nombre'] in target_cities:
            coastal_cities.append({
                "name": row['municipio_nombre'],
                "lat": row['latitude'],
                "lon": row['longitude'],
                "code": row['city_code']  # Now matches dim_city (e.g. '00002')
            })
            
    return coastal_cities
