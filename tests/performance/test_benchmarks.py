"""
Performance benchmark tests for ETL pipeline operations.

Uses pytest-benchmark to measure execution time of critical operations.
Run with: pytest tests/performance/ -m benchmark --benchmark-only
"""

from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("pytest_benchmark")


def _generate_observation_df(n_rows: int) -> pd.DataFrame:
    """Generate a synthetic weather observation DataFrame with n_rows."""
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "city": [f"City_{i % 50}" for i in range(n_rows)],
        "temperature": rng.uniform(0, 40, n_rows),
        "feels_like": rng.uniform(0, 40, n_rows),
        "temp_min": rng.uniform(0, 30, n_rows),
        "temp_max": rng.uniform(10, 45, n_rows),
        "pressure": rng.integers(990, 1040, n_rows).astype(float),
        "humidity": rng.integers(20, 100, n_rows).astype(float),
        "visibility": rng.integers(1000, 10000, n_rows).astype(float),
        "wind_speed": rng.uniform(0, 30, n_rows),
        "wind_deg": rng.integers(0, 360, n_rows).astype(float),
        "wind_gust": rng.uniform(0, 50, n_rows),
        "clouds": rng.integers(0, 100, n_rows).astype(float),
        "weather_id": rng.choice([800, 801, 802, 500, 300], n_rows),
        "weather_main": ["Clear"] * n_rows,
        "weather_description": ["clear sky"] * n_rows,
        "rain_1h": [np.nan] * n_rows,
        "rain_3h": [np.nan] * n_rows,
        "snow_1h": [np.nan] * n_rows,
        "snow_3h": [np.nan] * n_rows,
        "dt": rng.integers(1700000000, 1710000000, n_rows).astype(float),
    })


@pytest.mark.benchmark
@patch("src.loader.get_minio_client")
def test_benchmark_clean_value_1000(mock_get_minio_client, benchmark):
    """Benchmark clean_value() over 1000 mixed values."""
    mock_get_minio_client.return_value = Mock()

    from src.loader import Loader
    loader = Loader()

    values = [np.nan, 25.5, pd.NaT, "text", None, 0, float("nan")] * 143

    def run():
        for v in values:
            loader.clean_value(v)

    benchmark(run)


@pytest.mark.benchmark
@patch("src.loader.get_minio_client")
def test_benchmark_map_air_quality_1000(mock_get_minio_client, benchmark):
    """Benchmark _map_air_quality over 1000 rows."""
    mock_get_minio_client.return_value = Mock()

    from src.loader import Loader
    loader = Loader()

    rng = np.random.default_rng(42)
    rows = []
    for _ in range(1000):
        rows.append(pd.Series({
            "time": "2026-01-29T12:00:00",
            "pm10": rng.uniform(0, 50),
            "pm2_5": rng.uniform(0, 25),
            "carbon_monoxide": rng.uniform(0, 500),
            "nitrogen_dioxide": rng.uniform(0, 40),
            "sulphur_dioxide": rng.uniform(0, 20),
            "ozone": rng.uniform(0, 100),
            "aerosol_optical_depth": rng.uniform(0, 1),
            "dust": rng.uniform(0, 30),
        }))

    def run():
        for row in rows:
            loader._map_air_quality(row, city_id=1, extraction_date_id=20260129)

    benchmark(run)


@pytest.mark.benchmark
@patch("src.loader.get_minio_client")
def test_benchmark_get_date_id_1000(mock_get_minio_client, benchmark):
    """Benchmark get_date_id() over 1000 date strings."""
    mock_get_minio_client.return_value = Mock()

    from src.loader import Loader
    loader = Loader()

    dates = [f"2026-{m:02d}-{d:02d}" for m in range(1, 13) for d in range(1, 29)]
    dates = dates * 3  # ~1000 dates

    def run():
        for d in dates:
            loader.get_date_id(d)

    benchmark(run)
