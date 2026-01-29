"""
Debug Utilities for Weather Pipeline ETL
Provides diagnostic tools for troubleshooting pipeline issues.
"""
import logging
import json
import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime
from functools import wraps
import traceback
import time

# Import dependencies for connectivity checks
try:
    import psycopg2
except ImportError:
    psycopg2 = None

try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    Minio = None
    S3Error = None


def setup_debug_logging(level: str = "DEBUG") -> logging.Logger:
    """
    Configure detailed logging for debugging purposes.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)

    Returns:
        Configured logger instance
    """
    log_level = getattr(logging, level.upper(), logging.DEBUG)

    # Create formatter with detailed output
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)8s] %(name)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Setup console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    # Configure root logger
    logger = logging.getLogger('weather_pipeline')
    logger.setLevel(log_level)

    # Remove existing handlers to avoid duplicates
    logger.handlers = []
    logger.addHandler(console_handler)

    return logger


def timing_decorator(func):
    """
    Decorator to measure and log function execution time.

    Usage:
        @timing_decorator
        def my_function():
            ...
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger('weather_pipeline.timing')
        start_time = time.time()

        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            logger.info(f"{func.__name__} completed in {elapsed:.3f}s")
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"{func.__name__} failed after {elapsed:.3f}s: {e}")
            raise

    return wrapper


def validate_dataframe(
    df: pd.DataFrame,
    required_columns: List[str],
    name: str = "DataFrame"
) -> Dict[str, Any]:
    """
    Validate a DataFrame and return diagnostic information.

    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        name: Name for logging purposes

    Returns:
        Dictionary with validation results
    """
    result = {
        "name": name,
        "valid": True,
        "issues": [],
        "stats": {}
    }

    if df is None:
        result["valid"] = False
        result["issues"].append("DataFrame is None")
        return result

    if df.empty:
        result["valid"] = False
        result["issues"].append("DataFrame is empty")
        return result

    # Check required columns
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        result["valid"] = False
        result["issues"].append(f"Missing columns: {missing_cols}")

    # Collect statistics
    result["stats"] = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "null_counts": df.isnull().sum().to_dict(),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()}
    }

    # Check for high null ratios
    for col, null_count in result["stats"]["null_counts"].items():
        null_ratio = null_count / len(df)
        if null_ratio > 0.5:
            result["issues"].append(f"Column '{col}' has {null_ratio:.1%} null values")

    return result


def inspect_json_structure(data: dict, max_depth: int = 3) -> Dict[str, Any]:
    """
    Analyze JSON structure for debugging API responses.

    Args:
        data: JSON data as dictionary
        max_depth: Maximum depth to inspect

    Returns:
        Dictionary describing the structure
    """
    def _inspect(obj, depth=0):
        if depth >= max_depth:
            return f"<max depth reached>"

        if isinstance(obj, dict):
            return {
                "_type": "dict",
                "_keys": list(obj.keys()),
                "_sample": {k: _inspect(v, depth + 1) for k, v in list(obj.items())[:5]}
            }
        elif isinstance(obj, list):
            return {
                "_type": "list",
                "_length": len(obj),
                "_sample": _inspect(obj[0], depth + 1) if obj else None
            }
        else:
            return {"_type": type(obj).__name__, "_value": str(obj)[:50]}

    return _inspect(data)


def compare_dataframes(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    name1: str = "df1",
    name2: str = "df2"
) -> Dict[str, Any]:
    """
    Compare two DataFrames for debugging transformations.

    Args:
        df1: First DataFrame
        df2: Second DataFrame
        name1: Name of first DataFrame
        name2: Name of second DataFrame

    Returns:
        Dictionary with comparison results
    """
    result = {
        "shapes": {
            name1: df1.shape if df1 is not None else None,
            name2: df2.shape if df2 is not None else None
        },
        "columns": {
            "only_in_" + name1: [],
            "only_in_" + name2: [],
            "common": []
        },
        "dtype_differences": {}
    }

    if df1 is None or df2 is None:
        return result

    cols1 = set(df1.columns)
    cols2 = set(df2.columns)

    result["columns"]["only_in_" + name1] = list(cols1 - cols2)
    result["columns"]["only_in_" + name2] = list(cols2 - cols1)
    result["columns"]["common"] = list(cols1 & cols2)

    # Check dtype differences for common columns
    for col in result["columns"]["common"]:
        if df1[col].dtype != df2[col].dtype:
            result["dtype_differences"][col] = {
                name1: str(df1[col].dtype),
                name2: str(df2[col].dtype)
            }

    return result


def trace_exception(e: Exception) -> Dict[str, Any]:
    """
    Extract detailed information from an exception.

    Args:
        e: Exception instance

    Returns:
        Dictionary with exception details
    """
    tb = traceback.extract_tb(e.__traceback__)

    return {
        "type": type(e).__name__,
        "message": str(e),
        "traceback": [
            {
                "file": frame.filename,
                "line": frame.lineno,
                "function": frame.name,
                "code": frame.line
            }
            for frame in tb
        ]
    }


def check_minio_connectivity(
    endpoint: str,
    access_key: str,
    secret_key: str,
    secure: bool = False
) -> Dict[str, Any]:
    """
    Test MinIO server connectivity and permissions.

    Args:
        endpoint: MinIO server endpoint
        access_key: Access key
        secret_key: Secret key
        secure: Use HTTPS

    Returns:
        Dictionary with connectivity test results
    """
    result = {
        "connected": False,
        "buckets_accessible": False,
        "buckets": [],
        "error": None
    }

    if Minio is None:
        result["error"] = "minio library not installed"
        return result

    try:
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )

        # Test listing buckets
        buckets = client.list_buckets()
        result["connected"] = True
        result["buckets_accessible"] = True
        result["buckets"] = [b.name for b in buckets]

    except S3Error as e:
        result["error"] = f"S3 Error: {e.code} - {e.message}"
    except Exception as e:
        result["error"] = f"Connection Error: {str(e)}"

    return result


def check_postgres_connectivity(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str
) -> Dict[str, Any]:
    """
    Test PostgreSQL server connectivity.

    Args:
        host: Database host
        port: Database port
        database: Database name
        user: Username
        password: Password

    Returns:
        Dictionary with connectivity test results
    """
    result = {
        "connected": False,
        "version": None,
        "tables": [],
        "error": None
    }

    if psycopg2 is None:
        result["error"] = "psycopg2 library not installed"
        return result

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=5
        )

        result["connected"] = True

        with conn.cursor() as cur:
            # Get PostgreSQL version
            cur.execute("SELECT version()")
            result["version"] = cur.fetchone()[0]

            # List tables
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
            """)
            result["tables"] = [row[0] for row in cur.fetchall()]

        conn.close()

    except Exception as e:
        # Handle psycopg2-specific errors and generic exceptions
        if hasattr(e, 'pgerror') and e.pgerror:
            result["error"] = f"PostgreSQL Error: {e.pgerror}"
        else:
            result["error"] = f"Connection Error: {str(e)}"

    return result


def generate_diagnostic_report(
    include_minio: bool = True,
    include_postgres: bool = True
) -> Dict[str, Any]:
    """
    Generate a comprehensive diagnostic report for the ETL pipeline.

    Args:
        include_minio: Include MinIO connectivity check
        include_postgres: Include PostgreSQL connectivity check

    Returns:
        Dictionary with full diagnostic report
    """
    import os

    report = {
        "timestamp": datetime.now().isoformat(),
        "environment": {
            "python_version": None,
            "required_env_vars": {}
        },
        "connectivity": {}
    }

    # Check Python version
    import sys
    report["environment"]["python_version"] = sys.version

    # Check required environment variables
    required_vars = [
        "OPENWEATHER_API_KEY",
        "MINIO_ENDPOINT",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "POSTGRES_HOST",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DB"
    ]

    for var in required_vars:
        value = os.getenv(var)
        report["environment"]["required_env_vars"][var] = {
            "set": value is not None,
            "value": "***" if value else None  # Mask actual values
        }

    # Connectivity checks
    if include_minio:
        report["connectivity"]["minio"] = check_minio_connectivity(
            endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", ""),
            secret_key=os.getenv("MINIO_SECRET_KEY", ""),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true"
        )

    if include_postgres:
        report["connectivity"]["postgres"] = check_postgres_connectivity(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "weather"),
            user=os.getenv("POSTGRES_USER", ""),
            password=os.getenv("POSTGRES_PASSWORD", "")
        )

    return report
