"""
Database Connection Pool

Centralized connection management using psycopg2's ThreadedConnectionPool.
Replaces duplicated get_db_connection() methods across Loader and DimensionalLoader.

Configuration:
    - Uses environment variables for pool sizing (DB_POOL_MIN_CONN, DB_POOL_MAX_CONN)
    - Default: minconn=2, maxconn=20 (suitable for Airflow with parallel workers)
    - Supports connection health checks and automatic retry
"""

from __future__ import annotations

import atexit
import logging
import os
from typing import Optional

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

from src.config.database_config import get_postgres_config

logger = logging.getLogger(__name__)

_pool: Optional[ThreadedConnectionPool] = None

# Configuration via environment variables
DEFAULT_MIN_CONN = 2  # Minimum connections to keep alive
DEFAULT_MAX_CONN = 20  # Maximum connections for parallel Airflow tasks


def _get_pool_size() -> tuple[int, int]:
    """
    Get pool size from environment variables or use defaults.

    Returns:
        Tuple of (minconn, maxconn)
    """
    min_conn = int(os.getenv("DB_POOL_MIN_CONN", DEFAULT_MIN_CONN))
    max_conn = int(os.getenv("DB_POOL_MAX_CONN", DEFAULT_MAX_CONN))

    # Validate configuration
    if min_conn < 1:
        logger.warning(f"Invalid DB_POOL_MIN_CONN={min_conn}, using default={DEFAULT_MIN_CONN}")
        min_conn = DEFAULT_MIN_CONN

    if max_conn < min_conn:
        logger.warning(
            f"DB_POOL_MAX_CONN={max_conn} < DB_POOL_MIN_CONN={min_conn}, "
            f"using default={DEFAULT_MAX_CONN}"
        )
        max_conn = DEFAULT_MAX_CONN

    return min_conn, max_conn


def _get_pool() -> ThreadedConnectionPool:
    """Get or create the global connection pool (lazy initialization)."""
    global _pool
    if _pool is None or _pool.closed:
        cfg = get_postgres_config()
        min_conn, max_conn = _get_pool_size()

        _pool = ThreadedConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            user=cfg["user"],
            password=cfg["password"],
            # Additional connection parameters for production
            connect_timeout=10,  # Timeout for establishing connections
            keepalives=1,  # Enable TCP keepalive
            keepalives_idle=30,  # Time before sending keepalive probes
            keepalives_interval=10,  # Interval between keepalive probes
            keepalives_count=5,  # Number of keepalive probes before timeout
        )
        atexit.register(_close_pool)
        logger.info(f"Database connection pool created (minconn={min_conn}, maxconn={max_conn})")
    return _pool


def _close_pool() -> None:
    """Close the connection pool at shutdown."""
    global _pool
    if _pool is not None and not _pool.closed:
        _pool.closeall()
        logger.info("Database connection pool closed")


def get_db_connection() -> psycopg2.extensions.connection:
    """
    Get a database connection from the pool with automatic health check.

    Returns:
        psycopg2 connection object

    Note:
        Callers must return the connection via return_db_connection()
        or use the connection as a context manager.

    Raises:
        psycopg2.OperationalError: If connection is unavailable or unhealthy
    """
    pool = _get_pool()
    conn = pool.getconn()

    # Health check: verify connection is alive
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
    except psycopg2.OperationalError:
        # Connection is dead, return it to pool and get a new one
        logger.warning("Dead connection detected, acquiring new connection")
        pool.putconn(conn, close=True)
        conn = pool.getconn()

    return conn


def return_db_connection(conn: psycopg2.extensions.connection) -> None:
    """
    Return a connection to the pool.

    Args:
        conn: Connection to return
    """
    pool = _get_pool()
    pool.putconn(conn)


def get_pool_stats() -> dict[str, int]:
    """
    Get connection pool statistics for monitoring.

    Returns:
        Dictionary with pool statistics:
        - min_conn: Minimum connections
        - max_conn: Maximum connections
        - current_conn: Approximate current connections (may be inaccurate with ThreadedConnectionPool)

    Note:
        ThreadedConnectionPool doesn't expose internal state, so statistics are approximate
    """
    pool = _get_pool()
    min_conn, max_conn = _get_pool_size()

    return {
        "min_conn": min_conn,
        "max_conn": max_conn,
        "pool_closed": pool.closed,
    }


def reset_pool() -> None:
    """Reset the connection pool. For testing only."""
    global _pool
    if _pool is not None and not _pool.closed:
        _pool.closeall()
    _pool = None
