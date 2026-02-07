"""
Database Connection Pool

Centralized connection management using psycopg2's ThreadedConnectionPool.
Replaces duplicated get_db_connection() methods across Loader and DimensionalLoader.
"""

from __future__ import annotations

import atexit
import logging
from typing import Optional

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

from src.config.database_config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)

logger = logging.getLogger(__name__)

_pool: Optional[ThreadedConnectionPool] = None


def _get_pool() -> ThreadedConnectionPool:
    """Get or create the global connection pool (lazy initialization)."""
    global _pool
    if _pool is None or _pool.closed:
        _pool = ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        atexit.register(_close_pool)
        logger.info("Database connection pool created")
    return _pool


def _close_pool() -> None:
    """Close the connection pool at shutdown."""
    global _pool
    if _pool is not None and not _pool.closed:
        _pool.closeall()
        logger.info("Database connection pool closed")


def get_db_connection() -> psycopg2.extensions.connection:
    """
    Get a database connection from the pool.

    Returns:
        psycopg2 connection object

    Note:
        Callers must return the connection via return_db_connection()
        or use the connection as a context manager.
    """
    pool = _get_pool()
    return pool.getconn()


def return_db_connection(conn: psycopg2.extensions.connection) -> None:
    """
    Return a connection to the pool.

    Args:
        conn: Connection to return
    """
    pool = _get_pool()
    pool.putconn(conn)
