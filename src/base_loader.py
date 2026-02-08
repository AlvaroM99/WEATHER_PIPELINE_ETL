"""
Base Loader Module

Abstract base class providing database connection management
via context manager and standardized logging for all loaders.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

import psycopg2

from src.config.db_pool import get_db_connection, return_db_connection
from src.utils.etl_logger import BaseETLLogger


class BaseLoader(BaseETLLogger):
    """
    Abstract base for Loader and DimensionalLoader.

    Provides:
        - connection() context manager with auto commit/rollback
        - Logging via BaseETLLogger

    Attributes:
        logger: Logger instance named after the concrete class
    """

    def __init__(self) -> None:
        """Initialize BaseLoader with logger."""
        super().__init__()

    @contextmanager
    def connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """
        Context manager for database connections.

        Yields a connection from the pool. Commits on clean exit,
        rolls back on exception, and always returns to pool.

        Yields:
            psycopg2 connection object
        """
        conn = get_db_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            return_db_connection(conn)
