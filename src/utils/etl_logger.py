"""
ETL Logger Mixin

Provides standardized logging functionality for all ETL classes.
Eliminates duplicate logger initialization and logging methods.
"""

from __future__ import annotations

import logging
from typing import Optional


class BaseETLLogger:
    """
    Mixin class providing standardized logging for ETL components.

    Provides:
        - Automatic logger initialization using class name
        - Consistent log_start, log_end, log_error methods with emojis

    Usage:
        class MyETLClass(BaseETLLogger):
            def __init__(self):
                super().__init__()  # Initializes self.logger
                # ... rest of init

    Attributes:
        logger: Logger instance named after the concrete class
    """

    logger: logging.Logger

    def __init__(self) -> None:
        """Initialize logger with the concrete class name."""
        self.logger = logging.getLogger(self.__class__.__name__)

    def log_start(self, msg: str) -> None:
        """Log the start of an ETL operation."""
        self.logger.info(f"🚀 START: {msg}")

    def log_end(self, msg: str) -> None:
        """Log the end of an ETL operation."""
        self.logger.info(f"🏁 END: {msg}")

    def log_error(self, msg: str, error: Optional[Exception] = None) -> None:
        """
        Log an error message with optional exception details.

        Args:
            msg: Error message to log
            error: Optional exception to include in the log
        """
        if error:
            self.logger.error(f"❌ ERROR: {msg} - {str(error)}")
        else:
            self.logger.error(f"❌ ERROR: {msg}")
