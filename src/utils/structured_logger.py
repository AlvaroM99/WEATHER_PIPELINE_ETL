"""
Structured JSON Logger for ETL Pipeline

Provides production-ready logging with:
- JSON structured output for log aggregation tools (ELK, Datadog, CloudWatch)
- Standard log levels with context metadata
- No emojis (parseable by monitoring tools)
- Backward compatible with BaseETLLogger interface

Usage:
    class MyETLClass(StructuredETLLogger):
        def process(self):
            self.log_start("Processing data", extra={"record_count": 100})
            # ... work ...
            self.log_end("Processing complete", extra={"success_count": 95})
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from typing import Any, Dict, Optional


class JSONFormatter(logging.Formatter):
    """
    Custom JSON formatter for structured logging.

    Outputs logs in JSON format compatible with log aggregation tools.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.

        Args:
            record: Log record to format

        Returns:
            JSON string with structured log data
        """
        log_data: Dict[str, Any] = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": self.formatException(record.exc_info),
            }

        # Add custom fields from extra parameter
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)

        # Add process/thread info for debugging
        log_data["process"] = {
            "pid": record.process,
            "thread": record.thread,
            "thread_name": record.threadName,
        }

        return json.dumps(log_data, default=str)


class StructuredETLLogger:
    """
    Mixin class providing structured JSON logging for ETL components.

    Drop-in replacement for BaseETLLogger with JSON structured output.

    Features:
        - JSON structured logs compatible with log aggregation tools
        - Standard log levels (INFO, WARNING, ERROR)
        - Context metadata via 'extra' parameter
        - Automatic timestamp and process tracking
        - No emojis (production-ready)

    Usage:
        class MyETLClass(StructuredETLLogger):
            def __init__(self):
                super().__init__()  # Initializes self.logger
                # ... rest of init

    Attributes:
        logger: Structured JSON logger instance named after the concrete class
    """

    logger: logging.Logger

    def __init__(self, use_json_format: bool = True) -> None:
        """
        Initialize logger with the concrete class name.

        Args:
            use_json_format: If True, uses JSONFormatter. If False, uses standard format.
                            Set to False for local development, True for production.
        """
        self.logger = logging.getLogger(self.__class__.__name__)

        # Only configure handler if not already configured
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)

            if use_json_format:
                handler.setFormatter(JSONFormatter())
            else:
                # Human-readable format for local development
                handler.setFormatter(
                    logging.Formatter(
                        "%(asctime)s [%(levelname)8s] %(name)s - %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                    )
                )

            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
            self.logger.propagate = False

    def log_start(self, msg: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """
        Log the start of an ETL operation.

        Args:
            msg: Operation description
            extra: Additional context fields (e.g., {"city_count": 52, "source": "openweather"})
        """
        extra_fields = {"event_type": "operation_start", **(extra or {})}
        self.logger.info(msg, extra={"extra_fields": extra_fields})

    def log_end(self, msg: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """
        Log the end of an ETL operation.

        Args:
            msg: Completion message
            extra: Additional context fields (e.g., {"records_processed": 1000, "duration_sec": 45.2})
        """
        extra_fields = {"event_type": "operation_end", **(extra or {})}
        self.logger.info(msg, extra={"extra_fields": extra_fields})

    def log_error(
        self, msg: str, error: Optional[Exception] = None, extra: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log an error message with optional exception details.

        Args:
            msg: Error message to log
            error: Optional exception to include in the log
            extra: Additional context fields (e.g., {"city": "Madrid", "retry_count": 3})
        """
        extra_fields = {"event_type": "error", **(extra or {})}

        if error:
            # Let the formatter handle exception formatting
            self.logger.error(
                msg,
                exc_info=(type(error), error, error.__traceback__),
                extra={"extra_fields": extra_fields},
            )
        else:
            self.logger.error(msg, extra={"extra_fields": extra_fields})

    def log_metric(
        self, metric_name: str, value: float, unit: str = "", extra: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Log a metric for monitoring and alerting.

        Args:
            metric_name: Name of the metric (e.g., "api_response_time", "records_loaded")
            value: Numeric value of the metric
            unit: Unit of measurement (e.g., "seconds", "records", "MB")
            extra: Additional context fields
        """
        extra_fields = {
            "event_type": "metric",
            "metric_name": metric_name,
            "metric_value": value,
            "metric_unit": unit,
            **(extra or {}),
        }
        self.logger.info(
            f"Metric: {metric_name}={value}{unit}", extra={"extra_fields": extra_fields}
        )
