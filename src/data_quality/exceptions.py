"""
Data Quality Exceptions

Custom exceptions for data quality validation failures.
"""

from typing import Any, Dict, List, Optional


class DataQualityException(Exception):
    """
    Base exception for data quality failures.

    Raised when data fails validation checks and should not be loaded
    into the data warehouse.
    """

    def __init__(self, message: str, validation_results: Optional[Dict[str, Any]] = None) -> None:
        self.message = message
        self.validation_results = validation_results or {}
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.validation_results:
            failed_expectations = self.validation_results.get("failed_expectations", [])
            return f"{self.message} - Failed expectations: {failed_expectations}"
        return self.message


class ValidationError(DataQualityException):
    """
    Raised when schema validation fails.

    Indicates missing required columns or incorrect data types.
    """

    def __init__(self, message: str, missing_columns: Optional[list] = None, type_errors: Optional[list] = None):
        self.missing_columns = missing_columns or []
        self.type_errors = type_errors or []
        super().__init__(message)

    def __str__(self) -> str:
        details = []
        if self.missing_columns:
            details.append(f"Missing columns: {self.missing_columns}")
        if self.type_errors:
            details.append(f"Type errors: {self.type_errors}")
        return f"{self.message} - {'; '.join(details)}" if details else self.message


class AnomalyDetectionError(DataQualityException):
    """
    Raised when anomaly detection identifies suspicious data patterns.
    """

    def __init__(self, message: str, failed_expectations: List[Dict[str, Any]]) -> None:
        self.failed_expectations = failed_expectations or []
        super().__init__(message, validation_results={"failed_expectations": self.failed_expectations})


class CompletenessError(DataQualityException):
    """
    Raised when data completeness falls below acceptable thresholds.
    """

    def __init__(self, message: str, context: Dict[str, Any]) -> None:
        self.completeness_score = context.get("completeness_score")
        self.threshold = context.get("threshold")
        super().__init__(message, validation_results=context)

    def __str__(self) -> str:
        if self.completeness_score is not None and self.threshold is not None:
            return f"{self.message} - Score: {self.completeness_score:.2%}, Threshold: {self.threshold:.2%}"
        return self.message
