"""
Custom exceptions for data quality validation.
"""


class DataQualityException(Exception):
    """
    Base exception for data quality failures.

    Raised when data fails validation checks and should not be loaded
    into the data warehouse.
    """

    def __init__(self, message: str, validation_results: dict = None):
        self.message = message
        self.validation_results = validation_results or {}
        super().__init__(self.message)

    def __str__(self):
        if self.validation_results:
            failed_expectations = self.validation_results.get("failed_expectations", [])
            return f"{self.message} - Failed expectations: {failed_expectations}"
        return self.message


class ValidationError(DataQualityException):
    """
    Raised when schema validation fails.

    Indicates missing required columns or incorrect data types.
    """

    def __init__(self, message: str, missing_columns: list = None, type_errors: list = None):
        self.missing_columns = missing_columns or []
        self.type_errors = type_errors or []
        super().__init__(message)

    def __str__(self):
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

    def __init__(self, message: str, anomalies: list = None):
        self.anomalies = anomalies or []
        super().__init__(message)


class CompletenessError(DataQualityException):
    """
    Raised when data completeness falls below acceptable thresholds.
    """

    def __init__(self, message: str, completeness_score: float = None, threshold: float = None):
        self.completeness_score = completeness_score
        self.threshold = threshold
        super().__init__(message)

    def __str__(self):
        if self.completeness_score is not None and self.threshold is not None:
            return f"{self.message} - Score: {self.completeness_score:.2%}, Threshold: {self.threshold:.2%}"
        return self.message
