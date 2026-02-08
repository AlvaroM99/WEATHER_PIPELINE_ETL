"""
Base data quality validators.

Uses pandas-based validation for stability across Great Expectations versions.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.data_quality.exceptions import DataQualityException, ValidationError


@dataclass
class ValidationResult:
    """Result of a data quality validation."""

    success: bool
    table_name: str
    validation_time: datetime = field(default_factory=datetime.now)
    total_expectations: int = 0
    successful_expectations: int = 0
    failed_expectations: int = 0
    failed_details: List[Dict] = field(default_factory=list)
    statistics: Dict = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        """Calculate the percentage of passed expectations."""
        if self.total_expectations == 0:
            return 0.0
        return self.successful_expectations / self.total_expectations

    def to_dict(self) -> Dict:
        """Convert to dictionary for logging/storage."""
        return {
            "success": self.success,
            "table_name": self.table_name,
            "validation_time": self.validation_time.isoformat(),
            "total_expectations": self.total_expectations,
            "successful_expectations": self.successful_expectations,
            "failed_expectations": self.failed_expectations,
            "success_rate": self.success_rate,
            "failed_details": self.failed_details,
            "statistics": self.statistics,
        }


class DataQualityValidator:
    """
    Core data quality validator using pandas-based checks.

    Provides methods to validate DataFrames with support for
    schema validation, range checks, and completeness metrics.
    """

    def __init__(self, strict_mode: bool = False):
        """
        Initialize the validator.

        Args:
            strict_mode: If True, any validation failure raises an exception.
                        If False, validation results are returned without raising.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.strict_mode = strict_mode

    def validate_schema(
        self,
        df: pd.DataFrame,
        required_columns: List[str],
        column_types: Optional[Dict[str, str]] = None,
    ) -> Tuple[bool, List[str]]:
        """
        Validate that a DataFrame has required columns and correct types.

        Args:
            df: DataFrame to validate
            required_columns: List of column names that must exist
            column_types: Optional dict mapping column names to expected types
                         ('numeric', 'string', 'datetime', 'boolean')

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []

        # Check required columns
        missing = set(required_columns) - set(df.columns)
        if missing:
            errors.append(f"Missing required columns: {list(missing)}")

        # Check column types if specified
        if column_types:
            for col, expected_type in column_types.items():
                if col not in df.columns:
                    continue

                actual_dtype = df[col].dtype

                if expected_type == "numeric":
                    if not pd.api.types.is_numeric_dtype(actual_dtype):
                        errors.append(f"Column '{col}' should be numeric, got {actual_dtype}")
                elif expected_type == "string":
                    if not (
                        pd.api.types.is_string_dtype(actual_dtype)
                        or pd.api.types.is_object_dtype(actual_dtype)
                    ):
                        errors.append(f"Column '{col}' should be string, got {actual_dtype}")
                elif expected_type == "datetime":
                    if not pd.api.types.is_datetime64_any_dtype(actual_dtype):
                        errors.append(f"Column '{col}' should be datetime, got {actual_dtype}")
                elif expected_type == "boolean":
                    if not pd.api.types.is_bool_dtype(actual_dtype):
                        errors.append(f"Column '{col}' should be boolean, got {actual_dtype}")

        is_valid = len(errors) == 0
        return is_valid, errors

    def validate_range(
        self,
        df: pd.DataFrame,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        allow_null: bool = True,
        mostly: float = 0.99,
    ) -> Tuple[bool, int, float]:
        """
        Validate that column values fall within a specified range.

        Args:
            df: DataFrame to validate
            column: Column name to check
            min_value: Minimum allowed value (inclusive)
            max_value: Maximum allowed value (inclusive)
            allow_null: Whether NULL values are acceptable
            mostly: Proportion of values that must pass (0.0 to 1.0)

        Returns:
            Tuple of (is_valid, count of invalid values, compliance rate)
        """
        if column not in df.columns:
            return True, 0, 1.0  # Column doesn't exist, skip validation

        series = df[column]

        if not allow_null and series.isna().any():
            invalid_count = series.isna().sum()
            return False, invalid_count, 1.0 - (invalid_count / len(series))

        # Filter to non-null values for range check
        valid_series = series.dropna()

        if len(valid_series) == 0:
            return True, 0, 1.0

        violations = pd.Series([False] * len(valid_series), index=valid_series.index)

        if min_value is not None:
            violations = violations | (valid_series < min_value)
        if max_value is not None:
            violations = violations | (valid_series > max_value)

        invalid_count = int(violations.sum())
        compliance_rate = 1.0 - (invalid_count / len(valid_series))

        # Check if compliance meets the "mostly" threshold
        is_valid = compliance_rate >= mostly
        return is_valid, invalid_count, compliance_rate

    def validate_completeness(
        self, df: pd.DataFrame, columns: List[str], threshold: float = 0.95
    ) -> Tuple[bool, Dict[str, float]]:
        """
        Validate that columns meet a completeness threshold.

        Args:
            df: DataFrame to validate
            columns: List of columns to check
            threshold: Minimum proportion of non-null values required (0.0 to 1.0)

        Returns:
            Tuple of (is_valid, dict mapping column names to completeness scores)
        """
        if len(df) == 0:
            return True, {}

        scores = {}
        all_valid = True

        for col in columns:
            if col not in df.columns:
                scores[col] = 0.0
                all_valid = False
                continue

            non_null_count = df[col].notna().sum()
            completeness = non_null_count / len(df)
            scores[col] = completeness

            if completeness < threshold:
                all_valid = False

        return all_valid, scores

    def validate_uniqueness(self, df: pd.DataFrame, columns: List[str]) -> Tuple[bool, int]:
        """
        Validate that a combination of columns forms unique records.

        Args:
            df: DataFrame to validate
            columns: List of columns that should be unique together

        Returns:
            Tuple of (is_valid, count of duplicate records)
        """
        if not all(col in df.columns for col in columns):
            return False, len(df)

        duplicates = df.duplicated(subset=columns, keep=False)
        duplicate_count = int(duplicates.sum())

        return duplicate_count == 0, duplicate_count

    def validate_not_null(
        self, df: pd.DataFrame, column: str, mostly: float = 0.95
    ) -> Tuple[bool, int, float]:
        """
        Validate that a column has mostly non-null values.

        Args:
            df: DataFrame to validate
            column: Column to check
            mostly: Minimum proportion of non-null values (0.0 to 1.0)

        Returns:
            Tuple of (is_valid, null count, completeness rate)
        """
        if column not in df.columns:
            return True, 0, 1.0

        null_count = int(df[column].isna().sum())
        completeness = 1.0 - (null_count / len(df)) if len(df) > 0 else 1.0

        return completeness >= mostly, null_count, completeness

    def run_validation(
        self,
        df: pd.DataFrame,
        table_name: str,
        required_columns: List[str],
        numeric_ranges: Optional[Dict[str, Tuple[float, float]]] = None,
        completeness_columns: Optional[List[str]] = None,
        completeness_threshold: float = 0.95,
        mostly: float = 0.99,
    ) -> ValidationResult:
        """
        Run comprehensive validation on a DataFrame.

        Args:
            df: DataFrame to validate
            table_name: Name of the table being validated
            required_columns: Columns that must exist
            numeric_ranges: Dict mapping column names to (min, max) tuples
            completeness_columns: Columns to check for completeness
            completeness_threshold: Minimum completeness threshold
            mostly: Proportion threshold for range validations

        Returns:
            ValidationResult with validation details
        """
        failed_details = []
        total_checks = 0
        passed_checks = 0

        # Schema validation - check required columns exist
        total_checks += 1
        schema_valid, schema_errors = self.validate_schema(df, required_columns)
        if schema_valid:
            passed_checks += 1
        else:
            for error in schema_errors:
                failed_details.append(
                    {"expectation_type": "expect_column_to_exist", "error": error}
                )

        # Range validation for numeric columns
        if numeric_ranges:
            for col, (min_val, max_val) in numeric_ranges.items():
                if col not in df.columns:
                    continue  # Skip columns that don't exist

                total_checks += 1
                is_valid, invalid_count, compliance = self.validate_range(
                    df, col, min_val, max_val, mostly=mostly
                )
                if is_valid:
                    passed_checks += 1
                else:
                    failed_details.append(
                        {
                            "expectation_type": "expect_column_values_to_be_between",
                            "column": col,
                            "unexpected_count": int(invalid_count),
                            "unexpected_percent": float((1.0 - compliance) * 100),
                            "kwargs": {
                                "min_value": float(min_val),
                                "max_value": float(max_val),
                                "mostly": float(mostly),
                            },
                        }
                    )

        # Completeness validation for critical columns
        if completeness_columns:
            for col in completeness_columns:
                if col not in df.columns:
                    continue

                total_checks += 1
                is_valid, null_count, completeness = self.validate_not_null(
                    df, col, mostly=completeness_threshold
                )
                if is_valid:
                    passed_checks += 1
                else:
                    failed_details.append(
                        {
                            "expectation_type": "expect_column_values_to_not_be_null",
                            "column": col,
                            "unexpected_count": int(null_count),
                            "unexpected_percent": float((1.0 - completeness) * 100),
                            "kwargs": {"mostly": float(completeness_threshold)},
                        }
                    )

        success = len(failed_details) == 0

        # Calculate statistics
        statistics = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "evaluated_expectations": total_checks,
            "successful_expectations": passed_checks,
            "unsuccessful_expectations": total_checks - passed_checks,
            "success_percent": (passed_checks / total_checks * 100) if total_checks > 0 else 100,
        }

        result = ValidationResult(
            success=success,
            table_name=table_name,
            total_expectations=total_checks,
            successful_expectations=passed_checks,
            failed_expectations=total_checks - passed_checks,
            failed_details=failed_details,
            statistics=statistics,
        )

        if self.strict_mode and not success:
            raise DataQualityException(
                f"Validation failed for {table_name}", validation_results=result.to_dict()
            )

        return result
