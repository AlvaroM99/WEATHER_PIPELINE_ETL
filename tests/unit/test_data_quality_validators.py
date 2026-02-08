"""
Comprehensive tests for DataQuality validators module.

Tests cover ValidationResult, DataQualityValidator to achieve 70%+ coverage.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.data_quality.exceptions import DataQualityException, ValidationError
from src.data_quality.validators import DataQualityValidator, ValidationResult


class TestValidationResult:
    """Test ValidationResult dataclass."""

    def test_validation_result_creation(self):
        """Test creating a ValidationResult."""
        result = ValidationResult(
            success=True,
            table_name="test_table",
            total_expectations=10,
            successful_expectations=8,
            failed_expectations=2,
        )

        assert result.success is True
        assert result.table_name == "test_table"
        assert result.total_expectations == 10
        assert result.successful_expectations == 8
        assert result.failed_expectations == 2

    def test_success_rate_calculation(self):
        """Test success_rate property calculation."""
        result = ValidationResult(
            success=True,
            table_name="test_table",
            total_expectations=10,
            successful_expectations=8,
            failed_expectations=2,
        )

        assert result.success_rate == 0.8

    def test_success_rate_zero_expectations(self):
        """Test success_rate when no expectations."""
        result = ValidationResult(success=True, table_name="test_table", total_expectations=0)

        assert result.success_rate == 0.0

    def test_to_dict_conversion(self):
        """Test converting ValidationResult to dictionary."""
        result = ValidationResult(
            success=True,
            table_name="test_table",
            total_expectations=10,
            successful_expectations=8,
            failed_expectations=2,
            failed_details=[{"column": "test_col", "error": "test_error"}],
            statistics={"row_count": 100},
        )

        result_dict = result.to_dict()

        assert result_dict["success"] is True
        assert result_dict["table_name"] == "test_table"
        assert result_dict["total_expectations"] == 10
        assert result_dict["success_rate"] == 0.8
        assert len(result_dict["failed_details"]) == 1
        assert result_dict["statistics"]["row_count"] == 100


class TestDataQualityValidatorInit:
    """Test DataQualityValidator initialization."""

    def test_init_default_mode(self):
        """Test initialization with default strict_mode=False."""
        validator = DataQualityValidator()

        assert validator.strict_mode is False
        assert hasattr(validator, "logger")

    def test_init_strict_mode(self):
        """Test initialization with strict_mode=True."""
        validator = DataQualityValidator(strict_mode=True)

        assert validator.strict_mode is True


class TestValidateSchema:
    """Test validate_schema method."""

    def test_validate_schema_success(self):
        """Test successful schema validation."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"], "col3": [1.0, 2.0, 3.0]})

        validator = DataQualityValidator()
        is_valid, errors = validator.validate_schema(df, required_columns=["col1", "col2"])

        assert is_valid is True
        assert len(errors) == 0

    def test_validate_schema_missing_columns(self):
        """Test schema validation with missing columns."""
        df = pd.DataFrame({"col1": [1, 2, 3]})

        validator = DataQualityValidator()
        is_valid, errors = validator.validate_schema(df, required_columns=["col1", "col2", "col3"])

        assert is_valid is False
        assert len(errors) == 1
        assert "Missing required columns" in errors[0]

    def test_validate_schema_with_types(self):
        """Test schema validation with column types."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        validator = DataQualityValidator()
        is_valid, errors = validator.validate_schema(
            df, required_columns=["col1", "col2"], column_types={"col1": "numeric", "col2": "string"}
        )

        assert is_valid is True
        assert len(errors) == 0

    def test_validate_schema_wrong_types(self):
        """Test schema validation with incorrect column types."""
        df = pd.DataFrame({"col1": ["a", "b", "c"]})  # String instead of numeric

        validator = DataQualityValidator()
        is_valid, errors = validator.validate_schema(
            df, required_columns=["col1"], column_types={"col1": "numeric"}
        )

        assert is_valid is False
        assert len(errors) > 0


class TestValidateRange:
    """Test validate_range method."""

    def test_validate_range_success(self):
        """Test successful range validation."""
        df = pd.DataFrame({"temperature": [15.0, 20.0, 25.0, 30.0]})

        validator = DataQualityValidator()
        is_valid, out_of_range = validator.validate_range(df, "temperature", min_value=10.0, max_value=35.0)

        assert is_valid is True
        assert out_of_range == 0

    def test_validate_range_failures(self):
        """Test range validation with out-of-range values."""
        df = pd.DataFrame({"temperature": [5.0, 15.0, 40.0, 25.0]})

        validator = DataQualityValidator()
        is_valid, out_of_range = validator.validate_range(df, "temperature", min_value=10.0, max_value=35.0)

        assert is_valid is False
        assert out_of_range == 2  # 5.0 and 40.0 are out of range

    def test_validate_range_missing_column(self):
        """Test range validation with missing column."""
        df = pd.DataFrame({"other_col": [1, 2, 3]})

        validator = DataQualityValidator()
        is_valid, out_of_range = validator.validate_range(df, "temperature", min_value=10.0, max_value=35.0)

        assert is_valid is False
        assert out_of_range == 0


class TestValidateCompleteness:
    """Test validate_completeness method."""

    def test_validate_completeness_success(self):
        """Test successful completeness validation."""
        df = pd.DataFrame({"col1": [1, 2, 3, 4, 5], "col2": ["a", "b", "c", "d", "e"]})

        validator = DataQualityValidator()
        is_valid, completeness = validator.validate_completeness(df, required_columns=["col1", "col2"], threshold=0.9)

        assert is_valid is True
        assert completeness["col1"] == 1.0
        assert completeness["col2"] == 1.0

    def test_validate_completeness_with_nulls(self):
        """Test completeness validation with null values."""
        df = pd.DataFrame({"col1": [1, 2, None, 4, 5], "col2": ["a", None, "c", None, "e"]})

        validator = DataQualityValidator()
        is_valid, completeness = validator.validate_completeness(df, required_columns=["col1", "col2"], threshold=0.7)

        assert is_valid is True  # 0.8 and 0.6 average to 0.7
        assert completeness["col1"] == 0.8  # 4/5
        assert completeness["col2"] == 0.6  # 3/5

    def test_validate_completeness_below_threshold(self):
        """Test completeness validation below threshold."""
        df = pd.DataFrame({"col1": [1, None, None, None, 5]})

        validator = DataQualityValidator()
        is_valid, completeness = validator.validate_completeness(df, required_columns=["col1"], threshold=0.5)

        assert is_valid is False  # 0.4 < 0.5
        assert completeness["col1"] == 0.4


class TestValidateDataFrame:
    """Test validate_dataframe comprehensive method."""

    def test_validate_dataframe_success(self):
        """Test comprehensive DataFrame validation success."""
        df = pd.DataFrame(
            {"temperature": [15.0, 20.0, 25.0], "humidity": [60.0, 65.0, 70.0], "city": ["Madrid", "Barcelona", "Sevilla"]}
        )

        validator = DataQualityValidator()
        result = validator.validate_dataframe(
            df=df,
            table_name="weather_data",
            required_columns=["temperature", "humidity", "city"],
            column_types={"temperature": "numeric", "humidity": "numeric", "city": "string"},
            range_checks={"temperature": (10.0, 35.0), "humidity": (0.0, 100.0)},
            completeness_threshold=0.9,
        )

        assert result.success is True
        assert result.failed_expectations == 0

    def test_validate_dataframe_with_failures(self):
        """Test comprehensive DataFrame validation with failures."""
        df = pd.DataFrame({"temperature": [5.0, None, 40.0], "humidity": [60.0, 65.0, 70.0]})

        validator = DataQualityValidator()
        result = validator.validate_dataframe(
            df=df,
            table_name="weather_data",
            required_columns=["temperature", "humidity"],
            range_checks={"temperature": (10.0, 35.0)},
            completeness_threshold=0.9,
        )

        assert result.success is False
        assert result.failed_expectations > 0

    def test_validate_dataframe_strict_mode_raises(self):
        """Test that strict mode raises exception on validation failure."""
        df = pd.DataFrame({"temperature": [5.0, 40.0]})  # Out of range

        validator = DataQualityValidator(strict_mode=True)

        with pytest.raises(DataQualityException):
            validator.validate_dataframe(
                df=df,
                table_name="weather_data",
                required_columns=["temperature"],
                range_checks={"temperature": (10.0, 35.0)},
            )


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_dataframe(self):
        """Test validation with empty DataFrame."""
        df = pd.DataFrame()

        validator = DataQualityValidator()
        result = validator.validate_dataframe(df=df, table_name="empty_table", required_columns=[])

        assert result.success is True

    def test_single_row_dataframe(self):
        """Test validation with single row."""
        df = pd.DataFrame({"col1": [1]})

        validator = DataQualityValidator()
        result = validator.validate_dataframe(df=df, table_name="single_row", required_columns=["col1"])

        assert result.success is True

    def test_all_null_column(self):
        """Test validation with all-null column."""
        df = pd.DataFrame({"col1": [None, None, None]})

        validator = DataQualityValidator()
        is_valid, completeness = validator.validate_completeness(df, required_columns=["col1"], threshold=0.1)

        assert is_valid is False
        assert completeness["col1"] == 0.0
