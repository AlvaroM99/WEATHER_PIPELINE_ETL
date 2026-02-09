"""
Data quality metrics collection and reporting.

Tracks quality metrics over time and provides reporting capabilities
for monitoring data pipeline health.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any

import pandas as pd

from src.data_quality.validators import ValidationResult


@dataclass
class QualityMetric:
    """Individual quality metric measurement."""

    metric_name: str
    value: float
    timestamp: datetime = field(default_factory=datetime.now)
    table_name: str = ""
    dimension: str = ""  # completeness, accuracy, timeliness, validity
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metric_name": self.metric_name,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "table_name": self.table_name,
            "dimension": self.dimension,
            "metadata": self.metadata,
        }


@dataclass
class QualityReport:
    """Comprehensive quality report for a validation run."""

    report_id: str
    generated_at: datetime
    table_name: str
    overall_score: float
    dimension_scores: Dict[str, float]
    metrics: List[QualityMetric]
    validation_result: Optional[ValidationResult] = None
    recommendations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "report_id": self.report_id,
            "generated_at": self.generated_at.isoformat(),
            "table_name": self.table_name,
            "overall_score": self.overall_score,
            "dimension_scores": self.dimension_scores,
            "metrics": [m.to_dict() for m in self.metrics],
            "validation_result": (
                self.validation_result.to_dict() if self.validation_result else None
            ),
            "recommendations": self.recommendations,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)


class DataQualityMetrics:
    """
    Collects and calculates data quality metrics.

    Provides methods to:
    - Calculate completeness, accuracy, validity metrics
    - Track metrics over time
    - Generate quality reports
    - Detect anomalies in quality trends
    """

    # Default dimension weights for overall quality score
    DEFAULT_WEIGHTS: Dict[str, float] = {
        "completeness": 0.30,
        "validity": 0.35,
        "uniqueness": 0.20,
        "freshness": 0.15,
    }

    def __init__(self, weights: Optional[Dict[str, float]] = None) -> None:
        """
        Initialize DataQualityMetrics.

        Args:
            weights: Custom dimension weights for overall score calculation.
                     Keys: 'completeness', 'validity', 'uniqueness', 'freshness'.
                     Falls back to DEFAULT_WEIGHTS if not provided.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self._metrics_history: List[QualityMetric] = []
        self.weights: Dict[str, float] = weights or self.DEFAULT_WEIGHTS.copy()

    def calculate_completeness(
        self, df: pd.DataFrame, columns: Optional[List[str]] = None
    ) -> Dict[str, float]:
        """
        Calculate completeness score for each column.

        Completeness = (non-null values / total rows)

        Args:
            df: DataFrame to analyze
            columns: Specific columns to check (default: all)

        Returns:
            Dict mapping column names to completeness scores (0.0 to 1.0)
        """
        if len(df) == 0:
            return {}

        columns = columns or df.columns.tolist()
        scores = {}

        for col in columns:
            if col in df.columns:
                non_null = df[col].notna().sum()
                scores[col] = non_null / len(df)

        return scores

    def calculate_validity(self, df: pd.DataFrame, validation_result: ValidationResult) -> float:
        """
        Calculate validity score based on validation results.

        Validity = (passed expectations / total expectations)

        Args:
            df: DataFrame that was validated
            validation_result: Result from validation

        Returns:
            Validity score (0.0 to 1.0)
        """
        if validation_result.total_expectations == 0:
            return 1.0

        return validation_result.successful_expectations / validation_result.total_expectations

    def calculate_uniqueness(self, df: pd.DataFrame, key_columns: List[str]) -> float:
        """
        Calculate uniqueness score for key columns.

        Uniqueness = (unique combinations / total rows)

        Args:
            df: DataFrame to analyze
            key_columns: Columns that should be unique together

        Returns:
            Uniqueness score (0.0 to 1.0)
        """
        if len(df) == 0:
            return 1.0

        if not all(col in df.columns for col in key_columns):
            return 0.0

        unique_count = df[key_columns].drop_duplicates().shape[0]
        return float(unique_count) / float(len(df))

    def calculate_freshness(
        self, df: pd.DataFrame, timestamp_column: str, max_age_hours: float = 24.0
    ) -> float:
        """
        Calculate data freshness score.

        Freshness based on how recent the data is.

        Args:
            df: DataFrame to analyze
            timestamp_column: Column containing timestamps
            max_age_hours: Maximum acceptable age in hours

        Returns:
            Freshness score (0.0 to 1.0)
        """
        if len(df) == 0 or timestamp_column not in df.columns:
            return 0.0

        try:
            timestamps = pd.to_datetime(df[timestamp_column])
            latest = timestamps.max()
            now = datetime.now()

            if pd.isna(latest):
                return 0.0

            # Handle timezone-naive comparison
            if latest.tzinfo is not None:
                latest = latest.tz_localize(None)

            age_hours = (now - latest).total_seconds() / 3600

            if age_hours <= 0:
                return 1.0
            elif age_hours >= max_age_hours:
                return 0.0
            else:
                return float(1.0 - (age_hours / max_age_hours))

        except Exception as e:
            self.logger.warning(f"Error calculating freshness: {e}")
            return 0.0

    def calculate_range_conformity(
        self, df: pd.DataFrame, column: str, min_value: float, max_value: float
    ) -> float:
        """
        Calculate what percentage of values fall within expected range.

        Args:
            df: DataFrame to analyze
            column: Column to check
            min_value: Minimum expected value
            max_value: Maximum expected value

        Returns:
            Conformity score (0.0 to 1.0)
        """
        if len(df) == 0 or column not in df.columns:
            return 0.0

        valid_values = df[column].dropna()
        if len(valid_values) == 0:
            return 1.0

        in_range = ((valid_values >= min_value) & (valid_values <= max_value)).sum()
        return float(in_range) / float(len(valid_values))

    def generate_report(
        self,
        df: pd.DataFrame,
        table_name: str,
        validation_result: ValidationResult,
        key_columns: Optional[List[str]] = None,
        timestamp_column: Optional[str] = None,
    ) -> QualityReport:
        """
        Generate a comprehensive quality report.

        Args:
            df: DataFrame that was validated
            table_name: Name of the table
            validation_result: Result from validation
            key_columns: Columns for uniqueness check
            timestamp_column: Column for freshness check

        Returns:
            QualityReport with all metrics and recommendations
        """
        metrics = []
        dimension_scores = {}

        # Completeness
        completeness_scores = self.calculate_completeness(df)
        avg_completeness = (
            sum(completeness_scores.values()) / len(completeness_scores)
            if completeness_scores
            else 0.0
        )
        dimension_scores["completeness"] = avg_completeness

        for col, score in completeness_scores.items():
            metrics.append(
                QualityMetric(
                    metric_name=f"completeness_{col}",
                    value=score,
                    table_name=table_name,
                    dimension="completeness",
                )
            )

        # Validity
        validity = self.calculate_validity(df, validation_result)
        dimension_scores["validity"] = validity
        metrics.append(
            QualityMetric(
                metric_name="validity_score",
                value=validity,
                table_name=table_name,
                dimension="validity",
                metadata={
                    "total_expectations": validation_result.total_expectations,
                    "passed": validation_result.successful_expectations,
                    "failed": validation_result.failed_expectations,
                },
            )
        )

        # Uniqueness
        if key_columns:
            uniqueness = self.calculate_uniqueness(df, key_columns)
            dimension_scores["uniqueness"] = uniqueness
            metrics.append(
                QualityMetric(
                    metric_name="uniqueness_score",
                    value=uniqueness,
                    table_name=table_name,
                    dimension="uniqueness",
                    metadata={"key_columns": key_columns},
                )
            )

        # Freshness
        if timestamp_column:
            freshness = self.calculate_freshness(df, timestamp_column)
            dimension_scores["freshness"] = freshness
            metrics.append(
                QualityMetric(
                    metric_name="freshness_score",
                    value=freshness,
                    table_name=table_name,
                    dimension="freshness",
                    metadata={"timestamp_column": timestamp_column},
                )
            )

        # Calculate overall score (weighted average)
        overall_score = 0.0
        total_weight = 0.0
        for dim, score in dimension_scores.items():
            weight = self.weights.get(dim, 0.25)
            overall_score += score * weight
            total_weight += weight

        if total_weight > 0:
            overall_score /= total_weight

        # Generate recommendations
        recommendations = self._generate_recommendations(
            dimension_scores, validation_result, completeness_scores
        )

        # Store metrics in history
        self._metrics_history.extend(metrics)

        report_id = f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        return QualityReport(
            report_id=report_id,
            generated_at=datetime.now(),
            table_name=table_name,
            overall_score=overall_score,
            dimension_scores=dimension_scores,
            metrics=metrics,
            validation_result=validation_result,
            recommendations=recommendations,
        )

    def _generate_recommendations(
        self,
        dimension_scores: Dict[str, float],
        validation_result: ValidationResult,
        completeness_scores: Dict[str, float],
    ) -> List[str]:
        """Generate recommendations based on quality scores."""
        recommendations = []

        # Completeness recommendations
        if dimension_scores.get("completeness", 1.0) < 0.9:
            low_completeness = [col for col, score in completeness_scores.items() if score < 0.9]
            if low_completeness:
                recommendations.append(
                    f"Improve data completeness for columns: {', '.join(low_completeness[:5])}"
                )

        # Validity recommendations
        if dimension_scores.get("validity", 1.0) < 0.95:
            if validation_result.failed_details:
                failed_types = set(
                    d["expectation_type"] for d in validation_result.failed_details[:3]
                )
                recommendations.append(f"Address validation failures: {', '.join(failed_types)}")

        # Uniqueness recommendations
        if dimension_scores.get("uniqueness", 1.0) < 1.0:
            recommendations.append("Investigate and remove duplicate records before loading")

        # Freshness recommendations
        if dimension_scores.get("freshness", 1.0) < 0.5:
            recommendations.append("Data is stale - check extraction pipeline for delays")

        # Overall quality
        overall = sum(dimension_scores.values()) / len(dimension_scores) if dimension_scores else 0
        if overall < 0.8:
            recommendations.append("Overall data quality is below threshold - review data sources")

        return recommendations

    def get_metrics_summary(
        self, table_name: Optional[str] = None, dimension: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get summary of collected metrics.

        Args:
            table_name: Filter by table name
            dimension: Filter by quality dimension

        Returns:
            DataFrame with metrics summary
        """
        if not self._metrics_history:
            return pd.DataFrame()

        metrics_data = [m.to_dict() for m in self._metrics_history]
        df = pd.DataFrame(metrics_data)

        if table_name:
            df = df[df["table_name"] == table_name]
        if dimension:
            df = df[df["dimension"] == dimension]

        return df

    def detect_anomalies(
        self, table_name: str, metric_name: str, threshold_std: float = 2.0
    ) -> List[QualityMetric]:
        """
        Detect anomalies in metric values using standard deviation.

        Args:
            table_name: Table to analyze
            metric_name: Metric to check
            threshold_std: Number of standard deviations for anomaly detection

        Returns:
            List of anomalous metrics
        """
        relevant_metrics = [
            m
            for m in self._metrics_history
            if m.table_name == table_name and m.metric_name == metric_name
        ]

        if len(relevant_metrics) < 5:
            return []  # Not enough data for anomaly detection

        values = [m.value for m in relevant_metrics]
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        std = variance**0.5

        if std == 0:
            return []

        anomalies = []
        for metric in relevant_metrics:
            z_score = abs(metric.value - mean) / std
            if z_score > threshold_std:
                anomalies.append(metric)

        return anomalies

    def clear_history(self) -> None:
        """Clear metrics history."""
        self._metrics_history = []
