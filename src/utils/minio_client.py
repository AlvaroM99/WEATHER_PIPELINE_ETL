"""
MinIO Client Utilities

Type-annotated module for handling connection and operations with MinIO data lake.
Implements singleton pattern for efficient resource usage.
"""

from __future__ import annotations

import io
import json
import logging
import threading
from typing import Any, Dict, List, Optional

import pandas as pd
from minio import Minio
from minio.error import S3Error

from src.config.lake_config import (
    BRONZE_BUCKET,
    SILVER_BUCKET,
    get_minio_connection,
)

logger: logging.Logger = logging.getLogger(__name__)

# Singleton instance and lock
_client_instance: Optional["MinIOClient"] = None
_client_lock: threading.Lock = threading.Lock()


def get_minio_client() -> "MinIOClient":
    """
    Get the singleton MinIOClient instance.

    Thread-safe with lazy initialization. The client is created on first
    call and reused for subsequent calls within the same process.

    Returns:
        Singleton MinIOClient instance

    Example:
        client = get_minio_client()
        client.upload_json(bucket, path, data)
    """
    global _client_instance

    # Fast path without lock
    if _client_instance is not None:
        return _client_instance

    with _client_lock:
        # Double-check after acquiring lock
        if _client_instance is None:
            _client_instance = MinIOClient()
        return _client_instance


def reset_minio_client() -> None:
    """
    Reset the singleton instance. Thread-safe.

    Primarily used in tests to ensure fresh client state.
    Also useful if connection parameters change at runtime.
    """
    global _client_instance
    with _client_lock:
        _client_instance = None


class MinIOClient:
    """
    Client for interacting with MinIO data lake.

    NOTE: Prefer using get_minio_client() to get a singleton instance
    rather than instantiating this class directly.

    Attributes:
        client: Minio client instance
    """

    def __init__(self) -> None:
        """Initialize MinIO client and ensure buckets exist."""
        cfg = get_minio_connection()
        self.client: Minio = Minio(
            cfg["endpoint"],
            access_key=cfg["access_key"],
            secret_key=cfg["secret_key"],
            secure=cfg["secure"],
        )
        self._ensure_buckets()

    def _ensure_buckets(self) -> None:
        """
        Create buckets if they don't exist.

        Supports three API sources: OpenWeather, Open-Meteo, AEMET.
        """
        buckets: List[str] = [
            # OpenWeather API (existing)
            "bronze-openweather",
            "silver-openweather",
            # Open-Meteo API (new)
            "bronze-openmeteo",
            "silver-openmeteo",
            # AEMET API (new)
            "bronze-aemet",
            "silver-aemet",
        ]

        for bucket in buckets:
            try:
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                    logger.info(f"✅ Created bucket: {bucket}")
                else:
                    logger.info(f"Bucket already exists: {bucket}")
            except S3Error as e:
                logger.error(f"❌ Error creating bucket {bucket}: {e}")
                raise

    def upload_json(self, bucket: str, object_name: str, data: Dict[str, Any]) -> int:
        """
        Upload JSON data to MinIO.

        Args:
            bucket: Bucket name
            object_name: Object path in bucket
            data: Dictionary to upload as JSON

        Returns:
            Size of uploaded data in bytes
        """
        try:
            json_bytes: bytes = json.dumps(data, indent=2).encode("utf-8")
            self.client.put_object(
                bucket,
                object_name,
                io.BytesIO(json_bytes),
                length=len(json_bytes),
                content_type="application/json",
            )
            logger.info(f"Uploaded JSON to {bucket}/{object_name} ({len(json_bytes)} bytes)")
            return len(json_bytes)
        except S3Error as e:
            logger.error(f"Error uploading JSON to {bucket}/{object_name}: {e}")
            raise

    def upload_parquet(self, bucket: str, object_name: str, dataframe: pd.DataFrame) -> int:
        """
        Upload Parquet data to MinIO.

        Args:
            bucket: Bucket name
            object_name: Object path in bucket
            dataframe: Pandas DataFrame to upload as Parquet

        Returns:
            Size of uploaded data in bytes
        """
        try:
            parquet_buffer: io.BytesIO = io.BytesIO()
            dataframe.to_parquet(parquet_buffer, engine="pyarrow", index=False)
            parquet_buffer.seek(0)

            size: int = parquet_buffer.getbuffer().nbytes

            self.client.put_object(
                bucket,
                object_name,
                parquet_buffer,
                length=size,
                content_type="application/octet-stream",
            )
            logger.info(f"Uploaded Parquet to {bucket}/{object_name} ({size} bytes)")
            return size
        except S3Error as e:
            logger.error(f"Error uploading Parquet to {bucket}/{object_name}: {e}")
            raise

    def read_json(self, bucket: str, object_name: str) -> Dict[str, Any]:
        """
        Read JSON data from MinIO.

        Args:
            bucket: Bucket name
            object_name: Object path in bucket

        Returns:
            Dictionary with JSON data
        """
        try:
            response = self.client.get_object(bucket, object_name)
            data: Dict[str, Any] = json.loads(response.read())
            logger.info(f"Read JSON from {bucket}/{object_name}")
            return data
        except S3Error as e:
            logger.error(f"Error reading JSON from {bucket}/{object_name}: {e}")
            raise

    def read_parquet(self, bucket: str, object_name: str) -> pd.DataFrame:
        """
        Read Parquet data from MinIO.

        Args:
            bucket: Bucket name
            object_name: Object path in bucket

        Returns:
            Pandas DataFrame
        """
        try:
            response = self.client.get_object(bucket, object_name)
            df: pd.DataFrame = pd.read_parquet(io.BytesIO(response.read()))
            logger.info(f"Read Parquet from {bucket}/{object_name} ({len(df)} rows)")
            return df
        except S3Error as e:
            logger.error(f"Error reading Parquet from {bucket}/{object_name}: {e}")
            raise

    def list_objects(self, bucket: str, prefix: str = "") -> List[str]:
        """
        List objects in bucket with optional prefix.

        Args:
            bucket: Bucket name
            prefix: Optional prefix to filter objects

        Returns:
            List of object names
        """
        try:
            objects = self.client.list_objects(bucket, prefix=prefix, recursive=True)
            object_list: List[str] = [obj.object_name for obj in objects]
            logger.info(f"Listed {len(object_list)} objects in {bucket} with prefix '{prefix}'")
            return object_list
        except S3Error as e:
            logger.error(f"Error listing objects in {bucket}: {e}")
            raise
