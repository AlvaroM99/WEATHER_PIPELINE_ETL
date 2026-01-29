"""
MinIO Client Utilities
Handles connection and operations with MinIO data lake
"""

import io
import json
import logging
import os
import sys

from minio import Minio
from minio.error import S3Error

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.weather_config.lake_config import BRONZE_BUCKET, SILVER_BUCKET
from src.weather_config.storage_config import (
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
)

logger = logging.getLogger(__name__)


class MinIOClient:
    """Client for interacting with MinIO data lake"""

    def __init__(self):
        """Initialize MinIO client and ensure buckets exist"""
        self.client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        self._ensure_buckets()

    def _ensure_buckets(self):
        """
        Create buckets if they don't exist
        Supports three API sources: OpenWeather, Open-Meteo, AEMET
        """
        buckets = [
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

    def upload_json(self, bucket, object_name, data):
        """
        Upload JSON data to MinIO

        Args:
            bucket: Bucket name
            object_name: Object path in bucket
            data: Dictionary to upload as JSON

        Returns:
            Size of uploaded data in bytes
        """
        try:
            json_bytes = json.dumps(data, indent=2).encode("utf-8")
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

    def upload_parquet(self, bucket, object_name, dataframe):
        """
        Upload Parquet data to MinIO

        Args:
            bucket: Bucket name
            object_name: Object path in bucket
            dataframe: Pandas DataFrame to upload as Parquet

        Returns:
            Size of uploaded data in bytes
        """
        try:
            parquet_buffer = io.BytesIO()
            dataframe.to_parquet(parquet_buffer, engine="pyarrow", index=False)
            parquet_buffer.seek(0)

            size = parquet_buffer.getbuffer().nbytes

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

    def read_json(self, bucket, object_name):
        """
        Read JSON data from MinIO

        Args:
            bucket: Bucket name
            object_name: Object path in bucket

        Returns:
            Dictionary with JSON data
        """
        try:
            response = self.client.get_object(bucket, object_name)
            data = json.loads(response.read())
            logger.info(f"Read JSON from {bucket}/{object_name}")
            return data
        except S3Error as e:
            logger.error(f"Error reading JSON from {bucket}/{object_name}: {e}")
            raise

    def read_parquet(self, bucket, object_name):
        """
        Read Parquet data from MinIO

        Args:
            bucket: Bucket name
            object_name: Object path in bucket

        Returns:
            Pandas DataFrame
        """
        try:
            import pandas as pd

            response = self.client.get_object(bucket, object_name)
            df = pd.read_parquet(io.BytesIO(response.read()))
            logger.info(f"Read Parquet from {bucket}/{object_name} ({len(df)} rows)")
            return df
        except S3Error as e:
            logger.error(f"Error reading Parquet from {bucket}/{object_name}: {e}")
            raise

    def list_objects(self, bucket, prefix=""):
        """
        List objects in bucket with optional prefix

        Args:
            bucket: Bucket name
            prefix: Optional prefix to filter objects

        Returns:
            List of object names
        """
        try:
            objects = self.client.list_objects(bucket, prefix=prefix, recursive=True)
            object_list = [obj.object_name for obj in objects]
            logger.info(f"Listed {len(object_list)} objects in {bucket} with prefix '{prefix}'")
            return object_list
        except S3Error as e:
            logger.error(f"Error listing objects in {bucket}: {e}")
            raise
