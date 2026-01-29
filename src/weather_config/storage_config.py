"""
Storage Configuration
MinIO connection settings
"""

import os

# MinIO Connection Settings
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "MinIO2024!Secure")
MINIO_SECURE = False  # Use HTTP inside Docker network
