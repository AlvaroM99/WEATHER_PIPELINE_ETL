"""
Unit tests for MinIO Client
Tests JSON/Parquet upload/download operations and bucket management
"""

import json
from io import BytesIO
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
from minio.error import S3Error

from src.utils.minio_client import MinIOClient, reset_minio_client


@pytest.fixture(autouse=True)
def reset_minio_singleton():
    """Reset MinIO singleton before and after each test."""
    reset_minio_client()
    yield
    reset_minio_client()


@pytest.mark.unit
@patch("src.utils.minio_client.Minio")
def test_minio_client_initialization(mock_minio_class):
    """Test MinIO client initialization and bucket creation"""
    # Setup mock
    mock_client_instance = Mock()
    mock_client_instance.bucket_exists.return_value = False
    mock_minio_class.return_value = mock_client_instance

    # Execute
    client = MinIOClient()

    # Assert
    assert client is not None
    assert client.client is not None

    # Verify buckets were created
    expected_buckets = [
        "bronze-openweather",
        "silver-openweather",
        "bronze-openmeteo",
        "silver-openmeteo",
        "bronze-aemet",
        "silver-aemet",
    ]

    # Verify bucket_exists was called for all buckets
    assert mock_client_instance.bucket_exists.call_count == len(expected_buckets)

    # Verify make_bucket was called for all buckets (since bucket_exists returns False)
    assert mock_client_instance.make_bucket.call_count == len(expected_buckets)


@pytest.mark.unit
@patch("src.utils.minio_client.Minio")
def test_minio_client_buckets_already_exist(mock_minio_class):
    """Test initialization when buckets already exist"""
    # Setup mock - buckets exist
    mock_client_instance = Mock()
    mock_client_instance.bucket_exists.return_value = True
    mock_minio_class.return_value = mock_client_instance

    # Execute
    client = MinIOClient()

    # Assert - make_bucket should NOT be called if buckets exist
    assert mock_client_instance.make_bucket.call_count == 0


@pytest.mark.unit
@patch("src.utils.minio_client.Minio")
def test_upload_json_success(mock_minio_class):
    """Test successful JSON upload to MinIO"""
    # Setup mock
    mock_client_instance = Mock()
    mock_client_instance.bucket_exists.return_value = True
    mock_client_instance.put_object.return_value = None
    mock_minio_class.return_value = mock_client_instance

    # Test data
    test_data = {"city": "Madrid", "temperature": 25.5, "humidity": 60}
    bucket = "bronze-openweather"
    object_name = "test/madrid.json"

    # Execute
    client = MinIOClient()
    size = client.upload_json(bucket, object_name, test_data)

    # Assert
    assert size > 0
    assert mock_client_instance.put_object.called

    # Verify put_object was called with correct arguments
    call_args = mock_client_instance.put_object.call_args
    assert call_args[0][0] == bucket
    assert call_args[0][1] == object_name

    # Verify JSON was serialized correctly
    uploaded_bytes = call_args[0][2].read()
    uploaded_data = json.loads(uploaded_bytes.decode("utf-8"))
    assert uploaded_data == test_data


@pytest.mark.unit
@patch("src.utils.minio_client.Minio")
def test_upload_json_s3_error(mock_minio_class):
    """Test JSON upload handling S3 errors"""
    # Setup mock to raise S3Error
    mock_client_instance = Mock()
    mock_client_instance.bucket_exists.return_value = True
    mock_client_instance.put_object.side_effect = S3Error(
        "BucketNotFound", "message", "resource", "request_id", "host_id", None
    )
    mock_minio_class.return_value = mock_client_instance

    # Execute and expect exception
    client = MinIOClient()
    with pytest.raises(S3Error):
        client.upload_json("nonexistent-bucket", "test.json", {"key": "value"})


@pytest.mark.unit
@patch("src.utils.minio_client.Minio")
def test_read_json_success(mock_minio_class):
    """Test successful JSON read from MinIO"""
    # Setup mock
    test_data = {"city": "Barcelona", "temperature": 22.3}
    json_bytes = json.dumps(test_data).encode("utf-8")

    mock_response = Mock()
    mock_response.read.return_value = json_bytes

    mock_client_instance = Mock()
    mock_client_instance.bucket_exists.return_value = True
    mock_client_instance.get_object.return_value = mock_response
    mock_minio_class.return_value = mock_client_instance

    # Execute
    client = MinIOClient()
    result = client.read_json("bronze-openweather", "test/barcelona.json")

    # Assert
    assert result == test_data
    assert mock_client_instance.get_object.called


@pytest.mark.unit
@patch("src.utils.minio_client.Minio")
def test_upload_parquet_success(mock_minio_class):
    """Test successful Parquet upload to MinIO"""
    # Setup mock
    mock_client_instance = Mock()
    mock_client_instance.bucket_exists.return_value = True
    mock_client_instance.put_object.return_value = None
    mock_minio_class.return_value = mock_client_instance

    # Test DataFrame
    test_df = pd.DataFrame(
        {
            "city": ["Madrid", "Barcelona", "Sevilla"],
            "temperature": [25.5, 22.3, 28.1],
            "humidity": [60, 65, 55],
        }
    )

    bucket = "silver-openweather"
    object_name = "transformed/2026-01-29_weather.parquet"

    # Execute
    client = MinIOClient()
    size = client.upload_parquet(bucket, object_name, test_df)

    # Assert
    assert size > 0
    assert mock_client_instance.put_object.called

    # Verify put_object was called with correct bucket/object
    call_args = mock_client_instance.put_object.call_args
    assert call_args[0][0] == bucket
    assert call_args[0][1] == object_name

    # Verify content type is set correctly
    assert call_args[1]["content_type"] == "application/octet-stream"


@pytest.mark.unit
@patch("src.utils.minio_client.Minio")
def test_read_parquet_success(mock_minio_class):
    """Test successful Parquet read from MinIO"""
    # Setup mock
    test_df = pd.DataFrame({"city": ["Madrid", "Barcelona"], "temperature": [25.5, 22.3]})

    # Convert DataFrame to Parquet bytes
    parquet_buffer = BytesIO()
    test_df.to_parquet(parquet_buffer, engine="pyarrow", index=False)
    parquet_buffer.seek(0)
    parquet_bytes = parquet_buffer.read()

    mock_response = Mock()
    mock_response.read.return_value = parquet_bytes

    mock_client_instance = Mock()
    mock_client_instance.bucket_exists.return_value = True
    mock_client_instance.get_object.return_value = mock_response
    mock_minio_class.return_value = mock_client_instance

    # Execute
    client = MinIOClient()
    result_df = client.read_parquet("silver-openweather", "test.parquet")

    # Assert
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 2
    assert "city" in result_df.columns
    assert "temperature" in result_df.columns
    pd.testing.assert_frame_equal(result_df, test_df)


@pytest.mark.unit
@patch("src.utils.minio_client.Minio")
def test_list_objects_success(mock_minio_class):
    """Test successful listing of objects in bucket"""
    # Setup mock
    mock_obj1 = Mock()
    mock_obj1.object_name = "current/2026-01-29/madrid.json"

    mock_obj2 = Mock()
    mock_obj2.object_name = "current/2026-01-29/barcelona.json"

    mock_client_instance = Mock()
    mock_client_instance.bucket_exists.return_value = True
    mock_client_instance.list_objects.return_value = [mock_obj1, mock_obj2]
    mock_minio_class.return_value = mock_client_instance

    # Execute
    client = MinIOClient()
    objects = client.list_objects("bronze-openweather", prefix="current/2026-01-29/")

    # Assert
    assert len(objects) == 2
    assert "current/2026-01-29/madrid.json" in objects
    assert "current/2026-01-29/barcelona.json" in objects


@pytest.mark.unit
@patch("src.utils.minio_client.Minio")
def test_list_objects_empty_bucket(mock_minio_class):
    """Test listing objects in empty bucket"""
    # Setup mock
    mock_client_instance = Mock()
    mock_client_instance.bucket_exists.return_value = True
    mock_client_instance.list_objects.return_value = []
    mock_minio_class.return_value = mock_client_instance

    # Execute
    client = MinIOClient()
    objects = client.list_objects("bronze-openweather")

    # Assert
    assert objects == []


@pytest.mark.unit
@patch("src.utils.minio_client.Minio")
def test_json_roundtrip(mock_minio_class):
    """Test JSON upload and download roundtrip"""
    # Setup mock
    stored_data = {}

    def mock_put_object(bucket, object_name, data_stream, length, content_type):
        stored_data["content"] = data_stream.read()

    def mock_get_object(bucket, object_name):
        mock_response = Mock()
        mock_response.read.return_value = stored_data["content"]
        return mock_response

    mock_client_instance = Mock()
    mock_client_instance.bucket_exists.return_value = True
    mock_client_instance.put_object.side_effect = mock_put_object
    mock_client_instance.get_object.side_effect = mock_get_object
    mock_minio_class.return_value = mock_client_instance

    # Test data
    original_data = {
        "city": "Madrid",
        "weather": {"temp": 25.5, "humidity": 60},
        "timestamp": "2026-01-29T12:00:00",
    }

    # Execute upload
    client = MinIOClient()
    client.upload_json("test-bucket", "test.json", original_data)

    # Execute download
    retrieved_data = client.read_json("test-bucket", "test.json")

    # Assert
    assert retrieved_data == original_data


@pytest.mark.unit
@patch("src.utils.minio_client.Minio")
def test_parquet_dataframe_preservation(mock_minio_class):
    """Test that DataFrame dtypes and structure are preserved in Parquet roundtrip"""
    # Setup mock
    stored_data = {}

    def mock_put_object(bucket, object_name, data_stream, length, content_type):
        stored_data["content"] = data_stream.read()

    def mock_get_object(bucket, object_name):
        mock_response = Mock()
        mock_response.read.return_value = stored_data["content"]
        return mock_response

    mock_client_instance = Mock()
    mock_client_instance.bucket_exists.return_value = True
    mock_client_instance.put_object.side_effect = mock_put_object
    mock_client_instance.get_object.side_effect = mock_get_object
    mock_minio_class.return_value = mock_client_instance

    # Test DataFrame with mixed types
    original_df = pd.DataFrame(
        {
            "city": ["Madrid", "Barcelona"],
            "temperature": [25.5, 22.3],
            "humidity": [60, 65],
            "weather_code": [800, 801],
        }
    )

    # Execute upload
    client = MinIOClient()
    client.upload_parquet("test-bucket", "test.parquet", original_df)

    # Execute download
    retrieved_df = client.read_parquet("test-bucket", "test.parquet")

    # Assert
    pd.testing.assert_frame_equal(retrieved_df, original_df)
    assert retrieved_df["city"].dtype == original_df["city"].dtype
    assert retrieved_df["temperature"].dtype == original_df["temperature"].dtype


@pytest.mark.unit
@patch("src.utils.minio_client.Minio")
def test_bucket_creation_error_handling(mock_minio_class):
    """Test error handling during bucket creation"""
    # Setup mock to raise S3Error on make_bucket
    mock_client_instance = Mock()
    mock_client_instance.bucket_exists.return_value = False
    mock_client_instance.make_bucket.side_effect = S3Error(
        "AccessDenied", "Access denied", "resource", "request_id", "host_id", None
    )
    mock_minio_class.return_value = mock_client_instance

    # Execute and expect exception
    with pytest.raises(S3Error):
        MinIOClient()
