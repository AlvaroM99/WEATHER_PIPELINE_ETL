"""
Unit tests for HTTP utilities
Tests retry logic, backoff, and session configuration
"""
import pytest
import responses
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.weather_utils.http_utils import get_retrying_session


@pytest.mark.unit
def test_get_retrying_session_returns_session():
    """Test that get_retrying_session returns a valid Session"""
    session = get_retrying_session()

    assert session is not None
    assert isinstance(session, requests.Session)


@pytest.mark.unit
def test_get_retrying_session_has_retry_adapter():
    """Test that session has HTTPAdapter with retry configuration"""
    session = get_retrying_session()

    # Check that adapters are configured for both http and https
    http_adapter = session.get_adapter('http://example.com')
    https_adapter = session.get_adapter('https://example.com')

    assert isinstance(http_adapter, HTTPAdapter)
    assert isinstance(https_adapter, HTTPAdapter)


@pytest.mark.unit
def test_get_retrying_session_default_parameters():
    """Test default parameters of retry configuration"""
    session = get_retrying_session()

    # Get adapter and its retry configuration
    adapter = session.get_adapter('https://example.com')
    retry_config = adapter.max_retries

    assert isinstance(retry_config, Retry)
    assert retry_config.total == 3
    assert retry_config.backoff_factor == 0.3
    assert retry_config.status_forcelist == (500, 502, 504)


@pytest.mark.unit
def test_get_retrying_session_custom_parameters():
    """Test custom retry parameters"""
    custom_retries = 5
    custom_backoff = 0.5
    custom_status_list = (500, 502, 503, 504)

    session = get_retrying_session(
        retries=custom_retries,
        backoff_factor=custom_backoff,
        status_forcelist=custom_status_list
    )

    adapter = session.get_adapter('https://example.com')
    retry_config = adapter.max_retries

    assert retry_config.total == custom_retries
    assert retry_config.backoff_factor == custom_backoff
    assert retry_config.status_forcelist == custom_status_list


@pytest.mark.unit
@responses.activate
def test_retry_on_500_error():
    """Test that session retries on 500 Internal Server Error"""
    url = "https://api.example.com/data"

    # First two requests return 500, third succeeds
    responses.add(responses.GET, url, json={"error": "server error"}, status=500)
    responses.add(responses.GET, url, json={"error": "server error"}, status=500)
    responses.add(responses.GET, url, json={"data": "success"}, status=200)

    session = get_retrying_session(retries=3)
    response = session.get(url)

    # Assert final response is successful
    assert response.status_code == 200
    assert response.json() == {"data": "success"}

    # Verify it made 3 attempts (2 failures + 1 success)
    assert len(responses.calls) == 3


@pytest.mark.unit
@responses.activate
def test_retry_on_502_bad_gateway():
    """Test that session retries on 502 Bad Gateway"""
    url = "https://api.example.com/data"

    # First request returns 502, second succeeds
    responses.add(responses.GET, url, status=502)
    responses.add(responses.GET, url, json={"data": "ok"}, status=200)

    session = get_retrying_session(retries=3)
    response = session.get(url)

    # Assert
    assert response.status_code == 200
    assert len(responses.calls) == 2


@pytest.mark.unit
@responses.activate
def test_retry_on_504_gateway_timeout():
    """Test that session retries on 504 Gateway Timeout"""
    url = "https://api.example.com/data"

    # First request returns 504, second succeeds
    responses.add(responses.GET, url, status=504)
    responses.add(responses.GET, url, json={"data": "ok"}, status=200)

    session = get_retrying_session(retries=3)
    response = session.get(url)

    # Assert
    assert response.status_code == 200
    assert len(responses.calls) == 2


@pytest.mark.unit
@responses.activate
def test_no_retry_on_400_error():
    """Test that session does NOT retry on 400 Bad Request"""
    url = "https://api.example.com/data"

    # Only one response - 400 error
    responses.add(responses.GET, url, json={"error": "bad request"}, status=400)

    session = get_retrying_session(retries=3)
    response = session.get(url)

    # Assert - should fail immediately without retries
    assert response.status_code == 400
    assert len(responses.calls) == 1  # No retries on 400


@pytest.mark.unit
@responses.activate
def test_no_retry_on_401_unauthorized():
    """Test that session does NOT retry on 401 Unauthorized"""
    url = "https://api.example.com/data"

    responses.add(responses.GET, url, json={"error": "unauthorized"}, status=401)

    session = get_retrying_session(retries=3)
    response = session.get(url)

    # Assert - should fail immediately
    assert response.status_code == 401
    assert len(responses.calls) == 1


@pytest.mark.unit
@responses.activate
def test_max_retries_exceeded():
    """Test behavior when max retries is exceeded"""
    url = "https://api.example.com/data"

    # All requests return 500
    for _ in range(5):
        responses.add(responses.GET, url, status=500)

    session = get_retrying_session(retries=3)

    # Should raise exception after exhausting retries
    with pytest.raises(requests.exceptions.RetryError):
        session.get(url)

    # Verify it attempted 1 + 3 retries = 4 total attempts
    assert len(responses.calls) == 4


@pytest.mark.unit
@responses.activate
def test_successful_request_no_retry():
    """Test that successful requests don't trigger retries"""
    url = "https://api.example.com/data"

    responses.add(responses.GET, url, json={"data": "ok"}, status=200)

    session = get_retrying_session(retries=3)
    response = session.get(url)

    # Assert - only one call made
    assert response.status_code == 200
    assert len(responses.calls) == 1


@pytest.mark.unit
@responses.activate
def test_post_request_retry():
    """Test that POST requests also have retry logic"""
    url = "https://api.example.com/data"

    # First POST fails, second succeeds
    responses.add(responses.POST, url, status=500)
    responses.add(responses.POST, url, json={"created": True}, status=201)

    session = get_retrying_session(retries=3)
    response = session.post(url, json={"key": "value"})

    # Assert
    assert response.status_code == 201
    assert len(responses.calls) == 2


@pytest.mark.unit
def test_retry_allowed_methods():
    """Test that retry configuration includes correct HTTP methods"""
    session = get_retrying_session()

    adapter = session.get_adapter('https://example.com')
    retry_config = adapter.max_retries

    # Verify allowed methods
    allowed_methods = retry_config.allowed_methods
    assert "GET" in allowed_methods
    assert "POST" in allowed_methods
    assert "HEAD" in allowed_methods
    assert "OPTIONS" in allowed_methods


@pytest.mark.unit
@responses.activate
def test_custom_status_forcelist():
    """Test using custom status codes for retry"""
    url = "https://api.example.com/data"

    # Add 429 (Too Many Requests) to retry list
    responses.add(responses.GET, url, status=429)
    responses.add(responses.GET, url, json={"data": "ok"}, status=200)

    session = get_retrying_session(
        retries=3,
        status_forcelist=(429, 500, 502, 504)
    )
    response = session.get(url)

    # Assert - should retry on 429 and succeed
    assert response.status_code == 200
    assert len(responses.calls) == 2


@pytest.mark.unit
def test_session_mounts_http_and_https():
    """Test that session has adapters for both HTTP and HTTPS"""
    session = get_retrying_session()

    # Test that adapters are mounted
    http_adapter = session.get_adapter('http://example.com')
    https_adapter = session.get_adapter('https://example.com')

    assert http_adapter is not None
    assert https_adapter is not None

    # Verify both are HTTPAdapter instances
    assert isinstance(http_adapter, HTTPAdapter)
    assert isinstance(https_adapter, HTTPAdapter)


@pytest.mark.unit
@responses.activate
@pytest.mark.slow
def test_backoff_factor_timing():
    """Test that backoff factor delays retries (marked as slow test)"""
    import time
    url = "https://api.example.com/data"

    # All requests fail
    for _ in range(3):
        responses.add(responses.GET, url, status=500)

    session = get_retrying_session(retries=2, backoff_factor=0.1)

    start_time = time.time()
    try:
        session.get(url, timeout=5)
    except requests.exceptions.RetryError:
        pass
    elapsed_time = time.time() - start_time

    # With backoff_factor=0.1, delays are: 0.0s, 0.2s, 0.4s
    # Total should be at least 0.6 seconds
    # We use a lower threshold to account for test execution speed
    assert elapsed_time >= 0.3, f"Expected backoff delays, got {elapsed_time}s"


@pytest.mark.unit
def test_zero_retries():
    """Test session configuration with zero retries"""
    session = get_retrying_session(retries=0)

    adapter = session.get_adapter('https://example.com')
    retry_config = adapter.max_retries

    assert retry_config.total == 0
