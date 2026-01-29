"""
HTTP Utilities
Provides retry logic and session management for API requests
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def get_retrying_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504)):
    """
    Create a requests session with automatic retry logic

    Args:
        retries: Number of retry attempts
        backoff_factor: Backoff factor for exponential backoff
        status_forcelist: HTTP status codes to retry on

    Returns:
        requests.Session: Configured session with retry logic
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
