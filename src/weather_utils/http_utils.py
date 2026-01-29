"""
HTTP Utilities

Type-annotated module providing retry logic and session management for API requests.
"""

from __future__ import annotations

from typing import Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def get_retrying_session(
    retries: int = 3,
    backoff_factor: float = 0.3,
    status_forcelist: Tuple[int, ...] = (500, 502, 504),
) -> requests.Session:
    """
    Create a requests session with automatic retry logic.

    Args:
        retries: Number of retry attempts
        backoff_factor: Backoff factor for exponential backoff
        status_forcelist: HTTP status codes to retry on

    Returns:
        Configured session with retry logic
    """
    session: requests.Session = requests.Session()
    retry: Retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
    )
    adapter: HTTPAdapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
