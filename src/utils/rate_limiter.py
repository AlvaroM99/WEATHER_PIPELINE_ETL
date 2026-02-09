"""
Configurable Rate Limiter for API Requests

Provides production-ready rate limiting with:
- Token bucket algorithm with configurable rates
- Exponential backoff with jitter for retry logic
- Per-API endpoint configuration
- Thread-safe implementation

Usage:
    # Basic usage with default rate limiter
    limiter = RateLimiter(max_calls=10, period_seconds=1.0)

    with limiter:
        response = requests.get(url)

    # Advanced usage with exponential backoff
    @with_exponential_backoff(max_retries=3)
    def fetch_data(url):
        return requests.get(url)
"""

from __future__ import annotations

import random
import time
from datetime import datetime
from functools import wraps
from threading import Lock
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


class RateLimiter:
    """
    Token bucket rate limiter with configurable rate.

    Thread-safe implementation using a token bucket algorithm.
    Replaces hardcoded time.sleep() calls with intelligent rate limiting.

    Attributes:
        max_calls: Maximum number of calls allowed in the time period
        period_seconds: Time period in seconds
        tokens: Current available tokens
        last_refill: Timestamp of last token refill
    """

    def __init__(self, max_calls: int, period_seconds: float) -> None:
        """
        Initialize rate limiter.

        Args:
            max_calls: Maximum number of API calls allowed in period_seconds
            period_seconds: Time window in seconds (e.g., 1.0 for 1 second, 60.0 for 1 minute)
        """
        self.max_calls = max_calls
        self.period_seconds = period_seconds
        self.tokens = float(max_calls)
        self.last_refill = datetime.now()
        self.lock = Lock()

    def _refill_tokens(self) -> None:
        """Refill tokens based on elapsed time."""
        now = datetime.now()
        elapsed = (now - self.last_refill).total_seconds()

        # Calculate tokens to add based on elapsed time
        tokens_to_add = (elapsed / self.period_seconds) * self.max_calls
        self.tokens = min(self.max_calls, self.tokens + tokens_to_add)
        self.last_refill = now

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquire a token to make an API call.

        Args:
            blocking: If True, blocks until token is available
            timeout: Maximum time to wait in seconds (only used if blocking=True)

        Returns:
            True if token acquired, False if not available (non-blocking mode)
        """
        start_time = datetime.now()

        while True:
            with self.lock:
                self._refill_tokens()

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return True

                if not blocking:
                    return False

                # Check timeout
                if timeout is not None:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    if elapsed >= timeout:
                        return False

            # Sleep for a short time before retrying
            time.sleep(0.01)

    def __enter__(self) -> RateLimiter:
        """Context manager entry - acquire token."""
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        pass


class ExponentialBackoff:
    """
    Exponential backoff with jitter for retry logic.

    Implements best practices for API retry strategies:
    - Exponential delay between retries
    - Jitter to prevent thundering herd
    - Configurable max delay cap
    """

    def __init__(
        self,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
    ) -> None:
        """
        Initialize exponential backoff strategy.

        Args:
            base_delay: Initial delay in seconds (default 1.0)
            max_delay: Maximum delay cap in seconds (default 60.0)
            exponential_base: Base for exponential growth (default 2.0)
            jitter: Whether to add random jitter (default True)
        """
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter

    def get_delay(self, retry_count: int) -> float:
        """
        Calculate delay for given retry attempt.

        Args:
            retry_count: Current retry attempt (0-indexed)

        Returns:
            Delay in seconds to wait before retry
        """
        # Calculate exponential delay
        delay = min(self.base_delay * (self.exponential_base**retry_count), self.max_delay)

        # Add jitter to prevent thundering herd
        if self.jitter:
            delay = delay * (0.5 + random.random() * 0.5)

        return delay


def with_exponential_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator for automatic retry with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay cap in seconds
        exceptions: Tuple of exception types to catch and retry

    Returns:
        Decorated function with retry logic

    Example:
        @with_exponential_backoff(max_retries=3, base_delay=1.0)
        def fetch_weather(city: str) -> dict:
            response = requests.get(f"https://api.example.com/weather/{city}")
            response.raise_for_status()
            return response.json()
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            backoff = ExponentialBackoff(base_delay=base_delay, max_delay=max_delay)
            last_exception: Optional[Exception] = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = backoff.get_delay(attempt)
                        time.sleep(delay)
                    else:
                        # Max retries exceeded, re-raise
                        raise

            # Should never reach here, but type checker needs it
            if last_exception:
                raise last_exception
            raise RuntimeError("Retry logic failed unexpectedly")

        return wrapper

    return decorator


# Pre-configured rate limiters for common APIs
class APIRateLimiters:
    """Pre-configured rate limiters for external APIs."""

    # OpenWeatherMap: 60 calls/minute free tier
    OPENWEATHER = RateLimiter(max_calls=60, period_seconds=60.0)

    # Open-Meteo: No official limit, but recommend 10 calls/second to be respectful
    OPENMETEO = RateLimiter(max_calls=10, period_seconds=1.0)

    # AEMET: 60 calls/minute (official limit)
    AEMET = RateLimiter(max_calls=60, period_seconds=60.0)

    # MinIO: Internal, higher limits (100 calls/second)
    MINIO = RateLimiter(max_calls=100, period_seconds=1.0)


# Configurable rate limiter for easy overrides via environment variables
def get_rate_limiter(
    service: str,
    max_calls: Optional[int] = None,
    period_seconds: Optional[float] = None,
) -> RateLimiter:
    """
    Get a rate limiter for a specific service.

    Can be overridden via environment variables:
    - RATE_LIMIT_{SERVICE}_MAX_CALLS
    - RATE_LIMIT_{SERVICE}_PERIOD_SECONDS

    Args:
        service: Service name ("openweather", "openmeteo", "aemet", "minio")
        max_calls: Override max calls (optional)
        period_seconds: Override period (optional)

    Returns:
        RateLimiter instance for the service
    """
    import os

    service_upper = service.upper()

    # Check environment variables for overrides
    env_max_calls = os.getenv(f"RATE_LIMIT_{service_upper}_MAX_CALLS")
    env_period = os.getenv(f"RATE_LIMIT_{service_upper}_PERIOD_SECONDS")

    if env_max_calls:
        max_calls = int(env_max_calls)
    if env_period:
        period_seconds = float(env_period)

    # Use provided values or defaults from pre-configured limiters
    if max_calls is not None and period_seconds is not None:
        return RateLimiter(max_calls=max_calls, period_seconds=period_seconds)

    # Return pre-configured limiter
    limiter_map = {
        "openweather": APIRateLimiters.OPENWEATHER,
        "openmeteo": APIRateLimiters.OPENMETEO,
        "aemet": APIRateLimiters.AEMET,
        "minio": APIRateLimiters.MINIO,
    }

    return limiter_map.get(service.lower(), RateLimiter(max_calls=10, period_seconds=1.0))
