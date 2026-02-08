"""
Comprehensive tests for LazyLoader utilities module.

Tests cover lazy_load decorator and LazyProperty descriptor to achieve 70%+ coverage.
"""

import time
from unittest.mock import Mock

import pytest

from src.utils.lazy_loader import LazyProperty, lazy_load


class TestLazyLoadDecorator:
    """Test lazy_load decorator."""

    def test_lazy_load_caches_result(self):
        """Test that lazy_load caches the function result."""
        call_count = {"count": 0}

        @lazy_load
        def expensive_function():
            call_count["count"] += 1
            return "expensive_result"

        # First call should execute the function
        result1 = expensive_function()
        assert result1 == "expensive_result"
        assert call_count["count"] == 1

        # Second call should return cached value without executing
        result2 = expensive_function()
        assert result2 == "expensive_result"
        assert call_count["count"] == 1  # Still 1, not incremented

    def test_lazy_load_preserves_function_name(self):
        """Test that lazy_load preserves the original function name."""

        @lazy_load
        def my_function():
            return "result"

        assert my_function.__name__ == "my_function"

    def test_lazy_load_clear_cache(self):
        """Test that clear_cache() resets the cached value."""
        call_count = {"count": 0}

        @lazy_load
        def get_value():
            call_count["count"] += 1
            return call_count["count"]

        # First call
        result1 = get_value()
        assert result1 == 1

        # Second call (cached)
        result2 = get_value()
        assert result2 == 1

        # Clear cache
        get_value.clear_cache()

        # Third call (re-executed)
        result3 = get_value()
        assert result3 == 2

    def test_lazy_load_with_different_return_types(self):
        """Test lazy_load with various return types."""

        @lazy_load
        def get_dict():
            return {"key": "value"}

        @lazy_load
        def get_list():
            return [1, 2, 3]

        @lazy_load
        def get_int():
            return 42

        assert get_dict() == {"key": "value"}
        assert get_list() == [1, 2, 3]
        assert get_int() == 42

    def test_lazy_load_with_none_return(self):
        """Test lazy_load when function returns None."""

        @lazy_load
        def get_none():
            return None

        result = get_none()
        assert result is None

    def test_lazy_load_with_exception(self):
        """Test lazy_load when function raises exception."""

        @lazy_load
        def failing_function():
            raise ValueError("Test error")

        with pytest.raises(ValueError, match="Test error"):
            failing_function()

    def test_lazy_load_performance(self):
        """Test that lazy_load improves performance by caching."""

        @lazy_load
        def slow_function():
            time.sleep(0.1)  # Simulate expensive operation
            return "result"

        # First call (slow)
        start1 = time.time()
        result1 = slow_function()
        duration1 = time.time() - start1

        # Second call (fast, cached)
        start2 = time.time()
        result2 = slow_function()
        duration2 = time.time() - start2

        assert result1 == result2
        assert duration2 < duration1 / 10  # Cached call should be much faster


class TestLazyProperty:
    """Test LazyProperty descriptor."""

    def test_lazy_property_basic_usage(self):
        """Test basic LazyProperty functionality."""
        call_count = {"count": 0}

        class Config:
            @LazyProperty
            def api_key(self):
                call_count["count"] += 1
                return "secret_key"

        config = Config()

        # First access
        key1 = config.api_key
        assert key1 == "secret_key"
        assert call_count["count"] == 1

        # Second access (cached)
        key2 = config.api_key
        assert key2 == "secret_key"
        assert call_count["count"] == 1  # Not incremented

    def test_lazy_property_multiple_instances(self):
        """Test that LazyProperty caches per instance."""
        call_count = {"count": 0}

        class Config:
            def __init__(self, value):
                self.value = value

            @LazyProperty
            def computed_value(self):
                call_count["count"] += 1
                return self.value * 2

        config1 = Config(10)
        config2 = Config(20)

        # Access config1
        assert config1.computed_value == 20
        assert call_count["count"] == 1

        # Access config2
        assert config2.computed_value == 40
        assert call_count["count"] == 2

        # Access config1 again (cached)
        assert config1.computed_value == 20
        assert call_count["count"] == 2  # Not incremented

    def test_lazy_property_class_access(self):
        """Test accessing LazyProperty from class (not instance)."""

        class Config:
            @LazyProperty
            def api_key(self):
                return "secret_key"

        # Accessing from class should return the descriptor itself
        descriptor = Config.api_key
        assert isinstance(descriptor, LazyProperty)

    def test_lazy_property_with_none(self):
        """Test LazyProperty when function returns None."""

        class Config:
            @LazyProperty
            def nullable_value(self):
                return None

        config = Config()
        assert config.nullable_value is None

    def test_lazy_property_with_exception(self):
        """Test LazyProperty when function raises exception."""

        class Config:
            @LazyProperty
            def failing_property(self):
                raise ValueError("Property error")

        config = Config()

        with pytest.raises(ValueError, match="Property error"):
            _ = config.failing_property

    def test_lazy_property_attribute_name_storage(self):
        """Test that LazyProperty stores value with correct attribute name."""

        class Config:
            @LazyProperty
            def my_value(self):
                return "stored_value"

        config = Config()
        _ = config.my_value

        # Check that the cached value is stored with the correct name
        assert hasattr(config, "_lazy_my_value")
        assert getattr(config, "_lazy_my_value") == "stored_value"

    def test_lazy_property_different_types(self):
        """Test LazyProperty with different return types."""

        class Config:
            @LazyProperty
            def dict_value(self):
                return {"key": "value"}

            @LazyProperty
            def list_value(self):
                return [1, 2, 3]

            @LazyProperty
            def int_value(self):
                return 42

        config = Config()

        assert config.dict_value == {"key": "value"}
        assert config.list_value == [1, 2, 3]
        assert config.int_value == 42


class TestIntegrationScenarios:
    """Test real-world integration scenarios."""

    def test_lazy_load_for_config_loading(self):
        """Test lazy_load for configuration loading scenario."""

        @lazy_load
        def load_config():
            # Simulate expensive config loading
            return {"database": "postgres", "host": "localhost", "port": 5432}

        # Multiple accesses should only load once
        config1 = load_config()
        config2 = load_config()

        assert config1 is config2  # Same object reference

    def test_lazy_property_for_api_client(self):
        """Test LazyProperty for lazy API client initialization."""

        class ApiClient:
            @LazyProperty
            def connection(self):
                # Simulate expensive connection setup
                return Mock(name="MockConnection")

        client = ApiClient()

        # Connection should only be created on first access
        conn1 = client.connection
        conn2 = client.connection

        assert conn1 is conn2  # Same connection object

    def test_combined_usage(self):
        """Test using both lazy_load and LazyProperty together."""

        @lazy_load
        def get_global_config():
            return {"setting": "value"}

        class Service:
            @LazyProperty
            def config(self):
                return get_global_config()

        service = Service()
        config = service.config

        assert config == {"setting": "value"}
