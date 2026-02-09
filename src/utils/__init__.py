"""
Utilities Package

Shared utilities for the Weather Pipeline ETL.

Structure:
---------
    - etl_logger: BaseETLLogger mixin for consistent logging
    - structured_logger: Structured logging utilities
    - lazy_loader: @lazy_load decorator for memoizing values
    - city_utils: City data loading from GitHub
    - minio_client: MinIOClient for data lake operations
    - http_utils: HTTP session with retry logic
    - debug_utils: Diagnostic utilities
    - rate_limiter: API rate limiting utilities

Logger Modules Explained:
-------------------------
This package contains TWO logger modules that serve different purposes:

1. **etl_logger.py** - BaseETLLogger Mixin
   - Purpose: Provides a standardized logging interface for ETL classes
   - Usage: Inherit from BaseETLLogger to get a class-specific logger
   - Features: Automatic logger naming based on class name
   - Example:
     ```python
     class Extractor(BaseETLLogger):
         def __init__(self):
             super().__init__()  # Creates self.logger
             self.logger.info("Extractor initialized")
     ```

2. **structured_logger.py** - Structured Logging Utilities
   - Purpose: Provides structured logging helpers and formatters
   - Usage: Standalone functions for structured log output
   - Features: JSON formatting, context enrichment, log aggregation
   - Example:
     ```python
     from src.utils.structured_logger import log_with_context
     log_with_context(logger, "event", {"key": "value"})
     ```

Both modules are complementary and serve different use cases:
- Use BaseETLLogger for class-based ETL components (Extractor, Loader, Transformer)
- Use structured_logger for utility functions and advanced log formatting

Usage:
------
    from src.utils.etl_logger import BaseETLLogger
    from src.utils.minio_client import MinIOClient
    from src.utils.city_utils import get_cities
    from src.utils.http_utils import get_retrying_session
"""
