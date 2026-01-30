"""
Utilities Package

Shared utilities for the Weather Pipeline ETL.

Structure:
---------
    - etl_logger: BaseETLLogger mixin for consistent logging
    - lazy_loader: @lazy_load decorator for memoizing values
    - city_utils: City data loading from GitHub
    - minio_client: MinIOClient for data lake operations
    - http_utils: HTTP session with retry logic
    - debug_utils: Diagnostic utilities

Usage:
------
    from src.utils.etl_logger import BaseETLLogger
    from src.utils.minio_client import MinIOClient
    from src.utils.city_utils import get_cities
    from src.utils.http_utils import get_retrying_session
"""
