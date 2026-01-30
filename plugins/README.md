# Airflow Plugins Directory

This directory is used by Apache Airflow to load custom plugins and modules.

## Directory Structure

```
plugins/
├── README.md
└── src/          # Empty placeholder - see explanation below
```

## Why is `src/` empty?

The `src/` subdirectory appears empty in the repository, but this is intentional.

When running with Docker, the `docker-compose.yml` mounts the project's `src/` directory directly into this location:

```yaml
volumes:
  - ./plugins:/opt/airflow/plugins
  - ./src:/opt/airflow/plugins/src   # Mounts src/ here at runtime
```

This allows Airflow DAGs to import modules using:

```python
from src.extractor import Extractor
from src.config.app_config import POSTGRES_HOST
from src.utils.minio_client import MinIOClient
```

## Local Development

If running Airflow locally (without Docker), ensure the project root is in your `PYTHONPATH`:

```bash
export PYTHONPATH="${PYTHONPATH}:/path/to/WEATHER_PIPELINE_ETL"
```

## Note

Do not add files directly to `plugins/src/`. All source code should be in the root `src/` directory.
