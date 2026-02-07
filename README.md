# 🌦️ WEATHER_PIPELINE_ETL
> **Enterprise-Grade ETL Architecture for Meteorological Data Processing**

![Python](https://img.shields.io/badge/Python-3.11-blue?style=for-the-badge&logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.9.1-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-RELEASE.2024-c72c48?style=for-the-badge&logo=minio&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Pro-2496ED?style=for-the-badge&logo=docker&logoColor=white)

## 📋 Executive Summary

**WEATHER_PIPELINE_ETL** is a robust, scalable, and modular data engineering solution designed to ingest, transform, and analyze meteorological data from heterogeneous sources. Built with a "production-first" mindset, this project demonstrates advanced Data Engineering practices, including a multi-layered Medallion Architecture, comprehensive Data Quality checks, and a fully containerized microservices infrastructure.

The system integrates real-time and historical data from **OpenWeatherMap**, **Open-Meteo**, and **AEMET** (Spain's State Meteorological Agency), providing a unified analytical layer for weather forecasting, climatology studies, and marine conditions analysis.

---

## 🏗️ System Architecture

The project follows a strict **Medallion Architecture** ensuring data traceability, quality, and optimized storage formats.

```
Docker Container
┌─────────────────────────────────────────────────────────────┐
│                                                             |                                                                                                                                                                      
┌────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                           │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐    │
│  │ OpenWeather  │   │  Open-Meteo  │   │    AEMET     │    │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘    │
└─────────┼──────────────────┼──────────────────┼────────────┘
          │                  │                  │
          ▼                  ▼                  ▼
┌────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (MinIO)                    │
│              Raw JSON - Immutable Landing Zone             │
│  bronze-openweather/  bronze-openmeteo/  bronze-aemet/     │
└─────────────────────────┬──────────────────────────────────┘
                          │
                          ▼
┌────────────────────────────────────────────────────────────┐
│                    SILVER LAYER (MinIO)                    │
│           Cleaned Parquet - Standardized Schema            │
│  silver-openweather/  silver-openmeteo/  silver-aemet/     │
└─────────────────────────┬──────────────────────────────────┘
                          │                                              
                          ▼
┌────────────────────────────────────────────────────────────┐
│                 GOLD LAYER (PostgreSQL DWH)                │
│              Star Schema - Analytics-Ready                 │
│  dwh.fct_observation  dwh.fct_forecast_daily               │
│  dwh.dim_city  dwh.dim_date  dwh.dim_weather_station       │
└────────────────────────────────────────────────────────────┘

```

### 📋 Deep Dive: Implementation Details

#### 1. Data Lake Strategy (MinIO)
The Data Lake is the backbone of our storage strategy.
-   **Bucket Policy**: We use distinct buckets for `bronze` (raw JSON) and `silver` (processed Parquet) to allow different retention policies (e.g., Bronze deleted after 30 days, Silver kept forever).
-   **Implementation**: The `MinIOClient` (Singleton) ensures these buckets exist on startup (`_ensure_buckets` method), preventing "Bucket Not Found" runtime errors.
-   **Path Structure**: `source/date/file_timestamp.ext` ensures that consecutive runs never overwrite previous data, providing full history.

#### 2. The Staging Layer (PostgreSQL Schema)
A dedicated `staging` schema acts as a quality gate before data enters the production Warehouses.
-   **Mirror Tables**: The `init-staging-tables.sql` script creates tables identical to the Fact tables but removes Foreign Key constraints. This allows high-speed bulk inserts (`COPY` or batch inserts) without referential integrity checks slowing down the process.
-   **Validation Log**: A persistent `validation_log` table allows full auditability of failed batches.
-   **Quality Columns**:
    -   `_is_duplicate`: Boolean flag checking against existing production keys.
    -   `_null_count`: Integer tracking critical missing values.
-   **Logic**: The `Loader` class inserts into Staging first. Then, SQL-based validation procedures run. Only records passing the "Quality Gate" are moved to `DWH`.

#### 3. API Integration Engine
-   **Heterogeneous Sources**: Unified interface for 3 distinct APIs with different auth mechanisms (API Key, JWT, Free-tier).
-   **AEMET 2-Step Extraction**: Implemented in `Extractor._aemet_request`, enabling the complex async flow (Request URL $\rightarrow$ Poll Status $\rightarrow$ Download Data).
-   **Resilience**: Custom `BaseETLLogger` and retry logic with exponential backoff (`src.utils.http_utils`) ensures transient network failures don't crash the pipeline.

#### 4. Orchestration (Apache Airflow)
-   **Master DAG**: Controls the end-to-end flow.
-   **Modular Pipelines**: `extraction_pipeline`, `transformation_pipeline`, `loading_pipeline` are separate, reusable DAGs triggered via `TriggerDagRunOperator`.
-   **Dynamic Task Mapping**: Automatically generates tasks based on the number of cities/stations, ensuring parallel execution without hardcoding task IDs.

---

## 🛠️ Stack Tecnológico

-   **Apache Airflow**: Orquestación y programación del pipeline ETL
-   **MinIO**: Data Lake (S3 compatible) para capas Bronze y Silver
-   **PostgreSQL**: Data Warehouse para almacenar datos finales (Gold Layer) y Staging
-   **Docker Compose**: Infraestructura containerizada y orquestación de servicios
-   **Metabase**: Visualización y dashboards para usuarios finales
-   **pgAdmin**: Interfaz gráfica para gestión técnica de PostgreSQL
-   **OpenWeatherMap / Open-Meteo / AEMET**: Fuentes de datos meteorológicos heterogéneas

---

## ⚡ Key Technical Features

### Orchestration Details
The pipeline leverages Airflow 2.9+ features for maximum efficiency:
-   **Dynamic Task Mapping**: We don't hardcode "Madrid" or "Barcelona". The DAG reads the list of cities and dynamically spawns parallel extraction tasks.
-   **XCom for Metadata**: We follow best practices by NOT passing heavy dataframes through XCom. We pass `s3_paths` and `record_counts`. The data stays in MinIO.

### Data Quality & Validation
We don't just move data; we ensure it's correct.
-   **Great Expectations**: Integrated framework (~80% coverage) defining expectations like `expect_column_values_to_be_between(column="temperature", min_value=-50, max_value=60)`.
-   **Strict vs Warn Modes**: The `Loader` class supports a `strict_validation` flag. In Production, we can set this to `False` to log warnings instead of halting the pipeline, while Staging keeps it `True`.

### Configuration & Security
-   **Secrets Management**: A rigorous hierarchy implemented in `SecretsManager` (Adapter Pattern).
    1.  **Airflow Connections**: Primary source (Production).
    2.  **Environment Variables**: Fallback (Docker/Local).
    3.  **Config Defaults**: Last resort constants.
-   **Secure Connections**: All database and API interactions use parametrized queries/secure headers. No credentials are ever hardcoded or logged.

### Observability
-   **Metabase**: Connected directly to the `Gold` layer for building dashboards (Weather Forecasts, Historical Trends).
-   **Structured Logging**: Custom JSON-formatted logs with emoji indicators (🚀, ✅, ❌) for instant visual parsing in CloudWatch or Airflow Logs.

---

## 🌟 Extra Features

### Configuration System (Adapter Pattern)
The `SecretsManager` acts as an Adapter, providing a unified `get_credentials()` interface regardless of whether the app is running in Airflow (using `BaseHook`) or locally (using `.env`).
-   **Lazy Loading**: Connections are only established when requested.
-   **Fallbacks**: Graceful degradation from Production configuration to Dev configuration.

### Error Handling
-   **Granular Control**: Failures are handled at the `City` or `Station` level. One failed city does not fail the entire pipeline.
-   **Robust Fallbacks**: Default values for non-critical fields.
-   **Retry Logic**: `urllib3` retry strategies baked into the `requests.Session` singleton in `http_utils.py`.

### Security Implementation
-   **SQL Injection Prevention**: Strict usage of SQLAlchemy/Psycopg2 parameter binding.
-   **Network Isolation**: Docker networks separate `db` traffic from `web` traffic.

### Code Quality Tools
The project maintains a spotless codebase using a pre-commit pipeline:
-   **Black**: Uncompromising formatting.
-   **Ruff**: Lightning-fast linting.
-   **Mypy**: Static type checking (Strict mode enabled).
-   **Bandit**: Security analysis.

### Makefile
Automates the development lifecycle:
```bash
make up       # Start infrastructure
make down     # Stop infrastructure
make test     # Run full test suite
make lint     # Run all linters
```

---

## 🧩 Design Patterns

This project is a showcase of Software Engineering logic applied to Data Engineering.

### 1. Singleton Pattern (`MinIOClient`)
We ensure only one connection to the Data Lake is open per process, using a thread-safe implementation.

*File: `src/utils/minio_client.py`*
```python
_client_instance: Optional["MinIOClient"] = None
_client_lock: threading.Lock = threading.Lock()

def get_minio_client() -> "MinIOClient":
    global _client_instance
    if _client_instance is not None:
        return _client_instance
    with _client_lock:
        if _client_instance is None:
            _client_instance = MinIOClient()
        return _client_instance
```

### 2. Mixin Pattern (`BaseETLLogger`)
Instead of instantiating a logger in every class, we use a Mixin that provides standardized logging capabilities to any inheriting class.

*File: `src/utils/etl_logger.py`*
```python
class BaseETLLogger:
    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    def log_start(self, msg: str) -> None:
        self.logger.info(f"🚀 START: {msg}")
```

### 3. Strategy / Facade Pattern (`Extractor`)
The `Extractor` class acts as a Facade that encapsulates the complexities of different attributes. While it currently implements the extraction strategies internally, it provides a unified interface (`extract_openweather`, `extract_aemet`) that hides the diverse authentication and pagination logic of the underlying APIs.

*File: `src/extractor.py`*
```python
class Extractor(BaseETLLogger):
    def extract_openweather(self, **context) -> int:
        # Strategy for simple API key extraction using Requests
        ...
    
    def extract_aemet_daily_climatology(self, ...) -> int:
        # Strategy for complex 2-step async extraction with JWT
        ...
```

### 4. Adapter Pattern (`SecretsManager`)
The `SecretsManager` adapts the Airflow Connection interface and the OS Environment functionality into a single domain-specific interface.

*File: `src/config/secrets_manager.py`*
```python
class SecretsManager:
    def get_postgres_credentials(self) -> PostgresCredentials:
        if self._use_airflow:
            # Adapt Airflow Connection object
            conn = _get_airflow_connection(self.CONN_POSTGRES)
            if conn:
                return PostgresCredentials(...)
        
        # Adapt Environment Variables
        return PostgresCredentials(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            ...
        )
```

### 5. Template Method Pattern (`Transformer._transform_generic`)
We defined a skeleton of an algorithm in an operation, deferring some steps to client subclasses (or in this case, parameters). This allows us to reuse the same transformation logic for Hourly, Daily, and Pollen data.

*File: `src/transformer.py`*
```python
    def _transform_generic(self, context, upstream_keys, data_key, ...):
        # 1. Get Data (Generic)
        bronze_objects = self._get_upstream_data(...)
        
        # 2. Transform (Specific logic via parameters)
        target_data = data.get(data_key)
        
        # 3. Load (Generic)
        self.minio_client.upload_parquet(...)
```

---

## 📂 Project Structure

```
WEATHER_PIPELINE_ETL/
├── dags/                       # Airflow DAG Definitions
│   ├── extraction_pipeline.py
│   ├── transformation_pipeline.py
│   └── ...
├── docker/                     # Container Configurations
│   ├── airflow/
│   ├── postgres/               # SQL Init Scripts (Staging/DWH)
│   └── metabase/
├── src/                        # Application Source Code
│   ├── config/                 # Configuration & Adapters
│   │   ├── secrets_manager.py
│   │   └── ...
│   ├── utils/                  # Shared Utilities
│   │   ├── etl_logger.py       # Mixin
│   │   └── minio_client.py     # Singleton
│   ├── extractor.py            # Extraction Logic
│   ├── transformer.py          # Transformation Logic
│   └── loader.py               # Loading Logic
├── tests/                      # Pytest Suite
├── .env.example                # Template for Environment Variables
├── docker-compose.yml          # Services Orchestration
└── pyproject.toml              # Python Dependencies & Tool Config
```

---

## 🚀 Development & CI/CD

### Testing Strategy (`Pytest`)
-   **Unit Tests**: Isolated tests for transformation logic using mocked inputs.
-   **Integration Tests**: Validates interactions with MinIO and Postgres (using Service Containers).
-   **Mocking**: Extensive use of `requests_mock` to simulate API responses (including failures) without hitting real endpoints.
-   **E2E Tests**: Full pipeline runs on a subset of data.

### CI/CD Pipelines (GitHub Actions)
Four specialized workflows ensure stability:
1.  **Linting & Quality**: Runs Black, Isort, and Ruff. Blocks PRs on violation.
2.  **Unit & Integration Tests**: Runs `pytest` with coverage reports.
3.  **Security Audit**: Runs Bandit and checks for commited secrets.
4.  **Deployment**: (On merge to main) Builds Docker images and pushes to registry.

---

## 💻 Setup & Usage Guide

### Prerequisites
-   Docker & Docker Compose (v2.0+)
-   Python 3.11+
-   Make (optional, for shortcuts)

### 1. Installation
Clone the repository and secure your environment:
```bash
git clone https://github.com/AlvaroM99/WEATHER_PIPELINE_ETL.git
cd WEATHER_PIPELINE_ETL
cp .env.example .env
# Edit .env with your API Keys
```

### 2. Launch Infrastructure
Start the entire stack (Airflow, DB, MinIO, Metabase):
```bash
make up
# Or: docker-compose up -d --build
```
*Wait for Health Checks to pass (approx. 2-X minutes for first launch).*

### 3. Access Interfaces
| Service | URL | Credentials |
|:---|:---|:---|
| **Airflow** | `http://localhost:8080` | `airflow` / `airflow` |
| **MinIO** | `http://localhost:9001` | `minioadmin` / `minioadmin` |
| **Metabase** | `http://localhost:3000` | Setup Wizard |

---

<p align="center">
  <sub>Built with ❤️ by Alvaro M using Python, Airflow, and lots of Coffee ☕</sub>
</p>

---

## 📖 Referencias y Documentación Oficial

A continuación se listan las tecnologías principales utilizadas en este proyecto junto con enlaces a su documentación oficial para referencia técnica.

| Tecnología | Categoría | Descripción | Documentación Oficial |
| :--- | :--- | :--- | :--- |
| **Apache Airflow** | Orquestación | Plataforma para crear, programar y monitorizar flujos de trabajo programáticos. | [Documentación](https://airflow.apache.org/docs/) |
| **MinIO** | Data Lake | Almacenamiento de objetos de alto rendimiento compatible con AWS S3. | [Documentación](https://min.io/docs/minio/linux/index.html) |
| **PostgreSQL** | Base de Datos | Potente sistema de base de datos relacional de código abierto. | [Documentación](https://www.postgresql.org/docs/) |
| **Docker Compose** | Infraestructura | Herramienta para definir y ejecutar aplicaciones Docker de múltiples contenedores. | [Documentación](https://docs.docker.com/compose/) |
| **Metabase** | BI & Visualización | Herramienta simple y potente para análisis de datos y gráficos. | [Documentación](https://www.metabase.com/docs/latest/) |
| **Great Expectations** | Calidad de Datos | Framework líder para validar, documentar y perfilar datos. | [Documentación](https://docs.greatexpectations.io/docs/) |
| **Pandas** | Procesamiento | Biblioteca rápida y potente para análisis y manipulación de datos en Python. | [Documentación](https://pandas.pydata.org/docs/) |
| **Ruff** | Code Quality | Linter y formateador de Python extremadamente rápido. | [Documentación](https://docs.astral.sh/ruff/) |
| **Mypy** | Code Quality | Comprobador de tipos estáticos para Python. | [Documentación](https://mypy.readthedocs.io/en/stable/) |
