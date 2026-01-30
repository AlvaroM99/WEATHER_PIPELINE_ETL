# Makefile for Weather Pipeline ETL
# Run commands with: make <target>

.PHONY: help install install-dev install-test test test-unit test-integration lint format typecheck security clean coverage pre-commit docker-up docker-down

# Default target
help:
	@echo "Weather Pipeline ETL - Available Commands"
	@echo "=========================================="
	@echo ""
	@echo "Setup:"
	@echo "  install        Install production dependencies (requirements/base.txt)"
	@echo "  install-test   Install testing dependencies (requirements/test.txt)"
	@echo "  install-dev    Install all development dependencies (requirements/dev.txt)"
	@echo "  pre-commit     Install and setup pre-commit hooks"
	@echo ""
	@echo "Testing:"
	@echo "  test           Run all tests with coverage"
	@echo "  test-unit      Run only unit tests"
	@echo "  test-integration Run integration tests"
	@echo "  coverage       Generate HTML coverage report"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint           Run all linters (flake8, mypy)"
	@echo "  format         Format code with black and isort"
	@echo "  format-check   Check formatting without changes"
	@echo "  typecheck      Run mypy type checker"
	@echo "  security       Run security scans (bandit, safety)"
	@echo ""
	@echo "Docker:"
	@echo "  docker-up      Start all services"
	@echo "  docker-down    Stop all services"
	@echo "  docker-logs    View service logs"
	@echo ""
	@echo "Maintenance:"
	@echo "  clean          Remove cache and build files"
	@echo "  update-deps    Update dependencies"

# ============================================================================
# SETUP
# ============================================================================

install:
	pip install --upgrade pip
	pip install -r requirements/base.txt

install-dev:
	pip install --upgrade pip
	pip install -r requirements/dev.txt

install-test:
	pip install --upgrade pip
	pip install -r requirements/test.txt

pre-commit:
	pip install pre-commit
	pre-commit install
	pre-commit install --hook-type pre-push

# ============================================================================
# TESTING
# ============================================================================

test:
	pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html

test-unit:
	pytest tests/unit/ -v --cov=src --cov-report=term-missing -m "not database and not slow"

test-integration:
	pytest tests/integration/ -v -m "not database"

test-fast:
	pytest tests/ -v -x --no-cov -m "not slow and not database"

coverage:
	pytest tests/ --cov=src --cov-report=html:tests/htmlcov --cov-report=xml
	@echo "Coverage report generated in tests/htmlcov/"
	@echo "Open tests/htmlcov/index.html in your browser"

# ============================================================================
# CODE QUALITY
# ============================================================================

lint:
	@echo "Running ruff..."
	ruff check src/ tests/
	@echo "Running mypy..."
	mypy src/ --ignore-missing-imports

format:
	@echo "Formatting with black..."
	black src/ tests/
	@echo "Sorting imports with isort..."
	isort src/ tests/

format-check:
	@echo "Checking black formatting..."
	black --check --diff src/ tests/
	@echo "Checking isort..."
	isort --check-only --diff src/ tests/

typecheck:
	mypy src/ --ignore-missing-imports --show-error-codes

security:
	@echo "Running bandit security scan..."
	bandit -r src/ -ll --skip B101,B311
	@echo "Checking dependencies for vulnerabilities..."
	safety check || pip-audit

# ============================================================================
# DOCKER
# ============================================================================

docker-up:
	docker-compose up -d
	@echo "Services started. Access:"
	@echo "  - Airflow: http://localhost:8080"
	@echo "  - pgAdmin: http://localhost:5050"
	@echo "  - Metabase: http://localhost:3000"
	@echo "  - MinIO: http://localhost:9001"

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-restart:
	docker-compose restart

# ============================================================================
# MAINTENANCE
# ============================================================================

clean:
	@echo "Cleaning cache and build files..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf tests/.coverage tests/htmlcov/ coverage.xml 2>/dev/null || true
	@echo "Clean complete!"

update-deps:
	pip install --upgrade pip
	pip list --outdated --format=columns

# ============================================================================
# CI/CD LOCAL SIMULATION
# ============================================================================

ci-local:
	@echo "Simulating CI pipeline locally..."
	@echo ""
	@echo "Step 1: Format check"
	$(MAKE) format-check
	@echo ""
	@echo "Step 2: Linting"
	$(MAKE) lint
	@echo ""
	@echo "Step 3: Tests"
	$(MAKE) test
	@echo ""
	@echo "CI simulation complete!"

# ============================================================================
# DATA QUALITY
# ============================================================================

validate-quality:
	@echo "Running data quality framework validation..."
	python -c "from src.data_quality.validators import DataQualityValidator; print('Validators OK')"
	python -c "from src.data_quality.expectations import WEATHER_OBSERVATION_CONFIG; print('Expectations OK')"
	python -c "from src.data_quality.metrics import DataQualityMetrics; print('Metrics OK')"
	@echo "Data quality framework validated!"
