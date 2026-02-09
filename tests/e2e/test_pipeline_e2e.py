"""
End-to-end tests with real PostgreSQL using testcontainers.

These tests spin up real PostgreSQL containers to test
idempotency (ON CONFLICT DO NOTHING) and SQL injection safety
against real infrastructure.

Requires Docker to be running. Skipped automatically if testcontainers
is not installed.
"""

import psycopg2
import pytest

tc = pytest.importorskip("testcontainers")
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="module")
def postgres_container():
    """Start a real PostgreSQL 13 container for the test module."""
    with PostgresContainer("postgres:13") as postgres:
        yield postgres


@pytest.fixture
def pg_connection(postgres_container):
    """Get a real psycopg2 connection and create the DWH schema."""
    conn = psycopg2.connect(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        user=postgres_container.username,
        password=postgres_container.password,
        database=postgres_container.dbname,
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dwh")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dwh.dim_city (
                city_id SERIAL PRIMARY KEY,
                city_code VARCHAR(10) UNIQUE,
                city_name VARCHAR(100),
                latitude FLOAT,
                longitude FLOAT,
                country_code VARCHAR(5),
                is_coastal BOOLEAN
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dwh.fct_weather_observation (
                id SERIAL PRIMARY KEY,
                city_id INTEGER REFERENCES dwh.dim_city(city_id),
                date_id INTEGER,
                observation_timestamp TIMESTAMP,
                temperature FLOAT,
                UNIQUE (city_id, date_id, observation_timestamp)
            )
        """)
        # Seed dim_city
        cur.execute("""
            INSERT INTO dwh.dim_city (city_code, city_name, latitude, longitude, country_code, is_coastal)
            VALUES ('28079', 'Madrid', 40.4168, -3.7038, 'ES', false)
            ON CONFLICT (city_code) DO NOTHING
        """)

    conn.autocommit = False
    yield conn
    conn.close()


@pytest.mark.e2e
@pytest.mark.slow
def test_dim_city_insert_idempotent(pg_connection):
    """Test ON CONFLICT DO NOTHING with real PostgreSQL - insert same city twice."""
    conn = pg_connection
    with conn.cursor() as cur:
        # Insert same city twice
        cur.execute("""
            INSERT INTO dwh.dim_city (city_code, city_name, latitude, longitude, country_code, is_coastal)
            VALUES ('28079', 'Madrid', 40.4168, -3.7038, 'ES', false)
            ON CONFLICT (city_code) DO NOTHING
        """)
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM dwh.dim_city WHERE city_code = '28079'")
        count = cur.fetchone()[0]
        assert count == 1  # Still only one row


@pytest.mark.e2e
@pytest.mark.slow
def test_fact_observation_on_conflict_real_db(pg_connection):
    """Test that duplicate fact records are silently ignored in real PostgreSQL."""
    conn = pg_connection
    with conn.cursor() as cur:
        # Get city_id for Madrid
        cur.execute("SELECT city_id FROM dwh.dim_city WHERE city_code = '28079'")
        city_id = cur.fetchone()[0]

        # Insert a fact record
        cur.execute("""
            INSERT INTO dwh.fct_weather_observation (city_id, date_id, observation_timestamp, temperature)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (city_id, date_id, observation_timestamp) DO NOTHING
        """, (city_id, 20260129, "2026-01-29 12:00:00", 15.5))
        first_rowcount = cur.rowcount

        # Insert the same record again
        cur.execute("""
            INSERT INTO dwh.fct_weather_observation (city_id, date_id, observation_timestamp, temperature)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (city_id, date_id, observation_timestamp) DO NOTHING
        """, (city_id, 20260129, "2026-01-29 12:00:00", 15.5))
        second_rowcount = cur.rowcount

        conn.commit()

        assert first_rowcount == 1
        assert second_rowcount == 0  # Duplicate silently ignored

        cur.execute("SELECT COUNT(*) FROM dwh.fct_weather_observation")
        assert cur.fetchone()[0] == 1


@pytest.mark.e2e
@pytest.mark.slow
def test_sql_injection_safe_with_real_db(pg_connection):
    """Test that parameterized queries prevent SQL injection in real DB."""
    conn = pg_connection
    malicious_name = "'; DROP TABLE dwh.dim_city; --"

    with conn.cursor() as cur:
        # Use parameterized query (same pattern as production code)
        cur.execute(
            """
            INSERT INTO dwh.dim_city (city_code, city_name, latitude, longitude, country_code, is_coastal)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (city_code) DO NOTHING
            """,
            ("EVIL1", malicious_name, 0.0, 0.0, "XX", False),
        )
        conn.commit()

        # Table should still exist and be queryable
        cur.execute("SELECT COUNT(*) FROM dwh.dim_city")
        count = cur.fetchone()[0]
        assert count >= 1  # Table not dropped

        # Verify the malicious string was stored as data, not executed as SQL
        cur.execute("SELECT city_name FROM dwh.dim_city WHERE city_code = 'EVIL1'")
        stored_name = cur.fetchone()[0]
        assert stored_name == malicious_name

        # Clean up
        cur.execute("DELETE FROM dwh.dim_city WHERE city_code = 'EVIL1'")
        conn.commit()


@pytest.mark.e2e
@pytest.mark.slow
def test_read_committed_isolation(pg_connection):
    """Test that READ COMMITTED isolation works correctly in real PostgreSQL."""
    conn = pg_connection

    # PostgreSQL default isolation level is READ COMMITTED
    with conn.cursor() as cur:
        cur.execute("SHOW transaction_isolation")
        isolation = cur.fetchone()[0]
        assert isolation == "read committed"

        # Verify we can read our own uncommitted writes within same transaction
        cur.execute("""
            INSERT INTO dwh.dim_city (city_code, city_name, latitude, longitude, country_code, is_coastal)
            VALUES ('TEMP1', 'TempCity', 0.0, 0.0, 'XX', false)
            ON CONFLICT (city_code) DO NOTHING
        """)
        cur.execute("SELECT COUNT(*) FROM dwh.dim_city WHERE city_code = 'TEMP1'")
        assert cur.fetchone()[0] == 1

        conn.rollback()

        # After rollback, the row should not exist
        cur.execute("SELECT COUNT(*) FROM dwh.dim_city WHERE city_code = 'TEMP1'")
        assert cur.fetchone()[0] == 0
