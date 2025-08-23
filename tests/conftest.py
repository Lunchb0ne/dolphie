"""Test configuration and fixtures for Dolphie PostgreSQL testing."""

import pytest
import psycopg2
from testcontainers.postgres import PostgresContainer
from unittest.mock import MagicMock

from dolphie.Modules.PostgreSQL import PostgreSQLDatabase
from dolphie.Modules.ArgumentParser import Config


@pytest.fixture(scope="session")
def postgres_container():
    """Provide a PostgreSQL container for integration tests."""
    with PostgresContainer("postgres:15") as postgres:
        # Wait for the container to be ready
        postgres.get_connection_url()
        yield postgres


@pytest.fixture
def postgres_config(postgres_container):
    """Provide a config object for PostgreSQL testing."""
    url = postgres_container.get_connection_url()
    # Parse connection details from URL
    # postgresql://test:test@localhost:port/test
    parts = url.replace("postgresql://", "").split("/")
    host_part = parts[0].split("@")[1]
    host, port = host_part.split(":")
    
    return Config(
        app_version='6.10.2',
        database_type='postgresql',
        host=host,
        port=int(port),
        user="test",
        password="test",
        database="test"
    )


@pytest.fixture
def mock_app():
    """Provide a mock Textual app for testing."""
    app = MagicMock()
    app.notify = MagicMock()
    return app


@pytest.fixture
def mock_postgres_db(mock_app):
    """Provide a mocked PostgreSQL database for unit tests."""
    return PostgreSQLDatabase(
        app=mock_app,
        host="localhost",
        user="test",
        password="test",
        socket=None,
        port=5432,
        ssl=None,
        database="test",
        auto_connect=False
    )


@pytest.fixture
def real_postgres_db(mock_app, postgres_config):
    """Provide a real PostgreSQL database connection for integration tests."""
    db = PostgreSQLDatabase(
        app=mock_app,
        host=postgres_config.host,
        user=postgres_config.user,
        password=postgres_config.password,
        socket=None,
        port=postgres_config.port,
        ssl=None,
        database=postgres_config.database,
        auto_connect=True
    )
    yield db
    db.close()


@pytest.fixture
def sample_postgres_data():
    """Provide sample data for testing PostgreSQL queries."""
    return {
        "processlist": [
            {
                "id": 12345,
                "user": "test_user",
                "db": "test_db",
                "host": "127.0.0.1",
                "application_name": "test_app",
                "state": "active",
                "time": 10,
                "query": "SELECT * FROM test_table",
                "wait_event_type": None,
                "wait_event": None
            }
        ],
        "variables": [
            {"Variable_name": "version", "Value": "PostgreSQL 15.4"},
            {"Variable_name": "port", "Value": "5432"},
            {"Variable_name": "max_connections", "Value": "100"}
        ],
        "database_stats": [
            {
                "database_name": "test_db",
                "connections": 5,
                "transactions_committed": 1000,
                "transactions_rolled_back": 10,
                "blocks_read": 5000,
                "blocks_hit": 45000,
                "tuples_returned": 10000,
                "tuples_fetched": 8000,
                "tuples_inserted": 100,
                "tuples_updated": 50,
                "tuples_deleted": 10,
                "conflicts": 0,
                "temp_files": 5,
                "temp_bytes": 1024000,
                "deadlocks": 0
            }
        ]
    }