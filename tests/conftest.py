"""Test configuration and fixtures for Dolphie testing."""

import pytest
from unittest.mock import MagicMock

from dolphie.Modules.PostgreSQL import PostgreSQLDatabase


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