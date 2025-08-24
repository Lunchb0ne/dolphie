"""Simplified unit tests for PostgreSQL functionality."""

import pytest
import psycopg2
from unittest.mock import MagicMock, patch

from dolphie.Modules.PostgreSQL import PostgreSQLDatabase
from dolphie.Modules.ManualException import ManualException
from dolphie.DataTypes import ConnectionSource


class TestPostgreSQLDatabase:
    """Test essential PostgreSQL database functionality."""

    def test_basic_initialization(self, mock_app):
        """Test basic PostgreSQL database initialization."""
        db = PostgreSQLDatabase(
            app=mock_app,
            host="localhost",
            user="testuser",
            password="testpass",
            socket=None,
            port=5432,
            ssl=None,
            auto_connect=False
        )
        
        assert db.host == "localhost"
        assert db.user == "testuser"
        assert db.password == "testpass"
        assert db.port == 5432
        assert db.source == ConnectionSource.postgresql
        assert not db.has_connected
        assert not db.is_running_query

    @patch('psycopg2.connect')
    def test_successful_connection(self, mock_connect, mock_app):
        """Test successful database connection."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        mock_conn.closed = 0
        
        # Mock connection test
        mock_test_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_test_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=None)
        
        db = PostgreSQLDatabase(
            app=mock_app,
            host="localhost",
            user="testuser",
            password="testpass",
            socket=None,
            port=5432,
            ssl=None,
            auto_connect=False
        )
        
        db.connect()
        
        assert db.connection is not None
        assert db.cursor is not None
        assert db.connection_id == 12345
        assert db.has_connected is True
        assert db.is_connected() is True

    @patch('psycopg2.connect')
    def test_connection_failure(self, mock_connect, mock_app):
        """Test connection failure handling."""
        mock_connect.side_effect = psycopg2.OperationalError("Connection failed")
        
        db = PostgreSQLDatabase(
            app=mock_app,
            host="localhost",
            user="testuser",
            password="testpass",
            socket=None,
            port=5432,
            ssl=None,
            auto_connect=False
        )
        
        with pytest.raises(ManualException, match="Connection failed"):
            db.connect()

    @patch('psycopg2.connect')
    def test_query_execution(self, mock_connect, mock_app):
        """Test query execution with proper prefixing."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        mock_cursor.rowcount = 1
        mock_conn.closed = 0
        
        # Mock connection test
        mock_test_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_test_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=None)
        
        db = PostgreSQLDatabase(
            app=mock_app,
            host="localhost",
            user="testuser",
            password="testpass",
            socket=None,
            port=5432,
            ssl=None,
            auto_connect=False
        )
        
        db.connect()
        # Reset execute calls after connection
        mock_cursor.execute.reset_mock()
        
        result = db.execute("SELECT 1")
        
        assert result == 1
        # Verify query is prefixed with Dolphie comment
        mock_cursor.execute.assert_called_once_with("/* Dolphie */ SELECT 1", None)

    @patch('psycopg2.connect')
    def test_query_execution_concurrent_prevention(self, mock_connect, mock_app):
        """Test that concurrent queries are prevented."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        mock_conn.closed = 0
        
        # Mock connection test
        mock_test_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_test_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=None)
        
        db = PostgreSQLDatabase(
            app=mock_app,
            host="localhost",
            user="testuser",
            password="testpass",
            socket=None,
            port=5432,
            ssl=None,
            auto_connect=False
        )
        
        db.connect()
        db.is_running_query = True
        
        result = db.execute("SELECT 1")
        
        assert result is None
        mock_app.notify.assert_called_once()

    @patch('psycopg2.connect')
    def test_fetchall_and_fetchone(self, mock_connect, mock_app):
        """Test result fetching methods."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        mock_cursor.fetchall.return_value = [{"col1": "value1"}, {"col1": "value2"}]
        mock_conn.closed = 0
        
        # Mock connection test
        mock_test_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_test_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=None)
        
        db = PostgreSQLDatabase(
            app=mock_app,
            host="localhost",
            user="testuser",
            password="testpass",
            socket=None,
            port=5432,
            ssl=None,
            auto_connect=False
        )
        
        db.connect()
        
        # Test fetchall
        all_results = db.fetchall()
        assert len(all_results) == 2
        assert all_results[0]["col1"] == "value1"
        
        # Test fetchone (will return the same mock data)
        mock_cursor.fetchone.return_value = {"col1": "single_value"}
        one_result = db.fetchone()
        assert one_result["col1"] == "single_value"

    def test_connection_status_when_not_connected(self, mock_app):
        """Test connection status when not connected."""
        db = PostgreSQLDatabase(
            app=mock_app,
            host="localhost",
            user="testuser",
            password="testpass",
            socket=None,
            port=5432,
            ssl=None,
            auto_connect=False
        )
        
        assert not db.is_connected()

    @patch('psycopg2.connect')
    def test_close_connection(self, mock_connect, mock_app):
        """Test closing database connection."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        
        db = PostgreSQLDatabase(
            app=mock_app,
            host="localhost",
            user="testuser",
            password="testpass",
            socket=None,
            port=5432,
            ssl=None,
            auto_connect=False
        )
        
        db.connect()
        db.close()
        
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
        assert db.cursor is None
        assert db.connection is None

    def test_daemon_mode_max_reconnect_attempts(self, mock_app):
        """Test that daemon mode sets high reconnect attempts."""
        db = PostgreSQLDatabase(
            app=mock_app,
            host="localhost",
            user="testuser",
            password="testpass",
            socket=None,
            port=5432,
            ssl=None,
            auto_connect=False,
            daemon_mode=True
        )
        
        assert db.max_reconnect_attempts == 999999999

    def test_normal_mode_max_reconnect_attempts(self, mock_app):
        """Test that normal mode sets reasonable reconnect attempts."""
        db = PostgreSQLDatabase(
            app=mock_app,
            host="localhost",
            user="testuser",
            password="testpass",
            socket=None,
            port=5432,
            ssl=None,
            auto_connect=False,
            daemon_mode=False
        )
        
        assert db.max_reconnect_attempts == 3