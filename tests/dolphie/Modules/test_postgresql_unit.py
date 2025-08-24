"""Comprehensive unit tests for PostgreSQL functionality."""

import pytest
import psycopg2
from unittest.mock import MagicMock, patch, call

from dolphie.Modules.PostgreSQL import PostgreSQLDatabase
from dolphie.Modules.ManualException import ManualException
from dolphie.DataTypes import ConnectionSource


class TestPostgreSQLDatabase:
    """Test PostgreSQL database class functionality."""

    def test_init_basic_parameters(self, mock_app):
        """Test basic initialization of PostgreSQL database."""
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
        assert db.connection_id is None
        assert not db.has_connected
        assert not db.is_running_query
        assert db.max_reconnect_attempts == 3

    def test_init_daemon_mode(self, mock_app):
        """Test initialization in daemon mode."""
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

    @patch('psycopg2.connect')
    def test_connect_success(self, mock_connect, mock_app):
        """Test successful connection to PostgreSQL."""
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
        
        # Verify connection parameters
        mock_connect.assert_called_once()
        call_args = mock_connect.call_args[1]
        assert call_args["host"] == "localhost"
        assert call_args["user"] == "testuser"
        assert call_args["password"] == "testpass"
        assert call_args["port"] == 5432
        assert call_args["application_name"] == "Dolphie"
        assert call_args["connect_timeout"] == 5
        
        # Verify connection setup
        assert mock_conn.autocommit is True
        mock_conn.cursor.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT pg_backend_pid()")
        
        assert db.connection == mock_conn
        assert db.cursor == mock_cursor
        assert db.connection_id == 12345
        assert db.has_connected is True

    @patch('psycopg2.connect')
    def test_connect_with_database(self, mock_connect, mock_app):
        """Test connection with specific database."""
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
            auto_connect=False,
            database="mydb"
        )
        
        db.connect()
        
        call_args = mock_connect.call_args[1]
        assert call_args["database"] == "mydb"

    @patch('psycopg2.connect')
    def test_connect_with_ssl(self, mock_connect, mock_app):
        """Test connection with SSL configuration."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        
        ssl_config = {
            "ssl_ca": "/path/to/ca.pem",
            "ssl_cert": "/path/to/cert.pem",
            "ssl_key": "/path/to/key.pem"
        }
        
        db = PostgreSQLDatabase(
            app=mock_app,
            host="localhost",
            user="testuser",
            password="testpass",
            socket=None,
            port=5432,
            ssl=ssl_config,
            auto_connect=False
        )
        
        db.connect()
        
        call_args = mock_connect.call_args[1]
        assert call_args["sslrootcert"] == "/path/to/ca.pem"
        assert call_args["sslcert"] == "/path/to/cert.pem"
        assert call_args["sslkey"] == "/path/to/key.pem"
        assert call_args["sslmode"] == "require"

    @patch('psycopg2.connect')
    def test_connect_with_socket(self, mock_connect, mock_app):
        """Test connection using Unix socket."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        
        db = PostgreSQLDatabase(
            app=mock_app,
            host=None,
            user="testuser",
            password="testpass",
            socket="/var/run/postgresql",
            port=5432,
            ssl=None,
            auto_connect=False
        )
        
        db.connect()
        
        call_args = mock_connect.call_args[1]
        assert call_args["host"] == "/var/run/postgresql"

    @patch('psycopg2.connect')
    def test_connect_without_saving_connection_id(self, mock_connect, mock_app):
        """Test connection without saving connection ID."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        db = PostgreSQLDatabase(
            app=mock_app,
            host="localhost",
            user="testuser",
            password="testpass",
            socket=None,
            port=5432,
            ssl=None,
            save_connection_id=False,
            auto_connect=False
        )
        
        db.connect()
        
        # Connection ID query should not be executed
        mock_cursor.execute.assert_not_called()
        assert db.connection_id is None

    @patch('psycopg2.connect')
    def test_connect_failure(self, mock_connect, mock_app):
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
    def test_execute_query_success(self, mock_connect, mock_app):
        """Test successful query execution."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        mock_cursor.rowcount = 5
        mock_conn.closed = 0  # Not closed
        
        # Mock the is_connected method's connection test
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
        
        result = db.execute("SELECT * FROM test_table")
        
        # Verify query was prefixed and executed
        mock_cursor.execute.assert_called_with("/* Dolphie */ SELECT * FROM test_table", None)
        assert result == 5
        assert not db.is_running_query

    @patch('psycopg2.connect')
    def test_execute_with_values(self, mock_connect, mock_app):
        """Test query execution with parameters."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        mock_cursor.rowcount = 1
        mock_conn.closed = 0  # Not closed
        
        # Mock the is_connected method's connection test
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
        
        result = db.execute("SELECT * FROM test_table WHERE id = %s", (123,))
        
        mock_cursor.execute.assert_called_with("/* Dolphie */ SELECT * FROM test_table WHERE id = %s", (123,))
        assert result == 1

    def test_execute_when_not_connected(self, mock_app):
        """Test execute when not connected."""
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
        
        result = db.execute("SELECT 1")
        assert result is None

    @patch('psycopg2.connect')
    def test_execute_concurrent_query(self, mock_connect, mock_app):
        """Test execute when another query is running."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        mock_conn.closed = 0
        
        # Mock the is_connected method
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
        db.is_running_query = True  # Simulate running query
        
        result = db.execute("SELECT 1")
        assert result is None

    @patch('psycopg2.connect')
    def test_execute_connection_error_retry(self, mock_connect, mock_app):
        """Test execute with connection error and retry."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        
        # First call succeeds, second call fails with connection error
        mock_cursor.execute.side_effect = [None, psycopg2.OperationalError("connection lost"), None]
        mock_cursor.rowcount = 1
        mock_conn.closed = 0
        
        # Mock the is_connected method
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
        mock_cursor.execute.side_effect = [psycopg2.OperationalError("connection lost"), None]
        mock_cursor.rowcount = 1
        
        with patch.object(db, 'connect') as mock_reconnect:
            result = db.execute("SELECT 1")
            
        # Should attempt to reconnect
        mock_reconnect.assert_called_once()

    @patch('psycopg2.connect')
    def test_execute_ignore_error(self, mock_connect, mock_app):
        """Test execute with ignore_error flag."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        mock_cursor.execute.side_effect = psycopg2.ProgrammingError("syntax error")
        mock_conn.closed = 0
        
        # Mock the is_connected method
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
        mock_cursor.execute.side_effect = psycopg2.ProgrammingError("syntax error")
        
        # Should not raise exception with ignore_error=True
        result = db.execute("INVALID SQL", ignore_error=True)
        assert result is None

    def test_is_connection_error(self, mock_app):
        """Test detection of connection errors."""
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
        
        # Test various connection error types
        assert db._is_connection_error(psycopg2.OperationalError("connection lost"))
        assert db._is_connection_error(psycopg2.InterfaceError("interface error"))
        assert not db._is_connection_error(psycopg2.ProgrammingError("syntax error"))
        assert not db._is_connection_error(Exception("generic error"))

    @patch('psycopg2.connect')
    def test_fetchall(self, mock_connect, mock_app):
        """Test fetchall method."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        mock_cursor.fetchall.return_value = [{"id": 1}, {"id": 2}]
        
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
        result = db.fetchall()
        
        mock_cursor.fetchall.assert_called_once()
        assert result == [{"id": 1}, {"id": 2}]

    def test_fetchall_no_cursor(self, mock_app):
        """Test fetchall when no cursor exists."""
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
        
        result = db.fetchall()
        assert result == []

    @patch('psycopg2.connect')
    def test_fetchone(self, mock_connect, mock_app):
        """Test fetchone method."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.side_effect = [{"pg_backend_pid": 12345}, {"id": 1}]
        
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
        result = db.fetchone()
        
        assert result == {"id": 1}

    def test_fetchone_no_cursor(self, mock_app):
        """Test fetchone when no cursor exists."""
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
        
        result = db.fetchone()
        assert result == {}

    @patch('psycopg2.connect')
    def test_is_connected_success(self, mock_connect, mock_app):
        """Test is_connected when connection is active."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        mock_conn.closed = 0
        
        # Mock the connection test
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
        assert db.is_connected() is True

    def test_is_connected_not_connected(self, mock_app):
        """Test is_connected when not connected."""
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
        
        assert db.is_connected() is False

    @patch('psycopg2.connect')
    def test_is_connected_connection_closed(self, mock_connect, mock_app):
        """Test is_connected when connection is closed."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        mock_conn.closed = 1  # Connection closed
        
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
        assert db.is_connected() is False

    @patch('psycopg2.connect')
    def test_close(self, mock_connect, mock_app):
        """Test connection close."""
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
        assert db.connection is None
        assert db.cursor is None

    @patch('psycopg2.connect')
    def test_close_with_errors(self, mock_connect, mock_app):
        """Test connection close with errors."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        
        # Make close operations raise exceptions
        mock_cursor.close.side_effect = Exception("cursor close error")
        mock_conn.close.side_effect = Exception("connection close error")
        
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
        # Should not raise exception despite errors
        db.close()
        
        assert db.connection is None
        assert db.cursor is None