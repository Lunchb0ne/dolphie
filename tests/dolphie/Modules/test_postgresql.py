"""Comprehensive tests for PostgreSQL functionality."""

import pytest
import psycopg2
from unittest.mock import MagicMock, patch, call

from dolphie.Modules.PostgreSQL import PostgreSQLDatabase
from dolphie.Modules.ArgumentParser import ArgumentParser
from dolphie.Modules.ManualException import ManualException
from dolphie.DataTypes import ConnectionSource


# Integration Tests
def test_postgresql_uri_parsing():
    """Test that PostgreSQL URIs are parsed correctly"""
    import sys
    original_argv = sys.argv
    try:
        sys.argv = ['dolphie', 'postgresql://user:pass@localhost:5432/db']
        parser = ArgumentParser('6.10.2')
        
        assert parser.config.database_type == "postgresql"
        assert parser.config.port == 5432
        assert parser.config.host == "localhost"
        assert parser.config.user == "user"
        assert parser.config.password == "pass"
    finally:
        sys.argv = original_argv


def test_mysql_uri_still_works():
    """Test that MySQL URIs still work after PostgreSQL support"""
    import sys
    original_argv = sys.argv
    try:
        sys.argv = ['dolphie', 'mysql://user:pass@localhost:3306/db']
        parser = ArgumentParser('6.10.2')
        
        assert parser.config.database_type == "mysql"
        assert parser.config.port == 3306
        assert parser.config.host == "localhost"
    finally:
        sys.argv = original_argv


def test_postgresql_connection_source():
    """Test that PostgreSQL database has correct connection source"""
    mock_app = MagicMock()
    
    with patch('psycopg2.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # Mock the pg_backend_pid() query
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        
        db = PostgreSQLDatabase(
            app=mock_app,
            host="localhost",
            user="test",
            password="test",
            socket=None,
            port=5432,
            ssl=None,
            auto_connect=True
        )
        
        assert db.source == ConnectionSource.postgresql
        assert db.connection_id == 12345


def test_postgresql_queries_import():
    """Test that PostgreSQL queries can be imported and contain expected queries"""
    from dolphie.Modules.Queries import PostgreSQLQueries
    
    # Test that basic queries exist
    assert hasattr(PostgreSQLQueries, 'variables')
    assert hasattr(PostgreSQLQueries, 'processlist')
    assert hasattr(PostgreSQLQueries, 'database_stats')
    assert hasattr(PostgreSQLQueries, 'replication_status')
    
    # Test that queries contain PostgreSQL-specific content
    assert "pg_settings" in PostgreSQLQueries.variables
    assert "pg_stat_activity" in PostgreSQLQueries.processlist
    assert "pg_stat_database" in PostgreSQLQueries.database_stats


def test_default_mysql_behavior():
    """Test that default behavior still defaults to MySQL"""
    import sys
    original_argv = sys.argv
    try:
        sys.argv = ['dolphie', '--host', 'localhost']
        parser = ArgumentParser('6.10.2')
        
        assert parser.config.database_type == "mysql"
        assert parser.config.port == 3306
    finally:
        sys.argv = original_argv


def test_postgresql_default_port():
    """Test that PostgreSQL URI uses correct default port"""
    import sys
    original_argv = sys.argv
    try:
        sys.argv = ['dolphie', 'postgresql://user:pass@localhost/db']
        parser = ArgumentParser('6.10.2')
        
        assert parser.config.database_type == "postgresql"
        assert parser.config.port == 5432
    finally:
        sys.argv = original_argv


def test_postgresql_kill_query():
    """Test that PostgreSQL kill query uses pg_terminate_backend"""
    from dolphie.Modules.ArgumentParser import Config
    from dolphie.Dolphie import Dolphie
    from unittest.mock import MagicMock
    
    config = Config(
        app_version='6.10.2',
        database_type='postgresql',
        host='localhost',
        port=5432,
        user='test',
        password='test'
    )
    
    mock_app = MagicMock()
    dolphie = Dolphie(config, mock_app)
    
    # Mock connection source
    dolphie.connection_source = ConnectionSource.postgresql
    
    kill_query = dolphie.build_kill_query(12345)
    assert kill_query == "SELECT pg_terminate_backend(12345)"


def test_mysql_kill_query_still_works():
    """Test that MySQL kill query still works"""
    from dolphie.Modules.ArgumentParser import Config
    from dolphie.Dolphie import Dolphie
    from unittest.mock import MagicMock
    
    config = Config(
        app_version='6.10.2',
        database_type='mysql',
        host='localhost',
        port=3306,
        user='test',
        password='test'
    )
    
    mock_app = MagicMock()
    dolphie = Dolphie(config, mock_app)
    
    # Mock connection source and global variables
    dolphie.connection_source = ConnectionSource.mysql
    dolphie.global_variables = {}
    
    kill_query = dolphie.build_kill_query(12345)
    assert kill_query == "KILL 12345"


# Unit Tests
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
            'cert': '/path/to/cert.pem',
            'key': '/path/to/key.pem',
            'ca': '/path/to/ca.pem'
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
        assert call_args["sslmode"] == "require"
        assert call_args["sslcert"] == "/path/to/cert.pem"
        assert call_args["sslkey"] == "/path/to/key.pem"
        assert call_args["sslrootcert"] == "/path/to/ca.pem"

    @patch('psycopg2.connect')
    def test_connect_with_socket(self, mock_connect, mock_app):
        """Test connection with Unix socket."""
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
            socket="/var/run/postgresql",
            port=5432,
            ssl=None,
            auto_connect=False
        )
        
        db.connect()
        
        call_args = mock_connect.call_args[1]
        assert call_args["host"] == "/var/run/postgresql"

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
        
        with pytest.raises(ManualException):
            db.connect()

    @patch('psycopg2.connect')
    def test_execute_query_success(self, mock_connect, mock_app):
        """Test successful query execution."""
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
        
        # Test query execution
        test_query = "SELECT * FROM test_table"
        db.execute(test_query)
        
        # Verify query was prefixed and executed
        expected_query = "/* Dolphie */ SELECT * FROM test_table"
        mock_cursor.execute.assert_called_with(expected_query)

    @patch('psycopg2.connect')
    def test_execute_query_with_params(self, mock_connect, mock_app):
        """Test query execution with parameters."""
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
        
        # Test query execution with parameters
        test_query = "SELECT * FROM test_table WHERE id = %s"
        test_params = (123,)
        db.execute(test_query, test_params)
        
        # Verify query was prefixed and executed with parameters
        expected_query = "/* Dolphie */ SELECT * FROM test_table WHERE id = %s"
        mock_cursor.execute.assert_called_with(expected_query, test_params)

    @patch('psycopg2.connect')
    def test_fetchall(self, mock_connect, mock_app):
        """Test fetchall operation."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        
        test_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        mock_cursor.fetchall.return_value = test_data
        
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
        assert result == test_data
        mock_cursor.fetchall.assert_called_once()

    @patch('psycopg2.connect')
    def test_fetchone(self, mock_connect, mock_app):
        """Test fetchone operation."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        
        test_data = {"id": 1, "name": "test"}
        # Configure fetchone to return different values on subsequent calls
        mock_cursor.fetchone.side_effect = [{"pg_backend_pid": 12345}, test_data]
        
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
        assert result == test_data

    @patch('psycopg2.connect')
    def test_close(self, mock_connect, mock_app):
        """Test connection closing."""
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
    def test_reconnect_on_query_failure(self, mock_connect, mock_app):
        """Test automatic reconnection on query failure."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        # First call for initial connection
        # Second call for reconnection after failure
        mock_cursor.fetchone.side_effect = [
            {"pg_backend_pid": 12345},  # Initial connection
            {"pg_backend_pid": 67890}   # Reconnection
        ]
        
        # Simulate query failure then success on reconnect
        mock_cursor.execute.side_effect = [
            psycopg2.OperationalError("Connection lost"),  # First query fails
            None  # Second query (after reconnect) succeeds
        ]
        
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
        
        # This should trigger reconnection
        db.execute("SELECT 1")
        
        # Should have been called twice: initial connection + reconnection
        assert mock_connect.call_count == 2

    @patch('psycopg2.connect')
    def test_max_reconnect_attempts(self, mock_connect, mock_app):
        """Test that reconnection respects max attempts."""
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
        
        # Set max attempts to 2 for testing
        db.max_reconnect_attempts = 2
        
        with pytest.raises(ManualException):
            db.connect()
        
        # Should have tried initial connection + 2 reconnect attempts = 3 total
        assert mock_connect.call_count == 3

    @patch('psycopg2.connect')
    def test_connection_with_app_version(self, mock_connect, mock_app):
        """Test that application name includes app version when available."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        
        # Set app version
        mock_app.app_version = "6.10.2"
        
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
        
        call_args = mock_connect.call_args[1]
        assert call_args["application_name"] == "Dolphie 6.10.2"

    @patch('psycopg2.connect')
    def test_connection_error_handling_details(self, mock_connect, mock_app):
        """Test that connection errors include helpful details."""
        error_msg = "FATAL: password authentication failed for user \"testuser\""
        mock_connect.side_effect = psycopg2.OperationalError(error_msg)
        
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
        
        with pytest.raises(ManualException) as exc_info:
            db.connect()
        
        error_message = str(exc_info.value)
        assert "PostgreSQL connection failed" in error_message
        assert "localhost:5432" in error_message
        assert error_msg in error_message

    @patch('psycopg2.connect')
    def test_query_running_status(self, mock_connect, mock_app):
        """Test that query running status is tracked correctly."""
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
        
        # Initially not running query
        assert not db.is_running_query
        
        # Mock execute to check status during execution
        def mock_execute(query, params=None):
            assert db.is_running_query
        
        mock_cursor.execute.side_effect = mock_execute
        
        db.execute("SELECT 1")
        
        # Should not be running query after completion
        assert not db.is_running_query

    def test_init_with_minimal_parameters(self, mock_app):
        """Test initialization with minimal required parameters."""
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
        
        assert db.database is None
        assert db.ssl is None
        assert db.socket is None
        assert not db.daemon_mode