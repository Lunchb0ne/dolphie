import pytest
from unittest.mock import MagicMock, patch
from dolphie.Modules.PostgreSQL import PostgreSQLDatabase
from dolphie.Modules.ArgumentParser import ArgumentParser
from dolphie.DataTypes import ConnectionSource


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