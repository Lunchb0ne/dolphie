"""
Simplified integration tests for PostgreSQL that don't require Docker.
These tests focus on query syntax validation and basic functionality.
"""

import pytest
from unittest.mock import MagicMock, patch
import psycopg2

from dolphie.Modules.PostgreSQL import PostgreSQLDatabase
from dolphie.Modules.Queries import PostgreSQLQueries
from dolphie.DataTypes import ConnectionSource


class TestPostgreSQLQueriesSyntax:
    """Test PostgreSQL query syntax without requiring a real database."""

    def test_all_queries_are_strings(self):
        """Test that all PostgreSQL queries are valid strings."""
        queries = [
            ("variables", PostgreSQLQueries.variables),
            ("processlist", PostgreSQLQueries.processlist),
            ("database_stats", PostgreSQLQueries.database_stats),
            ("connection_stats", PostgreSQLQueries.connection_stats),
            ("replication_status", PostgreSQLQueries.replication_status),
            ("table_stats", PostgreSQLQueries.table_stats),
            ("index_stats", PostgreSQLQueries.index_stats),
            ("locks", PostgreSQLQueries.locks),
            ("slow_queries", PostgreSQLQueries.slow_queries),
            ("version", PostgreSQLQueries.version),
            ("kill_query", PostgreSQLQueries.kill_query),
            ("kill_connection", PostgreSQLQueries.kill_connection),
        ]
        
        for name, query in queries:
            assert isinstance(query, str), f"Query '{name}' is not a string"
            assert len(query.strip()) > 0, f"Query '{name}' is empty"
            assert "SELECT" in query.upper() or "%" in query, f"Query '{name}' doesn't appear to be a SQL query"

    def test_query_syntax_structure(self):
        """Test that queries have expected SQL structure."""
        # Test SELECT queries that should have FROM clauses
        select_queries = [
            ("variables", PostgreSQLQueries.variables),
            ("processlist", PostgreSQLQueries.processlist),
            ("database_stats", PostgreSQLQueries.database_stats),
        ]
        
        for name, query in select_queries:
            assert query.strip().upper().startswith("SELECT"), f"Query '{name}' should start with SELECT"
            assert "FROM" in query.upper(), f"Query '{name}' should contain FROM clause"
        
        # Test simple queries without FROM clause
        simple_queries = [
            ("version", PostgreSQLQueries.version),
        ]
        
        for name, query in simple_queries:
            assert query.strip().upper().startswith("SELECT"), f"Query '{name}' should start with SELECT"

    def test_postgresql_specific_syntax(self):
        """Test that queries use PostgreSQL-specific syntax."""
        # Check for PostgreSQL-specific tables/views
        assert "pg_settings" in PostgreSQLQueries.variables
        assert "pg_stat_activity" in PostgreSQLQueries.processlist
        assert "pg_stat_database" in PostgreSQLQueries.database_stats
        assert "pg_stat_replication" in PostgreSQLQueries.replication_status
        assert "pg_stat_user_tables" in PostgreSQLQueries.table_stats
        assert "pg_stat_user_indexes" in PostgreSQLQueries.index_stats
        assert "pg_locks" in PostgreSQLQueries.locks
        assert "pg_stat_statements" in PostgreSQLQueries.slow_queries

    def test_kill_query_syntax(self):
        """Test kill query syntax."""
        assert "pg_cancel_backend" in PostgreSQLQueries.kill_query
        assert "pg_terminate_backend" in PostgreSQLQueries.kill_connection
        assert "%s" in PostgreSQLQueries.kill_query
        assert "%s" in PostgreSQLQueries.kill_connection

    def test_query_parameter_placeholders(self):
        """Test that parameterized queries use correct placeholders."""
        # PostgreSQL uses %s for parameters
        parameterized_queries = [
            PostgreSQLQueries.kill_query,
            PostgreSQLQueries.kill_connection,
        ]
        
        for query in parameterized_queries:
            if "%" in query:
                assert "%s" in query, f"Query should use %s for parameters: {query}"


class TestPostgreSQLIntegrationMocked:
    """Integration tests using mocked PostgreSQL connections."""

    @patch('psycopg2.connect')
    def test_query_execution_flow(self, mock_connect, mock_app):
        """Test the complete query execution flow."""
        # Setup mocks
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_cursor.fetchone.return_value = {"pg_backend_pid": 12345}
        mock_conn.closed = 0
        
        # Mock connection test for is_connected
        mock_test_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_test_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=None)
        
        # Create database instance
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
        
        # Test version query
        mock_cursor.fetchone.return_value = {"version": "PostgreSQL 15.4"}
        mock_cursor.execute.reset_mock()
        
        db.execute(PostgreSQLQueries.version)
        result = db.fetchone()
        
        # Verify query was prefixed
        mock_cursor.execute.assert_called_with("/* Dolphie */ " + PostgreSQLQueries.version, None)
        assert result["version"] == "PostgreSQL 15.4"

    @patch('psycopg2.connect')
    def test_variables_query_execution(self, mock_connect, mock_app):
        """Test variables query execution."""
        # Setup mocks
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
        
        # Mock variables data
        mock_cursor.fetchall.return_value = [
            {"Variable_name": "version", "Value": "PostgreSQL 15.4"},
            {"Variable_name": "port", "Value": "5432"},
            {"Variable_name": "max_connections", "Value": "100"}
        ]
        
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
        
        mock_cursor.execute.reset_mock()
        
        db.execute(PostgreSQLQueries.variables)
        results = db.fetchall()
        
        assert len(results) == 3
        assert results[0]["Variable_name"] == "version"
        assert results[1]["Variable_name"] == "port"

    @patch('psycopg2.connect')
    def test_processlist_query_execution(self, mock_connect, mock_app):
        """Test processlist query execution."""
        # Setup mocks
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
        
        # Mock processlist data
        mock_cursor.fetchall.return_value = [
            {
                "id": 12345,
                "user": "testuser",
                "db": "testdb",
                "host": "127.0.0.1",
                "application_name": "test_app",
                "state": "active",
                "time": 10,
                "query": "SELECT 1",
                "wait_event_type": None,
                "wait_event": None
            }
        ]
        
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
        
        mock_cursor.execute.reset_mock()
        
        db.execute(PostgreSQLQueries.processlist)
        results = db.fetchall()
        
        assert len(results) == 1
        assert results[0]["user"] == "testuser"
        assert results[0]["state"] == "active"

    @patch('psycopg2.connect')
    def test_kill_query_execution(self, mock_connect, mock_app):
        """Test kill query execution."""
        # Setup mocks
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
        
        # Mock kill query result
        mock_cursor.fetchone.side_effect = [
            {"pg_backend_pid": 12345},  # Connection ID query
            {"pg_cancel_backend": True}  # Kill query result
        ]
        
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
        
        mock_cursor.execute.reset_mock()
        
        # Test kill query
        db.execute(PostgreSQLQueries.kill_query, (54321,))
        result = db.fetchone()
        
        # Verify the query was called with the correct parameter
        expected_query = "/* Dolphie */ " + PostgreSQLQueries.kill_query
        mock_cursor.execute.assert_called_with(expected_query, (54321,))
        assert result["pg_cancel_backend"] is True

    def test_connection_source_identification(self, mock_app):
        """Test that PostgreSQL connection is properly identified."""
        db = PostgreSQLDatabase(
            app=mock_app,
            host="localhost",
            user="test",
            password="test",
            socket=None,
            port=5432,
            ssl=None,
            auto_connect=False
        )
        
        assert db.source == ConnectionSource.postgresql

    @patch('psycopg2.connect')
    def test_error_handling_during_query(self, mock_connect, mock_app):
        """Test error handling during query execution."""
        # Setup mocks
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
            user="test",
            password="test",
            socket=None,
            port=5432,
            ssl=None,
            auto_connect=True
        )
        
        # Reset to test execute
        mock_cursor.execute.reset_mock()
        
        # Make execute fail with non-connection error
        mock_cursor.execute.side_effect = psycopg2.ProgrammingError("Syntax error")
        
        # Should raise ManualException for non-connection errors
        from dolphie.Modules.ManualException import ManualException
        with pytest.raises(ManualException):
            db.execute("INVALID SQL")

    @patch('psycopg2.connect')
    def test_query_prefixing_behavior(self, mock_connect, mock_app):
        """Test that all queries are properly prefixed."""
        # Setup mocks
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
            user="test",
            password="test",
            socket=None,
            port=5432,
            ssl=None,
            auto_connect=True
        )
        
        mock_cursor.execute.reset_mock()
        
        # Test various queries
        test_queries = [
            "SELECT 1",
            "SELECT version()",
            "SELECT * FROM pg_settings"
        ]
        
        for query in test_queries:
            mock_cursor.execute.reset_mock()
            db.execute(query)
            
            # Verify the query was prefixed
            expected_query = "/* Dolphie */ " + query
            mock_cursor.execute.assert_called_with(expected_query, None)


class TestPostgreSQLArgumentParsing:
    """Test PostgreSQL URI and argument parsing."""

    def test_postgresql_uri_components(self):
        """Test PostgreSQL URI parsing components."""
        import sys
        original_argv = sys.argv
        
        test_cases = [
            {
                "uri": "postgresql://user:pass@localhost:5432/mydb",
                "expected": {
                    "host": "localhost",
                    "port": 5432,
                    "user": "user",
                    "password": "pass",
                    "database_type": "postgresql"
                }
            },
            {
                "uri": "postgresql://user:pass@example.com/mydb",
                "expected": {
                    "host": "example.com",
                    "port": 5432,  # Default port
                    "user": "user",
                    "password": "pass",
                    "database_type": "postgresql"
                }
            }
        ]
        
        try:
            from dolphie.Modules.ArgumentParser import ArgumentParser
            
            for case in test_cases:
                sys.argv = ['dolphie', case["uri"]]
                parser = ArgumentParser('6.10.2')
                
                for key, expected_value in case["expected"].items():
                    actual_value = getattr(parser.config, key)
                    assert actual_value == expected_value, f"URI {case['uri']}: {key} should be {expected_value}, got {actual_value}"
                    
        finally:
            sys.argv = original_argv