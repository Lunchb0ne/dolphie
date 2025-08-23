"""Integration tests for PostgreSQL functionality using real database instances."""

import pytest
import psycopg2
from unittest.mock import MagicMock

from dolphie.Modules.PostgreSQL import PostgreSQLDatabase
from dolphie.Modules.Queries import PostgreSQLQueries
from dolphie.DataTypes import ConnectionSource


class TestPostgreSQLIntegration:
    """Integration tests using real PostgreSQL instances."""

    def test_connection_and_basic_query(self, real_postgres_db):
        """Test basic connection and query execution."""
        assert real_postgres_db.is_connected()
        assert real_postgres_db.source == ConnectionSource.postgresql
        assert real_postgres_db.connection_id is not None
        assert real_postgres_db.has_connected is True

    def test_version_query(self, real_postgres_db):
        """Test PostgreSQL version query."""
        real_postgres_db.execute(PostgreSQLQueries.version)
        result = real_postgres_db.fetchone()
        
        assert result is not None
        assert "PostgreSQL" in result["version"]

    def test_variables_query(self, real_postgres_db):
        """Test PostgreSQL variables query."""
        real_postgres_db.execute(PostgreSQLQueries.variables)
        results = real_postgres_db.fetchall()
        
        assert len(results) > 0
        
        # Check for expected variables
        variable_names = [row["Variable_name"] for row in results]
        assert "version" in variable_names
        assert "port" in variable_names
        assert "max_connections" in variable_names

    def test_processlist_query(self, real_postgres_db):
        """Test PostgreSQL processlist query."""
        real_postgres_db.execute(PostgreSQLQueries.processlist)
        results = real_postgres_db.fetchall()
        
        # Results can be empty, but should not error
        assert isinstance(results, list)
        
        # If there are results, check structure
        if results:
            row = results[0]
            expected_columns = [
                "id", "user", "db", "host", "application_name",
                "state", "time", "query", "wait_event_type", "wait_event"
            ]
            for col in expected_columns:
                assert col in row

    def test_database_stats_query(self, real_postgres_db):
        """Test PostgreSQL database statistics query."""
        real_postgres_db.execute(PostgreSQLQueries.database_stats)
        results = real_postgres_db.fetchall()
        
        assert len(results) > 0
        
        # Check for expected columns
        row = results[0]
        expected_columns = [
            "database_name", "connections", "transactions_committed",
            "transactions_rolled_back", "blocks_read", "blocks_hit",
            "tuples_returned", "tuples_fetched", "tuples_inserted",
            "tuples_updated", "tuples_deleted", "conflicts",
            "temp_files", "temp_bytes", "deadlocks"
        ]
        for col in expected_columns:
            assert col in row

    def test_connection_stats_query(self, real_postgres_db):
        """Test PostgreSQL connection statistics query."""
        real_postgres_db.execute(PostgreSQLQueries.connection_stats)
        results = real_postgres_db.fetchall()
        
        assert len(results) > 0
        
        # Check for expected columns
        row = results[0]
        assert "state" in row
        assert "count" in row

    def test_table_stats_query(self, real_postgres_db):
        """Test PostgreSQL table statistics query."""
        # Create a test table first
        real_postgres_db.execute("""
            CREATE TABLE IF NOT EXISTS test_table_stats (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100)
            )
        """)
        
        # Insert some data
        real_postgres_db.execute("""
            INSERT INTO test_table_stats (name) VALUES ('test1'), ('test2'), ('test3')
        """)
        
        # Query table stats
        real_postgres_db.execute(PostgreSQLQueries.table_stats)
        results = real_postgres_db.fetchall()
        
        # Should have at least our test table
        assert len(results) > 0
        
        # Check for expected columns
        if results:
            row = results[0]
            expected_columns = [
                "schemaname", "tablename", "seq_scan", "seq_tup_read",
                "idx_scan", "idx_tup_fetch", "inserts", "updates",
                "deletes", "hot_updates", "live_tuples", "dead_tuples",
                "n_mod_since_analyze", "last_vacuum", "last_autovacuum",
                "last_analyze", "last_autoanalyze", "vacuum_count",
                "autovacuum_count", "analyze_count", "autoanalyze_count"
            ]
            for col in expected_columns:
                assert col in row

        # Clean up
        real_postgres_db.execute("DROP TABLE IF EXISTS test_table_stats")

    def test_index_stats_query(self, real_postgres_db):
        """Test PostgreSQL index statistics query."""
        # Create a test table with index
        real_postgres_db.execute("""
            CREATE TABLE IF NOT EXISTS test_index_stats (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100)
            )
        """)
        
        real_postgres_db.execute("""
            CREATE INDEX IF NOT EXISTS idx_test_name ON test_index_stats(name)
        """)
        
        # Query index stats
        real_postgres_db.execute(PostgreSQLQueries.index_stats)
        results = real_postgres_db.fetchall()
        
        # Should have at least our test index
        assert len(results) > 0
        
        # Check for expected columns
        if results:
            row = results[0]
            expected_columns = [
                "schemaname", "tablename", "indexname",
                "idx_scan", "idx_tup_read", "idx_tup_fetch"
            ]
            for col in expected_columns:
                assert col in row

        # Clean up
        real_postgres_db.execute("DROP TABLE IF EXISTS test_index_stats CASCADE")

    def test_replication_status_query(self, real_postgres_db):
        """Test PostgreSQL replication status query."""
        real_postgres_db.execute(PostgreSQLQueries.replication_status)
        results = real_postgres_db.fetchall()
        
        # Results can be empty for non-replica setups
        assert isinstance(results, list)
        
        # If there are results, check structure
        if results:
            row = results[0]
            expected_columns = [
                "client_addr", "client_hostname", "client_port", "state",
                "sent_lsn", "write_lsn", "flush_lsn", "replay_lsn",
                "write_lag", "flush_lag", "replay_lag", "sync_priority",
                "sync_state", "application_name"
            ]
            for col in expected_columns:
                assert col in row

    def test_locks_query(self, real_postgres_db):
        """Test PostgreSQL locks query."""
        real_postgres_db.execute(PostgreSQLQueries.locks)
        results = real_postgres_db.fetchall()
        
        # Results can be empty if no locks
        assert isinstance(results, list)
        
        # If there are results, check structure
        if results:
            row = results[0]
            expected_columns = [
                "pid", "mode", "locktype", "database", "relation",
                "page", "tuple", "virtualxid", "transactionid",
                "granted", "usename", "query", "state", "query_start"
            ]
            for col in expected_columns:
                assert col in row

    def test_slow_queries_query(self, real_postgres_db):
        """Test PostgreSQL slow queries query (pg_stat_statements)."""
        # This might not work if pg_stat_statements extension is not installed
        try:
            real_postgres_db.execute(PostgreSQLQueries.slow_queries)
            results = real_postgres_db.fetchall()
            
            # Results can be empty
            assert isinstance(results, list)
            
            # If there are results, check structure
            if results:
                row = results[0]
                expected_columns = [
                    "user", "dbid", "query", "calls", "total_time",
                    "mean_time", "min_time", "max_time", "stddev_time",
                    "rows", "hit_percent"
                ]
                for col in expected_columns:
                    assert col in row
        except psycopg2.Error:
            # pg_stat_statements extension might not be installed
            pytest.skip("pg_stat_statements extension not available")

    def test_query_prefixing(self, real_postgres_db):
        """Test that queries are properly prefixed with Dolphie comment."""
        # Execute a simple query
        real_postgres_db.execute("SELECT 1 as test_value")
        
        # Check our own process in pg_stat_activity
        real_postgres_db.execute("""
            SELECT query 
            FROM pg_stat_activity 
            WHERE pid = pg_backend_pid()
        """)
        
        result = real_postgres_db.fetchone()
        if result and result.get("query"):
            # The query should contain our Dolphie prefix
            assert "/* Dolphie */" in result["query"]

    def test_kill_query_syntax(self, real_postgres_db):
        """Test kill query syntax (without actually killing)."""
        # Test pg_cancel_backend syntax
        try:
            real_postgres_db.execute("SELECT pg_cancel_backend(NULL)")
            result = real_postgres_db.fetchone()
            # Should return false for NULL input
            assert result is not None
        except psycopg2.Error:
            # Expected for invalid PID
            pass

        # Test pg_terminate_backend syntax
        try:
            real_postgres_db.execute("SELECT pg_terminate_backend(NULL)")
            result = real_postgres_db.fetchone()
            # Should return false for NULL input
            assert result is not None
        except psycopg2.Error:
            # Expected for invalid PID
            pass

    def test_connection_id_retrieval(self, real_postgres_db):
        """Test that connection ID is properly retrieved."""
        assert real_postgres_db.connection_id is not None
        assert isinstance(real_postgres_db.connection_id, int)
        assert real_postgres_db.connection_id > 0

    def test_reconnection_behavior(self, real_postgres_db):
        """Test reconnection after connection loss."""
        original_connection_id = real_postgres_db.connection_id
        
        # Close the connection
        real_postgres_db.close()
        assert not real_postgres_db.is_connected()
        
        # Reconnect
        real_postgres_db.connect()
        assert real_postgres_db.is_connected()
        
        # Should have a new connection ID
        new_connection_id = real_postgres_db.connection_id
        assert new_connection_id is not None
        assert new_connection_id != original_connection_id

    def test_transaction_behavior(self, real_postgres_db):
        """Test that autocommit is properly set."""
        # Create a test table
        real_postgres_db.execute("""
            CREATE TABLE IF NOT EXISTS test_autocommit (
                id SERIAL PRIMARY KEY,
                value TEXT
            )
        """)
        
        # Insert data (should auto-commit)
        real_postgres_db.execute("""
            INSERT INTO test_autocommit (value) VALUES ('test')
        """)
        
        # Check data was committed
        real_postgres_db.execute("""
            SELECT COUNT(*) as count FROM test_autocommit
        """)
        result = real_postgres_db.fetchone()
        assert result["count"] > 0
        
        # Clean up
        real_postgres_db.execute("DROP TABLE IF EXISTS test_autocommit")

    def test_multiple_connections(self, postgres_config, mock_app):
        """Test multiple simultaneous connections."""
        # Create two separate connections
        db1 = PostgreSQLDatabase(
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
        
        db2 = PostgreSQLDatabase(
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
        
        try:
            # Both should be connected with different connection IDs
            assert db1.is_connected()
            assert db2.is_connected()
            assert db1.connection_id != db2.connection_id
            
            # Both should be able to execute queries
            db1.execute("SELECT 1")
            result1 = db1.fetchone()
            
            db2.execute("SELECT 2")
            result2 = db2.fetchone()
            
            assert result1 is not None
            assert result2 is not None
            
        finally:
            db1.close()
            db2.close()

    def test_database_specific_connection(self, postgres_config, mock_app):
        """Test connection to specific database."""
        # Create a test database
        admin_db = PostgreSQLDatabase(
            app=mock_app,
            host=postgres_config.host,
            user=postgres_config.user,
            password=postgres_config.password,
            socket=None,
            port=postgres_config.port,
            ssl=None,
            database=postgres_config.database,  # Connect to default DB first
            auto_connect=True
        )
        
        try:
            # Create test database
            admin_db.execute("CREATE DATABASE test_dolphie_db")
            
            # Connect to the new database
            test_db = PostgreSQLDatabase(
                app=mock_app,
                host=postgres_config.host,
                user=postgres_config.user,
                password=postgres_config.password,
                socket=None,
                port=postgres_config.port,
                ssl=None,
                database="test_dolphie_db",
                auto_connect=True
            )
            
            try:
                assert test_db.is_connected()
                
                # Verify we're connected to the right database
                test_db.execute("SELECT current_database()")
                result = test_db.fetchone()
                assert result["current_database"] == "test_dolphie_db"
                
            finally:
                test_db.close()
                
        finally:
            # Clean up
            admin_db.execute("DROP DATABASE IF EXISTS test_dolphie_db")
            admin_db.close()


class TestPostgreSQLQueriesIntegration:
    """Test PostgreSQL queries class integration."""

    def test_all_queries_are_valid_sql(self, real_postgres_db):
        """Test that all PostgreSQL queries are syntactically valid."""
        queries_to_test = [
            ("variables", PostgreSQLQueries.variables),
            ("processlist", PostgreSQLQueries.processlist),
            ("database_stats", PostgreSQLQueries.database_stats),
            ("connection_stats", PostgreSQLQueries.connection_stats),
            ("replication_status", PostgreSQLQueries.replication_status),
            ("table_stats", PostgreSQLQueries.table_stats),
            ("index_stats", PostgreSQLQueries.index_stats),
            ("locks", PostgreSQLQueries.locks),
            ("version", PostgreSQLQueries.version),
        ]
        
        for name, query in queries_to_test:
            try:
                real_postgres_db.execute(query)
                results = real_postgres_db.fetchall()
                # Query should execute without error
                assert isinstance(results, list)
                print(f"✓ Query '{name}' executed successfully, returned {len(results)} rows")
            except psycopg2.Error as e:
                pytest.fail(f"Query '{name}' failed: {e}")

    def test_slow_queries_with_extension_check(self, real_postgres_db):
        """Test slow queries with proper extension checking."""
        try:
            # Check if pg_stat_statements extension is available
            real_postgres_db.execute("""
                SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
            """)
            extension_exists = real_postgres_db.fetchone()
            
            if extension_exists:
                real_postgres_db.execute(PostgreSQLQueries.slow_queries)
                results = real_postgres_db.fetchall()
                assert isinstance(results, list)
                print(f"✓ Slow queries executed successfully, returned {len(results)} rows")
            else:
                print("✓ pg_stat_statements extension not installed, skipping slow queries test")
                
        except psycopg2.Error as e:
            # Extension might not be available
            print(f"✓ pg_stat_statements not available: {e}")


class TestPostgreSQLErrorHandling:
    """Test PostgreSQL error handling and edge cases."""

    def test_connection_timeout(self, mock_app):
        """Test connection timeout handling."""
        # Try to connect to a non-existent host
        with pytest.raises(Exception):  # Could be ManualException or connection error
            db = PostgreSQLDatabase(
                app=mock_app,
                host="nonexistent.host.invalid",
                user="test",
                password="test",
                socket=None,
                port=5432,
                ssl=None,
                auto_connect=True
            )

    def test_invalid_credentials(self, postgres_config, mock_app):
        """Test invalid credentials handling."""
        with pytest.raises(Exception):  # Should raise authentication error
            db = PostgreSQLDatabase(
                app=mock_app,
                host=postgres_config.host,
                user="invalid_user",
                password="invalid_password",
                socket=None,
                port=postgres_config.port,
                ssl=None,
                auto_connect=True
            )

    def test_invalid_query_execution(self, real_postgres_db):
        """Test invalid query execution."""
        with pytest.raises(Exception):
            real_postgres_db.execute("INVALID SQL SYNTAX HERE")

    def test_query_execution_on_closed_connection(self, real_postgres_db):
        """Test query execution after connection is closed."""
        # Close the connection
        real_postgres_db.close()
        
        # Try to execute a query
        result = real_postgres_db.execute("SELECT 1")
        assert result is None