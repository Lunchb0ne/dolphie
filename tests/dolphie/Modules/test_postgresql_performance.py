"""Performance and stress tests for PostgreSQL functionality."""

import pytest
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import MagicMock

from dolphie.Modules.PostgreSQL import PostgreSQLDatabase
from dolphie.Modules.Queries import PostgreSQLQueries


class TestPostgreSQLPerformance:
    """Performance tests for PostgreSQL functionality."""

    def test_connection_speed(self, postgres_config, mock_app):
        """Test connection establishment speed."""
        start_time = time.time()
        
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
        
        connection_time = time.time() - start_time
        
        try:
            assert db.is_connected()
            # Connection should be fast (< 5 seconds)
            assert connection_time < 5.0
            print(f"✓ Connection established in {connection_time:.3f} seconds")
        finally:
            db.close()

    def test_query_execution_speed(self, real_postgres_db):
        """Test query execution performance."""
        # Test simple query speed
        start_time = time.time()
        real_postgres_db.execute("SELECT 1")
        real_postgres_db.fetchone()
        simple_query_time = time.time() - start_time
        
        # Should be very fast
        assert simple_query_time < 1.0
        print(f"✓ Simple query executed in {simple_query_time:.3f} seconds")
        
        # Test more complex query speed
        start_time = time.time()
        real_postgres_db.execute(PostgreSQLQueries.processlist)
        real_postgres_db.fetchall()
        complex_query_time = time.time() - start_time
        
        # Should still be reasonably fast
        assert complex_query_time < 5.0
        print(f"✓ Complex query executed in {complex_query_time:.3f} seconds")

    def test_multiple_queries_performance(self, real_postgres_db):
        """Test performance of multiple sequential queries."""
        start_time = time.time()
        
        for i in range(10):
            real_postgres_db.execute(f"SELECT {i}")
            real_postgres_db.fetchone()
        
        total_time = time.time() - start_time
        avg_time = total_time / 10
        
        # 10 queries should complete reasonably fast
        assert total_time < 10.0
        assert avg_time < 1.0
        print(f"✓ 10 queries executed in {total_time:.3f} seconds (avg: {avg_time:.3f}s)")

    def test_large_result_set_handling(self, real_postgres_db):
        """Test handling of large result sets."""
        # Create table with data
        real_postgres_db.execute("""
            CREATE TABLE IF NOT EXISTS test_large_data AS
            SELECT generate_series(1, 1000) as id, 
                   'test_data_' || generate_series(1, 1000) as name
        """)
        
        try:
            start_time = time.time()
            real_postgres_db.execute("SELECT * FROM test_large_data")
            results = real_postgres_db.fetchall()
            fetch_time = time.time() - start_time
            
            assert len(results) == 1000
            # Should handle 1000 rows reasonably fast
            assert fetch_time < 5.0
            print(f"✓ Fetched 1000 rows in {fetch_time:.3f} seconds")
            
        finally:
            real_postgres_db.execute("DROP TABLE IF EXISTS test_large_data")

    def test_concurrent_connections(self, postgres_config, mock_app):
        """Test concurrent database connections."""
        num_connections = 5
        connections = []
        
        def create_connection():
            return PostgreSQLDatabase(
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
        
        start_time = time.time()
        
        # Create connections concurrently
        with ThreadPoolExecutor(max_workers=num_connections) as executor:
            future_to_conn = {executor.submit(create_connection): i for i in range(num_connections)}
            
            for future in as_completed(future_to_conn):
                conn = future.result()
                connections.append(conn)
                assert conn.is_connected()
        
        creation_time = time.time() - start_time
        
        try:
            # Test concurrent queries
            def execute_query(db, query_id):
                db.execute(f"SELECT {query_id} as query_id")
                return db.fetchone()
            
            start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=num_connections) as executor:
                futures = [executor.submit(execute_query, conn, i) for i, conn in enumerate(connections)]
                results = [future.result() for future in as_completed(futures)]
            
            query_time = time.time() - start_time
            
            assert len(results) == num_connections
            # All operations should complete reasonably fast
            assert creation_time < 10.0
            assert query_time < 5.0
            
            print(f"✓ {num_connections} concurrent connections created in {creation_time:.3f}s")
            print(f"✓ {num_connections} concurrent queries executed in {query_time:.3f}s")
            
        finally:
            for conn in connections:
                conn.close()

    def test_connection_pool_stress(self, postgres_config, mock_app):
        """Test connection creation and destruction under stress."""
        num_iterations = 20
        
        def create_and_close_connection():
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
            
            # Execute a simple query
            db.execute("SELECT 1")
            result = db.fetchone()
            assert result is not None
            
            db.close()
            return True
        
        start_time = time.time()
        
        # Run iterations concurrently
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(create_and_close_connection) for _ in range(num_iterations)]
            results = [future.result() for future in as_completed(futures)]
        
        total_time = time.time() - start_time
        
        assert all(results)
        assert total_time < 30.0  # Should complete within reasonable time
        
        print(f"✓ {num_iterations} connection cycles completed in {total_time:.3f}s")


class TestPostgreSQLStress:
    """Stress tests for PostgreSQL functionality."""

    def test_reconnection_stress(self, postgres_config, mock_app):
        """Test reconnection under stress conditions."""
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
        
        try:
            # Repeatedly close and reconnect
            for i in range(10):
                assert db.is_connected()
                original_id = db.connection_id
                
                db.close()
                assert not db.is_connected()
                
                db.connect()
                assert db.is_connected()
                assert db.connection_id != original_id
                
                # Execute query to verify connection works
                db.execute("SELECT 1")
                result = db.fetchone()
                assert result is not None
            
            print("✓ 10 reconnection cycles completed successfully")
            
        finally:
            db.close()

    def test_query_timeout_handling(self, real_postgres_db):
        """Test handling of long-running queries."""
        # Create a query that takes some time but not too long for testing
        real_postgres_db.execute("SELECT pg_sleep(0.1)")
        result = real_postgres_db.fetchone()
        
        # Should complete successfully
        assert result is not None
        print("✓ Long-running query handled successfully")

    def test_error_recovery(self, real_postgres_db):
        """Test recovery from various error conditions."""
        # Test syntax error recovery
        try:
            real_postgres_db.execute("INVALID SQL SYNTAX")
        except Exception:
            pass  # Expected
        
        # Should still be able to execute valid queries
        real_postgres_db.execute("SELECT 1")
        result = real_postgres_db.fetchone()
        assert result is not None
        
        # Test permission error recovery
        try:
            real_postgres_db.execute("CREATE USER invalid_test_user")
        except Exception:
            pass  # Expected in test environment
        
        # Should still work
        real_postgres_db.execute("SELECT 2")
        result = real_postgres_db.fetchone()
        assert result is not None
        
        print("✓ Error recovery working correctly")

    def test_memory_usage_with_large_datasets(self, real_postgres_db):
        """Test memory usage with large result sets."""
        # Create larger test dataset
        real_postgres_db.execute("""
            CREATE TABLE IF NOT EXISTS test_memory_usage AS
            SELECT 
                generate_series(1, 5000) as id, 
                md5(random()::text) as hash_value,
                random() * 1000 as numeric_value,
                'This is a longer text field with some data ' || generate_series(1, 5000) as text_field
        """)
        
        try:
            # Query large dataset
            real_postgres_db.execute("SELECT * FROM test_memory_usage")
            results = real_postgres_db.fetchall()
            
            assert len(results) == 5000
            
            # Verify we can still perform operations
            real_postgres_db.execute("SELECT COUNT(*) as count FROM test_memory_usage")
            count_result = real_postgres_db.fetchone()
            assert count_result["count"] == 5000
            
            print("✓ Large dataset handling successful")
            
        finally:
            real_postgres_db.execute("DROP TABLE IF EXISTS test_memory_usage")


class TestPostgreSQLEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_null_values_handling(self, real_postgres_db):
        """Test handling of NULL values in queries."""
        real_postgres_db.execute("""
            CREATE TABLE IF NOT EXISTS test_nulls (
                id INTEGER,
                nullable_field TEXT,
                another_field INTEGER
            )
        """)
        
        try:
            # Insert data with NULLs
            real_postgres_db.execute("""
                INSERT INTO test_nulls VALUES 
                (1, 'not null', 100),
                (2, NULL, 200),
                (3, 'also not null', NULL)
            """)
            
            # Query with NULLs
            real_postgres_db.execute("SELECT * FROM test_nulls ORDER BY id")
            results = real_postgres_db.fetchall()
            
            assert len(results) == 3
            assert results[0]["nullable_field"] == "not null"
            assert results[1]["nullable_field"] is None
            assert results[2]["another_field"] is None
            
            print("✓ NULL values handled correctly")
            
        finally:
            real_postgres_db.execute("DROP TABLE IF EXISTS test_nulls")

    def test_special_characters_in_queries(self, real_postgres_db):
        """Test handling of special characters in queries."""
        real_postgres_db.execute("""
            CREATE TABLE IF NOT EXISTS test_special_chars (
                id INTEGER,
                special_text TEXT
            )
        """)
        
        try:
            # Insert data with special characters
            special_values = [
                "Text with 'single quotes'",
                'Text with "double quotes"',
                "Text with $dollar$ signs",
                "Text with unicode: ñáéíóú",
                "Text with newlines\nand\ttabs",
                "Text with semicolons; and backslashes\\",
            ]
            
            for i, value in enumerate(special_values):
                real_postgres_db.execute(
                    "INSERT INTO test_special_chars VALUES (%s, %s)",
                    (i + 1, value)
                )
            
            # Query back the data
            real_postgres_db.execute("SELECT * FROM test_special_chars ORDER BY id")
            results = real_postgres_db.fetchall()
            
            assert len(results) == len(special_values)
            for i, result in enumerate(results):
                assert result["special_text"] == special_values[i]
            
            print("✓ Special characters handled correctly")
            
        finally:
            real_postgres_db.execute("DROP TABLE IF EXISTS test_special_chars")

    def test_empty_result_sets(self, real_postgres_db):
        """Test handling of empty result sets."""
        # Query that returns no rows
        real_postgres_db.execute("SELECT * FROM pg_settings WHERE name = 'nonexistent_setting'")
        results = real_postgres_db.fetchall()
        
        assert results == []
        
        # fetchone on empty result
        real_postgres_db.execute("SELECT * FROM pg_settings WHERE name = 'nonexistent_setting'")
        result = real_postgres_db.fetchone()
        
        assert result is None
        
        print("✓ Empty result sets handled correctly")

    def test_very_long_query_strings(self, real_postgres_db):
        """Test handling of very long query strings."""
        # Create a long SELECT statement
        long_select = "SELECT " + ", ".join([f"{i} as col_{i}" for i in range(100)])
        
        real_postgres_db.execute(long_select)
        result = real_postgres_db.fetchone()
        
        assert result is not None
        assert len(result) == 100
        assert result["col_50"] == 50
        
        print("✓ Long query strings handled correctly")

    def test_concurrent_query_execution_safety(self, real_postgres_db):
        """Test safety of concurrent query execution attempts."""
        # Set is_running_query to True to simulate concurrent execution
        real_postgres_db.is_running_query = True
        
        result = real_postgres_db.execute("SELECT 1")
        
        # Should return None and notify app
        assert result is None
        real_postgres_db.app.notify.assert_called_once()
        
        # Reset for cleanup
        real_postgres_db.is_running_query = False
        
        print("✓ Concurrent query execution safety verified")