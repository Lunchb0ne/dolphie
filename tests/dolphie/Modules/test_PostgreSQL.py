"""Tests for PostgreSQL module functionality."""

from unittest.mock import MagicMock

import pytest

from dolphie.DataTypes import ConnectionSource, ConnectionStatus
from dolphie.Modules.PostgreSQL import (
    PostgreSQLDatabase,
    PostgreSQLDataProcessor,
    PostgreSQLMetricManager,
    PostgreSQLProcesslistThread,
)


class TestPostgreSQLDatabase:
    """Tests for PostgreSQLDatabase class."""

    def test_init_sets_connection_source(self):
        """Test that initialization sets correct connection source."""
        db = PostgreSQLDatabase(None, "localhost", "user", "pass", 5432)
        assert db.source == ConnectionSource.postgresql

    def test_init_sets_default_values(self):
        """Test that initialization sets correct default values."""
        db = PostgreSQLDatabase(None, "localhost", "user", "pass", 5432)
        assert db.host == "localhost"
        assert db.user == "user"
        assert db.password == "pass"
        assert db.port == 5432
        assert db.dbname == "postgres"
        assert db.connect_timeout == 3
        assert db.is_running_query is False
        assert db.server_version == ""
        assert db.server_version_major == 0

    def test_init_custom_dbname(self):
        """Test that custom dbname is set correctly."""
        db = PostgreSQLDatabase(None, "localhost", "user", "pass", 5432, dbname="mydb")
        assert db.dbname == "mydb"

    @pytest.mark.parametrize(
        ("version_string", "expected_version", "expected_major"),
        [
            ("14.5", "14.5", 14),
            ("15.2 (Ubuntu 15.2-1.pgdg22.04+1)", "15.2", 15),
            ("13.0", "13.0", 13),
            ("16.1 (Debian 16.1-1.pgdg120+1)", "16.1", 16),
            ("9.6.24", "9.6.24", 9),
            ("", "", 0),
            ("invalid", "invalid", 0),
        ],
    )
    def test_parse_server_version(self, version_string, expected_version, expected_major):
        """Test server version parsing handles various formats correctly."""
        db = PostgreSQLDatabase(None, "localhost", "user", "pass", 5432)
        version, major = db.parse_server_version(version_string)
        assert version == expected_version
        assert major == expected_major


class TestPostgreSQLProcesslistThread:
    """Tests for PostgreSQLProcesslistThread class."""

    def test_init_with_pid(self):
        """Test initialization with pid field (from database query)."""
        thread_data = {
            "pid": 12345,
            "user": "postgres",
            "db": "mydb",
            "host": "127.0.0.1",
            "state": "active",
            "time": 5,
            "wait_event": "ClientRead",
            "query": "SELECT * FROM users",
        }
        thread = PostgreSQLProcesslistThread(thread_data)

        assert thread.id == "12345"
        assert thread.user == "postgres"
        assert thread.db == "mydb"
        assert thread.host == "127.0.0.1"
        assert thread.state == "active"
        assert thread.time == 5
        assert thread.wait_event == "ClientRead"
        assert thread.query == "SELECT * FROM users"
        assert thread.command == "active"

    def test_init_with_id_fallback(self):
        """Test initialization falls back to 'id' field when 'pid' is missing (for replay)."""
        thread_data = {
            "id": "99999",
            "user": "appuser",
            "db": "testdb",
            "host": "10.0.0.1",
            "state": "idle",
            "time": 0,
            "wait_event": "",
            "query": "",
        }
        thread = PostgreSQLProcesslistThread(thread_data)
        assert thread.id == "99999"

    def test_thread_data_attribute_exists(self):
        """Test that thread_data attribute is stored for replay serialization."""
        thread_data = {
            "pid": 123,
            "user": "test",
            "db": "db",
            "host": "",
            "state": "",
            "time": 0,
            "wait_event": "",
            "query": "",
        }
        thread = PostgreSQLProcesslistThread(thread_data)
        assert thread.thread_data is thread_data

    def test_formatted_time_green_for_short_queries(self):
        """Test that short running queries get green color."""
        thread_data = {
            "pid": 1,
            "user": "",
            "db": "",
            "host": "",
            "state": "",
            "time": 2,
            "wait_event": "",
            "query": "SELECT 1",
        }
        thread = PostgreSQLProcesslistThread(thread_data)
        assert "[green]" in thread.formatted_time

    def test_formatted_time_yellow_for_medium_queries(self):
        """Test that medium running queries get yellow color."""
        thread_data = {
            "pid": 1,
            "user": "",
            "db": "",
            "host": "",
            "state": "",
            "time": 7,
            "wait_event": "",
            "query": "SELECT 1",
        }
        thread = PostgreSQLProcesslistThread(thread_data)
        assert "[yellow]" in thread.formatted_time

    def test_formatted_time_red_for_long_queries(self):
        """Test that long running queries get red color."""
        thread_data = {
            "pid": 1,
            "user": "",
            "db": "",
            "host": "",
            "state": "",
            "time": 15,
            "wait_event": "",
            "query": "SELECT 1",
        }
        thread = PostgreSQLProcesslistThread(thread_data)
        assert "[red]" in thread.formatted_time

    def test_handles_none_time(self):
        """Test that None time value is handled gracefully."""
        thread_data = {
            "pid": 1,
            "user": "",
            "db": "",
            "host": "",
            "state": "",
            "time": None,
            "wait_event": "",
            "query": "",
        }
        thread = PostgreSQLProcesslistThread(thread_data)
        assert thread.time == 0


class TestPostgreSQLMetricManager:
    """Tests for PostgreSQLMetricManager class."""

    def test_init_sets_connection_source(self):
        """Test that initialization sets correct connection source."""
        mm = PostgreSQLMetricManager(None)
        assert mm.connection_source == ConnectionSource.postgresql

    def test_init_creates_metric_instances(self):
        """Test that initialization creates all required metric instances."""
        mm = PostgreSQLMetricManager(None)

        # Check database metrics
        assert hasattr(mm.metrics, "dml")
        assert hasattr(mm.metrics, "transactions")
        assert hasattr(mm.metrics, "tuples")
        assert hasattr(mm.metrics, "wal")

        # Check system metrics
        assert hasattr(mm.metrics, "system_cpu")
        assert hasattr(mm.metrics, "system_memory")
        assert hasattr(mm.metrics, "system_disk_io")
        assert hasattr(mm.metrics, "system_network")

    def test_metric_graphs_defined(self):
        """Test that graph names are correctly defined for each metric."""
        mm = PostgreSQLMetricManager(None)

        assert mm.metrics.dml.graphs == ["graph_dml"]
        assert mm.metrics.transactions.graphs == ["graph_transactions"]
        assert "graph_tuples_activity" in mm.metrics.tuples.graphs
        assert mm.metrics.wal.graphs == ["graph_wal_bytes"]

    def test_refresh_data_updates_datetimes(self):
        """Test that refresh_data appends datetime entries."""
        mm = PostgreSQLMetricManager(None)
        assert len(mm.datetimes) == 0

        mm.refresh_data(global_status={}, system_utilization={})
        assert len(mm.datetimes) == 1

    def test_refresh_data_calculates_queries(self):
        """Test that refresh_data calculates total queries from tuple operations."""
        mm = PostgreSQLMetricManager(None)

        global_status = {
            "tup_returned": 100,
            "tup_fetched": 50,
            "tup_inserted": 10,
            "tup_updated": 5,
            "tup_deleted": 2,
        }

        mm.refresh_data(global_status=global_status, system_utilization={})

        # Total should be 100 + 50 + 10 + 5 + 2 = 167
        # Check that the metric was updated in the metric manager
        # Since this is the first tick and it's a rate metric, values[-1] will be 0
        # We check last_value to verify the raw calculation
        assert mm.metrics.dml.Queries.last_value == 167

        # Check legacy behavior: global_status should NOT be mutated
        assert "Queries" not in global_status

    def test_refresh_data_handles_empty_status(self):
        """Test that refresh_data handles empty global_status gracefully."""
        mm = PostgreSQLMetricManager(None)
        mm.refresh_data(global_status={}, system_utilization={})
        # Should not raise any exceptions

    def test_refresh_data_limits_datetime_history(self):
        """Test that datetime history is limited to 100 entries."""
        mm = PostgreSQLMetricManager(None)

        for _ in range(150):
            mm.refresh_data(global_status={}, system_utilization={})

        assert len(mm.datetimes) == 100

    def test_reset_clears_data(self):
        """Test that reset clears all stored data."""
        mm = PostgreSQLMetricManager(None)
        mm.datetimes.append("12:00:00")
        mm.global_status["test"] = 123

        mm.reset()

        assert len(mm.datetimes) == 0
        assert len(mm.global_status) == 0

    def test_metric_connection_sources(self):
        """Test that metrics have correct connection source associations."""
        mm = PostgreSQLMetricManager(None)

        assert ConnectionSource.postgresql in mm.metrics.transactions.connection_source
        assert ConnectionSource.postgresql in mm.metrics.tuples.connection_source
        assert ConnectionSource.postgresql in mm.metrics.wal.connection_source


class TestPostgreSQLDataProcessor:
    """Tests for PostgreSQLDataProcessor class."""

    def test_process_data_fetches_replication_and_table_health(self):
        """Test that data processor fetches replication and table health data."""
        app = MagicMock()
        # Mock dolphie object structure
        app.dolphie.panels.processlist.visible = True
        app.dolphie.panels.dashboard.visible = True

        mock_db = MagicMock()
        mock_db.server_version_major = 15
        mock_db.execute = MagicMock()
        mock_db.fetchall.return_value = []

        app.dolphie.main_db_connection = mock_db
        app.dolphie.metric_manager = MagicMock()

        processor = PostgreSQLDataProcessor(app)

        # We need to simulate the tab object passed to process_data
        tab = MagicMock()
        tab.dolphie = app.dolphie
        tab.dolphie.main_db_connection = mock_db

        processor.process_data(tab)

        # Verify execute was called for replication and table health
        assert mock_db.execute.call_count >= 3


class TestPostgreSQLVersionIntegration:
    """Integration tests for version-dependent feature enablement."""

    def test_wal_stats_enabled_for_postgres_14_and_above(self):
        """Test that WAL stats are fetched for PostgreSQL >= 14."""
        app = MagicMock()
        # Mock dolphie structure
        app.dolphie.panels.processlist.visible = False
        app.dolphie.panels.dashboard.visible = True
        app.dolphie.connection_status = ConnectionStatus.connected

        # Test for version 14
        mock_db = MagicMock()
        mock_db.is_connected.return_value = True
        mock_db.server_version_major = 14
        mock_db.execute = MagicMock()
        mock_db.fetchone.return_value = {}

        app.dolphie.main_db_connection = mock_db
        app.dolphie.metric_manager = MagicMock()

        processor = PostgreSQLDataProcessor(app)

        tab = MagicMock()
        tab.dolphie = app.dolphie

        processor.process_data(tab)

        # Verify WAL stats query executed
        # We need to import Queries to check against actual query string or just check call args generally
        # Here we'll check if any call contained "wal" in the query string or matched the specific query if possible
        # Since we don't have easy access to the exact query string constant from here without importing,
        # let's import it or check logic.
        from dolphie.Modules.Queries import PostgreSQLQueries

        # Check if execute was called with wal_stats query
        calls = [args[0][0] for args in mock_db.execute.call_args_list]
        assert PostgreSQLQueries.wal_stats in calls

        # Test for version 15
        mock_db.reset_mock()
        mock_db.server_version_major = 15
        mock_db.fetchone.return_value = {}
        processor.process_data(tab)

        calls = [args[0][0] for args in mock_db.execute.call_args_list]
        assert PostgreSQLQueries.wal_stats in calls

    def test_wal_stats_disabled_for_postgres_13_and_below(self):
        """Test that WAL stats are skipped for PostgreSQL < 14."""
        app = MagicMock()
        app.dolphie.panels.processlist.visible = False
        app.dolphie.panels.dashboard.visible = True
        app.dolphie.connection_status = ConnectionStatus.connected

        # Test for version 13
        mock_db = MagicMock()
        mock_db.is_connected.return_value = True
        mock_db.server_version_major = 13
        mock_db.execute = MagicMock()
        mock_db.fetchone.return_value = {}

        app.dolphie.main_db_connection = mock_db
        app.dolphie.metric_manager = MagicMock()

        processor = PostgreSQLDataProcessor(app)
        tab = MagicMock()
        tab.dolphie = app.dolphie

        processor.process_data(tab)

        from dolphie.Modules.Queries import PostgreSQLQueries

        # Check that wal_stats query was NOT called
        calls = [args[0][0] for args in mock_db.execute.call_args_list]
        assert PostgreSQLQueries.wal_stats not in calls

        # Test for version 9
        mock_db.reset_mock()
        mock_db.server_version_major = 9
        mock_db.fetchone.return_value = {}
        processor.process_data(tab)

        calls = [args[0][0] for args in mock_db.execute.call_args_list]
        assert PostgreSQLQueries.wal_stats not in calls

    def test_refresh_data_does_not_mutate_global_status(self):
        """Test that refresh_data does not mutate the passed global_status dictionary."""
        app = MagicMock()
        mm = PostgreSQLMetricManager(app)

        global_status = {
            "tup_returned": 100,
            "tup_fetched": 50,
            "tup_inserted": 10,
            "tup_updated": 5,
            "tup_deleted": 2,
        }
        original_status = global_status.copy()

        mm.refresh_data(global_status=global_status, system_utilization={})

        # Check that "Queries" was NOT added to the original dictionary
        assert "Queries" not in global_status
        assert global_status == original_status


class TestPostgreSQLDataProcessorErrors:
    """Tests for error handling in PostgreSQLDataProcessor."""

    def test_process_data_handles_replication_query_failure(self):
        """Test that process_data continues if replication query fails."""
        app = MagicMock()
        # Mock dolphie structure
        app.dolphie.panels.processlist.visible = False
        app.dolphie.panels.dashboard.visible = True

        mock_db = MagicMock()
        mock_db.server_version_major = 15

        # Simulate execute raising an exception for replication query
        def side_effect(query, *args, **kwargs):
            from dolphie.Modules.Queries import PostgreSQLQueries

            if query == PostgreSQLQueries.replication:
                raise Exception("Permission denied")
            if query == PostgreSQLQueries.table_health:
                return  # Success for others
            return None

        mock_db.execute.side_effect = side_effect
        mock_db.fetchall.return_value = []

        app.dolphie.main_db_connection = mock_db
        app.dolphie.metric_manager = MagicMock()

        processor = PostgreSQLDataProcessor(app)
        tab = MagicMock()
        tab.dolphie = app.dolphie

        # Should not raise exception
        processor.process_data(tab)

        # Verify empty list fallback
        assert app.dolphie.postgresql_replication == []

    def test_process_data_handles_table_health_query_failure(self):
        """Test that process_data continues if table health query fails."""
        app = MagicMock()
        app.dolphie.panels.processlist.visible = False
        app.dolphie.panels.dashboard.visible = True

        mock_db = MagicMock()
        mock_db.server_version_major = 15

        def side_effect(query, *args, **kwargs):
            from dolphie.Modules.Queries import PostgreSQLQueries

            if query == PostgreSQLQueries.table_health:
                raise Exception("Something went wrong")
            return None

        mock_db.execute.side_effect = side_effect
        mock_db.fetchall.return_value = []

        app.dolphie.main_db_connection = mock_db
        app.dolphie.metric_manager = MagicMock()

        processor = PostgreSQLDataProcessor(app)
        tab = MagicMock()
        tab.dolphie = app.dolphie

        processor.process_data(tab)

        assert app.dolphie.postgresql_table_health == []
