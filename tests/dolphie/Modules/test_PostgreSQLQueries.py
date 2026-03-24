import pytest
from dolphie.Modules.Queries import PostgreSQLQueries


class TestPostgreSQLQueries:
    def test_queries_are_strings(self):
        """Test that all query attributes are strings."""
        assert isinstance(PostgreSQLQueries.server_version, str)
        assert isinstance(PostgreSQLQueries.uptime, str)
        assert isinstance(PostgreSQLQueries.activity, str)
        assert isinstance(PostgreSQLQueries.replication, str)
        assert isinstance(PostgreSQLQueries.table_health, str)
        assert isinstance(PostgreSQLQueries.wal_stats, str)
        assert isinstance(PostgreSQLQueries.global_stats, str)

    def test_replication_query_contains_keywords(self):
        """Test that replication query fetches key fields."""
        q = PostgreSQLQueries.replication.lower()
        assert "pg_stat_replication" in q
        assert "application_name" in q
        assert "replay_lag" in q

    def test_table_health_query_contains_keywords(self):
        """Test that table health query fetches key fields."""
        q = PostgreSQLQueries.table_health.lower()
        assert "pg_stat_user_tables" in q
        assert "n_dead_tup" in q
        assert "vacuum" in q
