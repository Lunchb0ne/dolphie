from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import psycopg2
import pytest

from dolphie.DataTypes import ConnectionSource, ConnectionStatus, DatabaseRow, Panels, PostgreSQLProcesslistThread
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.MetricDefinitions import MetricData, MetricValue
from dolphie.Modules.MetricGraphDefinitions import GRAPH_TABS
from dolphie.Modules.MetricManager import MetricManager
from dolphie.Modules.PostgreSQL import PostgreSQLDatabase
from dolphie.Modules.Queries import PostgreSQLQueries
from dolphie.Modules.TabManager import Tab
from dolphie.Modules.WorkerDataProcessor import WorkerDataProcessor

BASE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


class InsufficientPrivilegeError(psycopg2.Error):
    pgcode = "42501"


class AdminShutdownError(psycopg2.Error):
    pgcode = "57P01"


class UndefinedTableError(psycopg2.Error):
    pgcode = "42P01"


class QueryDatabase(PostgreSQLDatabase):
    """A connection that answers each query with canned rows and records what it ran."""

    def __init__(self, rows_by_query: dict[str, list[DatabaseRow]], connection_id: int | None = None) -> None:
        super().__init__(app=MagicMock(), host="localhost", user="postgres", password="", port=5432)
        self.rows_by_query = rows_by_query
        self.connection_id = connection_id
        self.queries: list[str] = []
        self._rows: list[DatabaseRow] = []

    def execute(self, query: str, values: Any = None, ignore_error: bool = False) -> int | None:
        self.queries.append(query)
        self._rows = next((rows for prefix, rows in self.rows_by_query.items() if query.startswith(prefix)), [])
        return len(self._rows)

    def fetchall(self) -> list[DatabaseRow]:
        return [dict(row) for row in self._rows]

    def fetchone(self) -> DatabaseRow:
        return dict(self._rows[0]) if self._rows else {}


def metric_values(metric_data: MetricData) -> list[MetricValue]:
    return metric_data.snapshot()[1]


def make_connected_database(cursor: MagicMock) -> PostgreSQLDatabase:
    database = PostgreSQLDatabase(app=MagicMock(), host="db1", user="postgres", password="secret", port=5432)
    database.connection = MagicMock(closed=0)
    database.cursor = cursor
    return database


def server_rows(server_version_num: int, settings: dict[str, str] | None = None) -> dict[str, list[DatabaseRow]]:
    version = f"{server_version_num // 10000}.4 (Ubuntu {server_version_num // 10000}.4-1.pgdg24.04+1)"
    all_settings = {"server_version": version, "server_version_num": str(server_version_num), **(settings or {})}
    return {
        PostgreSQLQueries.settings: [{"name": name, "setting": value} for name, value in all_settings.items()],
        PostgreSQLQueries.global_stats: [
            {
                "xact_commit": Decimal(100),
                "xact_rollback": 2,
                "tup_returned": 10,
                "tup_fetched": 20,
                "tup_inserted": 3,
                "tup_updated": 4,
                "tup_deleted": 5,
                "conflicts": 0,
                "deadlocks": 1,
            }
        ],
        PostgreSQLQueries.wal_stats: [{"wal_records": 7, "wal_fpi": 1, "wal_bytes": 4096, "wal_buffers_full": 0}],
        PostgreSQLQueries.connection_stats: [{"active_connections": 1, "idle_connections": 2, "total_connections": 3}],
        PostgreSQLQueries.server_state: [{"uptime": 3600, "in_recovery": 0}],
        PostgreSQLQueries.replication: [{"host": "10.0.0.2", "application_name": "standby1", "replay_lag": 0}],
        PostgreSQLQueries.table_health: [{"table": "orders", "n_dead_tup": 50}],
        PostgreSQLQueries.activity.split("$1")[0]: [],
    }


def make_tab(database: QueryDatabase, *visible_panels: str) -> Tab:
    panels = Panels()
    for panel in visible_panels:
        getattr(panels, panel).visible = True

    dolphie = SimpleNamespace(
        main_db_connection=database,
        secondary_db_connection=SimpleNamespace(connection_id=None),
        connection_status=ConnectionStatus.connecting,
        global_variables={},
        global_status={},
        host_version=None,
        panels=panels,
        processlist_threads={},
        postgresql_replication=[],
        postgresql_table_health=[],
        show_idle_threads=False,
        user_filter=None,
        db_filter=None,
        host_filter=None,
        query_filter=None,
        query_time_filter=None,
        get_hostname=lambda host: host,
    )
    return cast(Tab, SimpleNamespace(dolphie=dolphie, replay_manager=None))


def test_processlist_thread_formats_postgresql_columns() -> None:
    thread = PostgreSQLProcesslistThread(
        {
            "id": 4242,
            "user": "app",
            "db": "orders",
            "host": None,
            "state": "idle in transaction",
            "time": 12,
            "wait_event": None,
            "query": "SELECT 1",
        }
    )

    assert thread.id == "4242"
    assert thread.command == "idle in transaction"
    # A Unix socket backend has no client address and a running backend may have no wait event
    assert thread.host == "[$dark_gray]N/A"
    assert thread.wait_event == "[$dark_gray]N/A"
    assert thread.formatted_time.startswith("[$red]")


def test_aggregates_are_cast_to_bigint_so_replays_can_serialize_them() -> None:
    # psycopg2 returns sum() over bigint as Decimal, which orjson refuses
    for column in ("xact_commit", "xact_rollback", "tup_returned", "tup_fetched", "tup_inserted"):
        assert f"sum({column})::bigint AS {column}" in PostgreSQLQueries.global_stats
    assert "wal_bytes::bigint AS wal_bytes" in PostgreSQLQueries.wal_stats


def test_postgresql_metrics_are_per_second_rates_after_a_baseline_poll() -> None:
    manager = MetricManager(None)
    manager.connection_source = ConnectionSource.postgresql

    for index, (commits, wal_bytes) in enumerate([(100, 1000), (160, 9000), (220, 9000)]):
        manager.refresh_data(
            BASE_TIME + timedelta(seconds=2 * index),
            polling_latency=2,
            global_status={"xact_commit": commits, "wal_bytes": wal_bytes, "Queries": commits * 10},
            system_utilization={"CPU_Percent": 50},
        )

    metrics = manager.metrics
    assert metric_values(metrics.postgresql_transactions.xact_commit) == [30, 30]
    assert metric_values(metrics.postgresql_wal_bytes.wal_bytes) == [4000, 0]
    assert metric_values(metrics.dml.Queries) == [300, 300]
    assert metric_values(metrics.system_cpu.CPU_Percent) == [50, 50]
    # MySQL-only series stay empty for a PostgreSQL host
    assert metric_values(metrics.threads.Threads_connected) == []


def test_only_postgresql_graph_tabs_and_the_system_tab_are_offered_for_postgresql() -> None:
    tab_ids = {tab.id for tab in GRAPH_TABS if ConnectionSource.postgresql in tab.connection_sources}

    assert tab_ids == {"system", "postgresql_transactions", "postgresql_tuples", "postgresql_wal"}


def test_connect_records_backend_pid_and_names_the_application(monkeypatch: pytest.MonkeyPatch) -> None:
    connection = MagicMock(closed=0)
    connection.get_backend_pid.return_value = 4242
    connect = MagicMock(return_value=connection)
    monkeypatch.setattr(psycopg2, "connect", connect)
    database = PostgreSQLDatabase(app=MagicMock(), host="db1", user="postgres", password="secret", port=5432)

    database.connect()

    assert database.source == ConnectionSource.postgresql
    assert database.connection_id == 4242
    assert connect.call_args.kwargs["application_name"] == "Dolphie"
    assert connection.autocommit is True


def test_connect_failure_raises_manual_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(psycopg2, "connect", MagicMock(side_effect=psycopg2.OperationalError("password failed")))
    database = PostgreSQLDatabase(app=MagicMock(), host="db1", user="postgres", password="wrong", port=5432)

    with pytest.raises(ManualException, match="password failed"):
        database.connect()


def test_privilege_error_notifies_once_and_skips_the_query_afterwards() -> None:
    cursor = MagicMock()
    cursor.execute.side_effect = InsufficientPrivilegeError("permission denied for pg_stat_wal")
    database = make_connected_database(cursor)
    app = cast(MagicMock, database.app)

    assert database.execute(PostgreSQLQueries.wal_stats) is None
    assert database.execute(PostgreSQLQueries.wal_stats) is None

    assert cursor.execute.call_count == 1
    assert app.notify.call_count == 1
    assert database.fetchall() == []


def test_query_error_raises_manual_exception_with_sqlstate() -> None:
    cursor = MagicMock()
    cursor.execute.side_effect = UndefinedTableError('relation "pg_stat_wal" does not exist')
    database = make_connected_database(cursor)

    with pytest.raises(ManualException, match="42P01"):
        database.execute(PostgreSQLQueries.wal_stats)


def test_admin_shutdown_reconnects_and_retries_the_query(monkeypatch: pytest.MonkeyPatch) -> None:
    first_cursor = MagicMock()
    first_cursor.execute.side_effect = AdminShutdownError("terminating connection due to administrator command")
    database = make_connected_database(first_cursor)

    new_connection = MagicMock(closed=0)
    new_cursor = new_connection.cursor.return_value
    new_cursor.rowcount = 1
    monkeypatch.setattr(psycopg2, "connect", MagicMock(return_value=new_connection))

    assert database.execute(PostgreSQLQueries.server_state) == 1
    new_cursor.execute.assert_called_once_with(PostgreSQLQueries.server_state, None)


def test_fetch_decodes_values_that_are_not_database_scalars() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [{"pid": 1, "client_addr": ["10.0.0.1"], "lag": Decimal("1.5"), "state": None}]
    database = make_connected_database(cursor)
    database.execute("SELECT 1")

    assert database.fetchall() == [{"pid": 1, "client_addr": "['10.0.0.1']", "lag": Decimal("1.5"), "state": None}]


@pytest.mark.parametrize(("server_version_num", "expect_wal"), [(140000, True), (130012, False)])
def test_wal_stats_are_only_queried_on_postgresql_14_and_later(server_version_num: int, expect_wal: bool) -> None:
    database = QueryDatabase(server_rows(server_version_num))
    tab = make_tab(database)

    WorkerDataProcessor(MagicMock()).process_postgresql_data(tab)

    assert (PostgreSQLQueries.wal_stats in database.queries) is expect_wal
    assert ("wal_records" in tab.dolphie.global_status) is expect_wal


def test_poll_collects_status_as_integers_and_derives_queries() -> None:
    database = QueryDatabase(server_rows(160004))
    tab = make_tab(database)

    WorkerDataProcessor(MagicMock()).process_postgresql_data(tab)

    dolphie = tab.dolphie
    assert dolphie.host_version == "16.4"
    assert dolphie.global_variables["server_version_num"] == "160004"
    assert dolphie.global_status["xact_commit"] == 100
    assert dolphie.global_status["Uptime"] == 3600
    assert dolphie.global_status["in_recovery"] == 0
    # The sum of tuple operations stands in for MySQL's Queries counter
    assert dolphie.global_status["Queries"] == 10 + 20 + 3 + 4 + 5
    # Standbys are always fetched so key 4 knows whether the replication panel has data
    assert dolphie.postgresql_replication == [{"host": "10.0.0.2", "application_name": "standby1", "replay_lag": 0}]


def test_panel_data_is_only_fetched_for_visible_panels() -> None:
    hidden = QueryDatabase(server_rows(160004))
    WorkerDataProcessor(MagicMock()).process_postgresql_data(make_tab(hidden))

    visible = QueryDatabase(server_rows(160004))
    WorkerDataProcessor(MagicMock()).process_postgresql_data(make_tab(visible, "dashboard", "processlist"))

    assert PostgreSQLQueries.table_health not in hidden.queries
    assert not any(query.startswith(PostgreSQLQueries.activity.split("$1")[0]) for query in hidden.queries)
    assert PostgreSQLQueries.table_health in visible.queries
    assert any(query.startswith(PostgreSQLQueries.activity.split("$1")[0]) for query in visible.queries)
