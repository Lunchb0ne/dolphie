from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any, cast

from dolphie.DataTypes import AnyProcesslistThread, DatabaseRow, PostgreSQLProcesslistThread
from dolphie.Modules.Queries import PostgreSQLQueries
from dolphie.Modules.TabManager import Tab
from dolphie.Panels.PostgreSQLProcesslist import create_panel, fetch_data


class ActivityDatabase:
    def __init__(self, rows: list[DatabaseRow], connection_id: int | None = None) -> None:
        self.rows = rows
        self.connection_id = connection_id
        self.queries: list[str] = []

    def execute(self, query: str) -> None:
        self.queries.append(query)

    def fetchall(self) -> list[DatabaseRow]:
        return [dict(row) for row in self.rows]


class FakeDataTable:
    def __init__(self) -> None:
        self.columns: dict[str, object] = {}
        self.rows: dict[str, list[object]] = {}
        self.sorted_by: tuple[str, bool] | None = None

    @property
    def row_count(self) -> int:
        return len(self.rows)

    def add_column(self, label: object, *, key: str, width: object) -> None:
        self.columns[key] = label

    def add_row(self, *values: object, key: str) -> None:
        self.rows[key] = list(values)

    @staticmethod
    def normalize_cells(cells: list[object]) -> list[object]:
        return cells

    def get_row(self, key: str) -> list[object]:
        return self.rows[key]

    def update_cell(self, row_key: str, column_key: str, value: object, update_width: bool = False) -> None:
        self.rows[row_key][list(self.columns).index(column_key)] = value

    def clear(self, columns: bool = False) -> None:
        self.rows.clear()
        if columns:
            self.columns.clear()

    def remove_row(self, key: str) -> None:
        self.rows.pop(key)

    def sort(self, column: str, reverse: bool = False) -> None:
        self.sorted_by = (column, reverse)


def backend(pid: int, **overrides: Any) -> DatabaseRow:
    row: DatabaseRow = {
        "id": pid,
        "user": "app",
        "db": "orders",
        "host": "10.0.0.5",
        "state": "active",
        "time": 3,
        "wait_event": None,
        "query": "SELECT 1",
    }
    row.update(overrides)
    return row


def make_tab(
    rows: list[DatabaseRow],
    *,
    main_connection_id: int | None = None,
    secondary_connection_id: int | None = None,
    **dolphie_overrides: Any,
) -> Tab:
    dolphie = SimpleNamespace(
        main_db_connection=ActivityDatabase(rows, main_connection_id),
        secondary_db_connection=SimpleNamespace(connection_id=secondary_connection_id),
        app=SimpleNamespace(batch_update=nullcontext),
        panels=SimpleNamespace(processlist=SimpleNamespace(title="Processlist")),
        processlist_threads={},
        replay_file=None,
        show_idle_threads=False,
        sort_by_time_descending=True,
        user_filter=None,
        db_filter=None,
        host_filter=None,
        query_filter=None,
        query_time_filter=None,
        get_hostname=lambda host: host,
        record_filter_dropdown_values=lambda: None,
    )
    for key, value in dolphie_overrides.items():
        setattr(dolphie, key, value)

    title = SimpleNamespace(update=lambda value: None)
    return cast(Tab, SimpleNamespace(dolphie=dolphie, processlist_datatable=FakeDataTable(), processlist_title=title))


def test_idle_backends_are_filtered_in_sql_unless_shown() -> None:
    hidden = ActivityDatabase([])
    fetch_data(make_tab([], main_db_connection=hidden))
    shown = ActivityDatabase([])
    fetch_data(make_tab([], main_db_connection=shown, show_idle_threads=True))

    assert hidden.queries == [PostgreSQLQueries.activity.replace("$1", "state IS DISTINCT FROM 'idle'")]
    assert shown.queries == [PostgreSQLQueries.activity.replace("$1", "TRUE")]


def test_dolphies_own_backends_are_left_out() -> None:
    tab = make_tab([backend(1), backend(2), backend(3)], main_connection_id=1, secondary_connection_id=2)

    assert list(fetch_data(tab)) == [3]


def test_filters_are_applied_to_live_backends() -> None:
    rows = [
        backend(1, user="app", time=1),
        backend(2, user="batch", time=30, query="VACUUM orders"),
        backend(3, user="app", time=30, host=None),
    ]

    assert list(fetch_data(make_tab(rows, user_filter="!batch"))) == [1, 3]
    assert list(fetch_data(make_tab(rows, query_time_filter=10))) == [2, 3]
    assert list(fetch_data(make_tab(rows, query_filter="VACUUM"))) == [2]
    # Host filters match the raw address, so a Unix socket backend with no address never matches
    assert list(fetch_data(make_tab(rows, host_filter="10.0.0"))) == [1, 2]


def test_panel_renders_rows_and_drops_backends_that_ended() -> None:
    tab = make_tab([])
    datatable = cast(FakeDataTable, tab.processlist_datatable)

    tab.dolphie.processlist_threads = {
        1: PostgreSQLProcesslistThread(backend(1)),
        2: PostgreSQLProcesslistThread(backend(2)),
    }
    create_panel(tab)
    assert set(datatable.rows) == {"1", "2"}
    assert datatable.sorted_by == ("time_seconds", True)

    tab.dolphie.processlist_threads = {2: PostgreSQLProcesslistThread(backend(2, state="idle in transaction"))}
    create_panel(tab)
    assert set(datatable.rows) == {"2"}
    assert "idle in transaction" in datatable.rows["2"]


def test_replay_applies_filters_when_rendering() -> None:
    threads: dict[int, AnyProcesslistThread] = {
        1: PostgreSQLProcesslistThread(backend(1, user="app")),
        2: PostgreSQLProcesslistThread(backend(2, user="batch")),
    }
    tab = make_tab([], replay_file="replay.db", processlist_threads=threads, user_filter="batch")

    create_panel(tab)

    assert set(cast(FakeDataTable, tab.processlist_datatable).rows) == {"2"}
    assert list(tab.dolphie.processlist_threads) == [2]
