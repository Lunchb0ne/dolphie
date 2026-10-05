from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import Any, cast

from rich.console import Console

from dolphie.DataTypes import ConnectionSource, Panels
from dolphie.Modules.MetricManager import MetricManager
from dolphie.Modules.TabManager import Tab
from dolphie.Panels.PostgreSQLDashboard import create_panel


class FakeSection:
    def __init__(self) -> None:
        self.display = False
        self.text = ""

    def update(self, renderable: object) -> None:
        console = Console(width=60, record=True)
        console.print(renderable)
        self.text = console.export_text()


def make_tab(global_status: dict[str, Any], table_health: list[dict[str, Any]]) -> Tab:
    metric_manager = MetricManager(None)
    metric_manager.connection_source = ConnectionSource.postgresql
    dolphie = SimpleNamespace(
        global_status=global_status,
        metric_manager=metric_manager,
        panels=Panels(),
        host_distro=ConnectionSource.postgresql,
        host_version="16.4",
        replay_file=None,
        dolphie_start_time=datetime.now().astimezone(),
        worker_processing_time=0.05,
        postgresql_table_health=table_health,
        system_utilization={},
    )
    sections = {f"dashboard_section_{index}": FakeSection() for index in range(1, 7)}
    return cast(Tab, SimpleNamespace(dolphie=dolphie, **sections))


def test_dashboard_shows_role_and_dead_tuples() -> None:
    tab = make_tab(
        {"Uptime": 90061, "in_recovery": 1, "total_connections": 12, "active_connections": 3, "wal_records": 0},
        [{"table": "orders", "n_dead_tup": 1500}],
    )

    create_panel(tab)

    host_information = cast(FakeSection, tab.dashboard_section_1).text
    assert "PostgreSQL 16.4" in host_information
    assert "Standby" in host_information
    assert "1 day, 1:01:01" in host_information
    assert "orders" in cast(FakeSection, tab.dashboard_section_5).text
    assert "Bytes" in cast(FakeSection, tab.dashboard_section_3).text


def test_dashboard_explains_missing_wal_stats_before_postgresql_14() -> None:
    tab = make_tab({"Uptime": 10, "in_recovery": 0}, [])

    create_panel(tab)

    assert "Primary" in cast(FakeSection, tab.dashboard_section_1).text
    assert "PostgreSQL 14+" in cast(FakeSection, tab.dashboard_section_3).text
    assert "No tables" in cast(FakeSection, tab.dashboard_section_5).text
