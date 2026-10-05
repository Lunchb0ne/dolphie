from __future__ import annotations

from types import SimpleNamespace
from typing import cast

from rich.console import Console

from dolphie.DataTypes import DatabaseRow, Panels
from dolphie.Modules.TabManager import Tab
from dolphie.Modules.Theme import ThemedTable
from dolphie.Panels.PostgreSQLReplication import create_panel


class FakeStatic:
    def __init__(self) -> None:
        self.display = True
        self.parent = SimpleNamespace(display=False)
        self.renderable: object = None

    def update(self, renderable: object) -> None:
        self.renderable = renderable


def make_tab(standbys: list[DatabaseRow]) -> Tab:
    dolphie = SimpleNamespace(
        panels=Panels(),
        postgresql_replication=standbys,
        get_hostname=lambda host: {"10.0.0.2": "standby1.example.com"}.get(host, host),
    )
    containers = {
        name: SimpleNamespace(display=True)
        for name in (
            "replication_container",
            "replication_thread_applier_container",
            "replication_status_grid",
            "clusterset_container",
            "galera_container",
            "group_replication_container",
            "replicas_container",
        )
    }
    return cast(Tab, SimpleNamespace(dolphie=dolphie, replication_status_single=FakeStatic(), **containers))


def render(renderable: object) -> str:
    console = Console(width=120, record=True)
    console.print(renderable)
    return console.export_text()


def test_panel_hides_when_there_are_no_standbys() -> None:
    tab = make_tab([])

    create_panel(tab)

    assert tab.replication_container.display is False
    # MySQL-only parts of the shared replication panel never show for PostgreSQL
    assert tab.galera_container.display is False
    assert tab.replicas_container.display is False


def test_panel_lists_each_standby_with_its_lag() -> None:
    tab = make_tab(
        [
            {
                "host": "10.0.0.2",
                "application_name": "walreceiver",
                "state": "streaming",
                "sync_state": "async",
                "write_lag": 0,
                "flush_lag": 2048,
                "replay_lag": None,
            },
            {
                "host": None,
                "application_name": "pg_basebackup",
                "state": "backup",
                "sync_state": "async",
                "write_lag": 0,
                "flush_lag": 0,
                "replay_lag": 0,
            },
        ]
    )

    create_panel(tab)

    single = cast(FakeStatic, tab.replication_status_single)
    assert tab.replication_container.display is True
    assert single.parent.display is True
    assert isinstance(single.renderable, ThemedTable)
    output = render(single.renderable)
    assert "Standbys (2)" in output
    assert "standby1.example.com" in output
    assert "2KB" in output
    # A standby that hasn't reported a replay position yet and a Unix socket client
    assert "N/A" in output
    assert "local" in output
