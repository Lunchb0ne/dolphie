from __future__ import annotations

from dolphie.Modules.Functions import coerce_int, coerce_str, format_bytes
from dolphie.Modules.TabManager import Tab
from dolphie.Modules.Theme import ThemedTable as Table


def _format_lag(value: object) -> str:
    # NULL until the standby has reported that position
    if value is None:
        return "[$dark_gray]N/A"
    lag = coerce_int(value)
    return "[$dark_gray]0" if lag == 0 else format_bytes(lag)


def create_panel(tab: Tab) -> None:
    dolphie = tab.dolphie

    # The replication panel's widgets are shared with MySQL; only the single status view is used
    tab.replication_thread_applier_container.display = False
    tab.replication_status_grid.display = False
    for container in (
        tab.clusterset_container,
        tab.galera_container,
        tab.group_replication_container,
        tab.replicas_container,
    ):
        container.display = False

    if not dolphie.postgresql_replication:
        tab.replication_container.display = False
        return

    tab.replication_container.display = True
    single_parent = tab.replication_status_single.parent
    if single_parent is not None:
        single_parent.display = True

    standby_count = len(dolphie.postgresql_replication)
    table = Table(
        title=f"{dolphie.panels.replication.formatted_key}Standbys ([$highlight]{standby_count}[/$highlight])",
        title_style="b_light_blue",
        box=None,
        header_style="label",
    )
    # The container is sized for MySQL's single replication status, so only what identifies a standby fits
    table.add_column("Host", no_wrap=True)
    table.add_column("Application", no_wrap=True)
    table.add_column("State", no_wrap=True)
    table.add_column("Sync", no_wrap=True)
    table.add_column("Write Lag", no_wrap=True)
    table.add_column("Flush Lag", no_wrap=True)
    table.add_column("Replay Lag", no_wrap=True)

    for row in dolphie.postgresql_replication:
        state = coerce_str(row.get("state"))
        table.add_row(
            dolphie.get_hostname(coerce_str(row.get("host"))) if row.get("host") else "[$dark_gray]local",
            coerce_str(row.get("application_name")),
            f"[$green]{state}" if state == "streaming" else f"[$yellow]{state}",
            coerce_str(row.get("sync_state")),
            _format_lag(row.get("write_lag")),
            _format_lag(row.get("flush_lag")),
            _format_lag(row.get("replay_lag")),
        )

    tab.replication_status_single.update(table)
