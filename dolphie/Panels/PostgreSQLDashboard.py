from __future__ import annotations

from datetime import datetime, timedelta

from dolphie.Modules.Functions import coerce_int, coerce_str, format_bytes, format_number
from dolphie.Modules.MetricDefinitions import MetricData
from dolphie.Modules.TabManager import Tab
from dolphie.Modules.Theme import ThemedTable as Table
from dolphie.Panels.Dashboard import create_system_utilization_table


def _add_rate_rows(table: Table, rows: dict[str, MetricData], format_bytes_value: bool = False) -> None:
    for label, metric_data in rows.items():
        latest_value = metric_data.latest_value()
        if latest_value is None:
            table.add_row(f"[$label]{label}", "0")
        else:
            table.add_row(
                f"[$label]{label}", format_bytes(latest_value) if format_bytes_value else format_number(latest_value)
            )


def create_panel(tab: Tab) -> None:
    dolphie = tab.dolphie

    global_status = dolphie.global_status
    metrics = dolphie.metric_manager.metrics

    table_title_style = "b_light_blue"

    ####################
    # Host Information #
    ####################
    table = Table(
        show_header=False,
        box=None,
        title=f"{dolphie.panels.dashboard.formatted_key}Host Information",
        title_style=table_title_style,
    )

    table.add_column()
    table.add_column(min_width=15)
    table.add_row("[$label]Version", f"{dolphie.host_distro} {dolphie.host_version}")
    table.add_row("[$label]Role", "Standby" if coerce_int(global_status.get("in_recovery")) else "Primary")
    table.add_row("[$label]Uptime", str(timedelta(seconds=coerce_int(global_status.get("Uptime")))))
    table.add_row(
        "[$label]Connections",
        (
            f"{format_number(coerce_int(global_status.get('total_connections')))} "
            f"[$label]active[/$label] {format_number(coerce_int(global_status.get('active_connections')))}"
        ),
    )
    if not dolphie.replay_file:
        runtime = str(datetime.now().astimezone() - dolphie.dolphie_start_time).split(".")[0]
        table.add_row("[$label]Runtime", runtime)

    if dolphie.worker_processing_time:
        table.add_row("[$label]Latency", f"{round(dolphie.worker_processing_time, 2)}s")

    tab.dashboard_section_1.update(table)

    ##################
    # Tuple Activity #
    ##################
    table = Table(show_header=False, box=None, title="Tuples/s", title_style=table_title_style)
    table.add_column()
    table.add_column(min_width=7)

    tuples = metrics.postgresql_tuples
    _add_rate_rows(
        table,
        {
            "Fetched": tuples.tup_fetched,
            "Returned": tuples.tup_returned,
            "Inserted": tuples.tup_inserted,
            "Updated": tuples.tup_updated,
            "Deleted": tuples.tup_deleted,
        },
    )

    tab.dashboard_section_2.update(table)

    ###############
    # WAL Activity #
    ###############
    table = Table(show_header=False, box=None, title="WAL/s", title_style=table_title_style)
    table.add_column()
    table.add_column(min_width=9)

    # pg_stat_wal only exists on PostgreSQL 14+
    if "wal_records" in global_status:
        _add_rate_rows(
            table,
            {"Records": metrics.postgresql_wal_records.wal_records, "FPI": metrics.postgresql_wal_records.wal_fpi},
        )
        _add_rate_rows(table, {"Bytes": metrics.postgresql_wal_bytes.wal_bytes}, format_bytes_value=True)
    else:
        table.add_row("[$label]Requires", "PostgreSQL 14+")

    tab.dashboard_section_3.update(table)

    ################
    # Transactions #
    ################
    table = Table(show_header=False, box=None, title="Transactions/s", title_style=table_title_style)
    table.add_column()
    table.add_column(min_width=7)

    transactions = metrics.postgresql_transactions
    _add_rate_rows(table, {"Commits": transactions.xact_commit, "Rollbacks": transactions.xact_rollback})
    table.add_row("[$label]Deadlocks", format_number(coerce_int(global_status.get("deadlocks"))))

    tab.dashboard_section_4.update(table)

    ###############
    # Dead Tuples #
    ###############
    table = Table(show_header=False, box=None, title="Dead Tuples", title_style=table_title_style)
    table.add_column(max_width=20, overflow="ellipsis", no_wrap=True)
    table.add_column(min_width=7)

    # Rows are sorted by n_dead_tup by the query
    for row in dolphie.postgresql_table_health[:5]:
        table.add_row(f"[$label]{coerce_str(row.get('table'))}", format_number(coerce_int(row.get("n_dead_tup"))))

    if not dolphie.postgresql_table_health:
        table.add_row("[$dark_gray]No tables", "")

    tab.dashboard_section_5.display = True
    tab.dashboard_section_5.update(table)

    ######################
    # System Utilization #
    ######################
    table = create_system_utilization_table(tab)

    if table:
        tab.dashboard_section_6.update(table)
