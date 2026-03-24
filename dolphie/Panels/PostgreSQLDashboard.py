from datetime import datetime, timedelta

from rich.style import Style
from rich.table import Table

from dolphie.Modules.Functions import format_bytes, format_number
from dolphie.Modules.MetricManager import MetricData
from dolphie.Modules.TabManager import Tab


def format_percent(value: float) -> str:
    """Format a percentage value with color."""
    if value >= 80:
        return f"[red]{value}%[/red]"
    elif value >= 60:
        return f"[yellow]{value}%[/yellow]"
    else:
        return f"[green]{value}%[/green]"


def create_system_utilization_table(tab: Tab) -> Table:
    """Create a system utilization metrics table (CPU, Memory, Disk I/O)."""
    try:
        dolphie = tab.dolphie
        system_utilization = dolphie.system_utilization or {}

        table_title_style = Style(color="#bbc8e8", bold=True)
        table = Table(show_header=False, box=None, title="System Utilization", title_style=table_title_style)
        table.add_column()
        table.add_column(min_width=20)

        # CPU - prefer system_utilization dict first since it gets collected for localhost
        cpu_percent = system_utilization.get("CPU_Percent")
        if cpu_percent is not None:
            try:
                formatted_cpu_percent = format_percent(float(cpu_percent))
                cpu_cores = system_utilization.get("CPU_Count", "N/A")
                table.add_row("[label]CPU%", f"{formatted_cpu_percent} [label]cores[/label] {cpu_cores}")
            except (ValueError, TypeError):
                table.add_row("[label]CPU%", "[dim]N/A[/dim]")
        else:
            table.add_row("[label]CPU%", "[dim]Not available[/dim]")

        # CPU Load Average
        load_averages = system_utilization.get("CPU_Load_Avg")
        if load_averages:
            try:
                formatted_load = " ".join(f"{float(avg):.2f}" for avg in load_averages)
                table.add_row("[label]Load", formatted_load)
            except (ValueError, TypeError):
                pass

        # Memory
        mem_used = system_utilization.get("Memory_Used")
        mem_total = system_utilization.get("Memory_Total")
        if mem_used is not None and mem_total is not None:
            try:
                memory_percent_used = round((float(mem_used) / float(mem_total)) * 100, 2)
                formatted_memory_percent_used = format_percent(memory_percent_used)
                table.add_row(
                    "[label]Memory",
                    (
                        f"{formatted_memory_percent_used}\n{format_bytes(int(mem_used))}"
                        f"[dark_gray]/[/dark_gray]{format_bytes(int(mem_total))}"
                    ),
                )
            except (ValueError, TypeError, ZeroDivisionError):
                table.add_row("[label]Memory", "[dim]N/A[/dim]\n")
        else:
            table.add_row("[label]Memory", "[dim]Not available[/dim]\n")

        # Swap
        swap_used = system_utilization.get("Swap_Used")
        swap_total = system_utilization.get("Swap_Total")
        if swap_used is not None and swap_total is not None:
            try:
                table.add_row(
                    "[label]Swap",
                    f"{format_bytes(int(swap_used))}[dark_gray]/[/dark_gray]{format_bytes(int(swap_total))}",
                )
            except (ValueError, TypeError):
                table.add_row("[label]Swap", "[dim]N/A[/dim]")
        else:
            table.add_row("[label]Swap", "[dim]Not available[/dim]")

        # Disk I/O
        disk_read = system_utilization.get("Disk_Read")
        disk_write = system_utilization.get("Disk_Write")
        if disk_read is not None and disk_write is not None:
            try:
                disk_info = (
                    f"[label]R[/label] {format_number(int(disk_read))}\n"
                    f"[label]W[/label] {format_number(int(disk_write))}"
                )
                table.add_row("[label]Disk", disk_info)
            except (ValueError, TypeError):
                table.add_row("[label]Disk", "[dim]N/A[/dim]\n")
        else:
            table.add_row("[label]Disk", "[dim]Not available[/dim]\n")

        return table
    except Exception as e:
        # If anything goes wrong, return a minimal table
        table_title_style = Style(color="#bbc8e8", bold=True)
        table = Table(show_header=False, box=None, title="System Utilization", title_style=table_title_style)
        table.add_column()
        table.add_column(min_width=20)
        table.add_row("[label]Error", f"[red]{str(e)[:50]}[/red]")
        return table


def create_panel(tab: Tab) -> Table:
    dolphie = tab.dolphie

    # We expect dolphie.metric_manager to be PostgreSQLMetricManager
    # accessing attributes like global_status is fine if we populated them
    global_status = dolphie.global_status

    table_title_style = Style(color="#bbc8e8", bold=True)

    ####################
    # Host Information #
    ####################
    table_information = Table(
        show_header=False,
        box=None,
        title=f"{dolphie.panels.get_key(dolphie.panels.dashboard.name)}Host Information",
        title_style=table_title_style,
    )

    table_information.add_column()
    table_information.add_column(min_width=25, max_width=35)

    server_version = global_status.get("server_version", "N/A")
    table_information.add_row("[label]Version", server_version)
    table_information.add_row("[label]Type", "PostgreSQL")

    uptime = global_status.get("Uptime")
    if uptime:
        table_information.add_row("[label]Uptime", str(timedelta(seconds=int(uptime))).split(".")[0])

    # Connections info (could be added from pg_stat_activity if needed)
    active_connections = global_status.get("active_connections")
    idle_connections = global_status.get("idle_connections")
    total_connections = global_status.get("total_connections")

    if total_connections:
        conn_str = f"[label]active[/label] {format_number(active_connections or 0)}"
        conn_str += f"[highlight]/[/highlight][label]idle[/label] {format_number(idle_connections or 0)}"
        table_information.add_row("[label]Connections", conn_str)

    if not dolphie.replay_file:
        runtime = str(datetime.now().astimezone() - dolphie.dolphie_start_time).split(".")[0]
        table_information.add_row(
            "[label]Runtime",
            f"{runtime} [label]Latency[/label] {round(dolphie.worker_processing_time, 2)}s",
        )
    else:
        if dolphie.worker_processing_time:
            table_information.add_row("[label]Latency", f"{round(dolphie.worker_processing_time, 2)}s")

    tab.dashboard_section_1.update(table_information)

    #################
    # Transactions  #
    #################
    # Replaces "Statistics/s"
    table_trans = Table(show_header=False, box=None, title="Transactions/s", title_style=table_title_style)
    table_trans.add_column()
    table_trans.add_column(min_width=6)

    metrics = dolphie.metric_manager.metrics.transactions

    xact_commit = getattr(metrics, "xact_commit", None)
    if xact_commit and xact_commit.values:
        table_trans.add_row("[label]Commits", format_number(xact_commit.values[-1]))

    xact_rollback = getattr(metrics, "xact_rollback", None)
    if xact_rollback and xact_rollback.values:
        table_trans.add_row("[label]Rollbacks", format_number(xact_rollback.values[-1]))

    tab.dashboard_section_4.update(table_trans)

    ##################
    # Tuple Activity #
    ##################
    table_tuples = Table(show_header=False, box=None, title="Tuple Activity/s", title_style=table_title_style)
    table_tuples.add_column()
    table_tuples.add_column(min_width=6)

    metrics = dolphie.metric_manager.metrics.tuples
    # map label -> metric_name
    tuple_map = {
        "Fetched": "tup_fetched",
        "Returned": "tup_returned",
        "Inserted": "tup_inserted",
        "Updated": "tup_updated",
        "Deleted": "tup_deleted",
    }

    for label, name in tuple_map.items():
        m: MetricData = getattr(metrics, name, None)
        if m and m.values:
            table_tuples.add_row(f"[label]{label}", format_number(m.values[-1]))

    tab.dashboard_section_2.update(table_tuples)

    ################
    # WAL Activity #
    ################
    table_wal = Table(show_header=False, box=None, title="WAL Activity/s", title_style=table_title_style)
    table_wal.add_column()
    table_wal.add_column(min_width=6)

    metrics = dolphie.metric_manager.metrics.wal

    m_bytes: MetricData = getattr(metrics, "wal_bytes", None)
    if m_bytes and m_bytes.values:
        table_wal.add_row("[label]Bytes", format_bytes(m_bytes.values[-1]))

    m_fpi: MetricData = getattr(metrics, "wal_fpi", None)
    if m_fpi and m_fpi.values:
        table_wal.add_row("[label]FPI", format_number(m_fpi.values[-1]))

    m_recs: MetricData = getattr(metrics, "wal_records", None)
    if m_recs and m_recs.values:
        table_wal.add_row("[label]Records", format_number(m_recs.values[-1]))

    tab.dashboard_section_3.display = True
    tab.dashboard_section_3.update(table_wal)

    ###############################
    # Table Health (Dead Tuples)  #
    ###############################
    table_health = Table(show_header=False, box=None, title="Table Health (Dead Tuples)", title_style=table_title_style)
    table_health.add_column()
    table_health.add_column(min_width=6)

    health_data = getattr(dolphie, "postgresql_table_health", [])
    if health_data:
        # Data is already sorted by n_dead_tup DESC from the query
        # We show the top 5 tables with most dead tuples
        for row in health_data[:5]:
            # Truncate table name if too long
            table_name = str(row.get("table", "Unknown"))
            if len(table_name) > 20:
                table_name = table_name[:17] + "..."

            dead_tup = int(row.get("n_dead_tup") or 0)
            table_health.add_row(f"[label]{table_name}", format_number(dead_tup))
    else:
        table_health.add_row("[dim]No Top Tables[/dim]", "")

    tab.dashboard_section_5.display = True
    tab.dashboard_section_5.update(table_health)

    #####################
    # System Utilization #
    #####################
    system_util_table = create_system_utilization_table(tab)
    if system_util_table:
        tab.dashboard_section_6.update(system_util_table)
