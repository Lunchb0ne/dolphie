import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import psycopg2
from loguru import logger
from psycopg2.extras import RealDictCursor

from dolphie.DataTypes import ConnectionSource, ConnectionStatus
from dolphie.Modules.Functions import format_query, format_time
from dolphie.Modules.ManualException import ManualException
from dolphie.Modules.MetricManager import (
    MetricColor,
    MetricData,
    MetricSource,
    SystemCPUMetrics,
    SystemDiskIOMetrics,
    SystemMemoryMetrics,
    SystemNetworkMetrics,
)
from dolphie.Modules.Queries import PostgreSQLQueries

if TYPE_CHECKING:
    from dolphie.App import DolphieApp
    from dolphie.Modules.TabManager import Tab


class PostgreSQLProcesslistThread:
    def __init__(self, thread_data: dict[str, str]):
        # Store raw thread data for replay serialization
        self.thread_data = thread_data

        # Map PostgreSQL column names to standard names
        self.id = str(thread_data.get("pid", thread_data.get("id", "")))
        self.user = thread_data.get("user", "")
        self.db = thread_data.get("db", "")
        self.host = thread_data.get("host", "")
        self.state = thread_data.get("state", "")
        self.time = int(thread_data.get("time") or 0)
        self.wait_event = thread_data.get("wait_event", "")
        self.query = thread_data.get("query", "")
        self.command = self.state

        self.formatted_query = self._get_formatted_query(self.query)
        self.formatted_time = self._get_formatted_time()

    def _get_formatted_query(self, query: str):
        return format_query(query)

    def _get_formatted_time(self) -> str:
        thread_color = self._get_time_color()
        return f"[{thread_color}]{format_time(self.time)}[/{thread_color}]" if thread_color else format_time(self.time)

    def _get_time_color(self) -> str:
        thread_color = ""
        if self.formatted_query.code:
            if self.time >= 10:
                thread_color = "red"
            elif self.time >= 5:
                thread_color = "yellow"
            else:
                thread_color = "green"
        return thread_color


class PostgreSQLDatabase:
    def __init__(
        self,
        app,
        host: str,
        user: str,
        password: str,
        port: int,
        dbname: str = "postgres",
        daemon_mode: bool = False,
        **kwargs,
    ):
        self.app = app
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.dbname = dbname
        self.connection = None
        self.source = ConnectionSource.postgresql
        self.server_version: str = ""
        self.server_version_major: int = 0
        self.daemon_mode = daemon_mode

        # Connection options
        self.connect_timeout: int = 3
        self.is_running_query: bool = False
        self.last_execute_successful: bool = False
        self.privilege_errors_notified: set = set()

        if daemon_mode:
            self.max_reconnect_attempts: int = 999999999
        else:
            self.max_reconnect_attempts: int = 3

    def parse_server_version(self, version_string: str) -> tuple[str, int]:
        """Parse PostgreSQL version string and return (version_str, major_version_int).

        Examples:
            '14.5' -> ('14.5', 14)
            '15.2 (Ubuntu 15.2-1.pgdg22.04+1)' -> ('15.2', 15)
        """
        if not version_string:
            return ("", 0)

        # Extract just the version number (first part before space or parenthesis)
        version_part = version_string.split()[0].split("(")[0].strip()
        try:
            major_version = int(version_part.split(".")[0])
        except (ValueError, IndexError):
            major_version = 0

        return (version_part, major_version)

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port,
                dbname=self.dbname,
                connect_timeout=self.connect_timeout,
                cursor_factory=RealDictCursor,
            )
            self.connection.autocommit = True
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL Connection Error: {e}")
            self.connection = None
            raise

    def is_connected(self):
        return bool(self.connection and self.connection.closed == 0)

    def close(self):
        if self.connection:
            self.connection.close()

    def _get_cursor(self):
        if not hasattr(self, "_cursor") or self._cursor.closed:
            self._cursor = self.connection.cursor()
        return self._cursor

    def execute(self, query: str, args=None, ignore_error=False):
        if not self.is_connected():
            self.last_execute_successful = False
            return None

        if self.is_running_query:
            # Prevent multiple queries from running simultaneously on the same connection
            self.app.notify(
                "Another query is already running, please repeat action",
                title="Unable to run multiple queries at the same time",
                severity="error",
                timeout=10,
            )
            self.last_execute_successful = False
            return None

        # Check if this query has already failed with a privilege error skip execution
        if query in self.privilege_errors_notified:
            self.last_execute_successful = False
            return None

        for attempt_number in range(self.max_reconnect_attempts):
            self.is_running_query = True

            try:
                cursor = self._get_cursor()
                cursor.execute(query, args)
                self.is_running_query = False
                self.last_execute_successful = True
                return cursor

            except psycopg2.Error as e:
                self.is_running_query = False
                self.last_execute_successful = False

                error_code = e.pgcode
                error_message = str(e).strip()

                # Handle privilege errors (42501)
                # We show a notification once and then skip this query in future
                if error_code == "42501":  # insufficient_privilege
                    if query not in self.privilege_errors_notified:
                        self.privilege_errors_notified.add(query)

                        logger.warning(
                            f"Privilege error (code {error_code}): {error_message}. "
                            f"Query: {query}. "
                            f"This query will be skipped and stats for this feature won't be available."
                        )

                        self.app.notify(
                            f"[$b_highlight]{self.host}:{self.port}[/$b_highlight]: [dim]{error_code}: "
                            f"{error_message}[/dim]\nQuery: [$b_light_blue]{query}[/$b_light_blue]\n"
                            "Stats for this feature won't be available.",
                            title="Insufficient Privileges",
                            severity="warning",
                            timeout=9,
                        )
                    return None

                if ignore_error:
                    return None

                # Handle Connection Errors
                # 57P01: admin_shutdown
                # 57P02: crash_shutdown
                # 57P03: cannot_connect_now
                # 08***: Connection exceptions
                if error_code in ("57P01", "57P02", "57P03") or (error_code and error_code.startswith("08")):
                    logger.error(f"PostgreSQL Connection Lost: {error_message}, attempting to reconnect...")

                    self.app.notify(
                        f"[$b_light_blue]{self.host}:{self.port}[/$b_light_blue]: {error_message}",
                        title="PostgreSQL Connection Lost",
                        severity="error",
                        timeout=10,
                    )

                    self.close()
                    try:
                        self.connect()
                        time.sleep(min(1 * (2**attempt_number), 20))
                    except Exception:
                        pass  # Loop will try again

                    if not self.is_connected():
                        continue

                    self.app.notify(
                        f"[$b_light_blue]{self.host}:{self.port}[/$b_light_blue]: Successfully reconnected",
                        title="PostgreSQL Connection Created",
                        severity="success",
                        timeout=10,
                    )

                    # Retry the query on the new connection
                    continue

                # Other errors
                logger.error(f"Query Error: {e}")
                return None

        if not self.is_connected():
            raise ManualException(f"Failed to reconnect to PostgreSQL after {self.max_reconnect_attempts} attempts")

    def fetchall(self):
        if not self.is_connected() or not self.last_execute_successful:
            return []
        try:
            return self._cursor.fetchall()
        except psycopg2.Error:
            return []

    def fetchone(self):
        if not self.is_connected() or not self.last_execute_successful:
            return None
        try:
            return self._cursor.fetchone()
        except psycopg2.Error:
            return None

    def fetch_value_from_field(self, query, field):
        self.execute(query)
        res = self.fetchone()
        return res.get(field) if res else None


# Metric Definitions


@dataclass
class DMLMetrics:
    """Equivalent to MySQL DML metrics for the sparkline."""

    Queries: MetricData
    graphs: list[str] = field(default_factory=lambda: ["graph_dml"])
    tab_name: str = "dml"
    graph_tab_name = "DML"
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.postgresql])
    use_with_replay: bool = True


@dataclass
class TransactionMetrics:
    xact_commit: MetricData
    xact_rollback: MetricData
    graphs: list[str]
    tab_name: str = "transactions"
    graph_tab_name = "Transactions"
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.postgresql])
    use_with_replay: bool = True


@dataclass
class TupleMetrics:
    tup_fetched: MetricData
    tup_returned: MetricData
    tup_inserted: MetricData
    tup_updated: MetricData
    tup_deleted: MetricData
    graphs: list[str]
    tab_name: str = "tuples"
    graph_tab_name = "Tuples"
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.postgresql])
    use_with_replay: bool = True


@dataclass
class WALMetrics:
    wal_records: MetricData
    wal_fpi: MetricData
    wal_bytes: MetricData
    graphs: list[str]
    tab_name: str = "wal"
    graph_tab_name = "WAL"
    metric_source: MetricSource = MetricSource.GLOBAL_STATUS  # We'll merge WAL stats into global_status dict
    connection_source: list[ConnectionSource] = field(default_factory=lambda: [ConnectionSource.postgresql])
    use_with_replay: bool = True


@dataclass
class PostgreSQLMetricInstances:
    dml: DMLMetrics
    transactions: TransactionMetrics
    tuples: TupleMetrics
    wal: WALMetrics
    system_cpu: SystemCPUMetrics
    system_memory: SystemMemoryMetrics
    system_disk_io: SystemDiskIOMetrics
    system_network: SystemNetworkMetrics


class PostgreSQLMetricManager:
    def __init__(self, replay_file: str, daemon_mode: bool = False):
        self.connection_source = ConnectionSource.postgresql
        self.replay_file = replay_file
        self.daemon_mode = daemon_mode
        self.datetimes: deque[str] = deque()
        self.metrics: PostgreSQLMetricInstances = None

        # Data Stores
        self.global_status: dict[str, int] = {}

        self.reset()

    def reset(self):
        self.datetimes.clear()
        self.global_status.clear()

        self.metrics = PostgreSQLMetricInstances(
            dml=DMLMetrics(
                Queries=MetricData(label="Queries", color=MetricColor.blue),
            ),
            transactions=TransactionMetrics(
                graphs=["graph_transactions"],
                xact_commit=MetricData(label="Commits", color=MetricColor.green),
                xact_rollback=MetricData(label="Rollbacks", color=MetricColor.red),
            ),
            tuples=TupleMetrics(
                graphs=["graph_tuples_activity", "graph_tuples_rows"],
                tup_fetched=MetricData(label="Fetched", color=MetricColor.blue),
                tup_returned=MetricData(label="Returned", color=MetricColor.gray),
                tup_inserted=MetricData(label="Inserted", color=MetricColor.green),
                tup_updated=MetricData(label="Updated", color=MetricColor.yellow),
                tup_deleted=MetricData(label="Deleted", color=MetricColor.red),
            ),
            wal=WALMetrics(
                graphs=["graph_wal_bytes"],
                wal_records=MetricData(label="Records", color=MetricColor.gray),
                wal_fpi=MetricData(label="FPI", color=MetricColor.blue),
                wal_bytes=MetricData(label="Bytes", color=MetricColor.red),
            ),
            system_cpu=SystemCPUMetrics(
                graphs=["graph_system_cpu"],
                CPU_Percent=MetricData(
                    label="CPU %",
                    color=MetricColor.blue,
                    per_second_calculation=False,
                    create_switch=False,
                ),
            ),
            system_memory=SystemMemoryMetrics(
                graphs=["graph_system_memory"],
                Memory_Total=MetricData(
                    label="Total",
                    color=MetricColor.blue,
                    per_second_calculation=False,
                    visible=False,
                    save_history=False,
                    create_switch=False,
                ),
                Memory_Used=MetricData(
                    label="Memory Used",
                    color=MetricColor.green,
                    per_second_calculation=False,
                    create_switch=False,
                ),
            ),
            system_disk_io=SystemDiskIOMetrics(
                graphs=["graph_system_disk_io"],
                Disk_Read=MetricData(label="IOPS Read", color=MetricColor.blue),
                Disk_Write=MetricData(label="IOPS Write", color=MetricColor.yellow),
            ),
            system_network=SystemNetworkMetrics(
                graphs=["graph_system_network"],
                Network_Down=MetricData(label="Net Dn", color=MetricColor.blue),
                Network_Up=MetricData(label="Net Up", color=MetricColor.gray),
            ),
        )

    def refresh_data(self, **kwargs) -> None:
        """Update metrics with the latest data.

        Args:
            global_status: Database metrics (transactions, tuples, WAL stats)
            system_utilization: System metrics (CPU, memory, disk, network)
        """
        global_status: dict = kwargs.get("global_status", {})
        system_utilization: dict = kwargs.get("system_utilization", {})

        # Create a local copy of global_status to avoid mutating the shared dictionary
        local_status = global_status.copy()

        # Calculate total queries from tuple operations (closest PostgreSQL equivalent to MySQL Queries metric)
        # In PostgreSQL, "queries" = sum of all tuple operations
        total_queries = (
            int(local_status.get("tup_returned", 0))
            + int(local_status.get("tup_fetched", 0))
            + int(local_status.get("tup_inserted", 0))
            + int(local_status.get("tup_updated", 0))
            + int(local_status.get("tup_deleted", 0))
        )
        local_status["Queries"] = total_queries

        # Update DML metrics (for sparkline)
        self._update_metrics(local_status, self.metrics.dml)

        # Update database metrics
        self._update_metrics(local_status, self.metrics.transactions)
        self._update_metrics(local_status, self.metrics.tuples)
        self._update_metrics(local_status, self.metrics.wal)

        # Update system metrics
        self._update_metrics(system_utilization, self.metrics.system_cpu)
        self._update_metrics(system_utilization, self.metrics.system_memory)
        self._update_metrics(system_utilization, self.metrics.system_network)
        self._update_metrics(system_utilization, self.metrics.system_disk_io)

        # Append datetime (must match plotext's date_form format set in Graph._setup_plot)
        self.datetimes.append(datetime.now(tz=timezone.utc).strftime("%d/%m/%y %H:%M:%S"))
        if len(self.datetimes) > 100:  # Max history
            self.datetimes.popleft()

    def _update_metrics(self, data_source: dict, metric_dataclass):
        for field_name, metric_data in metric_dataclass.__dict__.items():
            if not isinstance(metric_data, MetricData):
                continue

            raw_value = int(data_source.get(field_name, 0))

            # Rate calculation
            if metric_data.per_second_calculation:
                if metric_data.last_value is not None:
                    rate = raw_value - metric_data.last_value  # / interval
                    if rate < 0:
                        rate = 0  # Should not happen unless restart
                    metric_data.values.append(rate)
                else:
                    metric_data.values.append(0)
                metric_data.last_value = raw_value
            else:
                metric_data.values.append(raw_value)

            if len(metric_data.values) > 100:
                metric_data.values.popleft()


class PostgreSQLDataProcessor:
    def __init__(self, app: "DolphieApp"):
        self.app = app

    def process_data(self, tab: "Tab") -> None:
        dolphie = tab.dolphie
        db: PostgreSQLDatabase = dolphie.main_db_connection

        # Handle initial connection setup
        if dolphie.connection_status == ConnectionStatus.connecting:
            # Get and parse server version
            db.execute(PostgreSQLQueries.server_version)
            version_result = db.fetchone()
            if version_result:
                version_str = version_result.get("server_version", "")
                db.server_version, db.server_version_major = db.parse_server_version(version_str)
                dolphie.global_status["server_version"] = db.server_version
                # Set host_version to match MySQL pattern (for display purposes)
                dolphie.host_version = db.server_version

            self.app.tab_manager.update_connection_status(tab=tab, connection_status=ConnectionStatus.connected)

        # 1. Global Stats
        db.execute(PostgreSQLQueries.global_stats)
        global_stats = db.fetchone() or {}
        dolphie.global_status.update(global_stats)

        # 2. WAL Stats (PostgreSQL 14+ only)
        if db.server_version_major >= 14:
            db.execute(PostgreSQLQueries.wal_stats)
            wal_stats = db.fetchone() or {}
            dolphie.global_status.update(wal_stats)

        # 3. Connection Stats
        db.execute(PostgreSQLQueries.connection_stats)
        conn_stats = db.fetchone() or {}
        dolphie.global_status.update(conn_stats)

        # 4. Uptime
        db.execute(PostgreSQLQueries.uptime)
        uptime_res = db.fetchone()
        if uptime_res:
            dolphie.global_status["Uptime"] = uptime_res.get("extract", 0)

        # 5. Update metric manager's global_status reference
        # Note: refresh_data() is called by WorkerManager.run_worker_main() after process_data()
        dolphie.metric_manager.global_status = dolphie.global_status

        # 6. Processlist
        if dolphie.panels.processlist.visible:
            db.execute(PostgreSQLQueries.activity)
            rows = db.fetchall()
            dolphie.processlist_threads = {str(row["pid"]): PostgreSQLProcesslistThread(row) for row in rows}

        # 7. Replication Stats (for Primary)
        # We fetch this if we are a primary node (which is typical for Dolphie monitoring target)
        # If we are a standby, this will be empty.
        try:
            db.execute(PostgreSQLQueries.replication)
            dolphie.postgresql_replication = db.fetchall()
        except Exception as e:
            logger.debug(f"Failed to fetch replication stats: {e}")
            dolphie.postgresql_replication = []

        # 8. Table Health (Vacuum/Analyze)
        # Useful for identifying table bloat and maintenance needs
        try:
            db.execute(PostgreSQLQueries.table_health)
            dolphie.postgresql_table_health = db.fetchall()
        except Exception as e:
            logger.debug(f"Failed to fetch table health stats: {e}")
            dolphie.postgresql_table_health = []

    def refresh_screen(self, tab: "Tab") -> None:
        """Refresh the PostgreSQL screen for a given tab."""
        dolphie = tab.dolphie

        if tab.loading_indicator.display:
            tab.loading_indicator.display = False

        # Refresh all visible panels except graphs (handled separately)
        for panel in dolphie.panels.get_all_panels():
            if panel.visible and panel.name != "graphs":
                self.app.refresh_panel(tab, panel.name)

                # Update the sparkline for queries per second (same as MySQL)
                if panel.name == dolphie.panels.dashboard.name and dolphie.metric_manager.metrics.dml.Queries.values:
                    tab.sparkline.data = dolphie.metric_manager.metrics.dml.Queries.values
                    tab.sparkline.refresh()

        # Refresh graphs if visible
        if dolphie.panels.graphs.visible:
            self.app.update_graphs(tab.metric_graph_tabs.get_pane(tab.metric_graph_tabs.active).name)

        tab.refresh_replay_dashboard_section()

        # Take a snapshot of the processlist to be used for commands
        # since the data can change after a key is pressed
        if not dolphie.daemon_mode:
            dolphie.processlist_threads_snapshot = dolphie.processlist_threads.copy()
