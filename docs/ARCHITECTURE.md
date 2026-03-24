# Dolphie Architecture & Design

## Overview

Dolphie is a real-time analytics TUI (Terminal User Interface) for MySQL/MariaDB, PostgreSQL, and ProxySQL. Built with **Textual** (a Python TUI framework), it provides comprehensive database monitoring with a modern, responsive interface.

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    App.py (Entry Point)                 │
│                   DolphieApp (Textual App)              │
└──────────────────────┬──────────────────────────────────┘
                       │
        ┌──────────────┴──────────────┐
        │                             │
   ┌────▼────┐                ┌──────▼─────┐
   │TabManager│                │WorkerManager│
   │(Multi-tab)│                │(Async Data) │
   └────┬─────┘                └──────┬──────┘
        │                             │
        │                      ┌──────▼──────────┐
        │                      │ run_worker_main │
        │                      │    (async)      │
        │                      └──────┬──────────┘
        │                             │
   ┌────▼──────────┬──────────────────▼──────┐
   │               │                         │
 ┌─▼──┐   ┌───────▼──────┐  ┌───────▼──────┐
 │Tab │   │WorkerDataProc│  │DatabaseConn  │
 │    │   │              │  │              │
 └────┘   └───────┬──────┘  └───────┬──────┘
   │              │                 │
   │       ┌──────▼──────┐          │
   │       │Queries/SQL  │◄─────────┘
   │       │             │
   └───┬───┴──────┬──────┘
       │          │
   ┌───▼──────────▼──────┐
   │   Panel Widgets     │
   │ (Dashboard,        │
   │  Processlist,      │
   │  Replication, etc.)│
   └────────────────────┘
```

---

## Core Components

### 1. App.py - Main Application
**Class**: `DolphieApp` (extends Textual's `App`)

Responsibilities:
- Sets up the Textual application and event loop
- Manages multiple database connection tabs
- Handles keyboard events and command palette
- Coordinates between `TabManager` and `WorkerManager`
- Manages UI themes and layouts

**Key Attributes**:
- `config`: Command-line configuration
- `tab_manager`: Multi-tab connection management
- `worker_manager`: Background data refresh workers
- `command_manager`: User command handling

---

### 2. Dolphie.py - Per-Tab Data Model
**Class**: `Dolphie` (one instance per tab/connection)

Responsibilities:
- Stores all metrics and state for a single database connection
- Manages connection instances (primary & secondary)
- Tracks panel visibility states
- Stores parsed server information (version, type, cluster, etc.)

**Key Attributes**:
- `main_db_connection`: Primary DB connection (used in worker thread)
- `secondary_db_connection`: Secondary DB connection (ad-hoc queries)
- `connection_source`: `ConnectionSource.mysql`, `.postgresql`, or `.proxysql`
- `global_status`: Database status metrics
- `processlist_threads`: Active database connections
- `metric_manager`: Metrics calculation engine
- `panels`: Panel visibility states

---

### 3. Modules/MySQL.py, PostgreSQL.py - Database Abstraction
**Class**: `Database`, `PostgreSQLDatabase`

Responsibilities:
- Handles all database connections and query execution
- Detects ProxySQL vs MySQL when using the MySQL driver
- Manages reconnection logic
- Decodes and processes result data

**Key Methods**:
- `connect()`: Establish connection, detect source
- `execute(query, values)`: Run a query
- `fetchall()`, `fetchone()`: Retrieve results
- `is_connected()`: Health check

**Connection Detection** (MySQL driver path):
```python
# Try ProxySQL-specific query
self.cursor.execute("SELECT @@admin-version")
self.source = ConnectionSource.proxysql
# Fall back to MySQL if error
```

**Important**: PostgreSQL is not detected by probing via the MySQL driver. PostgreSQL uses `PostgreSQLDatabase` (psycopg2) and is selected at startup via `--type postgresql` or a `postgresql://...` URI.

---

### 4. Modules/Queries.py - SQL Statements
**Classes**: `MySQLQueries`, `PostgreSQLQueries`, `ProxySQLQueries`

Centralizes all SQL queries for:
- Status & Variables
- Processlist
- Replication information
- InnoDB/Performance Schema metrics
- PostgreSQL catalog views
- ProxySQL-specific stats

---

### 5. Modules/WorkerManager.py - Async Data Refresh
**Class**: `WorkerManager`

Manages Textual worker threads for background data fetching without blocking the UI.

**Main Methods**:
- `run_worker_main()`: Primary refresh loop (async)
  - Fetches data based on connection type
  - Calls `WorkerDataProcessor` to parse data
  - Updates `Dolphie` instance with new metrics
  - Runs on configurable interval
- `run_worker_replay()`: Loads data from replay files (for debugging)

---

### 6. Modules/WorkerDataProcessor.py - Data Parsing
**Class**: `WorkerDataProcessor`

Processes raw query results into usable data structures.

**Responsibilities**:
- Converts raw rows to typed objects (e.g., `ProcesslistThread`)
- Calculates derived metrics
- Detects server type (MySQL, MariaDB, PostgreSQL, ProxySQL)
- Detects cluster types (Galera, Group Replication, InnoDB Cluster)
- Handles database-specific data parsing

**Key Methods**:
- `process_processlist()`: Converts rows to `ProcesslistThread` objects
- `process_global_status()`: Parses SHOW STATUS results
- `process_replication_status()`: Extracts replication information
- Database-specific processing methods for each engine

---

### 7. Modules/MetricManager.py - Metrics Calculation
**Class**: `MetricManager` (database-specific subclasses)

Responsibilities:
- Tracks metric changes over time
- Calculates per-second rates (QPS, TPS, etc.)
- Generates graph data
- Maintains metric history (circular buffers)

**Metric Categories**:
```python
# MySQL metrics
MetricManager(
    dml=DMLMetrics(),          # Selects, Inserts, Updates, Deletes
    replication=ReplicationMetrics(),
    connections=ConnectionMetrics(),
    ...
)

# PostgreSQL metrics
PostgreSQLMetricManager(
    dml=DMLMetrics(),           # Query operations
    transactions=TransactionMetrics(),
    tuples=TupleMetrics(),      # Insert/Update/Delete activity
    wal=WALMetrics(),           # Write-Ahead Logging
    system_cpu=SystemCPUMetrics(),
    ...
)
```

---

### 8. Modules/TabManager.py - Multi-Tab Management
**Class**: `TabManager`

Responsibilities:
- Manages multiple database connections as tabs
- Handles tab creation, switching, and closing
- Updates connection status display
- Manages loading indicators
- Creates and organizes panel widgets

---

### 9. Panels/ - UI Views

Each panel is a Textual widget displaying specific data. Panels are loaded/hidden based on user key bindings.

**MySQL Panels**:
- `Dashboard.py` - Overview of key metrics
- `Processlist.py` - Active database threads
- `Replication.py` - Replication status
- `MetadataLocks.py` - Locked objects (MySQL 5.7+)
- `DDL.py` - Data definition language tracking
- `PerformanceSchemaMetrics.py` - Performance Schema deep-dive
- `StatementsSummaryMetrics.py` - Statement statistics

**PostgreSQL Panels**:
- `PostgreSQLDashboard.py` - Overview with host info, transactions, tuple activity, WAL
- `PostgreSQLProcesslist.py` - Active backend connections

**ProxySQL Panels**:
- `ProxySQLDashboard.py` - ProxySQL overview
- `ProxySQLProcesslist.py` - ProxySQL connections
- `ProxySQLHostgroupSummary.py` - Hostgroup statistics
- `ProxySQLQueryRules.py` - Query routing rules
- `ProxySQLCommandStats.py` - Command statistics

---

## UI Architecture

The Textual layout consists of:

```
┌──────────────────────────────────┐
│         TopBar Widget            │
│ [CONNECTED] Host | Help Text     │
├──────────────────────────────────┤
│                                  │
│     Active Panel Content         │
│  (Dashboard, Processlist, etc.)  │
│                                  │
│  ┌──────────────────────────┐    │
│  │ Sparkline (QPS Graph)    │    │
│  └──────────────────────────┘    │
│                                  │
├──────────────────────────────────┤
│ [Key Bindings] 1=Dash 2=List ... │
└──────────────────────────────────┘
```

### TopBar Widget
**File**: `Widgets/TopBar.py`

Displays:
- Dolphie logo & version
- Connection status (CONNECTED/DISCONNECTED)
- Host:Port information
- Recording indicator
- Dynamic help text

### Sparkline Widget
**Location**: Dashboard footer  
**Data Source**: Queries Per Second from metric manager

Updated with each refresh cycle to show real-time query activity.

---

## Data Flow Example: Dashboard Refresh

```
1. WorkerManager.run_worker_main() [ASYNC]
   ├── Check connection status
   ├── Execute refresh queries based on connection_source:
   │   ├── MySQL: SHOW GLOBAL STATUS, SHOW PROCESSLIST, InnoDB stats
   │   ├── PostgreSQL: pg_stat_*, pg_stat_activity, system metrics
   │   └── ProxySQL: stats_* tables
   │
   ├── Call WorkerDataProcessor.process_*()
   │   └── Convert raw rows to typed objects
   │
   └── Update Dolphie instance
       ├── Dolphie.global_status = new_values
       ├── Dolphie.processlist_threads = new_threads
       ├── Dolphie.metric_manager.refresh_data()
       │   └── Calculate deltas, rates, graph points
       │
       └── Textual automatically re-renders changed widgets
           └── Dashboard.py reads from Dolphie instance
               └── Display refreshes on screen
```

---

## Panel System

All panels are managed through the `Panels` class in `DataTypes.py`. Each panel can be toggled visible/hidden via key bindings.

**Key Binding System** (1-8):
- Key `1`: Dashboard
- Key `2`: Processlist
- Key `3`: Metric Graphs
- Key `4`: Replication (MySQL) / Hostgroup Summary (ProxySQL)
- Key `5`: Metadata Locks (MySQL) / Query Rules (ProxySQL)
- Key `6`: DDL (MySQL) / Command Stats (ProxySQL)
- Key `7`: Performance Schema Metrics (MySQL)
- Key `8`: Statements Summary (MySQL)

Each panel reads its data from the current `Dolphie` instance and renders based on the connection type.

---

## Connection Source Abstraction

Dolphie supports multiple database systems with connection source checking:

```python
if dolphie.connection_source == ConnectionSource.mysql:
    # MySQL-specific logic
    replication_status = dolphie.replication_status
    
elif dolphie.connection_source == ConnectionSource.postgresql:
    # PostgreSQL-specific logic
    tuple_activity = dolphie.metric_manager.metrics.tuples
    
elif dolphie.connection_source == ConnectionSource.proxysql:
    # ProxySQL-specific logic
    hostgroup_stats = dolphie.hostgroup_stats
```

Each panel checks the connection source and displays appropriate data.

---

## Startup Flow

```
1. main() in App.py
   ├── Parse command-line arguments
   ├── Create DolphieApp instance
   ├── Initialize TabManager
   │   └── Create first Dolphie instance with connection params
   ├── Load theme
   └── Run app.run() (enter Textual event loop)

2. First Connection (run_worker_main)
   ├── Connect to database
   ├── Fetch initial data (status, variables, processlist)
   ├── Process data with WorkerDataProcessor
   │   ├── Detect MySQL vs MariaDB vs PostgreSQL
   │   ├── Detect cluster types
   │   └── Parse processlist threads
   └── Update panels with initial data

3. Continuous Refresh (every N seconds)
   └── run_worker_main() repeats data fetching and processing
```

---

## Key Design Patterns

### 1. **Per-Tab Isolation**
Each connection tab has its own `Dolphie` instance with independent:
- Database connections
- Metrics and state
- Panel visibility
- Configuration

This allows viewing multiple databases simultaneously without interference.

### 2. **Async Refresh**
WorkerManager runs refresh logic in background Textual workers, preventing UI freezes during:
- Network latency
- Large result sets
- Complex calculations

### 3. **Metric Abstraction**
MetricManager provides a consistent interface across all connection types:
- Automatic per-second rate calculation
- Graph data generation
- Delta tracking

Database-specific subclasses define metrics relevant to each engine.

### 4. **Panel Polymorphism**
Each panel type (MySQL, PostgreSQL, ProxySQL) is a separate class. The UI framework displays the appropriate panel based on connection source.

---

## System Utilization

Dolphie collects system metrics (CPU, memory, disk I/O, network) using Python's `psutil` library:

**Collection Method**:
- `Dolphie.collect_system_utilization()` - Uses `psutil` to gather OS-level metrics
- Enabled automatically for local connections (localhost / local IP / UNIX socket). Remote hosts will show "Not available" for system metrics.

**Metric Categories**:
- CPU usage (percentage)
- Memory (total, used, free, percentage)
- Disk I/O (read/write IOPS)
- Swap usage
- Load average

Metrics are stored in `system_utilization` dict and displayed in Dashboard section 6.

---

## Performance Considerations

1. **Metric History Sizing**: Circular buffers with configurable max length (default: 1000 points)
2. **Query Optimization**: Queries use efficient filters (COUNT FILTER, WHERE clauses)
3. **Data Parsing**: Minimal object creation, reuse where possible
4. **UI Rendering**: Textual handles incremental updates automatically
5. **Connection Pooling**: Secondary connections for non-blocking queries

---

## Extension Points

Dolphie is designed to be extensible:

1. **New Database Types**: Create new subclasses for `Database`, `MetricManager`, panels
2. **Custom Panels**: Add new panel widgets following existing patterns
3. **Metrics**: Define new metric types in `MetricManager` subclasses
4. **Queries**: Add queries to `Queries.py` classes
5. **Widgets**: Extend `Widgets/` directory with custom components

Each new database type follows the same patterns established for MySQL and PostgreSQL.
