# Dolphie Panels Reference

## UI Layout Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         TopBar                                  │
│  [Connection Status] Host:Port  |  Help Text                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                      Active Panel Content                       │
│                     (Dashboard, Processlist,                    │
│                      Replication, etc.)                         │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Sparkline (Queries Per Second Graph)                    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│  [Key Bindings] 1=Dashboard 2=Processlist 3=Graphs...          │
└─────────────────────────────────────────────────────────────────┘
```

---

## TopBar Widget
**File**: `Widgets/TopBar.py`

Displays:
- **Dolphie Logo & Version**: Dolphin emoji + app name
- **Connection Status**: `[CONNECTED]` or `[DISCONNECTED]` with color coding
- **Host:Port**: Current database connection information
- **Recording Indicator**: Shows when replay data is being recorded
- **Help Text**: Dynamic help message (e.g., "press q to return")

```
🐬 Dolphie vX.Y.Z  |  [CONNECTED] 127.0.0.1:3306
```

---

## Sparkline Widget
**Location**: Dashboard footer (embedded in panel)  
**File Reference**: `Modules/TabManager.py`

The **sparkline** is a small inline graph showing **Queries Per Second (QPS)** over time.

**How it Works**:
1. Data source: `dolphie.metric_manager.metrics.dml.Queries.values`
2. Updated every refresh cycle with new metric values
3. Uses Textual's built-in `Sparkline` component

```python
tab.sparkline.data = dolphie.metric_manager.metrics.dml.Queries.values
tab.sparkline.refresh()
```

**Styling**: Color-coded to show trends
- Blue minimum values
- Blue-gray maximum values

---

## MySQL Panels

### 1. Dashboard
**File**: `Panels/Dashboard.py`  
**Key Binding**: `1`

**Purpose**: Overview of database health and key metrics

**Displays** (6 sections):

**Section 1**: Host Information
- Version & Edition
- OS & Type (MySQL/MariaDB/Galera/etc.)
- Server UUID
- Install directory

**Section 2**: Uptime & Threads
- Uptime duration
- Thread statistics (Connected, Running, Cached)
- Opened tables count

**Section 3**: Query Activity
- Queries per second
- DML operations (Selects, Inserts, Updates, Deletes)

**Section 4**: Connections
- New connections since startup
- Aborted connections

**Section 5**: InnoDB & Memory
- InnoDB buffer pool statistics
- Memory usage

**Section 6**: System Utilization
- CPU percentage
- Memory usage (percentage & bytes)
- Disk I/O (IOPS)
- Load average
- Swap usage

**Features**:
- Real-time metrics update
- Sparkline showing QPS trends

---

### 2. Processlist
**File**: `Panels/Processlist.py`  
**Key Binding**: `2`

**Purpose**: View active database connections and running queries

**Columns**:
- Thread ID
- Username
- Hostname/IP
- Database
- Command
- State
- Transaction State
- Rows Locked
- Rows Modified
- Duration
- Query text (truncated/formatted)
- Concurrency tickets (if enabled)

**Features**:
- Sortable columns
- Use `t` to open thread details (includes a formatted query when available)
- Filters: User, database, host, query text, time threshold
- Options:
  - Show transactions only (`--show-trxs-only`)
  - Show additional query columns
  - Use Performance Schema (MySQL 5.7+)

---

### 3. Metric Graphs
**File**: `Modules/TabManager.py`  
**Key Binding**: `3`

**Purpose**: Time-series graphs of key metrics over time

**Graph Types** (tabbed):
- **DML Operations**: Inserts, Updates, Deletes, Selects
- **Replication**: Slave lag, bytes behind master
- **Connections**: New connections, aborted connections
- **Questions**: Queries per second, slow queries
- **Disk I/O**: Read/write operations (via Performance Schema)
- **InnoDB**: Buffer pool efficiency
- **Queries**: Query latency, lock time

**Features**:
- Real-time plotting with `plotext`
- Auto-scales Y-axis based on data range
- Time-based X-axis
- Color-coded lines
- Max/Min indicators

---

### 4. Replication
**File**: `Panels/Replication.py`  
**Key Binding**: `4`

**Purpose**: Monitor replication status and replicas

**Displays**:

**Replication Status** (if replica):
- Master host, port, user
- Master binary log file & position
- Relay log position
- Slave IO/SQL thread status
- Seconds behind master (lag)
- Last replication error

**Replica Grid** (if master):
- Shows all connected replicas
- Per-replica: host, port, thread ID, binary log position, lag

**Group Replication** (if enabled):
- Member status table
- Cluster state
- Member details

**InnoDB Cluster/ClusterSet**:
- Cluster topology
- Member information
- Cluster status

---

### 5. Metadata Locks
**File**: `Panels/MetadataLocks.py`  
**Key Binding**: `5`

**Purpose**: Monitor locked objects and blocking locks (MySQL 5.7+)

**Columns**:
- Object Type (TABLE, SCHEMA, etc.)
- Object Schema
- Object Name
- Lock Type (EXCLUSIVE, SHARED, etc.)
- Lock Duration
- Blocking Locks
- Waiting Thread ID
- Owner Thread ID

**Features**:
- Shows deadlock chains
- Identifies blocking scenarios
- Requires `performance_schema = ON`

---

### 6. DDL
**File**: `Panels/DDL.py`  
**Key Binding**: `6`

**Purpose**: Monitor long-running DDL operations

**Columns**:
- Schema
- Table
- Event Name (ALTER TABLE, CREATE INDEX, etc.)
- Work Completed (percentage)
- Work Estimated
- Units Work Done
- Elapsed Time
- Estimated Time Remaining

**Features**:
- Real-time progress tracking
- Requires Performance Schema

---

### 7. Performance Schema Metrics
**File**: `Panels/PerformanceSchemaMetrics.py`  
**Key Binding**: `7`

**Purpose**: Deep dive into Performance Schema table I/O metrics

**Displays** (tabbed):

**File I/O Metrics**:
- File names being read/written
- I/O counts and bytes
- Event wait times

**Table I/O Waits**:
- Per-table I/O statistics
- Fetch vs insert/update/delete operations
- Lock wait times

**Radio Options**:
- Delta since last reset (default)
- Total since MySQL restart

**Features**:
- Sortable columns
- Schema/table filtering
- Requires `performance_schema = ON`

---

### 8. Statements Summary Metrics
**File**: `Panels/StatementsSummaryMetrics.py`

**Key Binding**: `8`

**Purpose**: Statement execution statistics

**Features**:
- Query execution counts
- Timing statistics
- Lock contention
- Filtering by schema

---

## PostgreSQL Panels

### 1. PostgreSQL Dashboard
**File**: `Panels/PostgreSQLDashboard.py`  
**Key Binding**: `1`

**Purpose**: Overview of PostgreSQL database health and metrics

**Displays** (6 sections):

**Section 1**: Host Information
- Version
- Type: PostgreSQL
- Uptime
- Connection Statistics (active, idle, total)
- Runtime & Latency

**Section 2**: Tuple Activity
- Fetched per second
- Returned per second
- Inserted per second
- Updated per second
- Deleted per second

**Section 3**: WAL Activity (PostgreSQL 14+)
- Bytes written per second
- Full Page Images (FPI)
- WAL records

**Section 4**: Transaction Activity
- Commits per second
- Rollbacks per second

**Section 5**: [Reserved for future metrics]

**Section 6**: System Utilization
- CPU (percentage, core count)
- Load average
- Memory (percentage, usage, total)
- Swap usage
- Disk I/O (read/write IOPS)
- Note: Only displayed for local connections

**Features**:
- Real-time metrics
- Graceful fallback for remote connections
- System metrics shown when available

---

### 2. PostgreSQL Processlist
**File**: `Panels/PostgreSQLProcesslist.py`  
**Key Binding**: `2`

**Purpose**: View active PostgreSQL backend connections

**Columns**:
- Backend PID
- Username
- Database
- Application Name
- Client Host/Port
- Connection State
- Query State
- Transaction State
- Query Start Time
- State Change Time
- Query Duration
- Query text

**Features**:
- Shows all active queries
- Displays wait events (if available)
- Color-coded by state (active, idle, idle in transaction)

---

### 3. Metric Graphs
**Key Binding**: `3`

**Purpose**: Time-series graphs of PostgreSQL metrics

**Graph Types** (auto-created):
- **DML/Queries**: Query operations per second
- **Transactions**: Commits and rollbacks
- **Tuple Activity**: Insert, update, delete, fetch operations
- **WAL Activity**: Write-Ahead Logging bytes (PostgreSQL 14+)
- **System CPU**: CPU usage over time
- **System Memory**: Memory usage over time
- **System Disk I/O**: Read/write IOPS
- **System Network**: Network activity (collected but not displayed)

**Features**:
- Automatic graph creation from metrics
- Real-time updates
- Scales based on data range

---

### 4. PostgreSQL Replication
**File**: `Panels/PostgreSQLReplication.py`  
**Key Binding**: `4`

**Purpose**: View standby replication status from `pg_stat_replication`

**Displays**:
- PID, host, user, application name
- Replication state and sync state
- Replay lag, write lag, and flush lag

---

### PostgreSQL Panel Key Bindings

| Key | Panel |
|-----|-------|
| `1` | PostgreSQL Dashboard |
| `2` | PostgreSQL Processlist |
| `3` | PostgreSQL Graphs |
| `4` | PostgreSQL Replication |

---

## ProxySQL Panels

### 1. ProxySQL Dashboard
**File**: `Panels/ProxySQLDashboard.py`

**Purpose**: ProxySQL overview and statistics

**Displays**:
- Hostgroup information
- Command statistics
- Queries routed
- Backend connection stats

---

### 2. ProxySQL Processlist
**File**: `Panels/ProxySQLProcesslist.py`

**Purpose**: Active ProxySQL connections

**Displays**:
- Client connections
- Backend server connections
- Query routing
- Connection duration

---

### 3. ProxySQL Hostgroup Summary
**File**: `Panels/ProxySQLHostgroupSummary.py`

**Purpose**: Hostgroup statistics and status

**Displays**:
- Hostgroup ID
- Server address & port
- Connection count
- Query count
- Status (ONLINE, OFFLINE, etc.)

---

### 4. ProxySQL Query Rules
**File**: `Panels/ProxySQLQueryRules.py`

**Purpose**: Query routing rules and statistics

**Displays**:
- Rule ID
- Match criteria
- Destination hostgroup
- Rule statistics

---

### 5. ProxySQL Command Stats
**File**: `Panels/ProxySQLCommandStats.py`

**Purpose**: Command execution statistics

**Displays**:
- Command type
- Execution count
- Timing statistics

---

## Metrics Graphs System

### Architecture

The graph system works by:

1. **Metric Manager** defines metrics with graph widget references:
   ```python
   DMLMetrics(
       graphs=["graph_dml"],  # List of widget IDs
       tab_name="DML Operations",
       ...
   )
   ```

2. **TabManager** creates graph widgets matching the IDs

3. **App.update_graphs()** periodically renders:
   ```python
   for metric in metric_manager.metrics:
       for graph_id in metric.graphs:
           getattr(tab, graph_id).render_graph(metric, ...)
   ```

### Automatic Graph Creation

**MySQL**: Graph widgets are created for all registered metrics
- System CPU, Memory, Disk I/O
- DML, Replication, Connections
- Questions, Disk I/O, InnoDB, Queries

**PostgreSQL**: Graph widgets auto-created from metric definitions
- DML, Transactions, Tuple Activity, WAL
- System CPU, Memory, Disk I/O, Network

**ProxySQL**: Custom graph sets based on ProxySQL metrics

---

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `1` | Toggle Dashboard |
| `2` | Toggle Processlist |
| `3` | Toggle Metric Graphs |
| `4` | Toggle Replication (MySQL) / Hostgroup Summary (ProxySQL) |
| `5` | Toggle Metadata Locks (MySQL) / Query Rules (ProxySQL) |
| `6` | Toggle DDL (MySQL) / Command Stats (ProxySQL) |
| `7` | Toggle Performance Schema Metrics (MySQL) |
| `8` | Toggle Statements Summary (MySQL) |
| `?` | Command palette (discover commands) |
| `q` | Quit (main UI); may act as “back” in some modal screens |
| `space` | Force a refresh cycle |
| `f` | Open thread filter modal |
| `c` | Clear all filters |
| `t` | Thread details (opens a selection modal) |

---

## Panel Design Patterns

### 1. Data Isolation
Each panel reads from a single `Dolphie` instance, ensuring consistent data presentation.

### 2. Lazy Rendering
Panels only re-render when data changes (Textual optimization).

### 3. Type-Specific Display
Each database type has dedicated panel classes:
- `Dashboard.py` (MySQL)
- `PostgreSQLDashboard.py` (PostgreSQL)
- `ProxySQLDashboard.py` (ProxySQL)

### 4. Error Graceful Degradation
Missing data shows as:
- `[dim]Not available[/dim]` - Not applicable or unavailable
- `--` or `-` - Missing metric value
- Empty section - No data collected yet

### 5. Real-Time Updates
All panels update automatically when WorkerManager refreshes data:
```python
# Textual detects Dolphie.global_status change
# Panel.watch_global_status() or Panel.on_mount() re-renders
```
