# PostgreSQL Support in Dolphie

## Overview

Dolphie provides comprehensive PostgreSQL support with panels equivalent to MySQL features. The implementation achieves feature parity for core monitoring dashboards while respecting PostgreSQL's architectural differences.

Note: PostgreSQL support is implemented with PostgreSQL-specific queries and types. Some MySQL-only panels (Metadata Locks, DDL, Performance Schema Metrics, Statements Summary) are not applicable to PostgreSQL.

---

## Current Implementation Status

### Completed Features ✅

#### Core Panels
1. **Dashboard** - Host info, transactions, tuple activity, WAL, system utilization
2. **Processlist** - Active backend connections with query details
3. **Metric Graphs** - Auto-generated graphs for all PostgreSQL metrics
4. **Replication** - Standby replica status from `pg_stat_replication`

#### Metrics Collection
- Transaction activity (commits, rollbacks)
- Tuple operations (insert, update, delete, fetch)
- WAL activity (bytes, FPI records)
- System metrics (CPU, memory, disk I/O, network)
- Connection statistics (active, idle, total)

#### System Integration
- PostgreSQL selection via `--type postgresql` or `postgresql://...` URI
- Native PostgreSQL support in MetricManager
- Per-second rate calculations
- Metric history tracking (circular buffers)

### Future Enhancements 📋

#### Medium Priority
- **Query Statistics**: pg_stat_statements integration
- **Lock Monitoring**: pg_locks view inspection

#### Low Priority
- **Index Health**: Index usage, bloat detection
- **Table Health**: Vacuum progress, dead tuple tracking
- **Connection Pooling**: pgBouncer/pgPool monitoring

---

## Architecture

### Selecting PostgreSQL

PostgreSQL is selected at startup (it is not auto-detected via the MySQL connection code path):

- Use a PostgreSQL URI: `postgresql://user:password@host:5432`
- Or specify `--type postgresql` and pass `-h/-P/-u/-p` flags.

ProxySQL is auto-detected after connecting via the MySQL driver by probing `SELECT @@admin-version`.

### Data Processing Pipeline

```
PostgreSQL Database
    ↓
PostgreSQL.execute() 
    ↓
PostgreSQL (or WorkerDataProcessor) processes results
    ↓
Dolphie.global_status + system_utilization
    ↓
PostgreSQLMetricManager.refresh_data()
    ↓
Metrics (system_cpu, dml, transactions, etc.)
    ↓
Panel Widgets (Dashboard, Processlist, Graphs)
    ↓
UI Display
```

### Metric Manager

**File**: `Modules/PostgreSQL.py` (PostgreSQLMetricManager class)

**Metrics Defined**:
- `dml` - Query operations per second
- `transactions` - Commits and rollbacks per second
- `tuples` - Tuple operations (insert, update, delete, fetch)
- `wal` - Write-Ahead Logging activity (PostgreSQL 14+)
- `system_cpu` - CPU percentage
- `system_memory` - Memory usage (total, used)
- `system_disk_io` - Disk I/O operations
- `system_network` - Network activity

Each metric tracks:
- Current value
- Per-second rate calculation
- History (circular buffer)
- Graph widget references

---

## SQL Queries

### Core Status Queries

**Basic Statistics** (aggregated across all databases):
```sql
SELECT
    sum(xact_commit) as xact_commit,
    sum(xact_rollback) as xact_rollback,
    sum(tup_returned) as tup_returned,
    sum(tup_fetched) as tup_fetched,
    sum(tup_inserted) as tup_inserted,
    sum(tup_updated) as tup_updated,
    sum(tup_deleted) as tup_deleted,
    sum(conflicts) as conflicts,
    sum(deadlocks) as deadlocks
FROM pg_stat_database
```

**Connection Statistics**:
```sql
SELECT
    COUNT(*) FILTER (WHERE state = 'active') AS active_connections,
    COUNT(*) FILTER (WHERE state = 'idle') AS idle_connections,
    COUNT(*) AS total_connections
FROM pg_stat_activity
WHERE pid != pg_backend_pid()
```

**WAL Activity** (PostgreSQL 14+):
```sql
SELECT
     wal_records,
     wal_fpi,
     wal_bytes,
     wal_buffers_full,
     wal_write,
     wal_sync,
     wal_write_time,
     wal_sync_time
FROM pg_stat_wal
```

### Performance Views Used

- `pg_stat_activity` - Connection state and current queries
- `pg_stat_database` - Database-level statistics
- `pg_stat_user_tables` - Table statistics
- `pg_stat_user_indexes` - Index statistics (optional)
- `pg_stat_statements` - Query history (optional, requires extension)

---

## Dashboard Layout

### Section 1: Host Information
- **Version**: PostgreSQL version (e.g., "15.2")
- **Type**: Always "PostgreSQL"
- **Uptime**: Cluster uptime
- **Connections**: Active, idle, total counts
- **Runtime**: How long the application has been connected
- **Latency**: Query execution time

### Section 2: Tuple Activity
Displays per-second rates:
- Fetched: Tuples retrieved per second
- Returned: Tuples returned per second
- Inserted: Tuples inserted per second
- Updated: Tuples updated per second
- Deleted: Tuples deleted per second

### Section 3: WAL Activity (PostgreSQL 14+)
Displays per-second rates:
- Bytes: WAL bytes written per second
- FPI: Full Page Images per second
- Records: WAL records per second

Falls back to historical data for earlier PostgreSQL versions.

### Section 4: Transaction Activity
Displays per-second rates:
- Commits: Transactions committed per second
- Rollbacks: Transactions rolled back per second

### Section 5: [Reserved]
Available for future metric sections.

### Section 6: System Utilization
Displays system-level metrics (localhost connections only):
- **CPU**: Percentage usage, core count
- **Load**: 1-minute, 5-minute, 15-minute load average
- **Memory**: Percentage, usage in bytes, total available
- **Swap**: Swap usage and total
- **Disk I/O**: Read and write IOPS

**Availability**: 
- ✅ Local connections (localhost)
- ❌ Remote connections show "[dim]Not available (remote host)[/dim]"
- Enabled automatically for local connections

---

## Processlist Panel

### Features

**Columns Displayed**:
- **PID**: PostgreSQL backend process ID
- **User**: Database user
- **Database**: Current database
- **Host**: Client address (from `client_addr`)
- **State**: Connection state (active, idle, idle in transaction)
- **Time**: Seconds since `query_start` (when available)
- **Wait Event**: `wait_event_type:wait_event` (when available)
- **Query**: SQL query text

**Color Coding**:
- Green: Active queries
- Yellow: Idle connections
- Red: Idle in transaction (resource-holding)

**Sorting**:
- Click column headers to sort
- Default sort by duration (longest running first)

**Filtering**:

The filter modal (`f`) supports common fields (user/host/database/query text/minimum time) when present in the processlist data.

---

## Metric Graphs

### Auto-Generated Graph Tabs

The graph system automatically creates tabs for each metric category:

| Tab | Metrics | Sources |
|-----|---------|---------|
| **DML** | Query operations | Tuple sum calculation |
| **Transactions** | Commits, Rollbacks | pg_stat_database |
| **Tuple Activity** | Insert, Update, Delete, Fetch | pg_stat_user_tables |
| **WAL** | Bytes, FPI, Records | pg_stat_database (14+) |
| **System CPU** | CPU % | psutil |
| **System Memory** | Memory usage | psutil |
| **System Disk I/O** | Read/Write IOPS | psutil |
| **System Network** | Upload/Download | psutil |

### Graph Features

- Real-time plotting with `plotext`
- Auto-scaling Y-axis
- Time-based X-axis
- Per-second rate calculations
- Color-coded lines
- Max/Min value indicators

---

## DML Query Calculation

PostgreSQL lacks a single "Queries" counter like MySQL. Dolphie calculates it as:

```python
queries_per_sec = (
    tup_returned +
    tup_fetched +
    tup_inserted +
    tup_updated +
    tup_deleted
) / time_delta
```

Implementation note: Dolphie derives a "Queries" metric from tuple counters and then charts the *delta between refresh cycles*. With the default 1s refresh interval, this approximates “per second”.

---

## System Utilization

### Collection Method

Uses Python's `psutil` library for OS-level metrics:

```python
def collect_system_utilization():
    return {
        'cpu': psutil.cpu_percent(),
        'memory_percent': psutil.virtual_memory().percent,
        'memory_total': psutil.virtual_memory().total,
        'memory_used': psutil.virtual_memory().used,
        'disk_io_read': psutil.disk_io_counters().read_count,
        'disk_io_write': psutil.disk_io_counters().write_count,
        ...
    }
```

### Availability

- ✅ **Localhost connections**: Full metrics available
- ❌ **Remote connections**: Shows "Not available (remote host)"

This is a security and technical limitation - we can't execute system commands on remote servers.

### Configuration

System utilization is enabled automatically only for local connections (localhost / local IP / UNIX socket). There is no CLI flag or config option to force-enable it for remote hosts.

---

## PostgreSQL-Specific Considerations

### Data Requirements

1. **Basic Monitoring**: Requires no special configuration
2. **Query Statistics**: Install `pg_stat_statements` extension
3. **Replication Info**: Set `wal_level = replica` or higher
4. **Lock Information**: Use `pg_locks` view

### Version Differences

| Feature | PostgreSQL 12 | PostgreSQL 14+ |
|---------|---------------|----------------|
| Basic Stats | ✅ | ✅ |
| WAL Activity | ❌ | ✅ |
| Connection States | ✅ | ✅ |
| pg_stat_activity | ✅ | ✅ |

### Query Optimization

All queries use:
- Efficient aggregation (SUM, COUNT)
- Filtered joins (WHERE clauses)
- No N+1 patterns
- Connection pooling where applicable

---

## Equivalent Features Mapping

| MySQL | PostgreSQL | Implementation |
|-------|-----------|-----------------|
| SHOW PROCESSLIST | pg_stat_activity | Full panel |
| SHOW GLOBAL STATUS | pg_stat_database | Dashboard sections |
| InnoDB Metrics | pg_stat_user_tables | Tuple activity |
| Query Cache | pg_stat_statements | Future panel |
| Replication Status | pg_stat_replication | Future panel |
| Lock Monitoring | pg_locks | Future panel |

---

## Common Issues & Solutions

### System Metrics Show "Not Available"

**Cause**: Connecting to remote PostgreSQL server

**Solution**: Connect to localhost instead, or disable system utilization

### Graphs Don't Appear

**Cause**: Metric data not being populated

**Solution**: 
1. Verify PostgreSQL version (need 14+ for WAL stats)
2. Check that `postgresql_metric_manager` is initialized
3. Ensure `refresh_data()` is being called

### Missing Columns in Processlist

**Cause**: PostgreSQL version differences or missing extensions

**Solution**: Graceful degradation shows available columns only

---

## Testing

### Local Connection Test
```bash
dolphie postgresql://user:password@localhost:5432
```
- Verify dashboard displays all sections including system utilization
- Check metric graphs update in real-time
- Verify processlist shows active queries

### Remote Connection Test
```bash
dolphie postgresql://user:password@remote.host:5432
```
- System utilization should show "Not available"
- Database metrics should display normally
- Processlist should function

### Metric Verification
1. Press `1` for Dashboard
2. Verify each section shows non-zero values after queries
3. Press `3` to view Metric Graphs
4. Verify graphs update every refresh cycle
5. Press `2` to view Processlist
6. Run a test query and verify it appears

---

## Future Enhancements

### Phase 1: Extended Monitoring
- [ ] PostgreSQL Replication panel
  - WAL slot consumption
  - Standby lag
  - Active replica connections
  
### Phase 2: Advanced Analytics
- [ ] Query Statistics panel (pg_stat_statements)
- [ ] Lock Monitoring panel (pg_locks)
- [ ] Table/Index Health panel

### Phase 3: Optimization
- [ ] Connection pooling status (pgBouncer/pgPool)
- [ ] Performance recommendations
- [ ] Slow query analysis

---

## Related Files

- `Modules/PostgreSQL.py` - Database connection, queries, metric manager
- `Panels/PostgreSQLDashboard.py` - Dashboard UI
- `Panels/PostgreSQLProcesslist.py` - Processlist UI
- `Modules/TabManager.py` - Graph widget creation
- `DataTypes.py` - PostgreSQL-specific data structures
