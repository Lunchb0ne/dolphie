# PostgreSQL support

Dolphie monitors PostgreSQL alongside MySQL, MariaDB, and ProxySQL. PostgreSQL uses its own connection class, queries, and panels, and plugs into the same metric catalog, graph dashboard, replay files, and daemon mode as the other sources.

## Connecting

```shell
dolphie postgresql://user:password@host:5432
dolphie --type postgresql -h host -u user -p password   # port defaults to 5432 with --type postgresql
```

- A `postgresql://` URI or `--type postgresql` selects PostgreSQL. A URI's scheme wins over `--type`. ProxySQL is never selected by flag; it is detected after a MySQL connection succeeds.
- Dolphie connects to the `postgres` database. A database in the URI path is ignored.
- Login options are resolved like MySQL's, so `~/.my.cnf`, `~/.mylogin.cnf`, `DOLPHIE_*` variables, and credential profiles can supply the user and password.
- Both connections set `application_name = 'Dolphie'`.

## Requirements

- PostgreSQL 10 or later: the queries use `pg_stat_activity.backend_type`, `pg_wal_lsn_diff()`, and `pg_current_wal_lsn()`. WAL statistics need PostgreSQL 14 (`pg_stat_wal`). Live testing so far covers PostgreSQL 16.
- Grant `pg_monitor` to the monitoring role. Without it, PostgreSQL hides other roles' query text in `pg_stat_activity` and most columns of `pg_stat_replication`.
- A query that fails with `insufficient_privilege` (42501) notifies once and is skipped from then on. Any other query error raises `ManualException`, the same as MySQL.
- The driver is `psycopg2-binary`, so installs need no compiler or libpq headers.

## What each poll collects

`WorkerDataProcessor.process_postgresql_data` runs these `PostgreSQLQueries` on the main connection:

| Query | Source | Stored in |
|---|---|---|
| `settings` | `pg_settings` | `dolphie.global_variables` (`v` key, change notifications) |
| `global_stats` | `pg_stat_database`, summed over every database | `global_status` |
| `wal_stats` | `pg_stat_wal`, only when `server_version_num >= 140000` | `global_status` |
| `connection_stats` | `pg_stat_activity` client backends | `global_status` |
| `server_state` | `pg_postmaster_start_time()`, `pg_is_in_recovery()` | `global_status["Uptime"]`, `["in_recovery"]` |
| `activity` | `pg_stat_activity` client backends | `processlist_threads`, while the processlist is visible |
| `replication` | `pg_stat_replication` | `dolphie.postgresql_replication`, every poll |
| `table_health` | `pg_stat_user_tables` of the connected database | `dolphie.postgresql_table_health`, while the dashboard is visible |

Aggregates are cast to `bigint` in SQL, because psycopg2 returns `numeric` as `Decimal`, which orjson can't serialize for replay files. `global_status["Queries"]` is derived as the sum of the five tuple counters, so the dashboard sparkline has a throughput series. It counts rows, not statements.

## Metrics and graphs

PostgreSQL metrics live in the shared catalog (`MetricDefinitions.py`) and are restricted to `ConnectionSource.postgresql`:

| Group | Series | Graph tab |
|---|---|---|
| `postgresql_transactions` | `xact_commit`, `xact_rollback` | Transactions |
| `postgresql_tuples` | `tup_fetched`, `tup_returned`, `tup_inserted`, `tup_updated`, `tup_deleted` | Tuples |
| `postgresql_wal_records` | `wal_records`, `wal_fpi` | WAL (only when `wal_records` is in `global_status`) |
| `postgresql_wal_bytes` | `wal_bytes` | WAL |
| `system_*` | CPU, memory, disk I/O, network | System (local hosts only) |
| `dml.Queries` | derived tuple sum | sparkline only |

Rates are per second and use `MetricManager`'s baseline and counter-reset handling. PostgreSQL flushes backend statistics to the cumulative stats system about once a second, so refresh intervals below 1s graph a sawtooth of zeros and doubled values.

## Panels and keys

| Key | Panel | Module |
|---|---|---|
| `1` | Dashboard: host information and role, tuples/s, WAL/s, transactions/s with deadlocks, top 5 tables by dead tuples, system utilization | `Panels/PostgreSQLDashboard.py` |
| `2` | Processlist: PID, user, host, database, state, wait event, age, query | `Panels/PostgreSQLProcesslist.py` |
| `3` | Metric graphs | `Widgets/MetricGraphDashboard.py` |
| `4` | Replication: standbys with write, flush, and replay lag in bytes. Refuses to open when there are none | `Panels/PostgreSQLReplication.py` |

- The processlist hides `idle` backends unless `i` is toggled. `idle in transaction` stays visible because it holds locks. Filters (`f`) are applied in Python with `filter_excludes`, since `Functions.filter_sql_condition` builds MySQL syntax. Dolphie's own backends are excluded by PID.
- The command catalogs are `ConnectionSource.postgresql` and `postgresql_replay` in `CommandManager.py`. Panels 5-8 and MySQL- or ProxySQL-only commands (`k`, `t`, `e`, `u`, ...) aren't offered.

## Replay and daemon mode

- `ReplayManager` records `replication` and `table_health` alongside the common payload and plays them back through `PostgreSQLReplayData`. The processlist round-trips as `PostgreSQLProcesslistThread`.
- Daemon mode (`-D`) records PostgreSQL the same way as MySQL. The dashboard isn't a daemon panel, so daemon recordings have no table health.

## Known gaps

- No database selection, so table health only covers the `postgres` database.
- Tabs created in a session share `--type`, so one session can't mix live MySQL and PostgreSQL hosts. Replay files carry their own connection source.
- No integration or snapshot tests against a real PostgreSQL server. `tests/integration` only runs MySQL, MariaDB, and ProxySQL.
- No kill, thread detail, lock, or `pg_stat_statements` views yet.
