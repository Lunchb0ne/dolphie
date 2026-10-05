# Agent steering: Dolphie with PostgreSQL

This repository is a fork of [charles-001/dolphie](https://github.com/charles-001/dolphie). The `feat-postgresql-support` branch adds PostgreSQL as a fourth connection source next to MySQL, MariaDB, and ProxySQL. Keep that diff upstreamable: follow upstream's patterns and touch shared code only where a connection-source branch has to go.

Read `CLAUDE.md` first. Its rules on workflow, threads and tabs, metrics, replay, config, and tests apply unchanged. This file covers what PostgreSQL adds. `docs/POSTGRESQL_SUPPORT.md` is the user-facing reference for queries, metrics, panels, and limits.

## Commands

- Setup is `uv sync --all-groups`. Before a push, run `uv run ruff format --check .`, `uv run ruff check .`, `uv run basedpyright`, and `uv run pytest` (or `mise run ci`). CI runs the same checks on Python 3.10 and 3.14.
- `uv run dolphie postgresql://postgres:postgres@localhost:5432` runs against a local server (`mise run run-pg`).
- Check `git diff uv.lock` after `uv add` or `uv lock`. It should only touch the dependency you changed. If it rewrites `revision` or unrelated markers, your uv version differs from upstream's. Relock with a matching uv: 0.12.10 reproduces the current lock.

## How PostgreSQL plugs in

- **Selection.** `Config.db_type` is set by `--type postgresql` or a `postgresql://` URI in `ArgumentParser`. It's not a config-file option. `Dolphie` copies it into `connection_source` at init, so `_create_connection` builds a `PostgreSQLDatabase`.
- **Connection.** `Modules/PostgreSQL.py` holds only `PostgreSQLDatabase`, which mirrors the public surface of `MySQL.Database`: `execute`, `fetchall`, `fetchone`, `fetch_value_from_field`, `connection_id`, `source`, and reconnects. `main_db_connection` is typed `Database | PostgreSQLDatabase`. A MySQL-only call such as `fetch_status_and_variables` narrows with `assert isinstance(db, Database)`. Don't add stub methods to `PostgreSQLDatabase` to satisfy the type checker.
- **Collection.** `WorkerDataProcessor.process_postgresql_data` runs the `PostgreSQLQueries` in `Queries.py`. The processlist comes from `Panels/PostgreSQLProcesslist.fetch_data`, the same shape ProxySQL uses.
- **Metrics.** The `postgresql_*` groups in `MetricDefinitions.py` are limited to `ConnectionSource.postgresql`. The graph tabs use `POSTGRESQL` in `MetricGraphDefinitions.GRAPH_TABS`. The `system_*` and `dml` groups also include PostgreSQL; `dml.Queries` feeds the sparkline. There is no PostgreSQL-specific `MetricManager`. A new series needs a group field, its entry in `create_metric_instances()`, and a `GRAPH_TABS` slot unless it is `graphable=False`. `validate_graph_definitions()` fails at import otherwise.
- **Panels.** `App.PANEL_MAPPING` maps `dashboard`, `processlist`, and `replication` to the PostgreSQL modules. The replication panel renders into MySQL's shared replication widgets: only `replication_status_single`, which is capped at `max-width: 85`. It hides the rest.
- **Keys.** `CommandManager` has `ConnectionSource.postgresql` and `postgresql_replay` catalogs, and `KeyEventManager` holds the PostgreSQL branch for key `4`. A key exists only if both have it.
- **Replay.** `PostgreSQLReplayData` carries the `replication` and `table_health` payload keys. `WorkerManager.run_worker_replay` has the PostgreSQL playback branch. A payload shape change still needs a `ReplayManager.schema_version` bump.
- **Shared code that branches on source.** `WorkerManager.run_worker_replicas` and `create_replica_panel` run for MySQL only. `Tab.toggle_entities_displays` leaves dashboard section 5 to the PostgreSQL dashboard. `monitor_read_only_change` skips PostgreSQL.

## Conventions for PostgreSQL code

- Add a `connection_source` branch beside the existing MySQL and ProxySQL ones. Don't build a parallel module stack. The pre-rebase branch had its own metric manager and refresh path, and both had to be removed to fit upstream.
- Put PostgreSQL SQL in `PostgreSQLQueries` and cast `numeric` aggregates to `bigint`. psycopg2 returns `numeric` as `Decimal`, which orjson can't write to replay files.
- Gate on `server_version_num` from `pg_settings` (already in `global_variables`), not on parsed `host_version`.
- Don't call `Functions.filter_sql_condition` for PostgreSQL, because it emits MySQL's `IFNULL`. The processlist filters in Python with `filter_excludes`.
- Error codes are SQLSTATE strings (`e.pgcode`). `42501` takes the privilege-skip path, `57P01`-`57P03` and class `08` take the reconnect path, and anything else raises `ManualException` without a numeric code.
- Use themed markup (`$label`, `$dark_gray`, `ThemedTable`), as `CLAUDE.md` requires. Never hardcode hex colors.
- Keep `psycopg2-binary`. It installs without libpq headers or a compiler, and `Dolphie.py` imports the driver for every user, MySQL included.

## Verifying a change

- Unit tests use fakes, never a live server: `QueryDatabase` in `tests/dolphie/Modules/test_PostgreSQL.py` and the `SimpleNamespace` tabs in `tests/dolphie/Panels/test_PostgreSQL*.py`. A bug fix gets a regression test that fails on the old behavior.
- `tests/integration` and the snapshot tests cover MySQL, MariaDB, and ProxySQL only, so CI never renders a PostgreSQL panel. Check rendering against a real server:
  - Drive the real app headless with `run_dolphie(Config(db_type=ConnectionSource.postgresql, ...))` from `tests/integration/harness.py`. Save SVGs with `app.save_screenshot()` and inspect them.
  - Record with `record_for_replay=True`, then play the file back with `replay_config()` and step frames with `right_square_bracket`. Playback starts paused.
  - For the replication panel and the standby path, run `pg_basebackup -R -c fast` into a second data directory and start it on another port.
- Keep refresh intervals at 1s or more when reading rates. PostgreSQL flushes backend stats about once a second, so faster polls graph a sawtooth.

## Known gaps

- Dolphie always connects to the `postgres` database, so table health only covers that database. The URI path is ignored.
- Tabs share one `--type`, so a session can't mix live MySQL and PostgreSQL hosts. The tab setup modal has no source choice and falls back to port 3306 when the host has none (`TabManager.setup_host_tab`).
- MySQL credential sources (`~/.my.cnf`, `~/.mylogin.cnf`) also feed PostgreSQL logins.
- There are no kill, thread-detail, lock, or `pg_stat_statements` views. The CLI description and README intro still say MySQL and ProxySQL only.

## Git

- `origin` is the fork. Rebase onto upstream's `main` (`https://github.com/charles-001/dolphie`), not `origin/main`, and push the branch with `--force-with-lease`.
- Use conventional commits with no AI or tool attribution, per `CLAUDE.md`.
