from __future__ import annotations

from rich.syntax import Syntax

from dolphie.DataTypes import AnyProcesslistThread, PostgreSQLProcesslistThread
from dolphie.Dolphie import Dolphie
from dolphie.Modules.Functions import coerce_int, coerce_str, filter_excludes, format_query
from dolphie.Modules.Queries import PostgreSQLQueries
from dolphie.Modules.TabManager import Tab


def create_panel(tab: Tab) -> None:
    dolphie = tab.dolphie

    columns = [
        {"name": "PID", "field": "id", "width": 11},
        {"name": "Username", "field": "user", "width": 20},
        {"name": "Host/IP", "field": "host", "width": 25},
        {"name": "Database", "field": "db", "width": 17},
        {"name": "State", "field": "command", "width": 19},
        {"name": "Wait Event", "field": "wait_event", "width": 22},
        {"name": "Age", "field": "formatted_time", "width": 9},
        {"name": "Query", "field": "formatted_query", "width": None},
        {"name": "time_seconds", "field": "time", "width": 0},
    ]

    # Refresh optimization
    query_length_max = 300
    processlist_datatable = tab.processlist_datatable

    # Clear table if columns change
    if len(processlist_datatable.columns) != len(columns):
        processlist_datatable.clear(columns=True)

    # Add columns to the datatable if it is empty
    if not processlist_datatable.columns:
        for column_data in columns:
            processlist_datatable.add_column(column_data["name"], key=column_data["name"], width=column_data["width"])

    column_names = [column_data["name"] for column_data in columns]
    column_fields = [column_data["field"] for column_data in columns]

    # Has to happen before the filtering below so replays remember the values being filtered out
    dolphie.record_filter_dropdown_values()

    if dolphie.replay_file:
        threads_to_render: dict[int, AnyProcesslistThread] = {
            thread_id: thread
            for thread_id, thread in dolphie.processlist_threads.items()
            if isinstance(thread, PostgreSQLProcesslistThread) and not _filtered_out(dolphie, thread)
        }
    else:
        # Not a replay file, so fetch_data() already filtered.
        threads_to_render = dolphie.processlist_threads

    changed = False

    with dolphie.app.batch_update():
        # Remove stale rows first
        if threads_to_render:
            active_row_keys = {str(thread_id) for thread_id in threads_to_render}
            rows_to_remove = set(processlist_datatable.rows.keys()) - active_row_keys
            if rows_to_remove:
                changed = True
                if len(rows_to_remove) > len(threads_to_render):
                    processlist_datatable.clear()
                else:
                    for row_key in rows_to_remove:
                        processlist_datatable.remove_row(row_key)
        elif processlist_datatable.row_count:
            changed = True
            processlist_datatable.clear()

        for thread_id, thread in threads_to_render.items():
            if not isinstance(thread, PostgreSQLProcesslistThread):
                continue

            row_key = str(thread_id)

            row_values = []
            for column_field in column_fields:
                value = getattr(thread, column_field)
                if column_field == "formatted_query" and isinstance(value, Syntax):
                    value = format_query(value.code[:query_length_max])
                row_values.append(value)

            row_values = processlist_datatable.normalize_cells(row_values)
            if row_key in processlist_datatable.rows:
                datatable_row = processlist_datatable.get_row(row_key)

                for column_id, (column_name, column_field) in enumerate(zip(column_names, column_fields, strict=True)):
                    new_val = row_values[column_id]
                    old_val = datatable_row[column_id]

                    # Compare text content for Syntax objects
                    cmp_new = new_val.code if isinstance(new_val, Syntax) else new_val
                    cmp_old = old_val.code if isinstance(old_val, Syntax) else old_val

                    if cmp_new != cmp_old or column_field == "formatted_time" or column_field == "time":
                        changed = True
                        processlist_datatable.update_cell(
                            row_key,
                            column_name,
                            new_val,
                            update_width=(column_field == "formatted_query"),
                        )
            else:
                changed = True
                processlist_datatable.add_row(*row_values, key=row_key)

        if changed:
            processlist_datatable.sort("time_seconds", reverse=dolphie.sort_by_time_descending)

    if dolphie.replay_file:
        dolphie.processlist_threads = threads_to_render

    tab.processlist_title.update(
        f"{dolphie.panels.processlist.title} ([$highlight]{processlist_datatable.row_count}[/$highlight])"
    )


def _filtered_out(dolphie: Dolphie, thread: PostgreSQLProcesslistThread) -> bool:
    # Filters are applied here instead of in SQL since Functions.filter_sql_condition builds MySQL syntax
    raw_host = coerce_str(thread.thread_data.get("host"))
    return bool(
        (dolphie.user_filter and filter_excludes(dolphie.user_filter, thread.user))
        or (dolphie.db_filter and filter_excludes(dolphie.db_filter, thread.db))
        or (dolphie.host_filter and filter_excludes(dolphie.host_filter, raw_host, partial=True))
        or (dolphie.query_time_filter and thread.time < dolphie.query_time_filter)
        or (dolphie.query_filter and filter_excludes(dolphie.query_filter, thread.formatted_query.code, partial=True))
    )


def fetch_data(tab: Tab) -> dict[int, AnyProcesslistThread]:
    dolphie = tab.dolphie

    # Idle backends are hidden unless specified. Idle in transaction is kept since it holds locks
    idle_filter = "TRUE" if dolphie.show_idle_threads else "state IS DISTINCT FROM 'idle'"

    dolphie.main_db_connection.execute(PostgreSQLQueries.activity.replace("$1", idle_filter))
    threads = dolphie.main_db_connection.fetchall()

    processlist_threads: dict[int, AnyProcesslistThread] = {}
    for thread in threads:
        thread_id = coerce_int(thread["id"])

        # Don't include Dolphie's threads
        if thread_id in (dolphie.main_db_connection.connection_id, dolphie.secondary_db_connection.connection_id):
            continue

        if thread["host"]:
            thread["host"] = dolphie.get_hostname(thread["host"])
        thread["query"] = thread["query"] or ""

        processlist_thread = PostgreSQLProcesslistThread(thread)
        if not _filtered_out(dolphie, processlist_thread):
            processlist_threads[thread_id] = processlist_thread

    return processlist_threads
