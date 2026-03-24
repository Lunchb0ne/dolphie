from textual.widgets import DataTable

from dolphie.Modules.TabManager import Tab


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie
    replication_datatable = tab.replication_datatable

    # We use `postgresql_replication` which is a list of dicts fetched in PostgreSQL.py
    replication_data = getattr(dolphie, "postgresql_replication", [])

    # Define Columns
    columns = [
        {"name": "PID", "field": "pid", "width": 8, "format_number": False},
        {"name": "Host", "field": "host", "width": 15, "format_number": False},
        {"name": "User", "field": "user", "width": 15, "format_number": False},
        {"name": "App Name", "field": "application_name", "width": 20, "format_number": False},
        {"name": "State", "field": "state", "width": 15, "format_number": False},
        {"name": "Sync", "field": "sync_state", "width": 10, "format_number": False},
        {"name": "Replay Lag", "field": "replay_lag", "width": 10, "format_number": False},
        {"name": "Write Lag", "field": "write_lag", "width": 10, "format_number": False},
        {"name": "Flush Lag", "field": "flush_lag", "width": 10, "format_number": False},
    ]

    # Setup Columns if not present
    if len(replication_datatable.columns) != len(columns):
        replication_datatable.clear(columns=True)

    if not replication_datatable.columns:
        for col in columns:
            replication_datatable.add_column(col["name"], key=col["name"], width=col["width"])

    # Update Rows
    current_keys = set()

    for row in replication_data:
        # Create a unique key using PID
        key = str(row.get("pid"))
        current_keys.add(key)

        row_values = []
        for col in columns:
            val = row.get(col["field"], "")
            row_values.append(str(val) if val is not None else "")

        if key in replication_datatable.rows:
            for idx, col in enumerate(columns):
                replication_datatable.update_cell(key, col["name"], row_values[idx])
        else:
            replication_datatable.add_row(*row_values, key=key)

    # Cleanup old rows
    existing_keys = set(replication_datatable.rows.keys())
    for key in existing_keys - current_keys:
        replication_datatable.remove_row(key)

    return replication_datatable
