from textual.widgets import DataTable

from dolphie.Modules.TabManager import Tab


def create_panel(tab: Tab) -> DataTable:
    dolphie = tab.dolphie
    processlist_datatable = tab.processlist_datatable

    # Define Columns
    columns = [
        {"name": "PID", "field": "id", "width": 8, "format_number": False},
        {"name": "User", "field": "user", "width": 15, "format_number": False},
        {"name": "DB", "field": "db", "width": 15, "format_number": False},
        {"name": "Host", "field": "host", "width": 20, "format_number": False},
        {"name": "State", "field": "state", "width": 20, "format_number": False},
        {"name": "Wait Event", "field": "wait_event", "width": 25, "format_number": False},
        {"name": "Duration", "field": "formatted_time", "width": 10, "format_number": False},
        {"name": "Query", "field": "formatted_query", "width": None, "format_number": False},
    ]

    # Setup Columns
    if len(processlist_datatable.columns) != len(columns):
        processlist_datatable.clear(columns=True)

    if not processlist_datatable.columns:
        for col in columns:
            processlist_datatable.add_column(col["name"], key=col["name"], width=col["width"])

    # Update Rows
    for thread_id, thread in dolphie.processlist_threads.items():
        if thread_id in processlist_datatable.rows:
            # Update logic
            row_data = [
                getattr(thread, col["field"]) for col in columns
            ]

            for idx, val in enumerate(row_data):
                col_name = columns[idx]["name"]
                processlist_datatable.update_cell(thread_id, col_name, val)

        else:
            # Add row
            row_data = [
                getattr(thread, col["field"]) for col in columns
            ]
            processlist_datatable.add_row(*row_data, key=thread_id)

    # Cleanup old rows
    current_ids = set(dolphie.processlist_threads.keys())
    existing_ids = set(processlist_datatable.rows.keys())
    for pid in existing_ids - current_ids:
        processlist_datatable.remove_row(pid)

    # Sort
    # processlist_datatable.sort(...)

    # Title
    panel_title = dolphie.panels.get_panel_title(dolphie.panels.processlist.name)
    thread_count = len(dolphie.processlist_threads)
    title = f"{panel_title} ([highlight]{thread_count}[/highlight])"
    tab.processlist_title.update(title)

    return processlist_datatable
