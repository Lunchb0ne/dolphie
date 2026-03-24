import pytest
from textual.widgets import DataTable
from unittest.mock import MagicMock

from dolphie.Panels import PostgreSQLReplication
from dolphie.Modules.TabManager import Tab


class TestPostgreSQLReplicationPanel:
    def test_create_panel_clears_columns_if_mismatch(self):
        """Test that columns are cleared if they don't match the expected set."""
        tab = MagicMock(spec=Tab)
        tab.dolphie = MagicMock()
        tab.dolphie.postgresql_replication = []

        # Setup mock datatable with existing random columns
        tab.replication_datatable = MagicMock(spec=DataTable)
        tab.replication_datatable.columns = ["random", "col"]
        tab.replication_datatable.rows = {}

        def clear_columns_side_effect(**kwargs):
            if kwargs.get("columns"):
                tab.replication_datatable.columns = []

        tab.replication_datatable.clear.side_effect = clear_columns_side_effect

        PostgreSQLReplication.create_panel(tab)

        # Should have cleared
        tab.replication_datatable.clear.assert_called_with(columns=True)
        # Should have added new columns (9 of them, including PID)
        assert tab.replication_datatable.add_column.call_count == 9

    def test_create_panel_adds_rows(self):
        """Test that rows are added from postgresql_replication data."""
        tab = MagicMock(spec=Tab)
        tab.dolphie = MagicMock()
        # Sample replication data
        tab.dolphie.postgresql_replication = [
            {
                "pid": 12345,
                "host": "10.0.0.2",
                "user": "repuser",
                "application_name": "standby1",
                "state": "streaming",
                "sync_state": "async",
                "replay_lag": "00:00:01",
                "write_lag": "00:00:00",
                "flush_lag": "00:00:00",
            }
        ]

        tab.replication_datatable = MagicMock(spec=DataTable)
        tab.replication_datatable.columns = []  # Simulate empty start
        tab.replication_datatable.rows = {}

        PostgreSQLReplication.create_panel(tab)

        # Check that add_row was called with correct values
        # We expect add_row(*values, key="12345")
        args, kwargs = tab.replication_datatable.add_row.call_args
        assert kwargs["key"] == "12345"
        assert args[0] == "12345"  # pid
        assert args[1] == "10.0.0.2"  # host
        assert args[3] == "standby1"  # app name

    def test_create_panel_updates_existing_rows(self):
        """Test that existing rows are updated instead of re-added."""
        tab = MagicMock(spec=Tab)
        tab.dolphie = MagicMock()
        tab.dolphie.postgresql_replication = [
            {
                "pid": 12345,
                "host": "10.0.0.2",
                "user": "repuser",
                "application_name": "standby1",
                "state": "streaming",
            }
        ]

        tab.replication_datatable = MagicMock(spec=DataTable)
        # Simulate that columns exist so we don't clear
        tab.replication_datatable.columns = [
            "PID",
            "Host",
            "User",
            "App Name",
            "State",
            "Sync",
            "Replay Lag",
            "Write Lag",
            "Flush Lag",
        ]
        # Simulate row exists
        tab.replication_datatable.rows = {"12345": "some object"}

        PostgreSQLReplication.create_panel(tab)

        # add_row should NOT be called
        tab.replication_datatable.add_row.assert_not_called()
        # update_cell should be called for each column (9 times)
        assert tab.replication_datatable.update_cell.call_count == 9

    def test_create_panel_removes_stale_rows(self):
        """Test that rows not present in current data are removed."""
        tab = MagicMock(spec=Tab)
        tab.dolphie = MagicMock()
        tab.dolphie.postgresql_replication = []  # No current data

        tab.replication_datatable = MagicMock(spec=DataTable)
        tab.replication_datatable.columns = [
            "PID",
            "Host",
            "User",
            "App Name",
            "State",
            "Sync",
            "Replay Lag",
            "Write Lag",
            "Flush Lag",
        ]
        # Simulate stale row exists
        tab.replication_datatable.rows = {"99999": "obj"}

        PostgreSQLReplication.create_panel(tab)

        tab.replication_datatable.remove_row.assert_called_with("99999")
