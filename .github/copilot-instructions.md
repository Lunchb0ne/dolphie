# Copilot Instructions for Dolphie

## Project Overview

Dolphie is a terminal-based user interface (TUI) application for real-time monitoring and analytics of MySQL, MariaDB, and ProxySQL databases. It provides a comprehensive dashboard with multiple panels for monitoring database performance, processlist, replication, metadata locks, and more.

## Architecture & Technology Stack

### Core Technologies
- **Python 3.8.1+**: Main language
- **Textual 5.x**: Modern TUI framework for terminal interfaces
- **Rich**: Terminal styling and formatting
- **PyMySQL**: MySQL/MariaDB database connectivity
- **Poetry**: Dependency management and packaging
- **SQLite + ZSTD**: Session recording and replay functionality

### Key Dependencies
- `loguru`: Structured logging
- `plotext`: Terminal-based graphs
- `sqlparse`: SQL query parsing and formatting
- `psutil`: System monitoring
- `orjson`: Fast JSON processing

## Project Structure

```
dolphie/
├── dolphie/
│   ├── App.py              # Main application entry point
│   ├── Dolphie.py          # Core database connection and data handling
│   ├── DataTypes.py        # Data models and type definitions
│   ├── Dolphie.tcss        # Textual CSS styling
│   ├── Modules/            # Core functionality modules
│   │   ├── ArgumentParser.py    # CLI argument processing
│   │   ├── CommandManager.py    # Hotkey and command management
│   │   ├── Functions.py         # Utility functions
│   │   ├── MySQL.py            # MySQL-specific functionality
│   │   ├── ReplayManager.py    # Session replay functionality
│   │   └── TabManager.py       # Tab management for multi-host
│   ├── Panels/             # UI panels for different views
│   │   ├── Dashboard.py         # Main dashboard panel
│   │   ├── Processlist.py       # Process list monitoring
│   │   ├── Replication.py       # Replication status
│   │   ├── MetadataLocks.py     # Metadata lock monitoring
│   │   └── ...
│   └── Widgets/            # Reusable UI components
│       ├── TopBar.py           # Application header
│       ├── CommandModal.py     # Command input dialogs
│       ├── ThreadScreen.py     # Thread detail views
│       └── ...
├── tests/                  # Test suite
├── examples/              # Example configurations
├── pyproject.toml         # Project configuration
└── README.md             # Documentation
```

## Development Guidelines

### Code Style and Patterns

1. **Follow Python PEP 8** with these specific conventions:
   - Use descriptive variable names (`connection_status` not `conn_stat`)
   - Class names in PascalCase (`DashboardPanel`)
   - Module names in snake_case (`argument_parser.py`)
   - Constants in UPPER_CASE (`CONNECTION_TIMEOUT`)

2. **Async/Await Pattern**: Use async methods for database operations and UI updates
   ```python
   @work(exclusive=True)
   async def refresh_data(self):
       """Refresh panel data asynchronously"""
       # Database operations here
   ```

3. **Error Handling**: Use try-except blocks with specific exceptions
   ```python
   try:
       result = await connection.execute(query)
   except pymysql.Error as e:
       self.log_error(f"Database error: {e}")
   ```

### Textual Framework Patterns

1. **Panel Structure**: All panels inherit from appropriate Textual widgets
   ```python
   class NewPanel(Static):
       def __init__(self, dolphie: Dolphie):
           super().__init__()
           self.dolphie = dolphie
           
       def compose(self) -> ComposeResult:
           """Define panel layout"""
           yield Container(...)
   ```

2. **Event Handling**: Use `@on` decorator for event handling
   ```python
   @on(Button.Pressed, "#refresh-button")
   def handle_refresh(self, event: Button.Pressed) -> None:
       """Handle refresh button press"""
   ```

3. **Reactive Updates**: Use reactive attributes for data binding
   ```python
   class Panel(Static):
       data = reactive({})
       
       def watch_data(self, data: dict) -> None:
           """React to data changes"""
           self.refresh_display()
   ```

### Database Interaction

1. **Connection Management**: Always use the dolphie instance for connections
   ```python
   def get_data(self):
       if not self.dolphie.connection:
           return {}
       
       cursor = self.dolphie.connection.cursor()
       # Execute queries...
   ```

2. **Query Organization**: Use the Queries modules for SQL statements
   ```python
   from dolphie.Modules.Queries import MySQLQueries
   
   query = MySQLQueries.processlist
   ```

3. **Performance Schema**: Use PerformanceSchemaMetrics for metrics collection
   ```python
   metrics = PerformanceSchemaMetrics(connection)
   data = metrics.get_statements_summary()
   ```

### Adding New Features

#### Creating a New Panel

1. Create file in `dolphie/Panels/NewPanel.py`
2. Inherit from appropriate Textual widget (Static, Container, etc.)
3. Implement required methods:
   ```python
   class NewPanel(Static):
       def __init__(self, dolphie: Dolphie):
           super().__init__()
           self.dolphie = dolphie
           
       def compose(self) -> ComposeResult:
           """Define panel layout"""
           
       @work(exclusive=True)
       async def refresh_data(self):
           """Fetch and update panel data"""
           
       def create_table(self, data: list) -> Table:
           """Create Rich table for display"""
   ```

4. Add panel to main app in `App.py`
5. Update command manager for panel hotkeys

#### Adding New Commands

1. Add command to `CommandManager.py`:
   ```python
   "new_command": {
       "human_key": "n",
       "description": "New command description"
   }
   ```

2. Handle command in app's `process_key_event` method
3. Update help text and documentation

#### Adding Database Queries

1. Add queries to appropriate module in `Modules/Queries.py`
2. Use consistent naming convention
3. Include appropriate error handling
4. Test with different MySQL/MariaDB versions

### Configuration and CLI

- Configuration uses dataclasses in `Config` class
- CLI arguments processed through `ArgumentParser`
- Support multiple configuration sources (CLI, config file, environment variables)
- Use credential profiles for secure connection management

### Testing

1. **Test Structure**: Mirror the main code structure in tests/
2. **Database Mocking**: Mock database connections for unit tests
3. **Test Categories**:
   - Unit tests for individual modules
   - Integration tests for panel functionality
   - Performance tests for data processing

```python
def test_panel_creation():
    """Test panel initialization"""
    dolphie = Mock()
    panel = NewPanel(dolphie)
    assert panel.dolphie == dolphie
```

### Recording and Replay

- Session data saved in SQLite with ZSTD compression
- Replay functionality allows debugging and analysis
- Consider replay compatibility when changing data structures
- Use `ReplayManager` for replay-specific functionality

### Performance Considerations

1. **Async Operations**: Use workers for database queries
2. **Data Caching**: Cache frequently accessed data
3. **Memory Management**: Clean up resources properly
4. **Query Optimization**: Use efficient SQL queries
5. **UI Updates**: Batch UI updates to avoid flickering

### Security Best Practices

1. **Connection Security**: Support SSL/TLS connections
2. **Credential Management**: Use login paths and credential profiles
3. **SQL Injection**: Use parameterized queries
4. **Sensitive Data**: Don't log passwords or sensitive information

### Debugging and Logging

1. **Logging**: Use loguru for structured logging
   ```python
   from loguru import logger
   logger.info("Operation completed successfully")
   logger.error(f"Error occurred: {error}")
   ```

2. **Debug Mode**: Support debug output for troubleshooting
3. **Error Reporting**: Provide meaningful error messages to users

### Common Patterns

#### Data Formatting
```python
from dolphie.Modules.Functions import format_bytes, format_number, format_time

# Format data for display
size_display = format_bytes(bytes_value)
count_display = format_number(count_value)
time_display = format_time(time_value)
```

#### Rich Table Creation
```python
def create_table(self, data: list) -> Table:
    table = Table(box=box.SIMPLE)
    table.add_column("Column 1")
    table.add_column("Column 2")
    
    for row in data:
        table.add_row(str(row.col1), str(row.col2))
    
    return table
```

#### Textual CSS Styling
- Use `.tcss` files for styling
- Follow consistent naming conventions for CSS classes
- Use semantic class names (`error-message` not `red-text`)

### Cross-Database Compatibility

- Support MySQL, MariaDB, and ProxySQL
- Handle version differences gracefully
- Use feature detection rather than version checking
- Provide fallbacks for unsupported features

### Contributing Guidelines

1. **Code Review**: All changes require review
2. **Testing**: Add tests for new functionality
3. **Documentation**: Update relevant documentation
4. **Backwards Compatibility**: Maintain compatibility when possible
5. **Performance**: Profile performance-critical changes

## Quick Reference

### Key Files to Modify
- `App.py`: Main application logic, panel management
- `Modules/CommandManager.py`: Adding new hotkeys/commands
- `Panels/`: Creating new monitoring panels
- `Modules/Queries.py`: Adding new database queries
- `DataTypes.py`: Adding new data structures

### Common Tasks
- Adding new panel: Create in `Panels/`, register in `App.py`
- Adding new command: Update `CommandManager.py` and `App.py`
- Adding new query: Update appropriate `Queries.py` module
- Styling changes: Modify `Dolphie.tcss` file

This comprehensive guide should help developers contribute effectively to the Dolphie project while maintaining code quality and consistency.