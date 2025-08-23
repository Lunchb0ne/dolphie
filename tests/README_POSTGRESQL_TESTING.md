# PostgreSQL Testing Documentation

## Testing Infrastructure for Dolphie PostgreSQL Support

This document describes the comprehensive testing setup for PostgreSQL functionality in Dolphie.

## Test Types

### 1. Unit Tests (`test_postgresql_unit.py`)
- **Purpose**: Test individual components in isolation using mocks
- **Dependencies**: None (uses mocks)
- **Coverage**: 
  - Connection management
  - Query execution logic
  - Error handling
  - SSL configuration
  - Socket connections
  - Reconnection behavior
  - Query prefixing

**Run with:** `python -m pytest tests/dolphie/Modules/test_postgresql_unit.py -v`

### 2. Simple Integration Tests (`test_postgresql_simple_integration.py`) 
- **Purpose**: Test integration without requiring real database
- **Dependencies**: None (uses mocks for database, real for parsing)
- **Coverage**:
  - Query syntax validation
  - PostgreSQL-specific SQL features
  - URI parsing
  - Query parameter handling

**Run with:** `python -m pytest tests/dolphie/Modules/test_postgresql_simple_integration.py -v`

### 3. Full Integration Tests (`test_postgresql_integration.py`)
- **Purpose**: Test with real PostgreSQL database instances
- **Dependencies**: Docker or PostgreSQL server
- **Coverage**:
  - Real database connectivity
  - All PostgreSQL queries execution
  - Query termination operations
  - Connection pooling
  - Database-specific functionality

**Run with Docker:** `python -m pytest tests/dolphie/Modules/test_postgresql_integration.py -v`

### 4. Performance Tests (`test_postgresql_performance.py`)
- **Purpose**: Test performance and stress scenarios
- **Dependencies**: Docker or PostgreSQL server
- **Coverage**:
  - Connection speed
  - Query execution performance
  - Concurrent connections
  - Memory usage
  - Stress testing
  - Error recovery

**Run with Docker:** `python -m pytest tests/dolphie/Modules/test_postgresql_performance.py -v`

## Running Tests

### Quick Test (No Dependencies)
```bash
# Run unit tests and simple integration tests
python -m pytest tests/dolphie/Modules/test_postgresql_unit.py tests/dolphie/Modules/test_postgresql_simple_integration.py -v
```

### Basic PostgreSQL Tests
```bash
# Run existing tests that don't require special setup
python -m pytest tests/dolphie/Modules/test_postgresql.py -v
```

### Full Test Suite (Requires Docker)
```bash
# Install Docker testing dependencies
pip install testcontainers

# Run all PostgreSQL tests
python -m pytest tests/dolphie/Modules/test_postgresql*.py -v
```

### Specific Test Categories
```bash
# Unit tests only
python -m pytest tests/dolphie/Modules/test_postgresql_unit.py -v

# Integration tests with real database
python -m pytest tests/dolphie/Modules/test_postgresql_integration.py -v

# Performance tests
python -m pytest tests/dolphie/Modules/test_postgresql_performance.py -v
```

## Test Configuration

### PostgreSQL Container Setup
The integration tests use `testcontainers` to automatically start PostgreSQL containers:

```python
@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:15") as postgres:
        yield postgres
```

### Test Database Setup
- **Image**: postgres:15
- **Default User**: test
- **Default Password**: test  
- **Default Database**: test
- **Automatic Cleanup**: Yes

### Alternative: Manual PostgreSQL Setup
If you prefer to use a local PostgreSQL instance instead of Docker:

1. Install PostgreSQL locally
2. Create test database:
   ```sql
   CREATE DATABASE test_dolphie;
   CREATE USER test_user WITH PASSWORD 'test_pass';
   GRANT ALL PRIVILEGES ON DATABASE test_dolphie TO test_user;
   ```
3. Set environment variables:
   ```bash
   export POSTGRES_HOST=localhost
   export POSTGRES_PORT=5432
   export POSTGRES_USER=test_user
   export POSTGRES_PASSWORD=test_pass
   export POSTGRES_DB=test_dolphie
   ```

## Test Coverage

### Functionality Covered
- ✅ PostgreSQL URI parsing and connection
- ✅ All PostgreSQL-specific queries
- ✅ Query execution and result handling
- ✅ Connection management and reconnection
- ✅ Error handling (connection and query errors)
- ✅ SSL and socket connections
- ✅ Query termination (pg_cancel_backend, pg_terminate_backend)
- ✅ Query prefixing for identification
- ✅ Performance under load
- ✅ Concurrent connections
- ✅ Memory usage with large datasets
- ✅ Edge cases and error recovery

### PostgreSQL Features Tested
- System variables (pg_settings)
- Process list (pg_stat_activity)
- Database statistics (pg_stat_database)
- Table statistics (pg_stat_user_tables)
- Index statistics (pg_stat_user_indexes)
- Replication monitoring (pg_stat_replication)
- Lock information (pg_locks)
- Query performance (pg_stat_statements)
- Connection statistics
- Version information

## CI/CD Integration

### GitHub Actions Example
```yaml
name: PostgreSQL Tests
on: [push, pull_request]
jobs:
  test-postgresql:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.8'
      - name: Install dependencies
        run: |
          pip install -e .
          pip install pytest testcontainers
      - name: Run unit tests
        run: pytest tests/dolphie/Modules/test_postgresql_unit.py -v
      - name: Run simple integration tests
        run: pytest tests/dolphie/Modules/test_postgresql_simple_integration.py -v
      - name: Run full integration tests
        run: pytest tests/dolphie/Modules/test_postgresql_integration.py -v
```

## Debugging Tests

### Enable Debug Logging
```bash
# Run tests with debug output
python -m pytest tests/dolphie/Modules/test_postgresql*.py -v -s --log-cli-level=DEBUG
```

### Run Specific Test
```bash
# Run a specific test method
python -m pytest tests/dolphie/Modules/test_postgresql_unit.py::TestPostgreSQLDatabase::test_connect_success -v
```

### Show Test Coverage
```bash
# Install coverage tool
pip install pytest-cov

# Run tests with coverage
python -m pytest tests/dolphie/Modules/test_postgresql*.py --cov=dolphie.Modules.PostgreSQL --cov-report=html
```

## Test Data and Fixtures

### Shared Fixtures (`conftest.py`)
- `postgres_container`: PostgreSQL Docker container
- `postgres_config`: Configuration for test database
- `mock_app`: Mocked Textual application
- `real_postgres_db`: Real PostgreSQL database connection
- `sample_postgres_data`: Sample data for testing

### Test Utilities
The tests include utilities for:
- Creating test tables and data
- Mocking PostgreSQL responses
- Performance benchmarking
- Error injection
- Connection lifecycle management

## Extending Tests

### Adding New Query Tests
1. Add query to `PostgreSQLQueries` class
2. Add unit test in `test_postgresql_unit.py`
3. Add integration test in `test_postgresql_integration.py`
4. Test query syntax in `test_postgresql_simple_integration.py`

### Adding Performance Tests
1. Add test method to `TestPostgreSQLPerformance` class
2. Use appropriate fixtures for real database connection
3. Include performance assertions (time limits)
4. Test with various data sizes

### Adding Error Scenarios
1. Add test to `TestPostgreSQLErrorHandling` class
2. Use appropriate mock configurations
3. Test both connection and query errors
4. Verify error recovery behavior

## Dependencies

### Required for All Tests
- pytest
- psycopg2-binary
- unittest.mock (built-in)

### Required for Integration Tests
- testcontainers
- docker (Docker daemon running)

### Optional for Enhanced Testing
- pytest-cov (coverage reporting)
- pytest-xdist (parallel test execution)
- pytest-benchmark (performance benchmarking)

## Security Considerations

### Test Data
- All test data is automatically generated
- No real credentials or sensitive data in tests
- Test databases are isolated and temporary

### Container Security
- PostgreSQL containers are ephemeral
- No persistent data storage
- Containers are automatically cleaned up

### Network Security
- Tests use local connections only
- No external network dependencies
- Isolated test network in Docker