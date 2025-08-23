import time
from ssl import SSLError

import psycopg2
import psycopg2.extras
from loguru import logger
from textual.app import App

from dolphie.DataTypes import ConnectionSource
from dolphie.Modules.ManualException import ManualException


class PostgreSQLDatabase:
    def __init__(
        self,
        app: App,
        host: str,
        user: str,
        password: str,
        socket: str,
        port: int,
        ssl: str,
        database: str = None,
        save_connection_id: bool = True,
        auto_connect: bool = True,
        daemon_mode: bool = False,
    ):
        self.app = app
        self.host = host
        self.user = user
        self.password = password
        self.socket = socket
        self.port = port
        self.ssl = ssl
        self.database = database
        self.save_connection_id = save_connection_id
        self.daemon_mode = daemon_mode

        self.connection = None
        self.cursor = None
        self.source = ConnectionSource.postgresql
        self.connection_id = None
        self.has_connected = False
        self.is_running_query = False

        if daemon_mode:
            self.max_reconnect_attempts: int = 999999999
        else:
            self.max_reconnect_attempts: int = 3

        if auto_connect:
            self.connect()

    def connect(self, reconnect_attempt: bool = False):
        try:
            # Build connection string
            conn_params = {
                "host": self.host,
                "user": self.user,
                "password": self.password,
                "port": int(self.port),
                "connect_timeout": 5,
                "application_name": "Dolphie",
            }

            # Add database if specified
            if self.database:
                conn_params["database"] = self.database

            # Handle SSL mode
            if self.ssl:
                if isinstance(self.ssl, dict):
                    ssl_mode = self.ssl.get("ssl_mode", "prefer")
                else:
                    ssl_mode = "prefer"
                conn_params["sslmode"] = ssl_mode

            # Handle Unix socket
            if self.socket:
                conn_params["host"] = self.socket
                del conn_params["port"]

            self.connection = psycopg2.connect(**conn_params)
            self.connection.autocommit = True
            self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            # Get connection ID for processlist filtering
            if self.save_connection_id:
                self.cursor.execute("SELECT pg_backend_pid()")
                result = self.cursor.fetchone()
                self.connection_id = result["pg_backend_pid"] if result else None

            self.has_connected = True

            if not reconnect_attempt:
                logger.info(f"Connected to PostgreSQL with Process ID {self.connection_id}")

        except psycopg2.Error as e:
            error_message = str(e).strip()
            
            if not reconnect_attempt:
                logger.error(f"PostgreSQL connection error: {error_message}")
                raise ManualException(error_message)

    def execute(self, query, values=None, ignore_error=False):
        if not self.is_connected():
            return None

        if self.is_running_query:
            self.app.notify(
                "Another query is already running, please repeat action",
                title="Unable to run multiple queries at the same time",
                severity="error",
                timeout=10,
            )
            return None

        # Prefix all queries with Dolphie so they can be easily identified in pg_stat_activity
        query = "/* Dolphie */ " + query

        for attempt_number in range(self.max_reconnect_attempts):
            self.is_running_query = True
            error_message = None

            try:
                self.cursor.execute(query, values)
                self.is_running_query = False
                return self.cursor.rowcount

            except AttributeError:
                # If the cursor is not defined, reconnect and try again
                self.is_running_query = False
                self.close()
                self.connect()
                time.sleep(1)

            except psycopg2.Error as e:
                self.is_running_query = False

                if ignore_error:
                    return None

                error_message = str(e).strip()

                # Check if the error is due to a connection issue
                if self.is_connection_error(e) or (self.daemon_mode and self.has_connected):
                    if error_message:
                        logger.error(
                            f"PostgreSQL connection error (attempt {attempt_number + 1}): {error_message}"
                        )

                    # Exponential backoff for reconnection attempts
                    if attempt_number < self.max_reconnect_attempts - 1:
                        backoff_time = min(2 ** attempt_number, 60)
                        time.sleep(backoff_time)

                        self.close()
                        self.connect(reconnect_attempt=True)
                    else:
                        raise ManualException(error_message)
                else:
                    # Non-connection error, raise immediately
                    raise ManualException(error_message)

        return None

    def is_connection_error(self, error):
        """Check if the error is a connection-related error"""
        return isinstance(error, (psycopg2.OperationalError, psycopg2.InterfaceError))

    def fetchall(self):
        if self.cursor:
            return self.cursor.fetchall()
        return []

    def fetchone(self):
        if self.cursor:
            return self.cursor.fetchone()
        return None

    def is_connected(self):
        try:
            if self.connection and not self.connection.closed:
                # Test the connection with a simple query
                with self.connection.cursor() as test_cursor:
                    test_cursor.execute("SELECT 1")
                return True
        except (psycopg2.Error, AttributeError):
            pass
        return False

    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
        except psycopg2.Error:
            pass
        finally:
            self.cursor = None

        try:
            if self.connection:
                self.connection.close()
        except psycopg2.Error:
            pass
        finally:
            self.connection = None

        self.has_connected = False