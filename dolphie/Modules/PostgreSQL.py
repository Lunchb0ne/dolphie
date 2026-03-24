from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from loguru import logger
from textual.app import App

from dolphie.DataTypes import ConnectionSource, ConnectionSourceType, DatabaseRow, DatabaseScalar
from dolphie.Modules.Functions import coerce_str, escape_markup
from dolphie.Modules.ManualException import ManualException

QueryValues = Sequence[Any] | Mapping[str, Any] | None

# SQLSTATEs that mean the server went away: admin_shutdown, crash_shutdown, cannot_connect_now
_CONNECTION_LOST_CODES = {"57P01", "57P02", "57P03"}
_INSUFFICIENT_PRIVILEGE_CODE = "42501"


class PostgreSQLDatabase:
    """A PostgreSQL connection with the same interface Dolphie uses for MySQL's Database."""

    def __init__(
        self,
        app: App,
        host: str,
        user: str | None,
        password: str | None,
        port: int,
        dbname: str = "postgres",
        save_connection_id: bool = True,
        daemon_mode: bool = False,
    ):
        self.app = app
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.dbname = dbname
        self.save_connection_id = save_connection_id

        self.connection: psycopg2.extensions.connection | None = None
        self.cursor: psycopg2.extras.RealDictCursor | None = None
        self.connection_id: int | None = None
        self.source: ConnectionSourceType | None = None
        self.is_running_query: bool = False
        self.has_connected: bool = False
        self.last_execute_successful: bool = False
        # Track queries that have already shown privilege error notifications.
        self.privilege_errors_notified: set[str] = set()

        self.max_reconnect_attempts: int = 999999999 if daemon_mode else 3

    def connect(self, reconnect_attempt: bool = False):
        try:
            connection = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=int(self.port),
                dbname=self.dbname,
                connect_timeout=5,
                application_name="Dolphie",
            )
            connection.autocommit = True
            self.connection = connection
            self.cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            self.source = ConnectionSource.postgresql

            # Backend PID for processlist filtering
            if self.save_connection_id:
                self.connection_id = connection.get_backend_pid()

            logger.info(f"Connected to {self.source} with Process ID {self.connection_id}")
            self.has_connected = True
        except psycopg2.Error as e:
            error_message = str(e).strip()
            if reconnect_attempt:
                logger.error(f"Failed to reconnect to {ConnectionSource.postgresql}: {error_message}")
                self.app.notify(
                    (
                        f"[$b_light_blue]{self.host}:{self.port}[/$b_light_blue]: "
                        f"Failed to reconnect to PostgreSQL: {escape_markup(error_message)}"
                    ),
                    title="PostgreSQL Reconnection Failed",
                    severity="error",
                    timeout=10,
                )
            else:
                raise ManualException(error_message) from e

    def close(self):
        connection = self.connection
        if connection is not None and self.is_connected():
            connection.close()

    def is_connected(self) -> bool:
        return bool(self.connection and self.connection.closed == 0)

    @staticmethod
    def _decode_value(value: object) -> DatabaseScalar:
        if isinstance(value, (str, int, float, Decimal, date, datetime, timedelta)) or value is None:
            return value
        return coerce_str(value)

    def _process_row(self, row: Mapping[str, object]) -> DatabaseRow:
        return {field: self._decode_value(value) for field, value in row.items()}

    def fetchall(self) -> list[DatabaseRow]:
        cursor = self.cursor
        if not self.is_connected() or not self.last_execute_successful or cursor is None:
            return []

        try:
            rows = cursor.fetchall()
        except psycopg2.ProgrammingError:  # The statement returned no rows to fetch
            return []
        return [self._process_row(row) for row in rows]

    def fetchone(self) -> DatabaseRow:
        cursor = self.cursor
        if not self.is_connected() or not self.last_execute_successful or cursor is None:
            return {}

        try:
            row = cursor.fetchone()
        except psycopg2.ProgrammingError:  # The statement returned no rows to fetch
            return {}
        return self._process_row(row) if row else {}

    def fetch_value_from_field(
        self,
        query: str,
        field: str | None = None,
        values: QueryValues = None,
        ignore_error: bool = False,
    ) -> DatabaseScalar:
        self.execute(query, values, ignore_error=ignore_error)
        data = self.fetchone()
        if not data:
            return None

        return data.get(field or next(iter(data)))

    def execute(self, query: str, values: QueryValues = None, ignore_error: bool = False) -> int | None:
        if not self.is_connected():
            self.last_execute_successful = False
            return None

        if self.is_running_query:
            self.app.notify(
                "Another query is already running, please repeat action",
                title="Unable to run multiple queries at the same time",
                severity="error",
                timeout=10,
            )
            self.last_execute_successful = False
            return None

        # Skip queries that already failed with a privilege error to save a database call
        if query in self.privilege_errors_notified:
            self.last_execute_successful = False
            return None

        for attempt_number in range(self.max_reconnect_attempts):
            self.is_running_query = True

            try:
                cursor = self.cursor
                if cursor is None:
                    raise ManualException("PostgreSQL cursor is not available", query=query)
                cursor.execute(query, values)
                self.is_running_query = False
                self.last_execute_successful = True

                return cursor.rowcount
            except psycopg2.Error as e:
                self.is_running_query = False
                self.last_execute_successful = False

                error_code = e.pgcode
                error_message = str(e).strip()

                # Show a notification only the first time a query fails with a privilege error
                if error_code == _INSUFFICIENT_PRIVILEGE_CODE:
                    if query not in self.privilege_errors_notified:
                        self.privilege_errors_notified.add(query)

                        logger.warning(
                            f"Privilege error (code {error_code}): {error_message}. Query: {query}. "
                            "This query will be skipped and stats for this feature won't be available."
                        )
                        self.app.notify(
                            f"[$b_highlight]{self.host}:{self.port}[/$b_highlight]: [dim]{error_code}: "
                            f"{escape_markup(error_message)}[/dim]\n"
                            f"Query: [$b_light_blue]{escape_markup(query)}[/$b_light_blue]\n"
                            "Stats for this feature won't be available.",
                            title="Insufficient Privileges",
                            severity="warning",
                            timeout=9,
                        )

                    return None

                if ignore_error:
                    return None

                # psycopg2 closes the connection on client-side errors. Class 08 is a connection exception
                if (
                    not self.is_connected()
                    or error_code in _CONNECTION_LOST_CODES
                    or (error_code and error_code.startswith("08"))
                ):
                    logger.error(f"{self.source} has lost its connection: {error_message}, attempting to reconnect...")
                    self.app.notify(
                        f"[$b_light_blue]{self.host}:{self.port}[/$b_light_blue]: {escape_markup(error_message)}",
                        title="PostgreSQL Connection Lost",
                        severity="error",
                        timeout=10,
                    )

                    self.close()
                    self.connect(reconnect_attempt=True)

                    if not self.is_connected():
                        # Exponential backoff, capped at 20 seconds
                        time.sleep(min(1 * (2**attempt_number), 20))
                        continue

                    self.app.notify(
                        f"[$b_light_blue]{self.host}:{self.port}[/$b_light_blue]: Successfully reconnected",
                        title="PostgreSQL Connection Created",
                        severity="information",
                        timeout=10,
                    )

                    # Retry the query
                    return self.execute(query, values)

                reason = f"{error_code}: {error_message}" if error_code else error_message
                raise ManualException(reason, query=query) from e

        if not self.is_connected():
            raise ManualException(
                f"Failed to reconnect to {ConnectionSource.postgresql} after {self.max_reconnect_attempts} attempts",
                query=query,
            )

        return None
