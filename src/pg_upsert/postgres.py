#!/usr/bin/env python

from __future__ import annotations

import getpass
import logging
from urllib.parse import urlparse, urlunparse

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.sql import Composable

logger = logging.getLogger(__name__)


class PostgresDB:
    """Base database object for connecting and executing SQL queries on a PostgreSQL database.

    Args:
        conn (psycopg2.extensions.connection, optional): An existing connection object to a PostgreSQL database.
        uri (str, optional): A connection URI for a PostgreSQL database.
        encoding (str, optional): The encoding to use for the database connection.
        **kwargs: Additional keyword arguments passed to `psycopg2.connect()`.

    Returns:
        PostgresDB: A new PostgresDB object for connecting to a PostgreSQL database and executing queries.

    Raises:
        AttributeError: If neither a connection URI nor an existing connection object is provided.
        psycopg2.Error: If an error occurs while connecting to the database or executing a query.
    """

    def __init__(
        self,
        uri: None | str = None,
        conn: None | psycopg2.extensions.connection = None,
        encoding: str = "utf-8",
        **kwargs,
    ):
        if conn is None and uri is None:
            raise AttributeError(
                "Either a connection URI or an existing connection object must be provided.",
            )
        if conn and uri:
            logger.warning(
                "Connection URI ignored as an existing connection object is provided.",
            )
            uri = None
        # If a URI is supplied, extract the password separately to avoid storing
        # it in the dsn (which would be exposed via conn.dsn / repr).
        self._password: str | None = None
        self._sanitized_uri: str | None = None
        self._connect_uri: str | None = None  # full URI with password for reconnection
        self._owns_connection = conn is None  # only manage connections we created
        if uri:
            uri, self._password, self._sanitized_uri = self._extract_password(uri)
            self._connect_uri = uri
        self.conn = conn or psycopg2.connect(uri, **kwargs)
        self.encoding = encoding
        self.in_transaction = False
        self._in_savepoint = False
        self.kwargs = kwargs
        if not self._is_valid_connection():
            raise psycopg2.Error(f"Error connecting to {self.conn.dsn}")

    def __repr__(self) -> str:
        """Return a string representation of the object."""
        params = self.conn.get_dsn_parameters() if self.conn else "No connection"
        return f"{self.__class__.__name__}({params})"

    def __del__(self):
        """Ensure the database connection is closed when the object is deleted, if open."""
        if getattr(self, "_owns_connection", False) and hasattr(self, "conn") and self.conn and not self.conn.closed:
            self.close()

    def _extract_password(self, uri: str) -> tuple[str, str | None, str]:
        """Extract password from URI, prompt if missing, and return (full_uri, password, sanitized_uri).

        Args:
            uri: The connection URI potentially containing a password.

        Returns:
            A tuple of (uri_with_password, password_or_none, sanitized_uri_without_password).
        """
        parsed = urlparse(uri)
        password = parsed.password
        if not password:
            # Check PGPASSWORD environment variable (standard PostgreSQL convention).
            import os

            password = os.environ.get("PGPASSWORD")
        if not password:
            user = parsed.username or "unknown"
            host = parsed.hostname or "localhost"
            port = parsed.port or 5432
            dbname = parsed.path.lstrip("/") if parsed.path else "unknown"
            from rich.console import Console as _Console

            _con = _Console(stderr=True)
            _con.print(
                f"\n  [bold]PostgreSQL[/bold] [dim]→[/dim] "
                f"[cyan]{user}[/cyan]@[cyan]{host}[/cyan]:[cyan]{port}[/cyan]/[cyan]{dbname}[/cyan]",
            )
            try:
                password = getpass.getpass("  Password: ")
            except (KeyboardInterrupt, EOFError):
                raise
            netloc = f"{parsed.username}:{password}@{parsed.hostname}"
            if parsed.port:
                netloc += f":{parsed.port}"
            uri = urlunparse(parsed._replace(netloc=netloc))
            parsed = urlparse(uri)

        # Build a sanitized URI (no password) for display/logging.
        sanitized_netloc = f"{parsed.username}@{parsed.hostname}"
        if parsed.port:
            sanitized_netloc += f":{parsed.port}"
        sanitized_uri = urlunparse(parsed._replace(netloc=sanitized_netloc))
        return uri, password, sanitized_uri

    def _is_valid_connection(self) -> bool:
        """Check if the database connection is valid."""
        try:
            with self.conn.cursor():
                self.conn.set_client_encoding(self.encoding)
            return True
        except psycopg2.Error:
            return False

    def open_db(self) -> None:
        """Ensure the database connection is open.

        Reconnects using the original URI (stored at init time) if the
        connection was created by this instance.  External connections
        (passed via ``conn=``) cannot be reopened.
        """
        if not self.conn or self.conn.closed:
            logger.debug("Opening database connection.")
            if self._connect_uri:
                self.conn = psycopg2.connect(self._connect_uri, **self.kwargs)
            elif not self._owns_connection and self.conn:
                # External connection — try DSN (may lack password).
                self.conn = psycopg2.connect(self.conn.dsn, **self.kwargs)
            else:
                raise psycopg2.OperationalError(
                    "Cannot reopen connection: no stored credentials. "
                    "Connections passed via conn= cannot be automatically reopened.",
                )
            self.in_transaction = False
            self.conn.set_client_encoding(self.encoding)
            self.conn.set_session(autocommit=False)

    def cursor(self):
        """Return a cursor for executing database queries."""
        self.open_db()
        return self.conn.cursor(cursor_factory=DictCursor)

    def close(self) -> None:
        """Close the database connection if open and owned by this instance.

        Connections provided externally via ``conn=`` are never closed —
        the caller retains ownership and is responsible for closing them.
        """
        if not self._owns_connection:
            return
        if self.conn and not self.conn.closed:
            self.rollback()
            self.conn.close()

    def commit(self) -> None:
        """Commit the current transaction."""
        if self.conn and self.in_transaction:
            self.conn.commit()
            self.in_transaction = False

    def rollback(self) -> None:
        """Rollback the current transaction."""
        if self.conn and self.in_transaction:
            self.conn.rollback()
            self.in_transaction = False

    def execute(
        self: PostgresDB,
        sql: str | Composable,
        params=None,
    ) -> psycopg2.extensions.cursor:
        """A shortcut to self.cursor().execute() that handles encoding.

        Handles insert, updates, deletes

        Args:
            sql (str | psycopg2.sql.Composable): The SQL query to execute. Accepts a `str` or `Composable` object.
            params (tuple, optional): A tuple of parameters to pass to the query.
                Note that a `Composable` object should not have parameters passed separately. Default is None.
        Returns:
            psycopg2.extensions.cursor: A cursor object for the executed query.
        """
        self.in_transaction = True
        try:
            curs = self.cursor()
            if isinstance(sql, Composable):
                logger.debug(f"\n{sql.as_string(curs)}")
                curs.execute(sql)
            else:
                if params is None:
                    logger.debug(f"\n{sql}")
                    curs.execute(sql.encode(self.encoding))
                else:
                    logger.debug(f"\nSQL:\n{sql}\nParameters:\n{params}")
                    curs.execute(sql.encode(self.encoding), params)
        except psycopg2.Error:
            if not self._in_savepoint:
                self.rollback()
            raise
        return curs

    def rowdict(self: PostgresDB, sql: str | Composable, params=None) -> tuple:
        """Convert a cursor object to an iterable that yields dictionaries of row data.

        yields dictionaries of row data with the following structure:
            0) dict_row (iterator) - an iterator that yields dictionaries of row data
            1) headers (list) - a list of column names
            2) rowcount (int) - the number of rows returned by the query
        """
        curs = self.execute(sql, params)
        if not curs.description:
            # No data returned
            return (iter([]), [], 0)
        headers = [d[0] for d in curs.description]

        def dict_row():
            """Convert a data row to a dictionary."""
            row = curs.fetchone()
            if row:
                r = [(c.decode(self.encoding, "backslashreplace") if isinstance(c, bytes) else c) for c in row]
                return dict(zip(headers, r, strict=True))
            return None

        return (iter(dict_row, None), headers, curs.rowcount)
