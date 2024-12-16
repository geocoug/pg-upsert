#!/usr/bin/env python

from __future__ import annotations

import getpass
import logging
from urllib.parse import urlparse, urlunparse

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.sql import Composable

from .__version__ import __description__, __title__, __version__

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
        # If a URI is supplied, check for a password and prompt if necessary
        if uri:
            uri = self._prompt_for_password(uri)
        self.conn = conn or psycopg2.connect(uri, **kwargs)
        self.encoding = encoding
        self.in_transaction = False
        self.kwargs = kwargs
        if not self._is_valid_connection():
            raise psycopg2.Error(f"Error connecting to {self.conn.dsn}")

    def __repr__(self) -> str:
        """Return a string representation of the object."""
        params = self.conn.get_dsn_parameters() if self.conn else "No connection"
        return f"{self.__class__.__name__}({params})"

    def __del__(self):
        """Ensure the database connection is closed when the object is deleted, if open."""
        if hasattr(self, "conn") and self.conn and not self.conn.closed:
            self.close()

    def _prompt_for_password(self, uri: str) -> str:
        """Prompt the user for a password."""
        try:
            parsed_uri = urlparse(uri)
            if parsed_uri.password:
                return uri
            prompt = f"The library {__title__} wants the password for PostgresDB(uri={uri}): "
            return urlunparse(
                parsed_uri._replace(
                    netloc=f"{parsed_uri.username}:{getpass.getpass(prompt)}@{parsed_uri.hostname}:{parsed_uri.port}",
                ),
            )
        except (KeyboardInterrupt, EOFError) as err:
            raise err

    def _is_valid_connection(self) -> bool:
        """Check if the database connection is valid."""
        try:
            with self.conn.cursor():
                self.conn.set_client_encoding(self.encoding)
            return True
        except psycopg2.Error:
            return False

    def open_db(self) -> None:
        """Ensure the database connection is open."""
        if not self.conn or self.conn.closed:
            logger.debug("Opening database connection.")
            self.conn = psycopg2.connect(self.conn.dsn, **self.kwargs)
            self.conn.set_client_encoding(self.encoding)
            self.conn.set_session(autocommit=False)
        elif self.conn.closed:
            logger.warning("Connection is closed; attempting to reopen.")
            self.conn = psycopg2.connect(self.conn.dsn, **self.kwargs)
            self.conn.set_client_encoding(self.encoding)

    def cursor(self):
        """Return a cursor for executing database queries."""
        self.open_db()
        return self.conn.cursor(cursor_factory=DictCursor)

    def close(self) -> None:
        """Close the database connection if open."""
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
        except Exception:
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
                if self.encoding:
                    r = [(c.decode(self.encoding, "backslashreplace") if isinstance(c, bytes) else c) for c in row]
                else:
                    r = row
                return dict(zip(headers, r, strict=True))
            return None

        return (iter(dict_row, None), headers, curs.rowcount)
