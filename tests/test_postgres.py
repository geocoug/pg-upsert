#!/usr/bin/env python

import getpass
from unittest.mock import patch

import psycopg2
import pytest
from dotenv import load_dotenv
from psycopg2.sql import SQL, Identifier, Literal

from pg_upsert import PostgresDB

load_dotenv()


def test_db_init_no_conn(global_variables):
    """Test that PostgresDB raises an AttributeError if neither
    a connection URI nor an existing connection object is provided."""
    with pytest.raises(AttributeError):
        PostgresDB()


def test_db_init_bad_conn(global_variables):
    """Test that PostgresDB raises an error if the connection fails."""
    with pytest.raises(psycopg2.Error):
        PostgresDB(uri="postgresql://user:pass@localhost:5432/db")


def test_db_init_conn(global_variables):
    """Test that PostgresDB initializes with a connection URI.

    Also test that PostgresDB initializes with an existing connection object."""
    # First, test that PostgresDB raises an error if no connection or URI is provided
    with pytest.raises(AttributeError):
        PostgresDB()
    uri = f"postgresql://{global_variables['POSTGRES_USER']}@{global_variables['POSTGRES_HOST']}:{global_variables['POSTGRES_PORT']}/{global_variables['POSTGRES_DB']}"
    # Initialize PostgresDB with the URI and check that PostgresDB prompts for a password with getpass
    with patch(
        "getpass.getpass",
        return_value=global_variables["POSTGRES_PASSWORD"],
    ) as mock_getpass:
        PostgresDB(uri=uri)
        assert mock_getpass.called
    # Now use an invalid password
    with pytest.raises(psycopg2.Error):
        with patch("getpass.getpass", return_value="wrongpassword") as mock_getpass:
            PostgresDB(uri=uri)
            assert mock_getpass.called
    # Test that PostgresDB throws an error with initializing with an invalid connection object
    with pytest.raises(psycopg2.Error):
        PostgresDB(conn=psycopg2.connect(uri))
    # Test that PostgresDB ignores the URI if an existing connection object is provided
    conn = psycopg2.connect(global_variables["URI"])
    with patch("psycopg2.connect") as mock_connect:
        PostgresDB(uri=global_variables["URI"], conn=conn)
        assert not mock_connect.called
    # Test that a keyboard interrupt or EOFError is raised when the user cancels the password prompt
    with patch("getpass.getpass", side_effect=KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt):
            PostgresDB(uri=uri)
    with patch("getpass.getpass", side_effect=EOFError):
        with pytest.raises(EOFError):
            PostgresDB(uri=uri)


def test_db_execute(db):
    """Test a simple query execution."""
    cur = db.execute("SELECT 1")
    assert cur.fetchone()[0] == 1


def test_db_execute_values(db):
    """Test a query execution with values."""
    cur = db.execute(SQL("SELECT {}").format(Literal(1)))
    assert cur.fetchone()[0] == 1
    cur = db.execute(
        SQL(
            """
            select table_name from information_schema.tables
            where table_schema={schema} and {column}={value}
        """,
        ).format(
            schema=Literal("staging"),
            column=Identifier("table_name"),
            value=Literal("genres"),
        ),
    )
    assert cur.fetchone()[0] == "genres"


def test_db_rowdict(db):
    """Test the rowdict function."""
    rows, headers, rowcount = db.rowdict("SELECT 1 as one, 2 as two")
    assert iter(rows)
    assert headers == ["one", "two"]
    assert rowcount == 1
    rows = list(rows)
    assert rows[0]["one"] == 1
    assert rows[0]["two"] == 2
    # Test that the rowdict function returns an empty list if no rows are returned
    rows, headers, rowcount = db.rowdict("SELECT 1 as one, 2 as two WHERE 1=0")
    assert not list(rows)


def test_db_rowdict_params(db):
    """Test the rowdict function with parameters."""
    rows, headers, rowcount = db.rowdict(
        SQL("SELECT {one} as one, {two} as two").format(
            one=Literal(1),
            two=Literal(2),
        ),
    )
    assert iter(rows)
    assert headers == ["one", "two"]
    assert rowcount == 1
    rows = list(rows)
    assert rows[0]["one"] == 1
    assert rows[0]["two"] == 2
