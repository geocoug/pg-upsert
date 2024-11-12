#!/usr/bin/env python

import psycopg2
import pytest
from dotenv import load_dotenv
from psycopg2.sql import SQL, Identifier, Literal

from pg_upsert.pg_upsert import PostgresDB

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
    PostgresDB(uri=global_variables["URI"])
    conn = psycopg2.connect(global_variables["URI"])
    PostgresDB(conn=conn)
    conn.close()


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
