#!/usr/bin/env python

# import logging
import logging
import os

import pytest
from dotenv import load_dotenv
from psycopg2.sql import SQL, Identifier, Literal

from pg_upsert.pg_upsert import PgUpsert, PostgresDB

load_dotenv()

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def global_variables():
    """Set global variables for the test session."""
    return {
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "localhost"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT", 5432),
        "POSTGRES_DB": os.getenv("POSTGRES_DB", "postgres"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER", "postgres"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }


@pytest.fixture(scope="session")
def db(global_variables):
    """Return a PostgresDB object."""
    db = PostgresDB(
        host=global_variables["POSTGRES_HOST"],
        database=global_variables["POSTGRES_DB"],
        user=global_variables["POSTGRES_USER"],
        passwd=global_variables["POSTGRES_PASSWORD"],
    )
    yield db
    db.execute(
        """
        delete from public.book_authors;
        delete from public.authors;
        delete from public.books;
        delete from public.genres;
    """,
    )
    db.close()


@pytest.fixture(scope="session")
def ups(global_variables):
    """Return a PgUpsert object."""
    obj = PgUpsert(
        host=global_variables["POSTGRES_HOST"],
        database=global_variables["POSTGRES_DB"],
        user=global_variables["POSTGRES_USER"],
        passwd=global_variables["POSTGRES_PASSWORD"],
        tables=("genres", "books", "authors", "book_authors"),
        stg_schema="staging",
        base_schema="public",
        do_commit=False,
        interactive=False,
        upsert_method="upsert",
    )
    yield obj
    obj.db.close()


def test_db_connection(db):
    """Test the database connection is successful, then close it."""
    assert db.conn is None
    db.open_db()
    assert db.conn is not None
    db.close()
    assert db.conn is None


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


def test_pgupsert_init(ups):
    """Test the PgUpsert object was initialized with the correct values."""
    assert ups.tables == ("genres", "books", "authors", "book_authors")
    assert ups.stg_schema == "staging"
    assert ups.base_schema == "public"
    assert ups.do_commit is False
    assert ups.interactive is False
    assert ups.upsert_method == "upsert"
    assert ups.control_table == "ups_control"
    assert ups.exclude_cols == ()
    assert ups.exclude_null_check_cols == ()


def test_pgupsert_control_table_init(ups):
    """Test that the control table was initialized with the correct columns and values."""
    cur = ups.db.execute(
        SQL(
            "select table_name from information_schema.tables where table_name={table}",
        ).format(
            table=Literal(ups.control_table),
        ),
    )
    assert cur.rowcount == 1
    assert cur.fetchone()[0] == ups.control_table
    # Test that the control table has the correct columns
    cur = ups.db.execute(
        SQL(
            "select column_name from information_schema.columns where table_name={table}",
        ).format(
            table=Literal(ups.control_table),
        ),
    )
    # Verify that all columns are present
    assert cur.rowcount == 10
    columns = [
        "table_name",
        "exclude_cols",
        "exclude_null_checks",
        "interactive",
        "null_errors",
        "pk_errors",
        "fk_errors",
        "ck_errors",
        "rows_updated",
        "rows_inserted",
    ]
    ctrl_columns = [row[0] for row in cur.fetchall()]
    for column in columns:
        assert column in ctrl_columns
    # Verify the default values
    cur = ups.db.execute(
        SQL(
            """select * from {control_table}
        where
            coalesce(exclude_cols, exclude_null_checks, null_errors,
            pk_errors, fk_errors, ck_errors) is not null
            or interactive
            or rows_updated != 0
            or rows_inserted != 0
            or table_name is null;
        ;""",
        ).format(control_table=Identifier(ups.control_table)),
    )
    assert cur.rowcount == 0


def test_pgupsert_upsert_methods(ups):
    """Test the upsert_methods function returns the correct methods."""
    assert ups._upsert_methods() == ("upsert", "update", "insert")


def test_pgupsert_validate_schemas(ups):
    """Test the validate_schemas function."""
    assert ups._validate_schemas() is None
    ups.stg_schema = "staging2"
    with pytest.raises(ValueError):
        ups._validate_schemas()
    ups.stg_schema = "staging"
    ups.base_schema = "public2"
    with pytest.raises(ValueError):
        ups._validate_schemas()
    ups.base_schema = "public"
    assert ups._validate_schemas() is None


def test_pgupsert_validate_table(ups):
    """Test the validate_table function."""
    assert ups._validate_table("genres") is None
    with pytest.raises(ValueError):
        ups._validate_table("genres2")
    assert ups._validate_table("genres") is None


def test_pgupsert_validate_control(ups):
    """Test the validate_control function."""
    pass
