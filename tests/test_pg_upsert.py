#!/usr/bin/env python

import pytest
from dotenv import load_dotenv
from psycopg2.sql import SQL, Identifier, Literal

load_dotenv()


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
