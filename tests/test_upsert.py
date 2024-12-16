#!/usr/bin/env python

import logging
import tkinter as tk
import unittest
from unittest.mock import MagicMock, patch

import pytest
from dotenv import load_dotenv
from psycopg2.sql import SQL, Identifier, Literal

from pg_upsert.upsert import PgUpsert

load_dotenv()

logger = logging.getLogger(__name__)


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


def test_pgupsert_init_ups_control(ups):
    """Test the init_ups_control function.

    Verify that the expected columns are present in the control table:
    - table_name
    - exclude_cols
    - exclude_null_checks
    - interactive
    - null_errors
    - pk_errors
    - fk_errors
    - ck_errors
    - rows_updated
    - rows_inserted

    Also verify that the initial upsert tables are in the control table with the correct values.
    """
    ups._init_ups_control()
    cur = ups.db.execute(
        SQL(
            "select table_name from {control_table}",
        ).format(
            control_table=Identifier(ups.control_table),
        ),
    )
    assert cur.rowcount == 4
    tables = [row[0] for row in cur.fetchall()]
    for table in ups.tables:
        assert table in tables
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
    for table in ups.tables:
        cur = ups.db.execute(
            SQL(
                "select * from {control_table} where table_name={table}",
            ).format(
                control_table=Identifier(ups.control_table),
                table=Literal(table),
            ),
        )
        assert cur.rowcount == 1


def test_pgupsert_qa_one_null_no_nulls(ups):
    """Test that the qa_one_null function catches null values
    in columns that should not be null and also considers the
    exclude_null_check_cols parameter."""
    ups.db.execute("update staging.genres set genre=null where genre='Fiction';")
    ups.qa_one_null("genres")
    cur = ups.db.execute(
        SQL(
            "select null_errors from {control_table} where table_name={table} and null_errors is not null",
        ).format(
            control_table=Identifier(ups.control_table),
            table=Literal("genres"),
        ),
    )
    assert cur.rowcount == 1


def test_pgupsert_qa_one_null_nulls(ups):
    ups.db.execute("update staging.books set publisher_id=null;")
    ups.qa_one_null("books")
    cur = ups.db.execute(
        SQL(
            "select null_errors from {control_table} where table_name={table} and null_errors is not null",
        ).format(
            control_table=Identifier(ups.control_table),
            table=Literal("books"),
        ),
    )
    assert cur.rowcount == 0


def test_pgupsert_qa_one_pk_dupes_one_table(ups):
    """Test that the qa_one_pk function catches duplicate primary keys in one table."""
    ups.db.execute(
        "insert into staging.authors (author_id, first_name, last_name) values ('JDoe', 'John', 'Doe');",
    )
    ups.qa_one_pk("authors")
    cur = ups.db.execute(
        SQL(
            "select pk_errors from {control_table} where table_name={table} and pk_errors is not null",
        ).format(
            control_table=Identifier(ups.control_table),
            table=Literal("authors"),
        ),
    )
    assert cur.rowcount == 1


def test_pgupsert_qa_one_pk_dupes_many_tables(ups):
    """Test that the qa_one_pk function catches duplicate primary keys in many tables."""
    ups.db.execute(
        """
        insert into staging.authors (author_id, first_name, last_name) values ('JDoe', 'John', 'Doe');
        insert into staging.book_authors (book_id, author_id) values ('B001', 'JDoe');
    """,
    )
    ups.qa_one_pk("authors")
    ups.qa_one_pk("book_authors")
    cur = ups.db.execute(
        SQL(
            "select pk_errors from {control_table} where pk_errors is not null",
        ).format(
            control_table=Identifier(ups.control_table),
        ),
    )
    assert cur.rowcount == 2


def test_pgupsert_qa_one_fk_one_table(ups):
    """Test that the qa_one_fk function catches foreign key constraint errors in one table."""
    ups.db.execute("update staging.books set genre = 'Green' where genre = 'Fiction';")
    ups.qa_one_fk("books")
    cur = ups.db.execute(
        SQL(
            "select fk_errors from {control_table} where table_name={table} and fk_errors is not null",
        ).format(
            control_table=Identifier(ups.control_table),
            table=Literal("books"),
        ),
    )
    assert cur.rowcount == 1


def test_pgupsert_qa_one_fk_many_tables(ups):
    """Test that the qa_one_fk function catches foreign key constraint errors in many tables."""
    ups.db.execute(
        """
        update staging.books set genre = 'Green' where genre = 'Fiction';
        update staging.book_authors set book_id = '9999' where book_id = 'B002';
    """,
    )
    ups.qa_one_fk("books")
    ups.qa_one_fk("book_authors")
    cur = ups.db.execute(
        SQL(
            "select fk_errors from {control_table} where fk_errors is not null",
        ).format(
            control_table=Identifier(ups.control_table),
        ),
    )
    assert cur.rowcount == 2


def test_pgupsert_upsert_one_insert(ups):
    """Test the upsert_one function with an update."""
    ups.upsert_one("genres")
    curs = ups.db.execute(
        SQL(
            "select rows_inserted, rows_updated from {control_table} where table_name={table}",
        ).format(
            control_table=Identifier(ups.control_table),
            table=Literal("genres"),
        ),
    )
    row = curs.fetchone()
    assert row[0] == 2
    assert row[1] == 0


def test_pgupsert_upsert_one_update(ups):
    """Test the upsert_one function with an update."""
    ups.upsert_one("genres")
    ups.upsert_one("genres")
    curs = ups.db.execute(
        SQL(
            "select rows_inserted, rows_updated from {control_table} where table_name={table}",
        ).format(
            control_table=Identifier(ups.control_table),
            table=Literal("genres"),
        ),
    )
    row = curs.fetchone()
    assert row[0] == 0
    assert row[1] == 2


def test_pgupsert_upsert_one_upsert(ups):
    """Test the upsert_one function with an upsert."""
    ups.upsert_one("genres")
    # Add a new genre
    ups.db.execute(
        "insert into staging.genres (genre, description) values ('Fantasy', 'Fantasy');",
    )
    ups.upsert_one("genres")
    curs = ups.db.execute(
        SQL(
            "select rows_inserted, rows_updated from {control_table} where table_name={table}",
        ).format(
            control_table=Identifier(ups.control_table),
            table=Literal("genres"),
        ),
    )
    row = curs.fetchone()
    assert row[0] == 1
    assert row[1] == 2


def test_pgupsert_upsert_no_commit_do_commit_false(ups):
    """Test that a successful upsert_one call does not commit the
    transaction unless the do_commit attribute is True and no errors are present.
    """
    ups.do_commit = False
    ups.upsert_one("genres").commit()
    curs = ups.db.execute("select count(*) from public.genres;")
    assert curs.fetchone()[0] == 0


def test_pgupsert_upsert_do_commit_true(ups):
    """Test that a successful upsert_one call does not commit the
    transaction unless the do_commit attribute is True and no errors are present.
    """
    ups.do_commit = True
    ups.upsert_one("genres").commit()
    curs = ups.db.execute("select count(*) from public.genres;")
    assert curs.fetchone()[0] == 2


def test_pgupsert_commit_standalone(ups):
    """Test that running commit() by itself does not make any changes."""
    ups.commit()

    curs = ups.db.execute(
        SQL(
            "select * from {control_table} where coalesce(rows_inserted, rows_updated) is not null",
        ).format(
            control_table=Identifier(ups.control_table),
            table=Literal("genres"),
        ),
    )
    assert curs.fetchone() is None


def test_pgupsert_show_control(ups):
    """Test the show_control method. If the interactive attribute is False, the method should log a string representation of the control table."""  # noqa: E501
    ups.interactive = False
    ups.show_control()
    with patch("pg_upsert.upsert.logger.info") as mock_logger:
        ups.show_control()
        mock_logger.assert_called_once()
        message = mock_logger.call_args[0][0]
        logger.info(message)
        assert isinstance(message, str)
        assert "table_name" in message
        assert "exclude_cols" in message
        assert "exclude_null_checks" in message
        assert "interactive" in message
        assert "null_errors" in message
        assert "pk_errors" in message
        assert "fk_errors" in message
        assert "ck_errors" in message
        assert "rows_updated" in message
        assert "rows_inserted" in message
