#!/usr/bin/env python
"""Shared fixtures for the pg-upsert test suite.

Tests that require a live PostgreSQL connection are marked with
``@pytest.mark.postgres``.  When the database is unreachable the
marker causes those tests to be skipped automatically so that
unit-level tests still run locally without Docker.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg2
import pytest
from dotenv import load_dotenv

from pg_upsert import PgUpsert, PostgresDB

load_dotenv()

PASSING_DATA = Path(__file__).parent / "data" / "schema_passing.sql"
FAILING_DATA = Path(__file__).parent / "data" / "schema_failing.sql"


# ---------------------------------------------------------------------------
# Postgres availability detection
# ---------------------------------------------------------------------------


def _pg_uri() -> str:
    """Build a connection URI from environment variables."""
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def _pg_available() -> bool:
    """Return True if a Postgres server is reachable."""
    try:
        conn = psycopg2.connect(_pg_uri())
        conn.close()
        return True
    except Exception:
        return False


PG_AVAILABLE = _pg_available()


# ---------------------------------------------------------------------------
# Pytest hooks & markers
# ---------------------------------------------------------------------------


def pytest_addoption(parser):
    parser.addoption(
        "--dev",
        action="store_true",
        default=False,
        help="Run tests in development mode.",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "postgres: mark test as requiring a live PostgreSQL database",
    )
    if config.getoption("--dev", default=False):  # pragma: no cover
        config.option.verbose = 1
        config.option.setupshow = True
        config.option.capture = "no"
        config.option.showcapture = "all"
        config.option.log_cli_level = "DEBUG"


def pytest_collection_modifyitems(config, items):
    """Auto-skip ``@pytest.mark.postgres`` tests when the database is unreachable."""
    if PG_AVAILABLE:
        return
    skip_pg = pytest.mark.skip(reason="PostgreSQL is not available")
    for item in items:
        if "postgres" in item.keywords:
            item.add_marker(skip_pg)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def global_variables():
    """Connection parameters drawn from the environment or sensible defaults."""
    return {
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "localhost"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
        "POSTGRES_DB": os.getenv("POSTGRES_DB", "postgres"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER", "postgres"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "URI": _pg_uri(),
    }


@pytest.fixture()
def db(global_variables):
    """A PostgresDB connected to the test database with passing schema loaded."""
    db = PostgresDB(uri=global_variables["URI"])
    db.execute(PASSING_DATA.read_text())
    db.commit()
    yield db
    db.execute(
        """
        drop table if exists staging.genres cascade;
        drop table if exists staging.books cascade;
        drop table if exists staging.authors cascade;
        drop table if exists staging.book_authors cascade;
        drop table if exists staging.publishers cascade;
        drop table if exists public.genres cascade;
        drop table if exists public.books cascade;
        drop table if exists public.authors cascade;
        drop table if exists public.book_authors cascade;
        drop table if exists public.publishers cascade;
    """,
    )
    db.commit()
    db.close()


@pytest.fixture()
def db_failing(global_variables):
    """A PostgresDB connected to the test database with failing schema loaded."""
    db = PostgresDB(uri=global_variables["URI"])
    db.execute(FAILING_DATA.read_text())
    db.commit()
    yield db
    db.execute(
        """
        drop table if exists staging.genres cascade;
        drop table if exists staging.books cascade;
        drop table if exists staging.authors cascade;
        drop table if exists staging.book_authors cascade;
        drop table if exists staging.publishers cascade;
        drop table if exists public.genres cascade;
        drop table if exists public.books cascade;
        drop table if exists public.authors cascade;
        drop table if exists public.book_authors cascade;
        drop table if exists public.publishers cascade;
    """,
    )
    db.commit()
    db.close()


@pytest.fixture()
def ups(db):
    """A PgUpsert object wired to the passing-data database."""
    return PgUpsert(
        conn=db.conn,
        tables=("genres", "books", "authors", "book_authors", "publishers"),
        staging_schema="staging",
        base_schema="public",
        do_commit=False,
        interactive=False,
        upsert_method="upsert",
        exclude_cols=("rev_user", "rev_time"),
    )


@pytest.fixture()
def ups_failing(db_failing):
    """A PgUpsert object wired to the failing-data database."""
    return PgUpsert(
        conn=db_failing.conn,
        tables=("genres", "books", "authors", "book_authors", "publishers"),
        staging_schema="staging",
        base_schema="public",
        do_commit=False,
        interactive=False,
        upsert_method="upsert",
        exclude_cols=("rev_user", "rev_time"),
    )


@pytest.fixture()
def ups_with_excludes(db):
    """A PgUpsert with exclude_cols and exclude_null_check_cols set."""
    return PgUpsert(
        conn=db.conn,
        tables=("genres", "books", "authors", "book_authors", "publishers"),
        staging_schema="staging",
        base_schema="public",
        do_commit=False,
        interactive=False,
        upsert_method="upsert",
        exclude_cols=("rev_user", "rev_time"),
        exclude_null_check_cols=("book_alias",),
    )
