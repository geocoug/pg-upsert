#!/usr/bin/env python

import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

from pg_upsert import PgUpsert, PostgresDB

load_dotenv()


DATA = Path(__file__).parent / "data.sql"


def pytest_addoption(parser):
    parser.addoption(
        "--dev",
        action="store_true",
        default=False,
        help="Run tests in development mode.",
    )


def pytest_configure(config):
    if config.getoption("--dev"):
        config.option.verbose = 1
        config.option.setupshow = True
        config.option.capture = "no"
        config.option.showcapture = "all"
        config.option.log_cli_level = "DEBUG"


@pytest.fixture(scope="session")
def global_variables():
    """Set global variables for the test session."""
    return {
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "localhost"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT", 5432),
        "POSTGRES_DB": os.getenv("POSTGRES_DB", "postgres"),
        "POSTGRES_USER": os.getenv("POSTGRES_USER", "postgres"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "URI": f"postgresql://{os.getenv('POSTGRES_USER', 'postgres')}:{os.getenv('POSTGRES_PASSWORD', 'postgres')}@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', 5432)}/{os.getenv('POSTGRES_DB', 'postgres')}",  # noqa
    }


@pytest.fixture(scope="function")
def db(global_variables):
    """Return a PostgresDB object."""
    db = PostgresDB(uri=global_variables["URI"])
    db.execute(DATA.read_text())
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


@pytest.fixture(scope="function")
def ups(global_variables, db):
    """Return a PgUpsert object."""
    yield PgUpsert(
        conn=db.conn,
        tables=("genres", "books", "authors", "book_authors", "publishers"),
        stg_schema="staging",
        base_schema="public",
        do_commit=False,
        interactive=False,
        upsert_method="upsert",
    )
