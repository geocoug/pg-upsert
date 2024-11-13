#!/usr/bin/env python

import logging
import os

import psycopg2
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
        "URI": f"postgresql://{os.getenv('POSTGRES_USER', 'postgres')}:{os.getenv('POSTGRES_PASSWORD', 'postgres')}@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', 5432)}/{os.getenv('POSTGRES_DB', 'postgres')}",  # noqa
    }


@pytest.fixture(scope="session")
def db(global_variables):
    """Return a PostgresDB object."""
    db = PostgresDB(uri=global_variables["URI"])
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
        uri=global_variables["URI"],
        tables=("genres", "books", "authors", "book_authors"),
        stg_schema="staging",
        base_schema="public",
        do_commit=False,
        interactive=False,
        upsert_method="upsert",
    )
    yield obj
    obj.db.close()
