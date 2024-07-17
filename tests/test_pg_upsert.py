#!/usr/bin/env python

import logging
import os

import pytest
from dotenv import load_dotenv

from pg_upsert.pg_upsert import PostgresDB

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
    logger.debug("Database object created.")
    yield db
    db.close()
    logger.debug("Database object closed.")


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
