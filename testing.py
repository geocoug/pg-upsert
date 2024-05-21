import logging
from pathlib import Path

from pg_upsert.pg_upsert import upsert

logfile = Path("pg_upsert.log")
if logfile.exists():
    logfile.unlink()

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        logging.FileHandler(logfile),
        logging.StreamHandler(),
    ],
)

upsert(
    host="localhost",
    database="dev",
    user="docker",  # Change this
    tables=["books", "authors", "genres", "book_authors"],
    stg_schema="staging",
    base_schema="public",
    upsert_method="upsert",
    commit=True,
    interactive=True,
    exclude_cols=[],
    exclude_null_check_columns=[],
)
