# pg_upsert

[![ci/cd](https://github.com/geocoug/pg_upsert/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/geocoug/pg_upsert/actions/workflows/ci-cd.yml)

Check data in a staging table or set of staging tables, then interactively update and insert (upsert) rows of a base table or base tables from the staging table(s) of the same name. Initial table checks include not-null, primary key, and foreign key checks. If any of these checks fail, the program will exit with an error message. If all checks pass, the program will display the number of rows to be inserted and updated, and ask for confirmation before proceeding. If the user confirms, the program will perform the upserts and display the number of rows inserted and updated. If the user does not confirm, the program will exit without performing any upserts.

## Installation

1. Create a virtual environment

    ```sh
    python -m venv .venv
    ```

2. Activate the virtual environment

    ```sh
    source .venv/bin/activate
    ```

3. Install the package

    ```sh
    pip install pg_upsert
    ```

## Usage

### CLI

```sh
usage: pg_upsert.py [-h] [-q] [-d] [-l LOGFILE] [-e EXCLUDE_COLUMNS] [-n NULL_COLUMNS] [-c] [-i] [-m METHOD] HOST DATABASE USER STAGING_SCHEMA BASE_SCHEMA TABLE [TABLE ...]

Update and insert (upsert) data from staging tables to base tables.

positional arguments:
  HOST                  database host
  DATABASE              database name
  USER                  database user
  STAGING_SCHEMA        staging schema name
  BASE_SCHEMA           base schema name
  TABLE                 table name(s)

options:
  -h, --help            show this help message and exit
  -q, --quiet           suppress all console output
  -d, --debug           display debug output
  -l LOGFILE, --log LOGFILE
                        write log to LOGFILE
  -e EXCLUDE_COLUMNS, --exclude EXCLUDE_COLUMNS
                        comma-separated list of columns to exclude from null checks
  -n NULL_COLUMNS, --null NULL_COLUMNS
                        comma-separated list of columns to exclude from null checks
  -c, --commit          commit changes to database
  -i, --interactive     display interactive GUI of important table information
  -m METHOD, --method METHOD
                        method to use for upsert
```

### Python

```py
import logging
from pathlib import Path

from pg_upsert import upsert

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

tables = [
    "customers",
    "purchase_orders",
]

exclude_cols = [
    "alias",
]

upsert(
    host="localhost",
    database="dbname",
    user="postgres",
    # passwd=,  # if not provided, will prompt for password
    tables=tables,
    stg_schema="staging",
    base_schema="public",
    upsert_method="upsert",  # "upsert" | "update" | "insert", default: "upsert"
    commit=False,  # optional, default=False
    interactive=False,  # optional, default=False
    exclude_cols=exclude_cols,  # optional
    exclude_null_check_columns=exclude_cols,  # optional
)
```

### Docker

```sh
docker run --rm -v $(pwd):/app ghcr.io/geocoug/pg_upsert [-h] [-q] [-d] [-l LOGFILE] [-e EXCLUDE_COLUMNS] [-n NULL_COLUMNS] [-c] [-i] [-m METHOD] HOST DATABASE USER STAGING_SCHEMA BASE_SCHEMA TABLE [TABLE ...]
```

## Credits

This project was created using inspiration from the [ExecSQL](https://execsql.readthedocs.io/en/latest/index.html) example script [pg_upsert.sql](https://osdn.net/projects/execsql-upsert/). The goal of this project is to provide a Python implementation of the same functionality without the need for ExecSQL.
