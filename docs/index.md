# Home

[![CI/CD](https://github.com/geocoug/pg_upsert/actions/workflows/ci-cd.yml/badge.svg)](https://pypi.org/project/pg_upsert/)
[![PyPI Latest Release](https://img.shields.io/pypi/v/pg_upsert.svg)](https://pypi.org/project/pg_upsert/)
[![pg_upsert Downloads Per Month](https://img.shields.io/pypi/dm/pg_upsert.svg?label=pypi%20downloads)](https://pypi.org/project/pg_upsert/)
[![Python Version Support](https://img.shields.io/pypi/pyversions/pg_upsert.svg)](https://pypi.org/project/pg_upsert/)

**pg_upsert** is a Python package that runs not-NULL, Primary Key, Foreign Key, and Check Constraint checks on PostgreSQL staging tables then updates and inserts (upsert) data from staging tables to base tables.

Looking for examples? Check out the [examples](./examples.md) page.

## Installation

You can install **pg_upsert** via pip from PyPI:

```bash
pip install pg_upsert
```

There is also a Docker image available on the GitHub Container Registry:

```bash
docker pull ghcr.io/geocoug/pg_upsert:latest
```

## Usage

### Python

Below is a simple example of how to use the `PgUpsert` class to upsert data from staging tables to base tables. For a more detailed example, check out the [examples](./examples.md) page.

```python
import logging

from pg_upsert import PgUpsert

logger = logging.getLogger("pg_upsert")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

PgUpsert(
    host="localhost",
    port=5432,
    database="dev",
    user="username",
    tables=("genres", "books", "authors", "book_authors"),
    stg_schema="staging",
    base_schema="public",
    do_commit=True,
    upsert_method="upsert",
    interactive=True,
    exclude_cols=("rev_user", "rev_time", "created_at", "updated_at"),
    exclude_null_check_cols=("rev_user", "rev_time", "created_at", "updated_at", "alias"),
).run()
```

### CLI

`pg_upsert` has a command-line interface that can be used to perform all the functionality of the `PgUpsert` class. The CLI can be accessed by running `pg_upsert` in the terminal.

```txt
usage: pg_upsert [--help] [--version] [--debug] [-q] [-l LOG] [-e EXCLUDE] [-n NULL] [-c] [-i] [-m {upsert,update,insert}] -h HOST -p PORT -d DATABASE -u USER -s STG_SCHEMA -b BASE_SCHEMA tables [tables ...]

Run not-NULL, Primary Key, Foreign Key, and Check Constraint checks on staging tables then update and insert (upsert) data from staging tables to base tables.

positional arguments:
  tables                table name(s)

options:
  --help                show this help message and exit
  --version             show program's version number and exit
  --debug               display debug output
  -q, --quiet           suppress all console output
  -l LOG, --log LOG     write log to LOGFILE
  -e EXCLUDE, --exclude-columns EXCLUDE
                        comma-separated list of columns to exclude from null checks
  -n NULL, --null-columns NULL
                        comma-separated list of columns to exclude from null checks
  -c, --commit          commit changes to database
  -i, --interactive     display interactive GUI of important table information
  -m {upsert,update,insert}, --upsert-method {upsert,update,insert}
                        method to use for upsert
  -h HOST, --host HOST  database host
  -p PORT, --port PORT  database port
  -d DATABASE, --database DATABASE
                        database name
  -u USER, --user USER  database user
  -s STG_SCHEMA, --staging-schema STG_SCHEMA
                        staging schema name
  -b BASE_SCHEMA, --base-schema BASE_SCHEMA
                        base schema name
```

### Docker

There is a Docker image available on the GitHub Container Registry that can be used to run `pg_upsert`:

```bash
docker pull ghcr.io/geocoug/pg_upsert:latest
```

## Credits

This project was created using inspiration from [execsql](https://execsql.readthedocs.io/en/latest/index.html)_ and the example script [pg_upsert.sql](https://osdn.net/projects/execsql-upsert/). The goal of this project is to provide a Python implementation of `pg_upsert.sql` without the need for ExecSQL.
