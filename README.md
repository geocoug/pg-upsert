# pg_upsert

[![ci/cd](https://github.com/geocoug/pg_upsert/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/geocoug/pg_upsert/actions/workflows/ci-cd.yml)
[![PyPI Latest Release](https://img.shields.io/pypi/v/pg_upsert.svg)](https://pypi.org/project/pg_upsert/)
[![PyPI Downloads](https://img.shields.io/pypi/dm/pg_upsert.svg?label=pypi%20downloads)](https://pypi.org/project/pg_upsert/)
[![Python Version Support](https://img.shields.io/pypi/pyversions/pg_upsert.svg)](https://pypi.org/project/pg_upsert/)

**pg_upsert** is a Python package that provides a method to *interactively* update and insert (upsert) rows of a base table or base tables from the staging table(s) of the same name. The package is designed to work exclusively with PostgreSQL databases.

The program will perform initial table checks in the form of not-null, primary key, foreign key, and check constraint checks. If any of these checks fail, the program will exit with an error message. If all checks pass, the program will display the number of rows to be inserted and updated, and ask for confirmation before proceeding. If the user confirms, the program will perform the upserts and display the number of rows inserted and updated. If the user does not confirm, the program will exit without performing any upserts.

## Credits

This project was created using inspiration from [ExecSQL](https://execsql.readthedocs.io/en/latest/index.html) and the example script [`pg_upsert.sql`](https://osdn.net/projects/execsql-upsert/). The goal of this project is to provide a Python implementation of `pg_upsert.sql` without the need for ExecSQL.

## Usage

### Python

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
    interactive=False,
).run()
```

### CLI

```sh
usage: pg_upsert [-h] [--version] [-q] [-d] [-l LOGFILE] [-e EXCLUDE_COLUMNS]
                 [-n NULL_COLUMNS] [-c] [-i] [-m UPSERT_METHOD]
                 HOST PORT DATABASE USER STAGING_SCHEMA BASE_SCHEMA TABLE
                 [TABLE ...]

Run not-NULL, Primary Key, Foreign Key, and Check Constraint checks on staging tables then update and insert (upsert) data from staging tables to base tables.

positional arguments:
  HOST                  database host
  PORT                  database port
  DATABASE              database name
  USER                  database user
  STAGING_SCHEMA        staging schema name
  BASE_SCHEMA           base schema name
  TABLE                 table name(s)

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  -q, --quiet           suppress all console output
  -d, --debug           display debug output
  -l LOGFILE, --log LOGFILE
                        write log to LOGFILE
  -e EXCLUDE_COLUMNS, --exclude EXCLUDE_COLUMNS
                        comma-separated list of columns to exclude from null
                        checks
  -n NULL_COLUMNS, --null NULL_COLUMNS
                        comma-separated list of columns to exclude from null
                        checks
  -c, --do-commit       commit changes to database
  -i, --interactive     display interactive GUI of important table information
  -m UPSERT_METHOD, --upsert-method UPSERT_METHOD
                        method to use for upsert
```

### Docker

```sh
docker pull ghcr.io/geocoug/pg_upsert:latest
```

## Contributing

1. Fork the repository
2. Create a new branch (`git checkout -b feature-branch`)
3. Create a Python virtual environment (`python -m venv .venv`)
4. Activate the virtual environment (`source .venv/bin/activate`)
5. Install dependencies (`pip install -r requirements.txt`)
6. Install pre-commit hooks (`python -m pre-commit install --install-hooks`)
7. Make your changes and run tests (`make test`)
8. Push your changes to the branch (`git push origin feature-branch`)
9. Create a pull request

### Running Tests Locally

Running tests locally requires a PostgreSQL database. The easiest way to set up a PostgreSQL database is to use Docker. The following command will create a PostgreSQL database called `dev` with the user `docker` and password `docker`.

```sh
docker run --name postgres -e POSTGRES_USER=docker -e POSTGRES_PASSWORD=<passwd> -e POSTGRES_DB=dev -p 5432:5432 -d postgres
```

Once initialized, import the test data by running the following command.

```sh
docker exec -i postgres psql -U docker -d dev < tests/data.sql
```

Create a `.env` file in the root directory with the following content, modifying the values as needed.

```sh
POSTGRES_HOST=
POSTGRES_PORT=5432
POSTGRES_DB=dev
POSTGRES_USER=docker
POSTGRES_PASSWORD=
```

Now you can run the tests using `make test`.
