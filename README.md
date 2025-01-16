# pg-upsert

[![ci/cd](https://github.com/geocoug/pg-upsert/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/geocoug/pg-upsert/actions/workflows/ci-cd.yml)
[![Documentation Status](https://readthedocs.org/projects/pg-upsert/badge/?version=latest)](https://pg-upsert.readthedocs.io/en/latest/?badge=latest)
[![PyPI Latest Release](https://img.shields.io/pypi/v/pg-upsert.svg)](https://pypi.org/project/pg-upsert/)
[![PyPI Downloads](https://img.shields.io/pypi/dm/pg-upsert.svg?label=pypi%20downloads)](https://pypi.org/project/pg-upsert/)
[![Python Version Support](https://img.shields.io/pypi/pyversions/pg-upsert.svg)](https://pypi.org/project/pg-upsert/)

**pg-upsert** is a Python package that provides a method to *interactively* update and insert (upsert) rows of a base table or base tables from the staging table(s) of the same name. The package is designed to work exclusively with PostgreSQL databases.

The program will perform initial table checks in the form of *not-null*, *primary key*, *foreign key*, and *check constraint* checks. If any of these checks fail, the program will exit with an error message. If all checks pass, the program will display the number of rows to be inserted and updated, and ask for confirmation before proceeding (when the `interactive` flag is set to `True`). If the user confirms, the program will perform the upserts and display the number of rows inserted and updated. If the user does not confirm, the program will exit without performing any upserts.

![Screenshot](https://raw.githubusercontent.com/geocoug/pg-upsert/refs/heads/main/pg-upsert-screenshot.png)

## Credits

This project was created using inspiration from [ExecSQL](https://execsql.readthedocs.io/en/latest/index.html) and the example script [`pg_upsert.sql`](https://osdn.net/projects/execsql-upsert/). The goal of this project is to provide a Python implementation of `pg_upsert.sql` without the need for ExecSQL.

## Usage

Two sample database schemas are provided in the [tests/data](https://github.com/geocoug/pg-upsert/blob/main/tests/data) folder and can be used to test the functionality of the `pg-upsert` package. Both schemas are identical, the only difference is the data contained within the tables. The [schema_failing.sql](https://github.com/geocoug/pg-upsert/blob/main/tests/data/schema_failing.sql) file contains some data rows which will pass the *not-null*, *primary key*, *foreign key*, and *check constraint* checks, and other rows that will fail the checks. The [schema_passing.sql](https://github.com/geocoug/pg-upsert/blob/main/tests/data/schema_passing.sql) file contains data rows that will pass all checks.

Below is an ERD of the example schema. Code examples below will use this schema for demonstration purposes.

![ERD](https://raw.githubusercontent.com/geocoug/pg-upsert/refs/heads/main/example-data-erd.png)

See the [Running Tests Locally](#running-tests-locally) section for more information on how to set up a test database locally with Docker.

### Python

Run PgUpsert using a URI:

```python
import logging

from pg_upsert import PgUpsert

logger = logging.getLogger("pg-upsert")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

# Run PgUpsert using a URI
PgUpsert(
    uri="postgresql://docker@localhost:5432/dev", # Note the missing password. pg-upsert will prompt for the password.
    encoding="utf-8",
    tables=("genres", "publishers", "books", "authors", "book_authors"),
    stg_schema="staging",
    base_schema="public",
    do_commit=True,
    upsert_method="upsert",
    interactive=True,
    exclude_cols=("rev_user", "rev_time"),
    exclude_null_check_cols=("book_alias"),
).run()
```

Run PgUpsert using an existing connection:

```py
import logging

import psycopg2
from pg_upsert import PgUpsert

logger = logging.getLogger("pg_upsert")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="dev",
    user="docker",
    password="docker",
)

PgUpsert(
    conn=conn,
    encoding="utf-8",
    tables=("genres", "publishers", "books", "authors", "book_authors"),
    stg_schema="staging",
    base_schema="public",
    do_commit=True,
    upsert_method="upsert",
    interactive=True,
    exclude_cols=("rev_user", "rev_time"),
    exclude_null_check_cols=("book_alias"),
).run()
```

### CLI

There are two key ways to run `pg-upsert` from the command line: using a configuration file or using command line arguments.

> [!IMPORTANT]
> If the user specifies a configuration file **and** command line arguments, the configuration file will override any command line arguments specified.

#### Command Line Arguments

Running `pg-upsert --help` will display the following help message:

```txt
 Usage: pg-upsert [OPTIONS]

 Run not-NULL, Primary Key, Foreign Key, and Check Constraint checks on staging tables then update and insert (upsert)
 data from staging tables to base tables.

╭─ Options ───────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --version             -v               Display the version and exit                                                 │
│ --debug                                Display debug output                                                         │
│ --docs                                 Open the documentation in a web browser                                      │
│ --quiet               -q               Suppress all console output                                                  │
│ --logfile             -l      PATH     Write log messages to a log file [default: None]                             │
│ --exclude-columns     -e      TEXT     Comma-separated list of columns to exclude from null checks [default: None]  │
│ --null-columns        -n      TEXT     Comma-separated list of columns to exclude from null checks [default: None]  │
│ --commit              -c               Commit changes to database                                                   │
│ --interactive         -i               Display interactive GUI of important table information                       │
│ --upsert-method       -m      TEXT     Method to use for upsert (upsert, update, insert) [default: upsert]          │
│ --host                -h      TEXT     Database host [default: None]                                                │
│ --port                -p      INTEGER  Database port [default: 5432]                                                │
│ --database            -d      TEXT     Database name [default: None]                                                │
│ --user                -u      TEXT     Database user [default: None]                                                │
│ --staging-schema      -s      TEXT     Staging schema name [default: staging]                                       │
│ --base-schema         -b      TEXT     Base schema name [default: public]                                           │
│ --encoding            -e      TEXT     Encoding of the database [default: utf-8]                                    │
│ --config-file         -f      PATH     Path to configuration YAML file [default: None]                              │
│ --tables              -t      TEXT     Table name(s) [default: None]                                                │
│ --install-completion                   Install completion for the current shell.                                    │
│ --show-completion                      Show completion for the current shell, to copy it or customize the           │
│                                        installation.                                                                │
│ --help                                 Show this message and exit.                                                  │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

An example of running `pg-upsert` from the command line is shown below:

```sh
pg-upsert -l pg_upsert.log -h localhost -p 5432 -d dev -u docker -s staging -b public -t authors -t publishers -t books -t book_authors -t genres
```

#### Configuration File

To use a configuration file, create a YAML file with the format below. This example is also provided in the [pg-upsert.example.yaml](https://github.com/geocoug/pg-upsert/blob/main/pg-upsert.example.yaml) file. The configuration file can be passed to `pg-upsert` using the `-f` or `--config-file` flag.

```yaml
debug: false
quiet: false
commit: false
interactive: false
upsert_method: "upsert"
logfile: "pg_upsert.log"
host: "localhost"
port: 5432
user: "postgres"
database: "postgres"
staging_schema: "staging"
base_schema: "public"
encoding: "utf-8"
tables:
  - "authors"
  - "publishers"
  - "books"
  - "book_authors"
  - "genres"
exclude_columns:
  - "rev_time"
  - "rev_user"
null_columns:
  - "book_alias"
```

Then, run `pg-upsert -f pg-upsert.example.yaml`.

### Docker

Pull the latest image from the GitHub Container Registry (GHCR) using the following command:

```sh
docker pull ghcr.io/geocoug/pg-upsert:latest
```

Once the image is pulled, you can run the image using either of the [cli](#cli) options. Below is an example:

```sh
docker run -it --rm ghcr.io/geocoug/pg-upsert:latest -v $(pwd):/app pg-upsert --help
```

The `-v` flag is used to mount the current directory to the `/app` directory in the container. This is useful for mounting configuration files or retaining log files.

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

Running tests locally requires a PostgreSQL database. The easiest way to set up a PostgreSQL database is to use Docker. The following command will create a PostgreSQL database called `dev` with the user `docker` and password `docker`:

```sh
docker run --name postgres -e POSTGRES_USER=docker -e POSTGRES_PASSWORD=docker -e POSTGRES_DB=dev -p 5432:5432 -d postgres:latest
```

Once initialized, import the test data:

```sh
docker exec -i postgres psql -U docker -d dev < tests/data/schema_failing.sql
```

Verify that the tables were created successfully:

```sh
docker exec -it postgres psql -U docker -d dev -c "
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
    AND table_schema NOT IN ('pg_catalog', 'information_schema')
    ORDER BY table_schema, table_name;
"
```

Create a `.env` file in the root directory with the following content, modifying the values as needed.

```sh
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=dev
POSTGRES_USER=docker
POSTGRES_PASSWORD=docker
```

Now you can run the tests using `make test`.
