# pg-upsert

[![ci/cd](https://github.com/geocoug/pg-upsert/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/geocoug/pg-upsert/actions/workflows/ci-cd.yml)
[![Documentation Status](https://readthedocs.org/projects/pg-upsert/badge/?version=latest)](https://pg-upsert.readthedocs.io/en/latest/?badge=latest)
[![PyPI Latest Release](https://img.shields.io/pypi/v/pg-upsert.svg)](https://pypi.org/project/pg-upsert/)
[![PyPI Downloads](https://img.shields.io/pypi/dm/pg-upsert.svg?label=pypi%20downloads)](https://pypi.org/project/pg-upsert/)
[![Python Version Support](https://img.shields.io/pypi/pyversions/pg-upsert.svg)](https://pypi.org/project/pg-upsert/)

**pg-upsert** is a Python package that provides a method to *interactively* update and/or insert (upsert) rows of a base table or base tables from the staging table(s) of the same name. It is designed to work exclusively with PostgreSQL databases.

![Screenshot](https://raw.githubusercontent.com/geocoug/pg-upsert/refs/heads/main/pg-upsert-screenshot.png)

## Why Use `pg-upsert`?

Managing data synchronization between staging and production tables in PostgreSQL can be complex and error-prone. **pg-upsert** simplifies this process by providing a structured, reliable, and interactive approach to upserting data. Here’s why you might want to use it:

- **Automated Integrity Checks** – Ensures that [NOT NULL](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-NOT-NULL), [PRIMARY KEY](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-PRIMARY-KEYS), [FOREIGN KEY](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-FK), and [CHECK CONSTRAINT](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-CHECK-CONSTRAINTS) rules are validated before any modifications occur. If all checks pass, the program will display the number of rows to be inserted and updated, and ask for confirmation before proceeding (**when the `interactive` flag is set to `True`**).
- **Interactive Confirmation** – Before performing upserts, the tool displays a summary of changes and waits for user confirmation, reducing accidental data corruption.
- **Flexible Upsert Strategies** – Supports multiple upsert methods (upsert, update, insert), allowing you to tailor the process to your needs.
- **Schema-Aware Execution** – Works across different schemas (staging and base) to help maintain data separation and versioning.
- **Minimal Dependencies** – Built specifically for PostgreSQL without requiring complex third-party dependencies.
- **Command-Line and Python API Support** – Run it as a script, integrate it into automated workflows, or execute it interactively via CLI.
- **Safe and Transparent** – Logs detailed messages about operations performed, making debugging and auditing easier.

Whether you need to merge staging data into production, synchronize changes across environments, or validate table integrity before inserts, **pg-upsert** is a lightweight yet powerful solution.

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

Run not-NULL, Primary Key, Foreign Key, and Check Constraint checks on staging tables then update and insert (upsert) data from staging tables to base tables.

╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────╮
│ --version          -v               Display the version and exit.                                 │
│ --debug                             Display debug output.                                         │
│ --docs                              Open the documentation in a web browser.                      │
│ --quiet            -q               Suppress all console output.                                  │
│ --logfile          -l      PATH     Write log messages to a log file. [default: None]             │
│ --exclude-columns  -x      TEXT     List of column names to exclude from the upsert process.      │
│                                     These columns will not be updated or inserted, but will       │
│                                     still be checked during the QA process. [default: None]       │
│ --null-columns     -n      TEXT     List of column names to exclude from the not-null check       │
│                                     during the QA process. Useful for auto-generated timestamps   │
│                                     or serial columns, which may not be populated immediately.    │
│                                     [default: None]                                               │
│ --commit           -c               If True, changes will be committed to the database once the   │
│                                     upsert process completes. If False, changes will be rolled    │
│                                     back.                                                         │
│ --interactive      -i               If True, the user will be prompted to confirm steps during    │
│                                     the upsert process. If False, the process runs automatically. │
│ --upsert-method    -m      TEXT     The method for upserting data. Must be 'upsert', 'update',    │
│                                     or 'insert'. [default: upsert]                                │
│ --host             -h      TEXT     Database host. [default: None]                                │
│ --port             -p      INTEGER  Database port. [default: 5432]                                │
│ --database         -d      TEXT     Database name. [default: None]                                │
│ --user             -u      TEXT     Database user. [default: None]                                │
│ --staging-schema   -s      TEXT     Name of the staging schema for QA checks and upserts.         │
│                                     Tables here must match names in the base schema.              │
│                                     [default: staging]                                            │
│ --base-schema      -b      TEXT     Name of the base schema where tables are updated or inserted. │
│                                     [default: public]                                             │
│ --encoding         -e      TEXT     The encoding for the database connection. [default: utf-8]    │
│ --config-file      -f      PATH     Path to configuration YAML file. [default: None]              │
│ --tables           -t      TEXT     Table names to perform QA checks on and upsert.               │
│                                     [default: None]                                               │
│ --generate-config  -g               Generate a template configuration file. Includes provided     │
│                                     options in the generated file.                                │
│ --help                              Show this message and exit.                                   │
╰───────────────────────────────────────────────────────────────────────────────────────────────────╯
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
interactive: true
upsert_method: "upsert"  # Options: "upsert", "insert", "update"
logfile: "pg_upsert.log"
host: "host.docker.internal"  # If running pg-upsert in Docker, use this to connect to the host machine. Otherwise, use the IP address or hostname of the database server.
port: 5432
user: "docker"
database: "dev"
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

In order to run the Docker image, you may need to setup X11 forwarding to allow GUI applications to run in the container. If you're using MacOS, [XQuartz](https://www.xquartz.org/) is a popular choice. To enable X11 forwarding, you need to allow connections from the container to your host machine's X11 server. This is typically done by running the `xhost` command on your host machine.

```sh
xhost +localhost
```

Next, pull the latest image from the GitHub Container Registry (GHCR) and run it using the following command:

```sh
docker run -it --rm \
  -e DISPLAY=host.docker.internal:0 \
  -v /tmp/.X11-unix:/tmp/.X11-unix \
  -v $(pwd):/app \
  ghcr.io/geocoug/pg-upsert:latest \
  --help
```

- The `-it` flag is used to run the container in interactive mode with a pseudo-TTY.
- The `--rm` flag is used to remove the container after it exits.
- The `-e` flag is used to set the `DISPLAY` environment variable to allow GUI applications to run in the container.
- The first `-v` flag is used to mount the X11 socket from the host to the container, allowing GUI applications to display on the host's screen.
- The second `-v` flag is used to mount the current directory to the `/app` directory in the container (useful for mounting configuration files or retaining log files).
- `ghcr.io/geocoug/pg-upsert:latest` is the image name and tag.
- `--help` is the `pg-upsert` command to run inside the container.

## Contributing

1. Fork the repository
2. Create a new branch (`git checkout -b <feature-branch>`)
3. Create a Python virtual environment (`python -m venv .venv` | `uv venv`)
4. Activate the virtual environment (`source .venv/bin/activate`)
5. Install dependencies (`pip install ".[dev]"` | `uv pip install ".[dev]"`)
6. Install pre-commit hooks (`python -m pre-commit install --install-hooks`)
7. Make your changes
8. Run tests with `tox`
9. Push your changes to the branch (`git push origin <feature-branch>`)
10. Create a pull request

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

Now you can run the tests using `tox`.
