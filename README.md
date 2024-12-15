# pg_upsert

[![ci/cd](https://github.com/geocoug/pg_upsert/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/geocoug/pg_upsert/actions/workflows/ci-cd.yml)
[![Documentation Status](https://readthedocs.org/projects/pg-upsert/badge/?version=latest)](https://pg-upsert.readthedocs.io/en/latest/?badge=latest)
[![PyPI Latest Release](https://img.shields.io/pypi/v/pg_upsert.svg)](https://pypi.org/project/pg_upsert/)
[![PyPI Downloads](https://img.shields.io/pypi/dm/pg_upsert.svg?label=pypi%20downloads)](https://pypi.org/project/pg_upsert/)
[![Python Version Support](https://img.shields.io/pypi/pyversions/pg_upsert.svg)](https://pypi.org/project/pg_upsert/)

**pg_upsert** is a Python package that provides a method to *interactively* update and insert (upsert) rows of a base table or base tables from the staging table(s) of the same name. The package is designed to work exclusively with PostgreSQL databases.

The program will perform initial table checks in the form of *not-null*, *primary key*, *foreign key*, and *check constraint* checks. If any of these checks fail, the program will exit with an error message. If all checks pass, the program will display the number of rows to be inserted and updated, and ask for confirmation before proceeding (when the `interactive` flag is set to `True`). If the user confirms, the program will perform the upserts and display the number of rows inserted and updated. If the user does not confirm, the program will exit without performing any upserts.

## Credits

This project was created using inspiration from [ExecSQL](https://execsql.readthedocs.io/en/latest/index.html) and the example script [`pg_upsert.sql`](https://osdn.net/projects/execsql-upsert/). The goal of this project is to provide a Python implementation of `pg_upsert.sql` without the need for ExecSQL.

## Usage

A sample database is provided in the [tests/data.sql](./tests/data.sql) file and can be used to test the functionality of the `pg_upsert` package. See the [Running Tests Locally](#running-tests-locally) section for more information on how to set up the test database locally with Docker.

### Python

Run PgUpsert using a URI:

```python
import logging

from pg_upsert import PgUpsert

logger = logging.getLogger("pg_upsert")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

# Run PgUpsert using a URI
PgUpsert(
    uri="postgresql://user@localhost:5432/database", # Note the missing password. pg_upsert will prompt for the password.
    encoding="utf-8",
    tables=("genres", "publishers", "books", "authors", "book_authors"),
    stg_schema="staging",
    base_schema="public",
    do_commit=True,
    upsert_method="upsert",
    interactive=True,
    exclude_cols=("rev_user", "rev_time", "created_at", "updated_at"),
    exclude_null_check_cols=("rev_user", "rev_time", "created_at", "updated_at", "alias"),
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
    dbname="database",
    user="user",
    password="password",
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
    exclude_cols=("rev_user", "rev_time", "created_at", "updated_at"),
    exclude_null_check_cols=("rev_user", "rev_time", "created_at", "updated_at", "alias"),
).run()
```

### CLI

`pg_upsert` can be run from the command line. There are two key ways to run `pg_upsert` from the command line: using a configuration file or using command line arguments.

> [!IMPORTANT]
> If the user specifies a configuration file **and** command line arguments, the configuration file will override any command line arguments specified.

#### Command Line Arguments

Running `pg_upsert --help` will display the following help message:

```txt
usage: pg_upsert [--help] [--version] [--debug] [--docs] [-q] [-l LOGFILE] [-e EXCLUDE_COLUMNS] [-n NULL_COLUMNS] [-c] [-i] [-m {upsert,update,insert}] [-h HOST] [-p PORT] [-d DATABASE] [-u USER] [-s STAGING_SCHEMA] [-b BASE_SCHEMA]
                 [--encoding ENCODING] [-f CONFIG_FILE] [-t TABLES [TABLES ...]]

Run not-NULL, Primary Key, Foreign Key, and Check Constraint checks on staging tables then update and insert (upsert) data from staging tables to base tables.

options:
  --help                show this help message and exit
  --version             show program's version number and exit
  --debug               display debug output
  --docs                open the documentation in a web browser
  -q, --quiet           suppress all console output
  -l LOGFILE, --logfile LOGFILE
                        write log to LOGFILE
  -e EXCLUDE_COLUMNS, --exclude-columns EXCLUDE_COLUMNS
                        comma-separated list of columns to exclude from null checks
  -n NULL_COLUMNS, --null-columns NULL_COLUMNS
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
  -s STAGING_SCHEMA, --staging-schema STAGING_SCHEMA
                        staging schema name
  -b BASE_SCHEMA, --base-schema BASE_SCHEMA
                        base schema name
  --encoding ENCODING   encoding of the database
  -f CONFIG_FILE, --config-file CONFIG_FILE
                        path to configuration yaml file
  -t TABLES [TABLES ...], --tables TABLES [TABLES ...]
                        table name(s)
```

An example of running `pg_upsert` from the command line is shown below:

```sh
pg_upsert -l pg_upsert.log -h localhost -p 5432 -d postgres -u postgres -s staging -b public -t authors publishers books book_authors genres
```

#### Configuration File

To use a configuration file, create a YAML file with the format below. This example is also provided in the [pg_upsert.example.yaml](./pg_upsert.example.yaml) file. The configuration file can be passed to `pg_upsert` using the `-f` or `--config-file` flag.

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
  - "alias"
  - "rev_time"
  - "rev_user"
null_columns:
  - "alias"
  - "created_at"
  - "updated_at"
```

Then, run `pg_upsert -f pg_upsert.yaml`.

### Docker

Pull the latest image from the GitHub Container Registry (GHCR) using the following command:

```sh
docker pull ghcr.io/geocoug/pg_upsert:latest
```

Once the image is pulled, you can run the image using either of the [cli](#cli) options. Below is an example:

```sh
docker run -it --rm ghcr.io/geocoug/pg_upsert:latest -v $(pwd):/app pg_upsert --help
```

> [!NOTE]
> The `-v` flag is used to mount the current directory to the `/app` directory in the container. This is useful for mounting configuration files or retaining log files.


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
docker run --name postgres -e POSTGRES_USER=docker -e POSTGRES_PASSWORD=docker -e POSTGRES_DB=dev -p 5432:5432 -d postgres
```

Once initialized, import the test data by running the following command.

```sh
docker exec -i postgres psql -U docker -d dev < tests/data.sql
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
