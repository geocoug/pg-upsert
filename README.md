# pg_upsert

[![ci/cd](https://github.com/geocoug/pg_upsert/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/geocoug/pg_upsert/actions/workflows/ci-cd.yml)
[![PyPI Latest Release](https://img.shields.io/pypi/v/pg_upsert.svg)](https://pypi.org/project/pg_upsert/)
[![PyPI Downloads](https://img.shields.io/pypi/dm/pg_upsert.svg?label=pypi%20downloads)](https://pypi.org/project/pg_upsert/)

**pg_upsert** is a Python package that provides a method to *interactively* update and insert (upsert) rows of a base table or base tables from the staging table(s) of the same name. The package is designed to work exclusively with PostgreSQL databases.

The program will perform initial table checks in the form of not-null, primary key, foreign key, and check constraint checks. If any of these checks fail, the program will exit with an error message. If all checks pass, the program will display the number of rows to be inserted and updated, and ask for confirmation before proceeding. If the user confirms, the program will perform the upserts and display the number of rows inserted and updated. If the user does not confirm, the program will exit without performing any upserts.

## Credits

This project was created using inspiration from [ExecSQL](https://execsql.readthedocs.io/en/latest/index.html) and the example script [`pg_upsert.sql`](https://osdn.net/projects/execsql-upsert/). The goal of this project is to provide a Python implementation of `pg_upsert.sql` without the need for ExecSQL.

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

upsert(
    host="localhost",
    database="",
    user="postgres",
    # passwd=,                                  # if not provided, will prompt for password
    tables=[],
    stg_schema="staging",
    base_schema="public",
    upsert_method="upsert",                     # "upsert" | "update" | "insert", default: "upsert"
    commit=False,                               # optional, default=False
    interactive=True,                           # optional, default=False
    exclude_cols=[],                            # optional
    exclude_null_check_columns=[],              # optional
)
```

### Docker

```sh
docker run --rm -v $(pwd):/app ghcr.io/geocoug/pg_upsert [-h] [-q] [-d] [-l LOGFILE] [-e EXCLUDE_COLUMNS] [-n NULL_COLUMNS] [-c] [-i] [-m METHOD] HOST DATABASE USER STAGING_SCHEMA BASE_SCHEMA TABLE [TABLE ...]
```

## Example

This example will demonstrate how to use `pg_upsert` to upsert data from staging tables to base tables.

1. Initialize a PostgreSQL database called `dev` with the following schema and data.

    ```sql
    -- Create base tables.
    drop table if exists public.genres cascade;
    create table public.genres (
        genre varchar(100) primary key,
        description varchar not null
    );

    drop table if exists public.books cascade;
    create table public.books (
        book_id varchar(100) primary key,
        book_title varchar(200) not null,
        genre varchar(100) not null,
        notes text,
        foreign key (genre) references genres(genre)
    );

    drop table if exists public.authors cascade;
    create table public.authors (
        author_id varchar(100) primary key,
        first_name varchar(100) not null,
        last_name varchar(100) not null,
        -- Check that the first and last name are not the same
        constraint chk_authors check (first_name <> last_name),
        -- Check that first_name only contains letters
        constraint chk_authors_first_name check (first_name ~ '^[a-zA-Z]+$'),
        -- Check that last_name only contains letters
        constraint chk_authors_last_name check (last_name ~ '^[a-zA-Z]+$')
    );

    drop table if exists public.book_authors cascade;
    create table public.book_authors (
        book_id varchar(100) not null,
        author_id varchar(100) not null,
        foreign key (author_id) references authors(author_id),
        foreign key (book_id) references books(book_id),
        constraint pk_book_authors primary key (book_id, author_id)
    );

    -- Create staging tables that mimic base tables.
    -- Note: staging tables have the same columns as base tables but no PK, FK, or NOT NULL constraints.
    create schema if not exists staging;

    drop table if exists staging.genres cascade;
    create table staging.genres (
        genre varchar(100),
        description varchar
    );

    drop table if exists staging.books cascade;
    create table staging.books (
        book_id varchar(100),
        book_title varchar(200),
        genre varchar(100),
        notes text
    );

    drop table if exists staging.authors cascade;
    create table staging.authors (
        author_id varchar(100),
        first_name varchar(100),
        last_name varchar(100)
    );

    drop table if exists staging.book_authors cascade;
    create table staging.book_authors (
        book_id varchar(100),
        author_id varchar(100)
    );

    -- Insert data into staging tables.
    insert into staging.genres (genre, description) values
        ('Fiction', 'Literary works that are imaginary, not based on real events or people'),
        ('Non-Fiction', 'Literary works based on real events, people, and facts');

    insert into staging.authors (author_id, first_name, last_name) values
        ('JDoe', 'John', 'Doe'),
        ('JSmith', 'Jane', 'Smith'),
        ('JTrent', 'Joe', 'Trent');

    insert into staging.books (book_id, book_title, genre, notes) values
        ('B001', 'The Great Novel', 'Fiction', 'An epic tale of love and loss'),
        ('B002', 'Not Another Great Novel', 'Non-Fiction', 'A comprehensive guide to writing a great novel');

    insert into staging.book_authors (book_id, author_id) values
        ('B001', 'JDoe'),
        ('B001', 'JTrent'),
        ('B002', 'JSmith');
    ```

2. Create a Python script called `upsert_data.py` that calls `pg_upsert` to upsert data from staging tables to base tables.

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

    upsert(
        host="localhost",
        database="dev",
        user="docker", # Change this
        tables=["books", "authors", "genres", "book_authors"],
        stg_schema="staging",
        base_schema="public",
        upsert_method="upsert",
        commit=True,
        interactive=False,
        exclude_cols=[],
        exclude_null_check_columns=[],
    )
    ```

3. Run the script: `python upsert_data.py`

    ```txt
    The script pg_upsert.py wants the password for PostgresDB(host=localhost, database=dev, user=docker):
    Upserting to public from staging
    Tables selected for upsert:
    books
    authors
    genres
    book_authors

    ===Non-NULL checks===
    Conducting non-null QA checks on table staging.books
    Conducting non-null QA checks on table staging.authors
    Conducting non-null QA checks on table staging.genres
    Conducting non-null QA checks on table staging.book_authors

    ===Primary Key checks===
    Conducting primary key QA checks on table staging.books
    Conducting primary key QA checks on table staging.authors
    Conducting primary key QA checks on table staging.genres
    Conducting primary key QA checks on table staging.book_authors

    ===Foreign Key checks===
    Conducting foreign key QA checks on table staging.books
    Conducting foreign key QA checks on table staging.authors
    Conducting foreign key QA checks on table staging.genres
    Conducting foreign key QA checks on table staging.book_authors

    ===Check Constraint checks===
    Conducting check constraint QA checks on table staging.books
    Conducting check constraint QA checks on table staging.authors
    Conducting check constraint QA checks on table staging.genres
    Conducting check constraint QA checks on table staging.book_authors

    ===QA checks passed. Starting upsert===
    Performing upsert on table public.genres
    Adding data to public.genres
        2 rows inserted
    Performing upsert on table public.authors
    Adding data to public.authors
        3 rows inserted
    Performing upsert on table public.books
    Adding data to public.books
        2 rows inserted
    Performing upsert on table public.book_authors
    Adding data to public.book_authors
        3 rows inserted

    Changes committed
    ```

4. Modify a row in the staging table.

    ```sql
    update staging.books set book_title = 'The Great Novel 2' where book_id = 'B001';
    ```

5. Run the script again, but this time set `interactive=True` in the `upsert` function call in `upsert_data.py`.

    The script will display GUI dialogs during the upsert process to show which rows will be added and which rows will be updated. The user can chose to confirm, skip, or cancel the upsert process at any time. The script will not commit any changes to the database until all of the upserts have been completed successfully.

    ![Screenshot](https://raw.githubusercontent.com/geocoug/pg_upsert/main/screenshot.png)

6. Let's test some of the QA checks. Modify the `staging.books` table to include a row with a missing value in the `book_title` and `Mystery` value in the `genre` column. The `book_title` column is a non-null column, and the `genre` column is a foreign key column. Let's also modify the `staging.authors` table by adding `JDoe` again as the `author_id` but this time we will set both the `first_name` and `last_name` to `Doe1`. This should trigger a primary key error and check constraint errors.

    ```sql
    insert into staging.books (book_id, book_title, genre, notes)
    values ('B003', null, 'Mystery', 'A book with no name!');

    insert into staging.authors (author_id, first_name, last_name)
    values ('JDoe', 'Doe1', 'Doe1');
    ```

    Run the script again: `python upsert_data.py`

    ```txt
    The script pg_upsert.py wants the password for PostgresDB(host=localhost, database=dev, user=docker):
    Upserting to public from staging
    Tables selected for upsert:
    books
    authors
    genres
    book_authors

    ===Non-NULL checks===
    Conducting non-null QA checks on table staging.books
        Column book_title has 1 null values
    Conducting non-null QA checks on table staging.authors
    Conducting non-null QA checks on table staging.genres
    Conducting non-null QA checks on table staging.book_authors

    ===Primary Key checks===
    Conducting primary key QA checks on table staging.books
    Conducting primary key QA checks on table staging.authors
        Duplicate key error in columns author_id
    Conducting primary key QA checks on table staging.genres
    Conducting primary key QA checks on table staging.book_authors

    ===Foreign Key checks===
    Conducting foreign key QA checks on table staging.books
        Foreign key error referencing genres
    Conducting foreign key QA checks on table staging.authors
    Conducting foreign key QA checks on table staging.genres
    Conducting foreign key QA checks on table staging.book_authors

    ===Check Constraint checks===
    Conducting check constraint QA checks on table staging.books
    Conducting check constraint QA checks on table staging.authors
        Check constraint chk_authors has 1 failing rows
        Check constraint chk_authors_first_name has 1 failing rows
        Check constraint chk_authors_last_name has 1 failing rows
    Conducting check constraint QA checks on table staging.genres
    Conducting check constraint QA checks on table staging.book_authors

    QA checks failed. Aborting upsert.
    ```

    The script failed to upsert data because there are non-null and foreign key checks that failed on the `staging.books` table, and primary key and check constraint that failed on the `staging.authors` table. The interactive GUI will display all values in the `books.genres` column that fail the foreign key check. No GUI dialogs are displayed for non-null checks, because there are no values to display. Similarly, if there is a primary key check that fails (like in the `staging.authors` table), a GUI dialog will be displayed with the primary keys in the table that are failing. No GUI dialogs are displayed for check constraint checks.

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

## Notes

- The user can modify the control table to set interactive specific to each table
- The user can modify the control table to set the upsert method specific to each table
- The user can modify the control table to set the exclude columns specific to each table
- The user can modify the control table to set the exclude null check columns specific to each table
- In upsert_one check that the table has a primary key before proceeding
- Replace all sys.exit() with a graceful exit that closes db connection and rolls back changes

upsert_all():

- What would happen if the user runs this method without running the QA checks first?
- What would happen if the user modified the qa_passed attribute to True without running the QA checks?
- What would happen if the user modified the control table to indicate that QA checks passed when they did not?

TODO:
- Modify the show() funciton to acutally show a query either via GUI or console
