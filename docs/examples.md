# Examples

## Detailed example

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

1. Create a Python script called `upsert_data.py` that calls `pg_upsert` to upsert data from staging tables to base tables.

    ```python
    import logging

    from pg_upsert import PgUpsert

    logger = logging.getLogger("pg_upsert")
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    PgUpsert(
        uri="postgresql://user@localhost:5432/database", # Note the missing password. pg_upsert will prompt for the password.
        tables=("genres", "books", "authors", "book_authors"),
        stg_schema="staging",
        base_schema="public",
        do_commit=True,
        upsert_method="upsert",
        interactive=False,
    ).run()
    ```

1. Run the script: `python upsert_data.py`

    ```text
    The script pg_upsert.py wants the password for PostgresDB(host=localhost, database=dev, user=username):
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

1. Modify a row in the staging table.

    ```sql
    update staging.books set book_title = 'The Great Novel 2' where book_id = 'B001';
    ```

1. Run the script again, but this time set `interactive=True` in the `upsert` function call in `upsert_data.py`.

    The script will display GUI dialogs during the upsert process to show which rows will be added and which rows will be updated. The user can chose to confirm, skip, or cancel the upsert process at any time. The script will not commit any changes to the database until all of the upserts have been completed successfully.

    ![](https://raw.githubusercontent.com/geocoug/pg_upsert/main/screenshot.png)

2. Let's test some of the QA checks. Modify the `staging.books` table to include a row with a missing value in the `book_title` and `Mystery` value in the `genre` column. The `book_title` column is a non-null column, and the `genre` column is a foreign key column. Let's also modify the `staging.authors` table by adding `JDoe` again as the `author_id` but this time we will set both the `first_name` and `last_name` to `Doe1`. This should trigger a primary key error and check constraint errors.

    ```sql
    insert into staging.books (book_id, book_title, genre, notes)
    values ('B003', null, 'Mystery', 'A book with no name!');

    insert into staging.authors (author_id, first_name, last_name)
    values ('JDoe', 'Doe1', 'Doe1');
    ```

    Run the script again: `python upsert_data.py`

    ```text
    The script pg_upsert.py wants the password for PostgresDB(host=localhost, database=dev, user=username):
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

## QA and upsert

Run all QA checks and then run the upsert process on all tables.

```python
upsert.run()
```

## QA checks only

Run all not-null, primary key, foreign key, and check constraint QA checks on all tables.

```python
upsert.qa_all()
```

## Upsert only

Run upsert procedures on all tables and commit changes. Changes will not be committed if `do_commit=False`.

```python
upsert.upsert_all().commit()
```

## Run upsert on one table

Run upsert procedures on one table and commit changes. Changes will not be committed if `do_commit=False`.

```python
upsert.upsert_one(table="authors").commit()
```

## Run a specific set of QA checks on one table

Run a specific set of QA checks on one table. The following QA checks are available: null checks, primary key checks, foreign key checks, and check constraint checks.

```python
# Null checks
upsert.qa_one_null("authors")
# Primary key checks
upsert.qa_one_pk("authors")
# Foreign key checks
upsert.qa_one_fk("authors")
# Check constraint checks
upsert.qa_one_ck("authors")
```

## Modify control table

Modify the control table on a table-by-table basis. The control table is initialized when the class is instantiated. Modifying the control table allows you to make fine-grained changes to the upsert process including excluding columns from the upsert process, toggling interactivity for a specific table, and excluding columns from not-null QA checks.

```python
upsert.db.execute(
  f"update {upsert.control_table} set exclude_cols = 'first_name,last_name', interactive=true where table_name = 'authors';"
)
upsert.upsert_one(table="authors").commit()
```
