# Examples

## Detailed example

This example demonstrates how to use [PgUpsert](./pg_upsert.md#pgupsert) to upsert data from staging tables to base tables.

### 1 - Initialize a PostgreSQL database

Initialize a PostgreSQL database called `dev` with the following schema and data (also available [here](https://github.com/geocoug/pg-upsert/blob/main/tests/data/schema_passing.sql)).

See the full SQL schema: [`tests/data/schema_passing.sql`](https://github.com/geocoug/pg-upsert/blob/main/tests/data/schema_passing.sql)

### 2 - Run PgUpsert

```python
from pg_upsert import PgUpsert

result = PgUpsert(
    uri="postgresql://user@localhost:5432/dev",
    tables=("genres", "publishers", "books", "authors", "book_authors"),
    staging_schema="staging",
    base_schema="public",
    do_commit=True,
    upsert_method="upsert",
    exclude_cols=("rev_user", "rev_time"),
    exclude_null_check_cols=("book_alias",),
).run()

# Inspect results programmatically
print(result.qa_passed)       # True
print(result.committed)       # True
print(result.total_inserted)  # 90
print(result.to_json())       # JSON for CI/CD
```

### 3 - Example output (passing data)

With all QA checks passing, pg-upsert shows pass indicators for each check and table, then performs the upsert:

```text
  PostgreSQL → user@localhost:5432/dev
  Password:

  Started at 2026-04-02 14:30:15
  Tables selected for upsert (staging → public)
    • genres
    • publishers
    • books
    • authors
    • book_authors

──────────── Column Existence checks ────────────
  ✓ staging.genres
  ✓ staging.publishers
  ✓ staging.books
  ✓ staging.authors
  ✓ staging.book_authors

────────────── Column Type checks ───────────────
  ✓ staging.genres
  ✓ staging.publishers
  ✓ staging.books
  ✓ staging.authors
  ✓ staging.book_authors

──────────── Non-NULL checks ────────────────────
  ✓ staging.genres
  ✓ staging.publishers
  ✓ staging.books
  ✓ staging.authors
  ✓ staging.book_authors

──────────── Primary Key checks ─────────────────
  ✓ staging.genres
  ✓ staging.publishers
  ✓ staging.books
  ✓ staging.authors
  ✓ staging.book_authors

──────────── Unique checks ──────────────────────
  ✓ staging.genres
  ✓ staging.publishers
  ✓ staging.books
  ✓ staging.authors
  ✓ staging.book_authors

──────────── Foreign Key checks ─────────────────
  ✓ staging.genres
  ✓ staging.publishers
  ✓ staging.books
  ✓ staging.authors
  ✓ staging.book_authors

──────────── Check Constraint checks ────────────
  ✓ staging.genres
  ✓ staging.publishers
  ✓ staging.books
  ✓ staging.authors
  ✓ staging.book_authors

╭─ QA Results ────────────────────────╮
│                                     │
│  ✓ genres                           │
│  ✓ publishers                       │
│  ✓ books                            │
│  ✓ authors                          │
│  ✓ book_authors                     │
│                                     │
│  All 5 tables passed QA checks     │
╰─────────────────────────────────────╯

──────────────── Upsert ─────────────────────────
  method=upsert  commit=ON

  public.genres
    + 19 rows inserted

  public.publishers
    + 21 rows inserted

  public.authors
    + 13 rows inserted

  public.books
    + 18 rows inserted

  public.book_authors
    + 19 rows inserted

       Summary of Changes
┌──────────────┬──────────────┬───────────────┐
│ table_name   │ rows_updated │ rows_inserted │
├──────────────┼──────────────┼───────────────┤
│ genres       │ 0            │ 19            │
│ publishers   │ 0            │ 21            │
│ authors      │ 0            │ 13            │
│ books        │ 0            │ 18            │
│ book_authors │ 0            │ 19            │
├──────────────┼──────────────┼───────────────┤
│ Total        │ 0            │ 90            │
└──────────────┴──────────────┴───────────────┘
  Changes committed

  Finished at 2026-04-02 14:30:23 (8.2 seconds)
```

### 4 - Example output (failing data)

When QA checks fail, pg-upsert shows which checks failed with details:

```text
──────────── Non-NULL checks ────────────────────
  ✓ staging.genres
  ✓ staging.publishers
  ✗ staging.books — book_title (1)
  ✓ staging.authors
  ✓ staging.book_authors

──────────── Primary Key checks ─────────────────
  ✓ staging.genres
  ✓ staging.publishers
  ✓ staging.books
  ✗ staging.authors — 1 duplicate keys (2 rows) in table staging.authors
      ┌───────────┬───────┐
      │ author_id │ nrows │
      ├───────────┼───────┤
      │ JDoe      │     2 │
      └───────────┴───────┘
  ✓ staging.book_authors

──────────── Foreign Key checks ─────────────────
  ✓ staging.genres
  ✓ staging.publishers
  ✗ staging.books — books_genre_fkey (1)
      ┌─────────┬───────┐
      │ genre   │ nrows │
      ├─────────┼───────┤
      │ Mystery │     1 │
      └─────────┴───────┘
  ✓ staging.authors
  ✓ staging.book_authors

╭─ QA Results ───────────────────────────────────────╮
│                                                    │
│  ✓ genres                                          │
│  ✓ publishers                                      │
│  ✗ books                                           │
│      null: book_title (1)                          │
│      fk: books_genre_fkey (1)                      │
│  ✗ authors                                         │
│      pk: 1 duplicate keys (2 rows) in staging..    │
│      ck: chk_authors_first_name (1)                │
│  ✓ book_authors                                    │
│                                                    │
│  2 of 5 tables failed QA checks                    │
╰────────────────────────────────────────────────────╯
```

No upsert is performed when QA checks fail.

### 5 - Using the `UpsertResult`

The `run()` method returns an `UpsertResult` object:

```python
result = PgUpsert(...).run()

# Overall status
result.qa_passed       # bool — did all QA checks pass?
result.committed       # bool — were changes committed?

# Aggregate counts
result.total_updated   # int — total rows updated
result.total_inserted  # int — total rows inserted

# Per-table details
for table in result.tables:
    print(table.table_name, table.rows_updated, table.rows_inserted)
    if not table.qa_passed:
        for error in table.qa_errors:
            print(f"  {error.check_type.value}: {error.details}")

# JSON serialization for CI/CD
print(result.to_json())
```

Example JSON output (`--output json`):

```json
{
  "qa_passed": true,
  "committed": true,
  "total_updated": 0,
  "total_inserted": 90,
  "tables": [
    {
      "table_name": "genres",
      "rows_updated": 0,
      "rows_inserted": 19,
      "qa_passed": true,
      "qa_errors": []
    }
  ]
}
```

## CLI examples

```sh
# Basic upsert with commit
pg-upsert -h localhost -d dev -u docker \
  -s staging -b public \
  -t genres -t publishers -t books \
  -x rev_user -x rev_time \
  --commit

# Schema-only validation
pg-upsert --check-schema -h localhost -d dev -u docker \
  -s staging -b public -t books

# JSON output for CI/CD
pg-upsert -h localhost -d dev -u docker \
  -s staging -b public -t genres \
  --output json --commit

# Interactive mode with Textual TUI
pg-upsert -h localhost -d dev -u docker \
  -s staging -b public -t genres \
  --interactive --ui textual

# Write to logfile
pg-upsert -h localhost -d dev -u docker \
  -s staging -b public -t genres \
  -l pg-upsert.log --commit
```
