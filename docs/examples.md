# Examples

## Detailed example

This example demonstrates how to use [PgUpsert](./pg_upsert.md#pgupsert) to upsert data from staging tables to base tables.

### 1 - Initialize a PostgreSQL database

Initialize a PostgreSQL database called `dev` with the following schema and data (also available [here](https://github.com/geocoug/pg-upsert/blob/main/tests/data/schema_passing.sql)).

See the full SQL schema: [`tests/data/schema_passing.sql`](https://github.com/geocoug/pg-upsert/blob/main/tests/data/schema_passing.sql)

![Example data ERD](https://github.com/geocoug/pg-upsert/blob/main/example-data-erd.png?raw=true)

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
│  All 5 tables passed QA checks      │
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
  "staging_schema": "staging",
  "base_schema": "public",
  "upsert_method": "upsert",
  "qa_passed": true,
  "committed": true,
  "total_updated": 0,
  "total_inserted": 90,
  "started_at": "2026-04-02 18:30:15",
  "finished_at": "2026-04-02 18:30:23",
  "duration_seconds": 8.234,
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

### 6 - Pipeline callbacks

Use the `callback` parameter to get per-table progress during a run. The callback receives a `PipelineEvent` at two points:

- `QA_TABLE_COMPLETE` — after all QA checks finish for a table
- `UPSERT_TABLE_COMPLETE` — after each table's upsert completes

```python
from pg_upsert import PgUpsert, CallbackEvent

def on_event(event):
    if event.event == CallbackEvent.QA_TABLE_COMPLETE:
        status = "passed" if event.qa_passed else "FAILED"
        print(f"  QA {status}: {event.table}")
        if not event.qa_passed:
            for err in event.qa_errors:
                print(f"    {err.check_type.value}: {err.details}")
    elif event.event == CallbackEvent.UPSERT_TABLE_COMPLETE:
        print(f"  Upserted {event.table}: "
              f"{event.rows_inserted} inserted, {event.rows_updated} updated")

result = PgUpsert(
    uri="postgresql://user@localhost:5432/dev",
    tables=("genres", "publishers", "books"),
    staging_schema="staging",
    base_schema="public",
    do_commit=True,
    callback=on_event,
).run()
```

Returning `False` from the callback aborts the pipeline and triggers a rollback:

```python
def abort_on_failure(event):
    if event.event == CallbackEvent.QA_TABLE_COMPLETE and not event.qa_passed:
        print(f"Aborting — QA failed for {event.table}")
        return False  # triggers rollback

result = PgUpsert(..., callback=abort_on_failure).run()
```

### 7 - Cleanup

pg-upsert creates temporary tables and views (all prefixed with `ups_`) during its pipeline. These are session-scoped and normally dropped when the connection closes. For long-lived connections, use `cleanup()` to drop them explicitly:

```python
import psycopg
from pg_upsert import PgUpsert

conn = psycopg.connect(...)

ups = PgUpsert(conn=conn, tables=("books",), staging_schema="staging", base_schema="public")
result = ups.run()
ups.cleanup()  # drops all ups_* temp objects; connection stays open

# conn is still usable for other work
```

### 8 - Configure from a YAML file or files

The same configuration file used by the CLI's `--config-file` flag can drive `PgUpsert` directly with `PgUpsert.from_config()`, so a single file works in both contexts:

```yaml
# pg-upsert.yaml
host: localhost
port: 5432
user: docker
database: dev
staging_schema: staging
base_schema: public
tables:
  - books
  - authors
exclude_columns:   # CLI-style keys are accepted (also: exclude_cols)
  - rev_user
  - rev_time
commit: false      # maps to do_commit
```

```python
from pg_upsert import PgUpsert

# Build straight from the file...
result = PgUpsert.from_config("pg-upsert.yaml").run()

# ...or override individual values (overrides win over the file).
# Overrides may include things YAML can't hold, e.g. an existing connection.
result = PgUpsert.from_config("pg-upsert.yaml", do_commit=True).run()

# Layer multiple files: later sources override earlier ones key-by-key.
# This is useful for a shared base config plus task-specific overrides.
result = PgUpsert.from_config([
    "base-config.yaml",      # shared defaults
    "task-specific.yaml"      # task-specific overrides
]).run()
```

Both CLI-style keys (`exclude_columns`, `null_columns`, `commit`) and the constructor's native names (`exclude_cols`, `do_commit`) are accepted, and unknown keys are ignored. When `host`, `port`, `database`, and `user` are present they are assembled into a connection URI; the password is prompted for (or read from `PGPASSWORD`) at connect time. A dictionary may be passed in place of a file path.

**Layering multiple files.** Pass a list (or tuple) of sources to keep a large shared config in one place and small task-specific overrides in another. Sources are shallow-merged left-to-right — later files override earlier ones key-by-key, and explicit overrides beat them all:

```python
# base.yaml has the connection + schemas; task.yaml has just the tables for this job.
result = PgUpsert.from_config(["base.yaml", "task.yaml"]).run()
```

Merging is shallow: a key in a later file *replaces* the earlier value rather than being deep-merged. For example, a `tables` list (or an `exclude_columns_by_table` mapping) in `task.yaml` wholly replaces the one in `base.yaml` — it is not appended to or combined. Connection parts (`host`/`port`/`database`/`user`) merge across files before the URI is built, so a later file can override just the `user`. Multiple configuration sources can be passed as a `list` or `tuple` and are shallow-merged left-to-right, so later sources override earlier ones key-by-key.

### 9 - Per-table column excludes

`exclude_columns` / `null_columns` apply to every table. To exclude columns from specific tables, add `exclude_columns_by_table` / `null_columns_by_table` — these map a table name to its own column list and are **merged on top of** the global lists for that table (the global lists still apply everywhere):

```yaml
# pg-upsert.yaml
exclude_columns:            # excluded from the upsert on every table
  - rev_user
  - rev_time
exclude_columns_by_table:   # ...plus these, only on the named table
  books:
    - isbn_legacy
null_columns_by_table:      # skip null checks for books.reprint_date only
  books:
    - reprint_date
```

With the config above, `books` excludes `rev_user`, `rev_time`, and `isbn_legacy` from the upsert; every other table excludes just `rev_user` and `rev_time`. The same mappings are available as constructor arguments:

```python
from pg_upsert import PgUpsert

result = PgUpsert(
    uri="postgresql://user@localhost:5432/dev",
    tables=("books", "authors"),
    staging_schema="staging",
    base_schema="public",
    exclude_cols=("rev_user", "rev_time"),
    exclude_cols_by_table={"books": ["isbn_legacy"]},
    exclude_null_check_cols_by_table={"books": ["reprint_date"]},
).run()
```

Every key in a per-table mapping must be one of the configured `tables`, otherwise a `ValueError` is raised.

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
PGPASSWORD=secret pg-upsert -h localhost -d dev -u docker \
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

# Export a fix sheet when QA fails (CSV — one file per table)
pg-upsert -h localhost -d dev -u docker \
  -s staging -b public -t genres -t books -t authors \
  --export-failures ./failures/ --export-format csv
#   -> ./failures/pg_upsert_failures_books.csv
#   -> ./failures/pg_upsert_failures_authors.csv
#   -> ./failures/pg_upsert_failures_schema.csv  (if schema issues exist)

# Same fix sheet as an XLSX workbook (one sheet per table)
pg-upsert -h localhost -d dev -u docker \
  -s staging -b public -t genres -t books -t authors \
  --export-failures ./failures/ --export-format xlsx
#   -> ./failures/pg_upsert_failures.xlsx

# Same fix sheet as a nested JSON file
pg-upsert -h localhost -d dev -u docker \
  -s staging -b public -t books \
  --export-failures ./failures/ --export-format json
#   -> ./failures/pg_upsert_failures.json
```
