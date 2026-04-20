# pg-upsert

[![ci/cd](https://github.com/geocoug/pg-upsert/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/geocoug/pg-upsert/actions/workflows/ci-cd.yml)
[![codecov](https://codecov.io/gh/geocoug/pg-upsert/graph/badge.svg)](https://codecov.io/gh/geocoug/pg-upsert)
[![Documentation Status](https://readthedocs.org/projects/pg-upsert/badge/?version=latest)](https://pg-upsert.readthedocs.io/en/latest/?badge=latest)
[![PyPI Latest Release](https://img.shields.io/pypi/v/pg-upsert.svg)](https://pypi.org/project/pg-upsert/)
[![PyPI Downloads](https://img.shields.io/pypi/dm/pg-upsert.svg?label=pypi%20downloads)](https://pypi.org/project/pg-upsert/)
[![Python Version Support](https://img.shields.io/pypi/pyversions/pg-upsert.svg)](https://pypi.org/project/pg-upsert/)

**pg-upsert** is a Python package for validating and upserting data from staging tables into base tables in PostgreSQL. It runs automated QA checks, reports errors with rich formatted output, and performs dependency-aware upserts.

![pg-upsert terminal output with interactive compare table](pg-upsert-screenshot.png)

## Why Use `pg-upsert`?

- **7 Automated QA Checks** – Validates [NOT NULL](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-NOT-NULL), [PRIMARY KEY](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-PRIMARY-KEYS), [UNIQUE](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-UNIQUE-CONSTRAINTS), [FOREIGN KEY](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-FK), [CHECK CONSTRAINT](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-CHECK-CONSTRAINTS), column existence, and column type compatibility before any modifications occur.
- **Interactive Confirmation** – Two UI backends: Textual TUI (terminal) and Tkinter (desktop). Auto-detected or choose with `--ui auto|textual|tkinter`. The compare-tables dialog includes a **Highlight Diffs** toggle that tints matching/changed rows and flags the exact cells that differ, skipping any columns excluded from the upsert.
- **Structured Results** – `run()` returns an `UpsertResult` with per-table stats, QA errors, and JSON serialization (`--output=json` for CI/CD pipelines).
- **Exportable Fix Sheets** – `--export-failures <dir>` writes an actionable report of failing rows: one row per unique violating staging row with an `_issues` column listing every problem (NULL in 'genre', duplicate PK, FK violation, etc.) so users can open it in Excel and fix the data. Supports CSV (file per table), JSON (nested), and XLSX (sheets per table) via `--export-format`.
- **Schema Validation** – `--check-schema` flag validates column existence and type compatibility without running data checks or upserts.
- **Flexible Upsert Strategies** – Supports `upsert`, `update`, and `insert` methods.
- **Dependency-Aware Ordering** – Tables are processed in FK dependency order automatically.
- **Rich Output** – Colored pass/fail indicators, formatted tables, and dual console+logfile output.

## Usage

### Python API

```python
from pg_upsert import PgUpsert

result = PgUpsert(
    uri="postgresql://user@localhost:5432/mydb",
    tables=("genres", "publishers", "books", "authors", "book_authors"),
    staging_schema="staging",
    base_schema="public",
    do_commit=True,
    upsert_method="upsert",
    exclude_cols=("rev_user", "rev_time"),
    exclude_null_check_cols=("book_alias",),
).run()

# UpsertResult provides structured access to results
print(result.qa_passed)       # True if all QA checks passed
print(result.committed)       # True if changes were committed
print(result.total_updated)   # Total rows updated across all tables
print(result.total_inserted)  # Total rows inserted across all tables
print(result.to_json())       # JSON serialization for CI/CD
```

Using an existing connection:

```python
import psycopg2
from pg_upsert import PgUpsert

conn = psycopg2.connect(host="localhost", port=5432, dbname="mydb", user="user", password="pass")

ups = PgUpsert(
    conn=conn,
    tables=("genres", "publishers", "books"),
    staging_schema="staging",
    base_schema="public",
    do_commit=True,
)
result = ups.run()

# Drop all ups_* temp objects (connection stays open)
ups.cleanup()
```

QA-only mode (no upsert):

```python
from pg_upsert import PgUpsert

ups = PgUpsert(
    uri="postgresql://user@localhost:5432/mydb",
    tables=("genres", "books"),
    staging_schema="staging",
    base_schema="public",
).qa_all()

# qa_passed is False only when ERROR-level findings exist.
# qa_errors returns ERROR findings only (block the upsert).
# qa_warnings returns WARNING findings only (informational, do not block).
# qa_findings returns all findings combined.
# qa_passed may be True while qa_warnings is non-empty.
if not ups.qa_passed:
    for err in ups.qa_errors:
        print(f"{err.table}: {err.check_type.value} — {err.details}")
```

Schema compatibility check (column existence and type mismatches only):

```python
from pg_upsert import PgUpsert

ups = PgUpsert(
    uri="postgresql://user@localhost:5432/mydb",
    tables=("genres", "books"),
    staging_schema="staging",
    base_schema="public",
).qa_column_existence().qa_type_mismatch()

if ups.qa_errors:
    for err in ups.qa_errors:
        print(f"{err.table}: {err.check_type.value} — {err.details}")
else:
    print("Schemas are compatible")
```

Pipeline callbacks (per-table progress):

```python
from pg_upsert import PgUpsert, CallbackEvent

def on_event(event):
    if event.event == CallbackEvent.QA_TABLE_COMPLETE:
        print(f"QA {'passed' if event.qa_passed else 'failed'} for {event.table}")
    elif event.event == CallbackEvent.UPSERT_TABLE_COMPLETE:
        print(f"{event.table}: {event.rows_inserted} inserted, {event.rows_updated} updated")

result = PgUpsert(
    uri="postgresql://user@localhost:5432/mydb",
    tables=("genres", "books"),
    staging_schema="staging",
    base_schema="public",
    do_commit=True,
    callback=on_event,
).run()
```

### CLI

```sh
pg-upsert -h localhost -p 5432 -d mydb -u user \
  -s staging -b public \
  -t genres -t publishers -t books -t authors -t book_authors \
  -x rev_user -x rev_time \
  --commit
```

| Option                    | Description                                               |
| ------------------------- | --------------------------------------------------------- |
| `-h`, `--host`            | Database host                                             |
| `-p`, `--port`            | Database port (default: 5432)                             |
| `-d`, `--database`        | Database name                                             |
| `-u`, `--user`            | Database user (see [Authentication](#authentication))     |
| `-s`, `--staging-schema`  | Staging schema name (default: staging)                    |
| `-b`, `--base-schema`     | Base schema name (default: public)                        |
| `-e`, `--encoding`        | Database connection encoding (default: utf-8)             |
| `-t`, `--table`           | Table name to process (repeatable)                        |
| `-x`, `--exclude-columns` | Columns to exclude from upsert (repeatable)               |
| `-n`, `--null-columns`    | Columns to skip during NOT NULL checks (repeatable)       |
| `-m`, `--upsert-method`   | `upsert`, `update`, or `insert` (default: upsert)         |
| `-c`, `--commit`          | Commit changes (default: roll back)                       |
| `-i`, `--interactive`     | Prompt for confirmation at each step                      |
| `-l`, `--logfile`         | Write log to file (appends, does not overwrite)           |
| `-o`, `--output`          | Output format: `text` (default) or `json`                 |
| `--check-schema`          | Validate column existence and types only, then exit       |
| `--compact`               | Use compact grid format for QA summary                    |
| `--ui`                    | Interactive UI: `auto` (default), `textual`, or `tkinter` |
| `--export-failures`       | Directory to write a QA failure fix sheet into            |
| `--export-format`         | Fix sheet format: `csv` (default), `json`, or `xlsx`      |
| `--export-max-rows`       | Max rows to capture per check per table (default 1000)    |
| `--strict-columns`        | Treat all missing staging columns as errors               |
| `-f`, `--config-file`     | Path to YAML configuration file                           |
| `-g`, `--generate-config` | Generate a template config file                           |
| `-v`, `--version`         | Show version and exit                                     |
| `--docs`                  | Open documentation in browser                             |
| `--debug`                 | Enable debug output                                       |

> [!NOTE]
> CLI arguments take precedence over configuration file values. Explicit CLI flags are never overridden by the config file.

#### Configuration File

Create a YAML config file (see [pg-upsert.example.yaml](https://github.com/geocoug/pg-upsert/blob/main/pg-upsert.example.yaml)):

```yaml
debug: false
commit: false
interactive: false
upsert_method: "upsert"  # Options: "upsert", "insert", "update"
logfile: "pg_upsert.log"
host: "localhost"
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
output: "text"  # Options: "text", "json"
check_schema: false
compact: false
ui_mode: "auto"  # Options: "auto", "textual", "tkinter"
export_failures: null  # Directory to write QA failure fix sheet; null to disable
export_format: "csv"  # Fix sheet format: "csv", "json", or "xlsx"
export_max_rows: 1000  # Max rows captured per check per table for the fix sheet
strict_columns: false  # Treat all missing staging columns as errors
```

Run with: `pg-upsert -f config.yaml`

### Docker

```sh
docker run -it --rm \
  -v $(pwd):/app \
  ghcr.io/geocoug/pg-upsert:latest \
  -h host.docker.internal -p 5432 -d dev -u docker \
  -s staging -b public -t genres --commit
```

## QA Checks

pg-upsert runs 7 types of QA checks on staging data before upserting:

| Check                | What it validates                                                                                                                                   |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Column Existence** | PK and NOT NULL (no default) columns must exist in staging (error); other missing columns produce warnings. Use `--strict-columns` for strict mode. |
| **Column Type**      | No hard type incompatibilities between staging and base (uses PostgreSQL's `pg_cast` catalog)                                                       |
| **NOT NULL**         | Non-nullable base columns have no NULL values in staging                                                                                            |
| **Primary Key**      | No duplicate values in PK columns                                                                                                                   |
| **Unique**           | No duplicate values in UNIQUE-constrained columns (NULLs allowed per PostgreSQL semantics)                                                          |
| **Foreign Key**      | All FK references point to existing rows in the referenced table                                                                                    |
| **Check Constraint** | All CHECK constraint expressions evaluate to true                                                                                                   |

> [!NOTE]
> pg-upsert is constraint-driven. Data checks pass vacuously when the
> base table has no constraints of that type, and **tables without a
> primary key are skipped during the upsert step** (a warning is
> printed). To upsert against a table, make sure the base table has a
> PK. See [Running Without Constraints](https://pg-upsert.readthedocs.io/qa_checks/#running-without-constraints)
> for details.

See the [QA Checks Reference](https://pg-upsert.readthedocs.io/) for detailed documentation.

## Authentication

pg-upsert resolves the database password in this order:

1. **Password in URI** (Python API only) — `postgresql://user:pass@host/db`
1. **`PGPASSWORD` environment variable** — standard PostgreSQL convention, works with both CLI and API
1. **Interactive prompt** — if neither of the above is set

For CI/CD pipelines, use `PGPASSWORD` to avoid interactive prompts:

```sh
PGPASSWORD=secret pg-upsert -h host -d db -u user \
  -s staging -b public -t books \
  --output json --commit
```

pg-upsert also supports PostgreSQL's [`.pgpass`](https://www.postgresql.org/docs/current/libpq-pgpass.html) file via psycopg2.

## Exit Codes

| Code | Meaning                                            |
| ---- | -------------------------------------------------- |
| 0    | QA passed and upsert completed (or user cancelled) |
| 1    | QA failed, schema check failed, or error           |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, available recipes, testing, and release process.

```bash
git clone https://github.com/geocoug/pg-upsert
cd pg-upsert
just sync
just test
```
