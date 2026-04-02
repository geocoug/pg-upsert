# pg-upsert

[![ci/cd](https://github.com/geocoug/pg-upsert/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/geocoug/pg-upsert/actions/workflows/ci-cd.yml)
[![codecov](https://codecov.io/gh/geocoug/pg-upsert/graph/badge.svg)](https://codecov.io/gh/geocoug/pg-upsert)
[![Documentation Status](https://readthedocs.org/projects/pg-upsert/badge/?version=latest)](https://pg-upsert.readthedocs.io/en/latest/?badge=latest)
[![PyPI Latest Release](https://img.shields.io/pypi/v/pg-upsert.svg)](https://pypi.org/project/pg-upsert/)
[![PyPI Downloads](https://img.shields.io/pypi/dm/pg-upsert.svg?label=pypi%20downloads)](https://pypi.org/project/pg-upsert/)
[![Python Version Support](https://img.shields.io/pypi/pyversions/pg-upsert.svg)](https://pypi.org/project/pg-upsert/)

**pg-upsert** is a Python package for validating and upserting data from staging tables into base tables in PostgreSQL. It runs automated QA checks, reports errors with rich formatted output, and performs dependency-aware upserts.

## Why Use `pg-upsert`?

- **7 Automated QA Checks** – Validates [NOT NULL](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-NOT-NULL), [PRIMARY KEY](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-PRIMARY-KEYS), [UNIQUE](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-UNIQUE-CONSTRAINTS), [FOREIGN KEY](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-FK), [CHECK CONSTRAINT](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-CHECK-CONSTRAINTS), column existence, and column type compatibility before any modifications occur.
- **Interactive Confirmation** – Two UI backends: Textual TUI (terminal) and Tkinter (desktop). Auto-detected or choose with `--ui auto|textual|tkinter`.
- **Structured Results** – `run()` returns an `UpsertResult` with per-table stats, QA errors, and JSON serialization (`--output=json` for CI/CD pipelines).
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

result = PgUpsert(
    conn=conn,
    tables=("genres", "publishers", "books"),
    staging_schema="staging",
    base_schema="public",
    do_commit=True,
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
| `-u`, `--user`            | Database user (password is prompted securely)             |
| `-s`, `--staging-schema`  | Staging schema name (default: staging)                    |
| `-b`, `--base-schema`     | Base schema name (default: public)                        |
| `-t`, `--table`           | Table name to process (repeatable)                        |
| `-x`, `--exclude-columns` | Columns to exclude from upsert (repeatable)               |
| `-n`, `--null-columns`    | Columns to skip during NOT NULL checks (repeatable)       |
| `-m`, `--upsert-method`   | `upsert`, `update`, or `insert` (default: upsert)         |
| `-c`, `--commit`          | Commit changes (default: roll back)                       |
| `-i`, `--interactive`     | Prompt for confirmation at each step                      |
| `-l`, `--logfile`         | Write log to file (appends, does not overwrite)           |
| `-o`, `--output`          | Output format: `text` (default) or `json`                 |
| `--check-schema`          | Validate column existence and types only, then exit       |
| `--ui`                    | Interactive UI: `auto` (default), `textual`, or `tkinter` |
| `-f`, `--config-file`     | Path to YAML configuration file                           |
| `-g`, `--generate-config` | Generate a template config file                           |
| `-v`, `--version`         | Show version and exit                                     |
| `--debug`                 | Enable debug output                                       |
| `-q`, `--quiet`           | Suppress console output                                   |

> [!NOTE]
> CLI arguments take precedence over configuration file values. Explicit CLI flags are never overridden by the config file.

#### Configuration File

Create a YAML config file (see [pg-upsert.example.yaml](https://github.com/geocoug/pg-upsert/blob/main/pg-upsert.example.yaml)):

```yaml
host: "localhost"
port: 5432
user: "docker"
database: "dev"
staging_schema: "staging"
base_schema: "public"
commit: true
upsert_method: "upsert"
tables:
  - "genres"
  - "publishers"
  - "books"
  - "authors"
  - "book_authors"
exclude_columns:
  - "rev_time"
  - "rev_user"
null_columns:
  - "book_alias"
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

| Check                | What it validates                                                                             |
| -------------------- | --------------------------------------------------------------------------------------------- |
| **Column Existence** | All base table columns exist in the staging table (respects `--exclude-columns`)              |
| **Column Type**      | No hard type incompatibilities between staging and base (uses PostgreSQL's `pg_cast` catalog) |
| **NOT NULL**         | Non-nullable base columns have no NULL values in staging                                      |
| **Primary Key**      | No duplicate values in PK columns                                                             |
| **Unique**           | No duplicate values in UNIQUE-constrained columns (NULLs allowed per PostgreSQL semantics)    |
| **Foreign Key**      | All FK references point to existing rows in the referenced table                              |
| **Check Constraint** | All CHECK constraint expressions evaluate to true                                             |

See the [QA Checks Reference](https://pg-upsert.readthedocs.io/) for detailed documentation.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, available recipes, testing, and release process.

```bash
git clone https://github.com/geocoug/pg-upsert
cd pg-upsert
just sync
just test
```
