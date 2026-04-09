# QA Checks Reference

pg-upsert runs 7 types of quality assurance checks on staging table data before performing any upsert operations. Checks run in the order listed below — schema checks first, then data checks.

## Column Existence

Verifies that every column in the base table also exists in the staging table. Columns listed in `exclude_cols` are not flagged as missing.

- **Control table column**: `column_errors`
- **Catalog source**: [`information_schema.columns`](https://www.postgresql.org/docs/current/infoschema-columns.html)
- **Example error**: `notes` (base column missing from staging)

## Column Type Compatibility

Detects hard type incompatibilities between staging and base columns. Only flags types where PostgreSQL has **no implicit or assignment cast** — soft coercions like `varchar(100)` to `text` are not flagged.

- **Control table column**: `type_errors`
- **Catalog source**: [`information_schema.columns`](https://www.postgresql.org/docs/current/infoschema-columns.html) + [`pg_cast`](https://www.postgresql.org/docs/current/catalog-pg-cast.html)
- **Example error**: `publisher_name (integer → varchar)`

## NOT NULL

Checks that [non-nullable columns](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-NOT-NULL) in the base table have no NULL values in the corresponding staging table columns. Columns listed in `exclude_null_check_cols` are skipped (useful for auto-generated columns like serials or timestamps).

- **Control table column**: `null_errors`
- **Catalog source**: [`information_schema.columns`](https://www.postgresql.org/docs/current/infoschema-columns.html) (`is_nullable = 'NO'`)
- **Example error**: `first_name (1), last_name (2)`

## Primary Key

Checks for duplicate values in [primary key](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-PRIMARY-KEYS) columns. Tables without a primary key are skipped.

- **Control table column**: `pk_errors`
- **Catalog source**: [`information_schema.table_constraints`](https://www.postgresql.org/docs/current/infoschema-table-constraints.html) + [`information_schema.key_column_usage`](https://www.postgresql.org/docs/current/infoschema-key-column-usage.html)
- **Example error**: `2 duplicate keys (4 rows) in table staging.authors`

## Unique Constraints

Checks for duplicate values in columns with [UNIQUE constraints](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-UNIQUE-CONSTRAINTS) (excluding primary keys, which are checked separately). Multiple NULL values are allowed per PostgreSQL semantics.

- **Control table column**: `unique_errors`
- **Catalog source**: [`pg_constraint`](https://www.postgresql.org/docs/current/catalog-pg-constraint.html) (`contype = 'u'`)
- **Example error**: `uq_authors_email (1 duplicates, 2 rows)`

## Foreign Key

Validates that all [foreign key](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-FK) references in the staging table point to existing rows in the referenced table. If the referenced table also has a staging version, both the base and staging versions are checked.

- **Control table column**: `fk_errors`
- **Catalog source**: [`pg_constraint`](https://www.postgresql.org/docs/current/catalog-pg-constraint.html) (`contype = 'f'`) + [`pg_attribute`](https://www.postgresql.org/docs/current/catalog-pg-attribute.html)
- **Example error**: `books_genre_fkey (3)`

## Check Constraints

Evaluates [CHECK constraint](https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-CHECK-CONSTRAINTS) expressions from the base table against staging data. The constraint SQL is extracted from [`pg_constraint`](https://www.postgresql.org/docs/current/catalog-pg-constraint.html) and applied as a `WHERE NOT (...)` filter.

- **Control table column**: `ck_errors`
- **Catalog source**: [`pg_constraint`](https://www.postgresql.org/docs/current/catalog-pg-constraint.html) (`contype = 'c'`)
- **Example error**: `chk_authors_first_name (1)`

## Running Without Constraints

pg-upsert is constraint-driven: every data QA check reads the base table's
constraint catalog and validates staging against what it finds. If a table
has no constraints of a given type, the corresponding check passes
vacuously — it has nothing to compare against.

Concretely, on a table with **no constraints at all**:

| Check                     | Behavior                                                                      |
| ------------------------- | ----------------------------------------------------------------------------- |
| Column existence          | Still runs — compares staging and base column lists regardless of constraints |
| Column type compatibility | Still runs — compares column types regardless of constraints                  |
| NOT NULL                  | Passes (no non-nullable columns to check)                                     |
| Primary Key               | Passes (no PK to check)                                                       |
| Unique Constraints        | Passes (no unique constraints)                                                |
| Foreign Key               | Passes (no FKs)                                                               |
| Check Constraints         | Passes (no CHECK expressions)                                                 |

!!! warning "Upsert is skipped for tables without a primary key"

    The **upsert step** requires a primary key on the base table to decide
    which staging rows are updates and which are inserts. If the base table
    has no PK, pg-upsert prints a warning and **skips that table entirely**
    — no rows are inserted or updated. The exit code is still `0` unless
    another table failed QA.

    ```text
      public.books
      Warning: Base table has no primary key
    ```

    To upsert against a table without a PK, add a PK to the base table
    first. If you only want to INSERT rows (not UPDATE existing ones),
    use `--upsert-method insert` — but this also requires a PK to detect
    which staging rows are "new" via the `ups_stgmatches` / `ups_nk`
    join logic.

In practice this means pg-upsert is most useful when your base schema has
at least primary keys. Tables without constraints still benefit from the
column existence and type compatibility checks, so `--check-schema` alone
can be used as a lightweight schema-compatibility validator on otherwise
unconstrained databases.

## Configuration

### Excluding columns from checks

Use `exclude_cols` to skip columns that exist in the base but not in staging (e.g., auto-generated `rev_time`, `rev_user`):

```python
PgUpsert(
    ...,
    exclude_cols=("rev_user", "rev_time"),
)
```

CLI: `pg-upsert ... -x rev_user -x rev_time`

### Excluding columns from NULL checks

Use `exclude_null_check_cols` for columns that are NOT NULL in the base but intentionally empty in staging (e.g., serial columns):

```python
PgUpsert(
    ...,
    exclude_null_check_cols=("book_alias",),
)
```

CLI: `pg-upsert ... -n book_alias`

## Output

Each QA check method prints per-table pass/fail output:

```text
  ✓ [1/3] staging.genres
  ✓ [2/3] staging.publishers
  ✗ [3/3] staging.books — book_title (1)
```

Pass indicators (`✓`) are shown for every table that has no errors for the current check. Fail indicators (`✗`) include the error details. Progress counters (`[N/total]`) appear when multiple tables are checked through `run()`, `qa_all()`, or any `qa_all_*()` facade method. This output goes to both the Rich console (stderr) and the logfile.

When run through `run()` or `qa_all()`, phase headers and a summary panel are also printed. When individual methods are called standalone (e.g., `qa_column_existence()`), only the per-table pass/fail lines are printed.

For programmatic use, the `CheckContext` dataclass can be passed to any `check_*` method to control progress display:

```python
from pg_upsert import CheckContext

ctx = CheckContext(table_num=1, total_tables=3)
errors = ups._qa.check_nulls("genres", ctx=ctx)
```

## Schema-Only Validation

Run only column existence and type compatibility checks without any data checks:

```sh
pg-upsert --check-schema -h localhost -d mydb -u user -s staging -b public -t books
```

```text
  ✓ staging.books
  ✓ staging.books
```

Exit code 0 means compatible, exit code 1 means issues found. Combine with `--output json` for machine-parseable results.

Via the Python API:

```python
ups = PgUpsert(
    uri="postgresql://user@localhost:5432/mydb",
    tables=("books",),
    staging_schema="staging",
    base_schema="public",
).qa_column_existence().qa_type_mismatch()

if ups.qa_errors:
    for err in ups.qa_errors:
        print(f"{err.table}: {err.check_type.value} — {err.details}")
```

## Exporting a Fix Sheet

When QA checks fail, pg-upsert can write a **fix sheet** — an actionable
report showing exactly which staging rows need to be corrected. Use
`--export-failures <dir>` to specify an output directory and
`--export-format` to pick a file format:

```bash
pg-upsert -h localhost -d dev -u docker \
  -s staging -b public -t books \
  --export-failures ./failures/ --export-format csv
```

The fix sheet contains **one row per unique violating staging row**
(deduped by primary key). Every problem found on that row is merged into
a single `_issues` column, with a parallel `_issue_types` column listing
the check types that flagged it. For example:

| book_id | title    | genre   | price | \_issues                                                      | \_issue_types |
| ------- | -------- | ------- | ----- | ------------------------------------------------------------- | ------------- |
| 101     | Dune     |         | 9.99  | NULL in 'genre'; duplicate PK (book_id)                       | null,pk       |
| 205     |          | sci-fi  | 14.99 | NULL in 'title'                                               | null          |
| 300     | Free     | fiction | -1    | check 'price_positive' failed                                 | ck            |
| 410     | Untitled | fic     | 5.0   | FK violation: publisher_id -> public.publishers(publisher_id) | fk            |

Three output formats are supported:

| `--export-format` | Output                       | Contents                                           |
| ----------------- | ---------------------------- | -------------------------------------------------- |
| `csv` (default)   | Directory of per-table files | `pg_upsert_failures_<table>.csv` per table         |
| `json`            | Single nested file           | `pg_upsert_failures.json` with a key per table     |
| `xlsx`            | Single workbook              | `pg_upsert_failures.xlsx` with one sheet per table |

Schema-level problems (missing columns, type mismatches) are written to a
dedicated `_schema` output: `pg_upsert_failures_schema.csv` (CSV mode),
the `_schema` key (JSON), or the `_schema` sheet (XLSX). They are kept
separate from the row-level fix sheets because they require a different
remediation path (fix the staging loader, not the data).

The row cap per check per table is controlled by `--export-max-rows`
(default 1000).
