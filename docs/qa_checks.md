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

## Schema-Only Validation

Run only column existence and type compatibility checks without any data checks:

```sh
pg-upsert --check-schema -h localhost -d mydb -u user -s staging -b public -t books
```

Exit code 0 means compatible, exit code 1 means issues found. Combine with `--output json` for machine-parseable results.
