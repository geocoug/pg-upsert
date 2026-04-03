# Architecture

## Module Structure

pg-upsert is organized into focused modules:

```
src/pg_upsert/
  __init__.py        # Public API exports
  __main__.py        # python -m pg_upsert entry point
  cli.py             # Typer CLI (argument parsing, config file loading)
  models.py          # Data classes: UpsertResult, TableResult, QAError
  upsert.py          # PgUpsert facade (orchestrates QA + upsert pipeline)
  postgres.py        # PostgresDB connection wrapper
  control.py         # ControlTable (temporary table tracking pipeline state)
  qa.py              # QARunner (7 QA check methods)
  executor.py        # UpsertExecutor (INSERT/UPDATE with dependency ordering)
  utils.py           # Logging formatter, elapsed time utility

  ui/                # Presentation layer
    __init__.py      # Re-exports UIBackend, get_ui_backend
    base.py          # UIBackend abstract base class
    factory.py       # get_ui_backend() — auto-detection and selection
    console.py       # ConsoleBackend (non-interactive, auto-continue)
    tkinter_backend.py  # TkinterBackend (desktop GUI dialogs)
    textual_backend.py  # TextualBackend (terminal TUI with row matching)
    display.py       # Rich-based formatting (tables, summaries, progress)
    legacy.py        # Original tkinter TableUI/CompareUI widgets
```

## Pipeline Flow

1. **CLI** (`cli.py`) parses arguments, loads config file, creates `PgUpsert`
1. **PgUpsert** (`upsert.py`) is a thin facade that creates:
    - `PostgresDB` — database connection
    - `ControlTable` — temporary table tracking per-table state
    - `QARunner` — QA check engine
    - `UpsertExecutor` — upsert engine
    - `UIBackend` — interactive dialog backend (via factory)
1. **`run()`** orchestrates: QA checks → upsert → commit → return `UpsertResult`

## QA Check Flow

`QARunner.run_all()` iterates over 7 check types × all tables:

1. Column Existence → `check_column_existence()`
1. Column Type Compatibility → `check_type_mismatch()`
1. NOT NULL → `check_nulls()`
1. Primary Key → `check_pks()`
1. Unique Constraints → `check_unique()`
1. Foreign Key → `check_fks()`
1. Check Constraints → `check_cks()`

Each method returns `list[QAError]`, writes errors to the control table, and prints its own pass/fail output via `display.print_check_table_pass()` / `display.print_check_table_fail()`. This means each method produces visible feedback whether called through `run_all()` or standalone (e.g., `qa_column_existence()`).

Schema checks (1-2) run first so column/type issues are caught before data checks.

## Upsert Flow

`UpsertExecutor.upsert_all()`:

1. Computes FK dependency order via recursive CTE
1. For each table in order:
    - Builds column lists and join expressions using `Identifier()` composition
    - Creates temporary views: `ups_cols`, `ups_pks`, `ups_basematches`, `ups_stgmatches`, `ups_newrows`
    - Executes UPDATE (matching rows) and INSERT (new rows)
    - Records accurate `cursor.rowcount` for each operation

## UI Backend Selection

`get_ui_backend(ui_mode)`:

- `"auto"`: DISPLAY/WAYLAND_DISPLAY set → tkinter, otherwise → textual
- `"textual"`: Terminal TUI (works in any terminal including SSH/containers)
- `"tkinter"`: Desktop GUI (requires display server)
- `"_console"`: Internal only — auto-continues without prompting (non-interactive mode)

## Display System

All user-facing output goes through `ui/display.py`:

- **Console** (stderr): Rich-formatted with colors, tables, panels
- **Logfile**: Plain-text equivalent via `_file_logger` (propagate=False)
- No duplicate output — display functions write to both channels

## Temporary Objects Reference

pg-upsert creates temporary tables and views (all prefixed with `ups_`) during QA checks and upsert operations. These objects are session-scoped — they are only visible to the connection that created them and are automatically dropped when the connection closes. Each object is dropped and recreated before use, so re-running on the same connection is safe.

**Why both tables and views?** Tables are used when data is queried multiple times or mutated (e.g., tracking processing state). Views are used for one-shot derived queries that don't need materialized storage.

### Control (`control.py`)

| Object                 | Type  | Description                                                                                                                                                                      |
| ---------------------- | ----- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ups_control`          | table | Main tracking table — one row per table with exclude settings, error flags, and row counts. Name is configurable via `control_table` parameter. Mutated throughout the pipeline. |
| `ups_validate_control` | table | Validation state for control table entries — records whether each table exists in both base and staging schemas.                                                                 |
| `ups_ctrl_invl_table`  | table | Aggregated list of invalid table references (missing from either schema). Empty if validation passes.                                                                            |

### QA Checks (`qa.py`)

#### Column Existence / Type Compatibility

| Object                | Type | Created by                 | Description                                                                              |
| --------------------- | ---- | -------------------------- | ---------------------------------------------------------------------------------------- |
| `ups_missing_cols`    | view | `check_column_existence()` | Base table columns that do not exist in the staging table.                               |
| `ups_type_mismatches` | view | `check_type_mismatch()`    | Columns present in both tables with incompatible types (no implicit or assignment cast). |

#### Primary Key Checks

| Object                    | Type  | Created by    | Description                                                         |
| ------------------------- | ----- | ------------- | ------------------------------------------------------------------- |
| `ups_primary_key_columns` | table | `check_pks()` | Primary key columns of the base table, used for duplicate checking. |
| `ups_pk_check`            | view  | `check_pks()` | Groups staging rows by PK columns — count > 1 indicates duplicates. |

#### Unique Constraint Checks

| Object                   | Type  | Created by       | Description                                                                        |
| ------------------------ | ----- | ---------------- | ---------------------------------------------------------------------------------- |
| `ups_unique_constraints` | table | `check_unique()` | All UNIQUE constraints on the table with their column lists.                       |
| `ups_uq_check`           | view  | `check_unique()` | Groups staging rows by unique constraint columns — count > 1 indicates duplicates. |

#### Foreign Key Checks

| Object                    | Type  | Created by    | Description                                                                         |
| ------------------------- | ----- | ------------- | ----------------------------------------------------------------------------------- |
| `ups_foreign_key_columns` | table | `check_fks()` | Complete map of all FK constraints in the database. Created once per session.       |
| `ups_sel_fks`             | table | `check_fks()` | FK constraints relevant to the table being checked.                                 |
| `ups_fk_constraints`      | table | `check_fks()` | Distinct constraints with error count and processing flag. Mutated during checks.   |
| `ups_one_fk`              | table | `check_fks()` | Single constraint's columns, extracted for the FK currently being checked.          |
| `ups_fk_check`            | view  | `check_fks()` | Staging rows with invalid foreign keys (referenced rows missing from base/staging). |

#### Check Constraint Checks

| Object                  | Type  | Created by    | Description                                                                                                 |
| ----------------------- | ----- | ------------- | ----------------------------------------------------------------------------------------------------------- |
| `ups_check_constraints` | table | `check_cks()` | All CHECK constraints in the base schema. Created once per session.                                         |
| `ups_sel_cks`           | table | `check_cks()` | CHECK constraints for the table being checked, with error count and processing flag. Mutated during checks. |
| `ups_ck_check_check`    | view  | `check_cks()` | Count of rows violating the current CHECK constraint.                                                       |
| `ups_ck_error_list`     | view  | `check_cks()` | Aggregated error summary from `ups_sel_cks` for all violated constraints.                                   |

### Upsert Operations (`executor.py`)

| Object               | Type  | Created by     | Description                                                                       |
| -------------------- | ----- | -------------- | --------------------------------------------------------------------------------- |
| `ups_dependencies`   | table | `upsert_all()` | FK parent-child relationships used to compute processing order.                   |
| `ups_ordered_tables` | table | `upsert_all()` | Tables ordered by FK dependency depth (parents before children).                  |
| `ups_cols`           | table | `upsert_one()` | Columns present in both staging and base, excluding `exclude_cols`.               |
| `ups_pks`            | table | `upsert_one()` | Primary key columns of the base table.                                            |
| `ups_basematches`    | view  | `upsert_one()` | Base table rows matching staging by PK — used for interactive comparison.         |
| `ups_stgmatches`     | view  | `upsert_one()` | Staging table rows matching base by PK — used for interactive comparison.         |
| `ups_nk`             | view  | `upsert_one()` | Non-key columns (in `ups_cols` but not `ups_pks`) — identifies columns to UPDATE. |
| `ups_newrows`        | view  | `upsert_one()` | Staging rows whose PKs don't exist in base — candidates for INSERT.               |

## Key Design Decisions

- **`run()` returns `UpsertResult`** — structured result with `.qa_passed`, `.committed`, `.to_json()`
- **SQL injection prevention** — column lists and join expressions built with `Identifier()` composition, not `SQL()` wrapping of database-derived strings
- **Lazy imports** — tkinter and textual are never imported at module level
- **Exit code 1 on QA failure** — CLI exits non-zero so CI pipelines detect failures
- **`--output json` suppresses all console output** — `display.console.quiet = True` ensures only clean JSON on stdout
- **Password resolution** — checks URI password → `PGPASSWORD` env var → interactive prompt (in that order)
- **Logfile append-only** — runs are separated by `====` header/footer with timestamps, version, and PostgreSQL version
