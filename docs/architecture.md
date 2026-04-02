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

Each method returns `list[QAError]` and writes errors to the control table.
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

## Key Design Decisions

- **`run()` returns `UpsertResult`** — structured result with `.qa_passed`, `.committed`, `.to_json()`
- **SQL injection prevention** — column lists and join expressions built with `Identifier()` composition, not `SQL()` wrapping of database-derived strings
- **Lazy imports** — tkinter and textual are never imported at module level
- **Exit code 1 on QA failure** — CLI exits non-zero so CI pipelines detect failures
- **`--output json` suppresses all console output** — `display.console.quiet = True` ensures only clean JSON on stdout
- **Password resolution** — checks URI password → `PGPASSWORD` env var → interactive prompt (in that order)
- **Logfile append-only** — runs are separated by `====` header/footer with timestamps, version, and PostgreSQL version
