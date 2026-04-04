______________________________________________________________________

## name: pg-upsert project context description: Canonical project context for pg-upsert — origin, tooling, roadmap, collaboration norms type: project

# pg-upsert Project Context

> **Canonical location:** `.claude/project_context.md` in the repo.
> Update this file whenever any architectural, tooling, or directional decision is made.
> Update the CHANGELOG.md whenever making a release or significant change.

______________________________________________________________________

## Origin

`pg-upsert` is a Python library for upserting data into PostgreSQL databases with automated integrity checks and interactive confirmation. Created and maintained by Caleb Grant (cgrant).

- Repo: `https://github.com/geocoug/pg-upsert` (git)
- PyPI package name: `pg_upsert`
- Documentation: `https://pg-upsert.readthedocs.io`
- Maintainer: Caleb Grant <grantcaleb22@gmail.com>

**Purpose:** Synchronize data between staging and production PostgreSQL tables with automated QA checks (NOT NULL, Primary Key, Foreign Key, Check Constraints) before performing upserts.

______________________________________________________________________

## What pg-upsert Does

A Python library and CLI tool that:

1. Connects to a PostgreSQL database
2. Runs QA checks on staging tables (NOT NULL, PK uniqueness, FK references, CHECK constraints)
3. Presents results and optionally prompts for interactive confirmation via a Tkinter GUI
4. Performs upsert (insert + update), update-only, or insert-only operations from staging tables to base tables
5. Supports YAML configuration files for repeatable workflows

______________________________________________________________________

## Repository Structure

```
src/pg_upsert/               # Main source package
  __init__.py                 # Exports PgUpsert, PostgresDB, models
  __main__.py                 # Entry point for python -m pg_upsert
  cli.py                      # Typer CLI implementation
  postgres.py                 # PostgresDB connection handler
  upsert.py                   # Core PgUpsert class — orchestration facade
  qa.py                       # QARunner — all QA check logic
  executor.py                 # UpsertExecutor — INSERT/UPDATE logic
  control.py                  # ControlTable — temp control table management
  models.py                   # Data models: UpsertResult, QAError, callbacks
  utils.py                    # CustomLogFormatter, elapsed_time()
  ui/                         # UI backends
    __init__.py               # UIBackend protocol, get_ui_backend()
    base.py                   # UIBackend abstract base
    factory.py                # Backend auto-detection
    display.py                # Rich-based display utilities (console + logfile)
    console.py                # Console-only backend (non-interactive)
    legacy.py                 # Tkinter CompareUI and TableUI
    tkinter_backend.py        # Tkinter UIBackend adapter
    textual_backend.py        # Textual TUI backend
tests/
  conftest.py                 # Pytest fixtures (global_variables, db, ups)
  test_cli.py                 # CLI argument and config tests
  test_postgres.py            # PostgresDB connection tests
  test_upsert.py              # Core upsert logic tests
  test_display.py             # Display function tests
  test_models.py              # Data model tests
  test_console_backend.py     # Console backend tests
  test_ui_factory.py          # UI factory tests
  test_utils.py               # Utility function tests
  data/
    schema_passing.sql         # Valid test data (all constraints pass)
    schema_failing.sql         # Invalid test data (constraint violations)
docs/                          # Zensical documentation
.github/workflows/
  ci-cd.yml                    # CI/CD pipeline
Dockerfile                     # Multi-stage Alpine build
pyproject.toml                 # Build config, tooling
.pre-commit-config.yaml        # Pre-commit hooks
pg-upsert.example.yaml         # Example YAML configuration
```

______________________________________________________________________

## Module Overview

| Module | Purpose | Lines |
|--------|---------|-------|
| `qa.py` | QARunner — null, PK, FK, CK, unique, column, type checks | ~1,020 |
| `upsert.py` | PgUpsert facade — orchestration, validation, commit logic | ~790 |
| `executor.py` | UpsertExecutor — INSERT/UPDATE SQL generation and execution | ~520 |
| `ui/legacy.py` | Tkinter CompareUI and TableUI dialogs | ~475 |
| `cli.py` | Typer CLI app — argument parsing, YAML config loading | ~430 |
| `ui/textual_backend.py` | Textual TUI backend for interactive mode | ~400 |
| `control.py` | ControlTable — temp table management, error tracking | ~385 |
| `ui/display.py` | Rich-based display utilities (console + file logging) | ~320 |
| `postgres.py` | PostgresDB — connection, query execution, transactions | ~235 |
| `models.py` | UpsertResult, QAError, PipelineCallback, data models | ~175 |

______________________________________________________________________

## Tooling Decisions

| Tool | Purpose | Decision |
|------|---------|----------|
| `uv` | Package/env management | Chosen over pip/poetry |
| `setuptools` | Build backend | via pyproject.toml (dynamic version from `importlib.metadata`) |
| `ruff` | Lint + format | line-length 120, target py313, select E/F/Q/B/I/UP/N/S/C4/T20/RET/SIM/PD/RUF |
| `tox-uv` | Multi-Python test matrix | py310–py313 |
| `bump-my-version` | Version bumping | tags + commits |
| `pre-commit` | Git hooks | gitleaks, validate-pyproject, ruff, mdlint, typos |
| `mkdocs` | Docs builder | Material theme, mkdocstrings (to migrate to zensical) |
| `pytest-cov` | Coverage | branch coverage enabled |
| `typer` | CLI framework | Modern CLI with type hints |

______________________________________________________________________

## Python Version Support

Requires Python >=3.10, <3.14. CI matrix: 3.10, 3.11, 3.12, 3.13.

______________________________________________________________________

## Dependencies

**Runtime:**
- `psycopg2-binary` — PostgreSQL client
- `typer` — CLI framework
- `pyyaml` — Config file parsing
- `rich` — Terminal formatting, tables, panels

**Dev:**
- pytest, pytest-cov, ruff, mkdocs + extensions, pre-commit, tox-uv, build, bump-my-version, twine

______________________________________________________________________

## CI/CD Pipeline (`.github/workflows/ci-cd.yml`)

Triggered on: push to `main`, any tag `v*.*.*`, pull requests.

1. **tests** — Python 3.10–3.13 on ubuntu-latest with PostgreSQL service (postgis/postgis:16-3.4-alpine)
2. **build** — runs only on `v*` tags, builds sdist + wheel
3. **publish** — pushes to PyPI via OIDC trusted publishing
4. **docker-build** — Multi-arch Docker (linux/amd64, linux/arm64), pushes to GHCR
5. **generate-release** — creates GitHub Release with dist artifacts

______________________________________________________________________

## Versioning

`bump-my-version` manages versions. Current: `1.18.2`. Version is read dynamically via `importlib.metadata`.

______________________________________________________________________

## Key Classes

### PgUpsert (upsert.py)
The facade class. Constructor accepts: `uri`/`conn`, `tables`, `staging_schema`, `base_schema`, `do_commit`, `interactive`, `upsert_method` (upsert/update/insert), `exclude_cols`, `exclude_null_check_cols`, `ui_mode`, `compact`, `callback`.

**QA methods:** `qa_all()`, `qa_all_null()`, `qa_one_null()`, `qa_all_pk()`, `qa_one_pk()`, `qa_all_fk()`, `qa_one_fk()`, `qa_all_ck()`, `qa_one_ck()`, `qa_all_unique()`, `qa_one_unique()`, `qa_column_existence()`, `qa_type_mismatch()`

**Upsert methods:** `upsert_all()`, `upsert_one()`, `run()` (full workflow), `commit()`, `cleanup()`

**Safety guards:** `commit()` refuses when `qa_passed` is False. `upsert_all()` refuses when `qa_passed` is False. All step-by-step QA methods update `qa_passed` automatically.

Delegates to: `QARunner` (qa.py), `UpsertExecutor` (executor.py), `ControlTable` (control.py).

### QARunner (qa.py)
Runs all QA checks: nulls, PKs, FKs, check constraints, unique constraints, column existence, type mismatches. Returns `list[QAError]`.

### UpsertExecutor (executor.py)
Executes INSERT/UPDATE operations. Handles dependency ordering, column exclusion, and row counting.

### ControlTable (control.py)
Manages the `ups_control` temporary table that tracks per-table state (errors, row counts, exclude columns).

### PostgresDB (postgres.py)
Connection handler. Methods: `open_db()`, `cursor()`, `execute()`, `rowdict()`, `commit()`, `rollback()`, `close()`.

### Models (models.py)
`UpsertResult`, `TableResult`, `QAError`, `QACheckType`, `PipelineCallback`, `PipelineEvent`, `CallbackEvent`, `UserCancelledError`.

### CLI (cli.py)
Typer app with options for database connection, schema names, upsert method, column exclusions, YAML config file, interactive mode, logging, `--output json`, `--check-schema`, `--compact`, `--ui`.

### UI (ui/ subpackage)
Pluggable UI backends: Tkinter (`legacy.py`), Textual TUI (`textual_backend.py`), Console (`console.py`). Factory auto-detects available backend. Rich-based `display.py` handles all console output and logfile sync.

______________________________________________________________________

## Roadmap / Upcoming Work

- [ ] Progress indication — table counters or progress bar during long batch runs
- [ ] Per-table column exclusion — allow different `exclude_cols` per table
- [ ] Per-table error recovery — continue with independent tables on failure
- [ ] Shell completion — Typer auto-generation

______________________________________________________________________

## Working with Claude

These principles guide all collaboration in this project:

**Craft over convenience** — take the approach that produces the best result, not the fastest one.

**Deliberate decision-making** — understand _why_ before choosing _what_.

**No time pressure** — quality and correctness matter more than speed. Take the time needed.

**Best practices baseline** — follow Python and project conventions unless there's a documented reason not to.

**Understand before acting** — read the code before changing it. Understand the full context.

**Minimal surface area** — change only what's needed. Don't refactor what wasn't asked to be refactored.

**Secure by default** — parameterized queries, validated inputs, no hardcoded secrets.

**Honesty over agreeableness** — flag problems, push back on bad ideas, ask clarifying questions.
