# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

______________________________________________________________________

## [Unreleased]

______________________________________________________________________

## [1.18.0] - 2026-04-03

### Added

- `PgUpsert.cleanup()` method — explicitly drops all `ups_*` temporary tables and views created during the pipeline. Useful for long-lived connections where you want to reclaim temp object space without closing the connection.
- `callback` parameter on `PgUpsert` — optional callable that fires `QA_TABLE_COMPLETE` and `UPSERT_TABLE_COMPLETE` events during the pipeline. Return `False` from the callback to abort with rollback. New types: `CallbackEvent`, `PipelineEvent`, `PipelineCallback`.

### Fixed

- Removed stale docstring in `QARunner.check_nulls()` referencing nonexistent temporary objects.

______________________________________________________________________

## [1.17.0] - 2026-04-03

### Changed

- All QA check methods now print pass/fail output individually via `display.print_check_table_pass()` / `display.print_check_table_fail()`. Previously, pass output was only produced when checks were run through `run_all()` — standalone calls (e.g., `qa_column_existence()`, `qa_type_mismatch()`) were silent on success.

______________________________________________________________________

## [1.16.1] - 2026-04-03

### Fixed

- `PostgresDB.__del__()` and `close()` no longer close externally-provided connections (`conn=`). Only connections created by `PostgresDB` itself are closed. Previously, garbage collection of a `PgUpsert` instance would close the caller's connection.

______________________________________________________________________

## [1.16.0] - 2026-04-03

### Added

- `PgUpsert.qa_errors` attribute — accumulates `QAError` instances from any QA method (`qa_all()`, `qa_column_existence()`, `qa_type_mismatch()`, individual `qa_one_*()` methods, etc.). Enables programmatic inspection of QA results without requiring a full `run()` call.
- `UpsertResult` now includes `staging_schema`, `base_schema`, `upsert_method`, `started_at`, `finished_at`, and `duration_seconds` in `to_dict()`/`to_json()` output.
- `PGPASSWORD` environment variable support for non-interactive authentication (standard PostgreSQL convention).
- `--output json` now suppresses all console output — only clean JSON on stdout.
- `--encoding` documented in README CLI options table.
- Authentication section in README explaining password resolution order.

### Fixed

- Fixed `elapsed_time()` calling `datetime.now()` twice — now uses cached value.
- Narrowed `except Exception` to specific exception types in `postgres.py` and `upsert.py`.
- Fixed stale docstring reference to `pg_upsert.ui_console` → `pg_upsert.ui.console`.

______________________________________________________________________

## [1.15.0] - 2026-04-02

### Added

- pg-upsert version and PostgreSQL version now logged in logfile header.
- Progress counters in QA check output: phase `(3/7)` in section headers, table `[1/5]` on pass lines.
- Logfile run header/footer with `====` separators and timestamps for easy navigation in appended logs.
- Table count shown in "Tables selected for upsert" message.
- Exit code 1 when QA checks fail — CI pipelines can detect failures without parsing output.
- New `docs/architecture.md` — module structure, pipeline flow, design decisions for contributors.
- API reference now documents `UpsertResult`, `TableResult`, `QAError`, `QACheckType`, `UserCancelledError`.
- `--docs` added to CLI options table in README.
- Exit Codes section in README.

### Changed

- README config file example updated to show all available options.
- Config file now supports `output`, `check_schema`, `compact`, `ui_mode` keys.
- Dockerfile: removed tkinter GUI deps (useless in Docker), installs `[tui]` extra (textual) instead. Added OCI labels.

______________________________________________________________________

## [1.14.0] - 2026-04-02

### Added

- `--compact` CLI flag for grid-style QA summary (✓/✗ per check type per table).
- Composite UNIQUE constraint (`uq_books_title_genre`) added to test schemas.
- Tests: composite UNIQUE violations, empty staging table edge cases (null/pk/unique/upsert with 0 rows), FK dependency ordering validation.
- 263 total tests, 93% coverage.

### Changed

- Removed `--quiet` CLI flag (no longer functional — all output goes through rich console).

### Fixed

- Fixed `open_db` reconnect fragility — now stores original URI at init time for reliable reconnection instead of regex-based URI reconstruction.

______________________________________________________________________

## [1.13.1] - 2026-04-02

### Fixed

- Fixed check constraint output using bare `logger.warning` instead of rich `✗` formatting — now consistent with all other check types.
- Removed `docs/index.md` from git tracking (auto-generated from README.md by justfile/RTD build).

______________________________________________________________________

## [1.13.0] - 2026-04-02

### Added

- 91 new tests: `test_models.py`, `test_display.py`, `test_ui_factory.py`, `test_console_backend.py`, facade method tests. Total: 256 tests, 93% coverage.

### Changed

- Rewrote `docs/examples.md` with current output format (rich ✓/✗ indicators, summary layout, UpsertResult JSON examples).
- Optimized `check_nulls()` from N+1 queries (one per column) to a single `SUM(CASE WHEN ... IS NULL)` query.
- Replaced all `SQL()` wrapping of DB-derived strings in `executor.py` with proper `Identifier()` composition for column lists, join expressions, SET clauses, and PK lists.

### Fixed

- Fixed `exclude_null_check_cols` generating invalid `NOT IN ('')` SQL when the exclusion list is empty — now conditionally includes the clause only when columns are specified.

______________________________________________________________________

## [1.12.0] - 2026-04-02

### Added

- Start and end timestamps with elapsed duration logged for every `run()` invocation.
- Row matching in Textual TUI comparison view — clicking a row in either table scrolls to the matching PK row in the other table.
- New documentation page: QA Checks Reference (`docs/qa_checks.md`) with PostgreSQL documentation links.
- Codecov badge in README.

### Changed

- **BREAKING**: Removed `__version__.py` — version is now read from package metadata via `importlib.metadata.version("pg_upsert")` at runtime.
- **BREAKING**: `--tables` CLI option renamed to `--table` (singular, since each flag specifies one table).
- Reorganized source into `ui/` subpackage: `ui/base.py`, `ui/factory.py`, `ui/console.py`, `ui/tkinter_backend.py`, `ui/textual_backend.py`, `ui/display.py`, `ui/legacy.py`.
- Textual comparison view row matching now uses `RowHighlighted` event (matching execsql's pattern) instead of `CursorChanged`. Pre-builds PK→row-index maps at mount time for O(1) lookup.
- Password prompt shows rich-formatted connection info: `PostgreSQL → user@host:port/db`.
- Invalid schema/table errors show rich `✗` formatting.
- "Tables selected for upsert" shown as labeled list with `staging → public` context.
- `--check-schema` output uses rich section header, per-table ✓/✗, and bold summary.
- All CLI validation errors use consistent rich formatting via `_cli_error()` helper.
- `--ui` validated before password prompt to avoid unnecessary prompting.
- Logfile appends instead of being deleted and recreated on each run.
- Display logger uses `propagate=False` to prevent duplicate console output.
- Removed `console` as a public `--ui` option — console backend is internal-only for non-interactive runs.
- Updated README: complete rewrite with CLI options table, fixed parameter names, documented UpsertResult.
- Updated `pyproject.toml` description, removed `markdown-include` dependency.
- `docs/index.md` auto-generated from `README.md` via justfile/RTD build.

### Fixed

- Fixed `_validate_schemas` and `_validate_table` executing same query twice — now caches cursor result.
- Guarded all `next(iter())` calls against StopIteration — replaced with `[0]` indexing or added safety comments.
- Fixed `strict=False` in `ui/console.py` zip — now `strict=True`.
- Fixed broad `except Exception` in config file loading — now catches `yaml.YAMLError, OSError`.
- Fixed misleading "No columns found in base table" log message — now says "No shared columns".
- Fixed dead code: unreachable `else` branch in postgres.py rowdict encoding check.

### Removed

- `src/pg_upsert/__version__.py` — version, title, description, and other metadata are no longer maintained in a separate file.
- `markdown-include` dependency (no longer used after docs migration to zensical).

______________________________________________________________________

## [1.11.2] - 2026-04-02

### Added

- Green ✓ checkmarks now shown for QA checks that pass, not just ✗ for failures. Gives users confidence that checks actually ran.

______________________________________________________________________

## [1.11.1] - 2026-04-02

### Changed

- All `display.print_*` functions now dual-write: rich output to console (stderr) and plain-text equivalent to the logger (logfile). Console and logfile are always in sync.
- All QA check types (null, column existence, type mismatch) now use consistent `✗` prefix via `print_check_table_fail()`, matching PK/FK/UNIQUE/CK checks.
- QA summary supports compact grid mode via `compact=True` parameter — shows ✓/✗ per check type per table in a minimal grid.

______________________________________________________________________

## [1.11.0] - 2026-04-02

### Added

- `ui_base.py` — `UIBackend` abstract base class defining `show_table()` and `show_comparison()` for all interactive dialogs.
- `ui_console.py` — `ConsoleBackend`: non-interactive backend that renders tables via `rich` and auto-continues (returns `0`) without prompting.
- `ui_tkinter.py` — `TkinterBackend`: wraps the existing `TableUI` and `CompareUI` from `ui.py` with lazy tkinter import, fixing headless import failures.
- `ui_textual.py` — `TextualBackend`: full-terminal TUI dialogs using `textual` `DataTable` and `Button` widgets.
- `ui_factory.py` — `get_ui_backend(ui_mode)` factory supporting `"auto"`, `"console"`, `"tkinter"`, and `"textual"` modes. Auto-detection uses `DISPLAY`/`WAYLAND_DISPLAY` environment variables.
- `--ui` CLI option: select UI backend (`auto`, `console`, `tkinter`, `textual`).
- `ui_mode` parameter on `PgUpsert.__init__` to control backend selection programmatically.
- `textual>=0.47.0` added as an optional `tui` extra and a `dev` dependency.

### Changed

- `ControlTable`, `QARunner`, and `UpsertExecutor` now accept a `ui: UIBackend | None` parameter; all `TableUI`/`CompareUI` calls replaced with `self._ui.show_table()` / `self._ui.show_comparison()`.
- `PgUpsert` creates the UI backend once and shares it with all sub-components.
- Non-interactive runs are always routed through `ConsoleBackend` regardless of `ui_mode`.
- `ui_tkinter.py` and `ui_textual.py` added to coverage omit list (require event loop to test).

______________________________________________________________________

## [1.10.0] - 2026-04-02

### Added

- New `display.py` module — rich-based output formatting with `rich.table.Table`, `rich.panel.Panel`, and colored pass/fail indicators.
- `print_qa_summary()` — compact per-table QA results with pass/fail status and indented error details.
- `print_check_start()`, `print_check_table_pass()`, `print_check_table_fail()` — consistent check-level output formatting.
- `print_upsert_summary()` — clean upsert results table with per-table row counts and totals.

### Changed

- Replaced `tabulate` dependency with `rich` for all data table rendering. Tables now auto-size, have box borders, and support color.
- QA check output now uses colored status indicators (green ✓ / red ✗) instead of raw markdown tables.
- QA error summary uses a vertical per-table layout instead of one wide table — much easier to read.
- `rich` is now an explicit direct dependency (was previously transitive via `typer`).

### Removed

- Removed `tabulate` dependency.
- Removed `_tabulate_sql()` helper methods from `qa.py`, `control.py`, and `upsert.py`.

______________________________________________________________________

## [1.9.2] - 2026-04-02

### Added

- Test data for column existence and type mismatch failures in `schema_failing.sql` — staging `books` is missing the `notes` column, staging `publishers.publisher_name` is `integer` (vs `varchar` in base).

______________________________________________________________________

## [1.9.1] - 2026-04-02

### Fixed

- Fixed UNIQUE constraint check incorrectly flagging multiple NULL values as duplicates. PostgreSQL allows multiple NULLs in UNIQUE columns; the check now excludes rows with NULL values in constrained columns.

______________________________________________________________________

## [1.9.0] - 2026-04-02

### Added

- `--output=json` CLI flag — outputs `UpsertResult` as machine-parseable JSON to stdout. Log messages are suppressed when JSON output is enabled.
- `--check-schema` CLI flag — pre-flight validation mode that runs column existence and type mismatch checks only, then exits. Exit code 0 = compatible, 1 = issues found. Works with `--output=json`.

______________________________________________________________________

## [1.8.0] - 2026-04-02

### Added

- **UNIQUE constraint QA checks** — new `check_unique()` method detects duplicate values in UNIQUE-constrained columns (not just primary keys). Uses `pg_constraint contype='u'`.
- **Column existence validation** — new `check_column_existence()` method flags base table columns missing from the staging table. Respects `exclude_cols` setting.
- **Column type mismatch detection** — new `check_type_mismatch()` method detects hard type incompatibilities between staging and base columns using PostgreSQL's `pg_cast` catalog. Only flags types with no implicit or assignment cast.
- New control table columns: `unique_errors`, `column_errors`, `type_errors`.
- New facade methods on `PgUpsert`: `qa_all_unique()`, `qa_one_unique()`, `qa_column_existence()`, `qa_type_mismatch()`.
- UNIQUE constraint (`uq_authors_email`) added to test schema `authors.email` column.

### Changed

- QA check ordering: column existence and type mismatch checks now run before data checks (null, PK, unique, FK, CK) to catch schema issues early.

______________________________________________________________________

## [1.7.0] - 2026-04-02

### Added

- `UpsertResult` dataclass — `run()` now returns structured results with `qa_passed`, `total_updated`, `total_inserted`, `to_dict()`, and `to_json()` methods.
- `TableResult` dataclass — per-table stats and QA errors.
- `QAError` dataclass — individual QA finding with table name, check type, and details.
- `QACheckType` enum — types of QA checks (null, pk, unique, fk, ck, type_mismatch, column_existence).
- New modules: `control.py` (ControlTable), `qa.py` (QARunner), `executor.py` (UpsertExecutor), `models.py` (dataclasses).

### Changed

- **BREAKING**: `run()` now returns `UpsertResult` instead of `self`. This is a breaking change for callers that chained methods after `run()`, but `run()` is designed as a terminal call.
- Decomposed the 2,100-line `PgUpsert` class into focused modules: `QARunner` (QA checks), `UpsertExecutor` (upsert operations), `ControlTable` (temp table management). `PgUpsert` is now a thin facade preserving the existing constructor API.
- Replaced while-True/processed-flag database loops with Python iteration in QA checks.
- `qa_one_ck()` now updates the control table directly, consistent with `qa_one_null/pk/fk`.
- Config file no longer silently overrides explicit CLI arguments — CLI args take precedence.
- Password is no longer embedded in the connection URI; extracted and stored separately for reconnection.

### Fixed

- Fixed `rows_updated` count — now uses `cursor.rowcount` from the actual UPDATE execution instead of the staging match count.
- Fixed `_validate_schemas()` executing the same query twice.
- Fixed `open_db()` unreachable dead code branch and reconnection failure.

______________________________________________________________________

## [1.6.1] - 2026-04-02

______________________________________________________________________

## [1.6.0] - 2026-04-02

### Added

- `UserCancelledError` exception — exported from `pg_upsert` for callers that use the library as an import.
- `CHANGELOG.md` — Keep a Changelog format, seeded from git tag history.
- `justfile` — Task automation recipes (sync, lint, test, docs, bump).
- `CONTRIBUTING.md` — Development setup, available recipes, testing, and release process.
- `SECURITY.md` — Vulnerability reporting policy.
- `.python-version` — Pin to Python 3.13.
- `.github/PULL_REQUEST_TEMPLATE.md` — PR checklist template.
- Python 3.14 support added to CI matrix and classifiers.
- macOS and Windows unit-test jobs in CI (no database required).
- Codecov coverage upload in CI.
- Comprehensive test suite rewrite — 149 tests covering QA checks (null, PK, FK, check constraints), upsert operations, interactive mode, failing data scenarios, CLI argument parsing, config file handling, and PostgresDB connection management. Tests auto-skip when no database is available. Coverage at 91%.

### Changed

- Switched build system from setuptools to [Hatchling](https://hatch.pypa.io/).
- Migrated documentation from MkDocs to [Zensical](https://zensical.dev/).
- Updated `.readthedocs.yaml` for Zensical build pipeline.
- Updated pre-commit hooks to latest versions; added `uv-pre-commit` (lockfile management) and `mdformat` (markdown formatting).
- Updated CI/CD workflow: concurrency groups, `twine check`, updated action versions.
- Interactive cancellation now raises `UserCancelledError` instead of calling `sys.exit(0)`, allowing proper transaction rollback and library use as an import.
- Updated `.gitignore` with organized sections and Zensical/uv patterns.
- Ruff target version set to `py310` (minimum supported) instead of `py313`.
- Test coverage floor set to 80%.

### Fixed

- Fixed exclude columns whitespace bug — `split(",")` now strips whitespace so `"col1, col2"` no longer produces `" col2"` with a leading space.
- Fixed `qa_passed` not being reset between `run()` calls — a second `run()` on the same `PgUpsert` instance could skip QA failures from the first run.
- Fixed `rich.print` shadowing the Python builtin `print` in `cli.py` — renamed import to `rprint`.
- Fixed `qa_one_pk()` returning `None` instead of `self` when table has no primary key — broke method chaining.
- Fixed CLI ignoring `--encoding` argument — was hardcoded to `"utf-8"` instead of using the user-provided value.
- Removed unused imports (`__description__`, `__version__`) from `upsert.py`, `postgres.py`, and `ui.py`.
- Removed dead commented-out code in `upsert.py`.

______________________________________________________________________

## [1.5.3] - 2025-02-12

### Fixed

- Fixed release action permissions.

______________________________________________________________________

## [1.5.2] - 2025-02-12

### Fixed

- Fixed bumpversion file path configuration.
- Updated CI/CD to auto-generate release notes.

______________________________________________________________________

## [1.5.1] - 2025-01-15

### Changed

- Updated Docker build to use Python 3.13 alpine image.
- Updated Docker section in README.
- Updated pre-commit hooks, added typos hook.
- Updated host in `pg-upsert.example.yaml` for Docker-specific runs.

______________________________________________________________________

## [1.5.0] - 2025-01-15

### Changed

- Converted CLI from argparse to [Typer](https://typer.tiangolo.com/).
- Renamed `stg_schema` parameter to `staging_schema` for consistency.
- Synced CLI argument descriptions with help text.

### Added

- CLI tests.

______________________________________________________________________

## [1.4.6] - 2024-12-16

### Fixed

- Fixed badge URL.
- Set build and publish jobs to run only on tag push.

______________________________________________________________________

## [1.4.5] - 2024-12-16

### Changed

- Updated documentation.
- Renamed `PgUpsert._show()` to `PgUpsert._tabulate_sql()`.

### Added

- Added `PgUpsert.show_control()` method.

______________________________________________________________________

## [1.4.4] - 2024-12-15

### Changed

- Updated documentation and examples.
- Applied custom debug log formatting.
- Switched to uv-based build.

### Added

- More tests.

______________________________________________________________________

## [1.4.3] - 2024-11-13

### Fixed

- Fixed indentation issue.

______________________________________________________________________

## [1.4.2] - 2024-11-13

### Changed

- Auto-updated pre-commit hooks.
- Removed `__del__` method.
- Refined dependency version requirements.

______________________________________________________________________

## [1.4.1] - 2024-11-13

### Added

- Added PyYAML as a dependency.

### Fixed

- Removed unused imports.
- Fixed `requirements.txt`.

______________________________________________________________________

## [1.4.0] - 2024-11-12

### Added

- Database connection parameters via individual host/port/user/database options.
- YAML configuration file support.

______________________________________________________________________

## [1.3.1] - 2024-10-29

### Fixed

- Fixed tag push conditional in CI/CD.

______________________________________________________________________

## [1.3.0] - 2024-10-29

### Changed

- CLI reconfiguration.
- Stream FK and PK errors to console and log file.

______________________________________________________________________

## [1.2.8] - 2024-10-03

### Changed

- Reconfigured package logging.
- Renamed `_version.py` to `__version__.py`.

______________________________________________________________________

## [1.2.7] - 2024-07-29

### Changed

- Switched documentation from Sphinx to MkDocs.

______________________________________________________________________

## [1.2.6] - 2024-07-23

### Changed

- Version bump release.

______________________________________________________________________

## [1.2.5] - 2024-07-23

### Changed

- Updated project links.

______________________________________________________________________

## [1.2.4] - 2024-07-22

### Fixed

- Wrapped CLI entrypoint in try/except block.

______________________________________________________________________

## [1.2.3] - 2024-07-22

### Changed

- Updated README examples.
- Added docs status badge.

______________________________________________________________________

## [1.2.2] - 2024-07-20

### Fixed

- Fixed invalid routine call.

______________________________________________________________________

## [1.2.1] - 2024-07-19

### Changed

- Version bump release.

______________________________________________________________________

## [1.2.0] - 2024-07-19

### Changed

- Version bump release.

______________________________________________________________________

## [1.1.4] - 2024-07-17

### Changed

- Version bump release.

______________________________________________________________________

## [1.1.3] - 2024-07-17

### Changed

- Version bump release.

______________________________________________________________________

## [1.1.2] - 2024-07-17

### Changed

- Version bump release.

______________________________________________________________________

## [1.1.1] - 2024-07-17

### Changed

- Version bump release.

______________________________________________________________________

## [1.1.0] - 2024-07-17

### Changed

- Version bump release.

______________________________________________________________________

## [1.0.0] - 2024-07-17

### Added

- Initial stable release.
- PostgreSQL upsert functionality with staging/base table support.
- NOT NULL, PRIMARY KEY, FOREIGN KEY, and CHECK CONSTRAINT validation.
- Interactive confirmation mode.
- CLI interface.
- Python API.
- MkDocs documentation.

______________________________________________________________________

## [0.0.9] - 2024-06-10

### Changed

- Pre-release version.

______________________________________________________________________

## [0.0.8] - 2024-06-10

### Changed

- Pre-release version.

______________________________________________________________________

## [0.0.7] - 2024-06-10

### Added

- Initial pre-release.
