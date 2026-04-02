# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

______________________________________________________________________

## [Unreleased]

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
