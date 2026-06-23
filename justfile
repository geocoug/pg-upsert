# Install: brew install just  |  cargo install just  |  uv tool install rust-just
# Usage:   just <recipe>


[private]
default:
    @just --list --unsorted

# ── Dependencies ──────────────────────────────────────────────────────────────

# Sync dependencies from lockfile
[group('dev')]
sync:
    uv sync --all-extras

# Update pre-commit hooks
[group('dev')]
update-hooks:
    uv run pre-commit autoupdate


# ── Code Quality ──────────────────────────────────────────────────────────────

# Run linter + formatter
[group('dev')]
lint:
    uv run ruff check .
    uv run ruff format .

# Run pre-commit hooks on all files
[group('dev')]
pre-commit:
    uv run pre-commit run --all-files

# Run tests
[group('dev')]
test:
    uv run tox -e py

# Run tests across all supported Python versions
[group('dev')]
test-all:
    uv run tox run-parallel

# Clean up Python build artifacts and caches
[group('dev')]
clean:
    find . -type d -name "__pycache__" -exec rm -rf {} +
    find . -type d -name ".pytest_cache" -exec rm -rf {} +
    find . -type d -name "dist" -exec rm -rf {} +
    find . -type d -name "build" -exec rm -rf {} +
    find . -type d -name "*.egg-info" -exec rm -rf {} +
    find . -type d -name ".ruff_cache" -exec rm -rf {} +
    find . -type d -name "site" -exec rm -rf {} +
    find . -type f -name "*.pyc" -exec rm -f {} +
    find . -type f -name "*.pyo" -exec rm -f {} +
    find . -type f -name ".coverage" -exec rm -rf {} +


# ── Documentation──────────────────────────────────────────────────────────────

# Build documentation
[group('docs')]
docs:
    cp README.md docs/index.md
    cp CHANGELOG.md docs/about/change_log.md
    uv run zensical build

# Serve documentation locally
[group('docs')]
docs-serve:
    cp README.md docs/index.md
    cp CHANGELOG.md docs/about/change_log.md
    uv run zensical serve


# ── Versioning ────────────────────────────────────────────────────────────────

# List the current version
[group('versioning')]
bump:
    uv run bump-my-version show-bump

# Bump patch version (e.g. 1.2.3 → 1.2.4)
[group('versioning')]
bump-patch:
    uv run bump-my-version bump patch

# Bump minor version (e.g. 1.2.3 → 1.3.0)
[group('versioning')]
bump-minor:
    uv run bump-my-version bump minor

# Bump major version (e.g. 1.2.3 → 2.0.0)
[group('versioning')]
bump-major:
    uv run bump-my-version bump major
