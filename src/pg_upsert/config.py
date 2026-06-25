#!/usr/bin/env python
"""Configuration loading shared by the CLI and :meth:`PgUpsert.from_config`.

A single ``pg-upsert`` YAML configuration file can be used both on the command
line (``--config-file``) and when constructing :class:`~pg_upsert.PgUpsert`
directly (:meth:`PgUpsert.from_config`).  This module is the single source of
truth for translating the file's (CLI-style) keys into the keyword arguments
the :class:`~pg_upsert.PgUpsert` constructor expects, so the two entry points
can never drift apart.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import quote

import yaml

# Keyword arguments accepted by ``PgUpsert.__init__``. Anything outside this set
# (plus the connection/alias keys handled below) is ignored so that a shared
# config file may carry CLI-only keys such as ``host`` or ``output``.
_VALID_PARAMS = frozenset(
    {
        "uri",
        "conn",
        "encoding",
        "tables",
        "staging_schema",
        "base_schema",
        "do_commit",
        "interactive",
        "upsert_method",
        "exclude_cols",
        "exclude_null_check_cols",
        "exclude_cols_by_table",
        "exclude_null_check_cols_by_table",
        "control_table",
        "ui_mode",
        "compact",
        "callback",
        "capture_detail_rows",
        "max_export_rows",
        "strict_columns",
    },
)

# CLI-style config keys mapped to their ``PgUpsert`` constructor counterparts.
_ALIASES = {
    "exclude_columns": "exclude_cols",
    "null_columns": "exclude_null_check_cols",
    "exclude_columns_by_table": "exclude_cols_by_table",
    "null_columns_by_table": "exclude_null_check_cols_by_table",
    "commit": "do_commit",
    "export_max_rows": "max_export_rows",
    "ui": "ui_mode",
}

# Config keys whose values are comma-separated strings on the CLI but lists in
# the constructor.
_LIST_KEYS = frozenset({"tables", "exclude_cols", "exclude_null_check_cols"})

# Config keys whose values are mappings of table name to a (comma-separated or
# list) set of columns. Each mapping value is normalised to a list.
_DICT_LIST_KEYS = frozenset({"exclude_cols_by_table", "exclude_null_check_cols_by_table"})

# Connection keys consumed to build a URI when one is not supplied directly.
_CONN_KEYS = frozenset({"host", "port", "database", "user"})


def load_config(path: str | Path) -> dict[str, Any]:
    """Read and parse a YAML configuration file.

    Args:
        path: Path to the configuration YAML file.

    Returns:
        The parsed configuration as a dictionary (empty if the file is blank).

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file is not valid YAML or does not contain a mapping.
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    try:
        with open(config_path) as file:
            config = yaml.safe_load(file)
    except (yaml.YAMLError, OSError) as e:
        raise ValueError(f"Error reading configuration file {config_path}: {e}") from e
    if config is None:
        return {}
    if not isinstance(config, dict):
        raise ValueError(
            f"Configuration file {config_path} must contain a mapping of keys to values, got {type(config).__name__}.",
        )
    return config


def _canonicalize(config: dict[str, Any]) -> dict[str, Any]:
    """Rename CLI-style alias keys to their canonical names (shallow).

    Ensures that, when several config sources are merged, the same setting
    spelled two ways (e.g. ``commit`` and ``do_commit``) collapses onto one
    key so later sources override earlier ones cleanly.
    """
    return {_ALIASES.get(key, key): value for key, value in config.items()}


def merge_configs(sources: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> dict[str, Any]:
    """Shallow-merge config mappings left-to-right; later sources win.

    Each top-level key is wholly replaced by the value from the last source
    that sets it (no deep/recursive merging of nested lists or dicts). Alias
    keys are canonicalised first so different spellings of the same setting
    override one another rather than coexisting.

    Args:
        sources: Ordered config mappings, lowest precedence first.

    Returns:
        A single merged mapping.
    """
    merged: dict[str, Any] = {}
    for source in sources:
        merged.update(_canonicalize(source))
    return merged


def load_sources(config: str | Path | dict | list | tuple) -> dict[str, Any]:
    """Resolve one or more config sources into a single merged mapping.

    Accepts a single source (a path or an already-parsed mapping) or an
    ordered ``list``/``tuple`` of them. Paths are read from disk; mappings are
    used as-is. Multiple sources are shallow-merged with later entries
    overriding earlier ones (see :func:`merge_configs`).

    Args:
        config: A path, a mapping, or a list/tuple of paths and/or mappings.

    Returns:
        The merged configuration mapping.
    """
    sources = list(config) if isinstance(config, (list, tuple)) else [config]
    dicts = [src if isinstance(src, dict) else load_config(src) for src in sources]
    return merge_configs(dicts)


def build_uri(*, host: str, database: str, user: str, port: int | str = 5432) -> str:
    """Build a PostgreSQL connection URI (without a password) from parts.

    The password is intentionally omitted; :class:`~pg_upsert.PostgresDB`
    prompts for it (or reads ``PGPASSWORD``) when the connection is opened.
    """
    return (
        f"postgresql://{quote(str(user), safe='')}@{quote(str(host), safe='')}:{port}/{quote(str(database), safe='')}"
    )


def _as_list(value: Any) -> Any:
    """Normalise a comma-separated string into a list of trimmed values."""
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return value


def config_to_kwargs(config: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    """Translate a config mapping into ``PgUpsert`` constructor keyword arguments.

    Accepts both CLI-style keys (e.g. ``exclude_columns``, ``commit``) and the
    constructor's native names (e.g. ``exclude_cols``, ``do_commit``).  Unknown
    keys are ignored so a single file can be shared between the CLI and the
    library.  Explicit ``overrides`` take precedence over the file's values.

    Args:
        config: Parsed configuration mapping.
        **overrides: Keyword arguments that win over the config file. May
            include non-serialisable values such as ``conn`` or ``callback``.

    Returns:
        A dictionary of keyword arguments suitable for ``PgUpsert(**kwargs)``.
    """
    merged: dict[str, Any] = {**config, **overrides}
    kwargs: dict[str, Any] = {}

    for key, value in merged.items():
        param = _ALIASES.get(key, key)
        if param not in _VALID_PARAMS:
            continue
        if param in _LIST_KEYS:
            kwargs[param] = _as_list(value)
        elif param in _DICT_LIST_KEYS and isinstance(value, dict):
            kwargs[param] = {table: _as_list(cols) for table, cols in value.items()}
        else:
            kwargs[param] = value

    # ``--export-failures`` implies row capture so failures can be exported.
    if merged.get("export_failures") and "capture_detail_rows" not in kwargs:
        kwargs["capture_detail_rows"] = True

    # Build a connection URI from parts only when one is not supplied directly.
    if not kwargs.get("uri") and not kwargs.get("conn"):
        conn_parts = {k: merged[k] for k in _CONN_KEYS if merged.get(k)}
        if {"host", "database", "user"} <= conn_parts.keys():
            kwargs["uri"] = build_uri(
                host=conn_parts["host"],
                database=conn_parts["database"],
                user=conn_parts["user"],
                port=conn_parts.get("port", 5432),
            )

    return kwargs


def is_recognized_key(key: str) -> bool:
    """Return ``True`` if *key* is a known config key (native, alias, or connection).

    Used by the CLI to decide whether a config-file key that has no matching
    command-line flag (e.g. ``exclude_columns_by_table``) should still be passed
    through to the constructor rather than warned about as unknown.
    """
    return key in _VALID_PARAMS or key in _ALIASES or key in _CONN_KEYS or key == "export_failures"
