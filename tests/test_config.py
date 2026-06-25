#!/usr/bin/env python
"""Tests for pg_upsert.config and PgUpsert.from_config — no database required."""

from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest

from pg_upsert import PgUpsert
from pg_upsert.config import (
    build_uri,
    config_to_kwargs,
    is_recognized_key,
    load_config,
)

EXAMPLE_YAML = Path(__file__).resolve().parents[1] / "pg-upsert.example.yaml"


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------


class TestLoadConfig:
    def test_reads_mapping(self, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_text("host: localhost\nport: 5432\n")
        assert load_config(path) == {"host": "localhost", "port": 5432}

    def test_accepts_str_path(self, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_text("user: docker\n")
        assert load_config(str(path)) == {"user": "docker"}

    def test_empty_file_returns_empty_dict(self, tmp_path):
        path = tmp_path / "empty.yaml"
        path.write_text("")
        assert load_config(path) == {}

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_config(tmp_path / "nope.yaml")

    def test_non_mapping_raises(self, tmp_path):
        path = tmp_path / "list.yaml"
        path.write_text("- a\n- b\n")
        with pytest.raises(ValueError, match="must contain a mapping"):
            load_config(path)

    def test_invalid_yaml_raises(self, tmp_path):
        path = tmp_path / "bad.yaml"
        path.write_text("a: [unterminated\n")
        with pytest.raises(ValueError, match="Error reading configuration file"):
            load_config(path)

    def test_example_yaml_parses(self):
        config = load_config(EXAMPLE_YAML)
        assert config["staging_schema"] == "staging"
        assert config["base_schema"] == "public"


# ---------------------------------------------------------------------------
# build_uri
# ---------------------------------------------------------------------------


class TestBuildUri:
    def test_basic(self):
        uri = build_uri(host="localhost", database="dev", user="docker")
        assert uri == "postgresql://docker@localhost:5432/dev"

    def test_custom_port(self):
        uri = build_uri(host="db", database="dev", user="u", port=6543)
        assert uri == "postgresql://u@db:6543/dev"

    def test_quotes_special_characters(self):
        uri = build_uri(host="localhost", database="my db", user="a@b")
        assert "a%40b" in uri
        assert "my%20db" in uri


# ---------------------------------------------------------------------------
# config_to_kwargs
# ---------------------------------------------------------------------------


class TestConfigToKwargs:
    def test_maps_cli_aliases_to_constructor_names(self):
        kwargs = config_to_kwargs(
            {
                "exclude_columns": ["rev_time"],
                "null_columns": ["alias"],
                "commit": True,
                "export_max_rows": 50,
            },
        )
        assert kwargs["exclude_cols"] == ["rev_time"]
        assert kwargs["exclude_null_check_cols"] == ["alias"]
        assert kwargs["do_commit"] is True
        assert kwargs["max_export_rows"] == 50

    def test_accepts_native_param_names(self):
        kwargs = config_to_kwargs({"exclude_cols": ["x"], "do_commit": True})
        assert kwargs["exclude_cols"] == ["x"]
        assert kwargs["do_commit"] is True

    def test_splits_comma_separated_strings(self):
        kwargs = config_to_kwargs({"exclude_columns": "a, b ,c", "tables": "t1,t2"})
        assert kwargs["exclude_cols"] == ["a", "b", "c"]
        assert kwargs["tables"] == ["t1", "t2"]

    def test_ignores_unknown_keys(self):
        kwargs = config_to_kwargs({"output": "json", "debug": True, "logfile": "x.log"})
        assert "output" not in kwargs
        assert "debug" not in kwargs
        assert "logfile" not in kwargs

    def test_builds_uri_from_connection_parts(self):
        kwargs = config_to_kwargs(
            {"host": "localhost", "database": "dev", "user": "docker", "port": 5432},
        )
        assert kwargs["uri"] == "postgresql://docker@localhost:5432/dev"

    def test_does_not_build_uri_when_parts_incomplete(self):
        kwargs = config_to_kwargs({"host": "localhost", "user": "docker"})
        assert "uri" not in kwargs

    def test_explicit_uri_preserved(self):
        kwargs = config_to_kwargs(
            {"host": "localhost", "database": "dev", "user": "docker", "uri": "postgresql://x@y/z"},
        )
        assert kwargs["uri"] == "postgresql://x@y/z"

    def test_conn_override_skips_uri_building(self):
        sentinel = object()
        kwargs = config_to_kwargs(
            {"host": "localhost", "database": "dev", "user": "docker"},
            conn=sentinel,
        )
        assert "uri" not in kwargs
        assert kwargs["conn"] is sentinel

    def test_export_failures_enables_capture(self):
        kwargs = config_to_kwargs({"export_failures": "out_dir"})
        assert kwargs["capture_detail_rows"] is True

    def test_export_failures_null_does_not_capture(self):
        kwargs = config_to_kwargs({"export_failures": None})
        assert "capture_detail_rows" not in kwargs

    def test_overrides_take_precedence(self):
        kwargs = config_to_kwargs({"commit": False, "do_commit": False}, do_commit=True)
        assert kwargs["do_commit"] is True

    def test_normalizes_by_table_alias_and_values(self):
        kwargs = config_to_kwargs(
            {"exclude_columns_by_table": {"books": "a, b", "authors": ["c"]}},
        )
        assert kwargs["exclude_cols_by_table"] == {"books": ["a", "b"], "authors": ["c"]}

    def test_null_columns_by_table_alias(self):
        kwargs = config_to_kwargs({"null_columns_by_table": {"books": ["x"]}})
        assert kwargs["exclude_null_check_cols_by_table"] == {"books": ["x"]}

    def test_accepts_native_by_table_name(self):
        kwargs = config_to_kwargs({"exclude_cols_by_table": {"books": ["x"]}})
        assert kwargs["exclude_cols_by_table"] == {"books": ["x"]}

    def test_example_yaml_produces_valid_kwargs(self):
        kwargs = config_to_kwargs(load_config(EXAMPLE_YAML))
        assert kwargs["staging_schema"] == "staging"
        assert kwargs["base_schema"] == "public"
        assert kwargs["exclude_cols"] == ["rev_time", "rev_user"]
        assert kwargs["uri"] == "postgresql://docker@localhost:5432/dev"
        assert kwargs["do_commit"] is False


# ---------------------------------------------------------------------------
# is_recognized_key
# ---------------------------------------------------------------------------


class TestIsRecognizedKey:
    @pytest.mark.parametrize(
        "key",
        [
            "exclude_cols",
            "exclude_cols_by_table",
            "exclude_columns",
            "exclude_columns_by_table",
            "null_columns_by_table",
            "host",
            "export_failures",
        ],
    )
    def test_recognized(self, key):
        assert is_recognized_key(key) is True

    @pytest.mark.parametrize("key", ["bogus", "output", "debug", ""])
    def test_unrecognized(self, key):
        assert is_recognized_key(key) is False


# ---------------------------------------------------------------------------
# PgUpsert.from_config
# ---------------------------------------------------------------------------


class TestFromConfig:
    def test_missing_file_raises_before_construction(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            PgUpsert.from_config(tmp_path / "missing.yaml")

    def test_routes_config_to_constructor(self):
        captured = {}

        def fake_init(self, **kwargs):
            captured.update(kwargs)

        with mock.patch.object(PgUpsert, "__init__", fake_init):
            PgUpsert.from_config(
                {
                    "host": "localhost",
                    "database": "dev",
                    "user": "docker",
                    "staging_schema": "staging",
                    "base_schema": "public",
                    "tables": ["books"],
                    "commit": True,
                },
            )
        assert captured["uri"] == "postgresql://docker@localhost:5432/dev"
        assert captured["staging_schema"] == "staging"
        assert captured["tables"] == ["books"]
        assert captured["do_commit"] is True

    def test_overrides_reach_constructor(self):
        captured = {}

        def fake_init(self, **kwargs):
            captured.update(kwargs)

        with mock.patch.object(PgUpsert, "__init__", fake_init):
            PgUpsert.from_config(str(EXAMPLE_YAML), do_commit=True)
        assert captured["do_commit"] is True
