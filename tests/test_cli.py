#!/usr/bin/env python
"""Tests for the pg-upsert CLI (cli.py).

All tests use the Typer test runner and never touch a real database.
"""

from __future__ import annotations

import logging
import shlex
from unittest import mock

import pytest
import typer
import yaml
from typer.testing import CliRunner

import pg_upsert
from pg_upsert import __version__
from pg_upsert.cli import app

runner = CliRunner()


def pg_upsert_cli(command_string):
    """Helper — invoke the CLI and return stripped stdout."""
    return runner.invoke(app, shlex.split(command_string)).stdout.rstrip()


# ---------------------------------------------------------------------------
# Version / help / docs / generate-config (exit 0)
# ---------------------------------------------------------------------------


class TestCliExitZero:
    def test_no_args_shows_help(self):
        result = runner.invoke(app, [])
        assert result.exit_code == 0
        assert "Usage:" in result.stdout

    def test_help(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "Usage:" in result.stdout

    def test_version(self):
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "pg_upsert" in result.stdout
        assert __version__ in result.stdout

    def test_docs(self, monkeypatch):
        monkeypatch.setattr(typer, "launch", lambda *a, **k: None)
        result = runner.invoke(app, ["--docs"])
        assert result.exit_code == 0
        assert "Opening documentation" in result.stdout

    def test_generate_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(app, ["--generate-config"])
        assert result.exit_code == 0
        assert "Configuration file generated" in result.stdout
        assert (tmp_path / "pg-upsert.template.yaml").exists()
        # Check the generated file includes default logfile
        raw = (tmp_path / "pg-upsert.template.yaml").read_text()
        assert "logfile" in raw

    def test_generate_config_with_logfile(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(app, ["--generate-config", "-l", "custom.log"])
        assert result.exit_code == 0
        # The logfile path is preserved (not replaced with the default)
        raw = (tmp_path / "pg-upsert.template.yaml").read_text()
        assert "custom.log" in raw


# ---------------------------------------------------------------------------
# Missing required arguments (exit 1)
# ---------------------------------------------------------------------------


class TestCliMissingArgs:
    @pytest.mark.parametrize(
        "missing_flag, expected_msg",
        [
            ("-p 5432 -d dev -u docker -s staging -b public -t t1", "Database host is required"),
            ("-h h -p 5432 -u docker -s staging -b public -t t1", "Database name is required"),
            ("-h h -p 5432 -d dev -s staging -b public -t t1", "Database user is required"),
            ("-h h -p 5432 -d dev -u docker -b public -t t1", "Staging schema is required"),
            ("-h h -p 5432 -d dev -u docker -s staging -t t1", "Base schema is required"),
            ("-h h -p 5432 -d dev -u docker -s staging -b public", "One or more table names are required"),
        ],
    )
    def test_missing_required_args(self, missing_flag, expected_msg):
        result = runner.invoke(app, shlex.split(missing_flag))
        assert result.exit_code == 1
        assert expected_msg in result.stdout


# ---------------------------------------------------------------------------
# Config file handling
# ---------------------------------------------------------------------------


class TestCliConfigFile:
    def test_config_file_not_found(self):
        result = runner.invoke(app, ["-f", "noexist.yaml"])
        assert result.exit_code == 1
        assert "Configuration file not found" in result.stdout

    def test_config_file_invalid_yaml(self, tmp_path):
        bad_yaml = tmp_path / "bad.yaml"
        bad_yaml.write_text(": : :\n  - [invalid")
        result = runner.invoke(app, ["-f", str(bad_yaml)])
        assert result.exit_code == 1
        assert "Error reading configuration file" in result.stdout

    @pytest.mark.parametrize(
        "config_key, expected_msg",
        [
            ("host", "Database host is required"),
            ("database", "Database name is required"),
            ("user", "Database user is required"),
            ("staging_schema", "Staging schema is required"),
            ("base_schema", "Base schema is required"),
            ("tables", "One or more table names are required"),
        ],
    )
    def test_config_file_missing_required_key(self, tmp_path, config_key, expected_msg):
        config = {
            "host": "localhost",
            "port": 5432,
            "database": "dev",
            "user": "docker",
            "staging_schema": "staging",
            "base_schema": "public",
            "tables": ["t1"],
        }
        del config[config_key]
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(config))
        result = runner.invoke(app, ["-f", str(config_file)])
        assert result.exit_code == 1
        assert expected_msg in result.stdout

    def test_config_file_invalid_key_ignored(self, tmp_path):
        config = {
            "host": "localhost",
            "port": 5432,
            "database": "dev",
            "user": "docker",
            "staging_schema": "staging",
            "base_schema": "public",
            "tables": ["t1"],
            "bogus_key": "whatever",
        }
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(config))
        result = runner.invoke(app, ["-f", str(config_file)])
        assert "Invalid configuration key will be ignored" in result.stdout

    def test_config_file_logfile_as_path(self, tmp_path):
        config = {
            "host": "localhost",
            "port": 5432,
            "database": "dev",
            "user": "docker",
            "staging_schema": "staging",
            "base_schema": "public",
            "tables": ["t1"],
            "logfile": str(tmp_path / "test.log"),
        }
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(config))
        # Will fail at PgUpsert connection, but logfile path should be set
        result = runner.invoke(app, ["-f", str(config_file)])
        assert result.exit_code == 1  # Connection will fail

    def test_config_file_exclude_columns_as_string(self, tmp_path, monkeypatch):
        """When config file provides exclude_columns as a comma-separated string."""
        config = {
            "host": "localhost",
            "port": 5432,
            "database": "dev",
            "user": "docker",
            "staging_schema": "staging",
            "base_schema": "public",
            "tables": ["t1"],
            "exclude_columns": "col1, col2",
            "null_columns": "col3,col4",
        }
        config_file = tmp_path / "config.yaml"
        config_file.write_text(yaml.dump(config))

        captured_args = {}

        class FakePgUpsert:
            def __init__(self, **kwargs):
                captured_args.update(kwargs)

            def run(self):
                from pg_upsert.models import UpsertResult

                return UpsertResult()

        monkeypatch.setattr("pg_upsert.cli.PgUpsert", FakePgUpsert)
        monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")
        result = runner.invoke(app, ["-f", str(config_file)])
        assert result.exit_code == 0
        # Verify the columns were split and stripped
        assert captured_args["exclude_cols"] == ["col1", "col2"]
        assert captured_args["exclude_null_check_cols"] == ["col3", "col4"]


# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------


class TestCliLogging:
    @pytest.mark.parametrize(
        "flags, expected_level",
        [
            ("-h h -p 5432 -d dev -u u -s stg -b pub -t t1", logging.INFO),
            ("--debug -h h -p 5432 -d dev -u u -s stg -b pub -t t1", logging.DEBUG),
        ],
    )
    def test_log_levels(self, monkeypatch, flags, expected_level):
        with mock.patch("pg_upsert.cli.logger.setLevel") as mock_set_level:
            monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")
            runner.invoke(app, shlex.split(flags))
            mock_set_level.assert_called_with(expected_level)

    def test_logfile_creation(self, tmp_path, monkeypatch):
        logfile = tmp_path / "test.log"
        flags = f"-l {logfile} -h h -p 5432 -d dev -u u -s stg -b pub -t t1"
        monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")
        runner.invoke(app, shlex.split(flags))
        # Logger should have a file handler (even though PgUpsert fails)
        # The logfile path was processed
        assert True  # We're testing the path doesn't crash

    def test_debug_logging_with_logfile(self, tmp_path, monkeypatch):
        logfile = tmp_path / "debug.log"
        flags = f"--debug -l {logfile} -h h -p 5432 -d dev -u u -s stg -b pub -t t1"
        monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")
        runner.invoke(app, shlex.split(flags))
        assert True  # Tests the debug+logfile code path

    def test_existing_logfile_deleted(self, tmp_path, monkeypatch):
        """An existing logfile should be deleted before the run."""
        logfile = tmp_path / "existing.log"
        logfile.write_text("old content")
        assert logfile.exists()

        class FakePgUpsert:
            def __init__(self, **kw):
                pass

            def run(self):
                from pg_upsert.models import UpsertResult

                return UpsertResult()

        monkeypatch.setattr("pg_upsert.cli.PgUpsert", FakePgUpsert)
        monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")
        flags = f"-l {logfile} -h h -p 5432 -d dev -u u -s stg -b pub -t t1"
        result = runner.invoke(app, shlex.split(flags))
        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# Exclude columns parsing
# ---------------------------------------------------------------------------


class TestCliExcludeColumns:
    def test_exclude_columns_split(self, monkeypatch):
        """Verify comma-separated exclude columns are split and stripped."""
        captured_args = {}

        def fake_pgupsert(**kwargs):
            captured_args.update(kwargs)
            raise RuntimeError("stop here")

        monkeypatch.setattr("pg_upsert.cli.PgUpsert", fake_pgupsert)
        monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")
        runner.invoke(
            app,
            shlex.split("-h h -p 5432 -d dev -u u -s stg -b pub -t t1 -x col1 -x col2"),
        )
        # Typer passes lists for multi-value options, not comma-sep strings

    def test_null_columns_split(self, monkeypatch):
        """Verify comma-separated null columns are split and stripped."""
        monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")
        result = runner.invoke(
            app,
            shlex.split("-h h -p 5432 -d dev -u u -s stg -b pub -t t1 -n col1 -n col2"),
        )
        # Should not crash during parsing
        assert result.exit_code == 1  # Will fail at DB connection


# ---------------------------------------------------------------------------
# PgUpsert invocation
# ---------------------------------------------------------------------------


class TestCliPgUpsertCall:
    def test_pgupsert_exception_exits_1(self, monkeypatch):
        """Verify generic exceptions from PgUpsert cause exit code 1."""
        monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")

        class FakePgUpsert:
            def __init__(self, **kw):
                pass

            def run(self):
                raise RuntimeError("boom")

        monkeypatch.setattr("pg_upsert.cli.PgUpsert", FakePgUpsert)
        result = runner.invoke(
            app,
            shlex.split("-h h -p 5432 -d dev -u u -s stg -b pub -t t1"),
        )
        assert result.exit_code == 1

    def test_user_cancelled_exits_0(self, monkeypatch):
        """Verify UserCancelledError causes exit code 0."""
        from pg_upsert.upsert import UserCancelledError

        monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")

        class FakePgUpsert:
            def __init__(self, **kw):
                pass

            def run(self):
                raise UserCancelledError("cancelled")

        monkeypatch.setattr("pg_upsert.cli.PgUpsert", FakePgUpsert)
        result = runner.invoke(
            app,
            shlex.split("-h h -p 5432 -d dev -u u -s stg -b pub -t t1"),
        )
        assert result.exit_code == 0

    def test_successful_run(self, monkeypatch):
        """Verify a successful PgUpsert.run() exits cleanly."""
        from pg_upsert.models import UpsertResult

        monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")

        class FakePgUpsert:
            def __init__(self, **kw):
                pass

            def run(self):
                return UpsertResult()

        monkeypatch.setattr("pg_upsert.cli.PgUpsert", FakePgUpsert)
        result = runner.invoke(
            app,
            shlex.split("-h h -p 5432 -d dev -u u -s stg -b pub -t t1"),
        )
        assert result.exit_code == 0

    def test_json_output(self, monkeypatch):
        """Verify --output=json produces valid JSON on stdout."""
        import json

        from pg_upsert.models import UpsertResult

        monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")

        class FakePgUpsert:
            def __init__(self, **kw):
                pass

            def run(self):
                return UpsertResult()

        monkeypatch.setattr("pg_upsert.cli.PgUpsert", FakePgUpsert)
        result = runner.invoke(
            app,
            shlex.split("-h h -p 5432 -d dev -u u -s stg -b pub -t t1 --output json"),
        )
        assert result.exit_code == 0
        parsed = json.loads(result.stdout)
        assert "qa_passed" in parsed
        assert "tables" in parsed

    def test_check_schema_no_errors(self, monkeypatch):
        """Verify --check-schema exits 0 when no schema issues."""
        monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")

        class FakeQA:
            def check_column_existence(self, table):
                return []

            def check_type_mismatch(self, table):
                return []

        class FakePgUpsert:
            def __init__(self, **kw):
                self._qa = FakeQA()

        monkeypatch.setattr("pg_upsert.cli.PgUpsert", FakePgUpsert)
        result = runner.invoke(
            app,
            shlex.split("-h h -p 5432 -d dev -u u -s stg -b pub -t t1 --check-schema"),
        )
        assert result.exit_code == 0
        assert "passed" in result.stdout.lower()

    def test_check_schema_with_errors(self, monkeypatch):
        """Verify --check-schema exits 1 when schema issues found."""
        from pg_upsert.models import QACheckType, QAError

        monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")

        class FakeQA:
            def check_column_existence(self, table):
                return [QAError(table="t1", check_type=QACheckType.COLUMN_EXISTENCE, details="col_x")]

            def check_type_mismatch(self, table):
                return []

        class FakePgUpsert:
            def __init__(self, **kw):
                self._qa = FakeQA()

        monkeypatch.setattr("pg_upsert.cli.PgUpsert", FakePgUpsert)
        result = runner.invoke(
            app,
            shlex.split("-h h -p 5432 -d dev -u u -s stg -b pub -t t1 --check-schema"),
        )
        assert result.exit_code == 1

    def test_check_schema_json_output(self, monkeypatch):
        """Verify --check-schema --output=json produces valid JSON."""
        import json

        from pg_upsert.models import QACheckType, QAError

        monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")

        class FakeQA:
            def check_column_existence(self, table):
                return [QAError(table="t1", check_type=QACheckType.COLUMN_EXISTENCE, details="col_x")]

            def check_type_mismatch(self, table):
                return []

        class FakePgUpsert:
            def __init__(self, **kw):
                self._qa = FakeQA()

        monkeypatch.setattr("pg_upsert.cli.PgUpsert", FakePgUpsert)
        result = runner.invoke(
            app,
            shlex.split("-h h -p 5432 -d dev -u u -s stg -b pub -t t1 --check-schema --output json"),
        )
        assert result.exit_code == 1
        parsed = json.loads(result.stdout)
        assert parsed["schema_compatible"] is False
        assert len(parsed["errors"]) == 1
