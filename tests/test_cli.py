import logging
import shlex
from pathlib import Path
from unittest import mock

import pytest
import typer
import yaml
from typer.testing import CliRunner

import pg_upsert
from pg_upsert import __title__, __version__
from pg_upsert.cli import app

runner = CliRunner()


def pg_upsert_cli(command_string):
    """Helper function to run the CLI with a command string."""
    command_list = shlex.split(command_string)
    result = runner.invoke(app, command_list)
    return result.stdout.rstrip()


def test_docs(monkeypatch):
    """Test that the --docs flag opens the documentation ."""
    expected_message_part = "Opening documentation"

    def mock_launch(*args, **kwargs):
        """Mock typer.launch() to return a message instead of opening the browser."""
        return expected_message_part

    monkeypatch.setattr(typer, "launch", mock_launch)
    assert expected_message_part in pg_upsert_cli("--docs")
    assert runner.invoke(app, ["--docs"]).exit_code == 0


@pytest.mark.parametrize(
    "command_string, expected_message_part",
    [
        ("", "Usage:"),  # No arguments should show the help message
        ("--help", "Usage:"),  # --help should show the help message
        (
            "--version",
            f"{__title__}: {__version__}",
        ),  # --version should show the version
        (
            "--generate-config",
            "Configuration file generated",
        ),  # --generate-config should generate a config file
    ],
)
def test_cli_messages_exit_0(command_string, expected_message_part):
    """Test the CLI messages for valid commands."""
    # Assert that the expected message part is in the stdout
    assert expected_message_part in pg_upsert_cli(command_string)
    # Assert that the cli exits with code 0
    assert runner.invoke(app, shlex.split(command_string)).exit_code == 0


@pytest.mark.parametrize(
    "command_string, expected_message_part",
    [
        (
            "-p 5432 -d dev -u docker -s staging -b public -t authors -t publishers -t books -t book_authors -t genres",
            "Database host is required",
        ),
        (
            "-h localhost -p 5432 -u docker -s staging -b public -t authors -t publishers -t books -t book_authors -t genres",  # noqa: E501
            "Database name is required",
        ),
        (
            "-h localhost -p 5432 -d dev -s staging -b public -t authors -t publishers -t books -t book_authors -t genres",  # noqa: E501
            "Database user is required",
        ),
        (
            "-h localhost -p 5432 -d dev -u docker -b public -t authors -t publishers -t books -t book_authors -t genres",  # noqa: E501
            "Staging schema is required",
        ),
        (
            "-h localhost -p 5432 -d dev -u docker -s staging -t authors -t publishers -t books -t book_authors -t genres",  # noqa: E501
            "Base schema is required",
        ),
        (
            "-h localhost -p 5432 -d dev -u docker -s staging -b public",
            "One or more table names are required",
        ),
        (
            "-f noexist.yaml",
            "Configuration file not found",
        ),
    ],
)
def test_cli_messages_exit_1(command_string, expected_message_part):
    """Test the CLI messages for invalid commands."""
    # Assert that the expected message part is in the stderr
    assert expected_message_part in pg_upsert_cli(command_string)
    # Assert that the cli exits with code 1
    assert runner.invoke(app, shlex.split(command_string)).exit_code == 1


@pytest.mark.parametrize(
    "config_key, expected_message_part",
    [
        ("host", "Database host is required"),
        ("database", "Database name is required"),
        ("user", "Database user is required"),
        ("staging_schema", "Staging schema is required"),
        ("base_schema", "Base schema is required"),
        ("tables", "One or more table names are required"),
    ],
)
def test_cli_config_file_messages_exit_1(tmp_path, config_key, expected_message_part):
    """Test the CLI messages for invalid config file."""
    # Read in the example config file and write it to a temporary file
    config_file = Path("pg-upsert.example.yaml")
    config_file_content = yaml.safe_load(config_file.resolve().read_text())
    del config_file_content[config_key]
    config_file_path = tmp_path / "pg-upsert.example.yaml"
    with open(config_file_path, "w") as file:
        yaml.dump(config_file_content, file)
    # Assert that the expected message part is in the stderr
    assert expected_message_part in pg_upsert_cli(f"--config-file {config_file_path}")
    # Assert that the cli exits with code 1
    assert runner.invoke(app, ["--config-file", config_file_path]).exit_code == 1


# @pytest.mark.parametrize(
#     "command_string",
#     [
#         "-h test_host -p 1234 -d test_db -u test_user -s test_stg_schema -b test_base_schema -t test_table",  # noqa: E501
#     ],
# )
# def test_config_file(capsys, tmp_path, monkeypatch, command_string):
#     """Test that the --config-file flag reads in a YAML config file and sets the options."""
#     config_file = Path("pg-upsert.example.yaml")
#     config_file_content = yaml.safe_load(config_file.resolve().read_text())
#     config_file_path = tmp_path / "pg-upsert.example.yaml"
#     with open(config_file_path, "w") as file:
#         yaml.dump(config_file_content, file)
#     with mock.patch("pg_upsert.PgUpsert") as mock_pg_upsert:
#         monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")
#         # runner.invoke(app, shlex.split(f"--config-file {config_file_path}"))
#         with mock.patch("sys.exit") as mock_exit:
#             try:
#                 typer.main.get_command(app)(["-f", config_file_path.as_posix()])
#             except SystemExit:
#                 pass
#         mock_pg_upsert.assert_called_once_with(
#             log_file="test_log.log",
#             host="test_host",
#             port=1234,
#             database="test_db",
#             user="test_user",
#             staging_schema="test_stg_schema",
#             base_schema="test_base_schema",
#             tables=["test_table"],
#         )


# def test_config_file_override():
#     """Test that the --config-file flag reads in a YAML config file and overrides the CLI options."""
#     pass


# def test_config_file_invalid_key(tmp_path, monkeypatch):
#     """Test that invalid keys in the config file are ignored."""
#     """Test that the --config-file flag reads in a YAML config file and sets the options."""
#     config_file = Path("pg-upsert.example.yaml")
#     config_file_content = yaml.safe_load(config_file.resolve().read_text())
#     # Add an invalid key to the config file
#     config_file_content["invalid_key"] = "invalid_value"
#     config_file_path = tmp_path / "pg-upsert.example.yaml"
#     with open(config_file_path, "w") as file:
#         yaml.dump(config_file_content, file)
#     monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")
#     # Assert that the invalid key is ignored
#     result = pg_upsert_cli(f"--config-file {config_file_path}")
#     assert (
#         f"Invalid configuration key will be ignored in {config_file_path}: invalid_key"
#         in result
#     )


# def test_logfile(tmp_path):
#     """Test that the --logfile flag creates a log file with the correct name."""
#     log_file_path = tmp_path / "test.log"
#     command_string = f"--logfile {log_file_path} -h localhost -p 5432 -d dev -u docker -s staging -b public -t authors"  # noqa: E501
#     runner.invoke(app, shlex.split(command_string))
#     assert log_file_path.exists()


@pytest.mark.parametrize(
    "command_string, expected_logging_level",
    [
        (
            "-h localhost -p 5432 -d dev -u docker -s staging -b public -t authors",
            logging.INFO,
        ),
        (
            "--debug -h localhost -p 5432 -d dev -u docker -s staging -b public -t authors",
            logging.DEBUG,
        ),
        (
            "--quiet -h localhost -p 5432 -d dev -u docker -s staging -b public -t authors",
            logging.INFO,
        ),
    ],
)
def test_log_levels(monkeypatch, command_string, expected_logging_level):
    """Test that the appropriate log level is set."""
    with mock.patch("pg_upsert.cli.logger.setLevel") as mock_set_level:
        monkeypatch.setattr(pg_upsert.postgres.getpass, "getpass", "password")
        runner.invoke(app, shlex.split(command_string))
        mock_set_level.assert_called_with(expected_logging_level)
