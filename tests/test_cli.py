import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest
import yaml

from pg_upsert.__version__ import __docs_url__, __version__
from pg_upsert.cli import clparser, main

logger = logging.getLogger(__name__)


@pytest.fixture
def mock_pgupsert():
    with patch("pg_upsert.cli.PgUpsert") as mock:
        yield mock


def test_clparser_debug():
    test_args = ["prog", "--debug"]
    with patch.object(sys, "argv", test_args):
        args = clparser()
        assert args.debug is True


def test_clparser_docs():
    test_args = ["prog", "--docs"]
    with patch.object(sys, "argv", test_args):
        args = clparser()
        assert args.docs is True


def test_clparser_logfile():
    test_args = ["prog", "--logfile", "test.log"]
    with patch.object(sys, "argv", test_args):
        args = clparser()
        assert args.logfile == Path("test.log")


def test_main_docs(mock_pgupsert):
    test_args = ["prog", "--docs"]
    with patch.object(sys, "argv", test_args):
        with patch("webbrowser.open") as mock_open:
            with pytest.raises(SystemExit):
                main()
            mock_open.assert_called_once()


def test_main_config_file(mock_pgupsert):
    test_args = ["prog", "--config-file", "test_config.yaml"]
    config_content = {
        "host": "localhost",
        "database": "test_db",
        "user": "test_user",
        "table": ["test_table"],
    }
    with patch.object(sys, "argv", test_args):
        with patch(
            "builtins.open",
            patch("io.StringIO", MagicMock(return_value=config_content)),
        ):
            with patch("yaml.safe_load", return_value=config_content):
                with pytest.raises(SystemExit):
                    main()
    # Test that the script exits if the configuration file is not found
    test_args = ["prog", "--config-file", "non_existent_config.yaml"]
    with patch.object(sys, "argv", test_args):
        with pytest.raises(SystemExit):
            main()


def test_main_missing_required_args(mock_pgupsert):
    test_args = ["prog"]
    with patch.object(sys, "argv", test_args):
        with pytest.raises(SystemExit):
            main()


def test_main_success(mock_pgupsert):
    test_args = [
        "prog",
        "--host",
        "localhost",
        "--database",
        "test_db",
        "--user",
        "test_user",
        "--table",
        "test_table",
    ]
    with patch.object(sys, "argv", test_args):
        main()
        mock_pgupsert.assert_called_once()


def test_clparser_help(capsys):
    """Test the --help argument."""
    with pytest.raises(SystemExit):
        with patch.object(sys, "argv", ["prog", "--help"]):
            clparser()
    captured = capsys.readouterr()
    assert "show this help message and exit" in captured.out


def test_clparser_version(capsys):
    """Test the --version argument."""
    with pytest.raises(SystemExit):
        with patch.object(sys, "argv", ["prog", "--version"]):
            clparser()
    captured = capsys.readouterr()
    assert "prog" in captured.out  # Replace with your program's actual name
    assert __version__ in captured.out


def test_docs_opening(monkeypatch):
    """Test the --docs argument opens the documentation."""
    mock_webbrowser = MagicMock()
    monkeypatch.setattr("webbrowser.open", mock_webbrowser)
    with pytest.raises(SystemExit):
        with patch.object(sys, "argv", ["prog", "--docs"]):
            main()
    mock_webbrowser.assert_called_once_with(__docs_url__)


def test_missing_required_args(capsys):
    """Test missing required arguments."""
    required_args = ["--host", "localhost", "--database", "testdb", "--user", "user"]
    for missing_arg in required_args:
        args = [arg for arg in required_args if arg != missing_arg]
        with pytest.raises(SystemExit):
            with patch.object(sys, "argv", ["prog", *args]):
                main()
        captured = capsys.readouterr()
        assert "usage" in captured.err
