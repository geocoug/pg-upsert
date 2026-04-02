"""Tests for pg_upsert.ui.console.ConsoleBackend — no database required."""

from __future__ import annotations

import io

from rich.console import Console

from pg_upsert.ui import display
from pg_upsert.ui.console import ConsoleBackend


def _make_backend() -> ConsoleBackend:
    return ConsoleBackend()


def _silence_console() -> tuple[Console, io.StringIO]:
    """Return a (Console, buf) pair writing to a StringIO buffer."""
    buf = io.StringIO()
    con = Console(file=buf, no_color=True, width=120)
    return con, buf


class TestConsoleBackendShowTable:
    def test_returns_zero_none(self):
        backend = _make_backend()
        con, buf = _silence_console()
        original = display.console
        display.console = con
        try:
            result = backend.show_table(
                title="Test Table",
                message="Some message",
                buttons=[("Continue", 0, "<Return>"), ("Cancel", 1, "<Escape>")],
                headers=["col1", "col2"],
                rows=[["a", "b"], ["c", "d"]],
            )
        finally:
            display.console = original
        assert result == (0, None)

    def test_empty_rows(self):
        backend = _make_backend()
        con, buf = _silence_console()
        original = display.console
        display.console = con
        try:
            result = backend.show_table(
                title="Empty",
                message="No rows",
                buttons=[("Continue", 0, "<Return>")],
                headers=["x"],
                rows=[],
            )
        finally:
            display.console = original
        assert result == (0, None)

    def test_renders_to_console(self):
        backend = _make_backend()
        con, buf = _silence_console()
        original = display.console
        display.console = con
        try:
            backend.show_table(
                title="Render Test",
                message="rendering check",
                buttons=[("OK", 0, "<Return>")],
                headers=["name", "value"],
                rows=[["alpha", "1"], ["beta", "2"]],
            )
        finally:
            display.console = original
        output = buf.getvalue()
        # At minimum the table headers should appear in the output
        assert "name" in output
        assert "value" in output

    def test_buttons_ignored(self):
        """Buttons argument is accepted but not used — return is always (0, None)."""
        backend = _make_backend()
        con, _ = _silence_console()
        original = display.console
        display.console = con
        try:
            result = backend.show_table(
                title="T",
                message="m",
                buttons=[("Cancel", 99, "<Escape>")],
                headers=["c"],
                rows=[["v"]],
            )
        finally:
            display.console = original
        assert result[0] == 0


class TestConsoleBackendShowComparison:
    def test_returns_zero_none(self):
        backend = _make_backend()
        con, buf = _silence_console()
        original = display.console
        display.console = con
        try:
            result = backend.show_comparison(
                title="Compare",
                message="Comparing staging vs base",
                buttons=[("Continue", 0, "<Return>")],
                stg_headers=["id", "name"],
                stg_data=[["1", "Alice"]],
                base_headers=["id", "name"],
                base_data=[["1", "Alice-old"]],
                pk_cols=["id"],
            )
        finally:
            display.console = original
        assert result == (0, None)

    def test_empty_data(self):
        backend = _make_backend()
        con, _ = _silence_console()
        original = display.console
        display.console = con
        try:
            result = backend.show_comparison(
                title="Compare Empty",
                message="no rows",
                buttons=[("OK", 0, "<Return>")],
                stg_headers=["x"],
                stg_data=[],
                base_headers=["x"],
                base_data=[],
                pk_cols=["x"],
            )
        finally:
            display.console = original
        assert result == (0, None)

    def test_sidebyside_ignored(self):
        """sidebyside parameter is accepted but not used for console output."""
        backend = _make_backend()
        con, _ = _silence_console()
        original = display.console
        display.console = con
        try:
            result = backend.show_comparison(
                title="T",
                message="m",
                buttons=[],
                stg_headers=["a"],
                stg_data=[["v"]],
                base_headers=["a"],
                base_data=[["w"]],
                pk_cols=["a"],
                sidebyside=True,
            )
        finally:
            display.console = original
        assert result == (0, None)

    def test_renders_both_tables(self):
        backend = _make_backend()
        con, buf = _silence_console()
        original = display.console
        display.console = con
        try:
            backend.show_comparison(
                title="Side by side",
                message="Here come two tables",
                buttons=[("OK", 0, "<Return>")],
                stg_headers=["id", "val"],
                stg_data=[["10", "new"]],
                base_headers=["id", "val"],
                base_data=[["10", "old"]],
                pk_cols=["id"],
            )
        finally:
            display.console = original
        output = buf.getvalue()
        # Both "staging" and "base" table titles should appear
        assert "staging" in output.lower() or "new" in output
        assert "base" in output.lower() or "old" in output
