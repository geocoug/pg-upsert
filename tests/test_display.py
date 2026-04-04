"""Tests for pg_upsert.ui.display — no database required.

Rich output is captured by redirecting console output to a StringIO buffer
so tests can assert on content without needing a real terminal.
"""

from __future__ import annotations

import io

import pytest
from rich.console import Console
from rich.table import Table

from pg_upsert.models import QACheckType, QAError
from pg_upsert.ui import display


def _capture(func, *args, **kwargs) -> str:
    """Call *func* with a temporary console writing to a string buffer.

    Temporarily replaces ``display.console`` so that all rich output is
    captured rather than written to stderr.
    """
    buf = io.StringIO()
    tmp = Console(file=buf, no_color=True, width=120)
    original = display.console
    display.console = tmp
    try:
        func(*args, **kwargs)
    finally:
        display.console = original
    return buf.getvalue()


# ---------------------------------------------------------------------------
# format_table
# ---------------------------------------------------------------------------


class TestFormatTable:
    def test_returns_rich_table(self):
        rows = [{"col1": "a", "col2": "b"}]
        t = display.format_table(rows)
        assert isinstance(t, Table)

    def test_empty_rows_returns_no_data_table(self):
        t = display.format_table([])
        assert isinstance(t, Table)
        # The single column label is "(no data)"
        assert t.columns[0].header == "(no data)"

    def test_headers_inferred_from_first_row(self):
        rows = [{"name": "Alice", "age": 30}]
        t = display.format_table(rows)
        headers = [col.header for col in t.columns]
        assert "name" in headers
        assert "age" in headers

    def test_explicit_headers(self):
        rows = [{"a": 1, "b": 2}]
        t = display.format_table(rows, headers=["a", "b"])
        headers = [col.header for col in t.columns]
        assert headers == ["a", "b"]

    def test_max_rows_truncation(self):
        rows = [{"x": str(i)} for i in range(10)]
        t = display.format_table(rows, max_rows=3)
        # 3 data rows + 1 "... and N more" row
        assert t.row_count == 4

    def test_no_truncation_within_limit(self):
        rows = [{"x": str(i)} for i in range(5)]
        t = display.format_table(rows, max_rows=10)
        assert t.row_count == 5

    def test_title_is_set(self):
        rows = [{"val": "v"}]
        t = display.format_table(rows, title="My Title")
        assert t.title == "My Title"

    def test_empty_rows_with_title(self):
        t = display.format_table([], title="Empty")
        assert t.title == "Empty"


# ---------------------------------------------------------------------------
# format_sql_result
# ---------------------------------------------------------------------------


class TestFormatSqlResult:
    def test_empty_rows_returns_no_results(self):
        result = display.format_sql_result([], headers=["col"])
        assert result == "(no results)"

    def test_returns_string(self):
        rows = [{"col": "value"}]
        result = display.format_sql_result(rows, headers=["col"])
        assert isinstance(result, str)

    def test_contains_header(self):
        rows = [{"mycolumn": "myvalue"}]
        result = display.format_sql_result(rows, headers=["mycolumn"])
        assert "mycolumn" in result

    def test_contains_value(self):
        rows = [{"letter": "Z"}]
        result = display.format_sql_result(rows, headers=["letter"])
        assert "Z" in result

    def test_multiple_rows(self):
        rows = [{"n": str(i)} for i in range(3)]
        result = display.format_sql_result(rows, headers=["n"])
        assert "0" in result
        assert "2" in result


# ---------------------------------------------------------------------------
# print_check_start
# ---------------------------------------------------------------------------


class TestPrintCheckStart:
    def test_does_not_raise(self):
        _capture(display.print_check_start, "Null")

    def test_outputs_label(self):
        out = _capture(display.print_check_start, "Primary Key")
        assert "Primary Key" in out


# ---------------------------------------------------------------------------
# print_check_table_pass
# ---------------------------------------------------------------------------


class TestPrintCheckTablePass:
    def test_does_not_raise(self):
        _capture(display.print_check_table_pass, "staging", "genres")

    def test_outputs_schema_and_table(self):
        out = _capture(display.print_check_table_pass, "staging", "books")
        assert "staging" in out
        assert "books" in out


# ---------------------------------------------------------------------------
# print_check_table_fail
# ---------------------------------------------------------------------------


class TestPrintCheckTableFail:
    def test_does_not_raise_no_details(self):
        _capture(display.print_check_table_fail, "staging", "genres", "null values found")

    def test_outputs_schema_table_message(self):
        out = _capture(display.print_check_table_fail, "staging", "genres", "null values")
        assert "staging" in out
        assert "genres" in out
        assert "null values" in out

    def test_with_detail_rows(self):
        detail_rows = [{"col": "genre", "count": "3"}]
        detail_headers = ["col", "count"]
        # Should not raise when detail rows are provided
        _capture(
            display.print_check_table_fail,
            "staging",
            "books",
            "null values found",
            detail_rows,
            detail_headers,
        )

    def test_with_empty_detail_rows(self):
        # Falsy detail_rows → detail table not rendered
        _capture(
            display.print_check_table_fail,
            "staging",
            "books",
            "error",
            [],
            ["col"],
        )


# ---------------------------------------------------------------------------
# print_qa_summary (panel mode)
# ---------------------------------------------------------------------------


class TestPrintQASummary:
    def test_all_passing_panel(self):
        tables = ["genres", "books"]
        errors: list[QAError] = []
        out = _capture(display.print_qa_summary, tables, errors)
        assert "genres" in out
        assert "books" in out

    def test_failing_table_panel(self):
        tables = ["genres", "books"]
        errors = [QAError(table="books", check_type=QACheckType.NULL, details="title (1)")]
        out = _capture(display.print_qa_summary, tables, errors)
        assert "books" in out

    def test_mixed_pass_fail_panel(self):
        tables = ["genres", "books", "authors"]
        errors = [
            QAError(table="books", check_type=QACheckType.NULL, details="title (1)"),
            QAError(table="authors", check_type=QACheckType.PRIMARY_KEY, details="author_id (2)"),
        ]
        out = _capture(display.print_qa_summary, tables, errors)
        assert "genres" in out
        assert "books" in out
        assert "authors" in out

    def test_compact_mode_dispatches(self):
        tables = ["genres"]
        errors: list[QAError] = []
        # compact=True should not raise
        _capture(display.print_qa_summary, tables, errors, compact=True)

    def test_empty_tables(self):
        # Degenerate case: no tables
        _capture(display.print_qa_summary, [], [])


# ---------------------------------------------------------------------------
# _print_qa_summary_compact (indirectly via print_qa_summary compact=True)
# ---------------------------------------------------------------------------


class TestPrintQASummaryCompact:
    def test_all_passing(self):
        tables = ["genres", "books"]
        errors: list[QAError] = []
        out = _capture(display.print_qa_summary, tables, errors, compact=True)
        assert "genres" in out

    def test_with_failing_table(self):
        tables = ["genres", "books"]
        errors = [
            QAError(table="genres", check_type=QACheckType.NULL, details="genre (1)"),
        ]
        out = _capture(display.print_qa_summary, tables, errors, compact=True)
        assert "genres" in out

    def test_all_check_types_shown(self):
        # Multiple error types for same table
        tables = ["books"]
        errors = [
            QAError(table="books", check_type=QACheckType.NULL, details="n (1)"),
            QAError(table="books", check_type=QACheckType.PRIMARY_KEY, details="pk (1)"),
            QAError(table="books", check_type=QACheckType.FOREIGN_KEY, details="fk (1)"),
        ]
        out = _capture(display.print_qa_summary, tables, errors, compact=True)
        assert "books" in out

    def test_single_table_passing(self):
        out = _capture(display.print_qa_summary, ["genres"], [], compact=True)
        assert "genres" in out


# ---------------------------------------------------------------------------
# Verify console is restored after capture helper
# ---------------------------------------------------------------------------


class TestCaptureHelper:
    def test_console_restored_after_call(self):
        original = display.console
        _capture(display.print_check_start, "Test")
        assert display.console is original

    def test_console_restored_after_exception(self):
        original = display.console

        def bad():
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            _capture(bad)
        assert display.console is original
