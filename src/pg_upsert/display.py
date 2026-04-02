"""Rich-based display utilities for pg-upsert output.

Replaces ``tabulate`` with ``rich.table.Table`` for data display and
provides consistent formatting for QA check results, summaries, and
error reporting.

All output is directed to stderr via :data:`console` so that stdout
remains clean for ``--output=json``.
"""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

if TYPE_CHECKING:
    from .models import QAError, UpsertResult

logger = logging.getLogger(__name__)

# Module-level console writing to stderr so --output=json stays clean.
console = Console(stderr=True)

# Symbols for pass/fail status.
PASS = Text("✓", style="bold green")
FAIL = Text("✗", style="bold red")


# ---------------------------------------------------------------------------
# Data table formatting (replaces tabulate)
# ---------------------------------------------------------------------------


def format_table(
    rows: list[dict],
    headers: list[str] | None = None,
    title: str | None = None,
    max_rows: int = 50,
) -> Table:
    """Build a :class:`rich.table.Table` from row dictionaries.

    Args:
        rows: List of row dicts (keys become column headers if *headers* is None).
        headers: Explicit column headers. Defaults to the keys of the first row.
        title: Optional table title.
        max_rows: Maximum rows to display (remainder shown as "... and N more").

    Returns:
        A :class:`rich.table.Table` ready for printing.
    """
    if not rows:
        t = Table(title=title, show_lines=False, pad_edge=False)
        t.add_column("(no data)")
        return t

    if headers is None:
        headers = list(rows[0].keys())

    t = Table(title=title, show_lines=True, pad_edge=True, expand=False)
    for h in headers:
        t.add_column(h, overflow="fold")

    display_rows = rows[:max_rows]
    for row in display_rows:
        t.add_row(*(str(row.get(h, "")) for h in headers))

    if len(rows) > max_rows:
        t.add_row(*([f"... and {len(rows) - max_rows} more"] + [""] * (len(headers) - 1)))

    return t


def format_sql_result(
    rows: list[dict],
    headers: list[str],
    title: str | None = None,
) -> str:
    """Execute-and-format helper: render row dicts as a string.

    This is the drop-in replacement for the old ``_tabulate_sql`` pattern.

    Args:
        rows: List of row dicts.
        headers: Column headers.
        title: Optional table title.

    Returns:
        A string containing the rendered table.
    """
    if not rows:
        return "(no results)"
    table = format_table(rows, headers, title=title)
    buf = io.StringIO()
    temp_console = Console(file=buf, width=120, no_color=True)
    temp_console.print(table)
    return buf.getvalue().rstrip()


# ---------------------------------------------------------------------------
# Check-level result formatting
# ---------------------------------------------------------------------------


def print_check_start(check_label: str) -> None:
    """Print a section header for a QA check category.

    Args:
        check_label: Human-readable check name (e.g. "Primary Key").
    """
    console.print()
    console.rule(f"[bold]{check_label} checks[/bold]", style="cyan")


def print_check_table_pass(schema: str, table: str) -> None:
    """Print a passing status for a single table check.

    Args:
        schema: The staging schema name.
        table: The table name.
    """
    console.print(f"  {PASS} {schema}.{table}")


def print_check_table_fail(
    schema: str,
    table: str,
    message: str,
    detail_rows: list[dict] | None = None,
    detail_headers: list[str] | None = None,
) -> None:
    """Print a failing status for a single table check with optional detail table.

    Args:
        schema: The staging schema name.
        table: The table name.
        message: Short error description.
        detail_rows: Optional list of row dicts for a detail table.
        detail_headers: Column headers for the detail table.
    """
    console.print(f"  {FAIL} {schema}.{table} — {message}")
    if detail_rows and detail_headers:
        detail_table = format_table(detail_rows, detail_headers)
        console.print(detail_table)


# ---------------------------------------------------------------------------
# QA summary
# ---------------------------------------------------------------------------


def print_qa_summary(
    tables: list[str],
    errors: list[QAError],
) -> None:
    """Print a compact QA results summary.

    Shows each table with pass/fail status. Failed tables show their
    error details indented below. Ends with a count of passed/failed.

    Args:
        tables: Ordered list of all table names checked.
        errors: All QA errors across all tables.
    """
    errors_by_table: dict[str, list[QAError]] = {}
    for err in errors:
        errors_by_table.setdefault(err.table, []).append(err)

    lines: list[Text | str] = []
    passed = 0
    failed = 0

    for table in tables:
        table_errors = errors_by_table.get(table, [])
        if not table_errors:
            passed += 1
            line = Text()
            line.append("  ✓ ", style="bold green")
            line.append(table)
            lines.append(line)
        else:
            failed += 1
            line = Text()
            line.append("  ✗ ", style="bold red")
            line.append(table, style="bold")
            lines.append(line)
            for err in table_errors:
                detail = Text()
                detail.append(f"      {err.check_type.value}: ", style="dim")
                detail.append(err.details)
                lines.append(detail)

    # Footer
    lines.append("")
    if failed == 0:
        footer = Text(f"  All {passed} tables passed QA checks", style="bold green")
    else:
        footer = Text(f"  {failed} of {passed + failed} tables failed QA checks", style="bold red")
    lines.append(footer)

    panel = Panel(
        "\n".join(str(line) for line in lines),
        title="[bold]QA Results[/bold]",
        border_style="red" if failed > 0 else "green",
        expand=False,
        padding=(1, 2),
    )
    console.print()
    console.print(panel)


# ---------------------------------------------------------------------------
# Upsert / commit summary
# ---------------------------------------------------------------------------


def print_upsert_summary(result: UpsertResult) -> None:
    """Print the final upsert summary showing per-table row counts.

    Args:
        result: The UpsertResult from a completed run.
    """
    t = Table(title="Upsert Summary", show_lines=True, expand=False)
    t.add_column("Table", style="bold")
    t.add_column("Updated", justify="right")
    t.add_column("Inserted", justify="right")

    for tr in result.tables:
        updated = str(tr.rows_updated) if tr.rows_updated else "-"
        inserted = str(tr.rows_inserted) if tr.rows_inserted else "-"
        t.add_row(tr.table_name, updated, inserted)

    # Totals row
    t.add_section()
    t.add_row(
        "[bold]Total[/bold]",
        f"[bold]{result.total_updated}[/bold]",
        f"[bold]{result.total_inserted}[/bold]",
    )

    console.print()
    console.print(t)

    if result.committed:
        console.print("  [bold green]Changes committed[/bold green]")
    else:
        console.print("  [dim]Changes rolled back[/dim]")
