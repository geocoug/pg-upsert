"""Rich-based display utilities for pg-upsert output.

Every ``print_*`` function writes rich-formatted output to stderr (via
:data:`console`) **and** logs a plain-text equivalent so that the logfile
always stays in sync with what appears on screen.

All console output goes to stderr so that stdout remains clean for
``--output=json``.
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
    from ..models import QAError, UpsertResult

# Logger for file-only output. Display functions write rich output to the
# console (stderr) for the user, and plain-text to this logger for the
# logfile. The logger is a child of "pg_upsert" so it inherits file handlers,
# but propagation is disabled so messages don't also appear on the stream
# handler (which would cause duplicate console output).
_file_logger = logging.getLogger("pg_upsert.display")
_file_logger.propagate = False

# Module-level console writing to stderr so --output=json stays clean.
console = Console(stderr=True)


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
    """Render row dicts as a plain-text table string.

    This is the drop-in replacement for the old ``_tabulate_sql`` pattern.
    Used for logger output and non-rich contexts.

    Args:
        rows: List of row dicts.
        headers: Column headers.
        title: Optional table title.

    Returns:
        A string containing the rendered table (no ANSI codes).
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


def print_check_start(
    check_label: str,
    phase: int | None = None,
    total_phases: int | None = None,
) -> None:
    """Print a section header for a QA check category.

    Args:
        check_label: Human-readable check name (e.g. "Primary Key").
        phase: Current phase number (1-based), or ``None`` to omit.
        total_phases: Total number of phases, or ``None`` to omit.
    """
    counter = f" ({phase}/{total_phases})" if phase and total_phases else ""
    console.print()
    console.rule(f"[bold]{check_label} checks[/bold]{counter}", style="cyan")
    _file_logger.info(f"=== {check_label} checks{counter} ===")


def print_check_table_pass(
    schema: str,
    table: str,
    table_num: int | None = None,
    total_tables: int | None = None,
) -> None:
    """Print a passing status line for a single table check.

    Args:
        schema: The staging schema name.
        table: The table name.
        table_num: Current table number (1-based), or ``None`` to omit.
        total_tables: Total number of tables, or ``None`` to omit.
    """
    counter = f"  [dim][{table_num}/{total_tables}][/dim]" if table_num and total_tables else ""
    counter_log = f"  [{table_num}/{total_tables}]" if table_num and total_tables else ""
    console.print(f"  [bold green]✓[/bold green] {schema}.{table}{counter}")
    _file_logger.info(f"  ✓ {schema}.{table}{counter_log}")


def print_check_table_fail(
    schema: str,
    table: str,
    message: str,
    detail_rows: list[dict] | None = None,
    detail_headers: list[str] | None = None,
    table_num: int | None = None,
    total_tables: int | None = None,
) -> None:
    """Print a failing status line for a single table check.

    Args:
        schema: The staging schema name.
        table: The table name.
        message: Short error description.
        detail_rows: Optional list of row dicts for a detail table.
        detail_headers: Column headers for the detail table.
        table_num: Current table number (1-based), or ``None`` to omit.
        total_tables: Total number of tables, or ``None`` to omit.
    """
    counter = f"  [dim][{table_num}/{total_tables}][/dim]" if table_num and total_tables else ""
    counter_log = f"  [{table_num}/{total_tables}]" if table_num and total_tables else ""
    console.print(f"  [bold red]✗[/bold red] {schema}.{table} — {message}{counter}")
    _file_logger.warning(f"  ✗ {schema}.{table} — {message}{counter_log}")
    if detail_rows and detail_headers:
        detail_table = format_table(detail_rows, detail_headers)
        from rich.padding import Padding

        console.print(Padding(detail_table, (0, 0, 0, 6)))  # indent 6 spaces
        _file_logger.warning(format_sql_result(detail_rows, detail_headers))


# ---------------------------------------------------------------------------
# QA summary — detailed panel (default)
# ---------------------------------------------------------------------------


def print_qa_summary(
    tables: list[str],
    errors: list[QAError],
    compact: bool = False,
) -> None:
    """Print QA results summary.

    Args:
        tables: Ordered list of all table names checked.
        errors: All QA errors across all tables.
        compact: If ``True``, use the compact grid format (Option B) instead
            of the default per-table panel.
    """
    if compact:
        _print_qa_summary_compact(tables, errors)
    else:
        _print_qa_summary_panel(tables, errors)


def _print_qa_summary_panel(
    tables: list[str],
    errors: list[QAError],
) -> None:
    """Default summary: per-table panel with pass/fail + error details."""
    errors_by_table: dict[str, list[QAError]] = {}
    for err in errors:
        errors_by_table.setdefault(err.table, []).append(err)

    rich_lines: list[Text | str] = []
    log_lines: list[str] = []
    passed = 0
    failed = 0

    for table in tables:
        table_errors = errors_by_table.get(table, [])
        if not table_errors:
            passed += 1
            line = Text()
            line.append("  ✓ ", style="bold green")
            line.append(table)
            rich_lines.append(line)
            log_lines.append(f"  [PASS] {table}")
        else:
            failed += 1
            line = Text()
            line.append("  ✗ ", style="bold red")
            line.append(table, style="bold")
            rich_lines.append(line)
            log_lines.append(f"  [FAIL] {table}")
            for err in table_errors:
                detail = Text()
                detail.append(f"      {err.check_type.value}: ", style="dim")
                detail.append(err.details)
                rich_lines.append(detail)
                log_lines.append(f"    - {err.check_type.value}: {err.details}")

    # Footer
    rich_lines.append("")
    if failed == 0:
        footer = Text(f"  All {passed} tables passed QA checks", style="bold green")
        footer_log = f"Result: All {passed} tables passed QA checks"
    else:
        footer = Text(f"  {failed} of {passed + failed} tables failed QA checks", style="bold red")
        footer_log = f"Result: {failed} of {passed + failed} tables failed QA checks"
    rich_lines.append(footer)

    panel = Panel(
        "\n".join(str(line) for line in rich_lines),
        title="[bold]QA Results[/bold]",
        border_style="red" if failed > 0 else "green",
        expand=False,
        padding=(1, 2),
    )
    console.print()
    console.print(panel)

    # Logfile
    _file_logger.info("=== QA Results ===")
    for line in log_lines:
        _file_logger.info(line)
    _file_logger.info(footer_log)


def _print_qa_summary_compact(
    tables: list[str],
    errors: list[QAError],
) -> None:
    """Compact summary: grid with ✓/✗ per check type per table."""
    from ..models import QACheckType

    check_types = [
        ("Col", QACheckType.COLUMN_EXISTENCE),
        ("Type", QACheckType.TYPE_MISMATCH),
        ("Null", QACheckType.NULL),
        ("PK", QACheckType.PRIMARY_KEY),
        ("UQ", QACheckType.UNIQUE),
        ("FK", QACheckType.FOREIGN_KEY),
        ("CK", QACheckType.CHECK_CONSTRAINT),
    ]

    errors_by_table: dict[str, set[QACheckType]] = {}
    for err in errors:
        errors_by_table.setdefault(err.table, set()).add(err.check_type)

    t = Table(show_lines=False, pad_edge=True, expand=False, box=None)
    t.add_column("Table", style="bold")
    for label, _ct in check_types:
        t.add_column(label, justify="center", width=5)

    failed = 0
    for table in tables:
        failed_checks = errors_by_table.get(table, set())
        if failed_checks:
            failed += 1
        cells = []
        for _label, ct in check_types:
            if ct in failed_checks:
                cells.append("[bold red]✗[/bold red]")
            else:
                cells.append("[green]✓[/green]")
        t.add_row(table, *cells)

    console.print()
    console.print(t)
    passed = len(tables) - failed
    if failed == 0:
        console.print(f"\n  [bold green]All {passed} tables passed QA checks[/bold green]")
    else:
        console.print(f"\n  [bold red]{failed} of {len(tables)} tables failed QA checks[/bold red]")

    # Logfile — simple text grid
    header = f"  {'Table':<20}" + "".join(f"{label:>5}" for label, _ct in check_types)
    _file_logger.info("=== QA Results ===")
    _file_logger.info(header)
    _file_logger.info(f"  {'─' * 20}" + "─" * (5 * len(check_types)))
    for table in tables:
        failed_checks = errors_by_table.get(table, set())
        cells = "".join("    ✗" if ct in failed_checks else "    ✓" for _label, ct in check_types)
        _file_logger.info(f"  {table:<20}{cells}")
    if failed == 0:
        _file_logger.info(f"Result: All {passed} tables passed QA checks")
    else:
        _file_logger.info(f"Result: {failed} of {len(tables)} tables failed QA checks")


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
        _file_logger.info("Changes committed")
    else:
        console.print("  [dim]Changes rolled back[/dim]")
        _file_logger.info("Changes rolled back")

    # Logfile
    _file_logger.info("=== Upsert Summary ===")
    for tr in result.tables:
        _file_logger.info(f"  {tr.table_name}: {tr.rows_updated} updated, {tr.rows_inserted} inserted")
    _file_logger.info(f"  Total: {result.total_updated} updated, {result.total_inserted} inserted")
