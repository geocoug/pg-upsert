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

from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

if TYPE_CHECKING:
    from ..models import CheckContext, QAError

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
    ctx: CheckContext | None = None,
) -> None:
    """Print a passing status line for a single table check.

    Args:
        schema: The staging schema name.
        table: The table name.
        ctx: Optional progress context with table counter.
    """
    counter = f"[dim][{ctx.table_num}/{ctx.total_tables}][/dim] " if ctx else ""
    counter_log = f"[{ctx.table_num}/{ctx.total_tables}] " if ctx else ""
    console.print(f"  [bold green]✓[/bold green] {counter}{schema}.{table}")
    _file_logger.info(f"  ✓ {counter_log}{schema}.{table}")


def print_check_table_warn(
    schema: str,
    table: str,
    message: str,
    ctx: CheckContext | None = None,
) -> None:
    """Print a warning status line for a single table check.

    Args:
        schema: The staging schema name.
        table: The table name.
        message: Short warning description.
        ctx: Optional progress context with table counter.
    """
    counter = f"[dim][{ctx.table_num}/{ctx.total_tables}][/dim] " if ctx else ""
    counter_log = f"[{ctx.table_num}/{ctx.total_tables}] " if ctx else ""
    console.print(f"  [bold yellow]⚠[/bold yellow] {counter}{schema}.{table} — {message}")
    _file_logger.warning(f"  ⚠ {counter_log}{schema}.{table} — {message}")


def print_check_table_fail(
    schema: str,
    table: str,
    message: str,
    detail_rows: list[dict] | None = None,
    detail_headers: list[str] | None = None,
    ctx: CheckContext | None = None,
) -> None:
    """Print a failing status line for a single table check.

    Args:
        schema: The staging schema name.
        table: The table name.
        message: Short error description.
        detail_rows: Optional list of row dicts for a detail table.
        detail_headers: Column headers for the detail table.
        ctx: Optional progress context with table counter.
    """
    counter = f"[dim][{ctx.table_num}/{ctx.total_tables}][/dim] " if ctx else ""
    counter_log = f"[{ctx.table_num}/{ctx.total_tables}] " if ctx else ""
    console.print(f"  [bold red]✗[/bold red] {counter}{schema}.{table} — {message}")
    _file_logger.warning(f"  ✗ {counter_log}{schema}.{table} — {message}")
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
    """Default summary: per-table panel with pass/fail/warn + error details."""
    from ..models import QASeverity

    errors_by_table: dict[str, list[QAError]] = {}
    for err in errors:
        errors_by_table.setdefault(err.table, []).append(err)

    rich_lines: list[Text | str] = []
    log_lines: list[str] = []
    passed = 0
    warned = 0
    failed = 0

    for table in tables:
        table_errors = errors_by_table.get(table, [])
        has_errors = any(e.severity == QASeverity.ERROR for e in table_errors)
        has_warnings = any(e.severity == QASeverity.WARNING for e in table_errors)
        if has_errors:
            failed += 1
            line = Text()
            line.append("  ✗ ", style="bold red")
            line.append(table, style="bold")
            rich_lines.append(line)
            log_lines.append(f"  [FAIL] {table}")
        elif has_warnings:
            warned += 1
            line = Text()
            line.append("  ⚠ ", style="bold yellow")
            line.append(table)
            rich_lines.append(line)
            log_lines.append(f"  [WARN] {table}")
        else:
            passed += 1
            line = Text()
            line.append("  ✓ ", style="bold green")
            line.append(table)
            rich_lines.append(line)
            log_lines.append(f"  [PASS] {table}")
        # Separate real findings from skip-warnings (checks that crashed
        # on missing columns and were caught by the savepoint).
        real_errors = [e for e in table_errors if not e.details.startswith("check skipped:")]
        skip_errors = [e for e in table_errors if e.details.startswith("check skipped:")]
        for err in real_errors:
            tag = err.check_type.value
            detail = Text()
            detail.append(f"      {tag}: ", style="dim")
            detail.append(err.details)
            rich_lines.append(detail)
            log_lines.append(f"    - {tag}: {err.details}")
        if skip_errors:
            n = len(skip_errors)
            skip_msg = f"{n} check(s) skipped due to missing columns"
            detail = Text()
            detail.append("      skipped: ", style="dim")
            detail.append(skip_msg)
            rich_lines.append(detail)
            log_lines.append(f"    - skipped: {skip_msg}")

    # Footer
    rich_lines.append("")
    total = passed + warned + failed
    if failed == 0 and warned == 0:
        footer = Text(f"  All {passed} tables passed QA checks", style="bold green")
        footer_log = f"Result: All {passed} tables passed QA checks"
    elif failed == 0:
        footer = Text(
            f"  {passed} of {total} tables passed, {warned} warned",
            style="bold yellow",
        )
        footer_log = f"Result: {passed} of {total} tables passed, {warned} warned"
    else:
        parts = [f"{failed} of {total} tables failed QA checks"]
        if warned > 0:
            parts.append(f"{warned} warned")
        footer = Text(f"  {', '.join(parts)}", style="bold red")
        footer_log = f"Result: {', '.join(parts)}"
    rich_lines.append(footer)

    if failed > 0:
        border = "red"
    elif warned > 0:
        border = "yellow"
    else:
        border = "green"
    panel = Panel(
        Group(*rich_lines),
        title="[bold]QA Results[/bold]",
        border_style=border,
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
    """Compact summary: grid with ✓/⚠/✗ per check type per table."""
    from ..models import QACheckType, QASeverity

    check_types = [
        ("Col", QACheckType.COLUMN_EXISTENCE),
        ("Type", QACheckType.TYPE_MISMATCH),
        ("Null", QACheckType.NULL),
        ("PK", QACheckType.PRIMARY_KEY),
        ("UQ", QACheckType.UNIQUE),
        ("FK", QACheckType.FOREIGN_KEY),
        ("CK", QACheckType.CHECK_CONSTRAINT),
    ]

    # Build per-table, per-check-type severity map.
    severity_map: dict[str, dict[QACheckType, QASeverity]] = {}
    for err in errors:
        current = severity_map.setdefault(err.table, {}).get(err.check_type)
        # ERROR trumps WARNING.
        if current is None or err.severity == QASeverity.ERROR:
            severity_map.setdefault(err.table, {})[err.check_type] = err.severity

    t = Table(show_lines=False, pad_edge=True, expand=False, box=None)
    t.add_column("Table", style="bold")
    for label, _ct in check_types:
        t.add_column(label, justify="center", width=5)

    failed = 0
    warned = 0
    for table in tables:
        table_sevs = severity_map.get(table, {})
        has_error = QASeverity.ERROR in table_sevs.values()
        has_warn = QASeverity.WARNING in table_sevs.values()
        if has_error:
            failed += 1
        elif has_warn:
            warned += 1
        cells = []
        for _label, ct in check_types:
            sev = table_sevs.get(ct)
            if sev == QASeverity.ERROR:
                cells.append("[bold red]✗[/bold red]")
            elif sev == QASeverity.WARNING:
                cells.append("[bold yellow]⚠[/bold yellow]")
            else:
                cells.append("[green]✓[/green]")
        t.add_row(table, *cells)

    console.print()
    console.print(t)
    passed = len(tables) - failed - warned
    total = len(tables)
    if failed == 0 and warned == 0:
        console.print(f"\n  [bold green]All {passed} tables passed QA checks[/bold green]")
    elif failed == 0:
        console.print(
            f"\n  [bold yellow]{passed} of {total} tables passed, {warned} warned[/bold yellow]",
        )
    else:
        parts = [f"{failed} of {total} tables failed QA checks"]
        if warned > 0:
            parts.append(f"{warned} warned")
        console.print(f"\n  [bold red]{', '.join(parts)}[/bold red]")

    # Logfile — simple text grid
    header = f"  {'Table':<20}" + "".join(f"{label:>5}" for label, _ct in check_types)
    _file_logger.info("=== QA Results ===")
    _file_logger.info(header)
    _file_logger.info(f"  {'─' * 20}" + "─" * (5 * len(check_types)))
    for table in tables:
        table_sevs = severity_map.get(table, {})
        cells_str = ""
        for _label, ct in check_types:
            sev = table_sevs.get(ct)
            if sev == QASeverity.ERROR:
                cells_str += "    ✗"
            elif sev == QASeverity.WARNING:
                cells_str += "    ⚠"
            else:
                cells_str += "    ✓"
        _file_logger.info(f"  {table:<20}{cells_str}")
    if failed == 0 and warned == 0:
        _file_logger.info(f"Result: All {passed} tables passed QA checks")
    elif failed == 0:
        _file_logger.info(f"Result: {passed} of {total} tables passed, {warned} warned")
    else:
        parts = [f"{failed} of {total} tables failed QA checks"]
        if warned > 0:
            parts.append(f"{warned} warned")
        _file_logger.info(f"Result: {', '.join(parts)}")
