"""Console-only UI backend for pg-upsert.

Renders tables to stderr via ``display.console`` and returns ``(0, None)``
without prompting — suitable for non-interactive / headless environments.
"""

from __future__ import annotations

import logging

from . import display
from .base import UIBackend

logger = logging.getLogger(__name__)


class ConsoleBackend(UIBackend):
    """UI backend that prints to the console and auto-continues.

    No user interaction is required. All ``show_*`` calls render the
    relevant table(s) using the rich display layer and immediately return
    ``(0, None)`` (the "Continue" / first-button value).
    """

    def show_table(
        self,
        title: str,
        message: str,
        buttons: list[tuple[str, int, str]],
        headers: list[str],
        rows: list[list],
    ) -> tuple[int, None]:
        """Render *rows* to the console and return ``(0, None)``.

        Args:
            title: Panel title (logged at INFO level).
            message: Description logged above the table.
            buttons: Ignored — auto-continues with value ``0``.
            headers: Column header labels.
            rows: Table data rows.

        Returns:
            ``(0, None)``
        """
        logger.info(title)
        row_dicts = [dict(zip(headers, row, strict=True)) for row in rows]
        table = display.format_table(row_dicts, headers=headers, title=message)
        display.console.print(table)
        return (0, None)

    def show_comparison(
        self,
        title: str,
        message: str,
        buttons: list[tuple[str, int, str]],
        stg_headers: list[str],
        stg_data: list,
        base_headers: list[str],
        base_data: list,
        pk_cols: list[str],
        sidebyside: bool = False,
    ) -> tuple[int, None]:
        """Render both comparison tables to the console and return ``(0, None)``.

        Args:
            title: Panel title (logged at INFO level).
            message: Description logged above the tables.
            buttons: Ignored — auto-continues with value ``0``.
            stg_headers: Column headers for the staging (new) table.
            stg_data: Row data for the staging table.
            base_headers: Column headers for the base (existing) table.
            base_data: Row data for the base table.
            pk_cols: Primary key columns (informational only for console).
            sidebyside: Ignored for console output.

        Returns:
            ``(0, None)``
        """
        logger.info(title)
        display.console.print(message)
        stg_dicts = [dict(zip(stg_headers, row, strict=True)) for row in stg_data]
        stg_table = display.format_table(stg_dicts, headers=stg_headers, title="New data (staging)")
        display.console.print(stg_table)
        base_dicts = [dict(zip(base_headers, row, strict=True)) for row in base_data]
        base_table = display.format_table(base_dicts, headers=base_headers, title="Existing data (base)")
        display.console.print(base_table)
        return (0, None)
