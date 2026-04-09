"""Tkinter UI backend for pg-upsert.

Wraps the existing :class:`~pg_upsert.ui.TableUI` and
:class:`~pg_upsert.ui.CompareUI` classes. tkinter is lazy-imported so
this module can be imported safely in headless environments as long as
:class:`TkinterBackend` is never *instantiated* without a display.
"""

from __future__ import annotations

from .base import UIBackend


class TkinterBackend(UIBackend):
    """UI backend that renders Tkinter GUI dialogs.

    Uses the existing :class:`~pg_upsert.ui.TableUI` and
    :class:`~pg_upsert.ui.CompareUI` from ``ui.py``. tkinter is imported
    lazily inside each method so that importing this module at the top level
    does **not** require a display.
    """

    def show_table(
        self,
        title: str,
        message: str,
        buttons: list[tuple[str, int, str]],
        headers: list[str],
        rows: list[list],
    ) -> tuple[int, None]:
        """Show a :class:`~pg_upsert.ui.TableUI` dialog.

        Args:
            title: Window title.
            message: Description shown above the table.
            buttons: Sequence of ``(label, value, keybinding)`` tuples.
            headers: Column header labels.
            rows: Table data rows.

        Returns:
            ``(button_value, None)`` from the activated dialog.
        """
        from .legacy import TableUI

        return TableUI(title, message, buttons, headers, rows).activate()

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
        exclude_cols: list[str] | None = None,
    ) -> tuple[int, None]:
        """Show a :class:`~pg_upsert.ui.CompareUI` dialog.

        Args:
            title: Window title.
            message: Description shown above the tables.
            buttons: Sequence of ``(label, value, keybinding)`` tuples.
            stg_headers: Column headers for the staging table.
            stg_data: Row data for the staging table.
            base_headers: Column headers for the base table.
            base_data: Row data for the base table.
            pk_cols: Primary key column names for mismatch highlighting.
            sidebyside: If ``True``, display the tables side by side.
            exclude_cols: Columns the upsert will not update; skipped when
                computing diffs for the "Highlight Diffs" toggle.

        Returns:
            ``(button_value, None)`` from the activated dialog.
        """
        from .legacy import CompareUI

        return CompareUI(
            title,
            message,
            buttons,
            stg_headers,
            stg_data,
            base_headers,
            base_data,
            pk_cols,
            sidebyside=sidebyside,
            exclude_cols=exclude_cols,
        ).activate()
