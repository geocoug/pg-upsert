"""Abstract base class for pg-upsert interactive UI backends."""

from __future__ import annotations

from abc import ABC, abstractmethod


class UIBackend(ABC):
    """Abstract interface for interactive UI dialogs.

    Concrete implementations handle rendering and user interaction
    for table-display and comparison dialogs. Each method must return
    a ``(button_value, None)`` tuple to match the contract established
    by the legacy ``TableUI`` and ``CompareUI`` classes.
    """

    @abstractmethod
    def show_table(
        self,
        title: str,
        message: str,
        buttons: list[tuple[str, int, str]],
        headers: list[str],
        rows: list[list],
    ) -> tuple[int, None]:
        """Show a single-table dialog with action buttons.

        Args:
            title: Window/panel title.
            message: Description shown above the table.
            buttons: Sequence of ``(label, value, keybinding)`` tuples.
            headers: Column header labels.
            rows: Table data rows (each row is a list of cell values).

        Returns:
            ``(button_value, None)`` — the value of the button the user clicked,
            and ``None`` as a placeholder for a future return value.
        """

    @abstractmethod
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
        """Show a two-table comparison dialog with action buttons.

        Args:
            title: Window/panel title.
            message: Description shown above the tables.
            buttons: Sequence of ``(label, value, keybinding)`` tuples.
            stg_headers: Column headers for the staging table.
            stg_data: Row data for the staging table.
            base_headers: Column headers for the base table.
            base_data: Row data for the base table.
            pk_cols: Primary key column names used to highlight mismatches.
            sidebyside: If ``True``, display the two tables side by side.

        Returns:
            ``(button_value, None)`` — the value of the button the user clicked,
            and ``None`` as a placeholder for a future return value.
        """
