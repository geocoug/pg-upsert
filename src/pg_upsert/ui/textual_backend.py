"""Textual TUI backend for pg-upsert.

Provides terminal-based interactive dialogs using the ``textual`` library.
textual is lazy-imported; this module can be imported safely without textual
installed as long as :class:`TextualBackend` is never instantiated.
"""

from __future__ import annotations

from .base import UIBackend


def _run_table_app(
    title: str,
    message: str,
    buttons: list[tuple[str, int, str]],
    headers: list[str],
    rows: list[list],
) -> int:
    """Build and run a textual app for a single-table dialog.

    Args:
        title: App title shown at the top.
        message: Description shown above the data table.
        buttons: Sequence of ``(label, value, keybinding)`` tuples.
        headers: Column header labels.
        rows: Table data rows (each row is a list of cell values).

    Returns:
        The integer value associated with the button the user clicked.
    """
    from textual.app import App, ComposeResult
    from textual.containers import Horizontal
    from textual.widgets import Button, DataTable, Footer, Header, Label

    _headers = headers
    _rows = rows
    _buttons = buttons
    _message = message

    class TableApp(App[int]):
        """Single-table confirmation dialog."""

        TITLE = title
        CSS = """
        Screen {
            align: center middle;
        }
        #message {
            padding: 1 2;
            width: 100%;
        }
        DataTable {
            height: 1fr;
            width: 100%;
        }
        #button-bar {
            dock: bottom;
            height: auto;
            padding: 1 2;
            align: right middle;
        }
        Button {
            margin: 0 1;
        }
        """

        def __init__(self) -> None:
            super().__init__()
            self._result: int = _buttons[0][1] if _buttons else 0

        def compose(self) -> ComposeResult:
            yield Header()
            yield Label(_message, id="message")
            yield DataTable(id="data-table")
            with Horizontal(id="button-bar"):
                for label, value, _key in _buttons:
                    yield Button(
                        label,
                        id=f"btn-{value}",
                        variant="primary" if value == 0 else "default",
                    )
            yield Footer()

        def on_mount(self) -> None:
            tbl = self.query_one("#data-table", DataTable)
            tbl.add_columns(*_headers)
            for row in _rows:
                tbl.add_row(*[str(cell) if cell is not None else "" for cell in row])
            # Focus the first button.
            if _buttons:
                first_btn = self.query_one(f"#btn-{_buttons[0][1]}", Button)
                first_btn.focus()

        def on_button_pressed(self, event: Button.Pressed) -> None:
            btn_id = event.button.id or ""
            if btn_id.startswith("btn-"):
                try:
                    self._result = int(btn_id[4:])
                except ValueError:
                    self._result = 0
            self.exit(self._result)

        def on_key(self, event) -> None:  # type: ignore[override]
            """Handle keyboard shortcuts from button specs."""
            for _label, value, key in _buttons:
                if key:
                    # Normalise: strip angle brackets, map common names.
                    clean = key.strip("<>").lower()
                    pressed = event.key.lower()
                    if pressed == clean or (clean == "return" and pressed == "enter"):
                        self._result = value
                        self.exit(value)
                        return

    result = TableApp().run()
    return result if result is not None else (_buttons[0][1] if _buttons else 0)


def _run_comparison_app(
    title: str,
    message: str,
    buttons: list[tuple[str, int, str]],
    stg_headers: list[str],
    stg_data: list,
    base_headers: list[str],
    base_data: list,
    pk_cols: list[str],
    sidebyside: bool = False,
) -> int:
    """Build and run a textual app for a two-table comparison dialog.

    Args:
        title: App title.
        message: Description shown above the tables.
        buttons: Sequence of ``(label, value, keybinding)`` tuples.
        stg_headers: Column headers for the staging table.
        stg_data: Row data for the staging table.
        base_headers: Column headers for the base table.
        base_data: Row data for the base table.
        pk_cols: Primary key columns (shown in table border title).
        sidebyside: If ``True``, arrange the tables horizontally.

    Returns:
        The integer value associated with the button the user clicked.
    """
    from textual.app import App, ComposeResult
    from textual.containers import Horizontal, Vertical
    from textual.widgets import Button, DataTable, Footer, Header, Label

    _buttons = buttons
    _message = message
    _stg_headers = stg_headers
    _stg_data = stg_data
    _base_headers = base_headers
    _base_data = base_data
    _pk_cols = pk_cols
    _sidebyside = sidebyside

    _table_css = (
        """
        #tables {
            layout: horizontal;
            height: 1fr;
        }
        .table-panel {
            width: 1fr;
            height: 100%;
            border: solid $accent;
        }
        """
        if sidebyside
        else """
        #tables {
            layout: vertical;
            height: 1fr;
        }
        .table-panel {
            width: 100%;
            height: 1fr;
            border: solid $accent;
        }
        """
    )

    class CompareApp(App[int]):
        """Two-table comparison dialog with row matching on PK columns."""

        TITLE = title
        CSS = (
            """
        #message {
            padding: 1 2;
            width: 100%;
        }
        #button-bar {
            dock: bottom;
            height: auto;
            padding: 1 2;
            align: right middle;
        }
        Button {
            margin: 0 1;
        }
        DataTable:focus {
            border: tall $accent;
        }
        """
            + _table_css
        )

        def __init__(self) -> None:
            super().__init__()
            self._result: int = _buttons[0][1] if _buttons else 0
            self._syncing: bool = False
            # PK column index lookups.
            self._stg_pk_idx = [i for i, h in enumerate(_stg_headers) if h in _pk_cols]
            self._base_pk_idx = [i for i, h in enumerate(_base_headers) if h in _pk_cols]
            # PK-value → row-index maps, built at mount time for O(1) lookup.
            self._stg_pk_map: dict[tuple, int] = {}
            self._base_pk_map: dict[tuple, int] = {}
            # Column keys, set at mount time.
            self._stg_col_keys: list = []
            self._base_col_keys: list = []

        def compose(self) -> ComposeResult:
            yield Header()
            yield Label(_message, id="message")
            container_cls = Horizontal if _sidebyside else Vertical
            with container_cls(id="tables"):
                yield DataTable(id="stg-table", classes="table-panel", cursor_type="row")
                yield DataTable(id="base-table", classes="table-panel", cursor_type="row")
            with Horizontal(id="button-bar"):
                for label, value, _key in _buttons:
                    yield Button(
                        label,
                        id=f"btn-{value}",
                        variant="primary" if value == 0 else "default",
                    )
            yield Footer()

        def _add_rows_and_build_map(
            self,
            tbl: DataTable,
            headers: list[str],
            data: list,
            pk_idx: list[int],
        ) -> tuple[list, dict[tuple, int]]:
            """Add rows to a DataTable and build a PK → row-index map."""
            col_keys = tbl.add_columns(*headers)
            pk_map: dict[tuple, int] = {}
            for row_num, row in enumerate(data):
                cells = [str(cell) if cell is not None else "" for cell in row]
                tbl.add_row(*cells)
                pk_vals = tuple(cells[i] for i in pk_idx)
                pk_map[pk_vals] = row_num
            return list(col_keys), pk_map

        def on_mount(self) -> None:
            stg_tbl = self.query_one("#stg-table", DataTable)
            stg_tbl.border_title = f"New data (staging) — PK: {', '.join(_pk_cols)}"
            self._stg_col_keys, self._stg_pk_map = self._add_rows_and_build_map(
                stg_tbl,
                _stg_headers,
                _stg_data,
                self._stg_pk_idx,
            )

            base_tbl = self.query_one("#base-table", DataTable)
            base_tbl.border_title = "Existing data (base)"
            self._base_col_keys, self._base_pk_map = self._add_rows_and_build_map(
                base_tbl,
                _base_headers,
                _base_data,
                self._base_pk_idx,
            )

            stg_tbl.focus()

        def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
            """Sync cursor: when a row is highlighted in one table, jump to the matching PK row in the other."""
            if self._syncing:
                return
            source = event.data_table
            if source.id not in ("stg-table", "base-table"):
                return

            self._syncing = True
            try:
                stg_tbl = self.query_one("#stg-table", DataTable)
                base_tbl = self.query_one("#base-table", DataTable)
                row_key = event.row_key

                if source.id == "stg-table":
                    # Extract PK values from the highlighted staging row.
                    pk_vals = tuple(str(source.get_cell(row_key, self._stg_col_keys[i])) for i in self._stg_pk_idx)
                    match_idx = self._base_pk_map.get(pk_vals)
                    if match_idx is not None:
                        base_tbl.move_cursor(row=match_idx, animate=False)
                else:
                    pk_vals = tuple(str(source.get_cell(row_key, self._base_col_keys[i])) for i in self._base_pk_idx)
                    match_idx = self._stg_pk_map.get(pk_vals)
                    if match_idx is not None:
                        stg_tbl.move_cursor(row=match_idx, animate=False)
            finally:
                self._syncing = False

        def on_button_pressed(self, event: Button.Pressed) -> None:
            btn_id = event.button.id or ""
            if btn_id.startswith("btn-"):
                try:
                    self._result = int(btn_id[4:])
                except ValueError:
                    self._result = 0
            self.exit(self._result)

        def on_key(self, event) -> None:  # type: ignore[override]
            """Handle keyboard shortcuts from button specs."""
            for _label, value, key in _buttons:
                if key:
                    clean = key.strip("<>").lower()
                    pressed = event.key.lower()
                    if pressed == clean or (clean == "return" and pressed == "enter"):
                        self._result = value
                        self.exit(value)
                        return

    result = CompareApp().run()
    return result if result is not None else (_buttons[0][1] if _buttons else 0)


class TextualBackend(UIBackend):
    """UI backend that renders terminal-based dialogs using textual.

    Requires ``textual>=0.47.0`` to be installed (available via the
    ``tui`` optional-dependency group). Each ``show_*`` call blocks
    until the user selects a button.
    """

    def show_table(
        self,
        title: str,
        message: str,
        buttons: list[tuple[str, int, str]],
        headers: list[str],
        rows: list[list],
    ) -> tuple[int, None]:
        """Display a single-table TUI dialog and wait for a button press.

        Args:
            title: App title.
            message: Description shown above the data table.
            buttons: Sequence of ``(label, value, keybinding)`` tuples.
            headers: Column header labels.
            rows: Table data rows.

        Returns:
            ``(button_value, None)``
        """
        value = _run_table_app(title, message, buttons, headers, rows)
        return (value, None)

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
        """Display a two-table comparison TUI dialog and wait for a button press.

        Args:
            title: App title.
            message: Description shown above the tables.
            buttons: Sequence of ``(label, value, keybinding)`` tuples.
            stg_headers: Column headers for the staging table.
            stg_data: Row data for the staging table.
            base_headers: Column headers for the base table.
            base_data: Row data for the base table.
            pk_cols: Primary key columns (shown in table border title).
            sidebyside: If ``True``, arrange tables horizontally.

        Returns:
            ``(button_value, None)``
        """
        value = _run_comparison_app(
            title,
            message,
            buttons,
            stg_headers,
            stg_data,
            base_headers,
            base_data,
            pk_cols,
            sidebyside=sidebyside,
        )
        return (value, None)
