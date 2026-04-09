#!/usr/bin/env python

from __future__ import annotations

import logging
import re
import tkinter as tk
import tkinter.font as tkfont
import tkinter.ttk as ttk

logger = logging.getLogger(__name__)


class CompareUI:
    # Diff highlighting palette (shared with execsql for visual consistency).
    _DIFF_TAG_MATCH = ("diff_match", "#a3d9a5", "#1a3a1a")
    _DIFF_TAG_CHANGED = ("diff_changed", "#f5d98e", "#3a2e00")
    _DIFF_TAG_ONLY = ("diff_only", "#f5a3a3", "#3a0a0a")
    _DIFF_MARKER = "● "  # Prefix applied to changed cells (ttk.Treeview has no cell bg).

    def __init__(
        self: CompareUI,
        title: str,
        message: str,
        button_list: list[tuple[str, int, str]],
        headers1: list[str],
        rows1: list | tuple,
        headers2: list[str],
        rows2: list | tuple,
        keylist: list[str],
        selected_button=0,
        sidebyside=False,
        exclude_cols: list[str] | None = None,
    ) -> None:
        """button_list: list of 3-tuples where the first item is the button label,
        the second item is the button's value, and the third (optional) value is the key
        to bind to the button.
        Key identifiers must be in the form taken by the Tk bind() function,
        e.g., "<Return>" and "<Escape>" for those keys, respectively.
        keylist: list of column names that make up a common key for both tables.
        selected_button: integer identifying which button should get the focus (0-based)
        exclude_cols: columns the upsert will not update; skipped from diff detection.
        """
        self.headers1 = headers1
        self.rows1 = rows1
        self.headers2 = headers2
        self.rows2 = rows2
        self.keylist = keylist
        self.exclude_cols = list(exclude_cols or [])
        self.return_value = None
        self.button_value = None
        # Diff-highlighting state (lazy; computed on first toggle).
        self._diff_on = False
        self._diff_result = None
        self._original_values1: dict[str, tuple] = {}
        self._original_values2: dict[str, tuple] = {}
        self._diff_summary_var: tk.StringVar | None = None
        self._diff_btn_text_var: tk.StringVar | None = None
        self.win = tk.Toplevel()
        self.win.title(title)

        controlframe = ttk.Frame(master=self.win, padding="3 3 3 3")
        # Diff highlighting toggle button and summary label.
        self._diff_btn_text_var = tk.StringVar(value="Highlight Diffs")
        diff_btn = ttk.Button(
            controlframe,
            textvariable=self._diff_btn_text_var,
            command=self._toggle_diffs,
        )
        diff_btn.grid(column=0, row=1, sticky=tk.W)
        self._diff_summary_var = tk.StringVar(value="")
        diff_summary_label = ttk.Label(
            controlframe,
            textvariable=self._diff_summary_var,
        )
        diff_summary_label.grid(column=1, row=1, sticky=tk.W, padx=(8, 0))
        self.msg_label = None
        # The ttk.Treeview widget that displays table 1.
        self.tbl1 = None
        # The ttk.Treeview widget that displays table 2.
        self.tbl2 = None
        # A list of ttk.Button objects
        self.buttons = []
        self.focus_button = selected_button
        self.button_clicked_value = None

        def wrap_msg(event: tk.Event):
            """Wrap the message text to fit the window width."""
            self.msg_label.configure(wraplength=event.width - 5)

        # Message frame and control.
        msgframe = ttk.Frame(master=self.win, padding="3 3 3 3")
        self.msg_label = ttk.Label(msgframe, text=message)
        self.msg_label.bind("<Configure>", wrap_msg)
        self.msg_label.grid(column=0, row=0, sticky=tk.EW)
        # Bottom button frame
        btnframe = ttk.Frame(master=self.win, padding="3 3 3 3")

        def find_match(
            from_table: ttk.Treeview,
            from_headers: list | tuple,
            to_table: ttk.Treeview,
            to_headers: list | tuple,
        ):
            """Highlight all rows in to_table that matches the selected row in
            from_table.  Tables are TreeView objects.
            """
            sel_item = from_table.focus()
            if sel_item is not None and sel_item != "":
                sel_dict = dict(
                    zip(from_headers, from_table.item(sel_item)["values"], strict=True),
                )
                key_dict = {k: sel_dict[k] for k in self.keylist}
                # Find the matching data (if any) in to_table.
                to_items = to_table.get_children()
                to_item = ""
                found = False
                for to_item in to_items:
                    to_table.selection_remove(to_item)
                    to_dict = dict(
                        zip(to_headers, to_table.item(to_item)["values"], strict=True),
                    )
                    if all(to_dict[k] == key_dict[k] for k in key_dict):
                        if not found:
                            to_table.see(to_item)
                            found = True
                        to_table.selection_add(to_item)

        def match2to1(event: tk.Event):
            """Find the matching row in table 1 for the selected row in table 2."""
            find_match(self.tbl1, self.headers1, self.tbl2, self.headers2)

        def match1to2(event: tk.Event):
            """Find the matching row in table 2 for the selected row in table 1."""
            find_match(self.tbl2, self.headers2, self.tbl1, self.headers1)

        # Create data tables.
        self.tablemaster = ttk.Frame(master=self.win)
        self.tableframe1, self.tbl1 = treeview_table(
            self.tablemaster,
            self.rows1,
            self.headers1,
            "browse",
        )
        self.tableframe2, self.tbl2 = treeview_table(
            self.tablemaster,
            self.rows2,
            self.headers2,
            "browse",
        )
        self.tbl1.bind("<ButtonRelease-1>", match2to1)
        self.tbl2.bind("<ButtonRelease-1>", match1to2)
        # Configure diff highlighting tags and snapshot original cell values.
        for _tbl in (self.tbl1, self.tbl2):
            for _name, _bg, _fg in (
                self._DIFF_TAG_MATCH,
                self._DIFF_TAG_CHANGED,
                self._DIFF_TAG_ONLY,
            ):
                _tbl.tag_configure(_name, background=_bg, foreground=_fg)
        for _iid in self.tbl1.get_children():
            self._original_values1[_iid] = tuple(self.tbl1.item(_iid)["values"])
        for _iid in self.tbl2.get_children():
            self._original_values2[_iid] = tuple(self.tbl2.item(_iid)["values"])
        # Compute diff eagerly and show summary regardless of toggle state.
        from .diff import compute_row_diffs

        self._diff_result = compute_row_diffs(
            list(self.headers1),
            list(self.rows1),
            list(self.headers2),
            list(self.rows2),
            list(self.keylist),
            exclude_cols=self.exclude_cols,
        )
        self._diff_summary_var.set(self._diff_result.summary)
        # Put the frames and other widgets in place.
        msgframe.grid(column=0, row=0, sticky=tk.EW)
        controlframe.grid(column=0, row=1, sticky=tk.EW)
        self.tablemaster.grid(column=0, row=2, sticky=tk.NSEW)
        if sidebyside:
            self.tableframe1.grid(column=0, row=0, sticky=tk.NSEW)
            self.tableframe2.grid(column=1, row=0, sticky=tk.NSEW)
            self.tablemaster.rowconfigure(0, weight=1)
            self.tablemaster.columnconfigure(0, weight=1)
            self.tablemaster.columnconfigure(1, weight=1)
        else:
            self.tableframe1.grid(column=0, row=0, sticky=tk.NSEW)
            self.tableframe2.grid(column=0, row=1, sticky=tk.NSEW)
            self.tablemaster.columnconfigure(0, weight=1)
            self.tablemaster.rowconfigure(0, weight=1)
            self.tablemaster.rowconfigure(1, weight=1)
        # Create buttons.
        btnframe.grid(column=0, row=3, sticky=tk.E)
        for colno, btn_spec in enumerate(button_list):
            btn_action = ClickSet(self, btn_spec[1]).click
            btn = ttk.Button(btnframe, text=btn_spec[0], command=btn_action)
            if btn_spec[2] is not None:
                self.win.bind(btn_spec[2], btn_action)
            self.buttons.append(btn)
            btn.grid(column=colno, row=0, sticky=tk.E, padx=3)
        # Allow resizing.
        self.win.columnconfigure(0, weight=1)
        self.win.rowconfigure(1, weight=0)
        self.win.rowconfigure(2, weight=2)
        msgframe.columnconfigure(0, weight=1)
        btnframe.columnconfigure(0, weight=1)
        # Other key bindings
        self.win.protocol("WM_DELETE_WINDOW", self.cancel)
        # Position window.
        self.win.update_idletasks()
        m = re.match(r"(\d+)x(\d+)\+(-?\d+)\+(-?\d+)", self.win.geometry())
        if m is not None:
            wwd = int(m.group(1))
            what = int(m.group(2))
            swd = self.win.winfo_screenwidth()
            sht = self.win.winfo_screenheight()
            xpos = (swd / 2) - (wwd / 2)
            ypos = (sht / 2) - (what / 2)
            self.win.geometry(f"{wwd}x{what}+{int(xpos)}+{int(ypos)}")
        # Limit resizing
        self.win.minsize(width=300, height=0)

    def cancel(self: CompareUI):
        """Cancel the dialog."""
        self.dialog_canceled = True
        self.win.destroy()

    def _toggle_diffs(self: CompareUI) -> None:
        """Toggle visual diff highlighting on both comparison tables.

        The summary line is populated eagerly at dialog init time and is
        NOT affected by this toggle — only the row/cell styling changes.
        """
        if self._diff_result is None:
            return
        self._diff_on = not self._diff_on

        self._apply_diffs_to_table(
            self.tbl1,
            self.headers1,
            self._diff_result.stg_row_states,
            self._diff_result.stg_changed_cols,
            self._original_values1,
        )
        self._apply_diffs_to_table(
            self.tbl2,
            self.headers2,
            self._diff_result.base_row_states,
            self._diff_result.base_changed_cols,
            self._original_values2,
        )

        if self._diff_btn_text_var is not None:
            self._diff_btn_text_var.set("Hide Diffs" if self._diff_on else "Highlight Diffs")

    def _apply_diffs_to_table(
        self: CompareUI,
        tbl: ttk.Treeview,
        headers: list[str],
        row_states: list[str],
        changed_cols: list[set[str]],
        original_values: dict[str, tuple],
    ) -> None:
        """Apply or remove diff tags and cell markers on *tbl*."""
        state_to_tag = {
            "match": self._DIFF_TAG_MATCH[0],
            "changed": self._DIFF_TAG_CHANGED[0],
            "only_stg": self._DIFF_TAG_ONLY[0],
            "only_base": self._DIFF_TAG_ONLY[0],
        }
        for iid in tbl.get_children():
            try:
                row_idx = int(iid)
            except ValueError:
                continue
            if row_idx >= len(row_states):
                continue
            if not self._diff_on:
                # Turning OFF: clear tags and restore original cell values.
                tbl.item(iid, tags=())
                if iid in original_values:
                    tbl.item(iid, values=original_values[iid])
                continue
            # Turning ON: apply tag and, for changed rows, prefix cell markers.
            state = row_states[row_idx]
            tag = state_to_tag.get(state, "")
            if tag:
                tbl.item(iid, tags=(tag,))
            if state == "changed" and iid in original_values:
                orig = original_values[iid]
                diff_set = changed_cols[row_idx]
                marked = tuple(
                    f"{self._DIFF_MARKER}{orig[i]}" if headers[i] in diff_set else orig[i] for i in range(len(headers))
                )
                tbl.item(iid, values=marked)

    def activate(self: CompareUI):
        """Activate the dialog."""
        # Window control
        self.win.grab_set()
        self.win._root().withdraw()
        self.win.focus_force()
        if self.focus_button:
            self.buttons[self.focus_button].focus()
        self.win.wait_window(self.win)
        self.win.update_idletasks()
        rv = self.return_value
        return (self.button_clicked_value, rv)


class TableUI:
    """A class for displaying a single table in a Tkinter window."""

    def __init__(
        self: TableUI,
        title: str,
        message: str,
        button_list: list[tuple[str, int, str]],
        headers: list[str],
        rows: list | tuple,
        selected_button=0,
    ) -> None:
        self.headers = headers
        self.rows = rows
        self.return_value = None
        self.button_value = None
        self.win = tk.Toplevel()
        self.win.title(title)
        self.msg_label = None
        self.tbl = None  # The ttk.Treeview widget that displays table 1.
        self.buttons = []  # A list of ttk.Button objects
        self.focus_button = selected_button
        self.button_clicked_value = None

        def wrap_msg(event: tk.Event):
            """Wrap the message text to fit the window width."""
            self.msg_label.configure(wraplength=event.width - 5)

        # Message frame and control.
        msgframe = ttk.Frame(master=self.win, padding="3 3 3 3")
        self.msg_label = ttk.Label(msgframe, text=message)
        self.msg_label.bind("<Configure>", wrap_msg)
        self.msg_label.grid(column=0, row=0, sticky=tk.EW)
        # Bottom button frame
        btnframe = ttk.Frame(master=self.win, padding="3 3 3 3")
        # Create data tables.
        self.tablemaster = ttk.Frame(master=self.win)
        self.tableframe, self.tbl1 = treeview_table(
            self.tablemaster,
            self.rows,
            self.headers,
            "browse",
        )
        # Put the frames and other widgets in place.
        msgframe.grid(column=0, row=0, sticky=tk.EW)
        self.tablemaster.grid(column=0, row=2, sticky=tk.NSEW)
        self.tableframe.grid(column=0, row=0, sticky=tk.NSEW)
        self.tablemaster.columnconfigure(0, weight=1)
        self.tablemaster.rowconfigure(0, weight=1)
        self.tablemaster.rowconfigure(1, weight=1)
        btnframe.grid(column=0, row=3, sticky=tk.E)
        for colno, btn_spec in enumerate(button_list):
            btn_action = ClickSet(self, btn_spec[1]).click
            btn = ttk.Button(btnframe, text=btn_spec[0], command=btn_action)
            if btn_spec[2] is not None:
                self.win.bind(btn_spec[2], btn_action)
            self.buttons.append(btn)
            btn.grid(column=colno, row=0, sticky=tk.E, padx=3)
        # Allow resizing.
        self.win.columnconfigure(0, weight=1)
        self.win.rowconfigure(1, weight=0)
        self.win.rowconfigure(2, weight=2)
        msgframe.columnconfigure(0, weight=1)
        btnframe.columnconfigure(0, weight=1)
        # Other key bindings
        self.win.protocol("WM_DELETE_WINDOW", self.cancel)
        # Position window.
        self.win.update_idletasks()
        m = re.match(r"(\d+)x(\d+)\+(-?\d+)\+(-?\d+)", self.win.geometry())
        if m is not None:
            wwd = int(m.group(1))
            what = int(m.group(2))
            swd = self.win.winfo_screenwidth()
            sht = self.win.winfo_screenheight()
            xpos = (swd / 2) - (wwd / 2)
            ypos = (sht / 2) - (what / 2)
            self.win.geometry(f"{wwd}x{what}+{int(xpos)}+{int(ypos)}")
        # Limit resizing
        self.win.minsize(width=300, height=0)

    def cancel(self: TableUI):
        """Cancel the dialog."""
        self.dialog_canceled = True
        self.win.destroy()

    def activate(self: TableUI):
        """Activate the dialog."""
        self.win.grab_set()
        self.win._root().withdraw()
        self.win.focus_force()
        if self.focus_button:
            self.buttons[self.focus_button].focus()
        self.win.wait_window(self.win)
        self.win.update_idletasks()
        rv = self.return_value
        return (self.button_clicked_value, rv)


class ClickSet:
    """A class for handling button clicks."""

    def __init__(self: ClickSet, ui_obj, button_value: int) -> None:
        self.ui_obj = ui_obj
        self.button_value = button_value

    def click(self: ClickSet, *args):
        """Handle a button click."""
        self.ui_obj.button_clicked_value = self.button_value
        self.dialog_canceled = self.button_value is None
        self.ui_obj.win.destroy()


def treeview_table(
    parent: ttk.Frame,
    rowset: list | tuple,
    column_headers: list | tuple,
    select_mode="none",
):
    """Creates a TreeView table containing the specified data, with scrollbars and
    status bar in an enclosing frame.
    This does not grid the table frame in its parent widget. Returns a tuple
    of 0: the frame containing the table,  and 1: the table widget itself.
    """
    nrows = range(len(rowset))
    ncols = range(len(column_headers))
    hdrwidths = [len(column_headers[j]) for j in ncols]
    if len(rowset) > 0:
        datawidthtbl = [
            [
                len(
                    (rowset[i][j] if isinstance(rowset[i][j], str) else str(rowset[i][j])),
                )
                for i in nrows
            ]
            for j in ncols
        ]
        datawidths = [max(cwidths) for cwidths in datawidthtbl]
    else:
        datawidths = hdrwidths
    colwidths = [max(hdrwidths[i], datawidths[i]) for i in ncols]
    # Set the font.
    ff = tkfont.nametofont("TkFixedFont")
    tblstyle = ttk.Style()
    tblstyle.configure("tblstyle", font=ff)
    charpixels = int(1.3 * ff.measure("0"))
    tableframe = ttk.Frame(master=parent, padding="3 3 3 3")
    statusframe = ttk.Frame(master=tableframe)
    # Create and configure the Treeview table widget
    tv_widget = ttk.Treeview(
        tableframe,
        columns=column_headers,
        selectmode=select_mode,
        show="headings",
    )
    tv_widget.configure()["style"] = tblstyle
    ysb = ttk.Scrollbar(tableframe, orient="vertical", command=tv_widget.yview)
    xsb = ttk.Scrollbar(tableframe, orient="horizontal", command=tv_widget.xview)
    tv_widget.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)
    # Status bar
    statusbar = ttk.Label(
        statusframe,
        text=f"    {len(rowset)} rows",
        relief=tk.RIDGE,
        anchor=tk.W,
    )
    tableframe.statuslabel = statusbar
    # Fill the Treeview table widget with data
    set_tv_headers(tv_widget, column_headers, colwidths, charpixels)
    fill_tv_table(tv_widget, rowset, statusbar)
    # Place the table
    tv_widget.grid(column=0, row=0, sticky=tk.NSEW)
    ysb.grid(column=1, row=0, sticky=tk.NS)
    xsb.grid(column=0, row=1, sticky=tk.EW)
    statusframe.grid(column=0, row=3, sticky=tk.EW)
    tableframe.columnconfigure(0, weight=1)
    tableframe.rowconfigure(0, weight=1)
    # Place the status bar
    statusbar.pack(side=tk.BOTTOM, fill=tk.X)
    # Allow resizing of the table
    tableframe.columnconfigure(0, weight=1)
    tableframe.rowconfigure(0, weight=1)
    #
    return tableframe, tv_widget


def set_tv_headers(
    tvtable: ttk.Treeview,
    column_headers: list,
    colwidths: list,
    charpixels: int,
):
    """Set the headers and column widths for a Treeview table widget."""
    pixwidths = [charpixels * col for col in colwidths]
    for i in range(len(column_headers)):
        hdr = column_headers[i]
        tvtable.column(hdr, width=pixwidths[i])
        tvtable.heading(
            hdr,
            text=hdr,
            command=lambda _col=hdr: treeview_sort_column(tvtable, _col, False),
        )


def treeview_sort_column(tv: ttk.Treeview, col: str, reverse: bool):
    """Sort a column in a Treeview table widget.

    From https://stackoverflow.com/questions/1966929/tk-treeview-column-sort#1967793
    """
    colvals = [(tv.set(k, col), k) for k in tv.get_children()]
    colvals.sort(reverse=reverse)
    # Rearrange items in sorted positions
    for index, (_val, k) in enumerate(colvals):
        tv.move(k, "", index)
    # Reverse sort next time
    tv.heading(col, command=lambda: treeview_sort_column(tv, col, not reverse))


def fill_tv_table(tvtable: ttk.Treeview, rowset: list | tuple, status_label=None):
    """Fill a Treeview table widget with data."""
    for i, row in enumerate(rowset):
        enc_row = [c if c is not None else "" for c in row]
        tvtable.insert(parent="", index="end", iid=str(i), values=enc_row)
    if status_label is not None:
        status_label.config(text=f"    {len(rowset)} rows")
