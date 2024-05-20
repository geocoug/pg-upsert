#!/usr/bin/env python

from __future__ import annotations

import argparse
import getpass
import logging
import re
import sys
import tkinter as tk
import tkinter.font as tkfont
import tkinter.ttk as ttk
from datetime import datetime
from pathlib import Path

import polars as pl
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.sql import SQL, Composable, Identifier, Literal
from tabulate import tabulate

description_long = """
Check data in a staging table or set of staging tables, then update and insert (upsert)
rows of a base table or base tables from the staging table(s) of the same name.
Initial table checks include not-null, primary key, and foreign key checks.
If any of these checks fail, the program will exit with an error message.
If all checks pass, the program will display the number of rows to be inserted
and updated, and ask for confirmation before proceeding. If the user confirms, the
program will perform the upserts and display the number of rows inserted and updated.
If the user does not confirm, the program will exit without performing any upserts.
"""

description_short = (
    "Update and insert (upsert) data from staging tables to base tables."
)


class PostgresDB:
    """Base database object."""

    def __init__(
        self: PostgresDB,
        host: str,
        database: str,
        user: str,
        **kwargs,
    ) -> None:
        self.host = host
        self.database = database
        self.user = user
        if ("passwd" in kwargs and kwargs["passwd"] is not None) or (
            "password" in kwargs and kwargs["password"] is not None
        ):
            self.passwd = kwargs["passwd"]
        else:
            self.passwd = self.get_password()
        self.port = 5432
        self.in_transaction = False
        self.encoding = "UTF8"
        self.conn = None

    def __repr__(self: PostgresDB) -> str:
        return f"{self.__class__.__name__}(host={self.host}, database={self.database}, user={self.user})"  # noqa: E501

    def __del__(self: PostgresDB) -> None:
        """Delete the instance."""
        self.close()

    def get_password(self):
        return getpass.getpass(
            f"The script {Path(__file__).name} wants the password for {self!s}: ",
        )

    def open_db(self: PostgresDB) -> None:
        """Open a database connection."""

        def db_conn(db):
            """Return a database connection object."""
            return psycopg2.connect(
                host=str(db.host),
                database=str(db.database),
                port=db.port,
                user=str(db.user),
                password=str(db.passwd),
            )

        if self.conn is None:
            self.conn = db_conn(self)
            self.conn.set_session(autocommit=False)
        self.encoding = self.conn.encoding

    def cursor(self: PostgresDB):
        """Return the connection cursor."""
        self.open_db()
        return self.conn.cursor(cursor_factory=DictCursor)

    def close(self: PostgresDB) -> None:
        """Close the database connection."""
        self.rollback()
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def commit(self: PostgresDB) -> None:
        """Commit the current transaction."""
        if self.conn:
            self.conn.commit()
        self.in_transaction = False

    def rollback(self: PostgresDB) -> None:
        """Roll back the current transaction."""
        if self.conn is not None:
            self.conn.rollback()
        self.in_transaction = False

    def execute(self: PostgresDB, sql: str | Composable, params=None):
        """A shortcut to self.cursor().execute() that handles encoding.

        Handles insert, updates, deletes
        """
        self.in_transaction = True
        try:
            curs = self.cursor()
            if isinstance(sql, Composable):
                curs.execute(sql)
            else:
                if params is None:
                    curs.execute(sql.encode(self.encoding))
                else:
                    curs.execute(sql.encode(self.encoding), params)
        except Exception:
            self.rollback()
            raise
        return curs

    def rowdict(self: PostgresDB, sql: str | Composable, params=None) -> tuple:
        """Convert a cursor object to an iterable that.

        yields dictionaries of row data.
        """
        curs = self.execute(sql, params)
        headers = [d[0] for d in curs.description]

        def dict_row():
            """Convert a data row to a dictionary."""
            row = curs.fetchone()
            if row:
                if self.encoding:
                    r = [
                        (
                            c.decode(self.encoding, "backslashreplace")
                            if isinstance(c, bytes)
                            else c
                        )
                        for c in row
                    ]
                else:
                    r = row
                return dict(zip(headers, r, strict=True))
            return None

        return (iter(dict_row, None), headers, curs.rowcount)

    def dataframe(
        self: PostgresDB,
        sql: str | Composable,
        params=None,
        **kwargs,
    ) -> pl.DataFrame:
        """Return query results as a Polars dataframe object."""
        data, cols, rowcount = self.rowdict(sql, params)
        return pl.DataFrame(data, infer_schema_length=rowcount, **kwargs)


class CompareUI:
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
    ) -> None:
        """button_list: list of 3-tuples where the first item is the button label,
        the second item is the button's value, and the third (optional) value is the key
        to bind to the button.
        Key identifiers must be in the form taken by the Tk bind() function,
        e.g., "<Return>" and "<Escape>" for those keys, respectively.
        keylist: list of column names that make up a common key for both tables.
        selected_button: integer identifying which button should get the focus (0-based)
        """
        self.headers1 = headers1
        self.rows1 = rows1
        self.headers2 = headers2
        self.rows2 = rows2
        self.keylist = keylist
        self.return_value = None
        self.button_value = None
        self.win = tk.Toplevel()
        self.win.title(title)

        def hl_unmatched(*args):
            """Highlight all rows in both tables that are not matched in the
            other table.

            Create a list of lists of key values for table1.
            """
            keyvals1 = []
            tblitems = self.tbl1.get_children()
            for row_item in tblitems:
                rowdict = dict(
                    zip(self.headers1, self.tbl1.item(row_item)["values"], strict=True),
                )
                keyvals1.append([rowdict[k] for k in self.keylist])
            # Create a list of lists of key values for table2.
            keyvals2 = []
            tblitems = self.tbl2.get_children()
            for row_item in tblitems:
                rowdict = dict(
                    zip(self.headers2, self.tbl2.item(row_item)["values"], strict=True),
                )
                keyvals2.append([rowdict[k] for k in self.keylist])
            # Create a list of only unique key values in common.
            keyvals = []
            for vals in keyvals1:
                if vals in keyvals2 and vals not in keyvals:
                    keyvals.append(vals)
            # Highlight rows in table 1
            tblitems = self.tbl1.get_children()
            for row_item in tblitems:
                self.tbl1.selection_remove(row_item)
                rowdict = dict(
                    zip(self.headers1, self.tbl1.item(row_item)["values"], strict=True),
                )
                rowkeys = [rowdict[k] for k in self.keylist]
                if rowkeys not in keyvals:
                    self.tbl1.selection_add(row_item)
            # Highlight rows in table 2
            tblitems = self.tbl2.get_children()
            for row_item in tblitems:
                self.tbl2.selection_remove(row_item)
                rowdict = dict(
                    zip(self.headers2, self.tbl2.item(row_item)["values"], strict=True),
                )
                rowkeys = [rowdict[k] for k in self.keylist]
                if rowkeys not in keyvals:
                    self.tbl2.selection_add(row_item)

        # The checkbox variable controlling highlighting.
        self.hl_both_var = tk.IntVar()
        self.hl_both_var.set(0)
        controlframe = ttk.Frame(master=self.win, padding="3 3 3 3")
        unmatch_btn = ttk.Button(
            controlframe,
            text="Show mismatches",
            command=hl_unmatched,
        )
        unmatch_btn.grid(column=0, row=1, sticky=tk.W)
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
            if self.hl_both_var.get() == 1:
                find_match(self.tbl1, self.headers1, self.tbl1, self.headers1)

        def match1to2(event: tk.Event):
            """Find the matching row in table 2 for the selected row in table 1."""
            find_match(self.tbl2, self.headers2, self.tbl1, self.headers1)
            if self.hl_both_var.get() == 1:
                find_match(self.tbl2, self.headers2, self.tbl2, self.headers2)

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
            wht = int(m.group(2))
            swd = self.win.winfo_screenwidth()
            sht = self.win.winfo_screenheight()
            xpos = (swd / 2) - (wwd / 2)
            ypos = (sht / 2) - (wht / 2)
            self.win.geometry("%dx%d+%d+%d" % (wwd, wht, xpos, ypos))
        # Limit resizing
        self.win.minsize(width=300, height=0)

    def cancel(self: CompareUI):
        """Cancel the dialog."""
        self.dialog_canceled = True
        self.win.destroy()

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
        # Explicitly delete the Tkinter variable to suppress Tkinter error message.
        self.hl_both_var = None
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
            wht = int(m.group(2))
            swd = self.win.winfo_screenwidth()
            sht = self.win.winfo_screenheight()
            xpos = (swd / 2) - (wwd / 2)
            ypos = (sht / 2) - (wht / 2)
            self.win.geometry("%dx%d+%d+%d" % (wwd, wht, xpos, ypos))
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
        # Explicitly delete the Tkinter variable to suppress Tkinter error message.
        self.hl_both_var = None
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
                    (
                        rowset[i][j]
                        if isinstance(rowset[i][j], str)
                        else str(rowset[i][j])
                    ),
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
        text="    %d rows" % len(rowset),
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
        status_label.config(text="    %d rows" % len(rowset))


def validate_schemas(base_schema: str, stg_schema: str):
    """Validate the base and staging schemas."""
    sql = SQL(
        """
        drop table if exists ups_ctrl_invl_schema cascade;
        select
            string_agg(schemas.schema_name
            || ' ('
            || schema_type
            || ')', '; ' order by schema_type
            ) as schema_string
        into temporary table ups_ctrl_invl_schema
        from
            (
            select
                {base_schema} as schema_name,
                'base' as schema_type
            union
            select

                {stg_schema} as schema_name,
                'staging' as schema_type
            ) as schemas
            left join information_schema.schemata as iss
            on schemas.schema_name=iss.schema_name
        where
            iss.schema_name is null
        having count(*)>0;

    """,
    ).format(
        base_schema=Literal(base_schema),
        stg_schema=Literal(stg_schema),
    )
    if db.execute(sql).rowcount > 0:
        errors.append(
            "Invalid schema(s) specified: {}".format(
                db.dataframe(
                    SQL(
                        "select schema_string from ups_ctrl_invl_schema",
                    ),
                )["schema_string"][0],
            ),
        )
        error_handler(errors)


def validate_table(base_schema: str, stg_schema: str, table: str):
    """Utility script to validate one table in both base and staging schema.

    Halts script processing if any either of the schemas are non-existent,
    or if either of the tables are not present within those schemas pass.
    """
    validate_schemas(base_schema, stg_schema)
    sql = SQL(
        """
        drop table if exists ups_invl_table cascade;
        select string_agg(
                tt.schema_name || '.' || tt.table_name || ' (' || tt.schema_type || ')',
                '; '
                order by tt.schema_name,
                    tt.table_name
            ) as schema_table into temporary table ups_invl_table
        from (
                select {base_schema} as schema_name,
                    'base' as schema_type,
                    {table} as table_name
                union
                select {stg_schema} as schema_name,
                    'staging' as schema_type,
                    {table} as table_name
            ) as tt
            left join information_schema.tables as iss
                on tt.schema_name = iss.table_schema
            and tt.table_name = iss.table_name
        where iss.table_name is null
        having count(*) > 0;
    """,
    ).format(
        base_schema=Literal(base_schema),
        stg_schema=Literal(stg_schema),
        table=Literal(table),
    )
    if db.execute(sql).rowcount > 0:
        errors.append(
            "Invalid table(s) specified: {}".format(
                db.dataframe(SQL("select schema_table from ups_invl_table"))[
                    "schema_table"
                ][0],
            ),
        )

        error_handler(errors)


def validate_control(base_schema: str, stg_schema: str, control_table: str):
    """Validate contents of control table against base and staging schema."""
    validate_schemas(base_schema, stg_schema)
    sql = SQL(
        """
        drop table if exists ups_validate_control cascade;
        select cast({base_schema} as text) as base_schema,
            cast({stg_schema} as text) as staging_schema,
            table_name,
            False as base_exists,
            False as staging_exists into temporary table ups_validate_control
        from {control_table};

        update ups_validate_control as vc
        set base_exists = True
        from information_schema.tables as bt
        where vc.base_schema = bt.table_schema
            and vc.table_name = bt.table_name
            and bt.table_type = cast('BASE TABLE' as text);
        update ups_validate_control as vc
        set staging_exists = True
        from information_schema.tables as st
        where vc.staging_schema = st.table_schema
            and vc.table_name = st.table_name
            and st.table_type = cast('BASE TABLE' as text);
        drop table if exists ups_ctrl_invl_table cascade;
        select string_agg(
                schema_table,
                '; '
                order by it.schema_table
            ) as schema_table into temporary table ups_ctrl_invl_table
        from (
                select base_schema || '.' || table_name as schema_table
                from ups_validate_control
                where not base_exists
                union
                select staging_schema || '.' || table_name as schema_table
                from ups_validate_control
                where not staging_exists
            ) as it
        having count(*) > 0;
    """,
    ).format(
        base_schema=Literal(base_schema),
        stg_schema=Literal(stg_schema),
        control_table=Identifier(control_table),
    )
    if db.execute(sql).rowcount > 0:
        error_handler(
            [
                "Invalid table(s) specified: {}".format(
                    db.dataframe("select schema_table from ups_ctrl_invl_table")[
                        "schema_table"
                    ][0],
                ),
            ],
        )


def staged_to_load(control_table: str, tables):
    """Creates a table having the structure that is used to drive
    the upsert operation on multiple staging tables.
    """
    sql = SQL(
        """
        drop table if exists {control_table} cascade;
        create temporary table {control_table} (
            table_name text not null unique,
            exclude_cols text,
            exclude_null_checks text,
            interactive boolean not null default false,
            null_errors text,
            pk_errors text,
            fk_errors text,
            rows_updated integer,
            rows_inserted integer
        );
        insert into {control_table}
            (table_name)
        select
            trim(unnest(string_to_array({tables}, ',')));
        """,
    ).format(
        control_table=Identifier(control_table),
        tables=Literal(",".join(tables)),
    )
    db.execute(sql)


def load_staging(base_schema: str, stg_schema: str, control_table: str):
    """Performs QA checks for nulls in non-null columns, for duplicated
    primary key values, and for invalid foreign keys in a set of staging
    tables to be loaded into base tables.  If there are failures in the
    QA checks, loading is not attempted.  If the loading step is carried
    out, it is done within a transaction.

    The "null_errors", "pk_errors", and "fk_errors" columns of the
    control table will be updated to identify any errors that occur,
    so that this information is available to the caller.

    The "rows_updated" and "rows_inserted" columns of the control table
    will be updated with counts of the number of rows affected by the
    upsert operation for each table.

    When the upsert operation updates the base table, all columns of the
    base table that are also in the staging table are updated.  The
    update operation does not test to see if column contents are different,
    and so does not update only those values that are different.
    """
    # Clear the columns of return values from the control table,
    # in case this control table has been used previously.
    db.execute(
        SQL(
            """
        update {control_table}
        set null_errors = null,
            pk_errors = null,
            fk_errors = null,
            rows_updated = null,
            rows_inserted = null;
        """,
        ).format(control_table=Identifier(control_table)),
    )
    qa_all(base_schema, stg_schema, control_table)


def qa_all(base_schema: str, stg_schema: str, control_table: str):
    """Conducts null, primary key, and foreign key checks on multiple staging tables
    containing new or revised data for staging tables, using the
    NULLQA_ONE, PKQA_ONE, and FKQA_ONE functions.
    """
    # Create a list of the selected tables with a loop control flag.
    db.execute(
        SQL(
            """
        drop table if exists ups_proctables cascade;
        select tl.table_name,
            tl.exclude_null_checks,
            tl.interactive,
            False::boolean as processed into temporary table ups_proctables
        from {control_table} as tl;
        """,
        ).format(control_table=Identifier(control_table)),
    )
    # Create a view returning a single unprocessed table, in order.
    db.execute(
        SQL(
            """
        drop view if exists ups_toprocess cascade;
        create temporary view ups_toprocess as
        select
            table_name,
            exclude_null_checks,
            interactive
        from ups_proctables
        where not processed
        limit 1;
        """,
        ),
    )
    interactive = db.dataframe("select interactive from ups_toprocess;")["interactive"][
        0
    ]
    # Null checks
    logging.info("")
    qa_check = "Non-NULL"
    logging.info(f"==={qa_check} checks===")
    start_time = datetime.now()
    qa_all_nullloop(base_schema, stg_schema, control_table, interactive)
    logging.debug(f"{qa_check} checks completed in {ellapsed_time(start_time)}")
    logging.info("")

    # Reset the loop control flag.
    db.execute("update ups_proctables set processed = False;")

    qa_check = "Primary Key"
    logging.info(f"==={qa_check} checks===")
    start_time = datetime.now()
    qa_all_pkloop(base_schema, stg_schema, control_table, interactive)
    logging.debug(f"{qa_check} checks completed in {ellapsed_time(start_time)}")
    logging.info("")

    # Reset the loop control flag.
    db.execute("update ups_proctables set processed = False;")

    qa_check = "Foreign Key"
    logging.info(f"==={qa_check} checks===")
    start_time = datetime.now()
    qa_all_fkloop(base_schema, stg_schema, control_table, interactive)
    logging.debug(f"{qa_check} checks completed in {ellapsed_time(start_time)}")
    logging.info("")


def qa_all_nullloop(
    base_schema: str,
    stg_schema: str,
    control_table: str,
    interactive: bool,
):
    null_errors = []
    while True:
        df = db.dataframe(SQL("select * from ups_toprocess;"))
        if df.is_empty():
            break
        null_qa_one(
            base_schema,
            stg_schema,
            table=df["table_name"][0],
            errors=null_errors,
            exclude_null_checks=df["exclude_null_checks"][0],
            interactive=interactive,
        )
        err_df = db.dataframe("select * from ups_null_error_list;")
        if not err_df.is_empty():
            db.execute(
                SQL(
                    """
                update {control_table}
                set null_errors = {null_errors}
                where table_name = {table};
                """,
                ).format(
                    control_table=Identifier(control_table),
                    null_errors=Literal(err_df["null_errors"][0]),
                    table=Literal(df["table_name"][0]),
                ),
            )

        db.execute(
            SQL(
                """update ups_proctables set processed = True
                where table_name = {table_name};""",
            ).format(table_name=Literal(df["table_name"][0])),
        )


def null_qa_one(
    base_schema: str,
    stg_schema: str,
    table: str,
    errors: list,
    exclude_null_checks: str,
    interactive: bool,
):
    logging.info(f"Conducting non-null QA checks on table {stg_schema}.{table}")
    validate_table(base_schema, stg_schema, table)
    # Create a table listing the columns of the base table that must
    # be non-null and that do not have a default expression.
    # Include a column for the number of rows with nulls in the staging table.
    # Include a 'processed' column for loop control.
    db.execute(
        SQL(
            """
        drop table if exists ups_nonnull_cols cascade;
        select column_name,
            0::integer as null_rows,
            False as processed
        into temporary table ups_nonnull_cols
        from information_schema.columns
        where table_schema = {base_schema}
            and table_name = {table}
            and is_nullable = 'NO'
            and column_default is null and column_name not in ({exclude_null_checks});
        """,
        ).format(
            base_schema=Literal(base_schema),
            table=Literal(table),
            exclude_null_checks=(
                SQL(",").join(Literal(col) for col in exclude_null_checks.split(","))
                if exclude_null_checks
                else Literal("")
            ),
        ),
    )

    # Process all non-nullable columns.
    while True:
        df = db.dataframe(
            """
            select column_name
            from ups_nonnull_cols
            where not processed
            limit 1;
            """,
        )
        if df.is_empty():
            break
        db.execute(
            SQL(
                """
            create or replace temporary view ups_qa_nonnull_col as
            select nrows
            from (
                select count(*) as nrows
                from {stg_schema}.{table}
                where {column_name} is null
                ) as nullcount
            where nrows > 0
            limit 1;
            """,
            ).format(
                stg_schema=Identifier(stg_schema),
                table=Identifier(table),
                column_name=Identifier(df["column_name"][0]),
            ),
        )
        null_df = db.dataframe("select * from ups_qa_nonnull_col;")
        if not null_df.is_empty():
            logging.warning(
                f"    Column {df['column_name'][0]} has {null_df['nrows'][0]} null values",  # noqa: E501
            )
            db.execute(
                SQL(
                    """
                update ups_nonnull_cols
                set null_rows = (
                        select nrows
                        from ups_qa_nonnull_col
                        limit 1
                    )
                where column_name = {column_name};
                """,
                ).format(column_name=Literal(df["column_name"][0])),
            )
        db.execute(
            SQL(
                """
            update ups_nonnull_cols
            set processed = True
            where column_name = {column_name};
            """,
            ).format(column_name=Literal(df["column_name"][0])),
        )

    # Update the control table with the number of rows with nulls in the staging table.
    db.execute(
        """
        create or replace temporary view ups_null_error_list as
        select string_agg(column_name || ' (' || null_rows || ')', ', ') as null_errors
        from ups_nonnull_cols
        where coalesce(null_rows, 0) > 0;
    """,
    )


def qa_all_pkloop(
    base_schema: str,
    stg_schema: str,
    control_table: str,
    interactive: bool,
):
    while True:
        df = db.dataframe(SQL("select * from ups_toprocess;"))
        if df.is_empty():
            break
        pk_errors = pk_qa_one(
            base_schema,
            stg_schema,
            table=df["table_name"][0],
            interactive=interactive,
        )
        if pk_errors:
            db.execute(
                SQL(
                    """
                update {control_table}
                set pk_errors = {pk_errors}
                where table_name = {table};
                """,
                ).format(
                    control_table=Identifier(control_table),
                    pk_errors=Literal(pk_errors[0]),
                    table=Literal(df["table_name"][0]),
                ),
            )

        db.execute(
            SQL(
                """update ups_proctables set processed = True where table_name = {table_name};""",  # noqa: E501
            ).format(table_name=Literal(df["table_name"][0])),
        )


def pk_qa_one(base_schema: str, stg_schema: str, table: str, interactive: bool):
    pk_errors = []
    logging.info(f"Conducting primary key QA checks on table {stg_schema}.{table}")
    validate_table(base_schema, stg_schema, table)
    # Create a table of primary key columns on this table
    db.execute(
        SQL(
            """
        drop table if exists ups_primary_key_columns cascade;
        select k.constraint_name, k.column_name, k.ordinal_position
        into temporary table ups_primary_key_columns
        from information_schema.table_constraints as tc
        inner join information_schema.key_column_usage as k
            on tc.constraint_type = 'PRIMARY KEY'
            and tc.constraint_name = k.constraint_name
            and tc.constraint_catalog = k.constraint_catalog
            and tc.constraint_schema = k.constraint_schema
            and tc.table_schema = k.table_schema
            and tc.table_name = k.table_name
            and tc.constraint_name = k.constraint_name
        where
            k.table_name = {table}
            and k.table_schema = {base_schema}
        order by k.ordinal_position
        ;
        """,
        ).format(table=Literal(table), base_schema=Literal(base_schema)),
    )
    df = db.dataframe("select * from ups_primary_key_columns;")
    if df.is_empty():
        return None
    logging.debug(f"  Checking constraint {df['constraint_name'][0]}")
    # Get a comma-delimited list of primary key columns to build SQL selection
    # for duplicate keys
    pkcol_df = db.dataframe(
        """
        select
            string_agg(column_name, ', ' order by ordinal_position) as pkcollist
        from ups_primary_key_columns
        ;
        """,
    )
    pkcollist = pkcol_df["pkcollist"][0]
    db.execute(
        SQL(
            """
            drop view if exists ups_pk_check cascade;
            create temporary view ups_pk_check as
            select {pkcollist}, count(*) as nrows
            from {stg_schema}.{table} as s
            group by {pkcollist}
            having count(*) > 1;
            """,
        ).format(
            pkcollist=SQL(pkcollist),
            stg_schema=Identifier(stg_schema),
            table=Identifier(table),
        ),
    )
    pk_check = db.dataframe("select * from ups_pk_check;")
    if not pk_check.is_empty():
        logging.warning(f"    Duplicate key error in columns {pkcollist}")
        err_df = db.dataframe(
            """
            select count(*) as errcnt, sum(nrows) as total_rows
            from ups_pk_check;
            """,
        )
        pk_errors.append(
            f"{err_df['errcnt'][0]} duplicated keys ({int(err_df['total_rows'][0])} rows) in table {stg_schema}.{table}",  # noqa: E501
        )
        logging.debug("")
        logging.debug(
            tabulate(
                pk_check.iter_rows(),
                headers=pk_check.columns,
                tablefmt="pipe",
                showindex=False,
                colalign=["left"] * len(pk_check.columns),
            ),
        )
        logging.debug("")
        if interactive:
            btn, return_value = TableUI(
                "Duplicate key error",
                f"{err_df['errcnt'][0]} duplicated keys ({int(err_df['total_rows'][0])} rows) in table {stg_schema}.{table}",  # noqa: E501
                [
                    ("Continue", 0, "<Return>"),
                    ("Cancel", 1, "<Escape>"),
                ],
                pk_check.columns,
                list(pk_check.iter_rows()),
            ).activate()
            if btn != 0:
                error_handler(["Script canceled by user."])

    return pk_errors


def qa_all_fkloop(
    base_schema: str,
    stg_schema: str,
    control_table: str,
    interactive: bool,
):
    while True:
        df = db.dataframe(SQL("select * from ups_toprocess;"))
        if df.is_empty():
            break
        fk_errors = fk_qa_one(
            base_schema,
            stg_schema,
            table=df["table_name"][0],
            interactive=interactive,
        )
        if fk_errors:
            db.execute(
                SQL(
                    """
                update {control_table}
                set fk_errors = {fk_errors}
                where table_name = {table};
                """,
                ).format(
                    control_table=Identifier(control_table),
                    fk_errors=Literal(fk_errors),
                    table=Literal(df["table_name"][0]),
                ),
            )

        db.execute(
            SQL(
                """update ups_proctables set processed = True where table_name = {table_name};""",  # noqa: E501
            ).format(table_name=Literal(df["table_name"][0])),
        )


def fk_qa_one(base_schema: str, stg_schema: str, table: str, interactive: bool):
    logging.info(f"Conducting foreign key QA checks on table {stg_schema}.{table}")
    # Create a table of *all* foreign key dependencies in this database.
    # Only create it once because it may slow the QA process down.
    if (
        db.execute(
            SQL(
                """select * from information_schema.tables
                where table_name = {ups_foreign_key_columns};""",
            ).format(ups_foreign_key_columns=Literal("ups_foreign_key_columns")),
        ).rowcount
        == 0
    ):
        db.execute(
            SQL(
                """
            select
                fkinf.constraint_name,
                fkinf.table_schema,
                fkinf.table_name,
                att1.attname as column_name,
                fkinf.uq_schema,
                cls.relname as uq_table,
                att2.attname as uq_column
            into
                temporary table {ups_foreign_key_columns}
            from
            (select
                    ns1.nspname as table_schema,
                    cls.relname as table_name,
                    unnest(cons.conkey) as uq_table_id,
                    unnest(cons.confkey) as table_id,
                    cons.conname as constraint_name,
                    ns2.nspname as uq_schema,
                    cons.confrelid,
                    cons.conrelid
                from
                pg_constraint as cons
                    inner join pg_class as cls on cls.oid = cons.conrelid
                    inner join pg_namespace ns1 on ns1.oid = cls.relnamespace
                    inner join pg_namespace ns2 on ns2.oid = cons.connamespace
                where
                cons.contype = 'f'
            ) as fkinf
            inner join pg_attribute att1 on
                att1.attrelid = fkinf.conrelid and att1.attnum = fkinf.uq_table_id
            inner join pg_attribute att2 on
                att2.attrelid = fkinf.confrelid and att2.attnum = fkinf.table_id
            inner join pg_class cls on cls.oid = fkinf.confrelid;
        """,
            ).format(ups_foreign_key_columns=Identifier("ups_foreign_key_columns")),
        )

    # Create a temporary table of just the foreign key relationships for the base
    # table corresponding to the staging table to check.
    db.execute(
        SQL(
            """
        drop table if exists ups_sel_fks cascade;
        select
            constraint_name, table_schema, table_name,
            column_name, uq_schema, uq_table, uq_column
        into
            temporary table ups_sel_fks
        from
            ups_foreign_key_columns
        where
            table_schema = {base_schema}
            and table_name = {table};
        """,
        ).format(base_schema=Literal(base_schema), table=Literal(table)),
    )
    # Create a temporary table of all unique constraint names for
    # this table, with an integer column to be populated with the
    # number of rows failing the foreign key check, and a 'processed'
    # flag to control looping.
    db.execute(
        SQL(
            """
        drop table if exists ups_fk_constraints cascade;
        select distinct
            constraint_name, table_schema, table_name,
            0::integer as fkerror_values,
            False as processed
        into temporary table ups_fk_constraints
        from ups_sel_fks;
    """,
        ),
    )
    while True:
        # Create a view to select one constraint to process.
        df = db.dataframe(
            SQL(
                """
            select constraint_name, table_schema, table_name
            from ups_fk_constraints
            where not processed
            limit 1;
            """,
            ),
        )
        if df.is_empty():
            break
        logging.debug(
            f"  Checking constraint {df['constraint_name'][0]} for table {table}",
        )
        db.execute(
            SQL(
                """
            drop table if exists ups_one_fk cascade;
            select column_name, uq_schema, uq_table, uq_column
            into temporary table ups_one_fk
            from ups_sel_fks
            where
                constraint_name = {constraint_name}
                and table_schema = {table_schema}
                and table_name = {table_name};
        """,
            ).format(
                constraint_name=Literal(df["constraint_name"][0]),
                table_schema=Literal(df["table_schema"][0]),
                table_name=Literal(df["table_name"][0]),
            ),
        )
        const_df = db.dataframe("select * from ups_one_fk;")
        # Create join expressions from staging table (s) to unique table (u)
        # and to staging table equivalent to unique table (su) (though we
        # don't know yet if the latter exists).  Also create a 'where'
        # condition to ensure that all columns being matched are non-null.
        # Also create a comma-separated list of the columns being checked.
        fk_df = db.dataframe(
            SQL(
                """
        select
          string_agg('s.' || column_name || ' = u.' || uq_column, ' and ') as u_join,
          string_agg('s.' || column_name || ' = su.' || uq_column, ' and ') as su_join,
          string_agg('s.' || column_name || ' is not null', ' and ') as s_not_null,
          string_agg('s.' || column_name, ', ') as s_checked
        from
          (select * from ups_one_fk) as fkcols;
            """,
            ),
        )
        # Determine whether a staging-table equivalent of the unique table exists.
        su_exists = False
        if (
            db.execute(
                SQL(
                    """select * from information_schema.tables
                    where table_name = {table} and table_schema = {stg_schema};""",
                ).format(
                    table=Literal(const_df["uq_table"][0]),
                    stg_schema=Literal(stg_schema),
                ),
            ).rowcount
            > 0
        ):
            su_exists = True

        # Construct a query to test for missing unique values for fk columns.
        query = SQL(
            """
            drop view if exists ups_fk_check cascade;
            create or replace temporary view ups_fk_check as
            select {s_checked}, count(*) as nrows
            from {stg_schema}.{table} as s
            left join {uq_schema}.{uq_table} as u on {u_join}
            """,
        ).format(
            s_checked=SQL(fk_df["s_checked"][0]),
            stg_schema=Identifier(stg_schema),
            table=Identifier(table),
            uq_schema=Identifier(const_df["uq_schema"][0]),
            uq_table=Identifier(const_df["uq_table"][0]),
            u_join=SQL(fk_df["u_join"][0]),
        )
        if su_exists:
            query += SQL(
                """ left join {stg_schema}.{uq_table} as su on {su_join}""",
            ).format(
                stg_schema=Identifier(stg_schema),
                uq_table=Identifier(const_df["uq_table"][0]),
                su_join=SQL(fk_df["su_join"][0]),
            )
        query += SQL(" where u.{uq_column} is null").format(
            uq_column=Identifier(const_df["uq_column"][0]),
        )
        if su_exists:
            query += SQL(" and su.{uq_column} is null").format(
                uq_column=Identifier(const_df["uq_column"][0]),
            )
        query += SQL(
            """ and {s_not_null}
                group by {s_checked};""",
        ).format(
            s_not_null=SQL(fk_df["s_not_null"][0]),
            s_checked=SQL(fk_df["s_checked"][0]),
        )

        db.execute(query)
        fk_check_df = db.dataframe("select * from ups_fk_check;")

        if not fk_check_df.is_empty():
            logging.warning(
                f"    Foreign key error referencing {const_df['uq_table'][0]}",
            )
            if interactive:
                btn, return_value = TableUI(
                    "Foreign Key Error",
                    f"Foreign key error referencing {const_df['uq_table'][0]}",
                    [
                        ("Continue", 0, "<Return>"),
                        ("Cancel", 1, "<Escape>"),
                    ],
                    fk_check_df.columns,
                    list(fk_check_df.iter_rows()),
                ).activate()
                if btn != 0:
                    error_handler(["Script canceled by user."])
            logging.debug("")
            logging.debug(
                tabulate(
                    fk_check_df.iter_rows(),
                    headers=fk_check_df.columns,
                    tablefmt="pipe",
                    showindex=False,
                    colalign=["left"] * len(fk_check_df.columns),
                ),
            )
            logging.debug("")

            db.execute(
                SQL(
                    """
                update ups_fk_constraints
                set fkerror_values = {fkerror_count}
                where constraint_name = {constraint_name}
                    and table_schema = {table_schema}
                    and table_name = {table_name};
                """,
                ).format(
                    fkerror_count=Literal(fk_check_df["nrows"][0]),
                    constraint_name=Literal(df["constraint_name"][0]),
                    table_schema=Literal(df["table_schema"][0]),
                    table_name=Literal(df["table_name"][0]),
                ),
            )
        db.execute(
            SQL(
                """
            update ups_fk_constraints
            set processed = True
            where
                constraint_name = {constraint_name}
                and table_schema = {table_schema}
                and table_name = {table_name};
                       """,
            ).format(
                constraint_name=Literal(df["constraint_name"][0]),
                table_schema=Literal(df["table_schema"][0]),
                table_name=Literal(df["table_name"][0]),
            ),
        )

    err_df = db.dataframe(
        SQL(
            """
        select string_agg(
            constraint_name || ' (' || fkerror_values || ')', ', '
            ) as fk_errors
        from ups_fk_constraints
        where coalesce(fkerror_values, 0) > 0;
        """,
        ),
    )
    return err_df["fk_errors"][0]


def upsert_all(
    base_schema: str,
    stg_schema: str,
    control_table: str,
    upsert_method: str,
):
    validate_control(base_schema, stg_schema, control_table)

    # Get a table of all dependencies for the base schema.
    db.execute(
        SQL(
            """
    drop table if exists ups_dependencies cascade;
    create temporary table ups_dependencies as
    select
        tc.table_name as child,
        tu.table_name as parent
    from
        information_schema.table_constraints as tc
        inner join information_schema.constraint_table_usage as tu
            on tu.constraint_name = tc.constraint_name
    where
        tc.constraint_type = 'FOREIGN KEY'
        and tc.table_name <> tu.table_name
        and tc.table_schema = {base_schema};
    """,
        ).format(base_schema=Literal(base_schema)),
    )

    # Create a list of tables in the base schema ordered by dependency.
    db.execute(
        SQL(
            """
    drop table if exists ups_ordered_tables cascade;
    with recursive dep_depth as (
        select
            dep.child as first_child,
            dep.child,
            dep.parent,
            1 as lvl
        from
            ups_dependencies as dep
        union all
        select
            dd.first_child,
            dep.child,
            dep.parent,
            dd.lvl + 1 as lvl
        from
            dep_depth as dd
            inner join ups_dependencies as dep on dep.parent = dd.child
                and dep.child <> dd.parent
                and not (dep.parent = dd.first_child and dd.lvl > 2)
        )
    select
        table_name,
        table_order
    into
        temporary table ups_ordered_tables
    from (
        select
            dd.parent as table_name,
            max(lvl) as table_order
        from
            dep_depth as dd
        group by
            table_name
        union
        select
            dd.child as table_name,
            max(lvl) + 1 as level
        from
            dep_depth as dd
            left join ups_dependencies as dp on dp.parent = dd.child
        where
            dp.parent is null
        group by
            dd.child
        union
        select distinct
            t.table_name,
            0 as level
        from
            information_schema.tables as t
            left join ups_dependencies as p on t.table_name=p.parent
            left join ups_dependencies as c on t.table_name=c.child
        where
            t.table_schema = {base_schema}
            and t.table_type = 'BASE TABLE'
            and p.parent is null
            and c.child is null
        ) as all_levels;
    """,
        ).format(base_schema=Literal(base_schema)),
    )

    # Create a list of the selected tables with ordering information.
    db.execute(
        SQL(
            """
    drop table if exists ups_proctables cascade;
    select
        ot.table_order,
        tl.table_name,
        tl.exclude_cols,
        tl.interactive,
        tl.rows_updated,
        tl.rows_inserted,
        False::boolean as processed
    into
        temporary table ups_proctables
    from
        {control_table} as tl
        inner join ups_ordered_tables as ot on ot.table_name = tl.table_name
        ;
    """,
        ).format(control_table=Identifier(control_table)),
    )

    while True:
        # Create a view returning a single unprocessed table, in order.
        proc_df = db.dataframe(
            SQL(
                """
            select
                table_name, exclude_cols, interactive,
                rows_updated, rows_inserted
            from ups_proctables
            where not processed
            order by table_order
            limit 1;
            """,
            ),
        )
        if proc_df.is_empty():
            break

        rows_updated, rows_inserted = upsert_one(
            base_schema,
            stg_schema,
            upsert_method,
            proc_df["table_name"][0],
            proc_df["exclude_cols"][0].split(",") if proc_df["exclude_cols"][0] else [],
            proc_df["interactive"][0],
        )

        db.execute(
            SQL(
                """
            update ups_proctables
            set rows_updated = {rows_updated},
                rows_inserted = {rows_inserted}
            where table_name = {table_name};
            """,
            ).format(
                rows_updated=Literal(rows_updated),
                rows_inserted=Literal(rows_inserted),
                table_name=Literal(proc_df["table_name"][0]),
            ),
        )

        db.execute(
            SQL(
                """
                update ups_proctables
                set processed = True
                where table_name = {table_name};
                """,
            ).format(table_name=Literal(proc_df["table_name"][0])),
        )

    # Move the update/insert counts back into the control table.
    db.execute(
        SQL(
            """
        update {control_table} as ct
        set
            rows_updated = pt.rows_updated,
            rows_inserted = pt.rows_inserted
        from
            ups_proctables as pt
        where
            pt.table_name = ct.table_name;
        """,
        ).format(control_table=Identifier(control_table)),
    )


def upsert_one(
    base_schema: str,
    stg_schema: str,
    upsert_method: str,
    table: str,
    exclude_cols: list[str],
    interactive: bool = False,
):
    rows_updated = 0
    rows_inserted = 0

    logging.info(f"Performing upsert on table {base_schema}.{table}")
    validate_table(base_schema, stg_schema, table)

    # Populate a (temporary) table with the names of the columns
    # in the base table that are to be updated from the staging table.
    # Include only those columns from staging table that are also in base table.
    # db.execute(
    query = SQL(
        """
        drop table if exists ups_cols cascade;
        select s.column_name
        into temporary table ups_cols
        from information_schema.columns as s
            inner join information_schema.columns as b on s.column_name=b.column_name
        where
            s.table_schema = {stg_schema}
            and s.table_name = {table}
            and b.table_schema = {base_schema}
            and b.table_name = {table}
        """,
    ).format(
        stg_schema=Literal(stg_schema),
        table=Literal(table),
        base_schema=Literal(base_schema),
    )
    if exclude_cols:
        query += SQL(
            """
            and s.column_name not in ({exclude_cols})
            """,
        ).format(
            exclude_cols=SQL(",").join(Literal(col) for col in exclude_cols),
        )
    query += SQL(" order by s.ordinal_position;")
    db.execute(query)

    # Populate a (temporary) table with the names of the primary key
    # columns of the base table.
    db.execute(
        SQL(
            """
        drop table if exists ups_pks cascade;
        select k.column_name
        into temporary table ups_pks
        from information_schema.table_constraints as tc
        inner join information_schema.key_column_usage as k
            on tc.constraint_type = 'PRIMARY KEY'
            and tc.constraint_name = k.constraint_name
            and tc.constraint_catalog = k.constraint_catalog
            and tc.constraint_schema = k.constraint_schema
            and tc.table_schema = k.table_schema
            and tc.table_name = k.table_name
            and tc.constraint_name = k.constraint_name
        where
            k.table_name = {table}
            and k.table_schema = {base_schema}
        order by k.ordinal_position;
        """,
        ).format(table=Literal(table), base_schema=Literal(base_schema)),
    )

    # Get all base table columns that are to be updated into a comma-delimited list.
    all_col_list = db.dataframe(
        SQL(
            """
        select string_agg(column_name, ', ') as cols from ups_cols;""",
        ),
    )["cols"][0]

    # Get all base table columns that are to be updated into a
    # comma-delimited list with a "b." prefix.
    base_col_list = db.dataframe(
        SQL(
            """
        select string_agg('b.' || column_name, ', ') as cols
        from ups_cols;""",
        ),
    )["cols"][0]

    # Get all staging table column names for columns that are to be updated
    # into a comma-delimited list with an "s." prefix.
    stg_col_list = db.dataframe(
        SQL(
            """
        select string_agg('s.' || column_name, ', ') as cols
        from ups_cols;""",
        ),
    )["cols"][0]

    # Get the primary key columns in a comma-delimited list.
    pk_col_list = db.dataframe(
        SQL(
            """
        select string_agg(column_name, ', ') as cols
        from ups_pks;""",
        ),
    )["cols"][0]

    # Create a join expression for key columns of the base (b) and
    # staging (s) tables.
    join_expr = db.dataframe(
        SQL(
            """
        select
            string_agg('b.' || column_name || ' = s.' || column_name, ' and ') as expr
        from
            ups_pks;
        """,
        ),
    )["expr"][0]

    # Create a FROM clause for an inner join between base and staging
    # tables on the primary key column(s).
    from_clause = SQL(
        """FROM {base_schema}.{table} as b
        INNER JOIN {stg_schema}.{table} as s ON {join_expr}""",
    ).format(
        base_schema=Identifier(base_schema),
        table=Identifier(table),
        stg_schema=Identifier(stg_schema),
        join_expr=SQL(join_expr),
    )

    # Create SELECT queries to pull all columns with matching keys from both
    # base and staging tables.
    db.execute(
        SQL(
            """
        drop view if exists ups_basematches cascade;
        create temporary view ups_basematches as select {base_col_list} {from_clause};

        drop view if exists ups_stgmatches cascade;
        create temporary view ups_stgmatches as select {stg_col_list} {from_clause};
        """,
        ).format(
            base_col_list=SQL(base_col_list),
            stg_col_list=SQL(stg_col_list),
            from_clause=from_clause,
        ),
    )
    # Get non-key columns to be updated
    db.execute(
        SQL(
            """
        drop view if exists ups_nk cascade;
        create temporary view ups_nk as
        select column_name from ups_cols
        except
        select column_name from ups_pks;
        """,
        ),
    )
    # Prompt user to examine matching data and commit, don't commit, or quit.

    do_updates = False
    update_stmt = None
    # if not stg_df.is_empty() and not nk_df.is_empty():
    if upsert_method in ("upsert", "update"):
        stg_curs = db.execute("select * from ups_stgmatches;")
        stg_cols = [col.name for col in stg_curs.description]
        stg_rowcount = stg_curs.rowcount
        stg_data = stg_curs.fetchall()
        nk_curs = db.execute("select * from ups_nk;")
        # nk_cols = [col.name for col in nk_curs.description]
        nk_rowcount = nk_curs.rowcount
        # nk_data = nk_curs.fetchall()
        if stg_rowcount > 0 and nk_rowcount > 0:
            base_curs = db.execute("select * from ups_basematches;")
            base_cols = [col.name for col in base_curs.description]
            # base_rowcount = base_curs.rowcount
            base_data = base_curs.fetchall()

            if interactive:
                btn, return_value = CompareUI(
                    "Compare Tables",
                    f"Do you want to make these changes? For table {table}, new data are shown in the top table; existing data are shown in the bottom table.",  # noqa: E501
                    [
                        ("Continue", 0, "<Return>"),
                        ("Skip", 1, "<Escape>"),
                        ("Cancel", 2, "<Escape>"),
                    ],
                    stg_cols,
                    stg_data,
                    base_cols,
                    base_data,
                    pk_col_list.split(", "),
                    sidebyside=False,
                ).activate()
            else:
                btn = 0
            if btn == 2:
                error_handler(["Upsert cancelled"])
            if btn == 0:
                do_updates = True
                # Create an assignment expression to update non-key columns of the
                # base table (un-aliased) from columns of the staging table (as s).
                ups_expr = db.dataframe(
                    SQL(
                        """
                    select string_agg(
                        column_name || ' = s.' || column_name, ', '
                        ) as col
                    from ups_nk;
                    """,
                    ),
                )["col"][0]
                # Create an UPDATE statement to update the base table with
                # non-key columns from the staging table.
                # No semicolon terminating generated SQL.
                update_stmt = SQL(
                    """
                    UPDATE {base_schema}.{table} as b
                    SET {ups_expr}
                    FROM {stg_schema}.{table} as s WHERE {join_expr}
                """,
                ).format(
                    base_schema=Identifier(base_schema),
                    table=Identifier(table),
                    stg_schema=Identifier(stg_schema),
                    ups_expr=SQL(ups_expr),
                    join_expr=SQL(join_expr),
                )
        else:
            logging.debug("  No data to update")

    do_inserts = False
    insert_stmt = None
    if upsert_method in ("upsert", "insert"):
        # Create a select statement to find all rows of the staging table
        # that are not in the base table.
        db.execute(
            SQL(
                """
            drop view if exists ups_newrows cascade;
            create temporary view ups_newrows as with newpks as (
                select {pk_col_list}
                from {stg_schema}.{table}
                except
                select {pk_col_list}
                from {base_schema}.{table}
            )
            select s.*
            from {stg_schema}.{table} as s
                inner join newpks using ({pk_col_list});
            """,
            ).format(
                stg_schema=Identifier(stg_schema),
                table=Identifier(table),
                pk_col_list=SQL(pk_col_list),
                base_schema=Identifier(base_schema),
            ),
        )
        # Prompt user to examine new data and continue or quit.
        # new_df = db.dataframe("select * from ups_newrows;")
        new_curs = db.execute("select * from ups_newrows;")
        new_cols = [col.name for col in new_curs.description]
        new_rowcount = new_curs.rowcount
        new_data = new_curs.fetchall()

        # if not new_df.is_empty():
        if new_rowcount > 0:
            if interactive:
                btn, return_value = TableUI(
                    "New Data",
                    f"Do you want to add these new data to the {base_schema}.{table} table?",  # noqa: E501
                    [
                        ("Continue", 0, "<Return>"),
                        ("Skip", 1, "<Escape>"),
                        ("Cancel", 2, "<Escape>"),
                    ],
                    # new_df.columns,
                    new_cols,
                    # list(new_df.iter_rows()),
                    new_data,
                ).activate()
            else:
                btn = 0
            if btn == 2:
                error_handler(["Upsert cancelled"])
            if btn == 0:
                do_inserts = True
                # Create an insert statement.  No semicolon terminating generated SQL.
                insert_stmt = SQL(
                    """
                    INSERT INTO {base_schema}.{table} ({all_col_list})
                    SELECT {all_col_list} FROM ups_newrows
                """,
                ).format(
                    base_schema=Identifier(base_schema),
                    table=Identifier(table),
                    all_col_list=SQL(all_col_list),
                )
        else:
            logging.debug("  No new data to insert")

    # Run the update and insert statements.
    if do_updates and update_stmt and upsert_method in ("upsert", "update"):
        logging.info(f"  Updating {base_schema}.{table}")
        logging.debug(f"    UPDATE statement for {base_schema}.{table}")
        logging.debug(f"{update_stmt.as_string(db.conn)}")
        db.execute(update_stmt)
        rows_updated = stg_rowcount
        logging.info(f"    {rows_updated} rows updated")
    if do_inserts and insert_stmt and upsert_method in ("upsert", "insert"):
        logging.info(f"  Adding data to {base_schema}.{table}")
        logging.debug(f"    INSERT statement for {base_schema}.{table}")
        logging.debug(f"{insert_stmt.as_string(db.conn)}")
        db.execute(insert_stmt)
        rows_inserted = new_rowcount
        logging.info(f"    {rows_inserted} rows inserted")
    return rows_updated, rows_inserted


def error_handler(errors: list[str]):
    """Log errors and exit."""
    for error in errors:
        logging.error(error)
    if errors:
        db.rollback()
        sys.exit(1)


def ellapsed_time(start_time: datetime):
    """Returns a string representing the ellapsed time since the start time."""
    dt = (datetime.now() - start_time).total_seconds()
    if dt < 60:
        return f"{round((datetime.now() - start_time).total_seconds(), 3)} seconds"
    if dt < 3600:
        return f"{int(dt // 60)} minutes, {round(dt % 60, 3)} seconds"
    return f"{int(dt // 3600)} hours, {int((dt % 3600)) // 60} minutes, {round(dt % 60, 3)} seconds"  # noqa: E501 UP034


def clparser() -> argparse.ArgumentParser:
    """Command line interface for the upsert function."""
    parser = argparse.ArgumentParser(
        description=description_short,
        epilog=description_long,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="suppress all console output",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="display debug output",
    )
    parser.add_argument(
        "-l",
        "--log",
        metavar="LOGFILE",
        help="write log to LOGFILE",
    )
    parser.add_argument(
        "-e",
        "--exclude",
        metavar="EXCLUDE_COLUMNS",
        help="comma-separated list of columns to exclude from null checks",
    )
    parser.add_argument(
        "-n",
        "--null",
        metavar="NULL_COLUMNS",
        help="comma-separated list of columns to exclude from null checks",
    )
    parser.add_argument(
        "-c",
        "--commit",
        action="store_true",
        help="commit changes to database",
    )
    parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="display interactive GUI of important table information",
    )
    parser.add_argument(
        "-m",
        "--method",
        metavar="METHOD",
        choices=["upsert", "update", "insert"],
        help="method to use for upsert",
    )
    parser.add_argument(
        "host",
        metavar="HOST",
        help="database host",
    )
    parser.add_argument(
        "database",
        metavar="DATABASE",
        help="database name",
    )
    parser.add_argument(
        "user",
        metavar="USER",
        help="database user",
    )
    parser.add_argument(
        "stg_schema",
        metavar="STAGING_SCHEMA",
        help="staging schema name",
    )
    parser.add_argument(
        "base_schema",
        metavar="BASE_SCHEMA",
        help="base schema name",
    )
    parser.add_argument(
        "tables",
        metavar="TABLE",
        nargs="+",
        help="table name(s)",
    )
    return parser


def upsert(
    host: str,
    database: str,
    user: str,
    tables: list[str],
    stg_schema: str,
    base_schema: str,
    upsert_method: str = "upsert",
    commit: bool = False,
    interactive: bool = False,
    exclude_cols: list[str] | None = None,
    exclude_null_check_columns: list[str] | None = None,
    **kwargs,
):
    """Upsert staging tables to base tables."""
    if exclude_null_check_columns is None:
        exclude_null_check_columns = []
    if exclude_cols is None:
        exclude_cols = []
    global db
    global errors
    global control_table
    global timer

    errors = []
    control_table = "ups_control"
    timer = datetime.now()
    logging.debug(f"Starting upsert at {timer.strftime('%Y-%m-%d %H:%M:%S')}")

    db = PostgresDB(
        host=host,
        database=database,
        user=user,
        passwd=kwargs.get("passwd", None),
    )
    logging.debug(f"Connected to {db}")

    validate_schemas(base_schema, stg_schema)
    for table in tables:
        validate_table(base_schema, stg_schema, table)

    logging.info(f"Upserting to {base_schema} from {stg_schema}")
    if interactive:
        btn, return_value = TableUI(
            "Upsert Tables",
            "Tables selected for upsert",
            [
                ("Continue", 0, "<Return>"),
                ("Cancel", 1, "<Escape>"),
            ],
            ["Table"],
            [[table] for table in tables],
        ).activate()
        if btn != 0:
            error_handler(["Script canceled by user."])
    else:
        logging.info("Tables selected for upsert:")
        for table in tables:
            logging.info(f"  {table}")

    # Initialize the control table
    logging.debug("Initializing control table")
    staged_to_load(control_table, tables)

    # Update the control table with the list of columns to exclude from null checks
    if exclude_cols:
        db.execute(
            SQL(
                """
                update {control_table}
                set exclude_cols = {exclude_cols};
            """,
            ).format(
                control_table=Identifier(control_table),
                exclude_cols=Literal(",".join(exclude_cols)),
            ),
        )
    if exclude_null_check_columns:
        db.execute(
            SQL(
                """
                update {control_table}
                set exclude_null_checks = {exclude_null_check_columns};
            """,
            ).format(
                control_table=Identifier(control_table),
                exclude_null_check_columns=Literal(
                    ",".join(exclude_null_check_columns),
                ),
            ),
        )
    if interactive:
        db.execute(
            SQL(
                """
                update {control_table}
                set interactive = {interactive};
            """,
            ).format(
                control_table=Identifier(control_table),
                interactive=Literal(interactive),
            ),
        )

    # Run not-null, primary key, and foreign key QA checks on the staging tables
    load_staging(base_schema, stg_schema, control_table)

    ctrl_df = db.dataframe(
        SQL(
            """
            select * from {control_table}
            where
                null_errors is not null
                or pk_errors is not null
                or fk_errors is not null;
            """,
        ).format(control_table=Identifier(control_table)),
    )

    qa_pass = False
    # if errors in control table
    if not ctrl_df.is_empty():
        if interactive:
            btn, return_value = TableUI(
                "QA Errors",
                "Below is a summary of errors.",
                [
                    ("Continue", 0, "<Return>"),
                    ("Cancel", 1, "<Escape>"),
                ],
                ctrl_df.columns,
                list(ctrl_df.iter_rows()),
            ).activate()
        error_handler(["QA checks failed. Aborting upsert."])
    else:
        qa_pass = True
        logging.info("===QA checks passed. Starting upsert===")

    if qa_pass:
        upsert_all(base_schema, stg_schema, control_table, upsert_method)

    final_ctrl_df = db.dataframe(
        SQL("select * from {control_table};").format(
            control_table=Identifier(control_table),
        ),
    )

    if interactive:
        btn, return_value = TableUI(
            "Upsert Summary",
            "Below is a summary of changes. Do you want to commit these changes? ",
            [
                ("Continue", 0, "<Return>"),
                ("Cancel", 1, "<Escape>"),
            ],
            final_ctrl_df.columns,
            list(final_ctrl_df.iter_rows()),
        ).activate()
    else:
        btn = 0

    logging.info("")

    if btn == 0:
        if final_ctrl_df.filter(
            (pl.col("rows_updated") > 0) | (pl.col("rows_inserted") > 0),
        ).is_empty():
            logging.info("No changes to commit")
            db.rollback()
        else:
            if commit:
                logging.info("Changes committed")
                db.commit()
            else:
                logging.info(
                    f"Commit set to {str(commit).upper()}, rolling back changes",
                )
                db.rollback()
    else:
        logging.info("Rolling back changes")
        db.rollback()

    logging.debug(f"Upsert completed in {ellapsed_time(timer)}")


if __name__ == "__main__":
    args = clparser().parse_args()
    logging.basicConfig(
        level=logging.INFO if not args.debug else logging.DEBUG,
        format="%(message)s",
        handlers=[
            logging.StreamHandler() if not args.quiet else logging.NullHandler(),
            logging.FileHandler(Path(args.log)) if args.log else logging.NullHandler(),
        ],
    )
    upsert(
        host=args.host,
        database=args.database,
        user=args.user,
        tables=args.tables,
        stg_schema=args.stg_schema,
        base_schema=args.base_schema,
        commit=args.commit,
        upsert_method=args.method,
        interactive=args.interactive,
        exclude_cols=args.exclude.split(",") if args.exclude else None,
        exclude_null_check_columns=args.null.split(",") if args.null else None,
    )
