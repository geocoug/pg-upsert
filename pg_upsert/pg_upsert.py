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

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.sql import SQL, Composable, Identifier, Literal
from tabulate import tabulate

from ._version import __version__

description = "Run not-NULL, Primary Key, Foreign Key, and Check Constraint checks on staging tables then update and insert (upsert) data from staging tables to base tables."

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[logging.NullHandler()],
)
logger = logging.getLogger(__name__)

# Get the __version__ from the __init__.py file.


class PostgresDB:
    """Base database object."""

    def __init__(
        self: PostgresDB,
        host: str,
        database: str,
        user: str,
        port: int = 5432,
        passwd: None | str = None,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        if passwd is not None:
            self.passwd = passwd
        else:
            self.passwd = self.get_password()
        self.in_transaction = False
        self.encoding = "UTF8"
        self.conn = None
        if not self.valid_connection():
            raise psycopg2.Error(f"Error connecting to {self!s}")

    def __repr__(self: PostgresDB) -> str:
        return (
            f"{self.__class__.__name__}(host={self.host}, port={self.port}, database={self.database}, user={self.user})"  # noqa: E501
        )

    def __del__(self: PostgresDB) -> None:
        """Delete the instance."""
        self.close()

    def get_password(self):
        try:
            return getpass.getpass(
                f"The script {Path(__file__).name} wants the password for {self!s}: ",
            )
        except (KeyboardInterrupt, EOFError) as err:
            raise err

    def valid_connection(self: PostgresDB) -> bool:
        """Test the database connection."""
        logger.debug(f"Testing connection to {self!s}")
        try:
            self.open_db()
            return True
        except psycopg2.Error:
            return False
        finally:
            self.close()

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
                logger.debug(f"\n{sql.as_string(curs)}")
                curs.execute(sql)
            else:
                if params is None:
                    logger.debug(f"\n{sql}")
                    curs.execute(sql.encode(self.encoding))
                else:
                    logger.debug(f"\nSQL:\n{sql}\nParameters:\n{params}")
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
                    r = [(c.decode(self.encoding, "backslashreplace") if isinstance(c, bytes) else c) for c in row]
                else:
                    r = row
                return dict(zip(headers, r, strict=True))
            return None

        return (iter(dict_row, None), headers, curs.rowcount)


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


# def error_handler(errors: list[str]):
#     """Log errors and exit."""
#     for error in errors:
#         logger.error(error)
#     if errors:
#         db.rollback()
#         sys.exit(1)


def ellapsed_time(start_time: datetime):
    """Returns a string representing the ellapsed time since the start time."""
    dt = (datetime.now() - start_time).total_seconds()
    if dt < 60:
        return f"{round((datetime.now() - start_time).total_seconds(), 3)} seconds"
    if dt < 3600:
        return f"{int(dt // 60)} minutes, {round(dt % 60, 3)} seconds"
    return f"{int(dt // 3600)} hours, {int((dt % 3600)) // 60} minutes, {round(dt % 60, 3)} seconds"  # noqa: UP034


def clparser() -> argparse.ArgumentParser:
    """Command line interface for the upsert function."""
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
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
        type=Path,
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
        "--do-commit",
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
        "--upsert-method",
        metavar="UPSERT_METHOD",
        default="upsert",
        choices=["upsert", "update", "insert"],
        help="method to use for upsert",
    )
    parser.add_argument(
        "host",
        metavar="HOST",
        help="database host",
    )
    parser.add_argument(
        "port",
        metavar="PORT",
        type=int,
        help="database port",
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


class PgUpsert:
    UPSERT_METHODS = ("upsert", "update", "insert")

    def __init__(
        self,
        host: str,
        database: str,
        user: str,
        port: int = 5432,
        passwd: None | str = None,
        tables: list | tuple | None = (),
        stg_schema: str | None = None,
        base_schema: str | None = None,
        do_commit: bool = False,
        interactive: bool = False,
        upsert_method: str = "upsert",
        exclude_cols: list | tuple | None = (),
        exclude_null_check_columns: list | tuple | None = (),
        **kwargs,
    ):
        if upsert_method not in self.UPSERT_METHODS:
            raise ValueError(
                f"Invalid upsert method: {upsert_method}. Must be one of {self.UPSERT_METHODS}",
            )
        if not base_schema:
            raise ValueError("No base schema specified")
        if not stg_schema:
            raise ValueError("No staging schema specified")
        if not tables:
            raise ValueError("No tables specified")
        if stg_schema == base_schema:
            raise ValueError(
                f"Staging and base schemas must be different. Got {stg_schema} for both.",
            )
        self.db = PostgresDB(
            host=host,
            port=port,
            database=database,
            user=user,
            passwd=passwd,
        )
        logger.debug(f"Connected to {self.db!s}")
        self.tables = tables
        self.stg_schema = stg_schema
        self.base_schema = base_schema
        self.do_commit = do_commit
        self.interactive = interactive
        self.upsert_method = upsert_method
        self.exclude_cols = exclude_cols
        self.exclude_null_check_columns = exclude_null_check_columns
        self.control_table = kwargs.get("control_table", "ups_control")
        self.qa_passed = False

    def __repr__(self):
        return f"{self.__class__.__name__}(db={self.db!r}, tables={self.tables}, stg_schema={self.stg_schema}, base_schema={self.base_schema}, do_commit={self.do_commit}, interactive={self.interactive}, upsert_method={self.upsert_method}, exclude_cols={self.exclude_cols}, exclude_null_check_columns={self.exclude_null_check_columns})"  # noqa: E501

    def __del__(self):
        self.db.close()

    def show_results(self, sql: str | Composable) -> None | str:
        """Display the results of a query in a table format. If the interactive flag is set,
        the results will be displayed in a Tkinter window. Otherwise, the results will be
        displayed in the console using the tabulate module."""
        rows, headers, rowcount = self.db.rowdict(sql)
        if rowcount == 0:
            logger.info("No results found")
            return None
        return f"{tabulate(rows, headers='keys', tablefmt='pipe', showindex=False)}"

    def run(self) -> None:
        """Run the upsert process."""
        self.validate_schemas()
        for table in self.tables:
            self.validate_table(table)
        logger.info(f"Upserting to {self.base_schema} from {self.stg_schema}")
        if self.interactive:
            logger.debug("Tables selected for upsert:")
            for table in self.tables:
                logger.debug(f"  {table}")
            btn, return_value = TableUI(
                "Upsert Tables",
                "Tables selected for upsert",
                [
                    ("Continue", 0, "<Return>"),
                    ("Cancel", 1, "<Escape>"),
                ],
                ["Table"],
                [[table] for table in self.tables],
            ).activate()
            if btn != 0:
                logger.info("Upsert cancelled")
                return
        else:
            logger.info("Tables selected for upsert:")
            for table in self.tables:
                logger.info(f"  {table}")
        self.init_ups_control()
        self.qa_all()
        if self.qa_passed:
            self.upsert_all()

        self.db.close()

    def validate_schemas(self: PgUpsert) -> None:
        """Validate that the base and staging schemas exist.

        Raises:
            ValueError: If either schema does not exist.
        """
        logger.debug(f"Validating schemas {self.base_schema} and {self.stg_schema}")
        sql = SQL(
            """
            select
                string_agg(schemas.schema_name
                || ' ('
                || schema_type
                || ')', '; ' order by schema_type
                ) as schema_string
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
            base_schema=Literal(self.base_schema),
            stg_schema=Literal(self.stg_schema),
        )
        if self.db.execute(sql).rowcount > 0:
            raise ValueError(
                f"Invalid schema(s): {next(iter(self.db.rowdict(sql)[0]))['schema_string']}",
            )

    def validate_table(self, table: str) -> None:
        """Utility script to validate one table in both base and staging schema.

        Halts script processing if any either of the schemas are non-existent,
        or if either of the tables are not present within those schemas pass.

        Args:
            table (str): The table name to validate.

        Raises:
            ValueError: If the table does not exist in either the base or staging schema.
        """
        logger.debug(
            f"Validating table {table} exists in {self.base_schema} and {self.stg_schema} schemas",
        )
        sql = SQL(
            """
            select string_agg(
                    tt.schema_name || '.' || tt.table_name || ' (' || tt.schema_type || ')',
                    '; '
                    order by tt.schema_name,
                        tt.table_name
                ) as schema_table
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
            base_schema=Literal(self.base_schema),
            stg_schema=Literal(self.stg_schema),
            table=Literal(table),
        )
        if self.db.execute(sql).rowcount > 0:
            raise ValueError(
                f"Invalid table(s): {next(iter(self.db.rowdict(sql)[0]))['schema_table']}",
            )

    def validate_control(self: PgUpsert) -> None:
        """Validate contents of control table against base and staging schema."""
        logger.debug("Validating control table")
        self.validate_schemas()
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
            base_schema=Literal(self.base_schema),
            stg_schema=Literal(self.stg_schema),
            control_table=Identifier(self.control_table),
        )
        if self.db.execute(sql).rowcount > 0:
            logger.error("Invalid table(s) specified:")
            rows, headers, rowcount = self.db.rowdict(
                SQL("select schema_table from ups_ctrl_invl_table"),
            )
            for row in rows:
                logger.error(f"  {row['schema_table']}")

    def init_ups_control(self: PgUpsert) -> None:
        """Creates a table having the structure that is used to drive
        the upsert operation on multiple staging tables.
        """
        logger.debug("Initializing upsert control table")
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
                ck_errors text,
                rows_updated integer,
                rows_inserted integer
            );
            insert into {control_table}
                (table_name)
            select
                trim(unnest(string_to_array({tables}, ',')));
            """,
        ).format(
            control_table=Identifier(self.control_table),
            tables=Literal(",".join(self.tables)),
        )
        self.db.execute(sql)
        # Update the control table with the list of columns to exclude from null checks
        if self.exclude_cols and len(self.exclude_cols) > 0:
            self.db.execute(
                SQL(
                    """
                    update {control_table}
                    set exclude_cols = {exclude_cols};
                """,
                ).format(
                    control_table=Identifier(self.control_table),
                    exclude_cols=Literal(",".join(self.exclude_cols)),
                ),
            )
        if self.exclude_null_check_columns and len(self.exclude_null_check_columns) > 0:
            self.db.execute(
                SQL(
                    """
                    update {control_table}
                    set exclude_null_checks = {exclude_null_check_columns};
                """,
                ).format(
                    control_table=Identifier(self.control_table),
                    exclude_null_check_columns=Literal(
                        ",".join(self.exclude_null_check_columns),
                    ),
                ),
            )
        if self.interactive:
            self.db.execute(
                SQL(
                    """
                    update {control_table}
                    set interactive = {interactive};
                """,
                ).format(
                    control_table=Identifier(self.control_table),
                    interactive=Literal(self.interactive),
                ),
            )
        rows, headers, rowcount = self.db.rowdict(
            SQL("select * from {control_table}").format(
                control_table=Identifier(self.control_table),
            ),
        )
        logger.debug(
            f"Control table after being initialized:\n{tabulate(rows, headers='keys', tablefmt='pipe', showindex=False)}",
        )

    def qa_all(self: PgUpsert) -> None:
        """Performs QA checks for nulls in non-null columns, for duplicated
        primary key values, for invalid foreign keys, and invalid check constraints
        in a set of staging tables to be loaded into base tables.
        If there are failures in the QA checks, loading is not attempted.
        If the loading step is carried out, it is done within a transaction.

        The "null_errors", "pk_errors", "fk_errors", "ck_errors" columns of the
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
        self.db.execute(
            SQL(
                """
            update {control_table}
            set null_errors = null,
                pk_errors = null,
                fk_errors = null,
                ck_errors = null,
                rows_updated = null,
                rows_inserted = null;
            """,
            ).format(control_table=Identifier(self.control_table)),
        )
        # Create a list of the selected tables with a loop control flag.
        self.db.execute(
            SQL(
                """
            drop table if exists ups_proctables cascade;
            select
                table_name,
                exclude_null_checks,
                interactive,
                False::boolean as processed
            into temporary table ups_proctables
            from {control_table};
            """,
            ).format(control_table=Identifier(self.control_table)),
        )
        # Create a view returning a single unprocessed table, in order.
        self.db.execute(
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

        qa_funcs = {
            "Non-NULL": self.qa_all_null,
            "Primary Key": self.qa_all_pk,
            "Foreign Key": self.qa_all_fk,
            "Check Constraint": self.qa_all_ck,
        }

        for qa_check, qa_func in qa_funcs.items():
            logger.info(f"==={qa_check} checks===")
            start_time = datetime.now()
            qa_func()
            logger.debug(f"{qa_check} checks completed in {ellapsed_time(start_time)}")
            logger.debug(f"Control table after {qa_check} checks:")
            ctrl = SQL("select * from {control_table};").format(
                control_table=Identifier(self.control_table),
            )
            if not self.interactive:
                logger.debug(f"\n{self.show_results(ctrl)}")
            # Reset the loop control flag in the control table.
            self.db.execute(SQL("update ups_proctables set processed = False;"))

        # Check for errors
        rows, headers, rowcount = self.db.rowdict(
            SQL(
                """select * from {control_table}
                where coalesce(null_errors, pk_errors, fk_errors, ck_errors) is not null;
                """,
            ).format(
                control_table=Identifier(self.control_table),
            ),
        )
        if rowcount > 0:
            ctrl = SQL("select * from {control_table};").format(
                control_table=Identifier(self.control_table),
            )
            logger.debug("QA checks failed")
            logger.debug(f"\n{self.show_results(ctrl)}")
            logger.debug("")
            if self.interactive:
                btn, return_value = TableUI(
                    "QA Errors",
                    "QA checks failed. Below is a summary of the errors:",
                    [
                        ("Continue", 0, "<Return>"),
                        ("Cancel", 1, "<Escape>"),
                    ],
                    headers,
                    list(rows),
                ).activate()
            else:
                logger.error("===QA checks failed. Below is a summary of the errors===")
                logger.error(self.show_results(ctrl))
            return
        self.qa_passed = True

    def qa_all_null(self: PgUpsert) -> None:
        """Performs null checks for non-null columns in selected staging tables."""
        while True:
            rows, headers, rowcount = self.db.rowdict(
                SQL("select * from ups_toprocess;"),
            )
            if rowcount == 0:
                break
            rows = next(iter(rows))
            self.qa_one_null(table=rows["table_name"])
            # Query the ups_null_error_list control table for the null errors.
            err_rows, err_headers, err_rowcount = self.db.rowdict(
                SQL("select * from ups_null_error_list;"),
            )
            if err_rowcount > 0:
                self.db.execute(
                    SQL(
                        """
                    update {control_table}
                    set null_errors = {null_errors}
                    where table_name = {table_name};
                    """,
                    ).format(
                        control_table=Identifier(self.control_table),
                        null_errors=Literal(next(iter(err_rows))["null_errors"]),
                        table_name=Literal(rows["table_name"]),
                    ),
                )
            # Set the 'processed' column to True in the control table.
            self.db.execute(
                SQL(
                    """
                update ups_proctables
                set processed = True
                where table_name = {table_name};
                """,
                ).format(table_name=Literal(rows["table_name"])),
            )

    def qa_one_null(self: PgUpsert, table: str) -> None:
        """Performs null checks for non-null columns in a single staging table."""
        logger.info(
            f"Checking for NULLs in non-NULL columns in table {self.stg_schema}.{table}",
        )
        self.validate_table(table)
        # Create a table listing the columns of the base table that must
        # be non-null and that do not have a default expression.
        # Include a column for the number of rows with nulls in the staging table.
        # Include a 'processed' column for loop control.
        self.db.execute(
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
                and column_default is null
                and column_name not in ({exclude_null_check_columns});
            """,
            ).format(
                base_schema=Literal(self.base_schema),
                table=Literal(table),
                exclude_null_check_columns=(
                    SQL(",").join(Literal(col) for col in self.exclude_null_check_columns)
                    if self.exclude_null_check_columns
                    else Literal("")
                ),
            ),
        )
        # Process all non-nullable columns.
        while True:
            rows, headers, rowcount = self.db.rowdict(
                SQL("select * from ups_nonnull_cols where not processed limit 1;"),
            )
            if rowcount == 0:
                break
            rows = next(iter(rows))
            logger.debug(f"  Checking column {rows['column_name']} for nulls")
            self.db.execute(
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
                    stg_schema=Identifier(self.stg_schema),
                    table=Identifier(table),
                    column_name=Identifier(rows["column_name"]),
                ),
            )
            # Get the number of rows with nulls in the staging table.
            null_rows, headers, rowcount = self.db.rowdict(
                SQL("select * from ups_qa_nonnull_col;"),
            )
            if rowcount > 0:
                null_rows = next(iter(null_rows))
                logger.warning(
                    f"    Column {rows['column_name']} has {null_rows['nrows']} null values",
                )
                # Set the number of rows with nulls in the control table.
                self.db.execute(
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
                    ).format(column_name=Literal(rows["column_name"])),
                )
            # Set the 'processed' column to True in the control table.
            self.db.execute(
                SQL(
                    """
                update ups_nonnull_cols
                set processed = True
                where column_name = {column_name};
                """,
                ).format(column_name=Literal(rows["column_name"])),
            )
        # Update the control table with the number of rows with nulls in the staging table.
        self.db.execute(
            """
            create or replace temporary view ups_null_error_list as
            select string_agg(column_name || ' (' || null_rows || ')', ', ') as null_errors
            from ups_nonnull_cols
            where coalesce(null_rows, 0) > 0;
        """,
        )

    def qa_all_pk(self: PgUpsert) -> None:
        """Performs primary key checks for duplicated primary key values in selected staging tables."""
        while True:
            rows, headers, rowcount = self.db.rowdict(
                SQL("select * from ups_toprocess;"),
            )
            if rowcount == 0:
                break
            rows = next(iter(rows))
            pk_errors = self.qa_one_pk(table=rows["table_name"])
            if pk_errors:
                self.db.execute(
                    SQL(
                        """
                    update {control_table}
                    set pk_errors = {pk_errors}
                    where table_name = {table_name};
                    """,
                    ).format(
                        control_table=Identifier(self.control_table),
                        pk_errors=Literal(",".join(pk_errors)),
                        table_name=Literal(rows["table_name"]),
                    ),
                )
            # Set the 'processed' column to True in the control table.
            self.db.execute(
                SQL(
                    """
                update ups_proctables
                set processed = True
                where table_name = {table_name};
                """,
                ).format(table_name=Literal(rows["table_name"])),
            )

    def qa_one_pk(self: PgUpsert, table: str) -> list | None:
        """Performs primary key checks for duplicated primary key values in a single staging table."""
        pk_errors = []
        logger.info(
            f"Checking for duplicated primary key values in table {self.stg_schema}.{table}",
        )
        self.validate_table(table)
        # Create a table listing the primary key columns of the base table.
        self.db.execute(
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
            ).format(table=Literal(table), base_schema=Literal(self.base_schema)),
        )
        rows, headers, rowcount = self.db.rowdict(
            "select * from ups_primary_key_columns;",
        )
        if rowcount == 0:
            logger.info("Table has no primary key")
            return None
        # rows = next(iter(rows))
        rows = list(rows)
        logger.debug(f"  Checking constraint {rows[0]['constraint_name']}")
        # Get a comma-delimited list of primary key columns to build SQL selection
        # for duplicate keys, ordered by ordinal position.
        pk_cols = SQL(",").join(Identifier(row["column_name"]) for row in rows)
        self.db.execute(
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
                pkcollist=pk_cols,
                stg_schema=Identifier(self.stg_schema),
                table=Identifier(table),
            ),
        )
        pk_errs, pk_headers, pk_rowcount = self.db.rowdict(
            "select * from ups_pk_check;",
        )
        if pk_rowcount > 0:
            logger.warning(
                f"    Duplicate key error in columns {pk_cols.as_string(self.db.cursor())}",
            )
            pk_errs = list(pk_errs)
            tot_errs, tot_headers, tot_rowcount = self.db.rowdict(
                SQL(
                    "select count(*) as errcount, sum(nrows) as total_rows from ups_pk_check;",
                ),
            )
            tot_errs = next(iter(tot_errs))
            err_msg = f"{tot_errs['errcount']} duplicate keys ({tot_errs['total_rows']} rows) in table {self.stg_schema}.{table}"
            pk_errors.append(err_msg)
            logger.debug("")
            err_sql = SQL("select * from ups_pk_check;")
            logger.debug(f"\n{self.show_results(err_sql)}")
            logger.debug("")
            if self.interactive:
                btn, return_value = TableUI(
                    "Duplicate key error",
                    err_msg,
                    [
                        ("Continue", 0, "<Return>"),
                        ("Cancel", 1, "<Escape>"),
                    ],
                    pk_headers,
                    list(pk_errs),
                ).activate()
                if btn != 0:
                    logger.warning("Script cancelled by user")
                    sys.exit(0)
        return pk_errors

    def qa_all_fk(self: PgUpsert) -> None:
        """Performs foreign key checks for invalid foreign key values in selected staging tables."""
        while True:
            rows, headers, rowcount = self.db.rowdict(
                SQL("select * from ups_toprocess;"),
            )
            if rowcount == 0:
                break
            rows = next(iter(rows))
            fk_errors = self.qa_one_fk(table=rows["table_name"])
            if fk_errors:
                self.db.execute(
                    SQL(
                        """
                    update {control_table}
                    set fk_errors = {fk_errors}
                    where table_name = {table_name};
                    """,
                    ).format(
                        control_table=Identifier(self.control_table),
                        fk_errors=Literal(",".join(fk_errors)),
                        table_name=Literal(rows["table_name"]),
                    ),
                )
            # Set the 'processed' column to True in the control table.
            self.db.execute(
                SQL(
                    """
                update ups_proctables
                set processed = True
                where table_name = {table_name};
                """,
                ).format(table_name=Literal(rows["table_name"])),
            )

    def qa_one_fk(self: PgUpsert, table: str) -> list | None:
        logger.info(
            f"Conducting foreign key QA checks on table {self.stg_schema}.{table}",
        )
        self.validate_table(table)
        # Create a table of *all* foreign key dependencies in this database.
        # Only create it once because it may slow the QA process down.
        if (
            self.db.execute(
                SQL(
                    """select * from information_schema.tables
                    where table_name = {ups_foreign_key_columns};""",
                ).format(ups_foreign_key_columns=Literal("ups_foreign_key_columns")),
            ).rowcount
            == 0
        ):
            self.db.execute(
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
        self.db.execute(
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
            ).format(base_schema=Literal(self.base_schema), table=Literal(table)),
        )
        # Create a temporary table of all unique constraint names for
        # this table, with an integer column to be populated with the
        # number of rows failing the foreign key check, and a 'processed'
        # flag to control looping.
        self.db.execute(
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
            rows, headers, rowcount = self.db.rowdict(
                SQL(
                    """select constraint_name, table_schema, table_name
                    from ups_fk_constraints where not processed limit 1;""",
                ),
            )
            if rowcount == 0:
                break
            rows = next(iter(rows))
            logger.debug(f"  Checking constraint {rows['constraint_name']}")
            self.db.execute(
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
                    constraint_name=Literal(rows["constraint_name"]),
                    table_schema=Literal(rows["table_schema"]),
                    table_name=Literal(rows["table_name"]),
                ),
            )
            const_rows, const_headers, const_rowcount = self.db.rowdict(
                "select * from ups_one_fk;",
            )
            if const_rowcount == 0:
                logger.debug("  No foreign key columns found")
                break
            const_rows = next(iter(const_rows))
            # Create join expressions from staging table (s) to unique table (u)
            # and to staging table equivalent to unique table (su) (though we
            # don't know yet if the latter exists).  Also create a 'where'
            # condition to ensure that all columns being matched are non-null.
            # Also create a comma-separated list of the columns being checked.
            fk_rows, fk_headers, fk_rowcount = self.db.rowdict(
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
            fk_rows = next(iter(fk_rows))
            # Determine whether a staging-table equivalent of the unique table exists.
            su_exists = False
            if (
                self.db.execute(
                    SQL(
                        """select * from information_schema.tables
                        where table_name = {table} and table_schema = {stg_schema};""",
                    ).format(
                        table=Literal(const_rows["uq_table"]),
                        stg_schema=Literal(self.stg_schema),
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
                s_checked=SQL(fk_rows["s_checked"]),
                stg_schema=Identifier(self.stg_schema),
                table=Identifier(table),
                uq_schema=Identifier(const_rows["uq_schema"]),
                uq_table=Identifier(const_rows["uq_table"]),
                u_join=SQL(fk_rows["u_join"]),
            )
            if su_exists:
                query += SQL(
                    """ left join {stg_schema}.{uq_table} as su on {su_join}""",
                ).format(
                    stg_schema=Identifier(self.stg_schema),
                    uq_table=Identifier(const_rows["uq_table"]),
                    su_join=SQL(fk_rows["su_join"]),
                )
                query += SQL(" where u.{uq_column} is null").format(
                    uq_column=Identifier(const_rows["uq_column"]),
                )
            if su_exists:
                query += SQL(" and su.{uq_column} is null").format(
                    uq_column=Identifier(const_rows["uq_column"]),
                )
            query += SQL(
                """ and {s_not_null}
                    group by {s_checked};""",
            ).format(
                s_not_null=SQL(fk_rows["s_not_null"]),
                s_checked=SQL(fk_rows["s_checked"]),
            )
            self.db.execute(query)
            check_sql = SQL("select * from ups_fk_check;")
            fk_check_rows, fk_check_headers, fk_check_rowcount = self.db.rowdict(
                check_sql,
            )
            if fk_check_rowcount > 0:
                fk_check_rows = next(iter(fk_check_rows))
                logger.warning(
                    f"    Foreign key error referencing {const_rows['uq_schema']}.{const_rows['uq_table']}",
                )
                logger.debug("")
                logger.debug(f"\n{self.show_results(check_sql)}")
                logger.debug("")
                if self.interactive:
                    btn, return_value = TableUI(
                        "Foreign key error",
                        f"Foreign key error referencing {const_rows['uq_schema']}.{const_rows['uq_table']}",
                        [
                            ("Continue", 0, "<Return>"),
                            ("Cancel", 1, "<Escape>"),
                        ],
                        fk_check_headers,
                        fk_check_rows,
                    ).activate()
                    if btn != 0:
                        logger.warning("Script cancelled by user")
                        sys.exit(0)

                self.db.execute(
                    SQL(
                        """
                update ups_fk_constraints
                set fkerror_values = {fkerror_count}
                where constraint_name = {constraint_name}
                    and table_schema = {table_schema}
                    and table_name = {table_name};
                """,
                    ).format(
                        fkerror_count=Literal(fk_check_rows["nrows"]),
                        constraint_name=Literal(rows["constraint_name"]),
                        table_schema=Literal(rows["table_schema"]),
                        table_name=Literal(rows["table_name"]),
                    ),
                )
            self.db.execute(
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
                    constraint_name=Literal(rows["constraint_name"]),
                    table_schema=Literal(rows["table_schema"]),
                    table_name=Literal(rows["table_name"]),
                ),
            )
        err_rows, err_headers, err_rowcount = self.db.rowdict(
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
        if err_rowcount > 0:
            return [err["fk_errors"] for err in list(err_rows) if err["fk_errors"]]
        return None

    def qa_all_ck(self: PgUpsert) -> None:
        while True:
            rows, headers, rowcount = self.db.rowdict(
                SQL("select * from ups_toprocess;"),
            )
            if rowcount == 0:
                break
            rows = next(iter(rows))
            self.qa_one_ck(table=rows["table_name"])
            err_rows, err_headers, err_rowcount = self.db.rowdict(
                "select * from ups_ck_error_list;",
            )
            if err_rowcount > 0:
                self.db.execute(
                    SQL(
                        """
                    update {control_table}
                    set ck_errors = {ck_errors}
                    where table_name = {table_name};
                    """,
                    ).format(
                        control_table=Identifier(self.control_table),
                        ck_errors=Literal(next(iter(err_rows))["ck_errors"]),
                        table_name=Literal(rows["table_name"]),
                    ),
                )
            # Set the 'processed' column to True in the control table.
            self.db.execute(
                SQL(
                    """
                update ups_proctables
                set processed = True
                where table_name = {table_name};
                """,
                ).format(table_name=Literal(rows["table_name"])),
            )

    def qa_one_ck(self: PgUpsert, table: str) -> list | None:
        logger.info(
            f"Conducting check constraint QA checks on table {self.stg_schema}.{table}",
        )
        # Create a table of *all* check constraints in this database.
        # Because this may be an expensive operation (in terms of time), the
        # table is not re-created if it already exists.  "Already exists"
        # means that a table with the expected name exists.  No check is
        # done to ensure that this table has the correct structure.  The
        # goal is to create the table of all check constraints only once to
        # minimize the time required if QA checks are to be run on multiple
        # staging tables.
        if (
            self.db.execute(
                SQL(
                    """select * from information_schema.tables
                    where table_name = {ups_check_constraints};""",
                ).format(ups_check_constraints=Literal("ups_check_constraints")),
            ).rowcount
            == 0
        ):
            self.db.execute(
                SQL(
                    """
                drop table if exists ups_check_constraints cascade;
                select
                    nspname as table_schema,
                    cast(conrelid::regclass as text) as table_name,
                    conname as constraint_name,
                    pg_get_constraintdef(pg_constraint.oid) AS consrc
                into temporary table ups_check_constraints
                from pg_constraint
                inner join pg_class on pg_constraint.conrelid = pg_class.oid
                inner join pg_namespace on pg_class.relnamespace=pg_namespace.oid
                where contype = 'c' and nspname = {base_schema};
            """,
                ).format(base_schema=Literal(self.base_schema)),
            )

        # Create a temporary table of just the check constraints for the base
        # table corresponding to the staging table to check. Include a
        # column for the number of rows failing the check constraint, and a
        # 'processed' flag to control looping.
        self.db.execute(
            SQL(
                """
            drop table if exists ups_sel_cks cascade;
            select
                constraint_name, table_schema, table_name, consrc,
                0::integer as ckerror_values,
                False as processed
            into temporary table ups_sel_cks
            from ups_check_constraints
            where
                table_schema = {base_schema}
                and table_name = {table};
            """,
            ).format(base_schema=Literal(self.base_schema), table=Literal(table)),
        )

        # Process all check constraints.
        while True:
            rows, headers, rowcount = self.db.rowdict(
                SQL(
                    """select constraint_name, table_schema, table_name, consrc
                    from ups_sel_cks where not processed limit 1;""",
                ),
            )
            if rowcount == 0:
                break
            rows = next(iter(rows))
            logger.debug(f"  Checking constraint {rows['constraint_name']}")
            # Remove the 'CHECK' keyword from the constraint definition.
            const_rows, const_headers, const_rowcount = self.db.rowdict(
                SQL(
                    """
                select
                    regexp_replace(consrc, '^CHECK\\s*\\((.*)\\)$', '\\1') as check_sql
                from ups_sel_cks
                where
                    constraint_name = {constraint_name}
                    and table_schema = {table_schema}
                    and table_name = {table_name};
                """,
                ).format(
                    constraint_name=Literal(rows["constraint_name"]),
                    table_schema=Literal(rows["table_schema"]),
                    table_name=Literal(rows["table_name"]),
                ),
            )
            const_rows = next(iter(const_rows))
            # Run the check_sql
            self.db.execute(
                SQL(
                    """
            create or replace temporary view ups_ck_check_check as
            select count(*) from {stg_schema}.{table}
            where not ({check_sql})
            """,
                ).format(
                    stg_schema=Identifier(self.stg_schema),
                    table=Identifier(table),
                    check_sql=SQL(const_rows["check_sql"]),
                ),
            )

            ck_check_rows, ck_check_headers, ck_check_rowcount = self.db.rowdict(
                "select * from ups_ck_check_check where count > 0;",
            )
            if ck_check_rowcount > 0:
                ck_check_rows = next(iter(ck_check_rows))
                logger.warning(
                    f"    Check constraint {rows['constraint_name']} has {ck_check_rowcount} failing rows",
                )
                self.db.execute(
                    SQL(
                        """
                    update ups_sel_cks
                    set ckerror_values = {ckerror_count}
                    where
                        constraint_name = {constraint_name}
                        and table_schema = {table_schema}
                        and table_name = {table_name};
                    """,
                    ).format(
                        ckerror_count=Literal(ck_check_rows["count"]),
                        constraint_name=Literal(rows["constraint_name"]),
                        table_schema=Literal(rows["table_schema"]),
                        table_name=Literal(rows["table_name"]),
                    ),
                )
            self.db.execute(
                SQL(
                    """
                update ups_sel_cks
                set processed = True
                where
                    constraint_name = {constraint_name}
                    and table_schema = {table_schema}
                    and table_name = {table_name};
                """,
                ).format(
                    constraint_name=Literal(rows["constraint_name"]),
                    table_schema=Literal(rows["table_schema"]),
                    table_name=Literal(rows["table_name"]),
                ),
            )

        # Update the control table with the number of rows failing the check constraint.
        self.db.execute(
            SQL(
                """
            create or replace temporary view ups_ck_error_list as
            select string_agg(
                constraint_name || ' (' || ckerror_values || ')', ', '
                ) as ck_errors
            from ups_sel_cks
            where coalesce(ckerror_values, 0) > 0;
            """,
            ),
        )

    def upsert_all(self: PgUpsert) -> None:
        if not self.qa_passed:
            self.qa_all()
        self.validate_control()
        logger.info("===Starting upsert procedures===")


def cli() -> None:
    """Main command line entrypoint for the upsert function."""
    args = clparser().parse_args()
    if args.log and args.log.exists():
        args.log.unlink()
    if not args.quiet:
        logger.addHandler(logging.StreamHandler())
    if args.log:
        logger.addHandler(logging.FileHandler(args.log))
    if args.debug:
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(lineno)d - %(message)s",
        )
        for handler in logger.handlers:
            handler.setFormatter(formatter)
    PgUpsert(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        tables=args.tables,
        stg_schema=args.stg_schema,
        base_schema=args.base_schema,
        do_commit=args.do_commit,
        upsert_method=args.upsert_method,
        interactive=args.interactive,
        exclude_cols=args.exclude.split(",") if args.exclude else None,
        exclude_null_check_columns=args.null.split(",") if args.null else None,
    ).run()


if __name__ == "__main__":
    cli()
