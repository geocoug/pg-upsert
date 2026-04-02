"""Control table management for pg-upsert."""

from __future__ import annotations

import logging

from psycopg2.sql import SQL, Identifier, Literal

from . import display
from .postgres import PostgresDB
from .ui import TableUI

logger = logging.getLogger(__name__)


class ControlTable:
    """Manages the temporary upsert control table in the database.

    The control table tracks per-table state throughout the QA and upsert
    pipeline: which columns to exclude, error strings from each QA check,
    and final row counts.

    Args:
        db: An open PostgresDB connection.
        table_name: Name of the temporary control table (default ``ups_control``).
    """

    def __init__(self, db: PostgresDB, table_name: str = "ups_control") -> None:
        self.db = db
        self.table_name = table_name

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def initialize(
        self,
        tables: list[str] | tuple[str, ...],
        exclude_cols: list[str] | tuple[str, ...] | None,
        exclude_null_check_cols: list[str] | tuple[str, ...] | None,
        interactive: bool,
    ) -> None:
        """Create and populate the control table.

        Drops any existing control table, recreates it, and inserts one row
        per table.  Exclude-column lists and the ``interactive`` flag are
        written into the table when provided.

        Args:
            tables: Ordered list of table names to process.
            exclude_cols: Column names to skip during upsert.
            exclude_null_check_cols: Column names to skip during null checks.
            interactive: Whether the run is interactive.
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
                unique_errors text,
                column_errors text,
                type_errors text,
                rows_updated integer,
                rows_inserted integer
            );
            insert into {control_table}
                (table_name)
            select
                trim(unnest(string_to_array({tables}, ',')));
            """,
        ).format(
            control_table=Identifier(self.table_name),
            tables=Literal(",".join(tables)),
        )
        self.db.execute(sql)

        if exclude_cols and len(exclude_cols) > 0:
            self.db.execute(
                SQL(
                    """
                    update {control_table}
                    set exclude_cols = {exclude_cols};
                """,
                ).format(
                    control_table=Identifier(self.table_name),
                    exclude_cols=Literal(",".join(exclude_cols)),
                ),
            )

        if exclude_null_check_cols and len(exclude_null_check_cols) > 0:
            self.db.execute(
                SQL(
                    """
                    update {control_table}
                    set exclude_null_checks = {exclude_null_check_cols};
                """,
                ).format(
                    control_table=Identifier(self.table_name),
                    exclude_null_check_cols=Literal(",".join(exclude_null_check_cols)),
                ),
            )

        if interactive:
            self.db.execute(
                SQL(
                    """
                    update {control_table}
                    set interactive = {interactive};
                """,
                ).format(
                    control_table=Identifier(self.table_name),
                    interactive=Literal(interactive),
                ),
            )

        debug_sql = SQL("select * from {control_table}").format(
            control_table=Identifier(self.table_name),
        )
        rows, headers, _rowcount = self.db.rowdict(debug_sql)
        logger.debug(
            f"Control table after being initialized:\n{display.format_sql_result(list(rows), headers)}",
        )

    def validate(self, base_schema: str, staging_schema: str) -> None:
        """Validate that all tables in the control table exist in both schemas.

        Creates temporary tables ``ups_validate_control`` and
        ``ups_ctrl_invl_table`` as side effects.

        Args:
            base_schema: Name of the base schema.
            staging_schema: Name of the staging schema.

        Raises:
            ValueError: If any table is missing from either schema.
        """
        logger.debug("Validating control table")

        # Re-create control table if it has been dropped.
        if (
            self.db.execute(
                SQL(
                    """
                select 1
                from information_schema.tables
                where table_name = {control_table}
            """,
                ).format(
                    control_table=Literal(self.table_name),
                ),
            ).rowcount
            == 0
        ):
            # Nothing to validate — the caller should initialise first.
            return

        sql = SQL(
            """
            drop table if exists ups_validate_control cascade;
            select cast({base_schema} as text) as base_schema,
                cast({staging_schema} as text) as staging_schema,
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
            staging_schema=Literal(staging_schema),
            control_table=Identifier(self.table_name),
        )
        if self.db.execute(sql).rowcount > 0:
            logger.error("Invalid table(s) specified:")
            rows, _headers, _rowcount = self.db.rowdict(
                SQL("select schema_table from ups_ctrl_invl_table"),
            )
            for row in rows:
                logger.error(f"  {row['schema_table']}")
            raise ValueError("Invalid table(s) specified")

    def set_qa_errors(self, table: str, column_name: str, errors_str: str) -> None:
        """Set a QA error column for *table* in the control table.

        Args:
            table: The table name to update.
            column_name: The error column to set (e.g. ``null_errors``, ``pk_errors``).
            errors_str: The error string to store.
        """
        self.db.execute(
            SQL(
                "update {control_table} set {col} = {errors} where table_name = {table};",
            ).format(
                control_table=Identifier(self.table_name),
                col=Identifier(column_name),
                errors=Literal(errors_str),
                table=Literal(table),
            ),
        )

    def set_row_counts(self, table: str, updated: int, inserted: int) -> None:
        """Update the rows_updated / rows_inserted counters for *table*.

        Args:
            table: The table name to update.
            updated: Number of rows updated.
            inserted: Number of rows inserted.
        """
        self.db.execute(
            SQL(
                """
            update {control_table}
            set
                rows_updated = {rows_updated},
                rows_inserted = {rows_inserted}
            where
                table_name = {table_name};
            """,
            ).format(
                control_table=Identifier(self.table_name),
                rows_updated=Literal(updated),
                rows_inserted=Literal(inserted),
                table_name=Literal(table),
            ),
        )

    def get_table_spec(self, table: str) -> dict | None:
        """Return the control row for *table* as a dict, or ``None`` if absent.

        Args:
            table: The table name to look up.

        Returns:
            A dictionary of the control row, or ``None`` if the table is not found.
        """
        rows, _headers, rowcount = self.db.rowdict(
            SQL(
                """
            select table_name, exclude_cols, interactive
            from {control_table}
            where table_name = {table};
            """,
            ).format(
                control_table=Identifier(self.table_name),
                table=Literal(table),
            ),
        )
        if rowcount == 0:
            return None
        return next(iter(rows))

    def get_all_specs(self) -> list[dict]:
        """Return all rows from the control table as a list of dicts.

        Returns:
            A list of dictionaries, one per table in the control table.
        """
        rows, _headers, _rowcount = self.db.rowdict(
            SQL("select * from {control_table}").format(
                control_table=Identifier(self.table_name),
            ),
        )
        return list(rows)

    def has_errors(self) -> bool:
        """Return ``True`` if any QA error column is non-null in the control table.

        Returns:
            True if any row has at least one non-null error column.
        """
        _rows, _headers, rowcount = self.db.rowdict(
            SQL(
                """select * from {control_table}
                where coalesce(null_errors, pk_errors, fk_errors, ck_errors,
                              unique_errors, column_errors, type_errors) is not null;
                """,
            ).format(
                control_table=Identifier(self.table_name),
            ),
        )
        return rowcount > 0

    def clear_results(self) -> None:
        """Reset all error and count columns to NULL.

        Called at the start of each ``run()`` to ensure a clean slate when the
        control table is reused.
        """
        self.db.execute(
            SQL(
                """
            update {control_table}
            set null_errors = null,
                pk_errors = null,
                fk_errors = null,
                ck_errors = null,
                unique_errors = null,
                column_errors = null,
                type_errors = null,
                rows_updated = null,
                rows_inserted = null;
            """,
            ).format(control_table=Identifier(self.table_name)),
        )

    def show(self, interactive: bool) -> None:
        """Display the current contents of the control table.

        In interactive mode a Tkinter ``TableUI`` window is shown.
        Otherwise the table is logged at INFO level.

        Args:
            interactive: If ``True``, display in a Tkinter window.
        """
        sql = SQL("select * from {control_table};").format(
            control_table=Identifier(self.table_name),
        )
        if interactive:
            ctrl_rows, ctrl_headers, _ctrl_rowcount = self.db.rowdict(sql)
            ctrl_rows = list(ctrl_rows)
            TableUI(
                "Control Table",
                "Control table contents:",
                [
                    ("Continue", 0, "<Return>"),
                    ("Cancel", 1, "<Escape>"),
                ],
                ctrl_headers,
                [[row[header] for header in ctrl_headers] for row in ctrl_rows],
            ).activate()
        else:
            rows, headers, _rowcount = self.db.rowdict(sql)
            logger.info(f"Control table contents:\n{display.format_sql_result(list(rows), headers)}")
