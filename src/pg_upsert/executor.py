"""Upsert execution logic for pg-upsert."""

from __future__ import annotations

import logging

from psycopg2.sql import SQL, Identifier, Literal

from .control import ControlTable
from .models import (
    CallbackEvent,
    PipelineCallback,
    PipelineEvent,
    TableResult,
    UserCancelledError,
)
from .postgres import PostgresDB
from .ui import UIBackend, display

logger = logging.getLogger(__name__)

# File-only logger for messages that also appear on the rich console.
# Uses the same child logger as display.py to avoid stream handler duplication.
_file_logger = logging.getLogger("pg_upsert.display")


class UpsertExecutor:
    """Executes upsert (INSERT + UPDATE) operations against base tables.

    Pulls data from staging tables into the corresponding base tables using
    the method specified at construction time.

    Args:
        db: An open PostgresDB connection.
        control: The ControlTable instance for reading per-table specs and
            writing row counts.
        staging_schema: Name of the staging schema.
        base_schema: Name of the base schema.
        upsert_method: One of ``"upsert"``, ``"update"``, or ``"insert"``.
    """

    def __init__(
        self,
        db: PostgresDB,
        control: ControlTable,
        staging_schema: str,
        base_schema: str,
        upsert_method: str = "upsert",
        ui: UIBackend | None = None,
    ) -> None:
        self.db = db
        self.control = control
        self.staging_schema = staging_schema
        self.base_schema = base_schema
        self.upsert_method = upsert_method
        if ui is None:
            from .ui.console import ConsoleBackend

            self._ui: UIBackend = ConsoleBackend()
        else:
            self._ui = ui

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_dependency_order(self) -> list[str]:
        """Return the table names from the control table sorted by FK dependency.

        Builds ``ups_dependencies`` and ``ups_ordered_tables`` temporary tables
        and returns the names of the control table's tables in the order they
        should be processed (parents before children).

        Returns:
            Ordered list of table names.
        """
        self.db.execute(
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
            ).format(base_schema=Literal(self.base_schema)),
        )
        self.db.execute(
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
            ).format(base_schema=Literal(self.base_schema)),
        )

        ordered_rows, _headers, _rowcount = self.db.rowdict(
            SQL(
                """
            select tl.table_name
            from {control_table} as tl
            inner join ups_ordered_tables as ot on ot.table_name = tl.table_name
            order by ot.table_order;
            """,
            ).format(control_table=Identifier(self.control.table_name)),
        )
        return [row["table_name"] for row in ordered_rows]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def upsert_one(self, table: str, interactive: bool = False) -> TableResult:
        """Perform an upsert operation on a single table.

        Reads the table specification from the control table, builds the
        appropriate UPDATE and/or INSERT statements, executes them, and
        writes the row counts back to the control table.

        Args:
            table: The table name to upsert.
            interactive: If ``True``, show Tkinter confirmation dialogs.

        Returns:
            A :class:`TableResult` with the accurate row counts from the
            executed statements.
        """
        rows_updated = 0
        rows_inserted = 0
        display.console.print(f"\n  [bold]{self.base_schema}.{table}[/bold]")
        _file_logger.info(f"Performing upsert on table {self.base_schema}.{table}")

        spec = self.control.get_table_spec(table)
        if spec is None:
            display.console.print(f"  [bold yellow]Warning: Table {table} not found in control table[/bold yellow]")
            _file_logger.warning(f"Table {table} not found in control table")
            return TableResult(table_name=table)

        # Use the interactive flag stored in the control table (set at init time)
        # so runtime mutation of PgUpsert.interactive doesn't affect ongoing upserts.
        interactive = bool(spec.get("interactive", interactive))

        # Build ups_cols — columns present in both staging and base, excluding excludes.
        query = SQL(
            """
            drop table if exists ups_cols cascade;
            select s.column_name, s.ordinal_position
            into temporary table ups_cols
            from information_schema.columns as s
                inner join information_schema.columns as b on s.column_name=b.column_name
            where
                s.table_schema = {staging_schema}
                and s.table_name = {table}
                and b.table_schema = {base_schema}
                and b.table_name = {table}
            """,
        ).format(
            staging_schema=Literal(self.staging_schema),
            table=Literal(table),
            base_schema=Literal(self.base_schema),
        )
        if spec["exclude_cols"]:
            query += SQL(
                """
                and s.column_name not in ({exclude_cols})
                """,
            ).format(
                exclude_cols=SQL(",").join(
                    Literal(col.strip()) for col in spec["exclude_cols"].split(",") if spec["exclude_cols"]
                ),
            )
        query += SQL(" order by s.ordinal_position;")
        self.db.execute(query)

        # Build ups_pks — primary key columns of the base table.
        self.db.execute(
            SQL(
                """
            drop table if exists ups_pks cascade;
            select k.column_name, k.ordinal_position
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
            ).format(table=Literal(table), base_schema=Literal(self.base_schema)),
        )

        # Fetch column names from the temp tables and build SQL fragments in Python
        # using Identifier() for proper escaping (instead of string_agg in SQL).
        col_rows, _ch, col_count = self.db.rowdict("select column_name from ups_cols order by ordinal_position;")
        col_names = [r["column_name"] for r in col_rows]
        if not col_names:
            display.console.print(
                "  [bold yellow]Warning: No shared columns between staging and base tables[/bold yellow]",
            )
            _file_logger.warning("No shared columns between staging and base tables")
            return TableResult(table_name=table)

        pk_rows, _ph, pk_count = self.db.rowdict("select column_name from ups_pks order by ordinal_position;")
        pk_names = [r["column_name"] for r in pk_rows]
        if not pk_names:
            display.console.print("  [bold yellow]Warning: Base table has no primary key[/bold yellow]")
            _file_logger.warning("Base table has no primary key")
            return TableResult(table_name=table)

        # Build Composable SQL fragments from column names.
        all_col_sql = SQL(", ").join(Identifier(c) for c in col_names)
        base_col_sql = SQL(", ").join(SQL("b.") + Identifier(c) for c in col_names)
        stg_col_sql = SQL(", ").join(SQL("s.") + Identifier(c) for c in col_names)
        pk_col_sql = SQL(", ").join(Identifier(c) for c in pk_names)
        join_sql = SQL(" AND ").join(SQL("b.") + Identifier(c) + SQL(" = s.") + Identifier(c) for c in pk_names)
        # Keep string versions for non-SQL uses (e.g., passing to UI as pk_cols list).
        pk_col_list = ", ".join(pk_names)

        from_clause = (
            SQL(
                """FROM {base_schema}.{table} as b
            INNER JOIN {staging_schema}.{table} as s ON """,
            ).format(
                base_schema=Identifier(self.base_schema),
                table=Identifier(table),
                staging_schema=Identifier(self.staging_schema),
            )
            + join_sql
        )

        self.db.execute(
            SQL(
                """
            drop view if exists ups_basematches cascade;
            create temporary view ups_basematches as select {base_col_list} {from_clause};

            drop view if exists ups_stgmatches cascade;
            create temporary view ups_stgmatches as select {stg_col_list} {from_clause};
            """,
            ).format(
                base_col_list=base_col_sql,
                stg_col_list=stg_col_sql,
                from_clause=from_clause,
            ),
        )

        self.db.execute(
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

        do_updates = False
        update_stmt = None
        if self.upsert_method in ("upsert", "update"):
            stg_curs = self.db.execute("select * from ups_stgmatches;")
            if stg_curs.rowcount == 0:
                logger.debug("  No rows in staging table matching primary key in base table")
            stg_cols = [col.name for col in stg_curs.description]
            stg_rowcount = stg_curs.rowcount
            stg_data = stg_curs.fetchall()
            nk_curs = self.db.execute("select * from ups_nk;")
            nk_rowcount = nk_curs.rowcount
            if stg_rowcount > 0 and nk_rowcount > 0:
                base_curs = self.db.execute("select * from ups_basematches;")
                if base_curs.rowcount == 0:
                    logger.debug("  No rows in base table matching primary key in staging table")
                    return TableResult(table_name=table)
                base_cols = [col.name for col in base_curs.description]
                base_data = base_curs.fetchall()
                if interactive:
                    # Extract exclude_cols from the control-table spec so the
                    # diff highlighting in the UI skips columns the upsert
                    # will not actually update.
                    _exclude_cols_str = spec.get("exclude_cols") or ""
                    _exclude_cols = [c.strip() for c in _exclude_cols_str.split(",") if c.strip()]
                    btn, _return_value = self._ui.show_comparison(
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
                        exclude_cols=_exclude_cols,
                    )
                else:
                    btn = 0
                if btn == 2:
                    display.print_check_table_fail(self.staging_schema, table, "Script cancelled by user")
                    raise UserCancelledError("Script cancelled by user during update confirmation")
                if btn == 0:
                    do_updates = True
                    # Build SET clause from non-key columns using Identifier().
                    nk_names = [c for c in col_names if c not in pk_names]
                    if not nk_names:
                        logger.warning("No non-key columns to update")
                        return TableResult(table_name=table)
                    ups_set_sql = SQL(", ").join(Identifier(c) + SQL(" = s.") + Identifier(c) for c in nk_names)
                    update_stmt = (
                        SQL(
                            "UPDATE {base_schema}.{table} as b SET ",
                        ).format(
                            base_schema=Identifier(self.base_schema),
                            table=Identifier(table),
                        )
                        + ups_set_sql
                        + SQL(
                            " FROM {staging_schema}.{table} as s WHERE ",
                        ).format(
                            staging_schema=Identifier(self.staging_schema),
                            table=Identifier(table),
                        )
                        + join_sql
                    )
            else:
                display.console.print("    [dim]no rows to update[/dim]")
                _file_logger.info("    no rows to update")

        do_inserts = False
        insert_stmt = None
        if self.upsert_method in ("upsert", "insert"):
            self.db.execute(
                SQL(
                    """
                drop view if exists ups_newrows cascade;
                create temporary view ups_newrows as with newpks as (
                    select {pk_col_list}
                    from {staging_schema}.{table}
                    except
                    select {pk_col_list}
                    from {base_schema}.{table}
                )
                select s.*
                from {staging_schema}.{table} as s
                    inner join newpks using ({pk_col_list});
                """,
                ).format(
                    staging_schema=Identifier(self.staging_schema),
                    table=Identifier(table),
                    pk_col_list=pk_col_sql,
                    base_schema=Identifier(self.base_schema),
                ),
            )
            new_curs = self.db.execute("select * from ups_newrows;")
            new_cols = [col.name for col in new_curs.description]
            new_rowcount = new_curs.rowcount
            new_data = new_curs.fetchall()
            if new_rowcount > 0:
                if interactive:
                    btn, _return_value = self._ui.show_table(
                        "New Data",
                        f"Do you want to add these new data to the {self.base_schema}.{table} table?",
                        [
                            ("Continue", 0, "<Return>"),
                            ("Skip", 1, "<Escape>"),
                            ("Cancel", 2, "<Escape>"),
                        ],
                        new_cols,
                        new_data,
                    )
                else:
                    btn = 0
                if btn == 2:
                    display.print_check_table_fail(self.staging_schema, table, "Script cancelled by user")
                    raise UserCancelledError("Script cancelled by user during insert confirmation")
                if btn == 0:
                    do_inserts = True
                    insert_stmt = SQL(
                        """
                        INSERT INTO {base_schema}.{table} ({all_col_list})
                        SELECT {all_col_list} FROM ups_newrows
                    """,
                    ).format(
                        base_schema=Identifier(self.base_schema),
                        table=Identifier(table),
                        all_col_list=all_col_sql,
                    )
            else:
                display.console.print("    [dim]no new data to insert[/dim]")
                _file_logger.info("    no new data to insert")

        # Execute and capture accurate rowcounts from the cursor.
        if do_updates and update_stmt and self.upsert_method in ("upsert", "update"):
            logger.debug(f"    UPDATE statement for {self.base_schema}.{table}")
            logger.debug(f"{update_stmt.as_string(self.db.conn)}")
            update_curs = self.db.execute(update_stmt)
            rows_updated = update_curs.rowcount  # fixed: was stg_rowcount
            display.console.print(f"    [green]↑[/green] {rows_updated} rows updated")
            _file_logger.info(f"    {rows_updated} rows updated")
        if do_inserts and insert_stmt and self.upsert_method in ("upsert", "insert"):
            logger.debug(f"    INSERT statement for {self.base_schema}.{table}")
            logger.debug(f"{insert_stmt.as_string(self.db.conn)}")
            insert_curs = self.db.execute(insert_stmt)
            rows_inserted = insert_curs.rowcount
            display.console.print(f"    [green]+[/green] {rows_inserted} rows inserted")
            _file_logger.info(f"    {rows_inserted} rows inserted")

        self.control.set_row_counts(table, rows_updated, rows_inserted)
        return TableResult(table_name=table, rows_updated=rows_updated, rows_inserted=rows_inserted)

    def upsert_all(
        self,
        tables: list[str] | tuple[str, ...],
        interactive: bool = False,
        callback: PipelineCallback | None = None,
    ) -> list[TableResult]:
        """Perform upsert operations on all *tables* in dependency order.

        Args:
            tables: Table names to process (will be reordered by FK dependency).
            interactive: If ``True``, show Tkinter confirmation dialogs.

        Returns:
            A list of :class:`TableResult` objects, one per table processed.
        """
        ordered_tables = self._get_dependency_order()
        # Filter to only tables that were requested.
        requested = set(tables)
        ordered_tables = [t for t in ordered_tables if t in requested]
        # Append any requested tables not found in dependency order.
        for t in tables:
            if t not in ordered_tables:
                ordered_tables.append(t)

        results: list[TableResult] = []
        for table in ordered_tables:
            result = self.upsert_one(table, interactive=interactive)
            results.append(result)
            if callback:
                event = PipelineEvent(
                    event=CallbackEvent.UPSERT_TABLE_COMPLETE,
                    table=table,
                    rows_updated=result.rows_updated,
                    rows_inserted=result.rows_inserted,
                )
                if callback(event) is False:
                    raise UserCancelledError(f"Pipeline aborted by callback after upsert for {table}")
        return results
