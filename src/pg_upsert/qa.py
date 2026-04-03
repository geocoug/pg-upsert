"""QA check logic for pg-upsert staging tables."""

from __future__ import annotations

import logging

from psycopg2.sql import SQL, Identifier, Literal

from .control import ControlTable
from .models import QACheckType, QAError, UserCancelledError
from .postgres import PostgresDB
from .ui import UIBackend, display

logger = logging.getLogger(__name__)


class QARunner:
    """Runs all QA checks against staging tables.

    Each ``check_*`` method performs one class of QA check (nulls, PKs, FKs,
    or check constraints) for a single table and returns any errors found.
    ``run_all`` orchestrates the full suite across all tables.

    Args:
        db: An open PostgresDB connection.
        control: The ControlTable instance tracking per-table state.
        staging_schema: Name of the staging schema.
        base_schema: Name of the base schema.
        exclude_null_check_cols: Column names to skip during null checks.
    """

    def __init__(
        self,
        db: PostgresDB,
        control: ControlTable,
        staging_schema: str,
        base_schema: str,
        exclude_null_check_cols: list[str] | tuple[str, ...] | None = None,
        ui: UIBackend | None = None,
    ) -> None:
        self.db = db
        self.control = control
        self.staging_schema = staging_schema
        self.base_schema = base_schema
        self.exclude_null_check_cols: list[str] | tuple[str, ...] = exclude_null_check_cols or ()
        if ui is None:
            from .ui.console import ConsoleBackend

            self._ui: UIBackend = ConsoleBackend()
        else:
            self._ui = ui

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Check methods
    # ------------------------------------------------------------------

    def check_nulls(self, table: str) -> list[QAError]:
        """Check for NULL values in non-nullable columns of *table*.

        Creates temporary objects ``ups_nonnull_cols``, ``ups_qa_nonnull_col``,
        and ``ups_null_error_list``.

        Args:
            table: The staging table name to check.

        Returns:
            A list of :class:`QAError` instances for any null violations found.
        """
        errors: list[QAError] = []
        logger.debug(f"Conducting not-null QA checks on table {self.staging_schema}.{table}")

        # Find non-nullable columns (excluding defaults and excluded columns).
        col_query = SQL(
            """select column_name from information_schema.columns
            where table_schema = {base_schema}
                and table_name = {table}
                and is_nullable = 'NO'
                and column_default is null""",
        ).format(base_schema=Literal(self.base_schema), table=Literal(table))
        if self.exclude_null_check_cols:
            col_query += SQL(" and column_name not in ({cols})").format(
                cols=SQL(",").join(Literal(col) for col in self.exclude_null_check_cols),
            )
        col_rows, _ch, _cc = self.db.rowdict(col_query)
        nonnull_cols = [r["column_name"] for r in col_rows]
        if not nonnull_cols:
            display.print_check_table_pass(self.staging_schema, table)
            return errors

        # Single query: count NULLs for all non-nullable columns at once.
        count_exprs = SQL(", ").join(
            SQL("sum(case when {col} is null then 1 else 0 end) as {alias}").format(
                col=Identifier(c),
                alias=Identifier(c),
            )
            for c in nonnull_cols
        )
        null_counts = self.db.execute(
            SQL("select {exprs} from {schema}.{table}").format(
                exprs=count_exprs,
                schema=Identifier(self.staging_schema),
                table=Identifier(table),
            ),
        ).fetchone()

        # Build error string from columns with null_count > 0.
        null_details: list[str] = []
        if null_counts:
            for i, col in enumerate(nonnull_cols):
                count = null_counts[i] or 0
                if count > 0:
                    null_details.append(f"{col} ({count})")

        if null_details:
            error_str = ", ".join(null_details)
            self.control.set_qa_errors(table, "null_errors", error_str)
            display.print_check_table_fail(self.staging_schema, table, error_str)
            errors.append(
                QAError(table=table, check_type=QACheckType.NULL, details=error_str),
            )
        else:
            display.print_check_table_pass(self.staging_schema, table)
        return errors

    def check_pks(self, table: str, interactive: bool = False) -> list[QAError]:
        """Check for duplicate primary key values in *table*.

        Creates temporary objects ``ups_primary_key_columns`` and
        ``ups_pk_check``.

        Args:
            table: The staging table name to check.
            interactive: If ``True``, show a Tkinter dialog for any errors found.

        Returns:
            A list of :class:`QAError` instances for any PK violations found.
        """
        errors: list[QAError] = []
        logger.debug(f"Conducting primary key QA checks on table {self.staging_schema}.{table}")

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
        pk_rows, _pk_headers, pk_rowcount = self.db.rowdict(
            "select * from ups_primary_key_columns;",
        )
        if pk_rowcount == 0:
            logger.info("Table has no primary key")
            display.print_check_table_pass(self.staging_schema, table)
            return errors

        pk_rows = list(pk_rows)
        logger.debug(f"  Checking constraint {pk_rows[0]['constraint_name']}")
        pk_cols = SQL(",").join(Identifier(row["column_name"]) for row in pk_rows)
        self.db.execute(
            SQL(
                """
            drop view if exists ups_pk_check cascade;
            create temporary view ups_pk_check as
            select {pkcollist}, count(*) as nrows
            from {staging_schema}.{table} as s
            group by {pkcollist}
            having count(*) > 1;
            """,
            ).format(
                pkcollist=pk_cols,
                staging_schema=Identifier(self.staging_schema),
                table=Identifier(table),
            ),
        )
        pk_errs, pk_headers, pk_rowcount = self.db.rowdict("select * from ups_pk_check;")
        if pk_rowcount > 0:
            logger.warning(
                f"    Duplicate key error in columns {pk_cols.as_string(self.db.cursor())}",
            )
            pk_errs = list(pk_errs)
            tot_errs, _tot_headers, _tot_rowcount = self.db.rowdict(
                SQL("select count(*) as errcount, sum(nrows) as total_rows from ups_pk_check;"),
            )
            tot_errs = next(iter(tot_errs))  # guarded by pk_rowcount > 0 check above
            err_msg = f"{tot_errs['errcount']} duplicate keys ({tot_errs['total_rows']} rows) in table {self.staging_schema}.{table}"  # noqa: E501
            display.print_check_table_fail(
                self.staging_schema,
                table,
                err_msg,
                detail_rows=pk_errs,
                detail_headers=pk_headers,
            )
            if interactive:
                btn, _return_value = self._ui.show_table(
                    "Duplicate key error",
                    err_msg,
                    [
                        ("Continue", 0, "<Return>"),
                        ("Cancel", 1, "<Escape>"),
                    ],
                    pk_headers,
                    [[row[header] for header in pk_headers] for row in pk_errs],
                )
                if btn != 0:
                    display.print_check_table_fail(self.staging_schema, table, "Script cancelled by user")
                    raise UserCancelledError("Script cancelled by user during primary key check")
            self.control.set_qa_errors(table, "pk_errors", err_msg)
            errors.append(
                QAError(table=table, check_type=QACheckType.PRIMARY_KEY, details=err_msg),
            )
        else:
            display.print_check_table_pass(self.staging_schema, table)
        return errors

    def check_fks(self, table: str, interactive: bool = False) -> list[QAError]:
        """Check for invalid foreign key references in *table*.

        Creates temporary objects ``ups_foreign_key_columns``,
        ``ups_sel_fks``, ``ups_fk_constraints``, ``ups_one_fk``, and
        ``ups_fk_check``.  The ``ups_foreign_key_columns`` table is created
        only once per session to minimise overhead.

        Args:
            table: The staging table name to check.
            interactive: If ``True``, show a Tkinter dialog for any errors found.

        Returns:
            A list of :class:`QAError` instances for any FK violations found.
        """
        errors: list[QAError] = []
        logger.debug(f"Conducting foreign key QA checks on table {self.staging_schema}.{table}")

        # Build the full FK map once per session.
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

        fk_error_strings: list[str] = []
        constraint_rows, _fkc_headers, _fkc_rowcount = self.db.rowdict(
            SQL(
                """select constraint_name, table_schema, table_name
                from ups_fk_constraints;""",
            ),
        )
        for constraint_row in constraint_rows:
            logger.debug(f"  Checking constraint {constraint_row['constraint_name']}")
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
                    constraint_name=Literal(constraint_row["constraint_name"]),
                    table_schema=Literal(constraint_row["table_schema"]),
                    table_name=Literal(constraint_row["table_name"]),
                ),
            )
            const_rows, _const_headers, const_rowcount = self.db.rowdict(
                "select * from ups_one_fk;",
            )
            if const_rowcount == 0:
                logger.debug("  No foreign key columns found")
                continue
            const_rows = list(const_rows)
            const_row = const_rows[0]

            fk_join_rows, _fkj_headers, _fkj_rowcount = self.db.rowdict(
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
            fk_join_row = next(iter(fk_join_rows))  # string_agg always returns exactly one aggregate row

            su_exists = (
                self.db.execute(
                    SQL(
                        """select * from information_schema.tables
                        where table_name = {table} and table_schema = {staging_schema};""",
                    ).format(
                        table=Literal(const_row["uq_table"]),
                        staging_schema=Literal(self.staging_schema),
                    ),
                ).rowcount
                > 0
            )

            query = SQL(
                """
                drop view if exists ups_fk_check cascade;
                create or replace temporary view ups_fk_check as
                select {s_checked}, count(*) as nrows
                from {staging_schema}.{table} as s
                left join {uq_schema}.{uq_table} as u on {u_join}
                """,
            ).format(
                s_checked=SQL(fk_join_row["s_checked"]),
                staging_schema=Identifier(self.staging_schema),
                table=Identifier(table),
                uq_schema=Identifier(const_row["uq_schema"]),
                uq_table=Identifier(const_row["uq_table"]),
                u_join=SQL(fk_join_row["u_join"]),
            )
            if su_exists:
                query += SQL(
                    """ left join {staging_schema}.{uq_table} as su on {su_join}""",
                ).format(
                    staging_schema=Identifier(self.staging_schema),
                    uq_table=Identifier(const_row["uq_table"]),
                    su_join=SQL(fk_join_row["su_join"]),
                )
            query += SQL(" where u.{uq_column} is null").format(
                uq_column=Identifier(const_row["uq_column"]),
            )
            if su_exists:
                query += SQL(" and su.{uq_column} is null").format(
                    uq_column=Identifier(const_row["uq_column"]),
                )
            query += SQL(
                """ and {s_not_null}
                    group by {s_checked};""",
            ).format(
                s_not_null=SQL(fk_join_row["s_not_null"]),
                s_checked=SQL(fk_join_row["s_checked"]),
            )
            self.db.execute(query)

            check_sql = SQL("select * from ups_fk_check;")
            fk_check_rows, fk_check_headers, fk_check_rowcount = self.db.rowdict(check_sql)
            if fk_check_rowcount > 0:
                fk_check_rows = list(fk_check_rows)
                fk_err_msg = f"Foreign key error referencing {const_row['uq_schema']}.{const_row['uq_table']}"
                display.print_check_table_fail(
                    self.staging_schema,
                    table,
                    fk_err_msg,
                    detail_rows=fk_check_rows,
                    detail_headers=fk_check_headers,
                )
                if fk_check_rows:
                    if interactive:
                        btn, _return_value = self._ui.show_table(
                            "Foreign key error",
                            f"Foreign key error referencing {const_row['uq_schema']}.{const_row['uq_table']}",
                            [
                                ("Continue", 0, "<Return>"),
                                ("Cancel", 1, "<Escape>"),
                            ],
                            fk_check_headers,
                            [[row[header] for header in fk_check_headers] for row in [fk_check_rows[0]]],
                        )
                        if btn != 0:
                            display.print_check_table_fail(self.staging_schema, table, "Script cancelled by user")
                            raise UserCancelledError("Script cancelled by user during foreign key check")
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
                            fkerror_count=Literal(fk_check_rows[0]["nrows"]),
                            constraint_name=Literal(constraint_row["constraint_name"]),
                            table_schema=Literal(constraint_row["table_schema"]),
                            table_name=Literal(constraint_row["table_name"]),
                        ),
                    )
                    err_detail = f"{constraint_row['constraint_name']} ({fk_check_rows[0]['nrows']})"
                    fk_error_strings.append(err_detail)
                    errors.append(
                        QAError(
                            table=table,
                            check_type=QACheckType.FOREIGN_KEY,
                            details=err_detail,
                        ),
                    )

        if fk_error_strings:
            self.control.set_qa_errors(table, "fk_errors", ",".join(fk_error_strings))
        else:
            display.print_check_table_pass(self.staging_schema, table)
        return errors

    def check_cks(self, table: str) -> list[QAError]:
        """Check for check-constraint violations in *table*.

        Creates temporary objects ``ups_check_constraints`` (once per session),
        ``ups_sel_cks``, ``ups_ck_check_check``, and ``ups_ck_error_list``.

        Args:
            table: The staging table name to check.

        Returns:
            A list of :class:`QAError` instances for any check-constraint violations.
        """
        errors: list[QAError] = []
        logger.debug(f"Conducting check constraint QA checks on table {self.staging_schema}.{table}")

        # Build full check-constraint map once per session.
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

        ck_rows, _ck_headers, _ck_rowcount = self.db.rowdict(
            SQL(
                """select constraint_name, table_schema, table_name, consrc
                from ups_sel_cks;""",
            ),
        )
        for ck_row in ck_rows:
            logger.debug(f"  Checking constraint {ck_row['constraint_name']}")
            const_rows, _const_headers, _const_rowcount = self.db.rowdict(
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
                    constraint_name=Literal(ck_row["constraint_name"]),
                    table_schema=Literal(ck_row["table_schema"]),
                    table_name=Literal(ck_row["table_name"]),
                ),
            )
            const_row = next(iter(const_rows))  # guarded: iterating over ck_rows from ups_sel_cks guarantees a match
            self.db.execute(
                SQL(
                    """
            create or replace temporary view ups_ck_check_check as
            select count(*) from {staging_schema}.{table}
            where not ({check_sql})
            """,
                ).format(
                    staging_schema=Identifier(self.staging_schema),
                    table=Identifier(table),
                    check_sql=SQL(const_row["check_sql"]),
                ),
            )
            ck_check_rows, _ck_check_headers, ck_check_rowcount = self.db.rowdict(
                "select * from ups_ck_check_check where count > 0;",
            )
            if ck_check_rowcount > 0:
                ck_check_row = next(iter(ck_check_rows))  # guarded by rowcount check above
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
                        ckerror_count=Literal(ck_check_row["count"]),
                        constraint_name=Literal(ck_row["constraint_name"]),
                        table_schema=Literal(ck_row["table_schema"]),
                        table_name=Literal(ck_row["table_name"]),
                    ),
                )

        # Build the error summary and update the control table directly.
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
        err_rows, _err_headers, err_rowcount = self.db.rowdict(
            "select * from ups_ck_error_list;",
        )
        if err_rowcount > 0:
            err_row = next(iter(err_rows))  # guarded by rowcount check above
            if err_row["ck_errors"]:
                error_str = err_row["ck_errors"]
                self.control.set_qa_errors(table, "ck_errors", error_str)
                display.print_check_table_fail(self.staging_schema, table, error_str)
                errors.append(
                    QAError(table=table, check_type=QACheckType.CHECK_CONSTRAINT, details=error_str),
                )
        if not errors:
            display.print_check_table_pass(self.staging_schema, table)
        return errors

    def check_unique(self, table: str, interactive: bool = False) -> list[QAError]:
        """Check for duplicate values in UNIQUE-constrained columns of *table*.

        Queries ``pg_constraint`` with ``contype='u'`` to find UNIQUE constraints
        on the base table, then checks the staging table for violations.

        Args:
            table: The staging table name to check.
            interactive: If ``True``, show a Tkinter dialog for any errors found.

        Returns:
            A list of :class:`QAError` instances for any UNIQUE violations found.
        """
        errors: list[QAError] = []
        logger.debug(f"Conducting unique constraint QA checks on table {self.staging_schema}.{table}")

        # Find all UNIQUE constraints on the base table (excluding PKs).
        self.db.execute(
            SQL(
                """
            drop table if exists ups_unique_constraints cascade;
            select
                con.conname as constraint_name,
                array_agg(att.attname order by u.ord) as column_names
            into temporary table ups_unique_constraints
            from pg_constraint con
            cross join lateral unnest(con.conkey) with ordinality as u(attnum, ord)
            inner join pg_attribute att
                on att.attrelid = con.conrelid and att.attnum = u.attnum
            inner join pg_class cls on cls.oid = con.conrelid
            inner join pg_namespace nsp on nsp.oid = cls.relnamespace
            where con.contype = 'u'
                and nsp.nspname = {base_schema}
                and cls.relname = {table}
            group by con.conname;
            """,
            ).format(base_schema=Literal(self.base_schema), table=Literal(table)),
        )

        uq_rows, _uq_headers, uq_rowcount = self.db.rowdict(
            "select * from ups_unique_constraints;",
        )
        if uq_rowcount == 0:
            logger.debug("  No unique constraints found")
            display.print_check_table_pass(self.staging_schema, table)
            return errors

        error_strings: list[str] = []
        for uq_row in uq_rows:
            constraint_name = uq_row["constraint_name"]
            col_names = uq_row["column_names"]  # list from array_agg
            logger.debug(f"  Checking unique constraint {constraint_name} on columns {col_names}")

            col_ids = SQL(",").join(Identifier(c) for c in col_names)
            # PostgreSQL allows multiple NULLs in UNIQUE columns, so exclude
            # rows where any constrained column is NULL.
            not_null_filter = SQL(" AND ").join(SQL("{col} IS NOT NULL").format(col=Identifier(c)) for c in col_names)
            self.db.execute(
                SQL(
                    """
                drop view if exists ups_uq_check cascade;
                create temporary view ups_uq_check as
                select {cols}, count(*) as nrows
                from {staging_schema}.{table}
                where {not_null_filter}
                group by {cols}
                having count(*) > 1;
                """,
                ).format(
                    cols=col_ids,
                    staging_schema=Identifier(self.staging_schema),
                    table=Identifier(table),
                    not_null_filter=not_null_filter,
                ),
            )
            uq_errs, uq_headers, uq_err_count = self.db.rowdict("select * from ups_uq_check;")
            if uq_err_count > 0:
                uq_errs = list(uq_errs)
                tot_rows, _th, _tc = self.db.rowdict(
                    "select count(*) as errcount, sum(nrows) as total_rows from ups_uq_check;",
                )
                tot_row = next(iter(tot_rows))  # guarded by uq_err_count > 0 check above
                err_detail = f"{constraint_name} ({tot_row['errcount']} duplicates, {tot_row['total_rows']} rows)"
                display.print_check_table_fail(
                    self.staging_schema,
                    table,
                    err_detail,
                    detail_rows=uq_errs,
                    detail_headers=uq_headers,
                )

                if interactive:
                    btn, _return_value = self._ui.show_table(
                        "Unique constraint error",
                        err_detail,
                        [
                            ("Continue", 0, "<Return>"),
                            ("Cancel", 1, "<Escape>"),
                        ],
                        uq_headers,
                        [[row[header] for header in uq_headers] for row in uq_errs],
                    )
                    if btn != 0:
                        display.print_check_table_fail(self.staging_schema, table, "Script cancelled by user")
                        raise UserCancelledError("Script cancelled by user during unique constraint check")

                error_strings.append(err_detail)
                errors.append(
                    QAError(table=table, check_type=QACheckType.UNIQUE, details=err_detail),
                )

        if error_strings:
            self.control.set_qa_errors(table, "unique_errors", ", ".join(error_strings))
        else:
            display.print_check_table_pass(self.staging_schema, table)
        return errors

    def check_column_existence(self, table: str) -> list[QAError]:
        """Check that all base table columns exist in the staging table.

        Columns listed in ``exclude_cols`` for this table are not flagged
        as missing.

        Args:
            table: The table name to check.

        Returns:
            A list of :class:`QAError` for any base columns missing from staging.
        """
        errors: list[QAError] = []
        logger.debug(f"Conducting column existence checks on table {self.staging_schema}.{table}")

        # Get exclude_cols for this table from the control table.
        spec = self.control.get_table_spec(table)
        exclude_cols: list[str] = []
        if spec and spec.get("exclude_cols"):
            exclude_cols = [c.strip() for c in spec["exclude_cols"].split(",")]

        # Find base columns missing from staging.
        self.db.execute(
            SQL(
                """
            drop view if exists ups_missing_cols cascade;
            create temporary view ups_missing_cols as
            select b.column_name
            from information_schema.columns b
            where b.table_schema = {base_schema}
                and b.table_name = {table}
                and b.column_name not in (
                    select s.column_name
                    from information_schema.columns s
                    where s.table_schema = {staging_schema}
                        and s.table_name = {table}
                )
            """,
            ).format(
                base_schema=Literal(self.base_schema),
                staging_schema=Literal(self.staging_schema),
                table=Literal(table),
            ),
        )

        missing_rows, _headers, missing_count = self.db.rowdict(
            "select * from ups_missing_cols;",
        )
        if missing_count == 0:
            display.print_check_table_pass(self.staging_schema, table)
            return errors

        # Filter out excluded columns.
        missing_cols = [row["column_name"] for row in missing_rows if row["column_name"] not in exclude_cols]
        if not missing_cols:
            display.print_check_table_pass(self.staging_schema, table)
            return errors

        err_detail = ", ".join(missing_cols)
        display.print_check_table_fail(self.staging_schema, table, f"missing columns: {err_detail}")
        self.control.set_qa_errors(table, "column_errors", err_detail)
        errors.append(
            QAError(table=table, check_type=QACheckType.COLUMN_EXISTENCE, details=err_detail),
        )
        return errors

    def check_type_mismatch(self, table: str) -> list[QAError]:
        """Check for hard type incompatibilities between staging and base columns.

        Only flags mismatches where PostgreSQL has no implicit or assignment
        cast between the types. Soft coercions (e.g., ``varchar`` to ``text``)
        are not flagged.

        Args:
            table: The table name to check.

        Returns:
            A list of :class:`QAError` for any type incompatibilities found.
        """
        errors: list[QAError] = []
        logger.debug(f"Conducting column type mismatch checks on table {self.staging_schema}.{table}")

        # Find columns present in both schemas with different types
        # where no implicit/assignment cast exists.
        self.db.execute(
            SQL(
                """
            drop view if exists ups_type_mismatches cascade;
            create temporary view ups_type_mismatches as
            select
                b.column_name,
                s.udt_name as staging_type,
                b.udt_name as base_type
            from information_schema.columns b
            inner join information_schema.columns s
                on s.table_schema = {staging_schema}
                and s.table_name = {table}
                and s.column_name = b.column_name
            where b.table_schema = {base_schema}
                and b.table_name = {table}
                and s.udt_name != b.udt_name
                and not exists (
                    select 1 from pg_cast
                    inner join pg_type src on src.oid = pg_cast.castsource
                    inner join pg_type tgt on tgt.oid = pg_cast.casttarget
                    where src.typname = s.udt_name
                        and tgt.typname = b.udt_name
                        and pg_cast.castcontext in ('i', 'a')
                );
            """,
            ).format(
                base_schema=Literal(self.base_schema),
                staging_schema=Literal(self.staging_schema),
                table=Literal(table),
            ),
        )

        mismatch_rows, _headers, mismatch_count = self.db.rowdict(
            "select * from ups_type_mismatches;",
        )
        if mismatch_count == 0:
            display.print_check_table_pass(self.staging_schema, table)
            return errors

        mismatch_details: list[str] = []
        for row in mismatch_rows:
            detail = f"{row['column_name']} ({row['staging_type']} → {row['base_type']})"
            mismatch_details.append(detail)

        err_detail = ", ".join(mismatch_details)
        display.print_check_table_fail(self.staging_schema, table, err_detail)
        self.control.set_qa_errors(table, "type_errors", err_detail)
        errors.append(
            QAError(table=table, check_type=QACheckType.TYPE_MISMATCH, details=err_detail),
        )
        return errors

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def run_all(
        self,
        tables: list[str] | tuple[str, ...],
        interactive: bool = False,
        compact: bool = False,
    ) -> list[QAError]:
        """Run all QA checks across *tables* and return every error found.

        Runs null, PK, FK, and check-constraint checks in order.  Between
        each check type the control table's ``processed`` flag is NOT used —
        instead we iterate directly over *tables* in Python.

        Args:
            tables: Ordered list of table names to check.
            interactive: If ``True``, show a Tkinter dialog when errors are found.

        Returns:
            A flat list of all :class:`QAError` instances across all tables and
            check types.
        """
        from datetime import datetime as _datetime

        from .utils import elapsed_time

        all_errors: list[QAError] = []

        check_types: list[tuple[str, bool]] = [
            ("Column Existence", False),
            ("Column Type", False),
            ("Non-NULL", False),
            ("Primary Key", True),
            ("Unique", True),
            ("Foreign Key", True),
            ("Check Constraint", False),
        ]
        check_funcs: dict[str, object] = {
            "Column Existence": self.check_column_existence,
            "Column Type": self.check_type_mismatch,
            "Non-NULL": self.check_nulls,
            "Primary Key": self.check_pks,
            "Unique": self.check_unique,
            "Foreign Key": self.check_fks,
            "Check Constraint": self.check_cks,
        }

        total_phases = len(check_types)
        for phase_num, (check_label, supports_interactive) in enumerate(check_types, 1):
            display.print_check_start(check_label, phase=phase_num, total_phases=total_phases)
            start_time = _datetime.now()
            check_func = check_funcs[check_label]
            for table in tables:
                table_errors = (
                    check_func(table, interactive=interactive)  # type: ignore[call-arg]
                    if supports_interactive
                    else check_func(table)  # type: ignore[call-arg]
                )
                all_errors.extend(table_errors)
            logger.debug(f"{check_label} checks completed in {elapsed_time(start_time)}")

        display.print_qa_summary(list(tables), all_errors, compact=compact)

        if self.control.has_errors() and interactive:
            ctrl_sql = SQL("select * from {control_table};").format(
                control_table=Identifier(self.control.table_name),
            )
            rows, headers, _rowcount = self.db.rowdict(ctrl_sql)
            rows = list(rows)
            self._ui.show_table(
                "QA Errors",
                "QA checks failed. Below is a summary of the errors:",
                [
                    ("Continue", 0, "<Return>"),
                    ("Cancel", 1, "<Escape>"),
                ],
                headers,
                [[row[header] for header in headers] for row in rows],
            )

        return all_errors
