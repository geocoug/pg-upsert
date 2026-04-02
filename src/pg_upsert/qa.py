"""QA check logic for pg-upsert staging tables."""

from __future__ import annotations

import logging

from psycopg2.sql import SQL, Identifier, Literal
from tabulate import tabulate

from .control import ControlTable
from .models import QACheckType, QAError, UserCancelledError
from .postgres import PostgresDB
from .ui import TableUI

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
    ) -> None:
        self.db = db
        self.control = control
        self.staging_schema = staging_schema
        self.base_schema = base_schema
        self.exclude_null_check_cols: list[str] | tuple[str, ...] = exclude_null_check_cols or ()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _tabulate_sql(self, sql: str | SQL) -> str | None:
        """Execute *sql* and return a formatted Markdown table.

        Args:
            sql: The SQL query to execute.

        Returns:
            A formatted GitHub-flavoured Markdown table string, or ``None``.
        """
        rows, headers, rowcount = self.db.rowdict(sql)
        if rowcount == 0:
            return None
        return f"{tabulate(rows, headers='keys', tablefmt='github', showindex=False)}"

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
        logger.info(f"Conducting not-null QA checks on table {self.staging_schema}.{table}")

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
                and column_name not in ({exclude_null_check_cols});
            """,
            ).format(
                base_schema=Literal(self.base_schema),
                table=Literal(table),
                exclude_null_check_cols=(
                    SQL(",").join(Literal(col) for col in self.exclude_null_check_cols)
                    if self.exclude_null_check_cols
                    else Literal("")
                ),
            ),
        )

        # Iterate over non-nullable columns using a Python loop.
        col_rows, _col_headers, _col_rowcount = self.db.rowdict(
            SQL("select * from ups_nonnull_cols;"),
        )
        for col_row in col_rows:
            column_name = col_row["column_name"]
            logger.debug(f"  Checking column {column_name} for nulls")
            self.db.execute(
                SQL(
                    """
                create or replace temporary view ups_qa_nonnull_col as
                select nrows
                from (
                    select count(*) as nrows
                    from {staging_schema}.{table}
                    where {column_name} is null
                    ) as nullcount
                where nrows > 0
                limit 1;
                """,
                ).format(
                    staging_schema=Identifier(self.staging_schema),
                    table=Identifier(table),
                    column_name=Identifier(column_name),
                ),
            )
            null_rows, _null_headers, null_rowcount = self.db.rowdict(
                SQL("select * from ups_qa_nonnull_col;"),
            )
            if null_rowcount > 0:
                null_row = next(iter(null_rows))
                nrows = null_row["nrows"]
                logger.warning(f"    Column {column_name} has {nrows} null values")
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
                    ).format(column_name=Literal(column_name)),
                )

        # Build the error string from accumulated null counts.
        self.db.execute(
            """
            create or replace temporary view ups_null_error_list as
            select string_agg(column_name || ' (' || null_rows || ')', ', ') as null_errors
            from ups_nonnull_cols
            where coalesce(null_rows, 0) > 0;
        """,
        )
        err_rows, _err_headers, err_rowcount = self.db.rowdict(
            SQL("select * from ups_null_error_list;"),
        )
        if err_rowcount > 0:
            err_row = next(iter(err_rows))
            if err_row["null_errors"]:
                error_str = err_row["null_errors"]
                self.control.set_qa_errors(table, "null_errors", error_str)
                errors.append(
                    QAError(table=table, check_type=QACheckType.NULL, details=error_str),
                )
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
        logger.info(f"Conducting primary key QA checks on table {self.staging_schema}.{table}")

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
            tot_errs = next(iter(tot_errs))
            err_msg = f"{tot_errs['errcount']} duplicate keys ({tot_errs['total_rows']} rows) in table {self.staging_schema}.{table}"  # noqa: E501
            logger.warning("")
            pk_check_sql = SQL("select * from ups_pk_check;")
            logger.warning(self._tabulate_sql(pk_check_sql))
            logger.warning("")
            if interactive:
                btn, _return_value = TableUI(
                    "Duplicate key error",
                    err_msg,
                    [
                        ("Continue", 0, "<Return>"),
                        ("Cancel", 1, "<Escape>"),
                    ],
                    pk_headers,
                    [[row[header] for header in pk_headers] for row in pk_errs],
                ).activate()
                if btn != 0:
                    logger.warning("Script cancelled by user")
                    raise UserCancelledError("Script cancelled by user during primary key check")
            self.control.set_qa_errors(table, "pk_errors", err_msg)
            errors.append(
                QAError(table=table, check_type=QACheckType.PRIMARY_KEY, details=err_msg),
            )
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
        logger.info(f"Conducting foreign key QA checks on table {self.staging_schema}.{table}")

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
            fk_join_row = next(iter(fk_join_rows))

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
                logger.warning(
                    f"    Foreign key error referencing {const_row['uq_schema']}.{const_row['uq_table']}",
                )
                logger.warning("")
                logger.warning(f"{self._tabulate_sql(check_sql)}")
                logger.warning("")
                if fk_check_rows:
                    if interactive:
                        btn, _return_value = TableUI(
                            "Foreign key error",
                            f"Foreign key error referencing {const_row['uq_schema']}.{const_row['uq_table']}",
                            [
                                ("Continue", 0, "<Return>"),
                                ("Cancel", 1, "<Escape>"),
                            ],
                            fk_check_headers,
                            [[row[header] for header in fk_check_headers] for row in [fk_check_rows[0]]],
                        ).activate()
                        if btn != 0:
                            logger.warning("Script cancelled by user")
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
        logger.info(f"Conducting check constraint QA checks on table {self.staging_schema}.{table}")

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
            const_row = next(iter(const_rows))
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
                ck_check_row = next(iter(ck_check_rows))
                logger.warning(
                    f"    Check constraint {ck_row['constraint_name']} has {ck_check_rowcount} failing rows",
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
            err_row = next(iter(err_rows))
            if err_row["ck_errors"]:
                error_str = err_row["ck_errors"]
                self.control.set_qa_errors(table, "ck_errors", error_str)
                errors.append(
                    QAError(table=table, check_type=QACheckType.CHECK_CONSTRAINT, details=error_str),
                )
        return errors

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def run_all(
        self,
        tables: list[str] | tuple[str, ...],
        interactive: bool = False,
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

        check_types = [
            ("Non-NULL", self.check_nulls),
            ("Primary Key", self.check_pks),
            ("Foreign Key", self.check_fks),
            ("Check Constraint", self.check_cks),
        ]

        for check_label, check_func in check_types:
            logger.info(f"==={check_label} checks===")
            start_time = _datetime.now()
            for table in tables:
                if check_func in (self.check_pks, self.check_fks):
                    table_errors = check_func(table, interactive=interactive)  # type: ignore[call-arg]
                else:
                    table_errors = check_func(table)  # type: ignore[call-arg]
                all_errors.extend(table_errors)
            logger.debug(f"{check_label} checks completed in {elapsed_time(start_time)}")

        if self.control.has_errors():
            ctrl_sql = SQL("select * from {control_table};").format(
                control_table=Identifier(self.control.table_name),
            )
            if interactive:
                rows, headers, _rowcount = self.db.rowdict(ctrl_sql)
                rows = list(rows)
                TableUI(
                    "QA Errors",
                    "QA checks failed. Below is a summary of the errors:",
                    [
                        ("Continue", 0, "<Return>"),
                        ("Cancel", 1, "<Escape>"),
                    ],
                    headers,
                    [[row[header] for header in headers] for row in rows],
                ).activate()
            else:
                from tabulate import tabulate as _tabulate

                rows, headers, _rowcount = self.db.rowdict(ctrl_sql)
                logger.error("===QA checks failed. Below is a summary of the errors===")
                logger.error(
                    _tabulate(rows, headers="keys", tablefmt="github", showindex=False),
                )

        return all_errors
