#!/usr/bin/env python

from __future__ import annotations

import logging
import sys
from datetime import datetime

import psycopg2
from psycopg2.sql import SQL, Composable, Identifier, Literal
from tabulate import tabulate

from .__version__ import __description__, __version__
from .postgres import PostgresDB
from .ui import (
    CompareUI,
    TableUI,
)
from .utils import ellapsed_time

logger = logging.getLogger(__name__)


class PgUpsert:
    """
    Perform one or all of the following operations on a set of PostgreSQL tables:

    - Perform QA checks on data in a staging table or set of staging tables. QA checks include not-null, primary key, foreign key, and check constraint checks.
    - Perform updates and inserts (upserts) on a base table or set of base tables from the staging table(s) of the same name.

    PgUpsert utilizes temporary tables and views inside the PostgreSQL database to dynamically generate SQL for QA checks and upserts. All temporary objects are initialized with the `ups_` prefix.

    The upsert process is transactional. If any part of the process fails, the transaction will be rolled back. Committing changes to the database is optional and can be controlled with the `do_commit` flag.

    To avoid SQL injection, all SQL statements are generated using the [`psycopg2.sql`](https://www.psycopg.org/docs/sql.html) module.

    Args:
        uri (str or None, optional): Connection URI for the PostgreSQL database. Defaults to None. **Note**: If a connection URI is not provided, an existing connection object must be provided.
        conn (psycopg2.extensions.connection or None, optional): An existing connection object to the PostgreSQL database. Defaults to None. **Note**: If a connection object is not provided, a connection URI must be provided. If both are provided, the connection object will be used.
        encoding (str, optional): The encoding to use for the database connection. Defaults to "utf-8".
        tables (list or tuple or None, optional): List of table names to perform QA checks on and upsert. Defaults to ().
        stg_schema (str or None, optional): Name of the staging schema where tables are located which will be used for QA checks and upserts. Tables in the staging schema must have the same name as the tables in the base schema that they will be upserted to. Defaults to None.
        base_schema (str or None, optional): Name of the base schema where tables are located which will be updated or inserted into. Defaults to None.
        do_commit (bool, optional): If True, changes will be committed to the database once the upsert process is complete. If False, changes will be rolled back. Defaults to False.
        interactive (bool, optional): If True, the user will be prompted with multiple dialogs to confirm various steps during the upsert process. If False, the upsert process will run without user intervention. Defaults to False.
        upsert_method (str, optional): The method to use for upserting data. Must be one of "upsert", "update", or "insert". Defaults to "upsert".
        exclude_cols (list or tuple or None, optional): List of column names to exclude from the upsert process. These columns will not be updated or inserted to, however, they will still be checked during the QA process.
        exclude_null_check_cols (list or tuple or None, optional): List of column names to exclude from the not-null check during the QA process. You may wish to exclude certain columns from null checks, such as auto-generated timestamps or serial columns as they may not be populated until after records are inserted or updated. Defaults to ().
        control_table (str, optional): Name of the temporary control table that will be used to track changes during the upsert process. Defaults to "ups_control".

    Example:

    ```python
    from pg_upsert import PgUpsert

    ups  = PgUpsert(
        uri="postgresql://user@localhost:5432/database", # Note the missing password. pg_upsert will prompt for the password.
        tables=("genres", "books", "publishers", "authors", "book_authors"),
        stg_schema="staging",
        base_schema="public",
        do_commit=False,
        upsert_method="upsert",
        interactive=False,
        exclude_cols=("rev_user", "rev_time", "created_at", "updated_at"),
        exclude_null_check_cols=("rev_user", "rev_time", "created_at", "updated_at", "alias"),
    )
    ```

    """  # noqa: E501

    def __init__(
        self,
        uri: None | str = None,
        conn: None | psycopg2.extensions.connection = None,
        encoding: str = "utf-8",
        tables: list | tuple | None = (),
        stg_schema: str | None = None,
        base_schema: str | None = None,
        do_commit: bool = False,
        interactive: bool = False,
        upsert_method: str = "upsert",
        exclude_cols: list | tuple | None = (),
        exclude_null_check_cols: list | tuple | None = (),
        control_table: str = "ups_control",
    ):
        if upsert_method not in self._upsert_methods():
            raise ValueError(
                f"Invalid upsert method: {upsert_method}. Must be one of {self._upsert_methods()}",
            )
        if not base_schema or not stg_schema:
            if not base_schema and not stg_schema:
                raise ValueError("No base or staging schema specified")
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
            uri=uri,
            conn=conn,
            encoding=encoding,
        )
        logger.debug(f"Connected to {self.db!s}")
        self.tables = tables
        self.stg_schema = stg_schema
        self.base_schema = base_schema
        self.do_commit = do_commit
        self.interactive = interactive
        self.upsert_method = upsert_method
        self.exclude_cols = exclude_cols
        self.exclude_null_check_cols = exclude_null_check_cols
        self.control_table = control_table
        self.qa_passed = False
        self._validate_schemas()
        for table in self.tables:
            self._validate_table(table)
        self._init_ups_control()

    @staticmethod
    def _upsert_methods() -> tuple[str, str, str]:
        """Return a tuple of valid upsert methods.

        Returns:
            tuple: A tuple with a length of 3 containing the valid upsert methods.
        """
        return ("upsert", "update", "insert")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(db={self.db!r}, tables={self.tables}, stg_schema={self.stg_schema}, base_schema={self.base_schema}, do_commit={self.do_commit}, interactive={self.interactive}, upsert_method={self.upsert_method}, exclude_cols={self.exclude_cols}, exclude_null_check_cols={self.exclude_null_check_cols})"  # noqa: E501

    def _tabulate_sql(self, sql: str | Composable) -> None | str:
        """Tabulate the results of a SQL query and return the formatted Markdown table.

        Args:
            sql (str or Composable): The SQL query to execute.

        Returns:
            None or str: The formatted Markdown table, if results are found.
        """

        rows, headers, rowcount = self.db.rowdict(sql)
        if rowcount == 0:
            logger.info("No results found")
            return None
        return f"{tabulate(rows, headers='keys', tablefmt='github', showindex=False)}"

    def _validate_schemas(self: PgUpsert) -> None:
        """Validate that the base and staging schemas exist."""
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

    def _validate_table(self, table: str) -> None:
        """Utility script to validate one table in both base and staging schema.

        Halts script processing if any either of the schemas are non-existent,
        or if either of the tables are not present within those schemas pass.

        Args:
            table (str): The table to validate.
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

    def _validate_control(self: PgUpsert) -> None:
        """Validate contents of control table against base and staging schema.

        This method will check if the control table exists and if the tables specified in the control table exist in the base and staging schemas. If the control table does not exist, it will be created. If any of the tables specified in the control table do not exist in the base or staging schema, an error will be raised.

        **Objects created:**

        | table / view | description |
        | ------------ | ----------- |
        | `ups_validate_control` | Temporary table containing the results of the validation. |
        | `ups_ctrl_invl_table` | Temporary table containing the names of invalid tables. |
        """  # noqa: E501
        logger.debug("Validating control table")
        self._validate_schemas()
        # Check if the control table exists
        if (
            self.db.execute(
                SQL(
                    """
                select 1
                from information_schema.tables
                where table_name = {control_table}
            """,
                ).format(
                    base_schema=Literal(self.base_schema),
                    control_table=Literal(self.control_table),
                ),
            ).rowcount
            == 0
        ):
            self._init_ups_control()
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
            raise ValueError("Invalid table(s) specified")

    def _init_ups_control(self: PgUpsert) -> None:
        """Creates a table having the structure that is used to drive
        the upsert operation on multiple staging tables.

        The control table is used to track the progress of the upsert process, including the number of rows updated and inserted, and any errors that occur during the QA process.

        **Objects created:**

        The name of the control table is specified by the `control_table` parameter. The default name is `ups_control`. Below is an example of the control table structure:

        | table_name | exclude_cols | exclude_null_checks | interactive | null_errors | pk_errors | fk_errors | ck_errors | rows_updated | rows_inserted |
        |------------|--------------|---------------------|-------------|-------------|-----------|-----------|-----------|--------------|---------------|
        | table1     | col1,col2    | col3                | False       | col3 (n)    |           |           |           |              |               |

        The table definition is as follows:

        | column name          | data type | required | description |
        |----------------------|-----------|----------|-------------|
        | `table_name`         | text      | yes      | The name of the table to process. |
        | `exclude_cols`       | text      | no       | A comma-separated list of columns to exclude from the upsert process. |
        | `exclude_null_checks`| text      | no       | A comma-separated list of columns to exclude from the not-null check during the QA process. |
        | `interactive`        | boolean   | yes      | A flag to indicate whether the QA and upsert processes should be interactive. |
        | `null_errors`        | text      | no       | A comma-separated list of columns with null values. |
        | `pk_errors`          | text      | no       | A comma-separated list of primary key errors. |
        | `fk_errors`          | text      | no       | A comma-separated list of foreign key errors. |
        | `ck_errors`          | text      | no       | A comma-separated list of check constraint errors. |
        | `rows_updated`       | integer   | no       | The number of rows updated during the upsert process. |
        | `rows_inserted`      | integer   | no       | The number of rows inserted during the upsert process. |

        """  # noqa: E501
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
        # Update the control table with the list of columns to exclude from being updated or inserted to.
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
        # Update the control table with the list of columns to exclude from null checks.
        if self.exclude_null_check_cols and len(self.exclude_null_check_cols) > 0:
            self.db.execute(
                SQL(
                    """
                    update {control_table}
                    set exclude_null_checks = {exclude_null_check_cols};
                """,
                ).format(
                    control_table=Identifier(self.control_table),
                    exclude_null_check_cols=Literal(
                        ",".join(self.exclude_null_check_cols),
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
        debug_sql = SQL("select * from {control_table}").format(
            control_table=Identifier(self.control_table),
        )
        logger.debug(
            f"Control table after being initialized:\n{self._tabulate_sql(debug_sql)}",
        )

    def show_control(self: PgUpsert) -> None:
        """Display contents of the control table.

        If the `interactive` flag is set to `True`, the control table will be displayed in a Tkinter window. Otherwise, the results will be logged.

        The control table definition is as follows:

        | column name          | data type | required | description |
        |----------------------|-----------|----------|-------------|
        | `table_name`         | text      | yes      | The name of the table to process. |
        | `exclude_cols`       | text      | no       | A comma-separated list of columns to exclude from the upsert process. |
        | `exclude_null_checks`| text      | no       | A comma-separated list of columns to exclude from the not-null check during the QA process. |
        | `interactive`        | boolean   | yes      | A flag to indicate whether the QA and upsert processes should be interactive. |
        | `null_errors`        | text      | no       | A comma-separated list of columns with null values. |
        | `pk_errors`          | text      | no       | A comma-separated list of primary key errors. |
        | `fk_errors`          | text      | no       | A comma-separated list of foreign key errors. |
        | `ck_errors`          | text      | no       | A comma-separated list of check constraint errors. |
        | `rows_updated`       | integer   | no       | The number of rows updated during the upsert process. |
        | `rows_inserted`      | integer   | no       | The number of rows inserted during the upsert process. |
        """  # noqa: E501
        self._validate_control()
        sql = SQL("select * from {control_table};").format(
            control_table=Identifier(self.control_table),
        )
        if self.interactive:
            ctrl_rows, ctrl_headers, ctrl_rowcount = self.db.rowdict(sql)
            btn, return_value = TableUI(
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
            logger.info(f"Control table contents:\n{self._tabulate_sql(sql)}")

    def qa_all(self: PgUpsert) -> PgUpsert:
        """Performs QA checks for nulls in non-null columns, for duplicated
        primary key values, for invalid foreign keys, and invalid check constraints
        in a set of staging tables to be loaded into base tables.
        If there are failures in the QA checks, loading is not attempted.
        If the loading step is carried out, it is done within a transaction.

        The `null_errors`, `pk_errors`, `fk_errors`, `ck_errors` columns of the
        control table will be updated to identify any errors that occur,
        so that this information is available to the caller.

        The `rows_updated` and `rows_inserted` columns of the control table
        will be updated with counts of the number of rows affected by the
        upsert operation for each table.

        When the upsert operation updates the base table, all columns of the
        base table that are also in the staging table are updated.  The
        update operation does not test to see if column contents are different,
        and so does not update only those values that are different.

        This method runs [`PgUpsert`](pg_upsert.md) methods in the following order:

        1. [`PgUpsert.qa_all_null`](pg_upsert.md#pg_upsert.PgUpsert.qa_all_null)
        2. [`PgUpsert.qa_all_pk`](pg_upsert.md#pg_upsert.PgUpsert.qa_all_pk)
        3. [`PgUpsert.qa_all_fk`](pg_upsert.md#pg_upsert.PgUpsert.qa_all_fk)
        4. [`PgUpsert.qa_all_ck`](pg_upsert.md#pg_upsert.PgUpsert.qa_all_ck)

        **Objects created:**

        The following temporary objects are created during the QA process (in addition to all objects created by the individual QA methods called):

        | table / view | description |
        |--------------|-------------|
        | ups_proctables | Temporary table containing the list of tables to process. |
        | ups_toprocess  | Temporary view returning a single unprocessed table. |

        **Example:**

        ```python
        PgUpsert(
            uri="postgresql://user@localhost:5432/database",
            tables=("genres", "books", "publishers", "authors", "book_authors"),
            stg_schema="staging",
            base_schema="public",
            do_commit=False,
            interactive=False,
            exclude_cols=("rev_user", "rev_time", "created_at", "updated_at"),
            exclude_null_check_cols=("rev_user", "rev_time", "created_at", "updated_at", "alias"),
        ).qa_all()
        ```
        """  # noqa: E501
        self._validate_control()
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
                logger.debug(f"\n{self._tabulate_sql(ctrl)}")
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
            logger.debug(f"\n{self._tabulate_sql(ctrl)}")
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
                    [[row[header] for header in headers] for row in rows],
                ).activate()
            else:
                logger.error("===QA checks failed. Below is a summary of the errors===")
                logger.error(self._tabulate_sql(ctrl))
            return self
        self.qa_passed = True
        return self

    def qa_all_null(self: PgUpsert) -> PgUpsert:
        """Performs null checks for non-null columns in selected staging tables.

        This method will loop through all rows (tables) in the control table and call the [`PgUpsert.qa_one_null`](pg_upsert.md#pg_upsert.PgUpsert.qa_one_null) method for each table (row) in the control table.
        """  # noqa: E501
        while True:
            rows, headers, rowcount = self.db.rowdict(
                SQL("select * from ups_toprocess;"),
            )
            if rowcount == 0:
                break
            rows = next(iter(rows))
            self.qa_one_null(table=rows["table_name"])
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
        return self

    def qa_one_null(self: PgUpsert, table: str) -> PgUpsert:
        """Performs null checks for non-null columns in a single staging table.

        This method will loop through all non-null columns in the staging table and check for null values. If any null values are found in a non-null column, the control table will be updated to include the name of the column and the number of rows with null values (e.g., `column_name (nrows)`). If multiple columns have null values, the failing column names will be concatenated with a comma (`,`) separator in the control table.

        Args:
            table (str): The name of the staging table to check for null values.

        **Objects created:**

        | table / view | description |
        |--------------|-------------|
        | `ups_nonnull_cols` | Temporary table containing the non-null columns of the base table. |
        | `ups_qa_nonnull_col` | Temporary view containing the number of rows with nulls in the staging table. |
        | `ups_null_error_list` | Temporary view containing the list of null errors. |
        """  # noqa: E501
        logger.info(
            f"Conducting not-null QA checks on table {self.stg_schema}.{table}",
        )
        self._validate_table(table)
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
                    table_name=Literal(table),
                ),
            )
        return self

    def qa_all_pk(self: PgUpsert) -> PgUpsert:
        """Performs primary key checks for duplicated primary key values in selected staging tables.

        This method will loop through all rows (tables) in the control table and call the [`PgUpsert.qa_one_pk`](pg_upsert.md#pg_upsert.PgUpsert.qa_one_pk) method for each table (row) in the control table.
        """  # noqa: E501
        while True:
            rows, headers, rowcount = self.db.rowdict(
                SQL("select * from ups_toprocess;"),
            )
            if rowcount == 0:
                break
            rows = next(iter(rows))
            self.qa_one_pk(table=rows["table_name"])
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
        return self

    def qa_one_pk(self: PgUpsert, table: str) -> PgUpsert:
        """Performs primary key checks for duplicated primary key values in a single staging table.

        This method will check for duplicate primary key values in the staging table. If any duplicate primary key values are found, the control table will be updated to include the number of duplicate keys and the number of rows with duplicate keys (e.g., `n duplicate keys (n rows)`).

        Args:
            table (str): The name of the staging table to check for duplicate primary key values.

        **Objects created:**

        | table / view | description |
        |--------------|-------------|
        | `ups_primary_key_columns` | Temporary table containing the primary key columns of the base table. |
        | `ups_pk_check` | Temporary view containing the duplicate primary key values. |
        """  # noqa: E501
        pk_errors = []
        logger.info(
            f"Conducting primary key QA checks on table {self.stg_schema}.{table}",
        )
        self._validate_table(table)
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
            err_msg = f"{tot_errs['errcount']} duplicate keys ({tot_errs['total_rows']} rows) in table {self.stg_schema}.{table}"  # noqa: E501
            pk_errors.append(err_msg)
            logger.warning("")
            err_sql = SQL("select * from ups_pk_check;")
            logger.warning(f"{self._tabulate_sql(err_sql)}")
            logger.warning("")
            if self.interactive:
                btn, return_value = TableUI(
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
                    sys.exit(0)
        if len(pk_errors) > 0:
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
                    table_name=Literal(table),
                ),
            )
        return self

    def qa_all_fk(self: PgUpsert) -> PgUpsert:
        """Performs foreign key checks for invalid foreign key values in selected staging tables.

        This method will loop through all rows (tables) in the control table and call the [`PgUpsert.qa_one_fk`](pg_upsert.md#pg_upsert.PgUpsert.qa_one_fk) method for each table (row) in the control table.
        """  # noqa: E501
        while True:
            rows, headers, rowcount = self.db.rowdict(
                SQL("select * from ups_toprocess;"),
            )
            if rowcount == 0:
                break
            rows = next(iter(rows))
            self.qa_one_fk(table=rows["table_name"])
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
        return self

    def qa_one_fk(self: PgUpsert, table: str) -> PgUpsert:
        """Performs foreign key checks for invalid foreign key values in a single staging table.

        This method will check for invalid foreign key values in the staging table. If any invalid foreign key values are found, the control table will be updated to include the foreign key name that is violated and the number of rows with invalid foreign key values (e.g., `fk_column (n rows)`).

        Args:
            table (str): The name of the staging table to check for invalid foreign key values.

        **Objects created:**

        | table / view | description |
        |--------------|-------------|
        | `ups_foreign_key_columns` | Temporary table containing the foreign key columns of the base table. |
        | `ups_sel_fks` | Temporary table containing the foreign key relationships for the base table. |
        | `ups_fk_constraints` | Temporary table containing the unique constraint names for the table. |
        | `ups_one_fk` | Temporary table containing the foreign key relationships for the base table. |
        | `ups_fk_check` | Temporary view containing the invalid foreign key values. |
        """  # noqa: E501
        logger.info(
            f"Conducting foreign key QA checks on table {self.stg_schema}.{table}",
        )
        self._validate_table(table)
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
                logger.warning("")
                logger.warning(f"{self._tabulate_sql(check_sql)}")
                logger.warning("")
                if self.interactive:
                    btn, return_value = TableUI(
                        "Foreign key error",
                        f"Foreign key error referencing {const_rows['uq_schema']}.{const_rows['uq_table']}",
                        [
                            ("Continue", 0, "<Return>"),
                            ("Cancel", 1, "<Escape>"),
                        ],
                        fk_check_headers,
                        [[row[header] for header in fk_check_headers] for row in [fk_check_rows]],
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
            err_rows = list(err_rows)
            # If any 'fk_errors' key is not None in the list of dictionaries,
            # update the control table with the list of foreign key errors.
            if any(err["fk_errors"] for err in err_rows):
                self.db.execute(
                    SQL(
                        """
                    update {control_table}
                    set fk_errors = {fk_errors}
                    where table_name = {table_name};
                    """,
                    ).format(
                        control_table=Identifier(self.control_table),
                        fk_errors=Literal(
                            ",".join(
                                [err["fk_errors"] for err in err_rows if err["fk_errors"]],
                            ),
                        ),
                        table_name=Literal(table),
                    ),
                )
        return self

    def qa_all_ck(self: PgUpsert) -> PgUpsert:
        """Performs check constraint checks for invalid check constraint values in selected staging tables.

        This method will loop through all rows (tables) in the control table and call the [`PgUpsert.qa_one_ck`](pg_upsert.md#pg_upsert.PgUpsert.qa_one_ck) method for each table (row) in the control table.

        **Objects created:**

        | table / view | description |
        |--------------|-------------|
        | `ups_check_constraints` | Temporary table containing the check constraints of the base table. |
        | `ups_sel_cks` | Temporary table containing the check constraints for the base table. |
        | `ups_ck_check_check` | Temporary view containing the check constraint values. |
        | `ups_ck_error_list` | Temporary table containing the list of check constraint errors. |
        """  # noqa: E501
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
        return self

    def qa_one_ck(self: PgUpsert, table: str) -> PgUpsert:
        """Performs check constraint checks for invalid check constraint values in a single staging table.

        This method will check for invalid check constraint values in the staging table. If any invalid check constraint values are found, the control table will be updated to include the check constraint name that is violated and the number of rows with invalid check constraint values (e.g., `constraint_name (n rows)`).

        Args:
            table (str): The name of the staging table to check for invalid check constraint values.

        **Objects created:**

        | table / view | description |
        |--------------|-------------|
        | `ups_check_constraints` | Temporary table containing the check constraints of the base table. |
        | `ups_sel_cks` | Temporary table containing the check constraints for the base table. |
        | `ups_ck_check_check` | Temporary view containing the check constraint values. |
        | `ups_ck_error_list` | Temporary table containing the list of check constraint errors. |
        """  # noqa: E501
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
        return self

    def upsert_all(self: PgUpsert) -> PgUpsert:
        """Performs upsert operations on all selected tables in the base schema.

        This method will loop through all rows (tables) in the control table and call the [`PgUpsert.upsert_one`](pg_upsert.md#pg_upsert.PgUpsert.upsert_one) method for each table (row) in the control table.

        **Objects created:**

        | table / view | description |
        |--------------|-------------|
        | `ups_dependencies` | Temporary table containing the dependencies of the base schema. |
        | `ups_ordered_tables` | Temporary table containing the selected tables ordered by dependency. |
        | `ups_proctables` | Temporary table containing the selected tables with ordering information. |
        """  # noqa: E501
        self._validate_control()
        if not self.qa_passed:
            logger.warning(
                "QA checks have not been run or have failed. Continuing anyway.",
            )
        logger.info(f"===Starting upsert procedures (COMMIT={self.do_commit})===")
        # Get a table of all dependencies for the base schema.
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
        # Create a list of tables in the base schema ordered by dependency.
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
        # Create a list of the selected tables with ordering information.
        self.db.execute(
            SQL(
                """
        drop table if exists ups_proctables cascade;
        select
            ot.table_order,
            tl.table_name,
            tl.exclude_cols,
            tl.interactive,
            False::boolean as processed
        into
            temporary table ups_proctables
        from
            {control_table} as tl
            inner join ups_ordered_tables as ot on ot.table_name = tl.table_name
            ;
        """,
            ).format(control_table=Identifier(self.control_table)),
        )
        while True:
            # Create a view returning a single unprocessed table, in order.
            proc_rows, proc_headers, proc_rowcount = self.db.rowdict(
                SQL(
                    """
                select
                    table_name, exclude_cols, interactive
                from ups_proctables
                where not processed
                order by table_order
                limit 1;
                """,
                ),
            )
            if proc_rowcount == 0:
                break
            proc_rows = next(iter(proc_rows))
            self.upsert_one(proc_rows["table_name"])
            self.db.execute(
                SQL(
                    """
                    update ups_proctables
                    set processed = True
                    where table_name = {table_name};
                    """,
                ).format(table_name=Literal(proc_rows["table_name"])),
            )
        return self

    def upsert_one(self: PgUpsert, table: str) -> PgUpsert:
        """Performs an upsert operation on a single table.

        This method will perform an upsert operation on a single table. The upsert operation will update rows in the base table with rows from the staging table where the primary key values match. If no matching primary key values are found, the method will insert the row from the staging table into the base table. If the base table has a matching primary key value, the method will update the row in the base table with the values from the staging table.

        Args:
            table (str): The name of the table to upsert.

        **Objects created:**

        | table / view | description |
        |--------------|-------------|
        | `ups_cols` | Temporary table containing the columns to be updated. |
        | `ups_pks` | Temporary table containing the primary key columns. |
        | `ups_fk_check` | Temporary view containing the foreign key check. |
        | `ups_toprocess` | Temporary table containing the tables to be processed. |
        """  # noqa: E501
        rows_updated = 0
        rows_inserted = 0
        logger.info(f"Performing upsert on table {self.base_schema}.{table}")
        self._validate_table(table)

        spec_rows, spec_headers, spec_rowcount = self.db.rowdict(
            SQL(
                """
            select table_name, exclude_cols, interactive
            from {control_table}
            where table_name = {table};
            """,
            ).format(
                control_table=Identifier(self.control_table),
                table=Literal(table),
            ),
        )
        if spec_rowcount == 0:
            logger.warning(f"Table {table} not found in control table")
            return self
        spec_rows = next(iter(spec_rows))
        # Populate a (temporary) table with the names of the columns
        # in the base table that are to be updated from the staging table.
        # Include only those columns from staging table that are also in base table.
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
            stg_schema=Literal(self.stg_schema),
            table=Literal(table),
            base_schema=Literal(self.base_schema),
        )
        if spec_rows["exclude_cols"]:
            query += SQL(
                """
                and s.column_name not in ({exclude_cols})
                """,
            ).format(
                exclude_cols=SQL(",").join(
                    Literal(col) for col in spec_rows["exclude_cols"].split(",") if spec_rows["exclude_cols"]
                ),
            )
        query += SQL(" order by s.ordinal_position;")
        self.db.execute(query)
        # Populate a (temporary) table with the names of the primary key
        # columns of the base table.
        self.db.execute(
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
            ).format(table=Literal(table), base_schema=Literal(self.base_schema)),
        )
        # Get all base table columns that are to be updated into a comma-delimited list.
        all_col_list = self.db.execute(
            SQL(
                """
            select string_agg(column_name, ', ') as cols from ups_cols;""",
            ),
        ).fetchone()
        if not all_col_list:
            logger.warning("No columns found in base table")
            return self
        all_col_list = next(iter(all_col_list))
        # Get all base table columns that are to be updated into a
        # comma-delimited list with a "b." prefix.
        base_col_list = self.db.execute(
            SQL(
                """
            select string_agg('b.' || column_name, ', ') as cols
            from ups_cols;""",
            ),
        ).fetchone()
        if not base_col_list:
            logger.warning("No columns found in base table")
            return self
        base_col_list = next(iter(base_col_list))
        # Get all staging table column names for columns that are to be updated
        # into a comma-delimited list with an "s." prefix.
        stg_col_list = self.db.execute(
            SQL(
                """
            select string_agg('s.' || column_name, ', ') as cols
            from ups_cols;""",
            ),
        ).fetchone()
        if not stg_col_list:
            logger.warning("No columns found in staging table")
            return self
        stg_col_list = next(iter(stg_col_list))
        # Get the primary key columns in a comma-delimited list.
        pk_col_list = self.db.execute(
            SQL(
                """
            select string_agg(column_name, ', ') as cols
            from ups_pks;""",
            ),
        ).fetchone()
        if not pk_col_list:
            logger.warning("Base table has no primary key")
            return self
        pk_col_list = next(iter(pk_col_list))
        # Create a join expression for key columns of the base (b) and
        # staging (s) tables.
        join_expr = self.db.execute(
            SQL(
                """
            select
                string_agg('b.' || column_name || ' = s.' || column_name, ' and ') as expr
            from
                ups_pks;
            """,
            ),
        ).fetchone()
        if not join_expr:
            logger.warning("Base table has no primary key")
            return self
        # Create a FROM clause for an inner join between base and staging
        # tables on the primary key column(s).
        from_clause = SQL(
            """FROM {base_schema}.{table} as b
            INNER JOIN {stg_schema}.{table} as s ON {join_expr}""",
        ).format(
            base_schema=Identifier(self.base_schema),
            table=Identifier(table),
            stg_schema=Identifier(self.stg_schema),
            join_expr=SQL(join_expr[0]),
        )
        # Create SELECT queries to pull all columns with matching keys from both
        # base and staging tables.
        self.db.execute(
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
        # Prepare updates
        if self.upsert_method in ("upsert", "update"):
            stg_curs = self.db.execute("select * from ups_stgmatches;")
            if stg_curs.rowcount == 0:
                logger.debug(
                    "  No rows in staging table matching primary key in base table",
                )
            stg_cols = [col.name for col in stg_curs.description]
            stg_rowcount = stg_curs.rowcount
            stg_data = stg_curs.fetchall()
            nk_curs = self.db.execute("select * from ups_nk;")
            nk_rowcount = nk_curs.rowcount
            if stg_rowcount > 0 and nk_rowcount > 0:
                base_curs = self.db.execute("select * from ups_basematches;")
                if base_curs.rowcount == 0:
                    logger.debug(
                        "  No rows in base table matching primary key in staging table",
                    )
                    return self
                base_cols = [col.name for col in base_curs.description]
                base_data = base_curs.fetchall()
                if spec_rows["interactive"]:
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
                    logger.warning("Script cancelled by user")
                    sys.exit(0)
                if btn == 0:
                    do_updates = True
                    # Create an assignment expression to update non-key columns of the
                    # base table (un-aliased) from columns of the staging table (as s).
                    ups_expr = self.db.execute(
                        SQL(
                            """
                        select string_agg(
                            column_name || ' = s.' || column_name, ', '
                            ) as col
                        from ups_nk;
                        """,
                        ),
                    ).fetchone()
                    if not ups_expr:
                        logger.warning("Unexpected error in upsert_one")
                        return self
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
                        base_schema=Identifier(self.base_schema),
                        table=Identifier(table),
                        stg_schema=Identifier(self.stg_schema),
                        ups_expr=SQL(ups_expr[0]),
                        join_expr=SQL(join_expr[0]),
                    )
            else:
                logger.info("  No rows to update")

        # Prepare the inserts.
        do_inserts = False
        insert_stmt = None
        if self.upsert_method in ("upsert", "insert"):
            # Create a select statement to find all rows of the staging table
            # that are not in the base table.
            self.db.execute(
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
                    stg_schema=Identifier(self.stg_schema),
                    table=Identifier(table),
                    pk_col_list=SQL(pk_col_list),
                    base_schema=Identifier(self.base_schema),
                ),
            )
            # Prompt user to examine new data and continue or quit.
            new_curs = self.db.execute("select * from ups_newrows;")
            new_cols = [col.name for col in new_curs.description]
            new_rowcount = new_curs.rowcount
            new_data = new_curs.fetchall()
            if new_rowcount > 0:
                if spec_rows["interactive"]:
                    btn, return_value = TableUI(
                        "New Data",
                        f"Do you want to add these new data to the {self.base_schema}.{table} table?",
                        [
                            ("Continue", 0, "<Return>"),
                            ("Skip", 1, "<Escape>"),
                            ("Cancel", 2, "<Escape>"),
                        ],
                        new_cols,
                        new_data,
                    ).activate()
                else:
                    btn = 0
                if btn == 2:
                    logger.warning("Script cancelled by user")
                    sys.exit(0)
                if btn == 0:
                    do_inserts = True
                    # Create an insert statement.  No semicolon terminating generated SQL.
                    insert_stmt = SQL(
                        """
                        INSERT INTO {base_schema}.{table} ({all_col_list})
                        SELECT {all_col_list} FROM ups_newrows
                    """,
                    ).format(
                        base_schema=Identifier(self.base_schema),
                        table=Identifier(table),
                        all_col_list=SQL(all_col_list),
                    )
            else:
                logger.info("  No new data to insert")
        # Run the update and insert statements.
        if do_updates and update_stmt and self.upsert_method in ("upsert", "update"):
            logger.info(f"  Updating {self.base_schema}.{table}")
            logger.debug(f"    UPDATE statement for {self.base_schema}.{table}")
            logger.debug(f"{update_stmt.as_string(self.db.cursor())}")
            self.db.execute(update_stmt)
            rows_updated = stg_rowcount
            logger.info(f"    {rows_updated} rows updated")
        if do_inserts and insert_stmt and self.upsert_method in ("upsert", "insert"):
            logger.info(f"  Adding data to {self.base_schema}.{table}")
            logger.debug(f"    INSERT statement for {self.base_schema}.{table}")
            logger.debug(f"{insert_stmt.as_string(self.db.cursor())}")
            self.db.execute(insert_stmt)
            rows_inserted = new_rowcount
            logger.info(f"    {rows_inserted} rows inserted")
        # Move the update/insert counts into the control table.
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
                control_table=Identifier(self.control_table),
                rows_updated=Literal(rows_updated),
                rows_inserted=Literal(rows_inserted),
                table_name=Literal(table),
            ),
        )
        return self

    def run(self: PgUpsert) -> PgUpsert:
        """Run all QA checks and upsert operations.

        This method runs `PgUpsert` methods in the following order:

        1. [`PgUpsert.qa_all()`](pg_upsert.md#pg_upsert.PgUpsert.qa_all)
        2. [`PgUpsert.upsert_all()`](pg_upsert.md#pg_upsert.PgUpsert.upsert_all)
        3. [`PgUpsert.commit()`](pg_upsert.md#pg_upsert.PgUpsert.commit)
        """
        start_time = datetime.now()
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
                return self
        else:
            logger.info("Tables selected for upsert:")
            for table in self.tables:
                logger.info(f"  {table}")
        self._init_ups_control()
        self.qa_all()
        if self.qa_passed:
            self.upsert_all()
            self.commit()
        logger.debug(f"Upsert completed in {ellapsed_time(start_time)}")
        return self

    def commit(self: PgUpsert) -> PgUpsert:
        """Commits the transaction to the database and show a summary of changes.

        Changes are committed if the following criteria are met:

        - The `do_commit` flag is set to `True`.
        - All QA checks have passed (i.e., the `qa_passed` flag is set to `True`). Note that no checking is done to ensure that QA checks have been run.
        - The summary of changes shows that rows have been updated or inserted.
        - If the `interactive` flag is set to `True` and the `do_commit` flag is is set to `False`, the user is prompted to commit the changes and the user selects "Continue".
        """  # noqa: E501
        self._validate_control()
        final_ctrl_sql = SQL("select * from {control_table}").format(
            control_table=Identifier(self.control_table),
        )
        final_ctrl_rows, final_ctrl_headers, final_ctrl_rowcount = self.db.rowdict(
            final_ctrl_sql,
        )
        if self.interactive:
            btn, return_value = TableUI(
                "Upsert Summary",
                "Below is a summary of changes. Do you want to commit these changes? ",
                [
                    ("Continue", 0, "<Return>"),
                    ("Cancel", 1, "<Escape>"),
                ],
                final_ctrl_headers,
                [[row[header] for header in final_ctrl_headers] for row in final_ctrl_rows],
            ).activate()
        else:
            btn = 0
            logger.info("")
            logger.info("Summary of changes:")
            logger.info(self._tabulate_sql(final_ctrl_sql))

        logger.info("")

        if btn == 0:
            upsert_rows, upsert_headers, upsert_rowcount = self.db.rowdict(
                SQL(
                    "select * from {control_table} where rows_updated > 0 or rows_inserted > 0",
                ).format(control_table=Identifier(self.control_table)),
            )
            if upsert_rowcount == 0:
                logger.info("No changes to commit")
                self.db.rollback()
            else:
                if self.do_commit:
                    self.db.commit()
                    logger.info("Changes committed")
                else:
                    logger.info(
                        "The do_commit flag is set to FALSE, rolling back changes.",
                    )
                    self.db.rollback()
        else:
            logger.info("Rolling back changes")
            self.db.rollback()
        # self.db.close()
        return self
