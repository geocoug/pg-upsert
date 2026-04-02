#!/usr/bin/env python

from __future__ import annotations

import logging
from datetime import datetime

import psycopg2
from psycopg2.sql import SQL, Composable, Identifier, Literal
from tabulate import tabulate

from .control import ControlTable
from .executor import UpsertExecutor
from .models import QAError, TableResult, UpsertResult, UserCancelledError
from .postgres import PostgresDB
from .qa import QARunner
from .ui import TableUI
from .utils import elapsed_time

logger = logging.getLogger(__name__)

# Re-export for backward compatibility — UserCancelledError lives in models.py.
__all__ = ["PgUpsert", "UserCancelledError"]


class PgUpsert:
    """
    Perform one or all of the following operations on a set of PostgreSQL tables:

    - Perform QA checks on data in a staging table or set of staging tables. QA checks include not-null, primary key, foreign key, and check constraint checks.
    - Perform updates and inserts (upserts) on a base table or set of base tables from the staging table(s) of the same name.

    PgUpsert utilizes temporary tables and views inside the PostgreSQL database to dynamically generate SQL for QA checks and upserts. All temporary objects are initialized with the `ups_` prefix.

    The upsert process is transactional. If any part of the process fails, the transaction will be rolled back. Committing changes to the database is optional and can be controlled with the `do_commit` flag.

    All SQL statements are generated using the [`psycopg2.sql`](https://www.psycopg.org/docs/sql.html) module.

    Args:
        uri (str or None, optional): Connection URI for the PostgreSQL database. Defaults to None. **Note**: If a connection URI is not provided, an existing connection object must be provided.
        conn (psycopg2.extensions.connection or None, optional): An existing connection object to the PostgreSQL database. Defaults to None. **Note**: If a connection object is not provided, a connection URI must be provided. If both are provided, the connection object will be used.
        encoding (str, optional): The encoding to use for the database connection. Defaults to "utf-8".
        tables (list or tuple or None, optional): List of table names to perform QA checks on and upsert. Defaults to ().
        staging_schema (str or None, optional): Name of the staging schema where tables are located which will be used for QA checks and upserts. Tables in the staging schema must have the same name as the tables in the base schema that they will be upserted to. Defaults to None.
        base_schema (str or None, optional): Name of the base schema where tables are located which will be updated or inserted into. Defaults to None.
        do_commit (bool, optional): If True, changes will be committed to the database once the upsert process has completed successfully. If False, changes will be rolled back. Defaults to False.
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
        staging_schema="staging",
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
        staging_schema: str | None = None,
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
        if not base_schema or not staging_schema:
            if not base_schema and not staging_schema:
                raise ValueError("No base or staging schema specified")
            if not base_schema:
                raise ValueError("No base schema specified")
            if not staging_schema:
                raise ValueError("No staging schema specified")
        if not tables:
            raise ValueError("No tables specified")
        if staging_schema == base_schema:
            raise ValueError(
                f"Staging and base schemas must be different. Got {staging_schema} for both.",
            )
        self.db = PostgresDB(
            uri=uri,
            conn=conn,
            encoding=encoding,
        )
        logger.debug(f"Connected to {self.db!s}")
        self.tables = tables
        self.staging_schema = staging_schema
        self.base_schema = base_schema
        self.do_commit = do_commit
        self.interactive = interactive
        self.upsert_method = upsert_method
        self.exclude_cols = exclude_cols
        self.exclude_null_check_cols = exclude_null_check_cols
        self.control_table = control_table
        self.qa_passed = False

        # Validate schemas once (not twice — bug fix).
        self._validate_schemas()
        for table in self.tables:
            self._validate_table(table)

        # Initialise sub-components.
        self._control = ControlTable(self.db, table_name=control_table)
        self._qa = QARunner(
            db=self.db,
            control=self._control,
            staging_schema=staging_schema,
            base_schema=base_schema,
            exclude_null_check_cols=exclude_null_check_cols or (),
        )
        self._executor = UpsertExecutor(
            db=self.db,
            control=self._control,
            staging_schema=staging_schema,
            base_schema=base_schema,
            upsert_method=upsert_method,
        )

        self._control.initialize(
            tables=list(tables),
            exclude_cols=list(exclude_cols) if exclude_cols else None,
            exclude_null_check_cols=list(exclude_null_check_cols) if exclude_null_check_cols else None,
            interactive=interactive,
        )

    @staticmethod
    def _upsert_methods() -> tuple[str, str, str]:
        """Return a tuple of valid upsert methods.

        Returns:
            tuple: A tuple with a length of 3 containing the valid upsert methods.
        """
        return ("upsert", "update", "insert")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(db={self.db!r}, tables={self.tables}, staging_schema={self.staging_schema}, base_schema={self.base_schema}, do_commit={self.do_commit}, interactive={self.interactive}, upsert_method={self.upsert_method}, exclude_cols={self.exclude_cols}, exclude_null_check_cols={self.exclude_null_check_cols})"  # noqa: E501

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
        logger.debug(f"Validating schemas {self.base_schema} and {self.staging_schema}")
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

                    {staging_schema} as schema_name,
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
            staging_schema=Literal(self.staging_schema),
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
            f"Validating table {table} exists in {self.base_schema} and {self.staging_schema} schemas",
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
                    select {staging_schema} as schema_name,
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
            staging_schema=Literal(self.staging_schema),
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
        self._control.validate(self.base_schema, self.staging_schema)

    def _init_ups_control(self: PgUpsert) -> None:
        """Re-initialise the control table (delegates to ControlTable.initialize)."""
        self._control.initialize(
            tables=list(self.tables),
            exclude_cols=list(self.exclude_cols) if self.exclude_cols else None,
            exclude_null_check_cols=(list(self.exclude_null_check_cols) if self.exclude_null_check_cols else None),
            interactive=self.interactive,
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
        self._control.show(self.interactive)

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
            staging_schema="staging",
            base_schema="public",
            do_commit=False,
            interactive=False,
            exclude_cols=("rev_user", "rev_time", "created_at", "updated_at"),
            exclude_null_check_cols=("rev_user", "rev_time", "created_at", "updated_at", "alias"),
        ).qa_all()
        ```
        """  # noqa: E501
        self._validate_control()
        self._control.clear_results()
        self._qa.run_all(list(self.tables), interactive=self.interactive)
        if not self._control.has_errors():
            self.qa_passed = True
        return self

    def qa_all_null(self: PgUpsert) -> PgUpsert:
        """Performs null checks for non-null columns in selected staging tables."""
        for table in self.tables:
            self._qa.check_nulls(table)
        return self

    def qa_one_null(self: PgUpsert, table: str) -> PgUpsert:
        """Performs null checks for non-null columns in a single staging table.

        Args:
            table (str): The name of the staging table to check for null values.
        """
        self._validate_table(table)
        self._qa.check_nulls(table)
        return self

    def qa_all_pk(self: PgUpsert) -> PgUpsert:
        """Performs primary key checks for duplicated primary key values in selected staging tables."""
        for table in self.tables:
            self._qa.check_pks(table, interactive=self.interactive)
        return self

    def qa_one_pk(self: PgUpsert, table: str) -> PgUpsert:
        """Performs primary key checks for duplicated primary key values in a single staging table.

        Args:
            table (str): The name of the staging table to check for duplicate primary key values.
        """
        self._validate_table(table)
        self._qa.check_pks(table, interactive=self.interactive)
        return self

    def qa_all_fk(self: PgUpsert) -> PgUpsert:
        """Performs foreign key checks for invalid foreign key values in selected staging tables."""
        for table in self.tables:
            self._qa.check_fks(table, interactive=self.interactive)
        return self

    def qa_one_fk(self: PgUpsert, table: str) -> PgUpsert:
        """Performs foreign key checks for invalid foreign key values in a single staging table.

        Args:
            table (str): The name of the staging table to check for invalid foreign key values.
        """
        self._validate_table(table)
        self._qa.check_fks(table, interactive=self.interactive)
        return self

    def qa_all_ck(self: PgUpsert) -> PgUpsert:
        """Performs check constraint checks for invalid check constraint values in selected staging tables."""
        for table in self.tables:
            self._qa.check_cks(table)
        return self

    def qa_one_ck(self: PgUpsert, table: str) -> PgUpsert:
        """Performs check constraint checks for invalid check constraint values in a single staging table.

        Args:
            table (str): The name of the staging table to check for invalid check constraint values.
        """
        self._qa.check_cks(table)
        return self

    def qa_all_unique(self: PgUpsert) -> PgUpsert:
        """Performs unique constraint checks on all selected staging tables."""
        for table in self.tables:
            self._qa.check_unique(table, interactive=self.interactive)
        return self

    def qa_one_unique(self: PgUpsert, table: str) -> PgUpsert:
        """Performs unique constraint checks on a single staging table.

        Args:
            table (str): The name of the staging table to check.
        """
        self._validate_table(table)
        self._qa.check_unique(table, interactive=self.interactive)
        return self

    def qa_column_existence(self: PgUpsert) -> PgUpsert:
        """Checks that all base table columns exist in the staging tables.

        Respects the ``exclude_cols`` setting — excluded columns are not flagged.
        """
        for table in self.tables:
            self._qa.check_column_existence(table)
        return self

    def qa_type_mismatch(self: PgUpsert) -> PgUpsert:
        """Checks for hard type incompatibilities between staging and base columns.

        Only flags mismatches where PostgreSQL has no implicit or assignment cast.
        """
        for table in self.tables:
            self._qa.check_type_mismatch(table)
        return self

    def upsert_all(self: PgUpsert) -> PgUpsert:
        """Performs upsert operations on all selected tables in the base schema.

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
        # Sync any runtime change to upsert_method before delegating.
        self._executor.upsert_method = self.upsert_method
        self._executor.upsert_all(list(self.tables), interactive=self.interactive)
        return self

    def upsert_one(self: PgUpsert, table: str) -> PgUpsert:
        """Performs an upsert operation on a single table.

        Args:
            table (str): The name of the table to upsert.
        """
        self._validate_table(table)
        # Sync any runtime change to upsert_method before delegating.
        self._executor.upsert_method = self.upsert_method
        self._executor.upsert_one(table, interactive=self.interactive)
        return self

    def run(self: PgUpsert) -> UpsertResult:
        """Run all QA checks and upsert operations.

        This method runs `PgUpsert` methods in the following order:

        1. [`PgUpsert.qa_all()`](pg_upsert.md#pg_upsert.PgUpsert.qa_all)
        2. [`PgUpsert.upsert_all()`](pg_upsert.md#pg_upsert.PgUpsert.upsert_all)
        3. [`PgUpsert.commit()`](pg_upsert.md#pg_upsert.PgUpsert.commit)

        Returns:
            UpsertResult: Structured result containing QA outcomes and row counts.
        """
        start_time = datetime.now()
        logger.info(f"Upserting to {self.base_schema} from {self.staging_schema}")
        if self.interactive:
            logger.debug("Tables selected for upsert:")
            for table in self.tables:
                logger.debug(f"  {table}")
            btn, _return_value = TableUI(
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
                return UpsertResult(tables=[], committed=False)
        else:
            logger.info("Tables selected for upsert:")
            for table in self.tables:
                logger.info(f"  {table}")

        # Reset qa_passed and reinitialise the control table for a fresh run.
        self.qa_passed = False
        self._init_ups_control()

        committed = False
        qa_errors: list[QAError] = []
        table_results: list[TableResult] = []

        try:
            self._control.clear_results()
            qa_errors = self._qa.run_all(list(self.tables), interactive=self.interactive)
            if not self._control.has_errors():
                self.qa_passed = True
            if self.qa_passed:
                table_results = self._executor.upsert_all(
                    list(self.tables),
                    interactive=self.interactive,
                )
                committed = self._do_commit()
        except UserCancelledError:
            logger.info("Rolling back changes due to user cancellation")
            self.db.rollback()

        logger.debug(f"Upsert completed in {elapsed_time(start_time)}")

        # Merge QA errors into table results.
        error_map: dict[str, list[QAError]] = {}
        for err in qa_errors:
            error_map.setdefault(err.table, []).append(err)

        # Build a TableResult for every table (including QA-only runs).
        result_map: dict[str, TableResult] = {r.table_name: r for r in table_results}
        for table in self.tables:
            if table not in result_map:
                result_map[table] = TableResult(table_name=table)
            result_map[table].qa_errors = error_map.get(table, [])

        return UpsertResult(
            tables=[result_map[t] for t in self.tables if t in result_map],
            committed=committed,
        )

    def _do_commit(self) -> bool:
        """Handle the commit/rollback decision and display the summary.

        Returns:
            bool: Whether the transaction was actually committed.
        """
        self._validate_control()
        final_ctrl_sql = SQL("select * from {control_table}").format(
            control_table=Identifier(self.control_table),
        )
        final_ctrl_rows, final_ctrl_headers, _final_ctrl_rowcount = self.db.rowdict(
            final_ctrl_sql,
        )
        final_ctrl_rows = list(final_ctrl_rows)
        if self.interactive:
            btn, _return_value = TableUI(
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
            upsert_rows, _upsert_headers, upsert_rowcount = self.db.rowdict(
                SQL(
                    "select * from {control_table} where rows_updated > 0 or rows_inserted > 0",
                ).format(control_table=Identifier(self.control_table)),
            )
            if upsert_rowcount == 0:
                logger.info("No changes to commit")
                self.db.rollback()
                return False
            if self.do_commit:
                self.db.commit()
                logger.info("Changes committed")
                return True
            logger.info("The commit flag is set to FALSE, rolling back changes.")
            self.db.rollback()
            return False
        logger.info("Rolling back changes")
        self.db.rollback()
        return False

    def commit(self: PgUpsert) -> PgUpsert:
        """Commits the transaction to the database and show a summary of changes.

        Changes are committed if the following criteria are met:

        - The `do_commit` flag is set to `True`.
        - All QA checks have passed (i.e., the `qa_passed` flag is set to `True`). Note that no checking is done to ensure that QA checks have been run.
        - The summary of changes shows that rows have been updated or inserted.
        - If the `interactive` flag is set to `True` and the `do_commit` flag is is set to `False`, the user is prompted to commit the changes and the user selects "Continue".
        """  # noqa: E501
        self._do_commit()
        return self
