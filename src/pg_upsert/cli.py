#!/usr/bin/env python

from __future__ import annotations

import logging
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated

import psycopg2
import typer
import yaml
from rich import print as rprint

from .upsert import PgUpsert, UserCancelledError
from .utils import CustomLogFormatter

_TITLE = "pg_upsert"
_DOCS_URL = "https://pg-upsert.readthedocs.io/"
_DESCRIPTION = "Run QA checks on PostgreSQL staging tables then upsert data into base tables."
try:
    _VERSION = version(_TITLE)
except PackageNotFoundError:
    _VERSION = "unknown"

logger = logging.getLogger(_TITLE)
logger.propagate = False


app = typer.Typer(add_completion=False)


@app.command(help=_DESCRIPTION, no_args_is_help=True)
def cli(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-v",
            help="Display the version and exit.",
        ),
    ] = False,
    debug: Annotated[
        bool,
        typer.Option(
            "--debug",
            help="Display debug output.",
        ),
    ] = False,
    docs: Annotated[
        bool,
        typer.Option(
            "--docs",
            help="Open the documentation in a web browser.",
        ),
    ] = False,
    logfile: Annotated[
        Path | None,
        typer.Option(
            "-l",
            "--logfile",
            help="Write log messages to a log file.",
        ),
    ] = None,
    exclude_columns: Annotated[
        list[str] | None,
        typer.Option(
            "--exclude-columns",
            "-x",
            help="List of column names to exclude from the upsert process. These columns will not be updated or inserted to, however, they will still be checked during the QA process.",  # noqa
        ),
    ] = None,
    null_columns: Annotated[
        list[str] | None,
        typer.Option(
            "--null-columns",
            "-n",
            help="List of column names to exclude from the not-null check during the QA process. You may wish to exclude certain columns from null checks, such as auto-generated timestamps or serial columns as they may not be populated until after records are inserted or updated.",  # noqa
        ),
    ] = None,
    commit: Annotated[
        bool,
        typer.Option(
            "--commit",
            "-c",
            help="If True, changes will be committed to the database once the upsert process has completed successfully. If False, changes will be rolled back.",  # noqa
        ),
    ] = False,
    interactive: Annotated[
        bool,
        typer.Option(
            "--interactive",
            "-i",
            help="If True, the user will be prompted with multiple dialogs to confirm various steps during the upsert process. If False, the upsert process will run without user intervention.",  # noqa
        ),
    ] = False,
    upsert_method: Annotated[
        str,
        typer.Option(
            "--upsert-method",
            "-m",
            help="The method to use for upserting data. Must be one of 'upsert', 'update', or 'insert'.",
        ),
    ] = "upsert",
    host: Annotated[
        str | None,
        typer.Option(
            "--host",
            "-h",
            help="Database host.",
        ),
    ] = None,
    port: Annotated[
        int,
        typer.Option(
            "--port",
            "-p",
            help="Database port.",
        ),
    ] = 5432,
    database: Annotated[
        str | None,
        typer.Option(
            "--database",
            "-d",
            help="Database name.",
        ),
    ] = None,
    user: Annotated[
        str | None,
        typer.Option(
            "--user",
            "-u",
            help="Database user.",
        ),
    ] = None,
    staging_schema: Annotated[
        str | None,
        typer.Option(
            "--staging-schema",
            "-s",
            help="Name of the staging schema where tables are located which will be used for QA checks and upserts. Tables in the staging schema must have the same name as the tables in the base schema that they will be upserted to.",  # noqa
        ),
    ] = None,
    base_schema: Annotated[
        str | None,
        typer.Option(
            "--base-schema",
            "-b",
            help="Name of the base schema where tables are located which will be updated or inserted into.",
        ),
    ] = None,
    encoding: Annotated[
        str,
        typer.Option(
            "--encoding",
            "-e",
            help="The encoding to use for the database connection.",
        ),
    ] = "utf-8",
    config_file: Annotated[
        Path | None,
        typer.Option(
            "--config-file",
            "-f",
            help="Path to configuration YAML file",
        ),
    ] = None,
    # list of table names
    tables: Annotated[
        list[str] | None,
        typer.Option(
            "--table",
            "-t",
            help="Table name to process (repeatable).",
        ),
    ] = None,
    generate_config: Annotated[
        bool,
        typer.Option(
            "--generate-config",
            "-g",
            help="Generate a template configuration file. If any other options are provided, they will be included in the generated file.",  # noqa
        ),
    ] = False,
    output: Annotated[
        str,
        typer.Option(
            "--output",
            "-o",
            help="Output format: 'text' for human-readable logs (default), or 'json' for machine-parseable JSON.",
        ),
    ] = "text",
    check_schema: Annotated[
        bool,
        typer.Option(
            "--check-schema",
            help="Run column existence and type mismatch checks only, then exit.",  # noqa
        ),
    ] = False,
    compact: Annotated[
        bool,
        typer.Option(
            "--compact",
            help="Use compact grid format for QA summary (✓/✗ per check type per table).",
        ),
    ] = False,
    ui_mode: Annotated[
        str,
        typer.Option(
            "--ui",
            help="UI backend for interactive mode: 'auto' (default), 'tkinter', or 'textual'.",
        ),
    ] = "auto",
    export_failures: Annotated[
        Path | None,
        typer.Option(
            "--export-failures",
            help="Directory to write a QA failure fix sheet into. Files are named 'pg_upsert_failures_<table>.<ext>' for CSV, or 'pg_upsert_failures.<ext>' for JSON/XLSX.",  # noqa
        ),
    ] = None,
    export_format: Annotated[
        str,
        typer.Option(
            "--export-format",
            help="Fix sheet format: 'csv' (one file per table), 'json' (nested single file), or 'xlsx' (single workbook with sheets per table). Default: csv.",  # noqa
        ),
    ] = "csv",
    export_max_rows: Annotated[
        int,
        typer.Option(
            "--export-max-rows",
            help="Maximum rows to capture per check per table for the fix sheet (default 1000).",
        ),
    ] = 1000,
    strict_columns: Annotated[
        bool,
        typer.Option(
            "--strict-columns",
            help="Treat all missing staging columns as errors. By default only primary key and NOT NULL (no default) columns are errors; others are warnings.",  # noqa
        ),
    ] = False,
) -> None:
    args = SimpleNamespace(**locals())
    if args.version:
        rprint(f"[bold]{_TITLE}[/bold]: {_VERSION}")
        sys.exit(0)
    if args.docs:
        rprint(":link: Opening documentation in a web browser...")
        typer.launch(_DOCS_URL)
        sys.exit(0)
    if args.generate_config:
        with open((Path().cwd() / "pg-upsert.template.yaml").resolve(), "w") as file:
            del args.version, args.config_file, args.docs, args.generate_config
            if not args.logfile:
                args.logfile = Path(f"{_TITLE}.log").as_posix()
            yaml.dump(args.__dict__, file, sort_keys=False, indent=2, encoding="utf-8")
        rprint(
            ":file_folder: Configuration file generated: [bold green]pg-upsert.template.yaml[/bold green]",
        )
        sys.exit(0)
    if args.config_file:
        if args.config_file.resolve().exists():
            try:
                with open(args.config_file) as file:
                    config = yaml.safe_load(file)
            except (yaml.YAMLError, OSError) as e:
                rprint(f"Error reading configuration file: {e}")
                sys.exit(1)
        else:
            rprint(f"Configuration file not found: {args.config_file}")
            sys.exit(1)
        # For each key in the configuration yaml, update the corresponding command line argument
        # only when the CLI arg is still at its default value (config file loses to explicit args).
        _cli_defaults: dict[str, object] = {
            "debug": False,
            "logfile": None,
            "exclude_columns": None,
            "null_columns": None,
            "commit": False,
            "interactive": False,
            "upsert_method": "upsert",
            "host": None,
            "port": 5432,
            "database": None,
            "user": None,
            "staging_schema": None,
            "base_schema": None,
            "encoding": "utf-8",
            "tables": None,
            "output": "text",
            "check_schema": False,
            "compact": False,
            "ui_mode": "auto",
            "export_failures": None,
            "export_format": "csv",
            "export_max_rows": 1000,
            "strict_columns": False,
        }
        _path_keys = {"logfile", "export_failures"}
        for key in config:
            if key in args.__dict__:
                # Only override if the current value is the CLI default.
                if getattr(args, key) == _cli_defaults.get(key):
                    value = config[key]
                    if key in _path_keys:
                        # Path-typed options: accept str/Path, treat falsy as
                        # unset, reject other types with a clear error.
                        if value is None or value is False or value == "":
                            setattr(args, key, None)
                        elif isinstance(value, (str, Path)):
                            setattr(args, key, Path(value))
                        else:
                            rprint(
                                f"Invalid value for {key!r} in {args.config_file}: "
                                f"expected a path string, got {type(value).__name__}",
                            )
                            sys.exit(1)
                    else:
                        setattr(args, key, value)
            else:
                rprint(
                    f"Invalid configuration key will be ignored in {args.config_file}: {key}",
                )
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    if args.logfile:
        file_handler = logging.FileHandler(args.logfile, mode="a")
        if args.debug:
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(
                logging.Formatter(
                    "[%(asctime)s] %(levelname)-8s %(name)-20s %(lineno)-5s: %(message)s",
                ),
            )
        else:
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(file_handler)
        # Also attach the file handler to the display logger so that
        # display.print_* functions write plain-text to the logfile.
        logging.getLogger("pg_upsert.display").addHandler(file_handler)
    if args.output == "json":
        # Suppress all rich console output — only JSON goes to stdout.
        from .ui import display

        display.console.quiet = True
    else:
        stream_handler = logging.StreamHandler(sys.stdout)
        if args.debug:
            stream_handler.setLevel(logging.DEBUG)
            stream_handler.setFormatter(
                CustomLogFormatter(
                    "[%(asctime)s] %(levelname)s %(name)s %(lineno)s: %(message)s",
                ),
            )
        else:
            stream_handler.setLevel(logging.INFO)
            stream_handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(stream_handler)

    def _cli_error(msg: str) -> None:
        from .ui import display

        display.console.print(f"  [bold red]Error:[/bold red] {msg}")
        logging.getLogger("pg_upsert.display").error(msg)
        sys.exit(1)

    if not args.host:
        _cli_error("Database host is required.")
    if not args.database:
        _cli_error("Database name is required.")
    if not args.user:
        _cli_error("Database user is required.")
    if not args.staging_schema:
        _cli_error("Staging schema is required.")
    if not args.base_schema:
        _cli_error("Base schema is required.")
    if not args.tables:
        _cli_error("One or more table names are required.")
    logger.debug(f"{_TITLE}: {_VERSION}")
    if args.debug:
        logger.debug("Command line arguments:")
        for arg in vars(args):
            padding = " " * (max(map(len, vars(args))) - len(arg))
            logger.debug(f"{arg}:{padding} {getattr(args, arg)}")
    if args.exclude_columns and isinstance(args.exclude_columns, str):
        args.exclude_columns = [col.strip() for col in args.exclude_columns.split(",")]
    if args.null_columns and isinstance(args.null_columns, str):
        args.null_columns = [col.strip() for col in args.null_columns.split(",")]
    # Validate --ui before connecting (so the user doesn't enter a password for nothing).
    if args.ui_mode not in ("auto", "tkinter", "textual"):
        _cli_error(f"Invalid --ui value {args.ui_mode!r}. Must be one of: auto, tkinter, textual")
    # Validate --export-failures early: path must be a directory (or not yet exist).
    if args.export_failures:
        if args.export_format not in ("csv", "json", "xlsx"):
            _cli_error(
                f"Unsupported --export-format {args.export_format!r}. Supported: csv, json, xlsx",
            )
        _export_path = Path(args.export_failures)
        if _export_path.exists() and not _export_path.is_dir():
            _cli_error(
                f"--export-failures path {args.export_failures!s} exists but is not a directory. "
                "Pass a directory path (it will be created if missing).",
            )
    try:
        from urllib.parse import quote

        ups = PgUpsert(
            uri=(
                f"postgresql://{quote(args.user, safe='')}"
                f"@{quote(args.host, safe='')}:{args.port}"
                f"/{quote(args.database, safe='')}"
            ),
            encoding=args.encoding,
            tables=args.tables,
            staging_schema=args.staging_schema,
            base_schema=args.base_schema,
            do_commit=args.commit,
            upsert_method=args.upsert_method,
            interactive=args.interactive,
            exclude_cols=args.exclude_columns,
            exclude_null_check_cols=args.null_columns,
            ui_mode=args.ui_mode,
            compact=args.compact,
            capture_detail_rows=bool(args.export_failures),
            max_export_rows=args.export_max_rows,
            strict_columns=args.strict_columns,
        )
        if args.check_schema:
            from .ui import display

            _fl = logging.getLogger("pg_upsert.display")
            if args.output != "json":
                display.console.print()
                display.console.rule("[bold]Schema Check[/bold]", style="cyan")
            _fl.info("=== Schema Check ===")
            from .models import CheckContext

            errors = []
            total = len(args.tables)
            for i, table in enumerate(args.tables, 1):
                ctx = CheckContext(table_num=i, total_tables=total)
                col_errs = ups._qa.check_column_existence(table, ctx=ctx)
                type_errs = ups._qa.check_type_mismatch(table, ctx=ctx)
                table_errs = col_errs + type_errs
                if not table_errs and args.output != "json":
                    display.print_check_table_pass(args.staging_schema, table, ctx=ctx)
                errors.extend(table_errs)
            from .models import QASeverity

            _has_errors = any(e.severity == QASeverity.ERROR for e in errors)
            if args.output == "json":
                import json

                print(
                    json.dumps(
                        {"schema_compatible": not _has_errors, "errors": [e.to_dict() for e in errors]},
                        indent=2,
                    ),
                )
            else:
                _n_errors = sum(1 for e in errors if e.severity == QASeverity.ERROR)
                _n_warnings = sum(1 for e in errors if e.severity == QASeverity.WARNING)
                display.console.print()
                if _n_errors:
                    display.console.print(
                        f"  [bold red]Schema check failed:[/bold red] {_n_errors} error(s)"
                        + (f", {_n_warnings} warning(s)" if _n_warnings else "")
                        + " found.",
                    )
                    _fl.error(
                        f"Schema check failed: {_n_errors} error(s)"
                        + (f", {_n_warnings} warning(s)" if _n_warnings else "")
                        + " found.",
                    )
                elif _n_warnings:
                    display.console.print(
                        f"  [bold yellow]Schema check passed with warnings:[/bold yellow] "
                        f"{_n_warnings} warning(s) found.",
                    )
                    _fl.warning(f"Schema check passed with warnings: {_n_warnings} warning(s) found.")
                else:
                    display.console.print(
                        "  [bold green]Schema check passed:[/bold green] "
                        "all staging tables are compatible with base tables.",
                    )
                    _fl.info("Schema check passed: all staging tables are compatible with base tables.")
            sys.exit(1 if _has_errors else 0)
        result = ups.run()
        if args.output == "json":
            print(result.to_json())
        if args.export_failures:
            from .ui import display as _display

            if result.qa_passed:
                _display.console.print("  No QA failures to export.")
            else:
                exported = result.export_failures(args.export_failures, fmt=args.export_format)
                if exported:
                    _display.console.print(
                        f"  Failures exported to [bold]{exported}[/bold] ({args.export_format})",
                    )
                else:
                    _display.console.print("  No exportable failure rows.")
        # Exit 1 if QA failed — so CI pipelines detect failures.
        if not result.qa_passed:
            sys.exit(1)
    except UserCancelledError:
        sys.exit(0)
    except (ValueError, psycopg2.Error, OSError, yaml.YAMLError) as e:
        from .ui import display

        display.console.print(f"  [bold red]Error:[/bold red] {e}")
        logging.getLogger("pg_upsert.display").error(str(e))
        sys.exit(1)
    except Exception as e:
        from .ui import display

        display.console.print(f"  [bold red]Unexpected error:[/bold red] {e}")
        logger.debug("Traceback:", exc_info=True)
        logging.getLogger("pg_upsert.display").error(str(e))
        sys.exit(1)
