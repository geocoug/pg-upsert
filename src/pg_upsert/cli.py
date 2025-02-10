#!/usr/bin/env python

from __future__ import annotations

import logging
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated

import typer
import yaml
from rich import print

from .__version__ import __description__, __docs_url__, __title__, __version__
from .upsert import PgUpsert
from .utils import CustomLogFormatter

logger = logging.getLogger(__title__)
logger.propagate = False


app = typer.Typer(add_completion=False)


@app.command(help=__description__, no_args_is_help=True)
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
    quiet: Annotated[
        bool,
        typer.Option(
            "--quiet",
            "-q",
            help="Suppress all console output.",
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
            "--tables",
            "-t",
            help="Table names to perform QA checks on and upsert.",
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
) -> None:
    args = SimpleNamespace(**locals())
    if args.version:
        print(f"[bold]{__title__}[/bold]: {__version__}")
        sys.exit(0)
    if args.docs:
        print(":link: Opening documentation in a web browser...")
        typer.launch(__docs_url__)
        sys.exit(0)
    if args.generate_config:
        with open((Path().cwd() / "pg-upsert.template.yaml").resolve(), "w") as file:
            del args.version, args.config_file, args.docs, args.generate_config
            if not args.logfile:
                args.logfile = Path(f"{__title__}.log").as_posix()
            yaml.dump(args.__dict__, file, sort_keys=False, indent=2, encoding="utf-8")
        print(
            ":file_folder: Configuration file generated: [bold green]pg-upsert.template.yaml[/bold green]",
        )
        sys.exit(0)
    if args.config_file:
        if args.config_file.resolve().exists():
            try:
                with open(args.config_file) as file:
                    config = yaml.safe_load(file)
            except Exception as e:
                print(f"Error reading configuration file: {e}")
                sys.exit(1)
        else:
            print(f"Configuration file not found: {args.config_file}")
            sys.exit(1)
        # For each key in the configuration yaml, update the corresponding command line argument
        for key in config:
            if key in args.__dict__:
                if key == "logfile":
                    setattr(args, key, Path(config[key]))
                else:
                    setattr(args, key, config[key])
            else:
                print(
                    f"Invalid configuration key will be ignored in {args.config_file}: {key}",
                )
    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    if args.logfile:
        file_handler = logging.FileHandler(args.logfile)
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
    if not args.quiet:
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
    if not args.host:
        logger.error("Database host is required.")
        sys.exit(1)
    if not args.database:
        logger.error("Database name is required.")
        sys.exit(1)
    if not args.user:
        logger.error("Database user is required.")
        sys.exit(1)
    if not args.staging_schema:
        logger.error("Staging schema is required.")
        sys.exit(1)
    if not args.base_schema:
        logger.error("Base schema is required.")
        sys.exit(1)
    if not args.tables:
        logger.error("One or more table names are required.")
        sys.exit(1)
    if args.logfile and args.logfile.exists():
        try:
            args.logfile.unlink()
        except Exception as err:
            logger.error(err)
    logger.debug(f"{__title__}: {__version__}")
    if args.debug:
        logger.debug("Command line arguments:")
        for arg in vars(args):
            padding = " " * (max(map(len, vars(args))) - len(arg))
            logger.debug(f"{arg}:{padding} {getattr(args, arg)}")
    if args.exclude_columns and isinstance(args.exclude_columns, str):
        args.exclude_columns = args.exclude_columns.split(",")
    if args.null_columns and isinstance(args.null_columns, str):
        args.null_columns = args.null_columns.split(",")
    try:
        PgUpsert(
            uri=f"postgresql://{args.user}@{args.host}:{args.port}/{args.database}",
            encoding="utf-8",
            tables=args.tables,
            staging_schema=args.staging_schema,
            base_schema=args.base_schema,
            do_commit=args.commit,
            upsert_method=args.upsert_method,
            interactive=args.interactive,
            exclude_cols=args.exclude_columns,
            exclude_null_check_cols=args.null_columns,
        ).run()
    except Exception as e:
        logger.error(e)
        sys.exit(1)
