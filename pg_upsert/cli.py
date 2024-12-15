#!/usr/bin/env python

from __future__ import annotations

import argparse
import logging
import sys
import webbrowser
from pathlib import Path

import yaml

from .__version__ import __description__, __docs_url__, __title__, __version__
from .upsert import PgUpsert
from .utils import CustomLogFormatter

logger = logging.getLogger(__title__)
logger.propagate = False


def clparser() -> argparse.Namespace:
    """Command line interface for the upsert function."""
    parser = argparse.ArgumentParser(
        add_help=False,
        description=__description__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--help",
        action="help",
        help="show this help message and exit",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="display debug output",
    )
    parser.add_argument(
        "--docs",
        action="store_true",
        help="open the documentation in a web browser",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="suppress all console output",
    )
    parser.add_argument(
        "-l",
        "--logfile",
        type=Path,
        help="write log to LOGFILE",
    )
    parser.add_argument(
        "-e",
        "--exclude-columns",
        dest="exclude_columns",
        type=str,
        help="comma-separated list of columns to exclude from null checks",
    )
    parser.add_argument(
        "-n",
        "--null-columns",
        dest="null_columns",
        type=str,
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
        "--upsert-method",
        dest="upsert_method",
        default="upsert",
        choices=["upsert", "update", "insert"],
        help="method to use for upsert",
    )
    parser.add_argument(
        "-h",
        "--host",
        type=str,
        help="database host",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        help="database port",
    )
    parser.add_argument(
        "-d",
        "--database",
        type=str,
        help="database name",
    )
    parser.add_argument(
        "-u",
        "--user",
        type=str,
        help="database user",
    )
    parser.add_argument(
        "-s",
        "--staging-schema",
        dest="staging_schema",
        default="staging",
        type=str,
        help="staging schema name",
    )
    parser.add_argument(
        "-b",
        "--base-schema",
        dest="base_schema",
        default="public",
        type=str,
        help="base schema name",
    )
    parser.add_argument(
        "--encoding",
        default="utf-8",
        type=str,
        required=False,
        help="encoding of the database",
    )
    parser.add_argument(
        "-f",
        "--config-file",
        dest="config_file",
        type=Path,
        required=False,
        help="path to configuration yaml file",
    )
    parser.add_argument(
        "-t",
        "--tables",
        nargs="+",
        default=[],
        help="table name(s)",
    )
    return parser.parse_args()


def main() -> None:
    args = clparser()
    if args.docs:
        try:
            webbrowser.open(__docs_url__)
        except Exception as e:
            logger.error(e)
            raise
        sys.exit(0)
    if args.config_file:
        if args.config_file.resolve().exists():
            try:
                with open(args.config_file) as file:
                    config = yaml.safe_load(file)
            except Exception as e:
                logger.error(e)
                sys.exit(1)
        else:
            logger.error(f"Configuration file not found: {args.config_file}")
            sys.exit(1)
        # For each key in the configuration yaml, update the corresponding command line argument
        for key in config:
            if key in vars(args):
                if key == "logfile":
                    setattr(args, key, Path(config[key]))
                else:
                    setattr(args, key, config[key])
            else:
                logger.warning(
                    f"Invalid configuration key will be ignored in {args.config_file}: {key}",
                )
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
            file_handler.setFormatter(logging.Formatter("[%(asctime)s]: %(message)s"))
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
    logger.debug(f"{__title__} (v{__version__})")
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
            stg_schema=args.staging_schema,
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
