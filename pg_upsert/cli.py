#!/usr/bin/env python

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .__version__ import __description__, __title__, __version__
from .pg_upsert import PgUpsert

logger = logging.getLogger(__title__)
logger.setLevel(logging.INFO)
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
        "-q",
        "--quiet",
        action="store_true",
        help="suppress all console output",
    )
    parser.add_argument(
        "-l",
        "--log",
        type=Path,
        help="write log to LOGFILE",
    )
    parser.add_argument(
        "-e",
        "--exclude-columns",
        dest="exclude",
        type=str,
        help="comma-separated list of columns to exclude from null checks",
    )
    parser.add_argument(
        "-n",
        "--null-columns",
        dest="null",
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
        default="upsert",
        choices=["upsert", "update", "insert"],
        help="method to use for upsert",
    )
    parser.add_argument(
        "-h",
        "--host",
        required=True,
        type=str,
        help="database host",
    )
    parser.add_argument(
        "-p",
        "--port",
        required=True,
        type=int,
        help="database port",
    )
    parser.add_argument(
        "-d",
        "--database",
        required=True,
        type=str,
        help="database name",
    )
    parser.add_argument(
        "-u",
        "--user",
        required=True,
        type=str,
        help="database user",
    )
    parser.add_argument(
        "-s",
        "--staging-schema",
        default="staging",
        dest="stg_schema",
        required=True,
        type=str,
        help="staging schema name",
    )
    parser.add_argument(
        "-b",
        "--base-schema",
        default="public",
        dest="base_schema",
        required=True,
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
        "tables",
        nargs="+",
        help="table name(s)",
    )
    return parser.parse_args()


def main() -> None:
    args = clparser()
    if args.log and args.log.exists():
        args.log.unlink()
    if not args.quiet:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(stream_handler)
    if args.log:
        file_handler = logging.FileHandler(args.log)
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)
    if args.debug:
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(lineno)d: %(message)s",
        )
        for handler in logger.handlers:
            handler.setFormatter(formatter)
            handler.setLevel(logging.DEBUG)
        logger.debug("Command line arguments:")
        for arg in vars(args):
            padding = " " * (max(map(len, vars(args))) - len(arg))
            logger.debug(f"{arg}:{padding} {getattr(args, arg)}")
    try:
        PgUpsert(
            uri=f"postgresql://{args.user}@{args.host}:{args.port}/{args.database}",
            encoding="utf-8",
            tables=args.tables,
            stg_schema=args.stg_schema,
            base_schema=args.base_schema,
            do_commit=args.commit,
            upsert_method=args.upsert_method,
            interactive=args.interactive,
            exclude_cols=args.exclude.split(",") if args.exclude else None,
            exclude_null_check_cols=args.null.split(",") if args.null else None,
        ).run()
    except Exception as e:
        logger.error(e)
        sys.exit(1)
