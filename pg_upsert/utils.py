#!/usr/bin/env python

from __future__ import annotations

import logging
from datetime import datetime
from typing import ClassVar


class CustomLogFormatter(logging.Formatter):
    """
    Custom logging formatter that adds color and padding to the log messages.
    """

    COLORS: ClassVar[dict[str, str]] = {
        "DEBUG": "\033[94m",  # Blue
        "INFO": "\033[92m",  # Green
        "WARNING": "\033[93m",  # Yellow
        "ERROR": "\033[91m",  # Red
        "CRITICAL": "\033[41m",  # Red background
        "RESET": "\033[0m",  # Reset to default
    }

    def format(self: CustomLogFormatter, record: logging.LogRecord) -> str:
        self.datefmt = "%Y-%m-%d %H:%M:%S"
        self.asctime = self.formatTime(record, self.datefmt)
        record.name = f"{record.name:<20}"
        record.lineno = f"{record.lineno!s:<5}"
        # Get the color corresponding to the log level
        color = self.COLORS.get(record.levelname, self.COLORS["RESET"])
        # Apply color and padding to the log level name
        record.levelname = f"{color}{record.levelname:<8}{self.COLORS['RESET']}"
        return super().format(record)


def ellapsed_time(start_time: datetime) -> str:
    """Returns a string representing the ellapsed time since the start time.

    Args:
        start_time (datetime): The start time to compare the current time to.

    Returns:
        str: A string representing the ellapsed time since the start time.
    """

    dt = (datetime.now() - start_time).total_seconds()
    if dt < 60:
        return f"{round((datetime.now() - start_time).total_seconds(), 3)} seconds"
    if dt < 3600:
        return f"{int(dt // 60)} minutes, {round(dt % 60, 3)} seconds"
    return f"{int(dt // 3600)} hours, {int((dt % 3600)) // 60} minutes, {round(dt % 60, 3)} seconds"  # noqa: UP034
