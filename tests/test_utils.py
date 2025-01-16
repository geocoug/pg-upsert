#!/usr/bin/env python

import logging
from datetime import datetime, timedelta

from pg_upsert.utils import CustomLogFormatter, elapsed_time

logger = logging.getLogger(__name__)


def test_elapsed_time():
    "Test that the elapsed_time function returns the correct string."
    assert isinstance(elapsed_time(datetime.now() - timedelta(seconds=5)), str)
    assert elapsed_time(datetime.now() - timedelta(seconds=5)) == "5.0 seconds"
    assert elapsed_time(datetime.now() - timedelta(minutes=1)) == "1 minutes, 0.0 seconds"
    assert elapsed_time(datetime.now() - timedelta(hours=1)) == "1 hours, 0 minutes, 0.0 seconds"
    assert elapsed_time(datetime.now() - timedelta(hours=1, minutes=1, seconds=1)) == "1 hours, 1 minutes, 1.0 seconds"
    assert elapsed_time(datetime.now() - timedelta(days=2)) == "48 hours, 0 minutes, 0.0 seconds"


def test_custom_log_formatter():
    "Test that the CustomLogFormatter class formats log messages correctly."
    formatter = CustomLogFormatter(
        "%(asctime)s %(levelname)-8s %(name)-20s %(lineno)-5s %(message)s",
    )
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=42,
        msg="Test message",
        args=(),
        exc_info=None,
    )
    assert (
        formatter.format(record)
        == f"{record.asctime} {CustomLogFormatter.COLORS['INFO']}INFO    {CustomLogFormatter.COLORS['RESET']} test                 42    Test message"  # noqa: E501
    )
    record.levelname = "DEBUG"
    assert (
        formatter.format(record)
        == f"{record.asctime} {CustomLogFormatter.COLORS['DEBUG']}DEBUG   {CustomLogFormatter.COLORS['RESET']} test                 42    Test message"  # noqa: E501
    )
