#!/usr/bin/env python
"""Tests for pg_upsert.utils."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from pg_upsert.utils import CustomLogFormatter, elapsed_time

# ---------------------------------------------------------------------------
# elapsed_time
# ---------------------------------------------------------------------------


class TestElapsedTime:
    def test_seconds(self):
        result = elapsed_time(datetime.now() - timedelta(seconds=5))
        assert result.startswith("5.") and result.endswith("seconds")

    def test_minutes(self):
        result = elapsed_time(datetime.now() - timedelta(minutes=1))
        assert result.startswith("1 minutes, 0.")

    def test_hours(self):
        result = elapsed_time(datetime.now() - timedelta(hours=1))
        assert result.startswith("1 hours, 0 minutes, 0.")

    def test_hours_minutes_seconds(self):
        result = elapsed_time(datetime.now() - timedelta(hours=1, minutes=1, seconds=1))
        assert result.startswith("1 hours, 1 minutes, 1.")

    def test_days_as_hours(self):
        result = elapsed_time(datetime.now() - timedelta(days=2))
        assert result.startswith("48 hours, 0 minutes, 0.")

    def test_returns_string(self):
        assert isinstance(elapsed_time(datetime.now() - timedelta(seconds=5)), str)


# ---------------------------------------------------------------------------
# CustomLogFormatter
# ---------------------------------------------------------------------------


class TestCustomLogFormatter:
    def _make_record(self, level=logging.INFO, msg="Test message"):
        return logging.LogRecord(
            name="test",
            level=level,
            pathname="test.py",
            lineno=42,
            msg=msg,
            args=(),
            exc_info=None,
        )

    def test_info_format(self):
        formatter = CustomLogFormatter(
            "%(asctime)s %(levelname)-8s %(name)-20s %(lineno)-5s %(message)s",
        )
        record = self._make_record(logging.INFO)
        formatted = formatter.format(record)
        assert CustomLogFormatter.COLORS["INFO"] in formatted
        assert CustomLogFormatter.COLORS["RESET"] in formatted
        assert "Test message" in formatted

    def test_debug_format(self):
        formatter = CustomLogFormatter(
            "%(asctime)s %(levelname)-8s %(name)-20s %(lineno)-5s %(message)s",
        )
        record = self._make_record(logging.DEBUG)
        formatted = formatter.format(record)
        assert CustomLogFormatter.COLORS["DEBUG"] in formatted

    def test_warning_format(self):
        formatter = CustomLogFormatter("%(levelname)s %(message)s")
        record = self._make_record(logging.WARNING, "warn msg")
        formatted = formatter.format(record)
        assert CustomLogFormatter.COLORS["WARNING"] in formatted

    def test_error_format(self):
        formatter = CustomLogFormatter("%(levelname)s %(message)s")
        record = self._make_record(logging.ERROR, "err msg")
        formatted = formatter.format(record)
        assert CustomLogFormatter.COLORS["ERROR"] in formatted

    def test_critical_format(self):
        formatter = CustomLogFormatter("%(levelname)s %(message)s")
        record = self._make_record(logging.CRITICAL, "crit msg")
        formatted = formatter.format(record)
        assert CustomLogFormatter.COLORS["CRITICAL"] in formatted
