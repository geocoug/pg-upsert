#!/usr/bin/env python

from __future__ import annotations

from datetime import datetime

from .__version__ import __description__, __version__


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
