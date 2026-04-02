"""UI subpackage for pg-upsert.

Provides the display utilities and interactive UI backends::

    from pg_upsert.ui import display, get_ui_backend, UIBackend
"""

from .base import UIBackend
from .factory import get_ui_backend

__all__ = ["UIBackend", "get_ui_backend", "display"]
