"""UI backend factory for pg-upsert.

Selects the appropriate :class:`~pg_upsert.ui_base.UIBackend` implementation
based on the requested mode and the runtime environment.
"""

from __future__ import annotations

from .base import UIBackend


def get_ui_backend(ui_mode: str = "auto") -> UIBackend:
    """Return the appropriate :class:`UIBackend` for *ui_mode*.

    Args:
        ui_mode: One of ``"auto"``, ``"tkinter"``, ``"textual"``, or
            ``"_console"`` (internal only — used for non-interactive runs).

            - ``"auto"`` — Use tkinter when a graphical display is available
              (``DISPLAY`` or ``WAYLAND_DISPLAY`` environment variable is set),
              otherwise use textual.
            - ``"tkinter"`` — Force the tkinter GUI backend.
            - ``"textual"`` — Force the textual TUI backend.
            - ``"_console"`` — Internal: non-interactive, auto-continues
              without prompting. Not exposed as a CLI option.

    Returns:
        A concrete :class:`UIBackend` instance.

    Raises:
        ValueError: If *ui_mode* is not one of the recognised values.
    """
    valid_modes = ("auto", "tkinter", "textual", "_console")
    public_modes = ("auto", "tkinter", "textual")
    if ui_mode not in valid_modes:
        raise ValueError(f"Invalid --ui value {ui_mode!r}. Must be one of: {', '.join(public_modes)}")

    if ui_mode == "_console":
        from .console import ConsoleBackend

        return ConsoleBackend()

    if ui_mode == "tkinter":
        from .tkinter_backend import TkinterBackend

        return TkinterBackend()

    if ui_mode == "textual":
        from .textual_backend import TextualBackend

        return TextualBackend()

    # ui_mode == "auto"
    import os

    if os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"):
        try:
            import tkinter  # noqa: F401

            from .tkinter_backend import TkinterBackend

            return TkinterBackend()
        except ImportError:
            pass

    from .textual_backend import TextualBackend

    return TextualBackend()
