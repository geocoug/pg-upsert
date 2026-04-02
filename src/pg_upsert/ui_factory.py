"""UI backend factory for pg-upsert.

Selects the appropriate :class:`~pg_upsert.ui_base.UIBackend` implementation
based on the requested mode and the runtime environment.
"""

from __future__ import annotations

from .ui_base import UIBackend


def get_ui_backend(ui_mode: str = "auto") -> UIBackend:
    """Return the appropriate :class:`UIBackend` for *ui_mode*.

    Args:
        ui_mode: One of ``"auto"``, ``"console"``, ``"tkinter"``, or
            ``"textual"``.

            - ``"auto"`` — Try tkinter when a graphical display is available
              (``DISPLAY`` or ``WAYLAND_DISPLAY`` environment variable is set),
              then fall back to textual if installed, then fall back to console.
            - ``"console"`` — Always use the console (non-interactive, no prompts).
            - ``"tkinter"`` — Force the tkinter GUI backend.
            - ``"textual"`` — Force the textual TUI backend.

    Returns:
        A concrete :class:`UIBackend` instance.

    Raises:
        ValueError: If *ui_mode* is not one of the recognised values.
    """
    valid_modes = ("auto", "console", "tkinter", "textual")
    if ui_mode not in valid_modes:
        raise ValueError(f"Invalid ui_mode {ui_mode!r}. Must be one of {valid_modes}.")

    if ui_mode == "console":
        from .ui_console import ConsoleBackend

        return ConsoleBackend()

    if ui_mode == "tkinter":
        from .ui_tkinter import TkinterBackend

        return TkinterBackend()

    if ui_mode == "textual":
        from .ui_textual import TextualBackend

        return TextualBackend()

    # ui_mode == "auto"
    import os

    if os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"):
        try:
            import tkinter  # noqa: F401

            from .ui_tkinter import TkinterBackend

            return TkinterBackend()
        except ImportError:
            pass

    try:
        import textual  # noqa: F401

        from .ui_textual import TextualBackend

        return TextualBackend()
    except ImportError:
        pass

    from .ui_console import ConsoleBackend

    return ConsoleBackend()
