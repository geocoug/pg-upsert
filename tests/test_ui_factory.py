"""Tests for pg_upsert.ui.factory — no database required."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from pg_upsert.ui.console import ConsoleBackend
from pg_upsert.ui.factory import get_ui_backend


class TestGetUiBackend:
    def test_console_backend(self):
        backend = get_ui_backend("_console")
        assert isinstance(backend, ConsoleBackend)

    def test_invalid_mode_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid --ui value"):
            get_ui_backend("invalid")

    def test_invalid_mode_message_shows_valid_options(self):
        with pytest.raises(ValueError, match="auto"):
            get_ui_backend("bad_mode")

    def test_textual_mode(self):
        # textual is installed in dev dependencies — should return TextualBackend
        from pg_upsert.ui.textual_backend import TextualBackend

        backend = get_ui_backend("textual")
        assert isinstance(backend, TextualBackend)

    def test_auto_no_display_returns_textual(self):
        """Without DISPLAY/WAYLAND_DISPLAY, auto falls back to TextualBackend."""
        from pg_upsert.ui.textual_backend import TextualBackend

        with patch.dict("os.environ", {}, clear=True):
            # Remove both display env vars if present
            import os

            env = {k: v for k, v in os.environ.items() if k not in ("DISPLAY", "WAYLAND_DISPLAY")}
            with patch.dict("os.environ", env, clear=True):
                backend = get_ui_backend("auto")
        assert isinstance(backend, TextualBackend)

    def test_auto_with_display_and_no_tkinter_returns_textual(self):
        """If DISPLAY is set but tkinter import fails, fall back to TextualBackend."""
        from pg_upsert.ui.textual_backend import TextualBackend

        with patch.dict("os.environ", {"DISPLAY": ":0"}), patch.dict("sys.modules", {"tkinter": None}):
            backend = get_ui_backend("auto")
        assert isinstance(backend, TextualBackend)

    def test_tkinter_mode_skipped_without_display(self):
        """get_ui_backend('tkinter') should import TkinterBackend without crashing on import."""
        # We just verify the import path works — actual display errors happen at show_* call time
        from pg_upsert.ui.tkinter_backend import TkinterBackend

        backend = get_ui_backend("tkinter")
        assert isinstance(backend, TkinterBackend)

    def test_all_valid_modes_do_not_raise_on_import(self):
        """All recognised modes should at minimum not raise ValueError."""
        valid_modes = ["_console", "textual"]
        for mode in valid_modes:
            backend = get_ui_backend(mode)
            assert backend is not None
