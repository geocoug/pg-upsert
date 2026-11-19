"""Interaction tests for the Textual comparison dialog."""

import asyncio
from decimal import Decimal

import pytest
from textual.app import App
from textual.geometry import Offset
from textual.widgets import DataTable

from pg_upsert.ui.textual_backend import TextualBackend


@pytest.mark.parametrize("sidebyside", [False, True], ids=["stacked", "side-by-side"])
@pytest.mark.parametrize("diff_on", [False, True], ids=["without-diffs", "with-diffs"])
def test_comparison_highlights_visible_primary_key_match(monkeypatch, sidebyside, diff_on):
    staging_rows = [
        (7, "staging-only", "new"),
        (Decimal("1.1"), "shared", "unchanged"),
        (Decimal("1.1"), "changed", "updated"),
        (Decimal("8.2"), "ambiguous", "new"),
        ("8.2", "ambiguous", "new"),
    ]
    base_rows = [
        ("ambiguous", 8.2, "existing"),
        *[(f"filler-{index}", 100 + index, "existing") for index in range(29)],
        ("shared", 1.1, "unchanged"),
        ("changed", Decimal("1.10"), "original"),
        ("base-only", 9, "existing"),
    ]
    shared_row = len(base_rows) - 3
    changed_row = len(base_rows) - 2

    async def interact(app):
        async with app.run_test(size=(100, 24)) as pilot:
            staging = app.query_one("#stg-table", DataTable)
            base = app.query_one("#base-table", DataTable)

            def row_offset(table, row):
                return table.scrollable_content_region.offset + Offset(
                    2,
                    table.header_height + row - table.scroll_offset.y,
                )

            def row_style(table, row):
                offset = row_offset(table, row)
                assert table.scrollable_content_region.contains(*offset)
                style = app.screen.get_style_at(*offset)
                assert style.meta["row"] == row
                return style

            async def click_row(table, row):
                assert await pilot.click(f"#{table.id}", offset=row_offset(table, row) - table.region.offset)
                await pilot.pause()

            if diff_on:
                assert await pilot.click("#btn-diff", offset=(1, 1))
                await pilot.pause()
            assert str(base.get_row_at(changed_row)[2]).startswith("● ") == diff_on
            assert not row_style(base, 0).underline
            assert not base.scrollable_content_region.contains(*row_offset(base, changed_row))

            await click_row(staging, 2)
            assert app.focused is staging
            assert base.cursor_row == changed_row
            assert row_style(base, changed_row).underline
            assert not row_style(base, shared_row).underline

            await pilot.press("up")
            await pilot.pause()
            assert base.cursor_row == shared_row
            assert row_style(base, shared_row).underline

            await pilot.press("down", "down")
            await pilot.pause()
            assert staging.cursor_row == 3
            assert base.cursor_row == changed_row
            assert not row_style(base, changed_row).underline

            await pilot.press("up")
            await pilot.pause()
            assert row_style(base, changed_row).underline

            await click_row(base, shared_row)
            assert app.focused is base
            assert staging.cursor_row == 1
            assert row_style(staging, 1).underline
            assert not row_style(staging, 2).underline

            await pilot.press("down")
            await pilot.pause()
            assert staging.cursor_row == 2
            assert row_style(staging, 2).underline
            assert await pilot.click("#btn-diff", offset=(1, 1))
            await pilot.pause()
            assert row_style(staging, 2).underline
            assert str(staging.get_row_at(2)[2]).startswith("● ") != diff_on

    monkeypatch.setattr(App, "run", lambda app: asyncio.run(interact(app)))
    TextualBackend().show_comparison(
        "Review changes",
        "Compare staging and base",
        [("Cancel", 1, "<Escape>")],
        ["id", "region", "name"],
        staging_rows,
        ["region", "id", "name"],
        base_rows,
        ["id", "region"],
        sidebyside=sidebyside,
    )
