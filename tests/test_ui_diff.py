"""Tests for pg_upsert.ui.diff — pure function, no UI required."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pg_upsert.ui.diff import DiffResult, compute_row_diffs

# ---------------------------------------------------------------------------
# All-match / all-changed / mixed cases
# ---------------------------------------------------------------------------


class TestAllMatch:
    def test_every_row_matches(self):
        headers = ["id", "name", "price"]
        stg = [(1, "A", 10), (2, "B", 20)]
        base = [(1, "A", 10), (2, "B", 20)]
        result = compute_row_diffs(headers, stg, headers, base, pk_cols=["id"])
        assert result.stg_row_states == ["match", "match"]
        assert result.base_row_states == ["match", "match"]
        assert all(len(s) == 0 for s in result.stg_changed_cols)
        assert "2 matching" in result.summary
        assert "0 differing" in result.summary


class TestAllChanged:
    def test_every_row_differs(self):
        headers = ["id", "name", "price"]
        stg = [(1, "A", 10), (2, "B", 20)]
        base = [(1, "X", 10), (2, "Y", 20)]
        result = compute_row_diffs(headers, stg, headers, base, pk_cols=["id"])
        assert result.stg_row_states == ["changed", "changed"]
        assert result.stg_changed_cols[0] == {"name"}
        assert result.stg_changed_cols[1] == {"name"}
        assert "2 differing" in result.summary

    def test_multiple_columns_change(self):
        headers = ["id", "name", "price"]
        stg = [(1, "A", 10)]
        base = [(1, "B", 20)]
        result = compute_row_diffs(headers, stg, headers, base, pk_cols=["id"])
        assert result.stg_changed_cols[0] == {"name", "price"}


class TestMixed:
    def test_some_match_some_change(self):
        headers = ["id", "name"]
        stg = [(1, "A"), (2, "B"), (3, "C")]
        base = [(1, "A"), (2, "X"), (3, "C")]
        result = compute_row_diffs(headers, stg, headers, base, pk_cols=["id"])
        assert result.stg_row_states == ["match", "changed", "match"]
        assert "2 matching" in result.summary
        assert "1 differing" in result.summary


# ---------------------------------------------------------------------------
# Native Python equality
# ---------------------------------------------------------------------------


class TestNativeEquality:
    def test_decimal_different_scales_equal(self):
        headers = ["id", "price"]
        stg = [(1, Decimal("9.99"))]
        base = [(1, Decimal("9.9900"))]
        result = compute_row_diffs(headers, stg, headers, base, pk_cols=["id"])
        assert result.stg_row_states == ["match"]

    def test_int_float_equal(self):
        headers = ["id", "qty"]
        stg = [(1, 5)]
        base = [(1, 5.0)]
        result = compute_row_diffs(headers, stg, headers, base, pk_cols=["id"])
        assert result.stg_row_states == ["match"]

    def test_str_vs_int_different(self):
        headers = ["id", "code"]
        stg = [(1, "5")]
        base = [(1, 5)]
        result = compute_row_diffs(headers, stg, headers, base, pk_cols=["id"])
        assert result.stg_row_states == ["changed"]

    def test_datetime_equal(self):
        headers = ["id", "ts"]
        stg = [(1, datetime(2025, 1, 1, 12, 0))]
        base = [(1, datetime(2025, 1, 1, 12, 0))]
        result = compute_row_diffs(headers, stg, headers, base, pk_cols=["id"])
        assert result.stg_row_states == ["match"]

    def test_datetime_different(self):
        headers = ["id", "ts"]
        stg = [(1, datetime(2025, 1, 1, 12, 0, 0))]
        base = [(1, datetime(2025, 1, 1, 12, 0, 1))]
        result = compute_row_diffs(headers, stg, headers, base, pk_cols=["id"])
        assert result.stg_row_states == ["changed"]


# ---------------------------------------------------------------------------
# None handling
# ---------------------------------------------------------------------------


class TestNoneHandling:
    def test_none_none_match(self):
        headers = ["id", "name"]
        stg = [(1, None)]
        base = [(1, None)]
        result = compute_row_diffs(headers, stg, headers, base, pk_cols=["id"])
        assert result.stg_row_states == ["match"]

    def test_none_empty_string_different(self):
        headers = ["id", "name"]
        stg = [(1, None)]
        base = [(1, "")]
        result = compute_row_diffs(headers, stg, headers, base, pk_cols=["id"])
        assert result.stg_row_states == ["changed"]
        assert result.stg_changed_cols[0] == {"name"}

    def test_none_value_different(self):
        headers = ["id", "name"]
        stg = [(1, None)]
        base = [(1, "Alice")]
        result = compute_row_diffs(headers, stg, headers, base, pk_cols=["id"])
        assert result.stg_row_states == ["changed"]


# ---------------------------------------------------------------------------
# Multi-column PKs
# ---------------------------------------------------------------------------


class TestMultiColumnPk:
    def test_composite_key_matching(self):
        headers = ["org_id", "dept_id", "name"]
        stg = [(1, 10, "A"), (1, 20, "B")]
        base = [(1, 10, "A"), (1, 20, "X")]
        result = compute_row_diffs(
            headers,
            stg,
            headers,
            base,
            pk_cols=["org_id", "dept_id"],
        )
        assert result.stg_row_states == ["match", "changed"]


# ---------------------------------------------------------------------------
# Column order / set differences
# ---------------------------------------------------------------------------


class TestColumnHandling:
    def test_different_column_order(self):
        stg_headers = ["id", "name", "price"]
        base_headers = ["price", "id", "name"]
        stg = [(1, "A", 10)]
        base = [(10, 1, "A")]
        result = compute_row_diffs(stg_headers, stg, base_headers, base, pk_cols=["id"])
        assert result.stg_row_states == ["match"]

    def test_column_only_in_staging_ignored(self):
        stg_headers = ["id", "name", "extra"]
        base_headers = ["id", "name"]
        stg = [(1, "A", "ignored")]
        base = [(1, "A")]
        result = compute_row_diffs(stg_headers, stg, base_headers, base, pk_cols=["id"])
        assert result.stg_row_states == ["match"]
        # 'extra' is not in shared cols so it cannot cause a diff.

    def test_column_only_in_base_ignored(self):
        stg_headers = ["id", "name"]
        base_headers = ["id", "name", "created"]
        stg = [(1, "A")]
        base = [(1, "A", datetime(2025, 1, 1))]
        result = compute_row_diffs(stg_headers, stg, base_headers, base, pk_cols=["id"])
        assert result.stg_row_states == ["match"]


# ---------------------------------------------------------------------------
# exclude_cols
# ---------------------------------------------------------------------------


class TestExcludeCols:
    def test_excluded_col_differs_still_match(self):
        headers = ["id", "name", "rev_user"]
        stg = [(1, "A", "alice")]
        base = [(1, "A", "bob")]
        result = compute_row_diffs(
            headers,
            stg,
            headers,
            base,
            pk_cols=["id"],
            exclude_cols=["rev_user"],
        )
        assert result.stg_row_states == ["match"]

    def test_excluded_col_plus_real_change(self):
        headers = ["id", "name", "rev_user"]
        stg = [(1, "Alice", "alice")]
        base = [(1, "Bob", "bob")]
        result = compute_row_diffs(
            headers,
            stg,
            headers,
            base,
            pk_cols=["id"],
            exclude_cols=["rev_user"],
        )
        assert result.stg_row_states == ["changed"]
        # Only `name` should be in the changed set, not `rev_user`.
        assert result.stg_changed_cols[0] == {"name"}

    def test_multiple_excluded_cols(self):
        headers = ["id", "name", "rev_user", "rev_time"]
        stg = [(1, "A", "x", datetime(2025, 1, 1))]
        base = [(1, "A", "y", datetime(2025, 6, 1))]
        result = compute_row_diffs(
            headers,
            stg,
            headers,
            base,
            pk_cols=["id"],
            exclude_cols=["rev_user", "rev_time"],
        )
        assert result.stg_row_states == ["match"]


# ---------------------------------------------------------------------------
# Summary / edge cases
# ---------------------------------------------------------------------------


class TestSummaryAndEdgeCases:
    def test_summary_format(self):
        headers = ["id", "name"]
        stg = [(1, "A"), (2, "B")]
        base = [(1, "X"), (2, "B")]
        result = compute_row_diffs(headers, stg, headers, base, pk_cols=["id"])
        assert result.summary == "1 matching | 1 differing | 0 only in staging | 0 only in base"

    def test_empty_inputs(self):
        headers = ["id", "name"]
        result = compute_row_diffs(headers, [], headers, [], pk_cols=["id"])
        assert result.stg_row_states == []
        assert result.base_row_states == []
        assert result.summary == "0 matching | 0 differing | 0 only in staging | 0 only in base"

    def test_only_in_staging(self):
        headers = ["id", "name"]
        stg = [(1, "A"), (2, "B")]
        base = [(1, "A")]
        result = compute_row_diffs(headers, stg, headers, base, pk_cols=["id"])
        assert result.stg_row_states == ["match", "only_stg"]
        assert "1 only in staging" in result.summary

    def test_only_in_base(self):
        headers = ["id", "name"]
        stg = [(1, "A")]
        base = [(1, "A"), (2, "B")]
        result = compute_row_diffs(headers, stg, headers, base, pk_cols=["id"])
        assert result.base_row_states == ["match", "only_base"]
        assert "1 only in base" in result.summary

    def test_missing_pk_column(self):
        stg_headers = ["id", "name"]
        base_headers = ["name"]  # no `id`
        stg = [(1, "A")]
        base = [("A",)]
        result = compute_row_diffs(stg_headers, stg, base_headers, base, pk_cols=["id"])
        # Can't match anything — all rows are "only in their table".
        assert result.stg_row_states == ["only_stg"]
        assert result.base_row_states == ["only_base"]

    def test_dataclass_default_factory(self):
        """Ensure DiffResult can be instantiated with no args."""
        d = DiffResult()
        assert d.stg_row_states == []
        assert d.stg_changed_cols == []
        assert d.summary == ""
