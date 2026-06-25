#!/usr/bin/env python
"""Tests for per-table column excludes (issue #30).

Pure-logic tests (merge + validation) run without a database; the
integration tests that verify values land merged in the control table are
marked ``@pytest.mark.postgres``.
"""

from __future__ import annotations

import pytest

from pg_upsert.control import _merge_excludes
from pg_upsert.upsert import PgUpsert

TABLES = ("genres", "books", "authors", "book_authors", "publishers")


# ---------------------------------------------------------------------------
# _merge_excludes (pure)
# ---------------------------------------------------------------------------


class TestMergeExcludes:
    def test_global_only(self):
        assert _merge_excludes(["a", "b"], None, "books") == ["a", "b"]

    def test_per_table_appended_to_global(self):
        assert _merge_excludes(["a"], {"books": ["b"]}, "books") == ["a", "b"]

    def test_other_table_unaffected(self):
        assert _merge_excludes(["a"], {"books": ["b"]}, "authors") == ["a"]

    def test_dedup_preserves_first_seen_order(self):
        assert _merge_excludes(["a", "b"], {"books": ["b", "c"]}, "books") == ["a", "b", "c"]

    def test_all_empty(self):
        assert _merge_excludes(None, None, "books") == []

    def test_accepts_tuples(self):
        assert _merge_excludes(("a",), {"books": ("b",)}, "books") == ["a", "b"]


# ---------------------------------------------------------------------------
# PgUpsert._validate_by_table (pure)
# ---------------------------------------------------------------------------


class TestValidateByTable:
    def test_none_returns_none(self):
        assert PgUpsert._validate_by_table(None, TABLES, "exclude_cols_by_table") is None

    def test_empty_returns_none(self):
        assert PgUpsert._validate_by_table({}, TABLES, "exclude_cols_by_table") is None

    def test_normalises_comma_string_value(self):
        result = PgUpsert._validate_by_table({"books": "a, b"}, TABLES, "exclude_cols_by_table")
        assert result == {"books": ["a", "b"]}

    def test_normalises_list_value_to_str(self):
        result = PgUpsert._validate_by_table({"books": ["a", 1]}, TABLES, "exclude_cols_by_table")
        assert result == {"books": ["a", "1"]}

    def test_rejects_non_dict(self):
        with pytest.raises(ValueError, match="must be a mapping"):
            PgUpsert._validate_by_table(["books"], TABLES, "exclude_cols_by_table")

    def test_rejects_unknown_table(self):
        with pytest.raises(ValueError, match="not in the configured tables"):
            PgUpsert._validate_by_table({"nope": ["x"]}, TABLES, "exclude_cols_by_table")

    def test_rejects_bad_value_type(self):
        with pytest.raises(ValueError, match="must be a list of column names"):
            PgUpsert._validate_by_table({"books": 5}, TABLES, "exclude_cols_by_table")


# ---------------------------------------------------------------------------
# Integration: per-table excludes land merged in the control table
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPerTableExcludesIntegration:
    def _make(self, db, **kwargs):
        return PgUpsert(
            conn=db.conn,
            tables=TABLES,
            staging_schema="staging",
            base_schema="public",
            do_commit=False,
            interactive=False,
            **kwargs,
        )

    def test_exclude_cols_merged_per_table(self, db):
        ups = self._make(
            db,
            exclude_cols=("rev_user",),
            exclude_cols_by_table={"books": ["rev_time"]},
        )
        books = ups._control.get_table_spec("books")
        authors = ups._control.get_table_spec("authors")
        # books gets global + per-table; authors gets global only.
        assert set(books["exclude_cols"].split(",")) == {"rev_user", "rev_time"}
        assert authors["exclude_cols"] == "rev_user"

    def test_per_table_only_other_tables_empty(self, db):
        ups = self._make(db, exclude_cols_by_table={"books": ["rev_time"]})
        assert ups._control.get_table_spec("books")["exclude_cols"] == "rev_time"
        assert not ups._control.get_table_spec("genres")["exclude_cols"]

    def test_null_check_excludes_merged_per_table(self, db):
        ups = self._make(
            db,
            exclude_null_check_cols_by_table={"books": ["book_alias"]},
        )
        assert ups._control.get_table_spec("books")["exclude_null_checks"] == "book_alias"
        assert not ups._control.get_table_spec("genres")["exclude_null_checks"]

    def test_invalid_table_rejected_at_construction(self, db):
        with pytest.raises(ValueError, match="not in the configured tables"):
            self._make(db, exclude_cols_by_table={"not_a_table": ["x"]})
