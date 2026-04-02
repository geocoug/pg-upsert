"""Tests for pg_upsert.models — no database required."""

from __future__ import annotations

import json

import pytest

from pg_upsert.models import (
    QACheckType,
    QAError,
    TableResult,
    UpsertResult,
    UserCancelledError,
)

# ---------------------------------------------------------------------------
# UserCancelledError
# ---------------------------------------------------------------------------


class TestUserCancelledError:
    def test_is_exception(self):
        assert issubclass(UserCancelledError, Exception)

    def test_can_be_raised_and_caught(self):
        with pytest.raises(UserCancelledError, match="cancelled by user"):
            raise UserCancelledError("cancelled by user")

    def test_empty_message(self):
        err = UserCancelledError()
        assert isinstance(err, Exception)


# ---------------------------------------------------------------------------
# QAError
# ---------------------------------------------------------------------------


class TestQAError:
    def test_to_dict_structure(self):
        err = QAError(table="genres", check_type=QACheckType.NULL, details="genre (2)")
        d = err.to_dict()
        assert d["table"] == "genres"
        assert d["check_type"] == "null"
        assert d["details"] == "genre (2)"

    def test_to_dict_all_check_types(self):
        for check_type in QACheckType:
            err = QAError(table="t", check_type=check_type, details="some detail")
            d = err.to_dict()
            assert d["check_type"] == check_type.value

    def test_to_dict_returns_dict(self):
        err = QAError(table="books", check_type=QACheckType.PRIMARY_KEY, details="pk (1)")
        assert isinstance(err.to_dict(), dict)


# ---------------------------------------------------------------------------
# TableResult
# ---------------------------------------------------------------------------


class TestTableResult:
    def test_defaults(self):
        tr = TableResult(table_name="genres")
        assert tr.rows_updated == 0
        assert tr.rows_inserted == 0
        assert tr.qa_errors == []

    def test_qa_passed_no_errors(self):
        tr = TableResult(table_name="genres")
        assert tr.qa_passed is True

    def test_qa_passed_with_errors(self):
        err = QAError(table="genres", check_type=QACheckType.NULL, details="genre (1)")
        tr = TableResult(table_name="genres", qa_errors=[err])
        assert tr.qa_passed is False

    def test_to_dict_no_errors(self):
        tr = TableResult(table_name="authors", rows_updated=5, rows_inserted=3)
        d = tr.to_dict()
        assert d["table_name"] == "authors"
        assert d["rows_updated"] == 5
        assert d["rows_inserted"] == 3
        assert d["qa_passed"] is True
        assert d["qa_errors"] == []

    def test_to_dict_with_errors(self):
        err = QAError(table="authors", check_type=QACheckType.FOREIGN_KEY, details="book_id (2)")
        tr = TableResult(table_name="authors", qa_errors=[err])
        d = tr.to_dict()
        assert d["qa_passed"] is False
        assert len(d["qa_errors"]) == 1
        assert d["qa_errors"][0]["check_type"] == "fk"

    def test_to_dict_keys_present(self):
        tr = TableResult(table_name="books")
        d = tr.to_dict()
        for key in ("table_name", "rows_updated", "rows_inserted", "qa_passed", "qa_errors"):
            assert key in d


# ---------------------------------------------------------------------------
# UpsertResult
# ---------------------------------------------------------------------------


class TestUpsertResult:
    def test_empty_result(self):
        result = UpsertResult()
        assert result.tables == []
        assert result.committed is False
        assert result.qa_passed is True  # vacuously true — no tables, all pass
        assert result.total_updated == 0
        assert result.total_inserted == 0

    def test_qa_passed_all_clean(self):
        t1 = TableResult(table_name="genres")
        t2 = TableResult(table_name="books")
        result = UpsertResult(tables=[t1, t2])
        assert result.qa_passed is True

    def test_qa_passed_one_failure(self):
        err = QAError(table="books", check_type=QACheckType.NULL, details="title (1)")
        t1 = TableResult(table_name="genres")
        t2 = TableResult(table_name="books", qa_errors=[err])
        result = UpsertResult(tables=[t1, t2])
        assert result.qa_passed is False

    def test_total_updated(self):
        t1 = TableResult(table_name="genres", rows_updated=10)
        t2 = TableResult(table_name="books", rows_updated=5)
        result = UpsertResult(tables=[t1, t2])
        assert result.total_updated == 15

    def test_total_inserted(self):
        t1 = TableResult(table_name="genres", rows_inserted=3)
        t2 = TableResult(table_name="books", rows_inserted=7)
        result = UpsertResult(tables=[t1, t2])
        assert result.total_inserted == 10

    def test_committed_flag(self):
        result = UpsertResult(committed=True)
        assert result.committed is True

    def test_to_dict_structure(self):
        t1 = TableResult(table_name="genres", rows_updated=2, rows_inserted=1)
        result = UpsertResult(tables=[t1], committed=True)
        d = result.to_dict()
        assert d["committed"] is True
        assert d["qa_passed"] is True
        assert d["total_updated"] == 2
        assert d["total_inserted"] == 1
        assert len(d["tables"]) == 1

    def test_to_dict_keys_present(self):
        result = UpsertResult()
        d = result.to_dict()
        for key in ("qa_passed", "committed", "total_updated", "total_inserted", "tables"):
            assert key in d

    def test_to_json_is_valid_json(self):
        t1 = TableResult(table_name="genres", rows_updated=5)
        result = UpsertResult(tables=[t1], committed=False)
        raw = result.to_json()
        parsed = json.loads(raw)
        assert parsed["total_updated"] == 5

    def test_to_json_indent(self):
        result = UpsertResult()
        raw = result.to_json(indent=4)
        # Indented JSON has leading spaces
        assert "    " in raw

    def test_to_json_default_indent(self):
        result = UpsertResult()
        raw = result.to_json()
        parsed = json.loads(raw)
        assert isinstance(parsed, dict)
