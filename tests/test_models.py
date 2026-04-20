"""Tests for pg_upsert.models — no database required."""

from __future__ import annotations

import json

import pytest

from pg_upsert.models import (
    CallbackEvent,
    PipelineEvent,
    QACheckType,
    QAError,
    QASeverity,
    RowViolation,
    SchemaIssue,
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
        assert d["severity"] == "error"

    def test_to_dict_all_check_types(self):
        for check_type in QACheckType:
            err = QAError(table="t", check_type=check_type, details="some detail")
            d = err.to_dict()
            assert d["check_type"] == check_type.value

    def test_to_dict_returns_dict(self):
        err = QAError(table="books", check_type=QACheckType.PRIMARY_KEY, details="pk (1)")
        assert isinstance(err.to_dict(), dict)

    def test_default_violations_and_schema_issues_empty(self):
        err = QAError(table="t", check_type=QACheckType.NULL, details="x")
        assert err.violations == []
        assert err.schema_issues == []

    def test_to_dict_excludes_violations_and_schema_issues(self):
        """Stability: to_dict stays compact so --output json does not grow."""
        err = QAError(
            table="books",
            check_type=QACheckType.NULL,
            details="title (1)",
            violations=[
                RowViolation(
                    pk_values=(1,),
                    row_data={"book_id": 1, "title": None},
                    issue_type="null",
                    issue_column="title",
                    description="NULL in 'title'",
                ),
            ],
            schema_issues=[
                SchemaIssue(check_type="column", column_name="foo", description="missing"),
            ],
        )
        d = err.to_dict()
        assert set(d.keys()) == {"table", "check_type", "details", "severity"}


class TestRowViolation:
    def test_defaults(self):
        v = RowViolation(
            pk_values=(1,),
            row_data={"id": 1, "name": "Alice"},
            issue_type="null",
        )
        assert v.issue_column is None
        assert v.constraint_name is None
        assert v.description == ""
        assert v.pk_columns == []

    def test_full_construction(self):
        v = RowViolation(
            pk_values=(1, 2),
            row_data={"org_id": 1, "dept_id": 2, "name": None},
            issue_type="fk",
            issue_column="dept_id",
            constraint_name="fk_dept",
            description="FK 'fk_dept' violation",
        )
        assert v.pk_values == (1, 2)
        assert v.issue_column == "dept_id"
        assert v.constraint_name == "fk_dept"


class TestSchemaIssue:
    def test_column_missing_defaults(self):
        s = SchemaIssue(check_type="column", column_name="description")
        assert s.staging_type is None
        assert s.base_type is None
        assert s.description == ""

    def test_type_mismatch_full(self):
        s = SchemaIssue(
            check_type="type",
            column_name="priority",
            staging_type="int4",
            base_type="text",
            description="type mismatch",
        )
        assert s.staging_type == "int4"
        assert s.base_type == "text"


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
        tr = TableResult(table_name="genres", _qa_findings=[err])
        assert tr.qa_passed is False

    def test_qa_passed_with_warning_only(self):
        from pg_upsert.models import QASeverity

        err = QAError(
            table="genres",
            check_type=QACheckType.COLUMN_EXISTENCE,
            details="notes",
            severity=QASeverity.WARNING,
        )
        tr = TableResult(table_name="genres", _qa_findings=[err])
        assert tr.qa_passed is True

    def test_qa_passed_with_warning_and_error(self):
        from pg_upsert.models import QASeverity

        warn = QAError(
            table="genres",
            check_type=QACheckType.COLUMN_EXISTENCE,
            details="notes",
            severity=QASeverity.WARNING,
        )
        err = QAError(table="genres", check_type=QACheckType.NULL, details="genre (1)")
        tr = TableResult(table_name="genres", _qa_findings=[warn, err])
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
        tr = TableResult(table_name="authors", _qa_findings=[err])
        d = tr.to_dict()
        assert d["qa_passed"] is False
        assert len(d["qa_errors"]) == 1
        assert d["qa_errors"][0]["check_type"] == "fk"

    def test_to_dict_keys_present(self):
        tr = TableResult(table_name="books")
        d = tr.to_dict()
        for key in ("table_name", "rows_updated", "rows_inserted", "qa_passed", "qa_errors", "qa_warnings"):
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
        t2 = TableResult(table_name="books", _qa_findings=[err])
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


# ---------------------------------------------------------------------------
# PipelineEvent
# ---------------------------------------------------------------------------


class TestPipelineEvent:
    def test_qa_errors_filters_to_error_only(self):
        warn = QAError(
            table="t",
            check_type=QACheckType.COLUMN_EXISTENCE,
            details="notes",
            severity=QASeverity.WARNING,
        )
        err = QAError(table="t", check_type=QACheckType.NULL, details="x (1)")
        event = PipelineEvent(
            event=CallbackEvent.QA_TABLE_COMPLETE,
            table="t",
            qa_findings=[warn, err],
        )
        assert len(event.qa_errors) == 1
        assert event.qa_errors[0].severity == QASeverity.ERROR

    def test_qa_warnings_filters_to_warning_only(self):
        warn = QAError(
            table="t",
            check_type=QACheckType.COLUMN_EXISTENCE,
            details="notes",
            severity=QASeverity.WARNING,
        )
        err = QAError(table="t", check_type=QACheckType.NULL, details="x (1)")
        event = PipelineEvent(
            event=CallbackEvent.QA_TABLE_COMPLETE,
            table="t",
            qa_findings=[warn, err],
        )
        assert len(event.qa_warnings) == 1
        assert event.qa_warnings[0].severity == QASeverity.WARNING

    def test_qa_passed_true_with_warnings_only(self):
        warn = QAError(
            table="t",
            check_type=QACheckType.COLUMN_EXISTENCE,
            details="notes",
            severity=QASeverity.WARNING,
        )
        event = PipelineEvent(
            event=CallbackEvent.QA_TABLE_COMPLETE,
            table="t",
            qa_passed=True,
            qa_findings=[warn],
        )
        assert event.qa_passed is True
        assert event.qa_errors == []
        assert len(event.qa_warnings) == 1
