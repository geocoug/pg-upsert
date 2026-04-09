"""Data models for pg-upsert results and QA reporting."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class UserCancelledError(Exception):
    """Raised when the user cancels an interactive operation."""


class QACheckType(Enum):
    """Types of QA checks performed on staging data."""

    NULL = "null"
    PRIMARY_KEY = "pk"
    UNIQUE = "unique"
    FOREIGN_KEY = "fk"
    CHECK_CONSTRAINT = "ck"
    TYPE_MISMATCH = "type"
    COLUMN_EXISTENCE = "column"


@dataclass
class RowViolation:
    """One problem detected on one staging row.

    Multiple violations may reference the same staging row (same
    ``pk_values``); :mod:`pg_upsert.export` deduplicates and merges
    them into a single fix-sheet entry per row.

    Attributes:
        pk_values: Primary key tuple for this staging row.  Used as the
            dedup key when building the fix sheet.  Tables without a PK
            fall back to a tuple of all column values.
        pk_columns: PK column names in declared order, parallel to
            ``pk_values``.  Empty for tables with no primary key.  Used
            by the export layer to sort the fix sheet by PK.
        row_data: Full staging row contents as a column -> value dict.
        issue_type: Short identifier — ``"null"``, ``"pk"``, ``"fk"``,
            ``"unique"``, or ``"ck"``.
        issue_column: For NULL/FK/UNIQUE, the column (or comma-joined
            columns) responsible for the violation.
        constraint_name: For PK/FK/UNIQUE/CK, the constraint that failed.
        description: Human-readable phrase used in the fix sheet's
            ``_issues`` column, e.g. ``"NULL in 'genre'"``.
    """

    pk_values: tuple
    row_data: dict[str, Any]
    issue_type: str
    pk_columns: list[str] = field(default_factory=list)
    issue_column: str | None = None
    constraint_name: str | None = None
    description: str = ""


@dataclass
class SchemaIssue:
    """One schema-level problem detected by column existence / type checks.

    Schema issues have no row data — they describe a structural mismatch
    between the staging and base tables.  They are written to a dedicated
    ``_schema`` output separate from the row-level fix sheets.

    Attributes:
        check_type: ``"column"`` (missing) or ``"type"`` (mismatch).
        column_name: Column with the issue.
        staging_type: Staging type (type mismatch only).
        base_type: Base type (type mismatch only).
        description: Human-readable description.
    """

    check_type: str
    column_name: str
    staging_type: str | None = None
    base_type: str | None = None
    description: str = ""


@dataclass
class QAError:
    """A single QA check finding.

    Attributes:
        table: The table where the error was found.
        check_type: The type of QA check that produced this error.
        details: Human-readable error summary, e.g. ``"genre (3)"``
            for 3 null values in the ``genre`` column.
        violations: Per-row violations captured when ``--export-failures``
            is active.  Used by the export module to build fix sheets.
            Excluded from :meth:`to_dict` to keep the ``--output json`` API
            stable.
        schema_issues: For column-existence and type-mismatch checks,
            structured metadata that the export module writes to the
            ``_schema`` output.  Excluded from :meth:`to_dict`.
    """

    table: str
    check_type: QACheckType
    details: str
    violations: list[RowViolation] = field(default_factory=list, repr=False)
    schema_issues: list[SchemaIssue] = field(default_factory=list, repr=False)

    def to_dict(self) -> dict:
        return {
            "table": self.table,
            "check_type": self.check_type.value,
            "details": self.details,
        }


@dataclass
class CheckContext:
    """Progress context passed through QA check methods to display functions.

    Attributes:
        table_num: Current table number (1-based).
        total_tables: Total number of tables being checked.
    """

    table_num: int
    total_tables: int


@dataclass
class TableResult:
    """Per-table result from a QA check or upsert operation.

    Attributes:
        table_name: The name of the table.
        rows_updated: Number of rows updated during the upsert.
        rows_inserted: Number of rows inserted during the upsert.
        qa_errors: List of QA errors found for this table.
    """

    table_name: str
    rows_updated: int = 0
    rows_inserted: int = 0
    qa_errors: list[QAError] = field(default_factory=list)

    @property
    def qa_passed(self) -> bool:
        """True if no QA errors were found for this table."""
        return len(self.qa_errors) == 0

    def to_dict(self) -> dict:
        return {
            "table_name": self.table_name,
            "rows_updated": self.rows_updated,
            "rows_inserted": self.rows_inserted,
            "qa_passed": self.qa_passed,
            "qa_errors": [e.to_dict() for e in self.qa_errors],
        }


@dataclass
class UpsertResult:
    """Structured result from a ``PgUpsert.run()`` call.

    Provides programmatic access to QA results and upsert statistics
    for all tables processed.

    Attributes:
        tables: Per-table results.
        committed: Whether the transaction was committed.
        staging_schema: Name of the staging schema.
        base_schema: Name of the base schema.
        upsert_method: The upsert method used (upsert, update, insert).
        started_at: ISO 8601 timestamp when the run started.
        finished_at: ISO 8601 timestamp when the run finished.
        duration_seconds: Elapsed time in seconds.
    """

    tables: list[TableResult] = field(default_factory=list)
    committed: bool = False
    staging_schema: str = ""
    base_schema: str = ""
    upsert_method: str = ""
    started_at: str = ""
    finished_at: str = ""
    duration_seconds: float = 0.0

    @property
    def qa_passed(self) -> bool:
        """True if all tables passed QA checks."""
        return all(t.qa_passed for t in self.tables)

    @property
    def total_updated(self) -> int:
        """Total rows updated across all tables."""
        return sum(t.rows_updated for t in self.tables)

    @property
    def total_inserted(self) -> int:
        """Total rows inserted across all tables."""
        return sum(t.rows_inserted for t in self.tables)

    def to_dict(self) -> dict:
        """Serialize to a dictionary for JSON output."""
        return {
            "staging_schema": self.staging_schema,
            "base_schema": self.base_schema,
            "upsert_method": self.upsert_method,
            "qa_passed": self.qa_passed,
            "committed": self.committed,
            "total_updated": self.total_updated,
            "total_inserted": self.total_inserted,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "tables": [t.to_dict() for t in self.tables],
        }

    def to_json(self, indent: int = 2) -> str:
        """Serialize to a JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def export_failures(self, directory: str | Path, fmt: str = "csv") -> Path | None:
        """Export QA violations as a "fix sheet" grouped by table.

        Writes one row per unique violating staging row to *directory*,
        with an ``_issues`` column summarising every problem found on
        that row.  Format is selected by *fmt* (``csv``, ``json``, or
        ``xlsx``); see :func:`pg_upsert.export.export_failures` for the
        exact file layout per format.

        Returns the directory written, or ``None`` if there are no
        exportable violations.
        """
        all_errors = [e for t in self.tables for e in t.qa_errors]
        from .export import export_failures

        return export_failures(all_errors, directory, fmt=fmt)


class CallbackEvent(Enum):
    """Events fired during the pg-upsert pipeline."""

    QA_TABLE_COMPLETE = "qa_table_complete"
    UPSERT_TABLE_COMPLETE = "upsert_table_complete"


@dataclass
class PipelineEvent:
    """Data passed to the pipeline callback at each event.

    Attributes:
        event: The type of event.
        table: The table name this event relates to.
        qa_passed: Whether QA passed for this table (``None`` if not yet determined).
        rows_updated: Rows updated (0 if not applicable yet).
        rows_inserted: Rows inserted (0 if not applicable yet).
        qa_errors: QA errors found for this table.
    """

    event: CallbackEvent
    table: str
    qa_passed: bool | None = None
    rows_updated: int = 0
    rows_inserted: int = 0
    qa_errors: list[QAError] = field(default_factory=list)


PipelineCallback = Callable[[PipelineEvent], bool | None]
"""Callback type for pipeline events.

Return ``False`` to abort the pipeline (triggers rollback).
Return ``True`` or ``None`` to continue.
"""
