"""Data models for pg-upsert results and QA reporting."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum


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
class QAError:
    """A single QA check finding.

    Attributes:
        table: The table where the error was found.
        check_type: The type of QA check that produced this error.
        details: Human-readable error description, e.g. ``"genre (3)"``
            for 3 null values in the ``genre`` column.
    """

    table: str
    check_type: QACheckType
    details: str

    def to_dict(self) -> dict:
        return {
            "table": self.table,
            "check_type": self.check_type.value,
            "details": self.details,
        }


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
