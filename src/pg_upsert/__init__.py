from importlib.metadata import PackageNotFoundError, version

from .models import (
    CallbackEvent,
    CheckContext,
    PipelineCallback,
    PipelineEvent,
    QACheckType,
    QAError,
    QASeverity,
    RowViolation,
    SchemaIssue,
    TableResult,
    UpsertResult,
)
from .postgres import PostgresDB
from .upsert import PgUpsert, UserCancelledError

try:
    __version__ = version("pg_upsert")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = [
    "PgUpsert",
    "PostgresDB",
    "UserCancelledError",
    "UpsertResult",
    "TableResult",
    "QAError",
    "QACheckType",
    "QASeverity",
    "RowViolation",
    "SchemaIssue",
    "CallbackEvent",
    "CheckContext",
    "PipelineEvent",
    "PipelineCallback",
]
