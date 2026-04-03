from importlib.metadata import PackageNotFoundError, version

from .cli import app
from .models import (
    CallbackEvent,
    PipelineCallback,
    PipelineEvent,
    QACheckType,
    QAError,
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
    "CallbackEvent",
    "PipelineEvent",
    "PipelineCallback",
]
